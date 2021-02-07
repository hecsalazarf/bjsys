use crate::task::{Task, TaskData, TaskId};
use embed::collections::{Queue, QueueDb, QueueEvent, SortedSetDb};
use embed::{Env, Environment, EnvironmentFlags, Manager, Result, Store, Transaction};

pub use embed::Error as RepoError;

#[derive(Debug, Clone)]
pub struct Repository {
  env: Env,
  tasks: Store<TaskData>,
  queues: QueueDb,
  delays: SortedSetDb,
}

impl Repository {
  pub async fn open() -> Result<Self> {
    let manager = Manager::singleton();
    // TODO: Pass configuration as arguments
    let mut env_flags = EnvironmentFlags::empty();
    env_flags.set(EnvironmentFlags::NO_SYNC, true);
    let mut builder = Environment::new();
    builder
      .set_map_size(1024 * 1024 * 1024)
      .set_max_dbs(20)
      .set_flags(env_flags);
    let env = manager
      .write()
      .expect("manager write guard")
      .get_or_init(builder, "/tmp/bjsys")?;

    let tasks = Store::open(&env, "tasks")?;
    let queues = QueueDb::open(&env)?;
    let delays = SortedSetDb::open(&env)?;

    Ok(Self {
      env,
      tasks,
      queues,
      delays,
    })
  }

  pub async fn create(&self, data: &TaskData) -> Result<TaskId> {
    let mut is_push = false;
    let waiting = self.get_queue(&data.queue, QueueGroup::Waiting);
    let mut txw = self.env.begin_rw_txn_async().await?;

    let id = TaskId::new_v4();
    if data.delay > 0 {
      let score = time_to_delay(data.delay);
      let set = self.delays.get(&data.queue);
      set.add(&mut txw, score, id.as_bytes())?;
    } else {
      waiting.push(&mut txw, id.as_bytes())?;
      is_push = true;
    }
    self.tasks.put(&mut txw, id.as_bytes(), data)?;
    txw.commit()?;

    if is_push {
      waiting.publish(QueueEvent::Push).await;
    }
    Ok(id)
  }

  pub async fn find_in_process<S: AsRef<str>>(&self, queue: S) -> Result<Vec<TaskId>> {
    let in_process = self.get_queue(queue.as_ref(), QueueGroup::InProcess);
    let txr = self.env.begin_ro_txn()?;

    in_process
      .iter(&txr)?
      .map(|res| res.map(|val| TaskId::from_slice(val).unwrap()))
      .collect()
  }

  pub async fn find_new<S: AsRef<str>>(&self, queue: S) -> Result<Task> {
    let queue = queue.as_ref();
    let waiting = self.get_queue(queue, QueueGroup::Waiting);
    let in_process = self.get_queue(queue, QueueGroup::InProcess);
    loop {
      let mut txw = self.env.begin_rw_txn_async().await?;

      if let Some(el) = waiting.pop(&mut txw)? {
        // If there is a task, push it to the in_process queue
        let id = TaskId::from_slice(el).expect("uuid from value");
        let id_bytes = id.as_bytes();
        in_process.push(&mut txw, id_bytes)?;
        let mut data = self.tasks.get(&txw, id_bytes)?.expect("task data");
        // Increment task's deliveries counter
        data.deliveries += 1;
        // Set time stamp
        data.processed_on = now_as_millis();
        // And then update it
        self.tasks.put(&mut txw, id_bytes, &data)?;
        txw.commit()?;
        return Ok(Task::from_parts(id, data));
      }

      // If no available task, drop transaction not to block other threads
      txw.abort();
      // Subscribe for queue events
      let mut sub = waiting.subscribe().await;
      loop {
        // Only listen to pushes
        if sub.recv().await == QueueEvent::Push {
          break;
        }
      }
    }
  }

  pub async fn finish(&self, task: Task) -> Result<()> {
    let (id, data) = task.into_parts();
    let id = id.as_bytes();
    let in_process = self.get_queue(&data.queue, QueueGroup::InProcess);
    let done = self.get_queue(&data.queue, QueueGroup::Done);

    let mut txw = self.env.begin_rw_txn_async().await?;

    if let Some(mut data) = self.tasks.get(&txw, id)? {
      data.status = data.status;
      data.message = data.message;
      data.finished_on = now_as_millis();
      in_process.remove(&mut txw, 1, id)?;
      done.push(&mut txw, id)?;
      self.tasks.put(&mut txw, id, &data)?;
      txw.commit()?;
      return Ok(());
    }
    Err(RepoError::NotFound)
  }

  pub async fn retry(&self, task: Task) -> Result<()> {
    let id = task.id().as_bytes();

    let mut txw = self.env.begin_rw_txn_async().await?;

    if let Some(data) = self.tasks.get(&txw, id)? {
      if (data.deliveries - 1) < data.retry {
        let score = time_to_delay(backoff_time(data.deliveries as u64));

        let in_process = self.get_queue(&data.queue, QueueGroup::InProcess);
        let delayed = self.delays.get(&data.queue);

        in_process.remove(&mut txw, 1, id)?;
        delayed.add(&mut txw, score, id)?;
        txw.commit()?;
        return Ok(());
      } else {
        txw.abort();
        return self.finish(task).await;
      }
    }
    Err(RepoError::NotFound)
  }

  pub async fn schedule_delayed<S: AsRef<str>>(&self, queue: S) -> Result<()> {
    let queue = queue.as_ref();
    let waiting = self.get_queue(queue, QueueGroup::Waiting);
    let delayed = self.delays.get(queue);
    let max = now_as_millis();
    let mut txw = self.env.begin_rw_txn_async().await?;

    for res in delayed.range_by_score(&txw, ..max)? {
      let id = res?;
      waiting.push(&mut txw, id)?;
      delayed.remove(&mut txw, id)?;
    }

    txw.commit()?;
    waiting.publish(QueueEvent::Push).await;
    Ok(())
  }

  pub async fn renqueue<Q, I>(&self, queue: Q, ids: I) -> Result<()>
  where
    Q: AsRef<str>,
    I: Send + Iterator<Item = TaskId>,
  {
    let in_process = self.get_queue(queue.as_ref(), QueueGroup::InProcess);
    let waiting = self.get_queue(queue.as_ref(), QueueGroup::Waiting);
    let mut txw = self.env.begin_rw_txn_async().await?;

    for id in ids {
      let id_bytes = id.as_bytes();
      in_process.remove(&mut txw, 1, id_bytes)?;
      waiting.push(&mut txw, id_bytes)?;
    }
    txw.commit()
  }

  fn get_queue(&self, queue: &str, group: QueueGroup) -> Queue {
    let suffix = match group {
      QueueGroup::InProcess => QueueSuffix::IN_PROCESS,
      QueueGroup::Done => QueueSuffix::DONE,
      QueueGroup::Waiting => QueueSuffix::WAITING,
    };

    let queue_name = format!("{}_{}", queue, suffix);
    self.queues.get(queue_name)
  }
}

/// Calculate the time in milliseconds at which a task should be processed.
/// We add the current time to the given delay.
fn time_to_delay(delay: u64) -> u64 {
  use std::time::SystemTime;

  let delay = std::time::Duration::from_millis(delay);
  let now = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap();

  if let Some(d) = now.checked_add(delay) {
    d.as_millis() as u64
  } else {
    u64::MAX
  }
}

/// Return the current time in milliseconds as u64
fn now_as_millis() -> u64 {
  use std::time::SystemTime;

  SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}

/// Retries are computed with an exponential backoff. The formula is taken
/// from the one used in Sidekiq.
/// ```
/// 15 + count ^ 4 + (rand(30) * (count + 1))
/// ```
/// * 15 establishes a minimum wait time.
/// * count.^4 is our exponential, the 20th retry will 20^4 (160,000 sec), or about two days.
/// * rand(30) gives us a random "smear". Sometimes people enqueue 1000s of jobs at one time,
/// which all fail for the same reason. This ensures we don't retry 1000s of jobs all at the
/// exact same time and take down a system.
fn backoff_time(count: u64) -> u64 {
  use rand::Rng;
  const POWER: u32 = 4;

  let mut rng = rand::thread_rng();
  let rnd = rng.gen_range(1..=30);

  // Multiplied by 1000 to obtain milliseconds
  (15 + count.pow(POWER) + (rnd * (count + 1))) * 1000
}

struct QueueSuffix;
impl QueueSuffix {
  const IN_PROCESS: &'static str = "inprocess";
  const DONE: &'static str = "done";
  const WAITING: &'static str = "waiting";
}

enum QueueGroup {
  InProcess,
  Done,
  Waiting,
}
