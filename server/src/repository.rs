use crate::task::{Task, TaskData, TaskId};
use embed::collections::{QueueDb, QueueEvent, SortedSetDb};
use embed::{Env, EnvironmentBuilder, EnvironmentFlags, Manager, Result, Store, Transaction};
use std::sync::Arc;

pub use embed::Error as RepoError;

#[derive(Debug)]
pub struct RepoBuilder {
  env_builder: EnvironmentBuilder,
  env_flags: EnvironmentFlags,
}

impl RepoBuilder {
  pub fn _no_sync(&mut self, active: bool) -> &mut Self {
    self.env_flags.set(EnvironmentFlags::NO_SYNC, active);
    self
  }

  pub fn _map_size(&mut self, size: usize) -> &mut Self {
    self.env_builder.set_map_size(size);
    self
  }

  pub fn open<P>(mut self, path: P) -> Result<Repository>
  where
    P: AsRef<std::path::Path>,
  {
    self.env_builder.set_flags(self.env_flags);

    let env = Manager::singleton()
      .write()
      .expect("manager write guard")
      .get_or_init(self.env_builder, path)?;

    let storage = RepoStorage {
      tasks: Store::open(&env, "tasks")?,
      queues: QueueDb::open(&env)?,
      sorted_sets: SortedSetDb::open(&env)?,
    };

    Ok(Repository {
      env,
      storage: Arc::new(storage),
    })
  }
}

impl Default for RepoBuilder {
  fn default() -> Self {
    let mut env_flags = EnvironmentFlags::empty();
    env_flags.insert(EnvironmentFlags::NO_SYNC);
    env_flags.insert(EnvironmentFlags::NO_SUB_DIR);
    let mut env_builder = embed::Environment::new();

    env_builder
      // 1Gb of data is about 3 millions of tasks
      .set_map_size(1024 * 1024 * 1024)
      // No particular reason, currently less than 10 Dbs are needed
      .set_max_dbs(10)
      .set_flags(env_flags);

    Self {
      env_builder,
      env_flags,
    }
  }
}

#[derive(Debug)]
struct RepoStorage {
  tasks: Store<TaskData>,
  queues: QueueDb,
  sorted_sets: SortedSetDb,
}

#[derive(Debug, Clone)]
pub struct Repository {
  env: Env,
  storage: Arc<RepoStorage>,
}

impl Repository {
  pub fn build() -> RepoBuilder {
    RepoBuilder::default()
  }

  pub async fn create(&self, data: &TaskData) -> Result<TaskId> {
    let mut is_push = false;
    let waiting = self.queue(&data.queue, QueueGroup::Waiting);
    let mut txw = self.env.begin_rw_txn_async().await?;

    let id = TaskId::new_v4();
    if data.delay > 0 {
      let score = time_to_delay(data.delay);
      let set = self.sorted_set(&data.queue);
      set.add(&mut txw, score, id.as_bytes())?;
    } else {
      waiting.push(&mut txw, id.as_bytes())?;
      is_push = true;
    }
    self.tasks().put(&mut txw, id.as_bytes(), data)?;
    txw.commit()?;

    if is_push {
      waiting.publish(QueueEvent::Push).await;
    }
    Ok(id)
  }

  pub async fn find_in_process<S: AsRef<str>>(&self, queue: S) -> Result<Vec<TaskId>> {
    let in_process = self.queue(queue.as_ref(), QueueGroup::InProcess);
    let txr = self.env.begin_ro_txn()?;

    in_process
      .iter(&txr)?
      .map(|res| res.map(|val| TaskId::from_slice(val).unwrap()))
      .collect()
  }

  /// Retrieves next task to be processed from `queue` and move it to the InProcess queue.
  pub async fn process_next<S: AsRef<str>>(&self, queue: S) -> Result<Option<Task>> {
    let queue = queue.as_ref();
    let waiting = self.queue(queue, QueueGroup::Waiting);
    let mut txw = self.env.begin_rw_txn_async().await?;

    if let Some(el) = waiting.pop(&mut txw)? {
      // If there is a task, push it to the in_process queue
      let id = TaskId::from_slice(el).expect("uuid from value");
      let id_bytes = id.as_bytes();
      let in_process = self.queue(queue, QueueGroup::InProcess);
      in_process.push(&mut txw, id_bytes)?;
      let mut data = self.tasks().get(&txw, id_bytes)?.expect("task data");
      // Increment task's deliveries counter
      data.deliveries += 1;
      // Set time stamp
      data.processed_on = now_as_millis();
      // And then update it
      self.tasks().put(&mut txw, id_bytes, &data)?;
      txw.commit()?;
      return Ok(Some(Task::from_parts(id, data)));
    }
    Ok(None)
  }

  /// Get notified when `queue` has been pushed with a new element.
  pub async fn notified<S: AsRef<str>>(&self, queue: S) {
    let waiting = self.queue(queue.as_ref(), QueueGroup::Waiting);
    // Subscribe for queue events
    let mut sub = waiting.subscribe().await;
    loop {
      // Only listen to pushes
      if sub.recv().await == QueueEvent::Push {
        break;
      }
    }
  }

  pub async fn finish(&self, task: Task) -> Result<()> {
    let (id, data) = task.into_parts();
    let id = id.as_bytes();
    let in_process = self.queue(&data.queue, QueueGroup::InProcess);
    let done = self.queue(&data.queue, QueueGroup::Done);

    let mut txw = self.env.begin_rw_txn_async().await?;

    if let Some(mut data) = self.tasks().get(&txw, id)? {
      data.status = data.status;
      data.message = data.message;
      data.finished_on = now_as_millis();
      in_process.remove(&mut txw, 1, id)?;
      done.push(&mut txw, id)?;
      self.tasks().put(&mut txw, id, &data)?;
      txw.commit()?;
      return Ok(());
    }
    Err(RepoError::NotFound)
  }

  pub async fn retry(&self, task: Task) -> Result<()> {
    let id = task.id().as_bytes();
    let mut txw = self.env.begin_rw_txn_async().await?;

    if let Some(data) = self.tasks().get(&txw, id)? {
      if (data.deliveries - 1) < data.retry {
        let score = time_to_delay(backoff_time(data.deliveries as u64));

        let in_process = self.queue(&data.queue, QueueGroup::InProcess);
        let delayed = self.sorted_set(&data.queue);

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
    let waiting = self.queue(queue, QueueGroup::Waiting);
    let delayed = self.sorted_set(queue);
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
    let in_process = self.queue(queue.as_ref(), QueueGroup::InProcess);
    let waiting = self.queue(queue.as_ref(), QueueGroup::Waiting);
    let mut txw = self.env.begin_rw_txn_async().await?;

    for id in ids {
      let id_bytes = id.as_bytes();
      in_process.remove(&mut txw, 1, id_bytes)?;
      waiting.push(&mut txw, id_bytes)?;
    }
    txw.commit()
  }

  fn queue(&self, queue: &str, group: QueueGroup) -> embed::collections::Queue {
    let suffix = match group {
      QueueGroup::InProcess => QueueSuffix::IN_PROCESS,
      QueueGroup::Done => QueueSuffix::DONE,
      QueueGroup::Waiting => QueueSuffix::WAITING,
    };

    let queue_name = format!("{}_{}", queue, suffix);
    self.storage.queues.get(queue_name)
  }

  fn sorted_set(&self, queue: &str) -> embed::collections::SortedSet {
    self.storage.sorted_sets.get(queue)
  }

  fn tasks(&self) -> &Store<TaskData> {
    &self.storage.tasks
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
