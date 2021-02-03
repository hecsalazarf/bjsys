use embed::collections::{QueueDb, QueueEvent, SortedSetDb};
use embed::{Environment, EnvironmentFlags, Manager, Result, Store, Transaction};
use proto::{AckRequest, FetchResponse, TaskData};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Storel {
  env: Arc<RwLock<Environment>>,
  tasks: Store<TaskData>,
  queues: QueueDb,
  delays: SortedSetDb,
}

impl Storel {
  pub async fn open() -> Result<Self> {
    let manager = Manager::singleton();
    // TODO: Pass configuration as arguments
    let mut env_flags = EnvironmentFlags::empty();
    env_flags.set(EnvironmentFlags::NO_SYNC, true);
    env_flags.set(EnvironmentFlags::NO_LOCK, true);
    let mut builder = Environment::new();
    builder
      .set_map_size(1024 * 1024 * 1024)
      .set_max_dbs(20)
      .set_flags(env_flags);
    let env = manager
      .write()
      .expect("write guard")
      .get_or_init(builder, "/tmp/bjsys")?;

    let wr_env = env.write().await;
    let tasks = Store::open(&wr_env, "tasks")?;
    let queues = QueueDb::open(&wr_env)?;
    let delays = SortedSetDb::open(&wr_env)?;
    drop(wr_env);

    Ok(Self {
      env,
      tasks,
      queues,
      delays,
    })
  }

  pub async fn create_task(&self, data: &TaskData) -> Result<Uuid> {
    let mut is_push = false;
    
    let queue_name = generate_key(&data.queue, QueueSuffix::WAITING);
    let queue = self.queues.get(queue_name);
    
    let writer = self.env.write().await;
    let mut txw = writer.begin_rw_txn()?;

    let id = Uuid::new_v4();
    if data.delay > 0 {
      let score = time_to_delay(data.delay);
      let set = self.delays.get(&data.queue);
      set.add(&mut txw, score, id.as_bytes())?;
    } else {
      queue.push(&mut txw, id.as_bytes())?;
      is_push = true;
    }
    self.tasks.put(&mut txw, id.as_bytes(), data)?;
    txw.commit()?;
    drop(writer);

    if is_push {
      queue.publish(QueueEvent::Push).await;
    }
    Ok(id)
  }

  pub async fn read_pending(&self, queue: &str) -> Result<Vec<Uuid>> {
    let writer = self.env.read().await;
    let txr = writer.begin_ro_txn()?;
    let queue_name = generate_key(queue, QueueSuffix::PENDING);
    let tasks = self.queues.get(queue_name).iter(&txr)?;

    tasks
      .map(|res| res.map(|val| Uuid::from_slice(val).unwrap()))
      .collect()
  }

  pub async fn read_new(&self, queue: &str) -> Result<FetchResponse> {
    let waiting = self.queues.get(generate_key(queue, QueueSuffix::WAITING));
    let pending = self.queues.get(generate_key(queue, QueueSuffix::PENDING));
    loop {
      let writer = self.env.write().await;
      let mut txw = writer.begin_rw_txn()?;

      if let Some(el) = waiting.pop(&mut txw)? {
        // If there is a task, push it to the pending queue
        let uuid = Uuid::from_slice(el).expect("uuid from value");
        pending.push(&mut txw, uuid.as_bytes())?;
        let mut task = self.tasks.get(&txw, uuid.as_bytes())?.expect("task data");
        // Increment task's deliveries counter
        task.deliveries += 1;
        // Set time stamp
        task.processed_on = now_as_millis();
        // And then update it
        self.tasks.put(&mut txw, uuid.as_bytes(), &task)?;
        txw.commit()?;
        return Ok(FetchResponse::from_pair(uuid.to_string(), task));
      }

      // If no available task, drop transaction not to block other threads
      txw.abort();
      drop(writer);
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

  pub async fn finish(&self, req: AckRequest) -> Result<bool> {
    let pending = self
      .queues
      .get(generate_key(&req.queue, QueueSuffix::PENDING));
    let done = self.queues.get(generate_key(&req.queue, QueueSuffix::DONE));

    let writer = self.env.write().await;
    let mut txw = writer.begin_rw_txn()?;
    // TODO: DO NOT CREATE UUID IN HERE
    let uuid = Uuid::parse_str(&req.task_id).expect("uuid from str");
    if let Some(mut data) = self.tasks.get(&txw, uuid.as_bytes())? {
      data.status = req.status;
      data.message = req.message;
      data.finished_on = now_as_millis();
      pending.remove(&mut txw, 1, uuid.as_bytes())?;
      done.push(&mut txw, &req.task_id)?;
      self.tasks.put(&mut txw, uuid.as_bytes(), &data)?;
      txw.commit()?;
      return Ok(true);
    }
    Ok(false)
  }

  pub async fn fail(&self, req: AckRequest) -> Result<bool> {
    let writer = self.env.write().await;
    let mut txw = writer.begin_rw_txn()?;

    // TODO: DO NOT CREATE UUID IN HERE
    let uuid = Uuid::parse_str(&req.task_id).expect("uuid from str");
    if let Some(data) = self.tasks.get(&txw, uuid.as_bytes())? {
      if (data.deliveries - 1) < data.retry {
        let score = time_to_delay(backoff_time(data.deliveries as u64));

        let pending = self
          .queues
          .get(generate_key(&req.queue, QueueSuffix::PENDING));
        let delayed = self.delays.get(&data.queue);

        pending.remove(&mut txw, 1, uuid.as_bytes())?;
        delayed.add(&mut txw, score, uuid.as_bytes())?;
        txw.commit()?;
        return Ok(true);
      } else {
        txw.abort();
        drop(writer);
        return self.finish(req).await;
      }
    }
    Ok(false)
  }

  pub async fn schedule_delayed(&self, queue_name: &str) -> Result<()> {
    let queue = self
      .queues
      .get(generate_key(queue_name, QueueSuffix::WAITING));
    let delayed = self.delays.get(queue_name);
    let max = now_as_millis();
    let writer = self.env.write().await;
    let mut txw = writer.begin_rw_txn()?;

    for res in delayed.range_by_score(&txw, ..max)? {
      let id = res?;
      queue.push(&mut txw, id)?;
      delayed.remove(&mut txw, id)?;
    }

    txw.commit()?;
    queue.publish(QueueEvent::Push).await;
    Ok(())
  }

  pub async fn renqueue<Q, I>(&self, queue: Q, ids: I) -> Result<()>
  where
    Q: AsRef<str>,
    I: Send + Iterator<Item = Uuid>,
  {
    let pending = self
      .queues
      .get(generate_key(queue.as_ref(), QueueSuffix::PENDING));
    let waiting = self
      .queues
      .get(generate_key(queue.as_ref(), QueueSuffix::WAITING));
    let writer = self.env.write().await;
    let mut txw = writer.begin_rw_txn()?;

    for id in ids {
      let id_bytes = id.as_bytes();
      pending.remove(&mut txw, 1, id_bytes)?;
      waiting.push(&mut txw, id_bytes)?;
    }
    txw.commit()
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

fn generate_key(queue: &str, suffix: &str) -> String {
  format!("{}_{}", queue, suffix)
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
  const PENDING: &'static str = "pending";
  const DONE: &'static str = "done";
  const WAITING: &'static str = "waiting";
}