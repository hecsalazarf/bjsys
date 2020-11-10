use super::stub::tasks::{AcknowledgeRequest, CreateRequest, FetchResponse};
use redis::{
  aio::{Connection as SingleConnection, ConnectionLike, MultiplexedConnection},
  AsyncCommands, Client, ConnectionAddr, ConnectionInfo,
};
use std::time::{Duration, SystemTime};
use tokio::{stream::StreamExt, sync::mpsc};
use utils::id::{generate_id, IdGenerator};

pub use redis::RedisError as StoreError;

struct QueueSuffix;

impl QueueSuffix {
  const PENDING: &'static str = "pending";
  const DELAYED: &'static str = "delayed";
  const DONE: &'static str = "done";
  const WAITING: &'static str = "waiting";
}

struct TaskHash;

impl TaskHash {
  const DATA: &'static str = "data";
  const QUEUE: &'static str = "queue";
  const RETRY: &'static str = "retry";
  const DELIVERIES: &'static str = "deliveries";
  const STATUS: &'static str = "status";
  const MESSAGE: &'static str = "message";
  const PROCESSED_ON: &'static str = "processed_on";
  const FINISHED_ON: &'static str = "finished_on";
}

#[tonic::async_trait]
pub trait InnerConnection: Sized + ConnectionLike {
  async fn create(conn_info: ConnectionInfo) -> Result<Self, StoreError>;
}

#[tonic::async_trait]
impl InnerConnection for SingleConnection {
  async fn create(conn_info: ConnectionInfo) -> Result<Self, StoreError> {
    Client::open(conn_info)?.get_async_connection().await
  }
}

#[tonic::async_trait]
impl InnerConnection for MultiplexedConnection {
  async fn create(conn_info: ConnectionInfo) -> Result<Self, StoreError> {
    Client::open(conn_info)?
      .get_multiplexed_async_connection()
      .await
  }
}

struct Connection<C: InnerConnection> {
  id: usize,
  inner: C,
}

impl<C: InnerConnection> Connection<C> {
  async fn start() -> Result<Connection<C>, StoreError> {
    // TODO: Retrieve connection info from configuration
    let conn_info = ConnectionInfo {
      db: 0,
      addr: Box::new(ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6380)),
      username: None,
      passwd: None,
    };

    let mut inner = C::create(conn_info).await?;
    let id = redis::cmd("CLIENT")
      .arg("ID")
      .query_async(&mut inner)
      .await?;

    Ok(Connection { id, inner })
  }

  fn id(&self) -> usize {
    self.id
  }

  async fn kill(conn: &mut Self, ids: Vec<usize>) {
    let mut pipe = &mut redis::pipe();
    for id in ids {
      pipe = pipe.cmd("CLIENT").arg("KILL").arg("ID").arg(id);
    }
    let _: Vec<u8> = pipe
      .query_async(&mut conn.inner)
      .await
      .expect("redis_cannot_kill");
  }
}

impl Clone for Connection<MultiplexedConnection> {
  fn clone(&self) -> Self {
    Self {
      id: self.id,
      inner: self.inner.clone(),
    }
  }
}

#[tonic::async_trait]
pub trait RedisStorage: Sized + Sync {
  type Connection: InnerConnection + Send;

  fn connection(&mut self) -> &mut Self::Connection;
  fn script(&self) -> &'static ScriptStore;

  async fn create_task(&mut self, req: &CreateRequest) -> Result<String, StoreError> {
    let waiting = generate_key(&req.queue, QueueSuffix::WAITING);
    let mut buffer = IdGenerator::encode_buffer();
    let id = generate_id().encode(&mut buffer);
    let mut pipe = redis::pipe();
    pipe
      .atomic()
      .hset_multiple(
        id,
        &[(TaskHash::DATA, &req.data), (TaskHash::QUEUE, &req.queue)],
      )
      .ignore()
      .hset(id, TaskHash::RETRY, req.retry)
      .ignore()
      .lpush(&waiting, id)
      .ignore()
      .query_async(self.connection())
      .await?;
    Ok(String::from(id))
  }

  async fn create_delayed_task(
    &mut self,
    req: &CreateRequest,
    delay: u64,
  ) -> Result<String, StoreError> {
    let mut pipe = redis::pipe();

    let mut buffer = IdGenerator::encode_buffer();
    let id = generate_id().encode(&mut buffer);
    let member = member_from_id(id, &req.queue);
    let score = time_to_delay(delay);

    pipe
      .atomic()
      .hset_multiple(
        id,
        &[(TaskHash::DATA, &req.data), (TaskHash::QUEUE, &req.queue)],
      )
      .ignore()
      .hset(id, TaskHash::RETRY, req.retry)
      .ignore()
      .zadd(QueueSuffix::DELAYED, &member, score)
      .ignore()
      .query_async(self.connection())
      .await?;
    Ok(String::from(id))
  }

  async fn read_pending(&mut self, queue: &str) -> Result<Vec<String>, StoreError> {
    let pending = generate_key(queue, QueueSuffix::PENDING);
    self.connection().lrange(pending, 0, -1).await
  }

  async fn read_new(&mut self, queue: &str) -> Result<FetchResponse, StoreError> {
    let waiting = generate_key(queue, QueueSuffix::WAITING);
    let pending = generate_key(queue, QueueSuffix::PENDING);

    let mut pipe = redis::pipe();
    let id: String = self.connection().brpoplpush(&waiting, &pending, 0).await?;

    let mut values: Vec<HashMap<String, String>> = pipe
      .atomic()
      .hgetall(&id)
      .hincr(&id, TaskHash::DELIVERIES, 1)
      .ignore()
      // Set timestamp
      .hset(&id, TaskHash::PROCESSED_ON, now_as_millis())
      .ignore()
      .query_async(self.connection())
      .await?;

    // Never fails, as a successful response always contains data
    let values = values.pop().unwrap();

    Ok(FetchResponse::from_map(id, values))
  }

  async fn finish(&mut self, req: &AcknowledgeRequest) -> Result<usize, StoreError> {
    let mut pipe = redis::pipe();

    let pending = generate_key(&req.queue, QueueSuffix::PENDING);
    let done = generate_key(&req.queue, QueueSuffix::DONE);
    pipe
      .atomic()
      // Remove from tail to head as older tasks are at the end
      .lrem(&pending, -1, &req.task_id)
      .ignore()
      // Move it to finished queue
      .lpush(&done, &req.task_id)
      .ignore()
      // Set status and message
      // Repeated hset, otherwise we have to parse any of the values. hset_multiple
      // only accepts values of the same type.
      .hset(&req.task_id, TaskHash::STATUS, req.status)
      .ignore()
      .hset(&req.task_id, TaskHash::MESSAGE, &req.message)
      .ignore()
      // Set timestamp
      .hset(&req.task_id, TaskHash::FINISHED_ON, now_as_millis())
      .ignore()
      .query_async(self.connection())
      .await?;
    Ok(1)
  }

  async fn fail(&mut self, req: &AcknowledgeRequest) -> Result<usize, StoreError> {
    let mut pipe = redis::pipe();

    let (deliveries, retry): (u64, u64) = pipe
      .atomic()
      .hget(&req.task_id, TaskHash::DELIVERIES)
      .hget(&req.task_id, TaskHash::RETRY)
      .query_async(self.connection())
      .await?;

    // Substract one not to consider the first delivery as an attempt
    if (deliveries - 1) < retry {
      let pending = generate_key(&req.queue, QueueSuffix::PENDING);
      let member = member_from_id(&req.task_id, &req.queue);
      let score = time_to_delay(backoff_time(deliveries));

      pipe.clear(); // Clear pipeline to reuse it

      pipe
        .atomic()
        .lrem(&pending, -1, &req.task_id)
        .ignore()
        .zadd(QueueSuffix::DELAYED, &member, score)
        .query_async(self.connection())
        .await?;
    } else {
      // Moved task to finished queue
      self.finish(req).await?;
    }
    Ok(1)
  }

  async fn schedule_delayed(&mut self, limit: u16) -> Result<Vec<String>, StoreError> {
    let max = now_as_millis();

    self
      .script()
      .prepare_for(ScriptStore::SCHEDULE_DELAY)
      .key("delayed")
      .arg(max)
      .arg(limit)
      .invoke_async(self.connection())
      .await
  }

  async fn renqueue<I>(&mut self, queue: &str, ids: I) -> Result<(), StoreError>
  where
    I: Send + Iterator<Item = String>,
  {
    let pending = generate_key(queue, QueueSuffix::PENDING);
    let waiting = generate_key(queue, QueueSuffix::WAITING);
    let mut pipe = redis::pipe();
    let pipe = pipe.atomic();

    for id in ids {
      pipe
        .lrem(&pending, -1, &id)
        .ignore()
        .lpush(&waiting, &id)
        .ignore();
    }
    pipe.query_async(self.connection()).await
  }
}

pub struct Store {
  conn: Connection<SingleConnection>,
  script: &'static ScriptStore,
}

impl Store {
  pub async fn connect() -> Result<Self, StoreError> {
    Connection::start().await.map(|conn| {
      let script = ScriptStore::new();
      Self { conn, script }
    })
  }

  pub async fn _connect_batch(size: usize) -> Result<Vec<Store>, StoreError> {
    let (tx, rx) = mpsc::unbounded_channel();
    // Create each connection concurrently
    for _ in 0..size {
      let txc = tx.clone();
      tokio::spawn(async move {
        let conn_res = Connection::start().await.map(|conn| Store {
          conn,
          script: ScriptStore::new(),
        });
        // We don't care if it fails
        txc.send(conn_res).unwrap_or(());
      });
    }
    // Drop the first sender, so that stream does not block indefinitely
    drop(tx);

    // Wait for all connections, fail at first error
    rx.collect().await
  }

  pub fn id(&self) -> usize {
    self.conn.id()
  }
}

impl RedisStorage for Store {
  type Connection = SingleConnection;
  fn connection(&mut self) -> &mut Self::Connection {
    &mut self.conn.inner
  }

  fn script(&self) -> &'static ScriptStore {
    self.script
  }
}

#[derive(Clone)]
pub struct MultiplexedStore {
  conn: Connection<MultiplexedConnection>,
  script: &'static ScriptStore,
}

impl MultiplexedStore {
  pub async fn connect() -> Result<Self, StoreError> {
    Connection::start().await.map(|conn| {
      let script = ScriptStore::new();
      Self { conn, script }
    })
  }

  pub async fn stop_by_id<I>(&mut self, ids: I)
  where
    I: Iterator<Item = usize>,
  {
    Connection::kill(&mut self.conn, ids.collect()).await;
  }
}

impl RedisStorage for MultiplexedStore {
  type Connection = MultiplexedConnection;
  fn connection(&mut self) -> &mut Self::Connection {
    &mut self.conn.inner
  }

  fn script(&self) -> &'static ScriptStore {
    self.script
  }
}

impl FetchResponse {
  fn from_map(id: String, mut values: HashMap<String, String>) -> Self {
    let data = values.remove("data").unwrap_or_default();
    let queue = values.remove("queue").unwrap_or_default();

    FetchResponse { id, queue, data }
  }
}

fn generate_key(queue: &str, suffix: &str) -> String {
  format!("{}_{}", queue, suffix)
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
  let rnd = rng.gen_range(1, 31);

  // Multiplied by 1000 to obtain milliseconds
  (15 + count.pow(POWER) + (rnd * (count + 1))) * 1000
}

/// Calculate the time in milliseconds at which a task should be processed.
/// We add the current time to the given delay.
fn time_to_delay(delay: u64) -> u64 {
  let delay = Duration::from_millis(delay);
  let now = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap();

  if let Some(d) = now.checked_add(delay) {
    d.as_millis() as u64
  } else {
    u64::MAX
  }
}

/// Create the member string for the delayed sorted set.
fn member_from_id(id: &str, queue: &str) -> String {
  let mut member = String::from(id);
  member.push_str(":");
  member.push_str(queue);

  member
}

/// Return the current time in milliseconds as u64
fn now_as_millis() -> u64 {
  SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}

use redis::{Script, ScriptInvocation};
use std::collections::HashMap;
use std::sync::Once;

pub struct ScriptStore {
  scripts: Option<HashMap<&'static str, Script>>,
}

impl ScriptStore {
  pub const SCHEDULE_DELAY: &'static str = "SCHEDULE_DELAY";

  pub fn new() -> &'static Self {
    static START: Once = Once::new();
    static mut SCRIPT: ScriptStore = ScriptStore { scripts: None };

    // Safe because we only write once in a synchronized fashion
    unsafe {
      START.call_once(|| {
        tracing::debug!("Loading store scripts");
        let mut scripts = HashMap::new();
        let mut iter = SCRIPTS.iter();
        while let Some(key) = iter.next() {
          let code = *iter.next().unwrap();
          scripts.insert(*key, Script::new(code));
        }

        SCRIPT.scripts = Some(scripts);
      });

      &SCRIPT
    }
  }

  pub fn prepare_for(&'static self, script: &str) -> ScriptInvocation {
    self
      .scripts
      .as_ref()
      .unwrap()
      .get(script)
      .unwrap()
      .prepare_invoke()
  }
}

const SCRIPTS: [&str; 2] = [
  // ----------------------------
  //       SCHEDULE_DELAY
  // ----------------------------
  // -- KEYS[1]: Sorted set key
  // -- ARGV[1]: Max score
  // -- ARGV[2]: Number of members to schedule
  ScriptStore::SCHEDULE_DELAY,
  r"
  local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[2])

  if table.maxn(tasks) < 1
  then
    -- If no delayed tasks are planned, do nothing
    return nil
  end

  local res = {}
  for i,value in ipairs(tasks) do
    -- For upcoming tasks, we find its queue and id by splitting the member
    -- of the sorted set
    local ix = string.find(value, ':')
    local id = string.sub(value,0, ix - 1)
    local queue = string.sub(value, ix + 1)

    -- Append suffixes to the queue
    local waiting = queue..'_waiting'
    
    -- Push task to the waiting queue
    redis.call('LPUSH', waiting, id)
    table.insert(res, id)
  end

  -- Remove values from delayed queue
    redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])

  return res
",
];
