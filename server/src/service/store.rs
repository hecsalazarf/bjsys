use super::stub::tasks::Task;
use redis::{
  aio::{Connection as SingleConnection, ConnectionLike, MultiplexedConnection},
  streams::{StreamId, StreamReadOptions, StreamReadReply},
  AsyncCommands, Client, ConnectionAddr, ConnectionInfo,
};
use tokio::{stream::StreamExt, sync::mpsc};

pub use redis::RedisError as StoreError;

struct KeySuffix;

impl KeySuffix {
  const PENDING: &'static str = "pending";
  const DELAYED: &'static str = "delayed";
}

struct StreamDefs;

impl StreamDefs {
  const DEFAULT_GROUP: &'static str = "default_group";
  const DEFAULT_CONSUMER: &'static str = "default_consumer";
  const NEW_ID: &'static str = ">";
  const AUTO_ID: &'static str = "*";
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

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

#[tonic::async_trait]
pub trait RedisStorage: Sized + Sync {
  type Connection: InnerConnection + Send;

  fn connection(&mut self) -> &mut Self::Connection;
  fn script(&self) -> &'static ScriptStore;

  async fn create_task(&mut self, task: Task) -> Result<String, StoreError> {
    let key = generate_key(&task.queue, KeySuffix::PENDING);
    self
      .connection()
      .xadd(
        key,
        StreamDefs::AUTO_ID,
        &[
          ("kind", &task.kind),
          ("data", &task.data),
          ("queue", &task.queue),
        ],
      )
      .await
  }

  async fn create_delayed_task(&mut self, task: Task, delay: u64) -> Result<String, StoreError> {
    let key = generate_key(&task.queue, KeySuffix::DELAYED);
    let mut member: String = self
      .connection()
      .xadd(
        &key,
        StreamDefs::AUTO_ID,
        &[
          ("kind", &task.kind),
          ("data", &task.data),
          ("queue", &task.queue),
        ],
      )
      .await?;

    let id = member.clone();
    member.push_str(":");
    member.push_str(&task.queue);
    let delay = Duration::from_millis(delay);

    let now = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap();

    let score = if let Some(d) = now.checked_add(delay) {
      d.as_millis() as u64
    } else {
      u64::MAX
    };

    self.connection().zadd("delayed", &member, score).await?;

    Ok(id)
  }

  async fn create_queue(&mut self, key: &str) -> Result<(), StoreError> {
    self
      .connection()
      .xgroup_create_mkstream(key, StreamDefs::DEFAULT_GROUP, 0)
      .await
  }

  async fn read_pending(&mut self, key: &str, count: usize) -> Result<VecDeque<Task>, StoreError> {
    let opts = StreamReadOptions::default()
      .count(count)
      .group(StreamDefs::DEFAULT_GROUP, StreamDefs::DEFAULT_CONSUMER);

    self.read_stream(key, "0", opts).await
  }

  async fn read_new(&mut self, key: &str, count: usize) -> Result<VecDeque<Task>, StoreError> {
    let opts = StreamReadOptions::default()
      .block(0)
      .count(count)
      .group(StreamDefs::DEFAULT_GROUP, StreamDefs::DEFAULT_CONSUMER);

    self.read_stream(key, StreamDefs::NEW_ID, opts).await
  }

  async fn read_stream(
    &mut self,
    key: &str,
    id: &str,
    opts: StreamReadOptions,
  ) -> Result<VecDeque<Task>, StoreError> {
    let mut reply: StreamReadReply = self.connection().xread_options(&[key], &[id], opts).await?;

    // Unwrap never panics as the key exists, otherwise Err is returned on redis xread
    let ids = reply.keys.pop().unwrap().ids;
    let tasks = ids.into_iter().map(|r| r.into()).collect();
    Ok(tasks)
  }

  async fn collect(&mut self, key: &str) -> Result<Task, StoreError> {
    let opts = StreamReadOptions::default()
      .block(0)
      .count(1)
      .group(StreamDefs::DEFAULT_GROUP, StreamDefs::DEFAULT_CONSUMER);

    let mut reply: StreamReadReply = self
      .connection()
      .xread_options(&[key], &[StreamDefs::NEW_ID], opts)
      .await?;
    // This never panicks as we block until getting a reply, and it
    // always has one single value
    let t = reply.keys.pop().unwrap().ids.pop().unwrap();
    Ok(t.into())
  }

  async fn ack(&mut self, task_id: &str, queue: &str) -> Result<usize, StoreError> {
    let key = generate_key(&queue, KeySuffix::PENDING);
    self
      .connection()
      .xack(key, StreamDefs::DEFAULT_GROUP, &[task_id])
      .await
  }

  async fn schedule_delayed(&mut self, limit: u16) -> Result<Vec<String>, StoreError> {
    let max = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;

    self
      .script()
      .prepare_for(ScriptStore::SCHEDULE_DELAY)
      .key("delayed")
      .arg(max)
      .arg(limit)
      .invoke_async(self.connection())
      .await
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

impl From<StreamId> for Task {
  fn from(s: StreamId) -> Self {
    let kind = s.get("kind").unwrap_or_default();
    let data = s.get("data").unwrap_or_default();
    let queue = s.get("queue").unwrap_or_default();
    let id = s.id;

    Task {
      id,
      kind,
      queue,
      data,
    }
  }
}

fn generate_key(queue: &str, suffix: &str) -> String {
  format!("{}_{}", queue, suffix)
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
    local delayed = queue..'_delayed'
    local pending = queue..'_pending'
    
    -- Read task data
    local data = redis.call('XRANGE', delayed, id, id)[1][2]
    -- Create task with given data in stream
    table.insert(res, redis.call('XADD', pending, '*', unpack(data)))
    -- Remove values from delayed queues
    redis.call('XDEL', delayed, id)
    redis.call('ZREM', KEYS[1], value)
  end

  return res
"
];
