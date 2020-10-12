use super::stub::tasks::Task;
use redis::{
  aio::{Connection as SingleConnection, ConnectionLike, MultiplexedConnection},
  streams::{StreamId, StreamReadOptions, StreamReadReply},
  AsyncCommands, Client, ConnectionAddr, ConnectionInfo, Script,
};
use tokio::{stream::StreamExt, sync::mpsc};

pub use redis::RedisError as StoreError;

const PENDING_SUFFIX: &str = "pending";
const DEFAULT_GROUP: &str = "default_group";
const DEFAULT_CONSUMER: &str = "default_consumer";

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

#[tonic::async_trait]
pub trait RedisStorage: Sized + Sync {
  type Connection: InnerConnection + Send;

  fn connection(&mut self) -> &mut Self::Connection;
  fn script(&self) -> &'static ScriptStore;

  async fn create_task(&mut self, task: Task) -> Result<String, StoreError> {
    let key = generate_key(&task.queue);
    self
      .connection()
      .xadd(
        key,
        "*",
        &[
          ("kind", &task.kind),
          ("data", &task.data),
          ("queue", &task.queue),
        ],
      )
      .await
  }

  async fn create_queue(&mut self, key: &str) -> Result<(), StoreError> {
    self
      .connection()
      .xgroup_create_mkstream(key, DEFAULT_GROUP, 0)
      .await
  }

  async fn read_pending(&mut self, key: &str, count: usize) -> Result<VecDeque<Task>, StoreError> {
    let opts = StreamReadOptions::default()
      .count(count)
      .group(DEFAULT_GROUP, DEFAULT_CONSUMER);

    self.read_stream(key, "0", opts).await
  }

  async fn read_new(&mut self, key: &str, count: usize) -> Result<VecDeque<Task>, StoreError> {
    let opts = StreamReadOptions::default()
      .block(0)
      .count(count)
      .group(DEFAULT_GROUP, DEFAULT_CONSUMER);

    self.read_stream(key, ">", opts).await
  }

  async fn read_stream(&mut self, key: &str, id: &str, opts: StreamReadOptions) -> Result<VecDeque<Task>, StoreError> {

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
      .group(DEFAULT_GROUP, DEFAULT_CONSUMER);

    let mut reply: StreamReadReply = self
      .connection()
      .xread_options(&[key], &[">"], opts)
      .await?;
    // This never panicks as we block until getting a reply, and it
    // always has one single value
    let t = reply.keys.pop().unwrap().ids.pop().unwrap();
    Ok(t.into())
  }

  async fn ack(&mut self, task_id: &str, queue: &str) -> Result<usize, StoreError> {
    let key = generate_key(&queue);
    self.connection().xack(key, DEFAULT_GROUP, &[task_id]).await
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

fn generate_key(queue: &str) -> String {
  format!("{}_{}", queue, PENDING_SUFFIX)
}

use std::collections::HashMap;
use std::sync::Once;

const PENDING_SCRIPT: &str = r"
  local pending = redis.call('xpending', KEYS[1], ARGV[1], '-', '+', '1', ARGV[2])
  if table.maxn(pending) == 1 then
    local id = pending[1][1]
    return redis.call('xrange', KEYS[1], id, id)
  else
    return pending
  end
";

const PENDING_NAME: &str = "PENDING_SCRIPT";

pub struct ScriptStore {
  scripts: Option<HashMap<&'static str, Script>>,
}

impl ScriptStore {
  fn new() -> &'static Self {
    static START: Once = Once::new();
    static mut SCRIPT: ScriptStore = ScriptStore { scripts: None };

    // Safe because we only write once in a synchronized fashion
    unsafe {
      START.call_once(|| {
        tracing::debug!("Loading store scripts");
        let mut scripts = HashMap::new();
        scripts.insert(PENDING_NAME, Script::new(PENDING_SCRIPT));
        SCRIPT.scripts = Some(scripts);
      });

      &SCRIPT
    }
  }

  fn _pending_task(&'static self) -> &'static Script {
    self.scripts.as_ref().unwrap().get(PENDING_NAME).unwrap()
  }
}
