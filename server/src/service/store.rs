use super::stub::tasks::Task;
use redis::{
  aio::ConnectionLike,
  streams::{StreamId, StreamRangeReply, StreamReadOptions, StreamReadReply},
  AsyncCommands, Client, ConnectionAddr, ConnectionInfo, Script,
};
use tokio::{stream::StreamExt, sync::mpsc::unbounded_channel};

pub use redis::aio::{Connection as SingleConnection, MultiplexedConnection};
pub use redis::RedisError as StoreError;

const PENDING_SUFFIX: &str = "pending";
const DEFAULT_GROUP: &str = "default_group";

pub struct Connection<T: ConnectionLike> {
  id: usize,
  inner: T,
}

impl<T: ConnectionLike> Connection<T> {
  pub async fn kill<I: Iterator<Item = usize>>(conn: &mut Self, ids: I) {
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

pub async fn connection() -> Result<Connection<SingleConnection>, StoreError> {
  // TODO: Retrieve connection info from configuration
  let conn_info = ConnectionInfo {
    db: 0,
    addr: Box::new(ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6380)),
    username: None,
    passwd: None,
  };

  let mut inner = Client::open(conn_info)?.get_async_connection().await?;
  let id = redis::cmd("CLIENT")
    .arg("ID")
    .query_async(&mut inner)
    .await?;

  Ok(Connection { id, inner })
}

#[tonic::async_trait]
pub trait RedisDriver: RedisStorage {
  async fn create_task(&mut self, task: Task) -> Result<String, StoreError> {
    let key = generate_key(&task.queue);
    self
      .connection()
      .xadd(key, "*", &[("kind", &task.kind), ("data", &task.data)])
      .await
  }

  async fn create_queue(&mut self, key: &str) -> Result<(), StoreError> {
    self
      .connection()
      .xgroup_create_mkstream(key, DEFAULT_GROUP, 0)
      .await
  }

  async fn get_pending(&mut self, key: &str, consumer: &str) -> Result<Option<Task>, StoreError> {
    let mut reply: StreamRangeReply = self
      .script()
      .pending_task()
      .key(key)
      .arg(DEFAULT_GROUP)
      .arg(consumer)
      .invoke_async(self.connection())
      .await?;
    if let Some(t) = reply.ids.pop() {
      return Ok(Some(t.into()));
    }
    Ok(None)
  }

  async fn collect(&mut self, key: &str, consumer: &str) -> Result<Task, StoreError> {
    // TODO Do not clone
    let opts = StreamReadOptions::default()
      .block(0)
      .count(1)
      .group(DEFAULT_GROUP, consumer);

    let mut reply: StreamReadReply = self
      .connection()
      .xread_options(&[key], &[">"], opts)
      .await?;
    // This never panicks as we block until getting a reply, and it
    // always has one single value
    let t = reply.keys.pop().unwrap().ids.pop().unwrap();
    Ok(t.into())
  }

  async fn ack(&mut self, key: &str, task_id: &str) -> Result<usize, StoreError> {
    self.connection().xack(key, DEFAULT_GROUP, &[task_id]).await
  }
}

pub trait RedisStorage: Sync {
  type Connection: ConnectionLike + Send;

  fn connection(&mut self) -> &mut Self::Connection;
  fn script(&self) -> &'static ScriptStore;
  fn conn_id(&self) -> usize;
}

pub struct Store {
  conn: Connection<SingleConnection>,
  script: &'static ScriptStore,
}

impl Store {
  pub async fn connect() -> Result<Self, StoreError> {
    connection().await.map(|conn| {
      let script = ScriptStore::new();
      Store {
        conn,
        script
      }
    })
  }

  pub async fn connect_batch(size: usize) -> Result<Vec<Store>, StoreError> {
    let (tx, rx) = unbounded_channel();
    // Create connection for each worker concurrently
    for _ in 0..size {
      // let consumer = format!("{}-{}", self.consumer, i);
      let txc = tx.clone();
      tokio::spawn(async move {
        let conn_res = connection().await.map(|conn| Store {
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
}

impl RedisDriver for Store {}

impl RedisStorage for Store {
  type Connection = SingleConnection;
  fn connection(&mut self) -> &mut Self::Connection {
    &mut self.conn.inner
  }

  fn script(&self) -> &'static ScriptStore {
    self.script
  }

  fn conn_id(&self) -> usize {
    self.conn.id
  }
}

impl From<StreamId> for Task {
  fn from(s: StreamId) -> Self {
    let kind = s.get("kind").unwrap_or_default();
    let data = s.get("data").unwrap_or_default();
    let id = s.id;

    Task {
      id,
      kind,
      queue: String::new(),
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

  fn pending_task(&'static self) -> &'static Script {
    self.scripts.as_ref().unwrap().get(PENDING_NAME).unwrap()
  }
}
