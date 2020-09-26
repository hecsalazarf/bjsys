use super::stub::tasks::{Consumer, Task};
use redis::{
  aio::ConnectionLike,
  streams::{StreamId, StreamRangeReply, StreamReadOptions, StreamReadReply},
  AsyncCommands, Client, ConnectionAddr, ConnectionInfo, Script,
};
use std::sync::Arc;
use tokio::{stream::StreamExt, sync::mpsc::unbounded_channel};

pub use redis::RedisError as StoreError;
pub use redis::aio::{Connection as Single, MultiplexedConnection as Multiplexed};

const PENDING_SUFFIX: &str = "pending";
const DEFAULT_GROUP: &str = "default_group";

/* enum ConnectionKind {
  Single,
  Multiplexed
} */

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

pub async fn connection() -> Result<Connection<Single>, StoreError> {
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
pub trait Storage {
  type CreateResult: Send + Sized;
  type Error: std::error::Error;
  async fn create_task(&mut self, task: Task) -> Result<Self::CreateResult, Self::Error>;
  async fn create_queue(&mut self) -> Result<(), Self::Error>;
  async fn get_pending(&mut self) -> Result<Option<Task>, Self::Error>;
  async fn collect(&mut self) -> Result<Task, Self::Error>;
  async fn ack(&mut self, task_id: &str, queue: &str) -> Result<usize, Self::Error>;
}

pub struct Builder {
  queue: String,
  consumer: String,
  key: String,
  workers: usize,
}

impl Default for Builder {
  fn default() -> Self {
    Self {
      queue: String::default(),
      consumer: String::default(),
      key: String::default(),
      workers: 1,
    }
  }
}

impl Builder {
  pub fn for_consumer(mut self, consumer: Consumer) -> Self {
    self.key = generate_key(&consumer.queue);
    self.queue = consumer.queue;
    self.consumer = consumer.hostname;
    self.workers = consumer.workers as usize;
    self
  }

  pub async fn connect(self) -> Result<Vec<Store<Single>>, StoreError> {
    let queue = Arc::new(self.queue);
    let key = Arc::new(self.key);

    let (tx, rx) = unbounded_channel();
    // Create connection for each worker concurrently
    for i in 0..self.workers {
      let consumer = format!("{}-{}", self.consumer, i);
      let txc = tx.clone();
      tokio::spawn(async move {
        let conn = connection().await.map(|c| (c, consumer));
        // We don't care if it fails
        txc.send(conn).unwrap_or(());
      });
    }
    // Drop the first sender, so that stream does not block indefinitely
    drop(tx);

    // Map Result to append connection and consumer to each Store
    let map = rx.map(|r| {
      r.map(|(conn, consumer)| Store {
        conn,
        script: ScriptStore::new(),
        queue: queue.clone(),
        consumer: Arc::new(consumer),
        key: key.clone(),
      })
    });

    // Wait for all connections, fail at first error
    map.collect().await
  }
}

pub fn build () -> Builder {
  Builder::default()
}

pub struct Store<T: ConnectionLike> {
  conn: Connection<T>,
  queue: Arc<String>,
  consumer: Arc<String>,
  key: Arc<String>,
  script: &'static ScriptStore,
}

impl<T: ConnectionLike> Store<T> {

  pub fn conn_id(&self) -> usize {
    self.conn.id
  }

  pub fn queue(&self) -> &str {
    self.queue.as_ref()
  }

  fn connection(&mut self) -> &mut impl ConnectionLike {
    &mut self.conn.inner
  }
}

#[tonic::async_trait]
impl<T: ConnectionLike + Send> Storage for Store<T> {
  type CreateResult = String;
  type Error = StoreError;
  async fn create_task(&mut self, task: Task) -> Result<Self::CreateResult, Self::Error> {
    let key = generate_key(&task.queue);
    self
      .connection()
      .xadd(key, "*", &[("kind", &task.kind), ("data", &task.data)])
      .await
  }

  async fn create_queue(&mut self) -> Result<(), Self::Error> {
    // TODO Do not clone
    let key = self.key.clone();
    self
      .connection()
      .xgroup_create_mkstream(key.as_ref(), DEFAULT_GROUP, 0)
      .await
  }

  async fn get_pending(&mut self) -> Result<Option<Task>, Self::Error> {
    let key = self.key.as_ref();

    let mut reply: StreamRangeReply = self
      .script
      .pending_task()
      .key(key)
      .arg(DEFAULT_GROUP)
      .arg(self.consumer.as_ref())
      .invoke_async(self.connection())
      .await?;
    if let Some(t) = reply.ids.pop() {
      return Ok(Some(t.into()));
    }
    Ok(None)
  }

  async fn collect(&mut self) -> Result<Task, Self::Error> {
    // TODO Do not clone
    let key = self.key.clone();

    let opts = StreamReadOptions::default()
      .block(0)
      .count(1)
      .group(DEFAULT_GROUP, self.consumer.as_ref());

    let mut reply: StreamReadReply = self
      .connection()
      .xread_options(&[key.as_ref()], &[">"], opts)
      .await?;
    // This never panicks as we block until getting a reply, and it
    // always has one single value
    let t = reply.keys.pop().unwrap().ids.pop().unwrap();
    Ok(t.into())
  }

  async fn ack(&mut self, task_id: &str, queue: &str) -> Result<usize, Self::Error> {
    let key = generate_key(queue);
    self.connection().xack(key, DEFAULT_GROUP, &[task_id]).await
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

struct ScriptStore {
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
