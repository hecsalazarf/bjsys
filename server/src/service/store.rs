use super::stub::tasks::{Consumer, Task};
use redis::{
  aio::MultiplexedConnection,
  streams::{
    StreamId, StreamPendingCountReply, StreamRangeReply, StreamReadOptions, StreamReadReply,
  },
  AsyncCommands, Client, ConnectionAddr, ConnectionInfo,
};
use std::sync::Arc;

pub use redis::RedisError as StoreError;

const PENDING_SUFFIX: &str = "pending";
const DEFAULT_GROUP: &str = "default_group";

#[derive(Clone)]
pub struct Connection {
  id: usize,
  inner: MultiplexedConnection,
}

impl Connection {
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

pub async fn connection() -> Result<Connection, StoreError> {
  // TODO: Retrieve connection info from configuration
  let conn_info = ConnectionInfo {
    db: 0,
    addr: Box::new(ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6380)),
    username: None,
    passwd: None,
  };

  let mut inner = Client::open(conn_info)?
    .get_multiplexed_async_connection()
    .await?;
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
  async fn create_task(&self, task: Task) -> Result<Self::CreateResult, Self::Error>;
  async fn create_queue(&self) -> Result<(), Self::Error>;
  async fn get_pending(&self) -> Result<Option<Task>, Self::Error>;
  async fn collect(&self) -> Result<Task, Self::Error>;
  async fn ack(&self, task_id: &str, queue: &str) -> Result<usize, Self::Error>;
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

  pub async fn connect(self) -> Result<Vec<Store>, StoreError> {
    let queue = Arc::new(self.queue);
    let key = Arc::new(self.key);
    let mut stores = Vec::with_capacity(self.workers);

    for i in 0..stores.capacity() {
      let conn = connection().await?;
      let consumer = format!("{}-{}", self.consumer, i);
      let store = Store {
        conn,
        queue: queue.clone(),
        consumer: Arc::new(consumer),
        key: key.clone(),
      };
      stores.push(store);
    }

    Ok(stores)
  }
}

#[derive(Clone)]
pub struct Store {
  conn: Connection,
  queue: Arc<String>,
  consumer: Arc<String>,
  key: Arc<String>,
}

impl Store {
  pub fn new() -> Builder {
    Builder::default()
  }

  pub fn conn_id(&self) -> usize {
    self.conn.id
  }

  pub fn consumer(&self) -> &str {
    self.consumer.as_ref()
  }

  pub fn queue(&self) -> &str {
    self.queue.as_ref()
  }

  fn connection(&self) -> MultiplexedConnection {
    // Cloning allows to send requests concurrently on the same
    // (tcp/unix socket) connection
    self.conn.inner.clone()
  }
}

#[tonic::async_trait]
impl Storage for Store {
  type CreateResult = String;
  type Error = StoreError;
  async fn create_task(&self, task: Task) -> Result<Self::CreateResult, Self::Error> {
    let key = generate_key(&task.queue);
    self
      .connection()
      .xadd(key, "*", &[("kind", &task.kind), ("data", &task.data)])
      .await
  }

  async fn create_queue(&self) -> Result<(), Self::Error> {
    self
      .connection()
      .xgroup_create_mkstream(self.key.as_ref(), DEFAULT_GROUP, 0)
      .await
  }

  async fn get_pending(&self) -> Result<Option<Task>, Self::Error> {
    let key = self.key.as_ref();
    let mut res: StreamPendingCountReply = self
      .connection()
      .xpending_consumer_count(key, DEFAULT_GROUP, "-", "+", 1, self.consumer.as_ref())
      .await?;
    // TODO: Create lua script to avoid double request
    if let Some(p) = res.ids.pop() {
      let mut reply: StreamRangeReply = self.connection().xrange(key, &p.id, &p.id).await?;
      if let Some(t) = reply.ids.pop() {
        return Ok(Some(t.into()));
      }
    }

    Ok(None)
  }

  async fn collect(&self) -> Result<Task, Self::Error> {
    let opts = StreamReadOptions::default()
      .block(0)
      .count(1)
      .group(DEFAULT_GROUP, self.consumer.as_ref());

    let mut reply: StreamReadReply = self
      .connection()
      .xread_options(&[self.key.as_ref()], &[">"], opts)
      .await?;
    // This never panicks as we block until getting a reply, and it
    // always has one single value
    let t = reply.keys.pop().unwrap().ids.pop().unwrap();
    Ok(t.into())
  }

  async fn ack(&self, task_id: &str, queue: &str) -> Result<usize, Self::Error> {
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
