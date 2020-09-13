use super::stub::tasks::Task;
use redis::{
  aio::MultiplexedConnection,
  streams::{
    StreamId, StreamPendingCountReply, StreamRangeReply, StreamReadOptions, StreamReadReply,
  },
  AsyncCommands,
};

pub use redis::ConnectionAddr;
pub use redis::ConnectionInfo;
pub use redis::ErrorKind as DbErrorKind;
pub use redis::IntoConnectionInfo;
pub use redis::RedisError as DbError;

const PENDING_SUFFIX: &str = "pending";
const DEFAULT_GROUP: &str = "default_group";

#[tonic::async_trait]
pub trait TasksStorage {
  type CreateResult: Send + Sized;
  type Error: std::error::Error;
  async fn create(&self, task: Task) -> Result<Self::CreateResult, Self::Error>;
  async fn create_queue(&self, queue: &str) -> Result<(), Self::Error>;
  async fn pending(&self, queue: &str, consumer: &str) -> Result<Option<Task>, Self::Error>;
  async fn wait_for_incoming(&self, queue: &str, consumer: &str) -> Result<Task, Self::Error>;
}

#[derive(Clone)]
pub struct Connection {
  pub id: usize,
  pub inner: MultiplexedConnection,
}

impl Connection {
  pub async fn start<T: IntoConnectionInfo>(conn_info: T) -> Result<Self, redis::RedisError> {
    let mut inner = redis::Client::open(conn_info)?
      .get_multiplexed_async_connection()
      .await?;
    let id = redis::cmd("CLIENT")
      .arg("ID")
      .query_async(&mut inner)
      .await?;

    Ok(Self { id, inner })
  }
}

#[derive(Clone)]
pub struct TasksRepository {
  conn: Connection,
}

impl TasksRepository {
  pub async fn connect<T: IntoConnectionInfo>(params: T) -> Result<Self, redis::RedisError> {
    let conn = Connection::start(params).await?;
    Ok(TasksRepository { conn })
  }

  pub fn conn_id(&self) -> usize {
    self.conn.id
  }
  
  fn connection(&self) -> MultiplexedConnection {
    // Cloning allows to send requests concurrently on the same
    // (tcp/unix socket) connection
    self.conn.inner.clone()
  }
}

#[tonic::async_trait]
impl TasksStorage for TasksRepository {
  type CreateResult = String;
  type Error = DbError;
  async fn create(&self, task: Task) -> Result<Self::CreateResult, Self::Error> {
    let key = format!("{}_{}", task.queue, PENDING_SUFFIX);
    self
      .connection()
      .xadd(&key, "*", &[("kind", &task.kind), ("data", &task.data)])
      .await
  }

  async fn create_queue(&self, queue: &str) -> Result<(), Self::Error> {
    let key = format!("{}_{}", queue, PENDING_SUFFIX);
    self
      .connection()
      .xgroup_create_mkstream(key, DEFAULT_GROUP, 0)
      .await
  }

  async fn pending(&self, queue: &str, consumer: &str) -> Result<Option<Task>, Self::Error> {
    let key = &format!("{}_{}", queue, PENDING_SUFFIX);
    let mut res: StreamPendingCountReply = self
      .connection()
      .xpending_consumer_count(key, DEFAULT_GROUP, "-", "+", 1, consumer)
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

  async fn wait_for_incoming(&self, queue: &str, consumer: &str) -> Result<Task, Self::Error> {
    let key = &format!("{}_{}", queue, PENDING_SUFFIX);

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
