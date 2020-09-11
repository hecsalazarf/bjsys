use super::stub::tasks::Task;
use redis::{
  streams::{StreamPendingCountReply, StreamRangeReply, StreamReadOptions, StreamReadReply},
  AsyncCommands,
};

pub use redis::ErrorKind as DbErrorKind;
pub use redis::RedisError as DbError;
pub use redis::ConnectionInfo;
pub use redis::ConnectionAddr;
pub use redis::IntoConnectionInfo;

const PENDING_SUFFIX: &str = "pending";
const DEFAULT_GROUP: &str = "default_group";

#[tonic::async_trait]
pub trait TasksStorage {
  type CreateResult: Send + Sized;
  type ErrorResult: std::error::Error;
  type PendingResult: Send + Sized;
  type WaitIncoming: Send + Sized;
  async fn create(&self, task: Task) -> Result<Self::CreateResult, Self::ErrorResult>;
  async fn create_queue(&self, queue: &str) -> Result<Self::CreateResult, Self::ErrorResult>;
  async fn pending(
    &self,
    queue: &str,
    consumer: &str,
  ) -> Result<Self::PendingResult, Self::ErrorResult>;
  async fn wait_for_incoming(
    &self,
    queue: &str,
    consumer: &str,
  ) -> Result<Self::WaitIncoming, Self::ErrorResult>;
}

pub struct TasksRepository {
  conn: redis::aio::MultiplexedConnection,
}

impl TasksRepository {
  pub async fn connect<T: IntoConnectionInfo>(params: T) -> Result<Self, redis::RedisError> {
    let conn = redis::Client::open(params)?
      .get_multiplexed_async_connection()
      .await?;
    Ok(TasksRepository { conn })
  }
}

impl Clone for TasksRepository {
  fn clone(&self) -> Self {
    Self {
      conn: self.conn.clone(),
    }
  }
}

#[tonic::async_trait]
impl TasksStorage for TasksRepository {
  type CreateResult = String;
  type ErrorResult = DbError;
  type PendingResult = StreamRangeReply;
  type WaitIncoming = StreamReadReply;
  async fn create(&self, task: Task) -> Result<Self::CreateResult, Self::ErrorResult> {
    let key = format!("{}_{}", task.queue, PENDING_SUFFIX);
    self
      .conn
      // Cloning allows to send requests concurrently on the same
      // (tcp/unix socket) connection
      .clone()
      .xadd(&key, "*", &[("kind", &task.kind), ("data", &task.data)])
      .await
  }

  async fn create_queue(&self, queue: &str) -> Result<Self::CreateResult, Self::ErrorResult> {
    let key = format!("{}_{}", queue, PENDING_SUFFIX);
    self
      .conn
      .clone()
      .xgroup_create_mkstream(key, DEFAULT_GROUP, 0)
      .await
  }

  async fn pending(
    &self,
    queue: &str,
    consumer: &str,
  ) -> Result<Self::PendingResult, Self::ErrorResult> {
    let key = &format!("{}_{}", queue, PENDING_SUFFIX);
    let mut conn = self.conn.clone();
    let mut res: StreamPendingCountReply = conn
      .xpending_consumer_count(key, DEFAULT_GROUP, "-", "+", 1, consumer)
      .await?;
    // TODO: Create lua script to avoid double request
    if let Some(t) = res.ids.pop() {
      conn.xrange(key, &t.id, &t.id).await
    } else {
      Ok(StreamRangeReply::default())
    }
  }

  async fn wait_for_incoming(
    &self,
    queue: &str,
    consumer: &str,
  ) -> Result<Self::WaitIncoming, Self::ErrorResult> {
    let key = &format!("{}_{}", queue, PENDING_SUFFIX);
    let mut conn = self.conn.clone();

    let opts = StreamReadOptions::default()
      .block(0)
      .count(1)
      .group(DEFAULT_GROUP, consumer);
    
      conn.xread_options(&[key], &[">"], opts).await
  }
}
