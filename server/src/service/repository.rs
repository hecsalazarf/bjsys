use super::stub::tasks::Task;
use redis::AsyncCommands;
// use redis::streams::StreamPendingId;

pub use redis::RedisError as DbError;
pub use redis::ErrorKind as DbErrorKind;

const PENDING_SUFFIX: &str = "pending";
const DEFAULT_GROUP: &str = "default_group";

#[tonic::async_trait]
pub trait TasksStorage {
  type CreateResult: Send + Sized;
  type ErrorResult: std::error::Error;
  type PendingResult: Send + Sized;
  async fn create(&self, task: Task) -> Result<Self::CreateResult, Self::ErrorResult>;
  async fn create_queue(&self, queue: &str) -> Result<Self::CreateResult, Self::ErrorResult>;
  async fn pending(&self, queue: &str, consumer: &str) -> Result<Self::PendingResult, Self::ErrorResult>;
}

pub struct TasksRepository {
  conn: redis::aio::MultiplexedConnection,
}

impl TasksRepository {
  pub async fn connect(params: &str) -> Result<Self, redis::RedisError> {
    let conn = redis::Client::open(params)?
      .get_multiplexed_async_connection()
      .await?;
    Ok(TasksRepository { conn })
  }
}

#[tonic::async_trait]
impl TasksStorage for TasksRepository {
  type CreateResult = String;
  type ErrorResult = DbError;
  type PendingResult = redis::streams::StreamRangeReply;
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

  async fn pending(&self, queue: &str, consumer: &str) -> Result<Self::PendingResult, Self::ErrorResult> {
    let key = format!("{}_{}", queue, PENDING_SUFFIX);
    let key = &key;
    let mut conn = self.conn.clone();
    let mut res: redis::streams::StreamPendingCountReply = conn
      .xpending_consumer_count(key, DEFAULT_GROUP, "-", "+", 1, consumer)
      .await?;
    
    if let Some(t) = res.ids.pop() {
      return conn
        .xrange(key, &t.id, &t.id)
        .await;
    } else {
      return Err(DbError::from((DbErrorKind::ExtensionError, "")))
    }
  }
}
