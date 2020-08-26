use super::stub::tasks::Task;
use redis::AsyncCommands;

pub use redis::RedisError as DbError;

const WAITING_SUFFIX: &'static str = "waiting";

#[tonic::async_trait]
pub trait TasksStorage {
  type CreateResult: Send + Sized;
  type ErrorResult: std::error::Error;
  async fn create(&mut self, task: Task) -> Result<Self::CreateResult, Self::ErrorResult>;
}

pub struct TasksRepository {
  conn: redis::aio::Connection,
}

impl TasksRepository {
  pub async fn connect(params: &str) -> Result<Self, redis::RedisError> {
    let conn = redis::Client::open(params)?.get_async_connection().await?;
    Ok(TasksRepository { conn })
  }
}

#[tonic::async_trait]
impl TasksStorage for TasksRepository {
  type CreateResult = String;
  type ErrorResult = DbError;
  async fn create(&mut self, task: Task) -> Result<Self::CreateResult, Self::ErrorResult> {
    let key = format!("{}_{}", task.queue, WAITING_SUFFIX);
    self
      .conn
      .xadd(&key, "*", &[("kind", &task.kind), ("data", &task.data)])
      .await
  }
}
