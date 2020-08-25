use super::stub::tasks::Task;
use redis::AsyncCommands;
use tracing::error;

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
  pub async fn connect(params: &str) -> Self {
    let connection = get_connection(params).await;
    if let Err(e) = &connection {
      error!("{}", e);
      std::process::exit(1);
    }

    TasksRepository {
      conn: connection.unwrap(),
    }
  }
}

#[tonic::async_trait]
impl TasksStorage for TasksRepository {
  type CreateResult = String;
  type ErrorResult = redis::RedisError;
  async fn create(&mut self, task: Task) -> Result<Self::CreateResult, Self::ErrorResult> {
    let key = format!("{}_{}", task.queue, WAITING_SUFFIX);
    self
      .conn
      .xadd(&key, "*", &[("kind", &task.kind), ("data", &task.data)])
      .await
  }
}

async fn get_connection(params: &str) -> redis::RedisResult<redis::aio::Connection> {
  let client = redis::Client::open(params)?;
  client.get_async_connection().await
}
