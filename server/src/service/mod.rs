mod repository;
pub mod stub;

use stub::tasks::server::{TasksCore, TasksCoreServer};
use stub::tasks::{AcknowledgeRequest, CreateRequest, CreateResponse, Empty, Task, Worker};
use tokio::{stream::Stream, sync::Mutex};
use tonic::{Request, Response, Status};
use tracing::{error, info};

use repository::{TasksRepository, TasksStorage};

pub struct TasksService {
  // Wrap repository inside a Mutex as it needs to be mutable
  // due to redis crate implementation, but trait definition
  // does not allow mutable references
  repository: Mutex<TasksRepository>,
}

impl TasksService {
  pub async fn new() -> Result<TasksCoreServer<Self>, Box<dyn std::error::Error>> {
    let repository = TasksRepository::connect("redis://127.0.0.1:6380/").await?;
    Ok(TasksCoreServer::new(TasksService {
      repository: Mutex::new(repository),
    }))
  }
}

type ServiceResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl TasksCore for TasksService {
  async fn create(&self, request: Request<CreateRequest>) -> ServiceResult<CreateResponse> {
    let task = request.into_inner().task;
    if let None = task {
      return Err(Status::invalid_argument("No task defined"));
    }

    self
      .repository
      .lock()
      .await
      .create(task.unwrap())
      .await
      .map(|r| {
        info!("Task created with id {}", r);
        Response::new(CreateResponse { task_id: r })
      })
      .map_err(|e| {
        error!("Cannot create task: {}", e);
        Status::unavailable("Service not available")
      })
  }

  async fn acknowledge(&self, _request: Request<AcknowledgeRequest>) -> ServiceResult<Empty> {
    Err(Status::unimplemented(""))
  }

  type FetchStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<Task, Status>> + Send + Sync + 'static>>;
  async fn fetch(&self, _request: Request<Worker>) -> ServiceResult<Self::FetchStream> {
    Err(Status::unimplemented(""))
  }
}
