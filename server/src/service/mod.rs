mod repository;
mod dispatcher;
pub mod stub;

use stub::tasks::server::{TasksCore, TasksCoreServer};
use stub::tasks::{AcknowledgeRequest, CreateRequest, CreateResponse, Empty, Task, Worker};
use tokio::sync::{mpsc};
use tonic::{Request, Response, Status};
use tracing::{error, info};

use repository::{TasksRepository, TasksStorage};
use dispatcher::Dispatcher;

pub struct TasksService {
  repository: TasksRepository,
  dispatcher: Dispatcher,
}

impl TasksService {
  pub async fn new() -> Result<TasksCoreServer<Self>, Box<dyn std::error::Error>> {
    let repository = TasksRepository::connect("redis://127.0.0.1:6380/").await?;
    let dispatcher = Dispatcher::new().await?;
    Ok(TasksCoreServer::new(TasksService {
      repository,
      dispatcher,
    }))
  }
}

type ServiceResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl TasksCore for TasksService {
  async fn create(&self, request: Request<CreateRequest>) -> ServiceResult<CreateResponse> {
    let task = request.into_inner().task;
    if task.is_none() {
      return Err(Status::invalid_argument("No task defined"));
    }

    self
      .repository
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

  type FetchStream = mpsc::Receiver<Result<Task, Status>>;
  async fn fetch(&self, request: Request<Worker>) -> ServiceResult<Self::FetchStream> {
    let recv = self.dispatcher.dispatch(request.into_inner()).await;
    Ok(Response::new(recv))
  }
}
