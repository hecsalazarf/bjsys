mod repository;
mod stub;

use std::pin::Pin;
use std::result::Result as StdResult;
use stub::tasks::server::{TasksCore, TasksCoreServer};
use stub::tasks::{AcknowledgeRequest, CreateRequest, Empty, Task, Worker};
use tokio::{stream::Stream, sync::Mutex};
use tonic::{Request, Response, Status};

use repository::{TasksRepository, TasksStorage};

pub struct TasksService {
  repository: Mutex<TasksRepository>,
}

impl TasksService {
  pub async fn new() -> TasksCoreServer<Self> {
    let repository = TasksRepository::connect("redis://127.0.0.1:6380/").await;
    TasksCoreServer::new(TasksService {
      repository: Mutex::new(repository),
    })
  }
}

type Result<T> = StdResult<Response<T>, Status>;

#[tonic::async_trait]
impl TasksCore for TasksService {
  async fn create(&self, request: Request<CreateRequest>) -> Result<Task> {
    let task = request.into_inner().task;
    if let None = task {
      return Err(Status::invalid_argument("No task defined"));
    }
    let mut repo = self.repository.lock().await;

    let res = repo.create(task.unwrap()).await;
    println!("Res {}", res.unwrap());
    Ok(Response::new(Task::default()))
  }

  async fn acknowledge(&self, _request: Request<AcknowledgeRequest>) -> Result<Empty> {
    unimplemented!()
  }

  type FetchStream = Pin<Box<dyn Stream<Item = StdResult<Task, Status>> + Send + Sync + 'static>>;
  async fn fetch(&self, _request: Request<Worker>) -> Result<Self::FetchStream> {
    unimplemented!();
  }
}
