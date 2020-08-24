mod stub;

use std::pin::Pin;
use std::result::Result as StdResult;
use stub::tasks::server::{TasksCore, TasksCoreServer};
use stub::tasks::{AcknowledgeRequest, CreateRequest, Empty, Task, Worker};
use tokio::stream::Stream;
use tonic::{Request, Response, Status};

pub struct TasksService;

impl TasksService {
  pub fn new() -> TasksCoreServer<Self> {
    TasksCoreServer::new(TasksService {})
  }
}

type Result<T> = StdResult<Response<T>, Status>;

#[tonic::async_trait]
impl TasksCore for TasksService {
  async fn create(&self, _request: Request<CreateRequest>) -> Result<Task> {
    unimplemented!()
  }

  async fn acknowledge(&self, _request: Request<AcknowledgeRequest>) -> Result<Empty> {
    unimplemented!()
  }

  type FetchStream = Pin<Box<dyn Stream<Item = StdResult<Task, Status>> + Send + Sync + 'static>>;
  async fn fetch(&self, _request: Request<Worker>) -> Result<Self::FetchStream> {
    unimplemented!();
  }
}
