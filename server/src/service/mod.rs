mod ack;
mod dispatcher;
mod store;
pub mod stub;

use stub::tasks::server::{TasksCore, TasksCoreServer};
use stub::tasks::{AcknowledgeRequest, CreateRequest, CreateResponse, Empty, Worker};
use tonic::{Request, Response, Status};
use tracing::{error, info};

use ack::AckManager;
use dispatcher::{Dispatcher, TaskStream};
use store::{Connection, ConnectionAddr, ConnectionInfo, Storage, Store};

pub struct TasksService {
  conn_info: ConnectionInfo,
  // This store must be used ONLY for non-blocking operations
  store: Store,
  ack_manager: AckManager,
}

impl TasksService {
  pub async fn new() -> Result<TasksCoreServer<Self>, Box<dyn std::error::Error>> {
    let conn_info = ConnectionInfo {
      db: 0,
      addr: Box::new(ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6380)),
      username: None,
      passwd: None,
    };

    let conn = Connection::start(conn_info.clone()).await?;

    let store = Store::from(conn.clone()).connect().await?;

    let ack_manager = AckManager::init(store.clone()).await?;

    Ok(TasksCoreServer::new(TasksService {
      conn_info,
      store,
      ack_manager,
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
      .store
      .create_task(task.unwrap())
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

  async fn acknowledge(&self, request: Request<AcknowledgeRequest>) -> ServiceResult<Empty> {
    self
      .ack_manager
      .check(request.into_inner())
      .await
      .map(|_| Response::new(Empty::default()))
  }

  type FetchStream = TaskStream;
  async fn fetch(&self, request: Request<Worker>) -> ServiceResult<Self::FetchStream> {
    let consumer = request.into_inner();
    let dispatcher = Dispatcher::init(
      self.conn_info.clone(),
      consumer,
      self.ack_manager.clone(),
    )
    .await
    .expect("Cannot create dispatcher");
    dispatcher.start_queue().await?;
    info!("Client \"{}\" connected", dispatcher.consumer());
    let tasks_stream = dispatcher.get_tasks().await;
    Ok(Response::new(tasks_stream))
  }
}
