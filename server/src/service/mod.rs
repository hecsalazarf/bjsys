mod ack;
mod dispatcher;
mod store;
pub mod stub;

use stub::tasks::server::{TasksCore, TasksCoreServer};
use stub::tasks::{AcknowledgeRequest, Consumer, CreateRequest, CreateResponse, Empty};
use tonic::{Request, Response, Status};
use tracing::{error, info};

use ack::AckManager;
use dispatcher::{MasterDispatcher, TaskStream};
use store::{MultiplexedStore, RedisStorage};

pub struct TasksService {
  // This store must be used ONLY for non-blocking operations
  store: MultiplexedStore,
  ack_manager: AckManager,
  dispatcher: MasterDispatcher,
}

impl TasksService {
  pub async fn new() -> Result<TasksCoreServer<Self>, Box<dyn std::error::Error>> {
    let store = MultiplexedStore::connect().await?;
    let ack_manager = AckManager::init(store.clone()).await?;
    let dispatcher = MasterDispatcher::init(store.clone(), ack_manager.clone()).await;
    Ok(TasksCoreServer::new(TasksService {
      store: store,
      ack_manager,
      dispatcher,
    }))
  }

  pub fn exit_signal() -> impl std::future::Future<Output = ()> {
    use xactor::Service;
    let signal = async {
      tokio::signal::ctrl_c().await.unwrap();
      info!("Shutting down...");
      let mut addr = xactor::Broker::from_registry().await.unwrap();
      addr.publish(ServiceCmd::Shutdown).unwrap();
    };
    signal
  }
}

type ServiceResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl TasksCore for TasksService {
  async fn create(&self, request: Request<CreateRequest>) -> ServiceResult<CreateResponse> {
    let request = request.into_inner();
    if request.task.is_none() {
      return Err(Status::invalid_argument("No task defined"));
    }
    let task = request.task.unwrap();

    let mut store = self.store.clone();
    let res = if request.delay > 0 {
      store.create_delayed_task(task, request.delay).await
    } else {
      store.create_task(task).await
    };

    res
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
  async fn fetch(&self, request: Request<Consumer>) -> ServiceResult<Self::FetchStream> {
    let request = request.into_inner();
    let queue = request.queue;
    let dispatcher = self.dispatcher.create(queue).await?;

    let tasks_stream = dispatcher.into_stream().await.expect("into_stream");
    Ok(Response::new(tasks_stream))
  }
}

#[xactor::message]
#[derive(Clone)]
pub enum ServiceCmd {
  Shutdown,
}
