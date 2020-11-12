mod ack;
mod dispatcher;
mod scheduler;
mod store;
mod interceptor;
pub mod stub;

use stub::tasks::server::{TasksCore, TasksCoreServer};
use stub::tasks::{AckRequest, CreateRequest, CreateResponse, Empty, FetchRequest};
use tonic::{Request, Response, Status};
use tracing::{error, info, debug};
use interceptor::RequestInterceptor;

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
    request.intercept()?;
    let payload = request.into_inner();
    // TODO: Validate request data;

    let mut store = self.store.clone();
    let res = if payload.delay > 0 {
      store.create_delayed_task(&payload, payload.delay).await
    } else {
      store.create_task(&payload).await
    };

    res
      .map(|r| {
        debug!("Task created with id {}", r);
        Response::new(CreateResponse { task_id: r })
      })
      .map_err(|e| {
        error!("Cannot create task: {}", e);
        Status::unavailable("Service not available")
      })
  }

  async fn ack(&self, request: Request<AckRequest>) -> ServiceResult<Empty> {
    request.intercept()?;
    self
      .ack_manager
      .check(request.into_inner())
      .await
      .map(|_| Response::new(Empty::default()))
  }

  type FetchStream = TaskStream;
  async fn fetch(&self, request: Request<FetchRequest>) -> ServiceResult<Self::FetchStream> {
    request.intercept()?;
    let payload = request.into_inner();
    let queue = payload.queue;
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
