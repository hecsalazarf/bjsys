use crate::dispatcher::{MasterDispatcher, TaskStream};
use crate::interceptor::RequestInterceptor;
use crate::manager::Manager;
use crate::store::{MultiplexedStore, RedisStorage};
use proto::server::{TasksCore, TasksCoreServer};
use proto::{AckRequest, CreateRequest, CreateResponse, Empty, FetchRequest};

use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

pub struct TasksService {
  // This store must be used ONLY for non-blocking operations
  store: MultiplexedStore,
  manager: Manager,
  dispatcher: MasterDispatcher,
}

impl TasksService {
  pub async fn new() -> Result<TasksCoreServer<Self>, Box<dyn std::error::Error>> {
    let store = MultiplexedStore::connect().await?;
    let manager = Manager::init(store.clone()).await?;
    let dispatcher = MasterDispatcher::init(store.clone(), manager.clone()).await;
    Ok(TasksCoreServer::new(TasksService {
      store: store,
      manager,
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
      .manager
      .ack(request.into_inner())
      .await
      .map(|_| Response::new(Empty::default()))
  }

  type FetchStream = TaskStream;
  async fn fetch(&self, request: Request<FetchRequest>) -> ServiceResult<Self::FetchStream> {
    request.intercept()?;
    let payload = request.into_inner();
    let stream = self.dispatcher.produce(payload.queue).await?;

    Ok(Response::new(stream))
  }
}

#[xactor::message]
#[derive(Clone)]
pub enum ServiceCmd {
  Shutdown,
}
