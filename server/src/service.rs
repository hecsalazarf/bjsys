use crate::dispatcher::{MasterDispatcher, TaskStream};
use crate::interceptor::RequestInterceptor;
use crate::manager::Manager;
use crate::store::{ConnectionInfo, MultiplexedStore, RedisStorage};
use proto::server::{TasksCore, TasksCoreServer};
use proto::{AckRequest, CreateRequest, CreateResponse, Empty, FetchRequest};
use std::net::SocketAddr;
use tonic::transport::{
  server::{Router, Unimplemented},
  Error, Server,
};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
pub trait Runnable {
  async fn listen_on(self, socket: SocketAddr) -> Result<(), Error>;
}

type ServiceRouter = Router<TasksCoreServer<TasksServiceImpl>, Unimplemented>;

pub struct TaskService {
  router: ServiceRouter,
}

impl TaskService {
  pub async fn init(redis_info: &ConnectionInfo) -> Result<Self, Box<dyn std::error::Error>> {
    let inner = TasksServiceImpl::init(redis_info).await?;
    let router = Server::builder().add_service(TasksCoreServer::new(inner));

    let service = Self { router };

    Ok(service)
  }
}

#[tonic::async_trait]
impl Runnable for TaskService {
  async fn listen_on(self, socket: SocketAddr) -> Result<(), Error> {
    tracing::info!("Server is ready at port {}", socket.port());
    let signal = TasksServiceImpl::exit_signal();
    self.router.serve_with_shutdown(socket, signal).await
  }
}

struct TasksServiceImpl {
  // This store must be used ONLY for non-blocking operations
  store: MultiplexedStore,
  manager: Manager,
  dispatcher: MasterDispatcher,
}

impl TasksServiceImpl {
  pub async fn init(redis_conn: &ConnectionInfo) -> Result<Self, Box<dyn std::error::Error>> {
    let store = MultiplexedStore::connect(redis_conn).await?;
    let manager = Manager::init(&store).await?;
    let dispatcher = MasterDispatcher::init(&store, &manager, redis_conn).await;

    let service = Self {
      store,
      manager,
      dispatcher,
    };
    Ok(service)
  }

  pub fn exit_signal() -> impl std::future::Future<Output = ()> {
    use xactor::Service;
    async {
      tokio::signal::ctrl_c().await.unwrap();
      tracing::info!("Shutting down...");
      let mut addr = xactor::Broker::from_registry().await.unwrap();
      addr.publish(ServiceCmd::Shutdown).unwrap();
    }
  }
}

type ServiceResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl TasksCore for TasksServiceImpl {
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
        tracing::debug!("Task created with id {}", r);
        Response::new(CreateResponse { task_id: r })
      })
      .map_err(|e| {
        tracing::error!("Cannot create task: {}", e);
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
