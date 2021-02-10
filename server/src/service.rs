use crate::dispatcher::MasterDispatcher;
use crate::interceptor::RequestInterceptor;
use crate::manager::Manager;
use crate::repository::{RepoError, Repository};
use crate::task::TaskStream;
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
  pub async fn init(repo: Repository) -> anyhow::Result<Self> {
    let inner = TasksServiceImpl::init(repo).await?;
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
  manager: Manager,
  dispatcher: MasterDispatcher,
  repo: Repository,
}

impl TasksServiceImpl {
  pub async fn init(repo: Repository) -> anyhow::Result<Self> {
    let manager = Manager::init(&repo).await?;
    let dispatcher = MasterDispatcher::init(&repo, &manager).await;

    let service = Self {
      repo,
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
    let task_data = request.into_inner().into();
    let res = self.repo.create(&task_data).await;

    res
      .map(|id| {
        let id = id.to_simple().to_string();
        tracing::debug!("Task created with id {}", id);
        Response::new(CreateResponse { task_id: id })
      })
      .map_err(|e| {
        tracing::error!("Cannot create task: {}", e);
        Status::unavailable("Service not available")
      })
  }

  async fn ack(&self, request: Request<AckRequest>) -> ServiceResult<Empty> {
    use std::convert::TryInto;
    request.intercept()?;
    let task = request
      .into_inner()
      .try_into()
      .map_err(|e| Status::invalid_argument(e))?;

    self
      .manager
      .ack(task)
      .await
      .map(|_| Response::new(Empty::default()))
      .map_err(|e| {
        if e == RepoError::NotFound {
          Status::invalid_argument("Task is not pending")
        } else {
          Status::unavailable("Service not available")
        }
      })
  }

  type FetchStream = TaskStream;
  async fn fetch(&self, request: Request<FetchRequest>) -> ServiceResult<Self::FetchStream> {
    request.intercept()?;
    let payload = request.into_inner();
    let stream = self.dispatcher.produce(payload.queue).await.map_err(|e| {
      tracing::error!("Cannot start stream {}", e);
      Status::unavailable("Internal error")
    })?;

    Ok(Response::new(stream))
  }
}

#[xactor::message]
#[derive(Clone)]
pub enum ServiceCmd {
  Shutdown,
}
