#[doc = r" Generated server implementations."]
pub mod tasks_core_server {
  #![allow(unused_variables, dead_code, missing_docs)]
  use tonic::codegen::*;
  #[doc = "Generated trait containing gRPC methods that should be implemented for use with TasksCoreServer."]
  #[async_trait]
  pub trait TasksCore: Send + Sync + 'static {
    #[doc = " Create a new task (producer)"]
    async fn create(
      &self,
      request: tonic::Request<::common::service::CreateRequest>,
    ) -> Result<tonic::Response<::common::service::CreateResponse>, tonic::Status>;
    #[doc = " Acknowledge that a task was processed (consumer)"]
    async fn ack(
      &self,
      request: tonic::Request<::common::service::AckRequest>,
    ) -> Result<tonic::Response<::common::service::Empty>, tonic::Status>;
    #[doc = "Server streaming response type for the Fetch method."]
    type FetchStream: Stream<Item = Result<::common::service::FetchResponse, tonic::Status>>
      + Send
      + Sync
      + 'static;
    #[doc = " Fetch to process tasks (consumer)"]
    async fn fetch(
      &self,
      request: tonic::Request<::common::service::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status>;
  }
  #[derive(Debug)]
  pub struct TasksCoreServer<T: TasksCore> {
    inner: _Inner<T>,
  }
  struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
  impl<T: TasksCore> TasksCoreServer<T> {
    pub fn new(inner: T) -> Self {
      let inner = Arc::new(inner);
      let inner = _Inner(inner, None);
      Self { inner }
    }
    pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
      let inner = Arc::new(inner);
      let inner = _Inner(inner, Some(interceptor.into()));
      Self { inner }
    }
  }
  impl<T, B> Service<http::Request<B>> for TasksCoreServer<T>
  where
    T: TasksCore,
    B: HttpBody + Send + Sync + 'static,
    B::Error: Into<StdError> + Send + 'static,
  {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Never;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
      Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<B>) -> Self::Future {
      let inner = self.inner.clone();
      match req.uri().path() {
        "/service.TasksCore/Create" => {
          #[allow(non_camel_case_types)]
          struct CreateSvc<T: TasksCore>(pub Arc<T>);
          impl<T: TasksCore> tonic::server::UnaryService<::common::service::CreateRequest> for CreateSvc<T> {
            type Response = ::common::service::CreateResponse;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(
              &mut self,
              request: tonic::Request<::common::service::CreateRequest>,
            ) -> Self::Future {
              let inner = self.0.clone();
              let fut = async move { (*inner).create(request).await };
              Box::pin(fut)
            }
          }
          let inner = self.inner.clone();
          let fut = async move {
            let interceptor = inner.1.clone();
            let inner = inner.0;
            let method = CreateSvc(inner);
            let codec = tonic::codec::ProstCodec::default();
            let mut grpc = if let Some(interceptor) = interceptor {
              tonic::server::Grpc::with_interceptor(codec, interceptor)
            } else {
              tonic::server::Grpc::new(codec)
            };
            let res = grpc.unary(method, req).await;
            Ok(res)
          };
          Box::pin(fut)
        }
        "/service.TasksCore/Ack" => {
          #[allow(non_camel_case_types)]
          struct AckSvc<T: TasksCore>(pub Arc<T>);
          impl<T: TasksCore> tonic::server::UnaryService<::common::service::AckRequest> for AckSvc<T> {
            type Response = ::common::service::Empty;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(
              &mut self,
              request: tonic::Request<::common::service::AckRequest>,
            ) -> Self::Future {
              let inner = self.0.clone();
              let fut = async move { (*inner).ack(request).await };
              Box::pin(fut)
            }
          }
          let inner = self.inner.clone();
          let fut = async move {
            let interceptor = inner.1.clone();
            let inner = inner.0;
            let method = AckSvc(inner);
            let codec = tonic::codec::ProstCodec::default();
            let mut grpc = if let Some(interceptor) = interceptor {
              tonic::server::Grpc::with_interceptor(codec, interceptor)
            } else {
              tonic::server::Grpc::new(codec)
            };
            let res = grpc.unary(method, req).await;
            Ok(res)
          };
          Box::pin(fut)
        }
        "/service.TasksCore/Fetch" => {
          #[allow(non_camel_case_types)]
          struct FetchSvc<T: TasksCore>(pub Arc<T>);
          impl<T: TasksCore> tonic::server::ServerStreamingService<::common::service::FetchRequest>
            for FetchSvc<T>
          {
            type Response = ::common::service::FetchResponse;
            type ResponseStream = T::FetchStream;
            type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
            fn call(
              &mut self,
              request: tonic::Request<::common::service::FetchRequest>,
            ) -> Self::Future {
              let inner = self.0.clone();
              let fut = async move { (*inner).fetch(request).await };
              Box::pin(fut)
            }
          }
          let inner = self.inner.clone();
          let fut = async move {
            let interceptor = inner.1;
            let inner = inner.0;
            let method = FetchSvc(inner);
            let codec = tonic::codec::ProstCodec::default();
            let mut grpc = if let Some(interceptor) = interceptor {
              tonic::server::Grpc::with_interceptor(codec, interceptor)
            } else {
              tonic::server::Grpc::new(codec)
            };
            let res = grpc.server_streaming(method, req).await;
            Ok(res)
          };
          Box::pin(fut)
        }
        _ => Box::pin(async move {
          Ok(
            http::Response::builder()
              .status(200)
              .header("grpc-status", "12")
              .header("content-type", "application/grpc")
              .body(tonic::body::BoxBody::empty())
              .unwrap(),
          )
        }),
      }
    }
  }
  impl<T: TasksCore> Clone for TasksCoreServer<T> {
    fn clone(&self) -> Self {
      let inner = self.inner.clone();
      Self { inner }
    }
  }
  impl<T: TasksCore> Clone for _Inner<T> {
    fn clone(&self) -> Self {
      Self(self.0.clone(), self.1.clone())
    }
  }
  impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "{:?}", self.0)
    }
  }
  impl<T: TasksCore> tonic::transport::NamedService for TasksCoreServer<T> {
    const NAME: &'static str = "service.TasksCore";
  }
}
