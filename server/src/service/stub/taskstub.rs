/// Create Request
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRequest {
  #[prost(message, optional, tag = "1")]
  pub task: ::std::option::Option<Task>,
}
/// Create Response
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateResponse {
  #[prost(string, tag = "1")]
  pub task_id: std::string::String,
}
/// Acknowledge Request
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcknowledgeRequest {
  #[prost(string, tag = "1")]
  pub task_id: std::string::String,
  #[prost(string, tag = "2")]
  pub queue: std::string::String,
  #[prost(enumeration = "TaskStatus", tag = "3")]
  pub status: i32,
}
/// Consumer details
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Consumer {
  #[prost(string, tag = "1")]
  pub hostname: std::string::String,
  #[prost(string, tag = "2")]
  pub kind: std::string::String,
  #[prost(string, tag = "3")]
  pub queue: std::string::String,
  #[prost(string, repeated, tag = "4")]
  pub label: ::std::vec::Vec<std::string::String>,
}
/// Task
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Task {
  #[prost(string, tag = "1")]
  pub id: std::string::String,
  #[prost(string, tag = "2")]
  pub kind: std::string::String,
  #[prost(string, tag = "3")]
  pub queue: std::string::String,
  #[prost(string, tag = "4")]
  pub data: std::string::String,
}
/// Empty response
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Empty {}
/// Task status
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TaskStatus {
  Done = 0,
  Canceled = 1,
  Failed = 2,
}
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
      request: tonic::Request<super::CreateRequest>,
    ) -> Result<tonic::Response<super::CreateResponse>, tonic::Status>;
    #[doc = " Acknowledge that a task was processed (consumer)"]
    async fn acknowledge(
      &self,
      request: tonic::Request<super::AcknowledgeRequest>,
    ) -> Result<tonic::Response<super::Empty>, tonic::Status>;
    #[doc = "Server streaming response type for the Fetch method."]
    type FetchStream: Stream<Item = Result<super::Task, tonic::Status>> + Send + Sync + 'static;
    #[doc = " Consumer that connects to process tasks (consumer)"]
    async fn fetch(
      &self,
      request: tonic::Request<super::Consumer>,
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
        "/taskstub.TasksCore/Create" => {
          #[allow(non_camel_case_types)]
          struct CreateSvc<T: TasksCore>(pub Arc<T>);
          impl<T: TasksCore> tonic::server::UnaryService<super::CreateRequest> for CreateSvc<T> {
            type Response = super::CreateResponse;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(&mut self, request: tonic::Request<super::CreateRequest>) -> Self::Future {
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
        "/taskstub.TasksCore/Acknowledge" => {
          #[allow(non_camel_case_types)]
          struct AcknowledgeSvc<T: TasksCore>(pub Arc<T>);
          impl<T: TasksCore> tonic::server::UnaryService<super::AcknowledgeRequest> for AcknowledgeSvc<T> {
            type Response = super::Empty;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(&mut self, request: tonic::Request<super::AcknowledgeRequest>) -> Self::Future {
              let inner = self.0.clone();
              let fut = async move { (*inner).acknowledge(request).await };
              Box::pin(fut)
            }
          }
          let inner = self.inner.clone();
          let fut = async move {
            let interceptor = inner.1.clone();
            let inner = inner.0;
            let method = AcknowledgeSvc(inner);
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
        "/taskstub.TasksCore/Fetch" => {
          #[allow(non_camel_case_types)]
          struct FetchSvc<T: TasksCore>(pub Arc<T>);
          impl<T: TasksCore> tonic::server::ServerStreamingService<super::Consumer> for FetchSvc<T> {
            type Response = super::Task;
            type ResponseStream = T::FetchStream;
            type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
            fn call(&mut self, request: tonic::Request<super::Consumer>) -> Self::Future {
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
    const NAME: &'static str = "taskstub.TasksCore";
  }
}
