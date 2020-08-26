/// Create Request
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRequest {
  #[prost(message, optional, tag = "1")]
  pub task: ::std::option::Option<Task>,
}
/// Acknowledge Request
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcknowledgeRequest {
  #[prost(string, tag = "1")]
  pub task_id: std::string::String,
  #[prost(enumeration = "TaskStatus", tag = "2")]
  pub status: i32,
}
/// Worker details
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Worker {
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
#[doc = r" Generated client implementations."]
pub mod tasks_core_client {
  #![allow(unused_variables, dead_code, missing_docs)]
  use tonic::codegen::*;
  pub struct TasksCoreClient<T> {
    inner: tonic::client::Grpc<T>,
  }
  impl TasksCoreClient<tonic::transport::Channel> {
    #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
      D: std::convert::TryInto<tonic::transport::Endpoint>,
      D::Error: Into<StdError>,
    {
      let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
      Ok(Self::new(conn))
    }
  }
  impl<T> TasksCoreClient<T>
  where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::ResponseBody: Body + HttpBody + Send + 'static,
    T::Error: Into<StdError>,
    <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
  {
    pub fn new(inner: T) -> Self {
      let inner = tonic::client::Grpc::new(inner);
      Self { inner }
    }
    pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
      let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
      Self { inner }
    }
    #[doc = " Create a new task (producer)"]
    pub async fn create(
      &mut self,
      request: impl tonic::IntoRequest<super::CreateRequest>,
    ) -> Result<tonic::Response<super::Task>, tonic::Status> {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/taskstub.TasksCore/Create");
      self.inner.unary(request.into_request(), path, codec).await
    }
    #[doc = " Acknowledge that a task was processed (consumer)"]
    pub async fn acknowledge(
      &mut self,
      request: impl tonic::IntoRequest<super::AcknowledgeRequest>,
    ) -> Result<tonic::Response<super::Empty>, tonic::Status> {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/taskstub.TasksCore/Acknowledge");
      self.inner.unary(request.into_request(), path, codec).await
    }
    #[doc = " Worker that connects to process tasks (consumer)"]
    pub async fn fetch(
      &mut self,
      request: impl tonic::IntoRequest<super::Worker>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<super::Task>>, tonic::Status> {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/taskstub.TasksCore/Fetch");
      self
        .inner
        .server_streaming(request.into_request(), path, codec)
        .await
    }
  }
  impl<T: Clone> Clone for TasksCoreClient<T> {
    fn clone(&self) -> Self {
      Self {
        inner: self.inner.clone(),
      }
    }
  }
  impl<T> std::fmt::Debug for TasksCoreClient<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "TasksCoreClient {{ ... }}")
    }
  }
}