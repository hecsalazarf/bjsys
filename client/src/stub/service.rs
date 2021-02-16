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
      request: impl tonic::IntoRequest<::common::service::CreateRequest>,
    ) -> Result<tonic::Response<::common::service::CreateResponse>, tonic::Status> {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/service.TasksCore/Create");
      self.inner.unary(request.into_request(), path, codec).await
    }
    #[doc = " Acknowledge that a task was processed (consumer)"]
    pub async fn ack(
      &mut self,
      request: impl tonic::IntoRequest<::common::service::AckRequest>,
    ) -> Result<tonic::Response<::common::service::Empty>, tonic::Status> {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/service.TasksCore/Ack");
      self.inner.unary(request.into_request(), path, codec).await
    }
    #[doc = " Fetch to process tasks (consumer)"]
    pub async fn fetch(
      &mut self,
      request: impl tonic::IntoRequest<::common::service::FetchRequest>,
    ) -> Result<
      tonic::Response<tonic::codec::Streaming<::common::service::FetchResponse>>,
      tonic::Status,
    > {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/service.TasksCore/Fetch");
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
