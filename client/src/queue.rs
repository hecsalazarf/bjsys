use crate::error::Error;
use crate::task::Builder;
use common::client::TasksCoreClient as Client;
use common::RequestExt;
use serde::Serialize;
use tonic::transport::channel::Channel;
use tonic::transport::{Endpoint, Uri};
use tonic::Request;

#[derive(Debug)]
pub struct QueueBuilder {
  name: String,
  endpoint: Endpoint,
}

impl Default for QueueBuilder {
  fn default() -> Self {
    Self {
      name: String::from("default"),
      endpoint: Endpoint::from_static("http://127.0.0.1:7330"),
    }
  }
}

impl QueueBuilder {
  pub fn with_name<T: Into<String>>(mut self, name: T) -> Self {
    self.name = name.into();
    self
  }

  pub fn endpoint(mut self, uri: Uri) -> Self {
    self.endpoint = uri.into();
    self
  }

  pub async fn connect(self) -> Result<Queue, Error> {
    let channel = self.endpoint.connect().await?;
    Ok(Queue {
      name: self.name,
      client: Client::new(channel),
    })
  }
}

#[derive(Debug)]
pub struct Queue {
  name: String,
  client: Client<Channel>,
}

impl Queue {
  pub fn configure() -> QueueBuilder {
    QueueBuilder::default()
  }

  pub async fn add<T>(&mut self, task: Builder<T>) -> Result<String, Error>
  where
    T: Serialize,
  {
    let mut request = Request::new(task.into_request(&self.name)?);
    let _req_id = request.make_idempotent();

    let response = self
      .client
      .create(request)
      .await?
      .into_inner();

    Ok(response.task_id)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn default_builder() {
    let name = "default";
    let builder = Queue::configure();
    assert_eq!(builder.name, String::from(name));
    assert_eq!(
      builder.endpoint.uri(),
      &Uri::from_static("http://127.0.0.1:7330")
    );
  }
}
