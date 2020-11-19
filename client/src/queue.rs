use crate::task::Builder;
use crate::taskstub::tasks_core_client::TasksCoreClient as Client;
use crate::{ChannelError, ChannelStatus};
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
      endpoint: Endpoint::from_static("http://localhost:11000"),
    }
  }
}

impl QueueBuilder {
  pub fn with_name<T: Into<String>>(mut self, name: T) -> Self {
    self.name = name.into();
    self
  }

  pub fn endpoint<T: Into<Uri>>(mut self, uri: T) -> Self {
    self.endpoint = Endpoint::from(uri.into());
    self
  }

  pub async fn connect(self) -> Result<Queue, ChannelError> {
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

  pub async fn add<T>(&mut self, task: Builder<T>) -> Result<String, ChannelStatus>
  where
    T: Serialize,
  {
    let request = task.into_request(&self.name).map_err(|e| {
      let error = format!("Cannot serialize data: {}", e);
      ChannelStatus::invalid_argument(error)
    })?;

    let response = self
      .client
      .create(Request::new(request))
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
      &Uri::from_static("http://localhost:11000")
    );
  }
}
