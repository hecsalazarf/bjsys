use crate::taskstub::tasks_core_client::TasksCoreClient as Client;
use tonic::transport::channel::Channel;
use tonic::transport::{Endpoint, Uri};

pub use tonic::transport::Error as ChannelError;

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
