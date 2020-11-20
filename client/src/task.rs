use crate::taskstub::{CreateRequest, FetchResponse};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;

pub use serde_json::Error as DataError;

pub struct Builder<T: Serialize> {
  data: T,
  delay: u64,
  retry: u32,
}

impl<T: Serialize> Builder<T> {
  pub fn new(data: T) -> Self {
    Self {
      data,
      delay: 0,
      retry: 25, // Default is 25 retries
    }
  }

  pub fn delay(&mut self, delay: Duration) -> &mut Self {
    self.delay = delay.as_millis() as u64;
    self
  }

  pub fn retry(&mut self, retries: u32) -> &mut Self {
    self.retry = retries;
    self
  }

  pub(crate) fn into_request<S>(self, queue: S) -> Result<CreateRequest, DataError>
  where
    S: Into<String>,
  {
    let request = CreateRequest {
      data: serde_json::to_string(&self.data)?,
      queue: queue.into(),
      delay: self.delay,
      retry: self.retry,
    };

    Ok(request)
  }
}

#[derive(Debug)]
pub struct Task<T: DeserializeOwned> {
  id: String,
  queue: String,
  data: T,
}

impl<T: DeserializeOwned> Task<T> {
  pub fn id(&self) -> &str {
    self.id.as_ref()
  }

  pub fn queue(&self) -> &str {
    self.queue.as_ref()
  }

  pub fn into_inner(self) -> T {
    self.data
  }

  pub(crate) fn from_response(response: FetchResponse) -> Result<Self, DataError> {
    let data = serde_json::from_str(&response.data)?;

    let task = Self {
      id: response.id,
      queue: response.queue,
      data,
    };

    Ok(task)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde::Deserialize;

  #[derive(Serialize, Debug, Deserialize, PartialEq)]
  struct FooData {
    number: u32,
    string: String,
  }

  #[test]
  fn default_builder() {
    let task = Builder::new(());
    assert_eq!(task.data, (), "Non-empty data");
    assert_eq!(task.delay, 0);
    assert_eq!(task.retry, 25);
  }

  #[test]
  fn data_builder() {
    let struct_data = FooData {
      number: 5,
      string: "bar".into(),
    };

    let task = Builder::new(&struct_data);
    assert_eq!(task.data, &struct_data);
  }
}
