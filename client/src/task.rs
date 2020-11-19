pub use crate::taskstub::CreateRequest;
use serde::Serialize;
use std::time::Duration;

pub use serde_json::Error as DataError;

pub struct Task {
  id: Option<String>,
  data: Option<String>,
  delay: u64,
  retry: u32,
}

impl Task {
  pub fn new() -> Self {
    Self { ..Self::default() }
  }

  pub fn with_data<T>(data: &T) -> Result<Self, DataError>
  where
    T: Serialize + ?Sized,
  {
    let mut task = Self::new();
    task.data = Some(serde_json::to_string(data)?);
    Ok(task)
  }

  pub fn delay(&mut self, delay: Duration) -> &mut Self {
    self.delay = delay.as_millis() as u64;
    self
  }

  pub fn retry(&mut self, retries: u32) -> &mut Self {
    self.retry = retries;
    self
  }

  pub fn id(&self) -> Option<&String> {
    self.id.as_ref()
  }

  pub fn data(&self) -> Option<&String> {
    self.data.as_ref()
  }

  pub(crate) fn into_stub<T>(self, queue: T) -> CreateRequest
  where
    T: Into<String>,
  {
    CreateRequest {
      data: self.data.unwrap_or_else(String::new),
      queue: queue.into(),
      delay: self.delay,
      retry: self.retry,
    }
  }
}

impl Default for Task {
  fn default() -> Self {
    Self {
      id: None,
      data: None,
      delay: 0,
      retry: 1,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[derive(Serialize)]
  struct FooData {
    number: u32,
    string: String,
  }

  #[test]
  fn empty_task() {
    let task = Task::new();
    assert_eq!(task.id(), None, "ID mismatch");
    assert_eq!(task.data(), None, "Non-empty data");
  }

  #[test]
  fn data_task() {
    let struct_data = FooData {
      number: 5,
      string: String::from("bar"),
    };

    let ser_data = "{\"number\":5,\"string\":\"bar\"}";
    let res = Task::with_data(&struct_data);
    assert!(res.is_ok(), "Data could not be added");

    let task = res.unwrap();
    assert_eq!(task.id(), None, "ID mismatch");
    assert_eq!(
      task.data().unwrap(),
      &String::from(ser_data),
      "Wrong serialization"
    );
  }
}
