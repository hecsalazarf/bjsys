pub use crate::taskstub::CreateRequest;
use serde::Serialize;

pub use serde_json::Error as DataError;

pub struct Task {
  id: Option<String>,
  data: Option<String>,
  delay: u64,
  retry: u32,
}

impl Task {
  pub fn new() -> Self {
    Self {
      ..Self::default()
    }
  }

  pub fn add_data<T>(&mut self, data: &T) -> Result<(), DataError>
  where
    T: Serialize + ?Sized,
  {
    self.data = Some(serde_json::to_string(data)?);
    Ok(())
  }

  pub fn id(&self) -> Option<&String> {
    self.id.as_ref()
  }

  pub fn data(&self) -> Option<&String> {
    self.data.as_ref()
  }

  pub fn into_stub<T>(self, queue: T) -> CreateRequest
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
  fn task_creation() {
    let mut task = Task::new();
    let struct_data = FooData {
      number: 5,
      string: String::from("bar"),
    };

    let ser_data = "{\"number\":5,\"string\":\"bar\"}";
    let res_data = task.add_data(&struct_data);

    assert_eq!(task.id(), None, "ID mismatch");
    assert!(res_data.is_ok(), "Data could not be added");
    assert_eq!(
      task.data().unwrap(),
      &String::from(ser_data),
      "Wrong serialization"
    );
  }
}
