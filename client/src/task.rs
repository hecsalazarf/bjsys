use crate::taskstub::Task as TaskStub;
use serde::Serialize;

pub use serde_json::Error as DataError;

#[derive(Default)]
pub struct Task {
  id: Option<String>,
  kind: String,
  data: Option<String>,
}

impl Task {
  pub fn new<T>(kind: T) -> Self
  where
    T: Into<String>,
  {
    Self {
      kind: kind.into(),
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

  pub fn kind(&self) -> &str {
    self.kind.as_ref()
  }

  pub fn data(&self) -> Option<&String> {
    self.data.as_ref()
  }

  pub fn into_stub<T>(self, queue: T) -> TaskStub
  where
    T: Into<String>,
  {
    TaskStub {
      id: self.id.unwrap_or_else(|| String::new()),
      kind: self.kind,
      data: self.data.unwrap_or_else(|| String::new()),
      queue: queue.into(),
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
    let mut task = Task::new("foo");
    let struct_data = FooData {
      number: 5,
      string: String::from("bar"),
    };

    let ser_data = "{\"number\":5,\"string\":\"bar\"}";
    let res_data = task.add_data(&struct_data);

    assert_eq!(task.id(), None, "ID mismatch");
    assert_eq!(task.kind(), "foo", "Kind mismatch");
    assert!(res_data.is_ok(), "Data could not be added");
    assert_eq!(
      task.data().unwrap(),
      &String::from(ser_data),
      "Wrong serialization"
    );
  }
}
