use serde::{Serialize, Deserialize};
pub use crate::stub::msg::*;

pub struct TaskHash;

impl TaskHash {
  pub const ID: &'static str = "id";
  pub const DATA: &'static str = "data";
  pub const QUEUE: &'static str = "queue";
  pub const RETRY: &'static str = "retry";
  pub const DELIVERIES: &'static str = "deliveries";
  pub const STATUS: &'static str = "status";
  pub const MESSAGE: &'static str = "message";
  pub const PROCESSED_ON: &'static str = "processed_on";
  pub const FINISHED_ON: &'static str = "finished_on";
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TaskData {
  pub args: String,
  pub queue: String,
  pub retry: u32,
  pub delay: u64,
  pub deliveries: u32,
  pub status: i32,
  pub message: String,
  pub processed_on: u64,
  pub finished_on: u64,
}

impl From<CreateRequest> for TaskData {
  fn from(val: CreateRequest) -> Self {
    Self {
      args: val.data,
      queue: val.queue,
      retry: val.retry,
      delay: val.delay,
      ..Self::default()
    }
  }
}

impl From<AckRequest> for TaskData {
  fn from(val: AckRequest) -> Self {
    Self {
      queue: val.queue,
      status: val.status,
      message: val.message,
      ..Self::default()
    }
  }
}
