use std::collections::HashMap;

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

impl FetchResponse {
  pub fn from_map(id: String, mut values: HashMap<String, String>) -> Self {
    let data = values.remove(TaskHash::DATA).unwrap_or_default();
    let queue = values.remove(TaskHash::QUEUE).unwrap_or_default();

    FetchResponse { id, queue, data }
  }
}
