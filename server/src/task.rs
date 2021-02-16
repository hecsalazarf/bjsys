use crate::dispatcher::Dispatcher;
use crate::repository::RepoError;
use common::service::{AckRequest, CreateRequest, FetchResponse};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, Notify};

pub use common::service::TaskStatus;
pub type TaskId = uuid::Uuid;

#[derive(Default, Debug)]
pub struct Task {
  id: TaskId,
  data: TaskData,
}

impl Task {
  pub fn from_parts(id: TaskId, data: TaskData) -> Self {
    Self { id, data }
  }

  pub fn id(&self) -> &TaskId {
    &self.id
  }
  pub fn data(&self) -> &TaskData {
    &self.data
  }

  pub fn status(&self) -> TaskStatus {
    // Never fails as it's previously validated
    TaskStatus::from_i32(self.data.status).unwrap()
  }

  pub fn into_parts(self) -> (TaskId, TaskData) {
    (self.id, self.data)
  }
}

impl std::convert::TryFrom<AckRequest> for Task {
  type Error = &'static str;
  fn try_from(val: AckRequest) -> Result<Self, Self::Error> {
    let task = Self {
      id: TaskId::parse_str(&val.task_id).map_err(|_| "uuid failed")?,
      data: val.into(),
    };
    Ok(task)
  }
}

impl From<Task> for FetchResponse {
  fn from(val: Task) -> Self {
    Self {
      id: val.id.to_simple().to_string(),
      queue: val.data.queue,
      data: val.data.args,
    }
  }
}

#[derive(Clone)]
pub struct InProcessTask {
  id: Arc<TaskId>,
  notify: Arc<Notify>,
}

impl InProcessTask {
  pub fn new(id: TaskId) -> Self {
    Self {
      id: Arc::new(id),
      notify: Arc::new(Notify::new()),
    }
  }

  pub fn id(&self) -> Weak<TaskId> {
    Arc::downgrade(&self.id)
  }

  pub async fn wait_to_finish(&self) {
    self.notify.notified().await;
  }

  pub fn ack(&self) {
    self.notify.notify_one();
  }
}

impl std::hash::Hash for InProcessTask {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.id.hash(state);
  }
}

impl PartialEq for InProcessTask {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl Eq for InProcessTask {}

impl std::borrow::Borrow<TaskId> for InProcessTask {
  fn borrow(&self) -> &TaskId {
    self.id.as_ref()
  }
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

pub struct TaskStream {
  stream: mpsc::Receiver<Result<Task, RepoError>>,
  dispatcher: Dispatcher,
  id: u64,
}

impl TaskStream {
  pub fn new(
    stream: mpsc::Receiver<Result<Task, RepoError>>,
    dispatcher: Dispatcher,
    id: u64,
  ) -> Self {
    Self {
      stream,
      dispatcher,
      id,
    }
  }
}

impl Drop for TaskStream {
  fn drop(&mut self) {
    self.dispatcher.drop_consumer(self.id).unwrap_or_else(|_| {
      tracing::debug!("Dispatcher {} was closed earlier", self.dispatcher.id())
    });
  }
}

mod stream {
  use super::{FetchResponse, TaskStream};
  use core::task::Poll;
  use futures_util::stream::Stream;
  use std::{pin::Pin, task::Context};
  use tonic::Status;

  impl Stream for TaskStream {
    type Item = Result<FetchResponse, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
      self.stream.poll_recv(cx).map(|o| {
        o.map(|r| {
          r.map(|task| FetchResponse::from(task)).map_err(|e| {
            tracing::error!("Consumer cannot fetch task {}", e);
            Status::unavailable("Internal error")
          })
        })
      })
    }
  }
}
