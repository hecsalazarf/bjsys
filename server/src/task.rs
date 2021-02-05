use crate::dispatcher::Dispatcher;
use crate::manager::Manager;
use core::task::Poll;
use futures_util::stream::Stream;
use proto::FetchResponse;
use std::borrow::Borrow;
use std::{
  pin::Pin,
  sync::{Arc, Weak},
  task::Context,
};
use tokio::sync::{mpsc, Notify};
use tonic::Status;
use uuid::Uuid;

pub struct TaskStream {
  stream: mpsc::Receiver<Result<FetchResponse, Status>>,
  dispatcher: Dispatcher,
  id: u64,
}

impl TaskStream {
  pub fn new(
    stream: mpsc::Receiver<Result<FetchResponse, Status>>,
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

impl Stream for TaskStream {
  type Item = Result<FetchResponse, Status>;
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
    self.stream.poll_recv(cx)
  }
}

impl Drop for TaskStream {
  fn drop(&mut self) {
    self.dispatcher.drop_consumer(self.id).unwrap_or_else(|_| {
      tracing::debug!("Dispatcher {} was closed earlier", self.dispatcher.id())
    });
  }
}

#[derive(Clone)]
pub struct WaitingTask {
  id: Arc<Uuid>,
  notify: Arc<Notify>,
  manager: Manager,
}

impl WaitingTask {
  pub fn new(id: Uuid, manager: Manager) -> Self {
    Self {
      id: Arc::new(id),
      notify: Arc::new(Notify::new()),
      manager,
    }
  }

  pub fn id(&self) -> Weak<Uuid> {
    Arc::downgrade(&self.id)
  }

  pub async fn wait_to_finish(&self) {
    self.notify.notified().await;
  }

  pub fn finish(self) {
    self.notify.notify_one();
    self.manager.finish(self.id);
  }

  pub fn acked(&self) {
    self.notify.notify_one();
  }
}

impl std::hash::Hash for WaitingTask {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.id.hash(state);
  }
}

impl PartialEq for WaitingTask {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl Eq for WaitingTask {}

impl Borrow<Uuid> for WaitingTask {
  fn borrow(&self) -> &Uuid {
    self.id.as_ref()
  }
}
