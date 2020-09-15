use super::store::{Storage, Store};
use super::stub::tasks::AcknowledgeRequest;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;
use tonic::Status;
use tracing::{error, info};
use xactor::{message, Actor, Addr, Context, Handler};

#[derive(Clone)]
pub struct AckManager {
  store: Store,
  worker: Addr<AckWorker>,
}

impl AckManager {
  pub async fn init(store: Store) -> Result<Self, Box<dyn std::error::Error>> {
    let worker = AckWorker::new().start().await?;
    Ok(Self { store, worker })
  }

  pub async fn check(&self, req: AcknowledgeRequest) -> Result<(), Status> {
    match self.store.ack(&req.task_id, &req.queue).await {
      Ok(r) => {
        if r > 0 {
          info!("Task {} was completed", req.task_id);
          self
            .worker
            .send(AckCmd::Done(Arc::new(req.task_id)))
            .expect("ack_done");
        }
        Ok(())
      }
      Err(e) => {
        error!("Cannot ack task {}: {}", req.task_id, e);
        Err(Status::unavailable("Service not available"))
      }
    }
  }

  pub fn in_process(&self, task_id: String) -> WaitingTask {
    let waiting = WaitingTask::new(task_id);
    self
      .worker
      .send(AckCmd::InProcess(waiting.clone()))
      .expect("ack_in_process");
    waiting
  }
}

#[message]
enum AckCmd {
  Done(Arc<String>),
  InProcess(WaitingTask),
}

struct AckWorker {
  tasks: HashMap<Arc<String>, Arc<Notify>>,
}

impl AckWorker {
  pub fn new() -> Self {
    Self {
      tasks: HashMap::new(),
    }
  }

  fn in_process(&mut self, task: WaitingTask) {
    self.tasks.insert(task.id, task.notify);
  }

  fn mark_done(&mut self, task_id: Arc<String>) {
    if let Some(n) = self.tasks.remove(&task_id) {
      info!("Notify");
      n.notify();
    }
  }
}

impl Actor for AckWorker {}

#[tonic::async_trait]
impl Handler<AckCmd> for AckWorker {
  async fn handle(&mut self, _ctx: &mut Context<Self>, cmd: AckCmd) {
    match cmd {
      AckCmd::InProcess(t) => self.in_process(t),
      AckCmd::Done(t) => self.mark_done(t),
    }
  }
}

#[derive(Clone, Debug)]
pub struct WaitingTask {
  id: Arc<String>,
  notify: Arc<Notify>,
}

impl WaitingTask {
  pub fn new(id: String) -> Self {
    Self {
      id: Arc::new(id),
      notify: Arc::new(Notify::new()),
    }
  }

  pub async fn wait_to_finish(&self) {
    self.notify.notified().await;
  }

  pub fn finish(&self) {
    self.notify.notify();
  }
}
