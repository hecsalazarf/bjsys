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

  pub async fn check(&self, req: &AcknowledgeRequest) -> Result<(), Status> {
    self
      .store
      .ack(&req.task_id, &req.queue)
      .await
      .map(|r| {
        if r > 0 {
          info!("Task {} was completed", req.task_id);
          self
            .worker
            .send(AckCmd::Done(req.task_id.clone()))
            .expect("ack_done");
        }
      })
      .map_err(|e| {
        error!("Cannot ack task {}: {}", req.task_id, e);
        Status::unavailable("Service not available")
      })
  }

  pub fn in_process(&self, task_id: String) -> Arc<Notify> {
    let notify = Arc::new(Notify::new());
    self
      .worker
      .send(AckCmd::InProcess(task_id, notify.clone()))
      .expect("ack_in_process");
    notify
  }
}

#[message]
enum AckCmd {
  Done(String),
  InProcess(String, Arc<Notify>),
}

struct AckWorker {
  tasks: HashMap<String, Arc<Notify>>,
}

impl AckWorker {
  pub fn new() -> Self {
    Self {
      tasks: HashMap::new(),
    }
  }

  fn in_process(&mut self, task_id: String, notify: Arc<Notify>) {
    self.tasks.insert(task_id, notify);
  }

  fn mark_done(&mut self, task_id: &str) {
    if let Some(n) = self.tasks.remove(task_id) {
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
      AckCmd::InProcess(t, n) => self.in_process(t, n),
      AckCmd::Done(t) => self.mark_done(&t),
    }
  }
}
