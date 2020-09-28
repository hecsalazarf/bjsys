use super::store::{RedisDriver, MultiplexedStore};
use super::stub::tasks::AcknowledgeRequest;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::Notify;
use tonic::Status;
use tracing::{error, info};
use xactor::{message, Actor, Addr, Context, Handler};

#[derive(Clone)]
pub struct AckManager {
  worker: Addr<AckWorker>,
}

impl AckManager {
  pub async fn init(store: MultiplexedStore) -> Result<Self, Box<dyn std::error::Error>> {
    let worker = AckWorker::new(store).start().await?;
    Ok(Self { worker })
  }

  pub async fn check(&self, req: AcknowledgeRequest) -> Result<(), Status> {
    self
      .worker
      .call(Acknowledge(req))
      .await
      .expect("cannot_check")
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
  InProcess(WaitingTask),
}

#[message(result = "Result<(), Status>")]
struct Acknowledge(AcknowledgeRequest);

struct AckWorker {
  tasks: HashMap<Arc<String>, Arc<Notify>>,
  store: MultiplexedStore,
}

impl AckWorker {
  pub fn new(store: MultiplexedStore) -> Self {
    Self {
      tasks: HashMap::new(),
      store,
    }
  }

  fn in_process(&mut self, task: WaitingTask) {
    self.tasks.insert(task.id, task.notify);
  }

  async fn mark_done(&mut self, req: AcknowledgeRequest) -> Result<(), Status> {
    match self.store.ack(&req.task_id, &req.queue).await {
      Ok(r) => {
        if r > 0 {
          info!("Task {} was completed", req.task_id);
          if let Some(n) = self.tasks.remove(&Arc::new(req.task_id)) {
            n.notify();
          }
        }
        Ok(())
      }
      Err(e) => {
        error!("Cannot ack task {}: {}", req.task_id, e);
        Err(Status::unavailable("Service not available"))
      }
    }
  }
}

impl Actor for AckWorker {}

#[tonic::async_trait]
impl Handler<AckCmd> for AckWorker {
  async fn handle(&mut self, _ctx: &mut Context<Self>, cmd: AckCmd) {
    match cmd {
      AckCmd::InProcess(t) => self.in_process(t),
    }
  }
}

#[tonic::async_trait]
impl Handler<Acknowledge> for AckWorker {
  async fn handle(&mut self, _ctx: &mut Context<Self>, ack: Acknowledge) -> Result<(), Status> {
    self.mark_done(ack.0).await
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

impl Hash for WaitingTask {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.id.hash(state);
  }
}

impl PartialEq for WaitingTask {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl Eq for WaitingTask {}
