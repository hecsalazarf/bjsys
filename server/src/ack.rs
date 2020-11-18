use crate::store::{MultiplexedStore, RedisStorage};
use crate::stub::tasks::{AckRequest, TaskStatus};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Weak};
use tokio::sync::Notify;
use tonic::Status;
use tracing::{debug, error};
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

  pub async fn check(&self, req: AckRequest) -> Result<(), Status> {
    self
      .worker
      .call(Acknowledge(req))
      .await
      .expect("cannot_check")
  }

  pub fn in_process(&self, task_id: String) -> WaitingTask {
    let waiting = WaitingTask::new(task_id, self.worker.clone());
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
  Remove(Arc<String>),
}

#[message(result = "Result<(), Status>")]
struct Acknowledge(AckRequest);

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

  fn remove_active(&mut self, id: Arc<String>) {
    self.tasks.remove(&id);
  }

  async fn report(&mut self, request: AckRequest) -> Result<(), Status> {
    // Never fails as it's previously validated
    let status = TaskStatus::from_i32(request.status).unwrap();

    let res = match status {
      TaskStatus::Done => self.store.finish(&request).await,
      TaskStatus::Failed => self.store.fail(&request).await,
      TaskStatus::Canceled => self.store.finish(&request).await,
    };

    match res {
      Ok(r) => {
        if r > 0 {
          debug!(
            "Task {} reported with status {}",
            request.task_id, request.status
          );
          if let Some(n) = self.tasks.remove(&request.task_id) {
            n.notify();
          }
        }
        Ok(())
      }
      Err(e) => {
        error!("Cannot report task {}: {}", request.task_id, e);
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
      AckCmd::Remove(id) => self.remove_active(id),
    }
  }
}

#[tonic::async_trait]
impl Handler<Acknowledge> for AckWorker {
  async fn handle(&mut self, _ctx: &mut Context<Self>, req: Acknowledge) -> Result<(), Status> {
    let req = req.0; // Unwrap request
    if self.tasks.contains_key(&req.task_id) {
      // Report task only if it's pending
      self.report(req).await
    } else {
      Err(Status::invalid_argument("Task is not pending"))
    }
  }
}

#[derive(Clone)]
pub struct WaitingTask {
  id: Arc<String>,
  notify: Arc<Notify>,
  worker: Addr<AckWorker>,
}

impl WaitingTask {
  fn new(id: String, worker: Addr<AckWorker>) -> Self {
    Self {
      id: Arc::new(id),
      notify: Arc::new(Notify::new()),
      worker,
    }
  }

  pub fn id(&self) -> Weak<String> {
    Arc::downgrade(&self.id)
  }

  pub async fn wait_to_finish(&self) {
    self.notify.notified().await;
  }

  pub fn finish(self) {
    self.notify.notify();
    self
      .worker
      .send(AckCmd::Remove(self.id))
      .expect("fail_to_finish");
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