use crate::store_lmdb::Storel;
use crate::task::{WaitingTask, Task, TaskStatus};
use std::collections::HashSet;
use std::sync::Arc;
use tonic::Status;
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context, Handler};

#[derive(Clone)]
pub struct Manager {
  actor: Addr<ManagerActor>,
}

impl Manager {
  pub async fn init(store: &Storel) -> Result<Self, Box<dyn std::error::Error>> {
    let actor = ManagerActor::new(&store).start().await?;
    Ok(Self { actor })
  }

  pub async fn ack(&self, task: Task) -> Result<(), Status> {
    self
      .actor
      .call(Acknowledge(task))
      .await
      .expect("call ack task")
  }

  pub fn in_process(&self, task_id: Uuid) -> WaitingTask {
    let waiting = WaitingTask::new(task_id, self.clone());
    self
      .actor
      .send(AckCmd::InProcess(waiting.clone()))
      .expect("call in process");
    waiting
  }

  pub fn finish(&self, uuid: Arc<Uuid>) {
    self
      .actor
      .send(AckCmd::Finish(uuid))
      .expect("call in process");
  }
}

#[message]
pub enum AckCmd {
  InProcess(WaitingTask),
  Finish(Arc<Uuid>),
}

#[message(result = "Result<(), Status>")]
struct Acknowledge(Task);

struct ManagerActor {
  tasks: HashSet<WaitingTask>,
  store: Storel,
}

impl ManagerActor {
  pub fn new(store: &Storel) -> Self {
    Self {
      tasks: HashSet::new(),
      store: store.clone(),
    }
  }

  fn in_process(&mut self, task: WaitingTask) {
    self.tasks.insert(task);
  }

  fn remove_active(&mut self, id: Arc<Uuid>) {
    self.tasks.remove(id.as_ref());
  }

  async fn ack(&mut self, task: Task) -> Result<(), Status> {
    let id = *task.id();
    let status = task.status();
  
    let res = match status {
      TaskStatus::Done => self.store.finish(task).await,
      TaskStatus::Failed => self.store.retry(task).await,
      TaskStatus::Canceled => self.store.finish(task).await,
    };

    match res {
      Ok(r) => {
        if r {
          tracing::debug!(
            "Task {} reported with status {:?}",
            id.to_simple(),
            status
          );
          if let Some(t) = self.tasks.take(&id) {
            t.acked();
          }
        }
        Ok(())
      }
      Err(e) => {
        tracing::error!("Cannot report task {}: {}", id, e);
        Err(Status::unavailable("Service not available"))
      }
    }
  }
}

impl Actor for ManagerActor {}

#[tonic::async_trait]
impl Handler<AckCmd> for ManagerActor {
  async fn handle(&mut self, _ctx: &mut Context<Self>, cmd: AckCmd) {
    match cmd {
      AckCmd::InProcess(t) => self.in_process(t),
      AckCmd::Finish(id) => self.remove_active(id),
    }
  }
}

#[tonic::async_trait]
impl Handler<Acknowledge> for ManagerActor {
  async fn handle(&mut self, _ctx: &mut Context<Self>, req: Acknowledge) -> Result<(), Status> {
    let task = req.0; // Unwrap request
    if self.tasks.contains(task.id()) {
      // Report task only if it's pending
      self.ack(task).await
    } else {
      Err(Status::invalid_argument("Task is not pending"))
    }
  }
}
