use crate::store_lmdb::Storel;
use crate::task::WaitingTask;
use proto::{AckRequest, TaskStatus};
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

  pub async fn ack(&self, req: AckRequest) -> Result<(), Status> {
    self
      .actor
      .call(Acknowledge(req))
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
struct Acknowledge(AckRequest);

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

  async fn ack(&mut self, request: AckRequest) -> Result<(), Status> {
    // Never fails as it's previously validated
    let status = TaskStatus::from_i32(request.status).unwrap();

    // TODO: DO NOT CLONE, ONLY FOR MIGRATION TO LMDB
    let reqd = request.clone();
    let res = match status {
      TaskStatus::Done => self.store.finish(reqd).await,
      TaskStatus::Failed => self.store.fail(reqd).await,
      TaskStatus::Canceled => self.store.finish(reqd).await,
    };

    match res {
      Ok(r) => {
        if r {
          tracing::debug!(
            "Task {} reported with status {}",
            request.task_id,
            request.status
          );
          // TODO: DO NOT CREATE UUID IN HERE
          let uuid = Uuid::parse_str(&request.task_id).expect("uuid from str");
          if let Some(t) = self.tasks.take(&uuid) {
            t.acked();
          }
        }
        Ok(())
      }
      Err(e) => {
        tracing::error!("Cannot report task {}: {}", request.task_id, e);
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
    let req = req.0; // Unwrap request
                     // TODO: DO NOT CREATE UUID IN HERE
    let uuid = Uuid::parse_str(&req.task_id).expect("uuid from str");
    if self.tasks.contains(&uuid) {
      // Report task only if it's pending
      self.ack(req).await
    } else {
      Err(Status::invalid_argument("Task is not pending"))
    }
  }
}
