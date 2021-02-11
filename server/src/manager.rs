use crate::repository::{RepoError, Repository};
use crate::task::{InProcessTask, Task, TaskId, TaskStatus};
use std::collections::HashSet;
use std::sync::Weak;
use xactor::{message, Actor, Addr, Context, Handler};

#[derive(Clone)]
pub struct Manager {
  actor: Addr<ManagerActor>,
}

impl Manager {
  pub async fn init(repo: &Repository) -> anyhow::Result<Self> {
    let actor = ManagerActor::new(&repo).start().await?;
    Ok(Self { actor })
  }

  pub async fn ack(&self, task: Task) -> Result<(), RepoError> {
    self
      .actor
      .call(Acknowledge(task))
      .await
      .expect("call ack task")
  }

  pub fn in_process(&self, task_id: TaskId) -> InProcessTask {
    let waiting = InProcessTask::new(task_id);
    self
      .actor
      .send(AckCmd::InProcess(waiting.clone()))
      .expect("call in process");
    waiting
  }

  pub fn finish(&self, uuid: Weak<TaskId>) {
    self
      .actor
      .send(AckCmd::Finish(uuid))
      .expect("call in process");
  }
}

#[message]
pub enum AckCmd {
  InProcess(InProcessTask),
  Finish(Weak<TaskId>),
}

#[message(result = "Result<(), RepoError>")]
struct Acknowledge(Task);

struct ManagerActor {
  tasks: HashSet<InProcessTask>,
  repo: Repository,
}

impl ManagerActor {
  pub fn new(repo: &Repository) -> Self {
    Self {
      tasks: HashSet::new(),
      repo: repo.clone(),
    }
  }

  fn in_process(&mut self, task: InProcessTask) {
    self.tasks.insert(task);
  }

  fn finish(&mut self, id: Weak<TaskId>) {
    if let Some(id) = id.upgrade() {
      if let Some(task) = self.tasks.take(id.as_ref()) {
        task.ack();
      }
    }
  }

  async fn ack(&mut self, task: Task) -> Result<(), RepoError> {
    let id = *task.id();
    let status = task.status();
    let res = match status {
      TaskStatus::Done => self.repo.finish(task).await,
      TaskStatus::Failed => self.repo.retry(task).await,
      TaskStatus::Canceled => self.repo.finish(task).await,
    };

    if let Err(e) = res {
      tracing::error!("Cannot report task {}: {}", id, e);
    } else {
      if let Some(t) = self.tasks.take(&id) {
        t.ack();
      }
      tracing::debug!("Task {} reported with status {:?}", id.to_simple(), status);
    }

    res
  }
}

impl Actor for ManagerActor {}

#[tonic::async_trait]
impl Handler<AckCmd> for ManagerActor {
  async fn handle(&mut self, _ctx: &mut Context<Self>, cmd: AckCmd) {
    match cmd {
      AckCmd::InProcess(t) => self.in_process(t),
      AckCmd::Finish(id) => self.finish(id),
    }
  }
}

#[tonic::async_trait]
impl Handler<Acknowledge> for ManagerActor {
  async fn handle(&mut self, _ctx: &mut Context<Self>, req: Acknowledge) -> Result<(), RepoError> {
    let task = req.0;
    if self.tasks.contains(task.id()) {
      // Report task only if it's pending
      self.ack(task).await
    } else {
      Err(RepoError::NotFound)
    }
  }
}
