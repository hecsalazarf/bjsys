use super::store::{MultiplexedStore, RedisStorage};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};
use xactor::{message, Actor, Addr, Context, Error as ActorError, Handler};

pub struct QueueScheduler {
  inner: Scheduler,
}

impl QueueScheduler {
  pub fn new(queue: Arc<String>, store: MultiplexedStore) -> Self {
    let instance = Scheduler::new(store, queue);
    Self { inner: instance }
  }

  pub async fn start(&mut self) -> Result<(), ActorError> {
    if let Scheduler::Inst(ref mut inst) = self.inner {
      let scheduler = inst.take().unwrap();
      let addr = scheduler.start().await?;
      self.inner = Scheduler::Started(addr);
    }
    Ok(())
  }

  pub fn stop(&mut self) -> Result<(), ActorError> {
    if let Scheduler::Started(ref mut addr) = self.inner {
      addr.stop(None)?;
    }
    Ok(())
  }
}

enum Scheduler {
  Inst(Option<SchedulerWorker>),
  Started(Addr<SchedulerWorker>),
}

impl Scheduler {
  fn new(store: MultiplexedStore, queue: Arc<String>) -> Self {
    Scheduler::Inst(Some(SchedulerWorker::new(store, queue)))
  }
}

pub struct SchedulerWorker {
  store: MultiplexedStore,
  queue: Arc<String>,
}

impl SchedulerWorker {
  pub fn new(store: MultiplexedStore, queue: Arc<String>) -> Self {
    Self { store, queue }
  }

  async fn poll_tasks(&mut self) {
    if let Err(e) = self.store.schedule_delayed(5).await {
      info!("Failed to schedule tasks {}", e);
    }
  }
}

#[tonic::async_trait]
impl Actor for SchedulerWorker {
  async fn started(&mut self, ctx: &mut Context<Self>) -> Result<(), ActorError> {
    ctx.send_interval(PollDelayed, Duration::from_millis(1000));
    debug!("Scheduler '{}' started", self.queue);
    Ok(())
  }

  async fn stopped(&mut self, _ctx: &mut Context<Self>) {
    debug!("Scheduler '{}' stopped", self.queue);
  }
}

#[message]
#[derive(Clone)]
struct PollDelayed;

#[tonic::async_trait]
impl Handler<PollDelayed> for SchedulerWorker {
  async fn handle(&mut self, _ctx: &mut Context<Self>, _: PollDelayed) {
    self.poll_tasks().await
  }
}
