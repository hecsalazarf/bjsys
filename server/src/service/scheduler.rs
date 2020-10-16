use super::store::{MultiplexedStore, RedisStorage};
use std::time::Duration;
use xactor::{Addr, Actor, Context, Error as ActorError, message, Handler};
use tracing::{debug, info};

pub struct Scheduler {
  _worker: Addr<SchedulerWorker>,
}

impl Scheduler {
  pub async fn init(store: MultiplexedStore) -> Self {

    let _worker = SchedulerWorker::new(store).start().await.expect("failed_scheduler");
    Self {
      _worker,
    }
  }
}

struct SchedulerWorker {
  store: MultiplexedStore,
}

impl SchedulerWorker {
  fn new(store: MultiplexedStore) -> Self {
    Self {
      store,
    }
  }

  async fn poll_tasks(&mut self) {
    if let Err(e) = self.store.schedule(5).await {
      info!("Failed to schedule tasks {}", e);
    }
  }
}

#[tonic::async_trait]
impl Actor for SchedulerWorker {
  async fn started(&mut self, ctx: &mut Context<Self>) -> Result<(), ActorError> {
    ctx.send_interval(PollDelayed, Duration::from_millis(1000));
    debug!("Scheduler started");
    Ok(())
  }

  async fn stopped(&mut self, _ctx: &mut Context<Self>) {
    debug!("Scheduler stopped");
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
