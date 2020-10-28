use super::dispatcher::{ActiveTasks, QueueDispatcher};
use super::store::{MultiplexedStore, RedisStorage, StoreError};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};
use xactor::{message, Actor, Addr, Context, Error as ActorError, Handler};

pub struct QueueScheduler {
  inner: Scheduler,
}

impl QueueScheduler {
  pub fn new(queue: Arc<String>, store: MultiplexedStore) -> Self {
    let instance = Scheduler::new(store, queue);
    Self { inner: instance }
  }

  pub async fn start(&mut self, dispatcher: Addr<QueueDispatcher>) -> Result<(), ActorError> {
    if let Scheduler::Inst(ref mut inst) = self.inner {
      let mut scheduler = inst.take().unwrap();
      scheduler.dispatcher.replace(dispatcher);
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
  dispatcher: Option<Addr<QueueDispatcher>>,
  queue: Arc<String>,
}

impl SchedulerWorker {
  pub fn new(store: MultiplexedStore, queue: Arc<String>) -> Self {
    Self {
      store,
      queue,
      dispatcher: None,
    }
  }

  async fn poll_delayed(&mut self) {
    if let Err(e) = self.store.schedule_delayed(5).await {
      error!(
        "Failed to schedule delayed tasks of '{}': {}",
        self.queue, e
      );
    }
  }

  async fn poll_stalled(&mut self) {
    let log_error = |queue: &str, e: StoreError| {
      error!("Failed to renqueue stalled tasks of '{}': {}", queue, e);
    };

    let pending = match self.store.read_pending(self.queue.as_ref()).await {
      Err(e) => {
        log_error(self.queue.as_ref(), e);
        Vec::new()
      }
      Ok(pending) => pending,
    };

    if pending.is_empty() {
      // No pending tasks, there's nothing to renqueue
      return;
    }

    let dispatcher = self.dispatcher.as_ref().unwrap();
    // Active-tasks call only fails when the dispatcher dropped. In such
    // case, we simply exit
    if let Ok(mut active) = dispatcher.call(ActiveTasks).await {
      let mut active = active.iter_mut();

      // Reverse iterator to insert from right to left in order to respect
      // the order in which they were added
      let stalled = pending.into_iter().rev().filter(|p| {
        // Filter pending task that are not active (stalled)
        let found = active.find(|a| {
          // We first get the reference from Weak pointer
          if let Some(id) = a.upgrade() {
            // Determine if the active ID is in the pending list
            return id.as_ref() == p;
          }
          false
        });

        // We only care of values not found, i.e, not active.
        found.is_none()
      });

      if let Err(e) = self.store.renqueue(self.queue.as_ref(), stalled).await {
        log_error(self.queue.as_ref(), e);
      }
    }
  }
}

#[tonic::async_trait]
impl Actor for SchedulerWorker {
  async fn started(&mut self, ctx: &mut Context<Self>) -> Result<(), ActorError> {
    ctx.send_interval(PollTasks::Delayed, Duration::from_secs(1));
    // TODO: The poll_stalled() has to be called at start, then at intervals
    // of the configured value
    ctx.send_interval(PollTasks::Stalled, Duration::from_secs(5));
    debug!("Scheduler '{}' started", self.queue);
    Ok(())
  }

  async fn stopped(&mut self, _ctx: &mut Context<Self>) {
    debug!("Scheduler '{}' stopped", self.queue);
  }
}

#[message]
#[derive(Clone)]
enum PollTasks {
  Delayed,
  Stalled,
}

#[tonic::async_trait]
impl Handler<PollTasks> for SchedulerWorker {
  async fn handle(&mut self, _ctx: &mut Context<Self>, kind: PollTasks) {
    match kind {
      PollTasks::Delayed => self.poll_delayed().await,
      PollTasks::Stalled => self.poll_stalled().await,
    }
  }
}
