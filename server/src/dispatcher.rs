use crate::manager::Manager;
use crate::scheduler::Scheduler;
use crate::service::ServiceCmd;
use crate::store_lmdb::Storel;
use crate::task::{TaskStream, WaitingTask, Task};
use embed::Error as StoreError;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, Notify, Semaphore};
use tonic::Status;
use tracing::{debug, error};
use uuid::Uuid;
use xactor::{message, Actor, Addr, Context as ActorContext, Error as ActorError, Handler};

pub struct MasterDispatcher {
  addr: Addr<MasterWorker>,
}

impl MasterDispatcher {
  pub async fn init(store: &Storel, manager: &Manager) -> Self {
    let worker = MasterWorker {
      dispatchers: HashMap::new(),
      store: store.clone(),
      manager: manager.clone(),
    };

    let addr = worker.start().await.expect("start worker");

    Self { addr }
  }

  pub async fn produce(&self, queue: String) -> Result<TaskStream, Status> {
    let stream = self
      .addr
      .call(QueueName(queue))
      .await
      .unwrap()
      .map_err(|_e| Status::unavailable(""))?;

    Ok(stream)
  }
}

#[derive(Clone)]
struct DispatcherValue {
  dispatcher: Dispatcher,
  fetcher: Fetcher,
}

type DispatcherRecord = (Arc<String>, DispatcherValue);
type Registration = Result<DispatcherRecord, ActorError>;

struct MasterWorker {
  dispatchers: HashMap<Arc<String>, DispatcherValue>,
  store: Storel,
  manager: Manager,
}

impl MasterWorker {
  async fn register(&mut self, addr: Addr<Self>, queue: String) -> Registration {
    let queue = Arc::new(queue);
    let fetcher = Fetcher::start(queue.clone(), self.store.clone()).await?;
    let dispatcher = Dispatcher::start(queue.clone(), addr, self.store.clone()).await?;

    let record = DispatcherValue {
      dispatcher,
      fetcher,
    };
    self.dispatchers.insert(queue.clone(), record.clone());
    Ok((queue, record))
  }

  async fn start_stream(&self, record: DispatcherRecord) -> Result<TaskStream, ActorError> {
    let (tx, rx) = mpsc::channel(1);

    let exit = Arc::new(Notify::new());
    let consumer = Consumer {
      manager: self.manager.clone(),
      queue: record.0,
      fetcher: record.1.fetcher,
      dispatcher: record.1.dispatcher,
      exit: exit.clone(),
      tx,
    };

    let dispatcher = consumer.dispatcher.clone();
    let worker_addr = consumer.start().await?;
    let id = worker_addr.actor_id();

    worker_addr
      .send(ConsumerCmd::Fetch)
      .expect("start fetching");

    let cv = ConsumerValue {
      _addr: worker_addr,
      pending_task: None,
      exit,
    };
    dispatcher.init_consumer(id, cv).await?;

    Ok(TaskStream::new(rx, dispatcher, id))
  }

  async fn unregister(&mut self, queue: Arc<String>) {
    if let Some(mut record) = self.dispatchers.remove(queue.as_ref()) {
      record.fetcher.stop().expect("stop fetcher");
    }
  }
}

#[message(result = "Result<TaskStream, ActorError>")]
struct QueueName(String);

#[message]
enum MasterCmd {
  Unregister(Arc<String>),
}

impl Actor for MasterWorker {}

#[tonic::async_trait]
impl Handler<QueueName> for MasterWorker {
  async fn handle(
    &mut self,
    ctx: &mut ActorContext<Self>,
    queue: QueueName,
  ) -> Result<TaskStream, ActorError> {
    let queue = queue.0;

    let record = if let Some(r) = self.dispatchers.get_key_value(&queue) {
      (r.0.clone(), r.1.clone())
    } else {
      self.register(ctx.address(), queue).await?
    };

    let stream = self.start_stream(record).await?;
    Ok(stream)
  }
}

#[tonic::async_trait]
impl Handler<MasterCmd> for MasterWorker {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, cmd: MasterCmd) {
    match cmd {
      MasterCmd::Unregister(queue) => self.unregister(queue).await,
    };
  }
}

#[derive(Clone)]
pub struct Dispatcher {
  /// Actor address
  addr: Addr<DispatcherActor>,
}

impl Dispatcher {
  async fn start(
    queue: Arc<String>,
    master: Addr<MasterWorker>,
    store: Storel,
  ) -> xactor::Result<Self> {
    let actor = DispatcherActor::new(queue, master, store);
    let addr = actor.start().await?;
    Ok(Self { addr })
  }

  pub fn id(&self) -> u64 {
    self.addr.actor_id()
  }

  pub fn drop_consumer(&self, consumer: u64) -> xactor::Result<()> {
    self.addr.send(DispatcherCmd::Drop(consumer))
  }

  pub async fn init_consumer(&self, consumer: u64, val: ConsumerValue) -> xactor::Result<()> {
    self.addr.call(DispatcherCmd::Init(consumer, val)).await
  }

  pub async fn attach_to(&self, consumer: u64, task: WaitingTask) -> xactor::Result<()> {
    self.addr.call(DispatcherCmd::Attach(consumer, task)).await
  }

  pub async fn detach_from(&self, consumer: u64) -> xactor::Result<()> {
    self.addr.call(DispatcherCmd::Detach(consumer)).await
  }

  pub async fn active_tasks(&self) -> xactor::Result<Vec<Weak<Uuid>>> {
    self.addr.call(ActiveTasks).await
  }
}

struct DispatcherActor {
  consumers: HashMap<u64, ConsumerValue>,
  master: Addr<MasterWorker>,
  queue: Arc<String>,
  scheduler: Scheduler,
}

impl DispatcherActor {
  fn new(queue: Arc<String>, master: Addr<MasterWorker>, store: Storel) -> Self {
    let consumers = HashMap::new();
    let scheduler = Scheduler::new(queue.clone(), store);
    Self {
      queue,
      master,
      consumers,
      scheduler,
    }
  }

  fn remove_consumer(&mut self, id: u64) -> usize {
    // Never fails as it was previously created
    let cv = self.consumers.remove(&id).unwrap();
    Self::stop_consumer(cv);
    self.consumers.len()
  }

  fn remove_all_consumers(&mut self) {
    let all = self.consumers.drain();
    for (_, cv) in all {
      Self::stop_consumer(cv);
    }
  }

  fn stop_consumer(cv: ConsumerValue) {
    if let Some(task) = cv.pending_task {
      task.finish();
    }
    cv.exit.notify_one();
  }

  fn attach_to(&mut self, id: u64, task: WaitingTask) {
    if let Some(consumer) = self.consumers.get_mut(&id) {
      consumer.pending_task = Some(task);
    }
  }

  fn detach_from(&mut self, id: u64) {
    if let Some(consumer) = self.consumers.get_mut(&id) {
      consumer.pending_task.take();
    }
  }

  fn active_tasks(&mut self) -> Vec<Weak<Uuid>> {
    let map = self.consumers.iter().filter_map(|(_, value)| {
      // Map all active tasks being processed, returning its ID
      value.pending_task.as_ref().map(|task| task.id())
    });

    map.collect()
  }
}

#[tonic::async_trait]
impl Actor for DispatcherActor {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    self.remove_all_consumers();
    // Inform master to drop this address
    self
      .master
      .call(MasterCmd::Unregister(self.queue.clone()))
      .await
      .unwrap();
    self.scheduler.stop().expect("stop scheduler");
    debug!("Dispatcher for '{}' stopped", self.queue);
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    ctx
      .subscribe::<ServiceCmd>()
      .await
      .expect("subscribe to shutdown");

    self
      .scheduler
      .start(Dispatcher {
        addr: ctx.address(),
      })
      .await?;
    debug!("Dispatcher for '{}' started", self.queue);
    Ok(())
  }
}

#[message]
enum DispatcherCmd {
  Attach(u64, WaitingTask),
  Detach(u64),
  Init(u64, ConsumerValue),
  Drop(u64),
}

#[message(result = "Vec<Weak<Uuid>>")]
struct ActiveTasks;

#[tonic::async_trait]
impl Handler<DispatcherCmd> for DispatcherActor {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: DispatcherCmd) {
    match cmd {
      DispatcherCmd::Attach(id, t) => {
        self.attach_to(id, t);
      }
      DispatcherCmd::Detach(id) => {
        self.detach_from(id);
      }
      DispatcherCmd::Init(id, addr) => {
        self.consumers.insert(id, addr);
      }
      DispatcherCmd::Drop(id) => {
        if self.remove_consumer(id) == 0 {
          ctx.stop(None);
        }
      }
    };
  }
}

#[tonic::async_trait]
impl Handler<ActiveTasks> for DispatcherActor {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, _: ActiveTasks) -> Vec<Weak<Uuid>> {
    self.active_tasks()
  }
}

#[tonic::async_trait]
impl Handler<ServiceCmd> for DispatcherActor {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: ServiceCmd) {
    match cmd {
      ServiceCmd::Shutdown => {
        ctx.stop(None);
      }
    }
  }
}

/// An actor that provides the `FetcherHandle`. There is a running actor per
/// queue being fetched.
#[derive(Clone)]
pub struct Fetcher {
  /// Actor address
  addr: Addr<FetcherActor>,
}

impl Fetcher {
  /// Creates a new `FetcherActor` for `queue`.
  pub async fn start(queue: Arc<String>, store: Storel) -> xactor::Result<Self> {
    let actor = FetcherActor::new(queue, store);
    let addr = actor.start().await?;
    Ok(Self { addr })
  }

  /// Stop actor.
  pub fn stop(&mut self) -> xactor::Result<()> {
    self.addr.stop(None)
  }

  /// Returns a `FetcherHandle` to fetch tasks.
  pub async fn get_handle(&mut self) -> xactor::Result<FetcherHandle> {
    self.addr.call(GetHandle).await
  }
}

/// Inner implementation of `Fetcher`
struct FetcherActor {
  /// Queue name.
  queue: Arc<String>,
  /// Storage DB.
  store: Storel,
  /// Semaphore that controls access to the the store.
  semaph: Arc<Semaphore>,
}

impl FetcherActor {
  fn new(queue: Arc<String>, store: Storel) -> Self {
    let semaph = Arc::new(Semaphore::new(1));
    Self {
      queue,
      store,
      semaph,
    }
  }

  async fn get_handle(&self) -> FetcherHandle {
    FetcherHandle {
      semaph: self.semaph.clone(),
      store: self.store.clone(),
      queue: self.queue.clone(),
    }
  }
}

/// Handle that limits the access to the storage in order to retrieve tasks
/// in sequence. This guarantees that consumers fetch tasks in the order they
/// arrive.
pub struct FetcherHandle {
  /// Semaphore that controls access to the the store.
  semaph: Arc<Semaphore>,
  /// Underlying storage DB
  store: Storel,
  /// Queue name
  queue: Arc<String>,
}

impl FetcherHandle {
  /// Fetch tasks from storage. If there is a running handle, this method
  /// blocks until the precedent handle completed.
  pub async fn fetch(&self) -> Result<Task, StoreError> {
    let _permit = self.semaph.acquire().await.expect("acquire fetch");
    self.store.find_new(self.queue.as_ref()).await
  }
}

#[tonic::async_trait]
impl Actor for FetcherActor {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    debug!("Fetcher for '{}' stopped", self.queue);
  }

  async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    debug!("Fetcher for '{}' started", self.queue);
    Ok(())
  }
}

/// Message used by the `FetcherActor` actor.
#[message(result = "FetcherHandle")]
struct GetHandle;

#[tonic::async_trait]
impl Handler<GetHandle> for FetcherActor {
  async fn handle(&mut self, _: &mut ActorContext<Self>, _: GetHandle) -> FetcherHandle {
    self.get_handle().await
  }
}

/// Message used by the `Consumer` actor.
#[message]
enum ConsumerCmd {
  /// Fetch tasks from storage.
  Fetch,
}

/// Consumer value for the `DispatcherActor`'s consumers dictionary.
pub struct ConsumerValue {
  /// Actor address of `Consumer`.
  _addr: Addr<Consumer>,
  /// Indicates that a pending task is in progress.
  pending_task: Option<WaitingTask>,
  /// Exit signal
  exit: Arc<Notify>,
}

/// A consumer is an actor that abtracts an incoming client connection, fetching
/// waiting tasks.
pub struct Consumer {
  manager: Manager,
  tx: mpsc::Sender<Result<Task, Status>>,
  dispatcher: Dispatcher,
  fetcher: Fetcher,
  queue: Arc<String>,
  exit: Arc<Notify>,
}

impl Consumer {
  async fn respond(&mut self, task: Task, id: u64) {
    let current_task = self.manager.in_process(*task.id());

    // Call to master may fail when its address has been dropped,
    // which it's posible only when the dispatcher received the stop
    // command. So we don't care for error because Dispatch Worker is
    // dropped as well.
    self
      .dispatcher
      .attach_to(id, current_task.clone())
      .await
      .unwrap_or(());
    self.tx.send(Ok(task)).await.expect("send incoming task");
    current_task.wait_to_finish().await;
    self.dispatcher.detach_from(id).await.unwrap_or(());
  }

  /// Fetch incoming tasks from storage; `id` is the identifier of `Consumer`.
  pub async fn fetch(&mut self, id: u64) {
    loop {
      let res = self.fetcher.get_handle().await;
      if res.is_err() {
        // The result may only fail when fetcher has dropped. In such case, we
        // break the loop.
        break;
      }
      let handler = res.unwrap();
      tokio::select! {
        // Wait for the first future to complete, either new task or exit signal
        res = handler.fetch() => {
          match res {
            Err(e) => {
              // On error, send error status to client
              error!("Cannot read task {}", e);
              self
                .tx
                .send(Err(Status::unavailable("unavailable")))
                .await
                .expect("send task");
            }
            Ok(t) => {
              // Send the new task to the client
              self.respond(t, id).await;
            }
          }
        }
        _ = self.exit.notified() => {
          // Exit signal is triggered when client connection has been dropped or
          // the system sent a "ctrl-c" notification to the process
          break;
        }
      };
    }
  }
}

#[tonic::async_trait]
impl Actor for Consumer {
  async fn stopped(&mut self, ctx: &mut ActorContext<Self>) {
    debug!(
      "Consumer [{}] disconnected from '{}'",
      ctx.actor_id(),
      self.queue
    );
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    debug!(
      "Consumer [{}] connected to '{}'",
      ctx.actor_id(),
      self.queue
    );
    Ok(())
  }
}

#[tonic::async_trait]
impl Handler<ConsumerCmd> for Consumer {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: ConsumerCmd) {
    match cmd {
      ConsumerCmd::Fetch => self.fetch(ctx.actor_id()).await,
    }
  }
}
