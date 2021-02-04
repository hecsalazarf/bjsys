use crate::manager::Manager;
use crate::scheduler::Scheduler;
use crate::service::ServiceCmd;
use crate::store_lmdb::Storel;
use crate::task::{TaskStream, WaitingTask};
use embed::Error as StoreError;
use proto::FetchResponse;
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
  dispatcher: Addr<Dispatcher>,
  fetcher: Addr<Fetcher>,
}

type DispatcherRecord = (Arc<String>, DispatcherValue);
type Registration = Result<DispatcherRecord, ActorError>;

pub struct MasterWorker {
  dispatchers: HashMap<Arc<String>, DispatcherValue>,
  store: Storel,
  manager: Manager,
}

impl MasterWorker {
  async fn register(&mut self, addr: Addr<Self>, queue: String) -> Registration {
    let queue = Arc::new(queue);
    let fetcher = Fetcher::new(queue.clone()).await.expect("connect to store");

    let reader_addr = fetcher.start().await?;
    let dispatcher = Dispatcher::new(queue.clone(), addr, self.store.clone());
    let dispatcher_addr = dispatcher.start().await?;
    let record = DispatcherValue {
      dispatcher: dispatcher_addr,
      fetcher: reader_addr,
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
    dispatcher.call(DispatcherCmd::Init(id, cv)).await?;

    Ok(TaskStream::new(rx, dispatcher, id))
  }

  async fn unregister(&mut self, queue: Arc<String>) {
    if let Some(mut record) = self.dispatchers.remove(queue.as_ref()) {
      record.fetcher.stop(None).expect("stop fetcher");
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

pub struct Dispatcher {
  consumers: HashMap<u64, ConsumerValue>,
  master: Addr<MasterWorker>,
  queue: Arc<String>,
  scheduler: Scheduler,
}

impl Dispatcher {
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

  fn add_pending(&mut self, id: u64, task: WaitingTask) {
    if let Some(consumer) = self.consumers.get_mut(&id) {
      consumer.pending_task = Some(task);
    }
  }

  fn remove_pending(&mut self, id: u64) {
    if let Some(consumer) = self.consumers.get_mut(&id) {
      consumer.pending_task.take();
    }
  }

  fn get_active(&mut self) -> Vec<Weak<Uuid>> {
    let map = self.consumers.iter().filter_map(|(_, value)| {
      // Map all active tasks being processed, returning its ID
      value.pending_task.as_ref().map(|task| task.id())
    });

    map.collect()
  }
}

#[tonic::async_trait]
impl Actor for Dispatcher {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    self.remove_all_consumers();
    // Inform master to drop this address
    self
      .master
      .call(MasterCmd::Unregister(self.queue.clone()))
      .await
      .unwrap();
    self.scheduler.stop().expect("stop scheduler");
    debug!("Queue '{}' is no longer being served", self.queue);
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    ctx
      .subscribe::<ServiceCmd>()
      .await
      .expect("subscribe to shutdown");

    self.scheduler.start(ctx.address()).await?;
    debug!("Queue '{}' is being dispatched", self.queue);
    Ok(())
  }
}

#[message]
pub enum DispatcherCmd {
  Add(u64, WaitingTask),
  Remove(u64),
  Init(u64, ConsumerValue),
  Disconnect(u64),
}

#[tonic::async_trait]
impl Handler<DispatcherCmd> for Dispatcher {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: DispatcherCmd) {
    match cmd {
      DispatcherCmd::Add(id, t) => {
        self.add_pending(id, t);
      }
      DispatcherCmd::Remove(id) => {
        self.remove_pending(id);
      }
      DispatcherCmd::Init(id, addr) => {
        self.consumers.insert(id, addr);
      }
      DispatcherCmd::Disconnect(id) => {
        if self.remove_consumer(id) == 0 {
          ctx.stop(None);
        }
      }
    };
  }
}

#[tonic::async_trait]
impl Handler<ServiceCmd> for Dispatcher {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: ServiceCmd) {
    match cmd {
      ServiceCmd::Shutdown => {
        ctx.stop(None);
      }
    }
  }
}

#[message(result = "Vec<Weak<Uuid>>")]
pub struct ActiveTasks;

#[tonic::async_trait]
impl Handler<ActiveTasks> for Dispatcher {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, _: ActiveTasks) -> Vec<Weak<Uuid>> {
    self.get_active()
  }
}

/// An actor that provides the `FetcherHandle`. There is a running actor per
/// queue being fetched.
struct Fetcher {
  /// Queue name.
  queue: Arc<String>,
  /// Storage DB.
  store: Storel,
  /// Semaphore that controls access to the the store.
  semaph: Arc<Semaphore>,
}

impl Fetcher {
  /// Creates a new `Fetcher` for `queue`.
  pub async fn new(queue: Arc<String>) -> Result<Self, StoreError> {
    let store = Storel::open().await?;
    let semaph = Arc::new(Semaphore::new(1));
    Ok(Self {
      queue,
      store,
      semaph,
    })
  }

  /// Returns a `FetcherHandle` to fetch tasks.
  pub async fn get_handler(&mut self) -> FetcherHandle {
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
struct FetcherHandle {
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
  pub async fn fetch(&self) -> Result<FetchResponse, StoreError> {
    let _permit = self.semaph.acquire().await.expect("acquire fetch");
    self.store.read_new(&self.queue).await
  }
}

#[tonic::async_trait]
impl Actor for Fetcher {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    debug!("Fetcher '{}' stopped", self.queue);
  }

  async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    debug!("Fetcher '{}' is delivering", self.queue);
    Ok(())
  }
}

/// Message used by the `Fetcher` actor.
#[message(result = "FetcherHandle")]
struct GetHandler;

#[tonic::async_trait]
impl Handler<GetHandler> for Fetcher {
  async fn handle(&mut self, _: &mut ActorContext<Self>, _: GetHandler) -> FetcherHandle {
    self.get_handler().await
  }
}

/// Message used by the `Consumer` actor.
#[message]
enum ConsumerCmd {
  /// Fetch tasks from storage.
  Fetch,
}

/// Consumer value for the `Dispatcher`'s consumers dictionary.
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
struct Consumer {
  manager: Manager,
  tx: mpsc::Sender<Result<FetchResponse, Status>>,
  dispatcher: Addr<Dispatcher>,
  fetcher: Addr<Fetcher>,
  queue: Arc<String>,
  exit: Arc<Notify>,
}

impl Consumer {
  async fn respond(&mut self, resp: FetchResponse, id: u64) {
    // TODO: DO NOT CREATE UUID IN HERE
    let uuid = Uuid::parse_str(&resp.id).expect("uuid from str");
    let task = self.manager.in_process(uuid);

    // Call to master may fail when its address has been dropped,
    // which it's posible only when the dispatcher received the stop
    // command. So we don't care for error because Dispatch Worker is
    // dropped as well.
    self
      .dispatcher
      .call(DispatcherCmd::Add(id, task.clone()))
      .await
      .unwrap_or(());
    self.tx.send(Ok(resp)).await.expect("send incoming task");
    task.wait_to_finish().await;
    self
      .dispatcher
      .call(DispatcherCmd::Remove(id))
      .await
      .unwrap_or(());
  }

  /// Fetch incoming tasks from storage; `id` is the identifier of `Consumer`.
  async fn fetch(&mut self, id: u64) {
    loop {
      let res = self.fetcher.call(GetHandler).await;
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
