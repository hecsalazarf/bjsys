use crate::manager::{Manager, WaitingTask};
use crate::scheduler::Scheduler;
use crate::service::ServiceCmd;
use crate::store::{ConnectionInfo, MultiplexedStore, RedisStorage, Store, StoreError};
use core::task::Poll;
use proto::FetchResponse;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::Context;
use tokio::{
  stream::Stream,
  sync::{mpsc, Notify},
};
use tonic::Status;
use tracing::{debug, error};
use xactor::{message, Actor, Addr, Context as ActorContext, Error as ActorError, Handler};

pub struct MasterDispatcher {
  addr: Addr<MasterWorker>,
}

impl MasterDispatcher {
  pub async fn init(
    store: &MultiplexedStore,
    manager: &Manager,
    conn_info: &ConnectionInfo,
  ) -> Self {
    let worker = MasterWorker {
      dispatchers: HashMap::new(),
      conn_info: conn_info.clone(),
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
  reader: Addr<Reader>,
  conn_id: usize,
}

type DispatcherRecord = (Arc<String>, DispatcherValue);
type Registration = Result<DispatcherRecord, ActorError>;

pub struct MasterWorker {
  dispatchers: HashMap<Arc<String>, DispatcherValue>,
  store: MultiplexedStore,
  manager: Manager,
  conn_info: ConnectionInfo,
}

impl MasterWorker {
  fn find(&self, queue: &String) -> Option<DispatcherRecord> {
    self
      .dispatchers
      .get_key_value(queue)
      .map(|r| (r.0.clone(), r.1.clone()))
  }

  async fn register(&mut self, addr: Addr<Self>, queue: String) -> Registration {
    let queue = Arc::new(queue);
    let reader = Reader::connect(queue.clone(), &self.conn_info)
      .await
      .expect("connect to store");

    let conn_id = reader.id();
    let reader_addr = reader.start().await?;
    let dispatcher = Dispatcher::new(queue.clone(), addr, self.store.clone());
    let dispatcher_addr = dispatcher.start().await?;
    let record = DispatcherValue {
      dispatcher: dispatcher_addr,
      reader: reader_addr,
      conn_id,
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
      reader: record.1.reader,
      dispatcher: record.1.dispatcher,
      exit,
      tx,
    };

    let exit = consumer.exit.clone();
    let dispatcher = consumer.dispatcher.clone();
    let worker_addr = consumer.start().await?;
    let id = worker_addr.actor_id();

    worker_addr
      .send(ConsumerCmd::Fetch)
      .expect("start fetching");

    let wr = ConsumerValue {
      _addr: worker_addr,
      pending_task: None,
      exit,
    };
    dispatcher.call(DispatcherCmd::Init(id, wr)).await?;

    Ok(TaskStream::new(rx, dispatcher, id))
  }

  async fn unregister(&mut self, queue: Arc<String>) {
    if let Some(mut record) = self.dispatchers.remove(queue.as_ref()) {
      use std::iter::once;
      self.store.stop_by_id(once(record.conn_id)).await;
      record.reader.stop(None).expect("stop reader");
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
    let record: DispatcherRecord;

    if let Some(r) = self.find(&queue) {
      record = r;
    } else {
      record = self.register(ctx.address(), queue).await?;
    }

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

struct ConsumerValue {
  _addr: Addr<Consumer>,
  pending_task: Option<WaitingTask>,
  exit: Arc<Notify>,
}

pub struct Dispatcher {
  consumers: HashMap<u64, ConsumerValue>,
  master: Addr<MasterWorker>,
  queue: Arc<String>,
  scheduler: Scheduler,
}

impl Dispatcher {
  fn new(queue: Arc<String>, master: Addr<MasterWorker>, store: MultiplexedStore) -> Self {
    let consumers = HashMap::new();
    let scheduler = Scheduler::new(queue.clone(), store);
    Self {
      queue,
      master,
      consumers,
      scheduler,
    }
  }

  async fn stop_consumer(&mut self, id: u64) -> usize {
    // Never fails as it was previously created
    let consumer = self.consumers.remove(&id).unwrap();
    if let Some(task) = consumer.pending_task {
      task.finish();
    }
    consumer.exit.notify();

    self.consumers.len()
  }

  async fn stop_all(&mut self) {
    let all = self.consumers.drain();
    for (_, consumer) in all {
      if let Some(task) = consumer.pending_task {
        task.finish();
      }
      consumer.exit.notify();
    }
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

  fn get_active(&mut self) -> Vec<Weak<String>> {
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
    self.stop_all().await;
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
enum DispatcherCmd {
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
        if self.stop_consumer(id).await == 0 {
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

#[message(result = "Vec<Weak<String>>")]
pub struct ActiveTasks;

#[tonic::async_trait]
impl Handler<ActiveTasks> for Dispatcher {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, _: ActiveTasks) -> Vec<Weak<String>> {
    self.get_active()
  }
}

struct Reader {
  key: Arc<String>,
  store: Store,
}

impl Reader {
  pub async fn connect(queue: Arc<String>, conn_info: &ConnectionInfo) -> Result<Self, StoreError> {
    let key = queue;
    let store = Store::connect(conn_info).await?;

    Ok(Self { key, store })
  }

  pub fn id(&self) -> usize {
    self.store.id()
  }

  pub async fn read(&mut self) -> Result<Option<FetchResponse>, StoreError> {
    let task = self.store.read_new(&self.key).await?;
    Ok(Some(task))
  }
}

#[tonic::async_trait]
impl Actor for Reader {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    debug!("Reader '{}' stopped", self.key);
  }

  async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    debug!("Reader '{}' is delivering", self.key);
    Ok(())
  }
}

#[message(result = "Result<Option<FetchResponse>, StoreError>")]
struct ReadTask;

#[tonic::async_trait]
impl Handler<ReadTask> for Reader {
  async fn handle(
    &mut self,
    _: &mut ActorContext<Self>,
    _: ReadTask,
  ) -> Result<Option<FetchResponse>, StoreError> {
    self.read().await
  }
}

#[message]
enum ConsumerCmd {
  Fetch,
}

struct Consumer {
  manager: Manager,
  tx: mpsc::Sender<Result<FetchResponse, Status>>,
  dispatcher: Addr<Dispatcher>,
  reader: Addr<Reader>,
  exit: Arc<Notify>,
  queue: Arc<String>,
}

impl Consumer {
  async fn send(&mut self, task: FetchResponse, id: u64) {
    let notify = self.manager.in_process(task.id.clone());

    // Call to master may fail when its address has been dropped,
    // which it's posible only when the dispatcher received the stop
    // command. So we don't care for error because Dispatch Worker is
    // dropped as well.
    self
      .dispatcher
      .call(DispatcherCmd::Add(id, notify.clone()))
      .await
      .unwrap_or(());
    self.tx.send(Ok(task)).await.expect("send incoming task");
    notify.wait_to_finish().await;
    self
      .dispatcher
      .call(DispatcherCmd::Remove(id))
      .await
      .unwrap_or(());
  }

  async fn fetch(&mut self, id: u64) {
    loop {
      tokio::select! {
        res = self.reader.call(ReadTask) => {
          if res.is_err() {
            // The result may only fail when master has dropped. In such case, we
            // break the loop.
            break;
          }
          match res.unwrap() {
            Err(e) => {
              error!("Cannot read task {}", e);
              self
                .tx
                .send(Err(Status::unavailable("unavailable")))
                .await
                .expect("send task");
            }
            Ok(Some(t)) => {
              self.send(t, id).await;
            }
            Ok(None) => {
              continue;
            }
          }
        }
        _ = self.exit.notified() => {
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
      "Worker [{}] disconnected from '{}'",
      ctx.actor_id(),
      self.queue
    );
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    debug!("Worker [{}] connected to '{}'", ctx.actor_id(), self.queue);
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

pub struct TaskStream {
  stream: mpsc::Receiver<Result<FetchResponse, Status>>,
  dispatcher: Addr<Dispatcher>,
  id: u64,
}

impl TaskStream {
  fn new(
    stream: mpsc::Receiver<Result<FetchResponse, Status>>,
    dispatcher: Addr<Dispatcher>,
    id: u64,
  ) -> Self {
    Self {
      stream,
      dispatcher,
      id,
    }
  }
}

impl Stream for TaskStream {
  type Item = Result<FetchResponse, Status>;
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
    self.stream.poll_recv(cx)
  }
}

impl Drop for TaskStream {
  fn drop(&mut self) {
    self
      .dispatcher
      .send(DispatcherCmd::Disconnect(self.id))
      .unwrap_or_else(|_| {
        debug!(
          "Dispatcher {} was closed earlier",
          self.dispatcher.actor_id()
        )
      });
  }
}
