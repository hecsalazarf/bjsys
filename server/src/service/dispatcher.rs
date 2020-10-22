use super::ack::{AckManager, WaitingTask};
use super::store::{MultiplexedStore, RedisStorage, Store, StoreError};
use super::stub::tasks::Task;
use super::ServiceCmd;
use core::task::Poll;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use tokio::{
  stream::Stream,
  sync::{mpsc, Notify},
};
use tonic::Status;
use tracing::{debug, error, info};
use xactor::{message, Actor, Addr, Context as ActorContext, Error as ActorError, Handler};

pub struct MasterDispatcher {
  addr: Addr<MasterWorker>,
}

impl MasterDispatcher {
  pub async fn init(store: MultiplexedStore, ack: AckManager) -> Self {
    let worker = MasterWorker {
      dispatchers: HashMap::new(),
      store,
      ack,
    };

    let addr = worker.start().await.expect("master_worker_failed");

    Self { addr }
  }

  pub async fn create(&self, queue: String) -> Result<Dispatcher, Status> {
    let disp = self
      .addr
      .call(QueueName(queue))
      .await
      .unwrap()
      .map_err(|_e| Status::unavailable(""))?;

    Ok(disp)
  }
}

struct DispatcherRecord {
  addr: Addr<QueueDispatcher>,
  reader: Addr<QueueReader>,
  conn_id: usize,
}

pub struct MasterWorker {
  dispatchers: HashMap<Arc<String>, DispatcherRecord>,
  store: MultiplexedStore,
  ack: AckManager,
}

type DispatcherTuple = Result<(Addr<QueueDispatcher>, Addr<QueueReader>), ActorError>;

impl MasterWorker {
  async fn register(&mut self, addr: Addr<Self>, queue: Arc<String>) -> DispatcherTuple {
    if self.dispatchers.contains_key(&queue) {
      let r = self.dispatchers.get(&queue).unwrap();
      Ok((r.addr.clone(), r.reader.clone()))
    } else {
      let reader = QueueReader::connect(&queue)
        .await
        .expect("failed_connect_store");
      let conn_id = reader.id();
      let reader = reader.start().await?;
      let qd = QueueDispatcher::new(queue.clone(), addr);
      let addr = qd.start().await?;
      let record = DispatcherRecord {
        addr: addr.clone(),
        reader: reader.clone(),
        conn_id,
      };
      self.dispatchers.insert(queue.clone(), record);
      Ok((addr, reader))
    }
  }

  async fn unregister(&mut self, queue: Arc<String>) {
    if let Some(mut r) = self.dispatchers.remove(&queue) {
      let conn_id = r.conn_id;
      self.store.stop_by_id(std::iter::once(conn_id)).await;
      r.reader.stop(None).expect("failed_stop_reader");
    }
  }
}

#[message(result = "Result<Dispatcher, ActorError>")]
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
  ) -> Result<Dispatcher, ActorError> {
    let queue = Arc::new(queue.0);
    let ack = self.ack.clone();

    let (dispatcher, reader) = self.register(ctx.address(), queue.clone()).await?;

    Ok(Dispatcher {
      dispatcher,
      ack,
      reader,
      queue,
    })
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
  dispatcher: Addr<QueueDispatcher>,
  ack: AckManager,
  reader: Addr<QueueReader>,
  queue: Arc<String>,
}

impl Dispatcher {
  pub async fn into_stream(self) -> Result<TaskStream, Box<dyn std::error::Error>> {
    let mut worker = DispatchWorker::from(self);
    let exit = worker.exit_signal();
    let dispatcher = worker.dispatcher();
    let rx = worker.receiver();

    let addr = worker.start().await?;
    addr.send(WorkerCmd::Fetch).expect("send_fetch");
    let id = addr.actor_id();
    let wr = WorkerRecord {
      _addr: addr,
      pending_task: None,
      exit,
    };
    dispatcher.call(DispatcherCmd::Init(id, wr)).await?;
    Ok(TaskStream::new(rx, dispatcher, id))
  }
}

struct WorkerRecord {
  _addr: Addr<DispatchWorker>,
  pending_task: Option<WaitingTask>,
  exit: Arc<Notify>,
}

pub struct QueueDispatcher {
  workers: HashMap<u64, WorkerRecord>,
  master: Addr<MasterWorker>,
  name: Arc<String>,
}

impl QueueDispatcher {
  fn new(name: Arc<String>, master: Addr<MasterWorker>) -> Self {
    let workers = HashMap::new();
    Self {
      name,
      master,
      workers,
    }
  }
  async fn stop_worker(&mut self, id: u64) -> usize {
    // Never fails as it was created before
    let worker = self.workers.remove(&id).unwrap();
    if let Some(task) = worker.pending_task {
      task.finish();
    }
    worker.exit.notify();

    self.workers.len()
  }

  async fn stop_all(&mut self) {
    for worker in self.workers.values() {
      if let Some(ref task) = worker.pending_task {
        task.finish();
      }
      worker.exit.notify();
    }
  }

  fn add_pending(&mut self, id: u64, task: WaitingTask) {
    if let Some(w) = self.workers.get_mut(&id) {
      w.pending_task = Some(task);
    }
  }

  fn remove_pending(&mut self, id: u64) {
    if let Some(w) = self.workers.get_mut(&id) {
      w.pending_task.take();
    }
  }
}

#[tonic::async_trait]
impl Actor for QueueDispatcher {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    self.stop_all().await;
    // Inform master to drop this address
    self
      .master
      .call(MasterCmd::Unregister(self.name.clone()))
      .await
      .unwrap();
    debug!("Queue '{}' is no longer being served", self.name);
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    ctx.subscribe::<ServiceCmd>().await.expect("sub_to_shut");
    debug!("Queue '{}' is being dispatched", self.name,);
    Ok(())
  }
}

#[message]
enum DispatcherCmd {
  Add(u64, WaitingTask),
  Remove(u64),
  Init(u64, WorkerRecord),
  Disconnect(u64),
}

#[tonic::async_trait]
impl Handler<DispatcherCmd> for QueueDispatcher {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: DispatcherCmd) {
    match cmd {
      DispatcherCmd::Add(id, t) => {
        self.add_pending(id, t);
      }
      DispatcherCmd::Remove(id) => {
        self.remove_pending(id);
      }
      DispatcherCmd::Init(id, addr) => {
        self.workers.insert(id, addr);
      }
      DispatcherCmd::Disconnect(id) => {
        if self.stop_worker(id).await == 0 {
          ctx.stop(None);
        }
      }
    };
  }
}

#[tonic::async_trait]
impl Handler<ServiceCmd> for QueueDispatcher {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: ServiceCmd) {
    match cmd {
      ServiceCmd::Shutdown => {
        ctx.stop(None);
      }
    }
  }
}

struct QueueReader {
  key: String,
  store: Store,
  // queue: VecDeque<Task>,
  // pending: bool,
}

impl QueueReader {
  pub async fn connect(queue: &str) -> Result<Self, StoreError> {
    let key = String::from(queue);
    // let queue = VecDeque::with_capacity(5);
    // let pending = true;
    let store = Store::connect().await?;

    Ok(Self {
      key,
      // queue,
      // pending,
      store,
    })
  }

  pub fn id(&self) -> usize {
    self.store.id()
  }

  pub async fn read(&mut self) -> Result<Option<Task>, StoreError> {
    // TODO: How to handle pending tasks?
    let task = self.store.read_new(&self.key, 5).await?;
    Ok(Some(task))
  }
}

#[tonic::async_trait]
impl Actor for QueueReader {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    debug!("QueueReader '{}' stopped", self.key);
  }

  async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    debug!("QueueReader '{}' is delivering", self.key);
    Ok(())
  }
}

#[message(result = "Result<Option<Task>, StoreError>")]
struct ReadTask;

#[tonic::async_trait]
impl Handler<ReadTask> for QueueReader {
  async fn handle(
    &mut self,
    _: &mut ActorContext<Self>,
    _: ReadTask,
  ) -> Result<Option<Task>, StoreError> {
    self.read().await
  }
}

#[message]
enum WorkerCmd {
  Fetch,
}

struct DispatchWorker {
  ack: AckManager,
  tx: mpsc::Sender<Result<Task, Status>>,
  rx: Option<mpsc::Receiver<Result<Task, Status>>>,
  master: Addr<QueueDispatcher>,
  reader: Addr<QueueReader>,
  exit: Arc<Notify>,
  queue: Arc<String>,
}

impl DispatchWorker {
  pub fn receiver(&mut self) -> mpsc::Receiver<Result<Task, Status>> {
    self.rx.take().unwrap()
  }

  pub fn dispatcher(&mut self) -> Addr<QueueDispatcher> {
    self.master.clone()
  }

  pub fn exit_signal(&mut self) -> Arc<Notify> {
    self.exit.clone()
  }

  async fn send(&mut self, task: Task, id: u64) {
    let notify = self.ack.in_process(task.id.clone());

    // Call to master may fail when its address has been dropped,
    // which it's posible only when the dispatcher received the stop
    // command. So we don't care for error because Dispatch Worker is
    // dropped as well.
    self
      .master
      .call(DispatcherCmd::Add(id, notify.clone()))
      .await
      .unwrap_or(());
    self
      .tx
      .send(Ok(task))
      .await
      .expect("Cannot send incoming task");
    notify.wait_to_finish().await;
    self
      .master
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
                .expect("failed_send_task");
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

impl From<Dispatcher> for DispatchWorker {
  fn from(dispatcher: Dispatcher) -> Self {
    let ack = dispatcher.ack;
    let reader = dispatcher.reader;
    let queue = dispatcher.queue;
    let master = dispatcher.dispatcher;
    let (tx, rx) = mpsc::channel(1);
    let rx = Some(rx);
    let exit = Arc::new(Notify::new());

    Self {
      ack,
      tx,
      rx,
      queue,
      reader,
      master,
      exit,
    }
  }
}

#[tonic::async_trait]
impl Actor for DispatchWorker {
  async fn stopped(&mut self, ctx: &mut ActorContext<Self>) {
    info!(
      "Worker [{}] disconnected from '{}'",
      ctx.actor_id(),
      self.queue
    );
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    info!("Worker [{}] connected to '{}'", ctx.actor_id(), self.queue);
    Ok(())
  }
}

#[tonic::async_trait]
impl Handler<WorkerCmd> for DispatchWorker {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: WorkerCmd) {
    match cmd {
      WorkerCmd::Fetch => self.fetch(ctx.actor_id()).await,
    }
  }
}

pub struct TaskStream {
  stream: mpsc::Receiver<Result<Task, Status>>,
  dispatcher: Addr<QueueDispatcher>,
  id: u64,
}

impl TaskStream {
  fn new(
    stream: mpsc::Receiver<Result<Task, Status>>,
    dispatcher: Addr<QueueDispatcher>,
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
  type Item = Result<Task, Status>;
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
