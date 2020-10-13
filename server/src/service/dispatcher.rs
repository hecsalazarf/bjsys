use super::ack::{AckManager, WaitingTask};
use super::store::{MultiplexedStore, RedisStorage, Store, StoreError};
use super::stub::tasks::Task;
use core::task::Poll;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::Context;
use std::sync::Arc;
use tokio::{stream::Stream, sync::{mpsc, Notify}};
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

    let addr = worker.start().await.expect("dispatcher_failed");

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
  dispatchers: HashMap<String, DispatcherRecord>,
  store: MultiplexedStore,
  ack: AckManager,
}

#[message(result = "Result<Dispatcher, ActorError>")]
struct QueueName(String);

#[message]
enum MasterCmd {
  Disconnect(String),
}

impl Actor for MasterWorker {}

#[tonic::async_trait]
impl Handler<QueueName> for MasterWorker {
  async fn handle(
    &mut self,
    ctx: &mut ActorContext<Self>,
    queue: QueueName,
  ) -> Result<Dispatcher, ActorError> {
    let queue = queue.0;
    let ack = self.ack.clone();
    let (dispatcher, reader) = if self.dispatchers.contains_key(&queue) {
      let r = self.dispatchers.get(&queue).unwrap();
      (r.addr.clone(), r.reader.clone())
    } else {
      let reader = QueueReader::connect(&queue).await.expect("failed_connect_store");
      let conn_id = reader.id();
      let reader = reader.start().await?;
      let qd = QueueDispatcher {
        workers: HashMap::new(),
        store: self.store.clone(),
        name: queue.clone(),
        master: ctx.address(),
        pending: VecDeque::with_capacity(5),
        still_pending: true,
      };
      let addr = qd.start().await?;
      let record = DispatcherRecord {
        addr: addr.clone(),
        reader: reader.clone(),
        conn_id,
      };
      self.dispatchers.insert(queue, record);
      (addr, reader)
    };

    let dispatcher = Dispatcher {
      dispatcher,
      ack,
      reader,
    };

    Ok(dispatcher)
  }
}

#[tonic::async_trait]
impl Handler<MasterCmd> for MasterWorker {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, cmd: MasterCmd) {
    match cmd {
      MasterCmd::Disconnect(queue) => {
        if let Some(mut r) = self.dispatchers.remove(&queue) {
          let conn_id = r.conn_id;
          self.store.stop_by_id(std::iter::once(conn_id)).await;
          r.reader.stop(None).expect("failes_stop_reader");
        }
      }
    }
  }
}

pub struct Dispatcher {
  dispatcher: Addr<QueueDispatcher>,
  ack: AckManager,
  reader: Addr<QueueReader>,
}

impl Dispatcher {
  pub async fn into_stream(self) -> Result<TaskStream, Box<dyn std::error::Error>> {
    let ack = self.ack;
    let dispatcher = self.dispatcher;
    let reader = self.reader;
    let (tx, rx) = mpsc::channel(1);
    let exit = Arc::new(Notify::new());

    let worker = DispatchWorker {
      ack,
      tx,
      master: dispatcher.clone(),
      reader,
      exit: exit.clone(),
    };
    let _addr = worker.start().await?;
    _addr.send(WorkerCmd::Fetch).expect("send_fetch");
    let id = _addr.actor_id();
    let wr = WorkerRecord {
      _addr,
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

use std::collections::VecDeque;

pub struct QueueDispatcher {
  workers: HashMap<u64, WorkerRecord>,
  master: Addr<MasterWorker>,
  store: MultiplexedStore,
  pending: VecDeque<Task>,
  still_pending: bool,
  name: String,
}

impl QueueDispatcher {
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
    for (_, worker) in &mut self.workers {
      if let Some(ref task) = worker.pending_task {
        task.finish();
      }
      worker.exit.notify();
    }
  }

  async fn read_pending(&mut self) -> Result<Option<Task>, StoreError> {
    if self.pending.is_empty() && self.still_pending {
      let key = format!("{}_{}", self.name, "pending");
      let mut tasks = self.store.read_pending(&key, 5).await?;
      if tasks.len() < 5 {
        self.still_pending = false;
      }
      self.pending.append(&mut tasks);
    }

    Ok(self.pending.pop_front())
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
      .call(MasterCmd::Disconnect(self.name.clone()))
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

use super::ServiceCmd;

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

#[message(result = "Result<Option<Task>, StoreError>")]
struct GetPending;

#[tonic::async_trait]
impl Handler<GetPending> for QueueDispatcher {
  async fn handle(
    &mut self,
    _ctx: &mut ActorContext<Self>,
    _: GetPending,
  ) -> Result<Option<Task>, StoreError> {
    self.read_pending().await
  }
}

struct QueueReader {
  key: String,
  store: Store,
  queue: VecDeque<Task>,
  pending: bool,
}

impl QueueReader {
  pub async fn connect(queue: &str) -> Result<Self, StoreError> {
    let key = format!("{}_{}", queue, "pending");
    let queue = VecDeque::with_capacity(5);
    let pending = true;
    let store = Self::init_store(&key).await?;

    Ok(Self {
      key,
      queue,
      pending,
      store,
    })
  }

  pub fn id (&self) -> usize {
    self.store.id()
  }

  pub async fn read_pending(&mut self) -> Result<Option<Task>, StoreError> {
    if self.queue.is_empty() && self.pending {
      let mut tasks = self.store.read_pending(&self.key, 5).await?;
      if tasks.len() < 5 {
        self.pending = false;
      }
      self.queue.append(&mut tasks);
    }

    Ok(self.queue.pop_front())
  }

  pub async fn read_new(&mut self) -> Result<Option<Task>, StoreError> {
    if self.queue.is_empty() {
      let mut tasks = self.store.read_new(&self.key, 5).await?;
      self.queue.append(&mut tasks);
    }

    Ok(self.queue.pop_front())
  }

  async fn init_store(key: &str) -> Result<Store, StoreError> {
    let mut store = Store::connect().await?;
    if let Err(e) = store.create_queue(key).await {
      if let Some(c) = e.code() {
        if c == "BUSYGROUP" {
          debug!("Queue {} was not created, exists already", key);
        }
      } else {
        return Err(e);
      }
    }

    Ok(store)
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
enum ReadTask {
  Pending,
  New,
}

#[tonic::async_trait]
impl Handler<ReadTask> for QueueReader {
  async fn handle(
    &mut self,
    _ctx: &mut ActorContext<Self>,
    read: ReadTask,
  ) -> Result<Option<Task>, StoreError> {
    match read {
      ReadTask::Pending => self.read_pending().await,
      ReadTask::New => self.read_new().await
    }
  }
}

#[message]
enum WorkerCmd {
  Fetch,
}

struct DispatchWorker {
  ack: AckManager,
  tx: mpsc::Sender<Result<Task, Status>>,
  master: Addr<QueueDispatcher>,
  reader: Addr<QueueReader>,
  exit: Arc<Notify>,
}

impl DispatchWorker {
  async fn wait_new(&mut self, id: u64) {
    loop {
      tokio::select! {
        res = self.reader.call(ReadTask::New) => {
          if let Ok(Some(task)) = res.unwrap() {
            self.send(task, id).await;
          } else {
            break;
          }
        }
        _ = self.exit.notified() => {
          break;
        }
      };
    }
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
      let res = self.reader.call(ReadTask::Pending).await;
      // The result may only fail when master has diopped. In such case, we
      // break the loop.
      match res.unwrap_or(Ok(None)) {
        Err(e) => {
          error!("Cannot get pending tasks {}", e);
          self
            .tx
            .send(Err(Status::unavailable("unavailable")))
            .await
            .expect("Cannot send pending error");
        }
        Ok(Some(t)) => {
          self.send(t, id).await;
        }
        Ok(None) => {
          break;
        }
      }
    }
    self.wait_new(id).await;
  }
}

#[tonic::async_trait]
impl Actor for DispatchWorker {
  async fn stopped(&mut self, ctx: &mut ActorContext<Self>) {
    info!("Worker [{}] disconnected", ctx.actor_id());
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    info!("Worker [{}] connected", ctx.actor_id());
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
