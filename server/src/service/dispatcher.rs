use super::ack::{AckManager, WaitingTask};
use super::store::{MultiplexedStore, RedisStorage, Store, StoreError};
use super::stub::tasks::Task;
use core::task::Poll;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use tokio::{stream::Stream, sync::mpsc};
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
  key: Arc<String>,
}

pub struct MasterWorker {
  dispatchers: HashMap<String, DispatcherRecord>,
  store: MultiplexedStore,
  ack: AckManager,
}

impl MasterWorker {
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
    let (dispatcher, store, key) = if self.dispatchers.contains_key(&queue) {
      let r = self.dispatchers.get(&queue).unwrap();
      let s = Store::connect().await.unwrap();
      (r.addr.clone(), s, r.key.clone())
    } else {
      let k = Arc::new(format!("{}_{}", queue, "pending"));
      let s = MasterWorker::init_store(&k).await.unwrap();
      let qd = QueueDispatcher {
        workers: HashMap::new(),
        store: self.store.clone(),
        name: queue.clone(),
        master: ctx.address(),
        pending: VecDeque::with_capacity(5),
        still_pending: true,
      };
      let a = qd.start().await?;
      let r = DispatcherRecord {
        addr: a.clone(),
        key: k.clone(),
      };
      self.dispatchers.insert(queue, r);
      (a, s, k)
    };

    let dispatcher = Dispatcher {
      store,
      dispatcher,
      ack,
      key,
    };

    Ok(dispatcher)
  }
}

#[tonic::async_trait]
impl Handler<MasterCmd> for MasterWorker {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, cmd: MasterCmd) {
    match cmd {
      MasterCmd::Disconnect(queue) => {
        self.dispatchers.remove(&queue);
      }
    }
  }
}

pub struct Dispatcher {
  store: Store,
  dispatcher: Addr<QueueDispatcher>,
  ack: AckManager,
  key: Arc<String>,
}

impl Dispatcher {
  pub async fn into_stream(self) -> Result<TaskStream, Box<dyn std::error::Error>> {
    let conn_id = self.store.id();
    let store = self.store;
    let ack = self.ack;
    let key = self.key;
    let dispatcher = self.dispatcher;
    let (tx, rx) = mpsc::channel(1);

    let worker = DispatchWorker {
      store,
      ack,
      tx,
      master: dispatcher.clone(),
      key,
    };
    let _addr = worker.start().await?;
    _addr.send(WorkerCmd::Fetch).expect("send_fetch");
    let wr = WorkerRecord {
      _addr,
      pending_task: None,
    };
    dispatcher.call(DispatcherCmd::Init(conn_id, wr)).await?;
    Ok(TaskStream::new(rx, dispatcher, conn_id))
  }
}

struct WorkerRecord {
  _addr: Addr<DispatchWorker>,
  pending_task: Option<WaitingTask>,
}

use std::collections::VecDeque;

pub struct QueueDispatcher {
  workers: HashMap<usize, WorkerRecord>,
  master: Addr<MasterWorker>,
  store: MultiplexedStore,
  pending: VecDeque<Task>,
  still_pending: bool,
  name: String,
}

impl QueueDispatcher {
  async fn stop_worker(&mut self, id: usize) -> usize {
    // Stop any blocking connection
    self.store.stop_by_id(std::iter::once(id)).await;

    // Never fails as it was created before
    let worker = self.workers.remove(&id).unwrap();
    if let Some(task) = worker.pending_task {
      task.finish();
    }

    self.workers.len()
  }

  async fn stop_all(&mut self) {
    let ids = self.workers.drain().map(|(id, worker)| {
      if let Some(task) = worker.pending_task {
        task.finish();
      }
      id
    });

    // Stop all connections at once
    self.store.stop_by_id(ids).await;
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

  fn add_pending(&mut self, id: usize, task: WaitingTask) {
    if let Some(w) = self.workers.get_mut(&id) {
      w.pending_task = Some(task);
    }
  }

  fn remove_pending(&mut self, id: usize) {
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
  Add(usize, WaitingTask),
  Remove(usize),
  Init(usize, WorkerRecord),
  Disconnect(usize),
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
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, _: GetPending) -> Result<Option<Task>, StoreError> {
    self.read_pending().await
  }
}


#[message]
enum WorkerCmd {
  Fetch,
}

struct DispatchWorker {
  store: Store,
  ack: AckManager,
  tx: mpsc::Sender<Result<Task, Status>>,
  master: Addr<QueueDispatcher>,
  key: Arc<String>,
}

impl DispatchWorker {
  async fn wait_new(&mut self) {
    while let Ok(task) = self.store.collect(self.key.as_ref()).await {
      self.send(task).await;
    }
  }

  async fn send(&mut self, task: Task) {
    let id = self.store.id();
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

  async fn fetch(&mut self) {
    loop {
      let res = self.master.call(GetPending).await;
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
          self.send(t).await;
        }
        Ok(None) => {
          break;
        }
      }
    }
    self.wait_new().await;
  }
}

#[tonic::async_trait]
impl Actor for DispatchWorker {
  async fn stopped(&mut self, ctx: &mut ActorContext<Self>) {
    info!("Worker {}[{}] disconnected", self.key, ctx.actor_id());
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    info!("Worker {}[{}] connected", self.key, ctx.actor_id());
    Ok(())
  }
}

#[tonic::async_trait]
impl Handler<WorkerCmd> for DispatchWorker {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, cmd: WorkerCmd) {
    match cmd {
      WorkerCmd::Fetch => self.fetch().await,
    }
  }
}

pub struct TaskStream {
  stream: mpsc::Receiver<Result<Task, Status>>,
  dispatcher: Addr<QueueDispatcher>,
  id: usize,
}

impl TaskStream {
  fn new(
    stream: mpsc::Receiver<Result<Task, Status>>,
    dispatcher: Addr<QueueDispatcher>,
    id: usize,
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
