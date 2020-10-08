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

pub struct MasterWorker {
  dispatchers: HashMap<String, Addr<QueueDispatcher>>,
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

impl Actor for MasterWorker {}

#[tonic::async_trait]
impl Handler<QueueName> for MasterWorker {
  async fn handle(
    &mut self,
    _ctx: &mut ActorContext<Self>,
    queue: QueueName,
  ) -> Result<Dispatcher, ActorError> {
    let queue = queue.0;
    let ack = self.ack.clone();
    let key = format!("{}_{}", queue, "pending");
    let (dispatcher, store) = if self.dispatchers.contains_key(&queue) {
      let d = self.dispatchers.get(&queue).unwrap().clone();
      let s = Store::connect().await.unwrap();

      (d, s)
    } else {
      let s = MasterWorker::init_store(&key).await.unwrap();
      let d = QueueDispatcher {
        workers: HashMap::new(),
        master_store: self.store.clone(),
        name: queue,
      };
      let d = d.start().await?;
      (d, s)
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

pub struct Dispatcher {
  store: Store,
  dispatcher: Addr<QueueDispatcher>,
  ack: AckManager,
  key: String,
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
      key: Arc::new(key),
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

pub struct QueueDispatcher {
  workers: HashMap<usize, WorkerRecord>,
  master_store: MultiplexedStore,
  name: String,
}

impl QueueDispatcher {
  async fn stop_worker(&mut self, id: usize) -> usize {
    // Stop any blocking connection
    self.master_store.stop_by_id(std::iter::once(id)).await;

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
    self.master_store.stop_by_id(ids).await;
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
    info!("Consumer '{}' has disconnected", self.name);
  }

  async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    ctx.subscribe::<ServiceCmd>().await.expect("sub_to_shut");
    info!(
      "Consumer '{}' has connected with {} worker(s)",
      self.name,
      self.workers.capacity()
    );
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

  async fn send_and_wait(&mut self, task: Task) {
    self.send(task).await;
    self.wait_new().await;
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
    match self.store.get_pending(self.key.as_ref()).await {
      Err(e) => {
        error!("Cannot get pending tasks {}", e);
        self
          .tx
          .send(Err(Status::unavailable("unavailable")))
          .await
          .expect("Cannot send pending error");
      }
      Ok(Some(task)) => {
        self.send_and_wait(task).await;
      }
      Ok(None) => {
        self.wait_new().await;
      }
    }
  }
}

#[tonic::async_trait]
impl Actor for DispatchWorker {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    info!("Worker disconnected");
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
