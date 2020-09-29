use super::ack::{AckManager, WaitingTask};
use super::store::{MultiplexedStore, RedisStorage, Store, StoreError};
use super::stub::tasks::{Consumer, Task};
use core::task::Poll;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use tokio::{stream::Stream, sync::mpsc};
use tonic::Status;
use tracing::{debug, error, info};
use xactor::{message, Actor, Addr, Context as ActorContext, Error as ActorError, Handler};

pub struct Dispatcher {
  workers: Vec<(usize, Addr<DispatchWorker>)>,
  master_store: MultiplexedStore,
  waiting_tasks: HashSet<WaitingTask>,
  name: String,
}

impl Dispatcher {
  pub async fn build(
    consumer: Consumer,
    ack_manager: AckManager,
    master_store: MultiplexedStore,
  ) -> Result<DispatchBuilder, Status> {
    let name = consumer.hostname;
    let key = format!("{}_{}", consumer.queue, "pending");
    let workers = consumer.workers as usize;

    let stores = Self::init_store(workers, &key).await.map_err(|e| {
      error!("Cannot init dispatcher {}", e);
      Status::unavailable("unavailable") // TODO: Better error description
    })?;

    let builder = DispatchBuilder {
      stores,
      ack_manager,
      master_store,
      name,
      key,
    };

    Ok(builder)
  }

  async fn init_store(workers: usize, key: &str) -> Result<Vec<Store>, StoreError> {
    let mut stores = Store::connect_batch(workers).await?;
    if let Err(e) = stores[0].create_queue(key).await {
      if let Some(c) = e.code() {
        if c == "BUSYGROUP" {
          debug!("Queue {} was not created, exists already", key);
        }
      } else {
        return Err(e);
      }
    }

    Ok(stores)
  }
}

#[tonic::async_trait]
impl Actor for Dispatcher {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    // Stop any blocking connection
    self
      .master_store
      .stop_by_id(self.workers.iter().map(|e| e.0))
      .await;

    for task in self.waiting_tasks.iter() {
      task.finish();
    }

    info!("Consumer '{}' has disconnected", self.name);
  }

  async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
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
  Add(WaitingTask),
  Remove(WaitingTask),
  Init(usize, Addr<DispatchWorker>),
}

#[tonic::async_trait]
impl Handler<DispatcherCmd> for Dispatcher {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, cmd: DispatcherCmd) {
    match cmd {
      DispatcherCmd::Add(t) => {
        self.waiting_tasks.insert(t);
      }
      DispatcherCmd::Remove(t) => {
        self.waiting_tasks.remove(&t);
      }
      DispatcherCmd::Init(conn_id, addr) => self.workers.push((conn_id, addr)), // self.workers.push(a),
    };
  }
}

pub struct DispatchBuilder {
  stores: Vec<Store>,
  ack_manager: AckManager,
  master_store: MultiplexedStore,
  name: String,
  key: String,
}

impl DispatchBuilder {
  pub async fn into_stream(self) -> Result<TaskStream, Box<dyn std::error::Error>> {
    let n = self.stores.len(); // Number of workers
    let dispatcher = Dispatcher {
      workers: Vec::with_capacity(n),
      master_store: self.master_store,
      waiting_tasks: HashSet::new(),
      name: self.name.clone(),
    };
    let dispatcher = dispatcher.start().await?;
    let (tx, rx) = mpsc::channel(n);

    let key = Arc::new(self.key);
    let mut i: usize = 0;
    for store in self.stores {
      let consumer = format!("{}-{}", &self.name, i);
      let conn_id = store.id();
      let worker = DispatchWorker {
        store,
        ack_manager: self.ack_manager.clone(),
        tx: tx.clone(),
        master: dispatcher.clone(),
        key: key.clone(),
        consumer,
      };
      let worker_addr = worker.start().await?;
      worker_addr.send(WorkerCmd::Fetch).expect("send_fetch");
      dispatcher
        .call(DispatcherCmd::Init(conn_id, worker_addr))
        .await?;
      i += 1;
    }
    Ok(TaskStream::new(rx, dispatcher))
  }
}

#[message]
enum WorkerCmd {
  Fetch,
}

struct DispatchWorker {
  store: Store,
  ack_manager: AckManager,
  tx: mpsc::Sender<Result<Task, Status>>,
  master: Addr<Dispatcher>,
  key: Arc<String>,
  consumer: String,
}

impl DispatchWorker {
  async fn wait_new(&mut self) {
    while let Ok(task) = self
      .store
      .collect(self.key.as_ref(), self.consumer.as_ref())
      .await
    {
      self.send(task).await;
    }
  }

  async fn send_and_wait(&mut self, task: Task) {
    self.send(task).await;
    self.wait_new().await;
  }

  async fn send(&mut self, task: Task) {
    let notify = self.ack_manager.in_process(task.id.clone());

    // Call to master may fail when its address has been dropped,
    // which it's posible only when the dispatcher received the stop
    // command. So we don't care for error because Dispatch Worker is
    // dropped as well.
    self
      .master
      .call(DispatcherCmd::Add(notify.clone()))
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
      .call(DispatcherCmd::Remove(notify))
      .await
      .unwrap_or(());
  }

  async fn fetch(&mut self) {
    match self
      .store
      .get_pending(self.key.as_ref(), self.consumer.as_ref())
      .await
    {
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
    debug!("Worker {} disconnected", self.consumer);
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
  dispatcher: Addr<Dispatcher>,
}

impl TaskStream {
  fn new(stream: mpsc::Receiver<Result<Task, Status>>, dispatcher: Addr<Dispatcher>) -> Self {
    Self { stream, dispatcher }
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
    self.dispatcher.stop(None).expect("drop_dispatcher");
  }
}
