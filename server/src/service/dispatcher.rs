use super::ack::{AckManager, WaitingTask};
use super::store::{connection, Connection, Storage, Store, StoreError};
use super::stub::tasks::{Consumer, Task};
use core::task::Poll;
use std::collections::HashSet;
use std::pin::Pin;
use std::task::Context;
use tokio::{stream::Stream, sync::mpsc};
use tonic::Status;
use tracing::{debug, error, info};
use xactor::{message, Actor, Addr, Context as ActorContext, Handler, Error as ActorError};

pub struct Dispatcher {
  workers: Vec<(usize, Addr<DispatchWorker>)>,
  master_conn: Connection,
  waiting_tasks: HashSet<WaitingTask>,
  name: String,
}

impl Dispatcher {
  pub async fn init(
    consumer: Consumer,
    ack_manager: AckManager,
  ) -> Result<DispatchBuilder, Status> {
    DispatchBuilder::init(consumer, ack_manager).await
  }
}

#[tonic::async_trait]
impl Actor for Dispatcher {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    // Kill any blocking connection
    Connection::kill(&mut self.master_conn, self.workers.iter().map(|e| e.0)).await;

    for task in self.waiting_tasks.iter() {
      task.finish();
    }

    info!("Consumer '{}' has disconnected", self.name);
  }

  async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    info!("Consumer '{}' has connected with {} workers", self.name, self.workers.capacity());
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
  master_conn: Connection,
  name: String,
}

impl DispatchBuilder {
  pub async fn into_stream(self) -> Result<TaskStream, Box<dyn std::error::Error>> {
    let n = self.stores.len(); // Number of workers
    let dispatcher = Dispatcher {
      workers: Vec::with_capacity(n),
      master_conn: self.master_conn,
      waiting_tasks: HashSet::new(),
      name: self.name,
    };
    let dispatcher = dispatcher.start().await?;
    let (tx, rx) = mpsc::channel(n);

    for store in self.stores.into_iter() {
      let conn_id = store.conn_id();
      let worker = DispatchWorker {
        store: store,
        ack_manager: self.ack_manager.clone(),
        tx: tx.clone(),
        master: dispatcher.clone(),
      };
      let worker_addr = worker.start().await?;
      worker_addr.send(WorkerCmd::Fetch).expect("send_fetch");
      dispatcher
        .call(DispatcherCmd::Init(conn_id, worker_addr))
        .await?;
    }
    Ok(TaskStream::new(rx, dispatcher))
  }

  async fn init(consumer: Consumer, ack_manager: AckManager) -> Result<DispatchBuilder, Status> {
    let name = consumer.hostname.clone();
    let (stores, master_conn) = Self::init_store(consumer).await.map_err(|e| {
      error!("Cannot init dispatcher {}", e);
      Status::unavailable("unavailable") // TODO: Better error description
    })?;

    Ok(DispatchBuilder {
      stores,
      ack_manager,
      master_conn,
      name,
    })
  }

  async fn init_store(consumer: Consumer) -> Result<(Vec<Store>, Connection), StoreError> {
    let stores = Store::new().for_consumer(consumer).connect().await?;
    let master_conn = connection().await?;
    if let Err(e) = stores[0].create_queue().await {
      if let Some(c) = e.code() {
        if c == "BUSYGROUP" {
          debug!(
            "Queue {} was not created, exists already",
            stores[0].queue()
          );
        }
      } else {
        return Err(e);
      }
    }

    Ok((stores, master_conn))
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
}

impl DispatchWorker {
  async fn wait_new(&mut self) {
    while let Ok(task) = self.store.collect().await {
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
    match self.store.get_pending().await {
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

impl Actor for DispatchWorker {}

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
