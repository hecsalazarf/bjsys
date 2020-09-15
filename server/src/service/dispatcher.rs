use super::store::{Connection, ConnectionInfo, Storage, Store, StoreErrorKind};
use super::stub::tasks::Task;
use super::ack::AckManager;
use core::task::Poll;
use std::pin::Pin;
use std::task::Context;
use std::sync::Arc;
use tokio::{
  stream::Stream,
  sync::mpsc::{channel, Receiver, Sender},
  sync::watch::{channel as watch, Receiver as WReceiver, Sender as WSender},
  sync::Notify,
};
use tonic::Status;
use tracing::{debug, error, info};
use xactor::{message, Actor, Addr, Context as ActorContext, Handler};

pub struct Dispatcher {
  store: Store,
  master_conn: Connection,
  worker: Addr<DispatchWorker>,
  rx_watch: WReceiver<Option<Arc<Notify>>>,
}

impl Dispatcher {
  pub async fn init(
    conn_info: ConnectionInfo,
    queue: String,
    consumer: String,
    ack_manager: AckManager,
  ) -> Result<Self, Box<dyn std::error::Error>> {
    let store = Store::new(conn_info.clone())
      .with_group(queue, consumer)
      .connect()
      .await?;

    let (tx_watch, rx_watch) = watch(None);

    let worker = DispatchWorker {
      store: store.clone(),
      ack_manager,
      tx_watch
    };

    let master_conn = Connection::start(conn_info).await?;
    Ok(Self {
      store,
      master_conn,
      worker: worker.start().await?,
      rx_watch,
    })
  }

  pub async fn get_tasks(self) -> TaskStream {
    let (tx, rx) = channel(1);
    let addr = self.start().await.unwrap();
    addr.send(TxChannel(tx)).unwrap();
    TaskStream::new(rx, addr)
  }

  pub async fn start_queue(&self, queue: &str) -> Result<(), Status> {
    if let Err(e) = self.store.create_queue().await {
      if e.kind() != StoreErrorKind::ExtensionError {
        error!("DB failed {}", e);
        return Err(Status::unavailable("unavailable"));
      } else {
        debug!("Queue {} was not created, exists already", queue);
      }
    }
    Ok(())
  }
}

#[message]
pub struct TxChannel(Sender<Result<Task, Status>>);

#[tonic::async_trait]
impl Actor for Dispatcher {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    let _: u8 = redis::cmd("CLIENT")
      .arg("KILL")
      .arg("ID")
      .arg(self.store.conn_id())
      .query_async(&mut self.master_conn.inner)
      .await
      .expect("redis_cannot_kill");

    if let Some(Some(n)) = self.rx_watch.recv().await {
      n.notify();
    }
  }
}

#[tonic::async_trait]
impl Handler<TxChannel> for Dispatcher {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, tx: TxChannel) {
    let mut tx = tx.0;
    match self.store.get_pending().await {
      Err(e) => {
        error!("Cannot get pending tasks {}", e);
        tx.send(Err(Status::unavailable("unavailable")))
          .await
          .expect("Cannot send pending error");
      }
      Ok(Some(task)) => {
        self.worker.send(DispatchCmd::WaitThen(tx, task)).expect("wait_then");
      }
      Ok(None) => {
        self.worker.send(DispatchCmd::WaitNew(tx)).expect("no_wait");
      }
    }
  }
}


#[message]
enum DispatchCmd {
  WaitNew(Sender<Result<Task, Status>>),
  WaitThen(Sender<Result<Task, Status>>, Task),
}

struct DispatchWorker {
  store: Store,
  ack_manager: AckManager,
  tx_watch: WSender<Option<Arc<Notify>>>,
}

impl DispatchWorker {
  async fn wait_new(&mut self, mut tx: Sender<Result<Task, Status>>) {
    while let Ok(task) = self.store.collect().await {
      let notify = self.ack_manager.in_process(task.id.clone());
      self.tx_watch.broadcast(Some(notify.clone())).expect("watch_error");
      tx.send(Ok(task)).await.expect("Cannot send incoming task");
      notify.notified().await;
    }
    info!("Client \"{}\" disconnected", self.store.consumer());
  }

  async fn wait_then(&mut self, mut tx: Sender<Result<Task, Status>>, task: Task) {
    let notify = self.ack_manager.in_process(task.id.clone());
    self.tx_watch.broadcast(Some(notify.clone())).expect("watch_error");
    tx.send(Ok(task)).await.expect("Cannot send incoming task");
    notify.notified().await;
    info!("Notify wait then");
    self.wait_new(tx).await;
  }
}

#[tonic::async_trait]
impl Actor for DispatchWorker {
  async fn stopped(&mut self, _ctx: &mut ActorContext<Self>) {
    info!("Dispatched worker stopped")
  }
}

#[tonic::async_trait]
impl Handler<DispatchCmd> for DispatchWorker {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, cmd: DispatchCmd) {
    match cmd {
      DispatchCmd::WaitNew(t) => self.wait_new(t).await,
      DispatchCmd::WaitThen(tx, t) => self.wait_then(tx, t).await,
    }
  }
}


pub struct TaskStream {
  stream: Receiver<Result<Task, Status>>,
  // Keep dispatcher address to stop actor when stream drops
  _addr: Addr<Dispatcher>,
}

impl TaskStream {
  fn new(stream: Receiver<Result<Task, Status>>, _addr: Addr<Dispatcher>) -> Self {
    Self { stream, _addr }
  }
}

impl Stream for TaskStream {
  type Item = Result<Task, Status>;
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
    self.stream.poll_recv(cx)
  }
}
