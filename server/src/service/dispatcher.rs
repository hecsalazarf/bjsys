use super::store::{Connection, IntoConnectionInfo, Storage, Store, StoreErrorKind};
use super::stub::tasks::Task;
use core::task::Poll;
use std::pin::Pin;
use std::task::Context;
use tokio::{
  stream::Stream,
  sync::mpsc::{channel, Receiver, Sender},
};
use tonic::Status;
use tracing::{debug, error, info};
use xactor::{message, Actor, Addr, Context as ActorContext, Handler};

struct AckManager;

#[message]
struct Ack;

impl Actor for AckManager {}

#[tonic::async_trait]
impl Handler<Ack> for AckManager {
  async fn handle(&mut self, _ctx: &mut ActorContext<Self>, _msg: Ack) {
    tokio::time::delay_for(std::time::Duration::from_secs(5)).await;
    info!("Task acked");
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
        tx.send(Ok(task)).await.expect("Cannot send pending task");
        self.ack_addr.call(Ack).await.unwrap();
        self.collect(tx);
      }
      Ok(None) => {
        self.collect(tx);
      }
    }
  }
}

#[derive(Clone)]
pub struct Dispatcher {
  ack_addr: Addr<AckManager>,
  store: Store,
  master_conn: Connection,
}

impl Dispatcher {
  pub async fn init<T: IntoConnectionInfo + Clone>(
    conn_info: T,
    queue: String,
    consumer: String,
  ) -> Result<Self, Box<dyn std::error::Error>> {
    let store = Store::new()
      .with_group(queue, consumer)
      .connect(conn_info.clone())
      .await?;

    let master_conn = Connection::start(conn_info).await?;
    Ok(Self {
      store,
      ack_addr: AckManager.start().await.unwrap(),
      master_conn,
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

  fn collect(&self, mut tx: Sender<Result<Task, Status>>) {
    let store = self.store.clone();
    let ack_addr = self.ack_addr.clone();
    tokio::spawn(async move {
      while let Ok(task) = store.collect().await {
        tx.send(Ok(task)).await.expect("Cannot send incoming task");
        ack_addr.call(Ack).await.unwrap();
      }
      info!("Client \"{}\" disconnected", store.consumer());
    });
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
