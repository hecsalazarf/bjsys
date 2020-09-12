use super::repository::{
  Connection, ConnectionAddr, ConnectionInfo, DbErrorKind, IntoConnectionInfo, TasksRepository,
  TasksStorage,
};
use super::stub::tasks::Task;
use tonic::Status;
use tracing::{debug, error, info};
use xactor::{message, Actor, Addr, Context as ActorContext, Handler};
use tokio::{
  sync::mpsc::{channel, Receiver, Sender},
  stream::Stream
};
use core::task::Poll;
use std::pin::Pin;
use std::task::Context;

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
pub struct DispatchStop;

struct DispatchKiller {
  conn: Connection,
  id_to: usize,
}

impl DispatchKiller {
  async fn start(id_to: usize) -> Addr<Self> {
    let conn_info = ConnectionInfo {
      db: 0,
      addr: Box::new(ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6380)),
      username: None,
      passwd: None,
    };
    let conn = Connection::start(conn_info).await.expect("conn_err_killer");

    let killer = Self { conn, id_to };

    killer.start().await.expect("Cannot create killer")
  }
}

impl Actor for DispatchKiller {}

#[tonic::async_trait]
impl Handler<DispatchStop> for DispatchKiller {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, _msg: DispatchStop) {
    let _: u8 = redis::cmd("CLIENT")
      .arg("KILL")
      .arg("ID")
      .arg(self.id_to)
      .query_async(&mut self.conn.inner)
      .await
      .expect("redis_cannot_kill");
    ctx.stop(None);
  }
}

pub struct Dispatcher {
  ack_addr: Addr<AckManager>,
  repo: TasksRepository,
  queue: String,
  consumer: String,
}

impl Dispatcher {
  pub async fn new<T: IntoConnectionInfo + Clone>(
    conn_info: T,
    queue: String,
    consumer: String,
  ) -> Result<Self, Box<dyn std::error::Error>> {
    let repo = TasksRepository::connect(conn_info.clone()).await?;
    Ok(Self {
      repo,
      ack_addr: AckManager.start().await.unwrap(),
      queue,
      consumer,
    })
  }

  pub async fn get_tasks(self) -> TaskStream {
    let (mut tx, rx) = channel(1);
    let killer = DispatchKiller::start(self.repo.conn_id()).await;
    tokio::spawn(async move {
      match self.repo.pending(&self.queue, &self.consumer).await {
        Err(e) => {
          error!("Cannot get pending tasks {}", e);
          tx.send(Err(Status::unavailable("unavailable")))
            .await
            .expect("Cannot send pending error");
        }
        Ok(Some(task)) => {
          tx.send(Ok(task)).await.expect("Cannot send pending task");
          self.ack_addr.call(Ack).await.unwrap();
          self.wait(tx).await;
        }
        Ok(None) => {
          self.wait(tx).await;
        }
      }
    });
    TaskStream::new(rx, killer)
  }

  pub async fn start_queue(&self, queue: &str) -> Result<(), Status> {
    if let Err(e) = self.repo.create_queue(queue).await {
      if e.kind() != DbErrorKind::ExtensionError {
        error!("DB failed {}", e);
        return Err(Status::unavailable("unavailable"));
      } else {
        debug!("Queue {} was not created, exists already", queue);
      }
    }
    Ok(())
  }

  async fn wait(self, mut tx: Sender<Result<Task, Status>>) {
    while let Ok(task) = self
      .repo
      .wait_for_incoming(&self.queue, &self.consumer)
      .await
    {
      tx.send(Ok(task)).await.expect("Cannot send incoming task");
      self.ack_addr.call(Ack).await.unwrap();
    }
    info!("Client \"{}\" disconnected", self.consumer);
  }
}

pub struct TaskStream {
  stream: Receiver<Result<Task, Status>>,
  killer: Addr<DispatchKiller>,
}

impl TaskStream {
  fn new(stream: Receiver<Result<Task, Status>>, killer: Addr<DispatchKiller>) -> Self {
    Self { stream, killer }
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
      .killer
      .send(DispatchStop)
      .expect("cannot_kill_dispatcher");
  }
}
