use super::repository::{DbErrorKind, IntoConnectionInfo, TasksRepository, TasksStorage};
use super::stub::tasks::Task;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::Status;
use tracing::{debug, error, info};
use xactor::{message, Actor, Addr, Context, Handler};

struct AckManager;

#[message]
struct Ack;

impl Actor for AckManager {}

#[tonic::async_trait]
impl Handler<Ack> for AckManager {
  async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: Ack) {
    tokio::time::delay_for(std::time::Duration::from_secs(5)).await;
    info!("Task acked");
  }
}

pub struct Dispatcher {
  ack_addr: Addr<AckManager>,
  repo: TasksRepository,
  queue: String,
  consumer: String,
}

impl Dispatcher {
  pub async fn new<T: IntoConnectionInfo>(
    conn_info: T,
    queue: String,
    consumer: String,
  ) -> Result<Self, Box<dyn std::error::Error>> {
    let repo = TasksRepository::connect(conn_info).await?;
    Ok(Self {
      repo,
      ack_addr: AckManager.start().await.unwrap(),
      queue,
      consumer,
    })
  }

  pub async fn get_tasks(self) -> Receiver<Result<Task, Status>> {
    let (mut tx, rx) = channel(1);
    // TODO: This spawned task in hung after client disconnects. We must
    // finish it
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
    rx
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
    loop {
      match self
        .repo
        .wait_for_incoming(&self.queue, &self.consumer)
        .await
      {
        Err(e) => {
          error!("{:?}", e.kind());
          tx.send(Err(Status::unavailable("")))
            .await
            .expect("erro_wait_incoming");
        }
        Ok(task) => {
          // TODO: This will panick for hung threads. We must exit from loop
          tx.send(Ok(task)).await.expect("Cannot send incoming task");
          self.ack_addr.call(Ack).await.unwrap();
        }
      }
    }
  }
}
