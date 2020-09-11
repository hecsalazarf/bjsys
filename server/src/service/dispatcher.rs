use super::repository::{DbErrorKind, IntoConnectionInfo, TasksRepository, TasksStorage};
use super::stub::tasks::{Task, Worker};
use tokio::sync::mpsc::{channel, Receiver};
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
}

impl Dispatcher {
  pub async fn new<T: IntoConnectionInfo>(
    conn_info: T,
  ) -> Result<Self, Box<dyn std::error::Error>> {
    let repo = TasksRepository::connect(conn_info).await?;
    Ok(Self {
      repo,
      ack_addr: AckManager.start().await.unwrap(),
    })
  }
  pub async fn get_tasks(self, worker: &Worker) -> Receiver<Result<Task, Status>> {
    let (mut tx, rx) = channel(4);
    let queue = worker.queue.clone();
    let hostname = worker.hostname.clone();
    tokio::spawn(async move {
      // TODO: This spawned task in hung after client disconnects. We must
      // finish it
      match self.repo.pending(&queue, &hostname).await {
        Err(e) => {
          error!("Cannot get pending tasks {}", e);
          tx.send(Err(Status::unavailable("unavailable")))
            .await
            .unwrap();
        }
        Ok(Some(task)) => {
            tx.send(Ok(task)).await.unwrap();
            self.ack_addr.call(Ack).await.unwrap();

            let addr = self.ack_addr.clone();
            let queue = queue.clone();
            let hostname = hostname.clone();
            loop {
              match self.repo.wait_for_incoming(&queue, &hostname).await {
                Err(e) => {
                  error!("{:?}", e.kind());
                  tx.send(Err(Status::unavailable("")))
                    .await
                    .expect("erro_wait_icoming");
                }
                Ok(task) => {
                  tx.send(Ok(task)).await.unwrap();
                  addr.call(Ack).await.unwrap();
                }
              }
            }
        }
        Ok(None) => {
          unimplemented!();
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
}
