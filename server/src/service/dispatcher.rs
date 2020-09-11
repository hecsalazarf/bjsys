use super::repository::{DbErrorKind, TasksRepository, TasksStorage};
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
  repository: TasksRepository,
}

impl Dispatcher {
  pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
    let repository = TasksRepository::connect("redis://127.0.0.1:6380/").await?;
    Ok(Self {
      repository,
      ack_addr: AckManager.start().await.unwrap(),
    })
  }
  pub async fn get_tasks(&self, worker: &Worker) -> Receiver<Result<Task, Status>> {
    let (mut tx, rx) = channel(4);
    let queue = &worker.queue;
    let hostname = &worker.hostname;

    match self.repository.pending(queue, hostname).await {
      Err(e) => {
        error!("Cannot get pending tasks {}", e);
        tx.send(Err(Status::unavailable("unavailable")))
          .await
          .unwrap();
      }
      Ok(mut r) => {
        if let Some(t) = r.ids.pop() {
          let task = Task {
            id: t.id.clone(),
            kind: t.get("kind").unwrap(),
            queue: String::new(),
            data: t.get("data").unwrap(),
          };
          let addr = self.ack_addr.clone();
          tx.send(Ok(task)).await.unwrap();


          let repo = self.repository.clone();
          let queue = queue.clone();
          let hostname = hostname.clone();
          tokio::spawn(async move {
            if true { // check comes from pending
              addr.call(Ack).await.unwrap();
            }
            loop {
              match repo.wait_for_incoming(&queue, &hostname).await {
                Err(e) => {
                  error!("{:?}", e.kind());
                  tx.send(Err(Status::unavailable(""))).await.expect("erro_wait_icoming");
                }
                Ok(mut res) => {
                  let mut t = res.keys.pop().unwrap();
                  let t = t.ids.pop().unwrap();
                  let task = Task {
                    id: t.id.clone(),
                    kind: t.get("kind").unwrap(),
                    queue: queue.clone(),
                    data: t.get("data").unwrap(),
                  };
                  tx.send(Ok(task)).await.unwrap();
                  addr.call(Ack).await.unwrap();
                }
              }
            }
          });
        }
      }
    }
    rx
  }

  pub async fn start_queue(&self, queue: &str) -> Result<(), Status> {
    if let Err(e) = self.repository.create_queue(queue).await {
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
