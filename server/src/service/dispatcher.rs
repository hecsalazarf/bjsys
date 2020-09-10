use super::stub::tasks::{Worker, Task};
use super::repository::{TasksRepository, TasksStorage, DbErrorKind};
use tokio::sync::mpsc::{Sender, Receiver, channel};
use tonic::Status;
use tracing::{info, error};

pub struct Dispatcher {
  _ack_channel: (Sender<bool>, Receiver<bool>),
  repository: TasksRepository
}

impl Dispatcher {
  pub async fn new () -> Result<Self, Box<dyn std::error::Error>> {
    let repository = TasksRepository::connect("redis://127.0.0.1:6380/").await?;
    Ok(Self {
      repository,
      _ack_channel: channel(100),
    })
  }
  
  pub async fn dispatch(&self, worker: Worker) -> Receiver<Result<Task, Status>> {
    let (mut tx, rx) = channel(4);
    let queue = &worker.queue;
    let hostname = &worker.hostname;
    if let Err(e) = self.repository.create_queue(queue).await {
      match e.kind() {
        DbErrorKind::ExtensionError => info!("Queue {} exists already", queue),
        _ => {
          error!("DB throw an error {}", e);
          tx.send(Err(Status::unavailable("unavailable"))).await.unwrap();
        },
      }
    }

    match self.repository.pending(queue, hostname).await {
      Err(e) => {
        error!("{}", e);
        tx.send(Err(Status::unavailable("unavailable"))).await.unwrap();
      },
      Ok(mut r) => {
        info!("res {:?}", r);
        if let Some(t) = r.ids.pop() {
          let task = Task {
            id: t.id.clone(),
            kind: t.get("kind").unwrap(),
            queue: String::new(),
            data: t.get("data").unwrap(),
          };
          tx.send(Ok(task)).await.unwrap()
        }
      }
    }
    tx.send(Err(Status::unimplemented(""))).await.unwrap();
    rx
  }
}
