use client::queue::{ChannelError, Queue};
use client::task::Task;
use serde::Serialize;

#[derive(Serialize)]
struct FooData {
  number: u32,
  string: String,
}

async fn create_queue() -> Result<Queue, ChannelError> {
  Queue::configure().with_name("test-queue").connect().await
}

#[tokio::test]
async fn queue_connect() {
  let queue = create_queue().await;
  assert!(queue.is_ok(), "Cannot connect to server");
}

#[tokio::test]
async fn add_task() {
  let data = FooData {
    number: 4,
    string: String::from("hello queue")
  };
  let mut task = Task::new("foo");
  let _d = task.add_data(&data);
  let mut queue = create_queue().await.unwrap();

  let res = queue.add(task).await;
  assert!(res.is_ok());
  println!("Task id: {}", res.unwrap());
}
