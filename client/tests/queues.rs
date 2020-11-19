use client::queue::Queue;
use client::task::Builder;
use client::ChannelError;
use serde::Serialize;

#[derive(Serialize)]
struct FooData {
  number: u32,
  string: String,
}

async fn create_queue() -> Result<Queue, ChannelError> {
  Queue::configure().with_name("myqueue").connect().await
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
    string: String::from("hello queue"),
  };
  let task = Builder::new(&data);
  let mut queue = create_queue().await.unwrap();

  let res = queue.add(task).await;
  if let Err(ref e) = res {
    println!("ERROR: {:?}", e);
  }
  assert!(res.is_ok());
  println!("Task id: {}", res.unwrap());
}
