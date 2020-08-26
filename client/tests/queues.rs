use client::queue::{Queue};

#[tokio::test]
async fn queue_connects() {
  let name = "test-queue";
  let queue = Queue::configure().with_name(name).connect().await;
  assert!(queue.is_ok(), "Cannot connect to server");
}
