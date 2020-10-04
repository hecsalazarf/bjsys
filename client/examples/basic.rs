use client::worker::{Processor, ProcessError, Worker};
use client::task::TaskStub;
use tokio::time::delay_for;
use std::time::Duration;

struct TestProcessor;

#[tonic::async_trait]
impl Processor for TestProcessor {
  async fn process(&mut self, task: &TaskStub) -> Result<(), ProcessError> {
    println!("Processing task: {:?}", task);
    delay_for(Duration::from_millis(3000)).await;
    println!("Processed after 3s");
    Ok(())
  }
}

#[tokio::main]
async fn main() {
  let processor = TestProcessor;
  let worker = Worker::new(processor)
    .for_queue("myqueue")
    .connect().await.unwrap();

  worker.run().await;
}



