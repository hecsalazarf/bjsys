use crate::task::TaskStub;
use crate::taskstub::tasks_core_client::TasksCoreClient as Client;
use crate::taskstub::{AcknowledgeRequest, Consumer};
use tonic::transport::channel::Channel;
use tonic::transport::{Endpoint, Uri};
use xactor::{message, Actor, Addr, Context, Handler};

pub use tonic::transport::Error as ChannelError;
pub use tonic::{Request, Status as ChannelStatus};

#[derive(Debug)]
pub struct WorkerBuilder<P: Processor> {
  consumer: Consumer,
  endpoint: Endpoint,
  processor: Option<P>,
}

impl<P: Processor> Default for WorkerBuilder<P> {
  fn default() -> Self {
    let endpoint = Endpoint::from_static("http://localhost:11000");
    let consumer = Consumer {
      hostname: String::from("rust"),
      queue: String::from("default"),
      workers: 1,
      ..Consumer::default()
    };

    Self {
      endpoint,
      consumer,
      processor: None,
    }
  }
}

impl<P: Processor> WorkerBuilder<P> {
  pub fn for_queue<T: Into<String>>(mut self, name: T) -> Self {
    self.consumer.queue = name.into();
    self
  }

  pub fn endpoint<T: Into<Uri>>(mut self, uri: T) -> Self {
    self.endpoint = Endpoint::from(uri.into());
    self
  }

  pub async fn connect(self) -> Result<Worker<P>, ChannelError> {
    let channel = self.endpoint.connect().await?;
    let consumer = self.consumer;
    let processor = self.processor.unwrap();

    let worker = WorkerProcessor {
      consumer,
      client: Client::new(channel),
      processor,
    };
    let addr = worker.start().await.expect("start_worker");
    Ok(Worker { addr })
  }
}

pub struct Worker<P: Processor> {
  addr: Addr<WorkerProcessor<P>>,
}

impl<P: Processor> Worker<P> {
  pub fn new(processor: P) -> WorkerBuilder<P> {
    let processor = Some(processor);

    WorkerBuilder {
      processor,
      ..WorkerBuilder::default()
    }
  }

  pub async fn run(&self) {
    self.addr.call(WorkerCmd::Run).await.expect("fetch");
  }
}

#[message]
enum WorkerCmd {
  Run,
}

struct WorkerProcessor<P: Processor> {
  consumer: Consumer,
  client: Client<Channel>,
  processor: P,
}

impl<P: Processor> WorkerProcessor<P> {
  async fn fetch(&mut self) {
    let mut stream = self
      .client
      .fetch(self.consumer.clone())
      .await
      .unwrap()
      .into_inner();
    while let Some(task) = stream.message().await.unwrap() {
      if let Err(e) = self.processor.process(&task).await {
        println!("Process error = {:?}", e);
        // TODO: Send ack with error
      }
      let ack = AcknowledgeRequest {
        queue: task.queue,
        task_id: task.id,
        status: 0,
      };
      if let Err(e) = self.client.acknowledge(ack).await {
        println!("Ack error = {:?}", e);
      }
    }
  }
}

impl<P: Processor> Actor for WorkerProcessor<P> {}

#[tonic::async_trait]
impl<P: Processor> Handler<WorkerCmd> for WorkerProcessor<P> {
  async fn handle(&mut self, _ctx: &mut Context<Self>, cmd: WorkerCmd) {
    match cmd {
      WorkerCmd::Run => self.fetch().await,
    }
  }
}

#[derive(Debug)]
pub enum ProcessError {
  Failed,
  Canceled,
}

#[tonic::async_trait]
pub trait Processor: Sized + Send + 'static {
  async fn process(&mut self, task: &TaskStub) -> Result<(), ProcessError>;
}
