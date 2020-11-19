use crate::taskstub::tasks_core_client::TasksCoreClient as Client;
use crate::taskstub::{AckRequest, FetchRequest, TaskStatus};
use tonic::transport::channel::Channel;
use tonic::transport::{Endpoint, Uri};
use xactor::{message, Actor, Addr, Context, Handler};

pub use tonic::transport::Error as ChannelError;
pub use tonic::{Request, Status as ChannelStatus};

// TODO: Transform fetch response into Task. Do not expose FetchTesponse
pub use crate::taskstub::FetchResponse;

#[derive(Debug)]
pub struct WorkerBuilder<P: Processor> {
  consumer: FetchRequest,
  endpoint: Endpoint,
  processor: Option<P>,
}

impl<P: Processor> Default for WorkerBuilder<P> {
  fn default() -> Self {
    let endpoint = Endpoint::from_static("http://localhost:11000");
    let consumer = FetchRequest {
      hostname: String::from("rust"),
      queue: String::from("default"),
      ..FetchRequest::default()
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
    let addr = worker.start().await.expect("start worker");
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

  pub async fn run(&self) -> Result<(), ChannelStatus> {
    self.addr.call(WorkerCmd::Fetch).await.expect("fetch tasks")
  }
}

#[message(result = "Result<(), ChannelStatus>")]
enum WorkerCmd {
  Fetch,
}

struct WorkerProcessor<P: Processor> {
  consumer: FetchRequest,
  client: Client<Channel>,
  processor: P,
}

impl<P: Processor> WorkerProcessor<P> {
  async fn fetch(&mut self) -> Result<(), ChannelStatus> {
    let mut stream = self.client.fetch(self.consumer.clone()).await?.into_inner();

    while let Some(task) = stream.message().await? {
      let mut status = TaskStatus::Done;
      let mut message = String::new();

      match self.processor.process(&task).await {
        Err(e) => {
          let (s, m) = match e {
            ProcessError::Failed(m) => (TaskStatus::Failed, m),
            ProcessError::Canceled(m) => (TaskStatus::Canceled, m),
          };

          status = s;
          message = m;
        }
        Ok(Some(m)) => {
          message = m;
        }
        _ => {}
      }

      let req = AckRequest {
        queue: task.queue,
        task_id: task.id,
        status: status.into(),
        message,
      };
      self.client.ack(req).await?;
    }

    Ok(())
  }
}

impl<P: Processor> Actor for WorkerProcessor<P> {}

#[tonic::async_trait]
impl<P: Processor> Handler<WorkerCmd> for WorkerProcessor<P> {
  async fn handle(
    &mut self,
    _ctx: &mut Context<Self>,
    cmd: WorkerCmd,
  ) -> Result<(), ChannelStatus> {
    match cmd {
      WorkerCmd::Fetch => self.fetch().await,
    }
  }
}

#[derive(Debug)]
pub enum ProcessError {
  Failed(String),
  Canceled(String),
}

impl ProcessError {
  pub fn failed<M: Into<String>>(message: M) -> Self {
    Self::Failed(message.into())
  }

  pub fn canceled<M: Into<String>>(message: M) -> Self {
    Self::Canceled(message.into())
  }
}

#[tonic::async_trait]
pub trait Processor: Sized + Send + 'static {
  async fn process(&mut self, task: &FetchResponse) -> Result<Option<String>, ProcessError>;
}
