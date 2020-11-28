use crate::error::{Error, ProcessCode, ProcessError};
use crate::task::{Context, Task};
use proto::client::TasksCoreClient as Client;
use proto::{AckRequest, FetchRequest, TaskStatus};
use serde::{de::DeserializeOwned, Serialize};
use tonic::transport::channel::Channel;
use tonic::transport::{Endpoint, Uri};
use xactor::{message, Actor, Addr, Context as ActorContext, Handler};

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
  pub fn for_queue<S: Into<String>>(mut self, name: S) -> Self {
    self.consumer.queue = name.into();
    self
  }

  pub fn endpoint<U: Into<Uri>>(mut self, uri: U) -> Self {
    self.endpoint = Endpoint::from(uri.into());
    self
  }

  pub async fn connect(self) -> Result<Worker<P>, Error> {
    let channel = self.endpoint.connect().await?;
    let consumer = self.consumer;
    let processor = self.processor.unwrap();

    let worker = ProcessorWorker {
      consumer,
      client: Client::new(channel),
      processor,
    };
    let addr = worker.start().await.expect("start worker");
    Ok(Worker { addr })
  }
}

pub struct Worker<P: Processor> {
  addr: Addr<ProcessorWorker<P>>,
}

impl<P: Processor> Worker<P> {
  pub fn builder(processor: P) -> WorkerBuilder<P> {
    let processor = Some(processor);

    let worker = WorkerBuilder {
      processor,
      ..WorkerBuilder::default()
    };

    worker
  }

  pub async fn run(&self) -> Result<(), Error> {
    self.addr.call(WorkerCmd::Fetch).await.expect("fetch tasks")
  }
}

#[message(result = "Result<(), Error>")]
enum WorkerCmd {
  Fetch,
}

struct ProcessorWorker<P: Processor> {
  consumer: FetchRequest,
  client: Client<Channel>,
  processor: P,
}

impl<P: Processor> ProcessorWorker<P> {
  async fn fetch(&mut self, worker_id: u64) -> Result<(), Error> {
    let mut stream = self.client.fetch(self.consumer.clone()).await?.into_inner();

    while let Some(response) = stream.message().await? {
      let id = response.id.clone();
      let queue = response.queue.clone();

      let task = Task::from_response(response)?;
      let ctx = Context::new(worker_id);
      let (status, message) = match self.processor.process(task, ctx).await {
        Err(e) => {
          let status = match e.code() {
            ProcessCode::Failed => TaskStatus::Failed,
            ProcessCode::Canceled => TaskStatus::Canceled,
          };

          (status, e.into_msg())
        }
        Ok(ref m) => {
          // It's developer's responsability to send a message that can be serialized
          (TaskStatus::Done, serde_json::to_string(m).unwrap())
        }
      };
      let req = AckRequest {
        queue,
        task_id: id,
        status: status.into(),
        message,
      };
      self.client.ack(req).await?;
    }

    Ok(())
  }
}

impl<P: Processor> Actor for ProcessorWorker<P> {}

#[tonic::async_trait]
impl<P: Processor> Handler<WorkerCmd> for ProcessorWorker<P> {
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: WorkerCmd) -> Result<(), Error> {
    match cmd {
      WorkerCmd::Fetch => self.fetch(ctx.actor_id()).await,
    }
  }
}

#[tonic::async_trait]
pub trait Processor: Sized + Send + 'static {
  type Ok: Serialize;
  type Data: DeserializeOwned + Sized + Send + 'static;
  async fn process(
    &mut self,
    task: Task<Self::Data>,
    ctx: Context,
  ) -> Result<Self::Ok, ProcessError>;
}
