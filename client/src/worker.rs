use crate::error::{Error, ProcessCode, ProcessError};
use crate::task::{Context, Task};
use proto::client::TasksCoreClient as Client;
use proto::{AckRequest, FetchRequest, TaskStatus};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use tonic::transport::channel::Channel;
use tonic::transport::{Endpoint, Uri};
use xactor::{message, Actor, Addr, Context as ActorContext, Handler};

#[derive(Debug)]
pub struct WorkerBuilder<T, P>
where
  T: DeserializeOwned,
  P: Processor<T>,
{
  consumer: FetchRequest,
  endpoint: Endpoint,
  processor: Option<P>,
  task_inner: PhantomData<T>,
}

impl<T, P> Default for WorkerBuilder<T, P>
where
  T: DeserializeOwned,
  P: Processor<T>,
{
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
      task_inner: PhantomData,
    }
  }
}

impl<T, P> WorkerBuilder<T, P>
where
  T: DeserializeOwned + Send + 'static,
  P: Processor<T>,
{
  pub fn for_queue<S: Into<String>>(mut self, name: S) -> Self {
    self.consumer.queue = name.into();
    self
  }

  pub fn endpoint<U: Into<Uri>>(mut self, uri: U) -> Self {
    self.endpoint = Endpoint::from(uri.into());
    self
  }

  pub async fn connect(self) -> Result<Worker<T, P>, Error> {
    let channel = self.endpoint.connect().await?;
    let consumer = self.consumer;
    let processor = self.processor.unwrap();

    let worker = WorkerProcessor {
      consumer,
      client: Client::new(channel),
      processor,
      task_inner: PhantomData,
    };
    let addr = worker.start().await.expect("start worker");
    Ok(Worker { addr })
  }
}

pub struct Worker<T, P>
where
  T: DeserializeOwned + Send + 'static,
  P: Processor<T>,
{
  addr: Addr<WorkerProcessor<T, P>>,
}

impl<T, P> Worker<T, P>
where
  T: DeserializeOwned + Send,
  P: Processor<T>,
{
  pub fn builder(processor: P) -> WorkerBuilder<T, P> {
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

struct WorkerProcessor<T, P>
where
  T: DeserializeOwned + Send + 'static,
  P: Processor<T>,
{
  consumer: FetchRequest,
  client: Client<Channel>,
  processor: P,
  task_inner: PhantomData<T>,
}

impl<T, P> WorkerProcessor<T, P>
where
  T: DeserializeOwned + Send,
  P: Processor<T>,
{
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

impl<T, P> Actor for WorkerProcessor<T, P>
where
  T: DeserializeOwned + Send,
  P: Processor<T>,
{
}

#[tonic::async_trait]
impl<T, P> Handler<WorkerCmd> for WorkerProcessor<T, P>
where
  T: DeserializeOwned + Send,
  P: Processor<T>,
{
  async fn handle(&mut self, ctx: &mut ActorContext<Self>, cmd: WorkerCmd) -> Result<(), Error> {
    match cmd {
      WorkerCmd::Fetch => self.fetch(ctx.actor_id()).await,
    }
  }
}

#[tonic::async_trait]
pub trait Processor<T: DeserializeOwned>: Sized + Send + 'static {
  type Ok: Serialize;
  async fn process(&mut self, task: Task<T>, ctx: Context) -> Result<Self::Ok, ProcessError>;
}
