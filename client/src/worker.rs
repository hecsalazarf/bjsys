use crate::task::Task;
use crate::taskstub::tasks_core_client::TasksCoreClient as Client;
use crate::taskstub::{AckRequest, FetchRequest, TaskStatus};
use crate::{ChannelError, ChannelStatus};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use tonic::transport::channel::Channel;
use tonic::transport::{Endpoint, Uri};
use xactor::{message, Actor, Addr, Context, Handler};

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

  pub async fn connect(self) -> Result<Worker<T, P>, ChannelError> {
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

  pub async fn run(&self) -> Result<(), ChannelStatus> {
    self.addr.call(WorkerCmd::Fetch).await.expect("fetch tasks")
  }
}

#[message(result = "Result<(), ChannelStatus>")]
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
  async fn fetch(&mut self) -> Result<(), ChannelStatus> {
    let mut stream = self.client.fetch(self.consumer.clone()).await?.into_inner();

    while let Some(response) = stream.message().await? {
      let mut status = TaskStatus::Done;
      let mut message = String::new();
      let id = response.id.clone();
      let queue = response.queue.clone();

      let task = Task::from_response(response).map_err(|e| {
        let error = format!("Cannot serialize data: {}", e);
        ChannelStatus::invalid_argument(error)
      })?;
      match self.processor.process(task).await {
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
pub trait Processor<T: DeserializeOwned>: Sized + Send + 'static {
  async fn process(&mut self, task: Task<T>) -> Result<Option<String>, ProcessError>;
}
