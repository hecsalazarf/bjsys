use crate::error::{Error, ProcessCode, ProcessError};
use crate::task::{Context, Task};
use futures_util::stream::{FuturesUnordered, StreamExt};
use proto::client::TasksCoreClient as Client;
use proto::{AckRequest, FetchRequest, TaskStatus};
use std::sync::Arc;
use tonic::transport::{channel::Channel, Endpoint, Uri};
use xactor::{message, Actor, Addr, Context as ActorContext, Handler};

#[derive(Debug)]
pub struct WorkerBuilder<P> {
  consumer: FetchRequest,
  endpoint: Endpoint,
  processor: P,
  concurrency: u16,
}

impl<P: Processor + Clone> WorkerBuilder<P> {
  pub fn new(processor: P) -> Self {
    let endpoint = Endpoint::from_static("http://127.0.0.1:7330");
    let concurrency = 1;
    let consumer = FetchRequest {
      hostname: String::from("rust"),
      queue: String::from("default"),
      ..FetchRequest::default()
    };

    Self {
      endpoint,
      consumer,
      processor,
      concurrency,
    }
  }

  pub fn for_queue<S: Into<String>>(mut self, name: S) -> Self {
    self.consumer.queue = name.into();
    self
  }

  pub fn endpoint(mut self, uri: Uri) -> Self {
    self.endpoint = uri.into();
    self
  }

  pub fn concurrency(mut self, value: u16) -> Self {
    if value == 0 {
      panic!("concurrency must be greater than 0");
    }
    self.concurrency = value;
    self
  }

  pub async fn connect(self) -> Result<Worker<P>, Error> {
    let channel = self.endpoint.connect().await?;
    let worker = ProcessorWorker {
      consumer: Arc::new(self.consumer),
      processor: self.processor,
      client: Client::new(channel),
    };

    let futures = FuturesUnordered::new();
    for _ in 1..self.concurrency {
      futures.push(worker.clone().start());
    }
    futures.push(worker.start());

    let addrs = futures
      .map(|res| res.expect("start worker"))
      .collect()
      .await;

    Ok(Worker { addrs })
  }
}

pub struct Worker<P> {
  addrs: Vec<Addr<ProcessorWorker<P>>>,
}

impl<P: Processor> Worker<P> {
  pub async fn run(&self) -> Result<(), Error> {
    let futures = FuturesUnordered::new();
    for addr in &self.addrs {
      futures.push(addr.call(WorkerCmd::Fetch))
    }

    let acc = futures
      .map(|res| res.expect("fetch tasks"))
      .collect::<Vec<Result<(), Error>>>()
      .await;

    if let Some(err) = acc.into_iter().find(|r| r.is_err()) {
      return err;
    }
    Ok(())
  }
}

#[message(result = "Result<(), Error>")]
enum WorkerCmd {
  Fetch,
}

#[derive(Clone)]
struct ProcessorWorker<P> {
  consumer: Arc<FetchRequest>,
  client: Client<Channel>,
  processor: P,
}

impl<P: Processor> ProcessorWorker<P> {
  async fn fetch(&mut self, worker_id: u64) -> Result<(), Error> {
    let request = self.consumer.as_ref().clone();
    let mut stream = self.client.fetch(request).await?.into_inner();

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
pub trait Processor: Sized + Send + Clone + 'static {
  type Ok: serde::Serialize;
  type Data: serde::de::DeserializeOwned + Sized + Send + 'static;
  async fn process(
    &mut self,
    task: Task<Self::Data>,
    ctx: Context,
  ) -> Result<Self::Ok, ProcessError>;

  fn configure(self) -> WorkerBuilder<Self> {
    WorkerBuilder::new(self)
  }
}
