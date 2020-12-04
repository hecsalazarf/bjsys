use server::app::App;
use std::env;

#[tokio::main]
async fn main() {
  let mut dir = env::current_exe().unwrap();
  dir.pop();

  let app = App::builder()
    .with_args(env::args_os())
    .with_tracing(true)
    .working_dir(dir)
    .init()
    .await;

  app.listen().await;
}
