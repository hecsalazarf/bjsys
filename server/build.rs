const OUT_DIR: &'static str = "src/service/stub/";
use proto::Proto;

fn main() {
  Proto::configure(OUT_DIR).build_client(false).compile();
}
