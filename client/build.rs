use proto::Proto;

const OUT_DIR: &str = "src/";

fn main() {
  Proto::configure(OUT_DIR).build_server(false).compile();
}
