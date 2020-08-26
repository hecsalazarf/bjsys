use proto::Proto;

const OUT_DIR: &'static str = "src/";

fn main() {
  Proto::configure(OUT_DIR).build_server(false).compile();
}
