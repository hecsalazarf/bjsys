use clap::{App as Cli, Arg, ArgMatches, Error, ErrorKind};
use redis::ConnectionInfo;
use std::{ffi::OsString, net::SocketAddr};
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug)]
pub struct Config {
  socket: SocketAddr,
  redis_conn: ConnectionInfo,
  log_filter: LevelFilter,
}

impl Config {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn with_args(args: Vec<OsString>) -> Self {
    let mut config = Self::new();
    config.merge_cli(args);
    config
  }

  pub fn socket(&self) -> SocketAddr {
    self.socket
  }

  pub fn redis_conn(&self) -> &ConnectionInfo {
    &self.redis_conn
  }

  pub fn log_filter(&self) -> LevelFilter {
    self.log_filter
  }

  fn merge_cli(&mut self, args: Vec<OsString>) {
    let matches = Self::cli_matches(args);

    // Port
    if let Some(port_str) = matches.value_of(ArgName::PORT) {
      if let Ok(port) = u16::from_str_radix(port_str, 10) {
        self.socket.set_port(port);
      } else {
        Self::exit(&format!("Invalid port '{}'", port_str));
      }
    }

    // Redis connection info
    if let Some(url) = matches.value_of(ArgName::REDIS) {
      if let Ok(conn_info) = url.parse() {
        self.redis_conn = conn_info;
      } else {
        Self::exit(&format!("Invalid Redis URL '{}'", url));
      }
    }

    // Log filter
    if let Some(filter) = matches.value_of(ArgName::LOG_FILTER) {
      self.log_filter = match filter {
        "off" => LevelFilter::OFF,
        "error" => LevelFilter::ERROR,
        "warn" => LevelFilter::WARN,
        "info" => LevelFilter::INFO,
        "debug" => LevelFilter::DEBUG,
        "trace" => LevelFilter::TRACE,
        _ => Self::exit(&format!("Invalid log level '{}'", filter)),
      }
    }
  }

  fn cli_matches<'a>(args: Vec<OsString>) -> ArgMatches<'a> {
    Cli::new(clap::crate_name!())
      .version(clap::crate_version!())
      .version_short("v")
      .author(clap::crate_authors!())
      .about(clap::crate_description!())
      .arg(
        Arg::with_name(ArgName::PORT)
          .short("p")
          .long("port")
          .help(&format!("Server port (default: {})", DefaultValue::PORT))
          .takes_value(true),
      )
      .arg(
        Arg::with_name(ArgName::REDIS)
          .short("r")
          .long("redis")
          .help(&format!("Redis URL (default: {})", DefaultValue::REDIS_URL))
          .takes_value(true)
          .value_name("URL"),
      )
      .arg(
        Arg::with_name(ArgName::LOG_FILTER)
          .short("l")
          .long("loglevel")
          .help(&format!(
            "Log level (default: {})",
            DefaultValue::LOG_FILTER
          ))
          .takes_value(true)
          .value_name("LEVEL"),
      )
      .get_matches_from(args)
  }

  fn exit(desc: &str) -> ! {
    let err = Error::with_description(desc, ErrorKind::InvalidValue);
    err.exit()
  }
}

impl Default for Config {
  fn default() -> Self {
    let socket = SocketAddr::new(DefaultValue::HOST.parse().unwrap(), DefaultValue::PORT);
    let redis_conn = DefaultValue::REDIS_URL.parse().unwrap();
    let log_filter = DefaultValue::LOG_FILTER;

    Self {
      socket,
      redis_conn,
      log_filter,
    }
  }
}

impl From<Vec<OsString>> for Config {
  fn from(args: Vec<OsString>) -> Self {
    if args.len() > 1 {
      Config::with_args(args)
    } else {
      Config::default()
    }
  }
}

struct ArgName;

impl ArgName {
  const PORT: &'static str = "PORT";
  const REDIS: &'static str = "REDIS";
  const LOG_FILTER: &'static str = "LOG_FILTER";
}

struct DefaultValue;

impl DefaultValue {
  const PORT: u16 = 7330;
  const HOST: &'static str = "0.0.0.0";
  #[cfg(unix)]
  const REDIS_URL: &'static str = "redis+unix:///tmp/redis.sock?db=0";
  #[cfg(not(unix))]
  const REDIS_URL: &'static str = "redis://127.0.0.1/0";
  const LOG_FILTER: LevelFilter = LevelFilter::INFO;
}
