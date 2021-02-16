use clap::{App as Cli, Arg, ArgMatches, Error, ErrorKind};
use std::{ffi::OsString, net::SocketAddr};
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug)]
pub struct Config {
  socket: SocketAddr,
  log_filter: LevelFilter,
  sync: bool,
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

  pub fn log_filter(&self) -> LevelFilter {
    self.log_filter
  }

  pub fn is_sync(&self) -> bool {
    self.sync
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

    // Sync
    self.sync = matches.is_present(ArgName::SYNC);

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
        Arg::with_name(ArgName::SYNC)
          .short("s")
          .long("sync")
          .help("Flush system buffers to disk on every transaction")
          .long_help(
            "Flush system buffers to disk on every transaction.
This guarantees ACID properties but decreases performance.",
          ),
      )
      .arg(
        Arg::with_name(ArgName::LOG_FILTER)
          .long("log-level")
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
    let log_filter = DefaultValue::LOG_FILTER;
    let sync = false;

    Self {
      socket,
      log_filter,
      sync,
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
  const LOG_FILTER: &'static str = "LOG_FILTER";
  const SYNC: &'static str = "SYNC";
}

struct DefaultValue;

impl DefaultValue {
  const PORT: u16 = 7330;
  const HOST: &'static str = "0.0.0.0";
  const LOG_FILTER: LevelFilter = LevelFilter::INFO;
}
