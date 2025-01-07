#![allow(unused_imports)]
use bytes::{Bytes, BytesMut};
use clap::Parser;
use core::str;
use log::{info, log_enabled, Level, LevelFilter};
use std::char::ToLowercase;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::usize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

mod config;
mod error;
mod info;
mod ops;
mod rdb;
mod resp;
mod server;
mod state;

use config::{Config, ConfigOperation, ConfigParam};
use error::{RDBError, RedisError};
use info::{Info, InfoOperation, ReplicationInfo};
use ops::Operation;
use rdb::RdbParser;
use resp::{RespParser, RespType};
use server::RedisServer;
use state::{State, SetExpiryArgs, SetMap, SetOverwriteArgs};

struct RedisBuffer {
    index: usize,
    buffer: Bytes,
}

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long, default_value = "/data")]
    dir: String,

    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,

    #[arg(long)]
    replicaof: Option<String>,

    #[arg(short, long, default_value = "info")]
    logging: String,
}

fn setup_logger(log_level: LevelFilter) -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| out.finish(format_args!("[{}] {}", record.level(), message)))
        .level(log_level)
        .chain(std::io::stdout())
        .chain(fern::log_file("output.log")?)
        .apply()?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let log_level = match args.logging.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => {
            println!("Invalid log level '{}', defaulting to 'info'", args.logging);
            LevelFilter::Info
        }
    };

    setup_logger(log_level).unwrap();

    let redis_server = RedisServer::new(
        args.host.as_str(),
        args.port,
        args.dir,
        args.dbfilename,
        args.replicaof,
    );
    redis_server.run().await
}
