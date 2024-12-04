#![allow(unused_imports)]
use bytes::{Bytes, BytesMut};
use core::str;
use std::char::ToLowercase;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::usize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use clap::Parser;

mod config;
mod error;
mod resp;
mod server;
mod state;

use config::{Config, ConfigOperation, ConfigParam};
use error::RedisError;
use resp::{Operation, RespParser, RespType};
use server::RedisServer;
use state::{RedisState, SetExpiryArgs, SetMap, SetOverwriteArgs};

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
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let redis_server = RedisServer::new(args.host.as_str(), args.port, args.dir, args.dbfilename);
    redis_server.run().await
}
