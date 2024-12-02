#![allow(unused_imports)]
use bytes::{Bytes, BytesMut};
use tokio::sync::RwLock;
use core::str;
use std::char::ToLowercase;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::usize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod resp;
mod error;
mod server;
mod state;
mod config;

use resp::{RespParser, RespType, Operation};
use error::RedisError;
use server::RedisServer;
use state::{RedisState, SetMap, SetOverwriteArgs, SetExpiryArgs};
use config::{Config, ConfigParam, ConfigOperation};

struct RedisBuffer {
    index: usize,
    buffer: Bytes,
}

#[tokio::main]
async fn main() {
    let redis_server = RedisServer::new("127.0.0.1", 6379);
    redis_server.run().await
}
