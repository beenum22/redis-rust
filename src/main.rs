#![allow(unused_imports)]
use bytes::{Bytes, BytesMut};
use core::str;
use std::char::ToLowercase;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::usize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

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

#[tokio::main]
async fn main() {
    let redis_server = RedisServer::new("127.0.0.1", 6379);
    redis_server.run().await
}
