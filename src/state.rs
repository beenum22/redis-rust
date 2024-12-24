use lzf;
use std::fs::{read, File};
use std::io::{BufReader, Read};
use std::str;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;

use crate::{error::RDBError, error::RedisError, resp::Operation, Config};

pub(crate) struct RedisState {
    pub(crate) state: RwLock<HashMap<String, SetMap>>,
    pub(crate) config: RwLock<Config>,
}

impl RedisState {
    pub(crate) fn new(state: RwLock<HashMap<String, SetMap>>, config: RwLock<Config>) -> Self {
        Self { state, config }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum SetOverwriteArgs {
    NX,
    XX,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum SetExpiryArgs {
    EX(u64),
    PX(u128),
    EXAT(u64),
    PXAT(u128),
}

#[derive(Clone, PartialEq, Debug)]
pub struct SetMap {
    pub(crate) expiry: Option<SetExpiryArgs>,
    pub(crate) key: String,
    pub(crate) val: String,
    pub(crate) overwrite: Option<SetOverwriteArgs>,
    pub(crate) keepttl: Option<bool>,
    pub(crate) expiry_timestamp: Option<SystemTime>,
}
