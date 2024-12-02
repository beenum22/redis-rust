use std::{collections::HashMap, time::SystemTime};

use tokio::sync::RwLock;

use crate::Config;

pub(crate) struct RedisState {
    pub(crate) state: RwLock<HashMap<String, SetMap>>,
    pub(crate) config: RwLock<Config>,
}

impl RedisState {
    pub(crate) fn new(state: RwLock<HashMap<String, SetMap>>, config: RwLock<Config>) -> Self {
        Self {
            state,
            config,
        }
    }
}

#[derive(Clone)]
pub(crate) enum SetOverwriteArgs {
    NX,
    XX,
}

#[derive(Clone)]
pub(crate) enum SetExpiryArgs {
    EX(u64),
    PX(u128),
    EXAT(u64),
    PXAT(u128),
}

#[derive(Clone)]
pub(crate) struct SetMap {
    pub(crate) expiry: Option<SetExpiryArgs>,
    pub(crate) key: String,
    pub(crate) val: String,
    pub(crate) overwrite: Option<SetOverwriteArgs>,
    pub(crate) keepttl: Option<bool>,
    pub(crate) expiry_timestamp: Option<SystemTime>,
}