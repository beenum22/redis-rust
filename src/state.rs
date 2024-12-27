use lzf;
use std::fs::{read, File};
use std::io::{BufReader, Read};
use std::str;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;

use crate::config::ConfigPair;
use crate::{
    error::RDBError, error::RedisError, error::StateError, resp::Operation, Config, ConfigParam,
};

#[derive(Debug)]
pub(crate) struct RedisState {
    pub(crate) state: Arc<RwLock<HashMap<String, SetMap>>>,
    pub(crate) config: Arc<RwLock<Config>>,
}

impl RedisState {
    pub(crate) fn new(dir: String, dbfilename: String) -> Self {
        // Self { state, config }
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(Config::new(dir, dbfilename))),
        }
    }

    pub(crate) async fn get_config_dir(
        config: Arc<RwLock<Config>>,
    ) -> Result<ConfigPair, RedisError> {
        let config_ro = config.read().await;
        Ok(config_ro.dir.clone().ok_or(RedisError::State(StateError::UnknownConfig))?)
    }

    pub(crate) async fn set_config_dir(config: Arc<RwLock<Config>>, dir: String) -> Result<(), RedisError> {
        let mut config_w = config.write().await;
        config_w.dir = Some(ConfigPair {
            key: "dir".to_string(),
            value: dir,
        });
        Ok(())
    }

    pub(crate) async fn get_config_dbfilename(
        config: Arc<RwLock<Config>>,
    ) -> Result<ConfigPair, RedisError> {
        let config_ro = config.read().await;
        Ok(config_ro.dbfilename.clone().ok_or(RedisError::State(StateError::UnknownConfig))?)
    }

    pub(crate) async fn set_config_dbfilename(
        config: Arc<RwLock<Config>>,
        dbfilename: String,
    ) -> Result<(), RedisError> {
        let mut config_w = config.write().await;
        config_w.dbfilename = Some(ConfigPair {
            key: "dbfilename".to_string(),
            value: dbfilename,
        });
        Ok(())
    }

    pub(crate) async fn get_key(
        state: Arc<RwLock<HashMap<String, SetMap>>>,
        key: &String,
    ) -> Result<Option<SetMap>, RedisError> {
        let state_ro = state.read().await;
        Ok(state_ro.get(key).cloned())
    }

    pub(crate) async fn set_key(
        state: Arc<RwLock<HashMap<String, SetMap>>>,
        key: String,
        set_map: SetMap,
    ) -> Result<(), RedisError> {
        let mut state_w = state.write().await;
        state_w.insert(key, set_map);
        Ok(())
    }

    pub(crate) async fn unset_key(
        state: Arc<RwLock<HashMap<String, SetMap>>>,
        key: String,
    ) -> Result<(), RedisError> {
        let mut state_w = state.write().await;
        state_w.remove(&key);
        Ok(())
    }

    pub(crate) async fn has_key(
        state: Arc<RwLock<HashMap<String, SetMap>>>,
        key: &String,
    ) -> Result<bool, RedisError> {
        let state_ro = state.read().await;
        Ok(state_ro.contains_key(key))
    }

    pub(crate) async fn get_all_keys(state: Arc<RwLock<HashMap<String, SetMap>>>) -> Result<Vec<String>, RedisError> {
        let mut arr: Vec<String> = Vec::new();
        let state_ro = state.read().await;
        // let s = state_ro.keys();
        for k in state_ro.keys() {
            arr.push(k.to_string());
            // let sd = k.to_string();
        }
        // Ok(state_ro.keys().cloned().collect())
        Ok(arr)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_new() {
        let redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        let config_ro = redis_state.config.read().await;
        let state_ro = redis_state.state.read().await;
        assert_eq!(config_ro.dir.as_ref().unwrap().value, "/data".to_string());
        assert_eq!(
            config_ro.dbfilename.as_ref().unwrap().value,
            "dump.rdb".to_string()
        );
        assert_eq!(state_ro.len(), 0);
    }

    #[tokio::test]
    async fn test_get_config_dir() {
        let redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        assert_eq!(
            RedisState::get_config_dir(redis_state.config.clone())
                .await
                .unwrap()
                .value,
            "/data".to_string()
        );
    }

    #[tokio::test]
    async fn test_get_config_dbfilename() {
        let redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        assert_eq!(
            RedisState::get_config_dbfilename(redis_state.config.clone())
                .await
                .unwrap()
                .value,
            "dump.rdb".to_string()
        );
    }

    #[tokio::test]
    async fn test_set_config_dir() {
        let redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        RedisState::set_config_dir(redis_state.config.clone(), "/tmp".to_string())
            .await
            .unwrap();
        let dir = RedisState::get_config_dir(redis_state.config.clone())
            .await
            .unwrap();
        assert_eq!(dir.value, "/tmp".to_string());
    }

    #[tokio::test]
    async fn test_set_config_dbfilename() {
        let redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        RedisState::set_config_dbfilename(redis_state.config.clone(), "dump2.rdb".to_string())
            .await
            .unwrap();
        let dbfilename = RedisState::get_config_dbfilename(redis_state.config.clone())
            .await
            .unwrap();
        assert_eq!(dbfilename.value, "dump2.rdb".to_string());
    }

    #[tokio::test]
    async fn test_get_key() {
        let redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        let set_map = SetMap {
            expiry: None,
            key: "key".to_string(),
            val: "val".to_string(),
            overwrite: None,
            keepttl: None,
            expiry_timestamp: None,
        };
        let mut state_w = redis_state.state.write().await;
        state_w.insert("key".to_string(), set_map.clone());
        drop(state_w);
        let set_map = RedisState::get_key(redis_state.state.clone(), &"key".to_string())
            .await
            .unwrap();
        assert_eq!(set_map, set_map);
    }

    #[tokio::test]
    async fn test_set_key() {
        let mut redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        let set_map = SetMap {
            expiry: None,
            key: "key".to_string(),
            val: "val".to_string(),
            overwrite: None,
            keepttl: None,
            expiry_timestamp: None,
        };
        RedisState::set_key(
            redis_state.state.clone(),
            "key".to_string(),
            set_map.clone(),
        )
        .await
        .unwrap();
        let set_map = RedisState::get_key(redis_state.state.clone(), &"key".to_string())
            .await
            .unwrap();
        assert_eq!(set_map, set_map);
    }

    #[tokio::test]
    async fn test_unset_key() {
        let mut redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        let set_map = SetMap {
            expiry: None,
            key: "key".to_string(),
            val: "val".to_string(),
            overwrite: None,
            keepttl: None,
            expiry_timestamp: None,
        };
        let mut state_w = redis_state.state.write().await;
        state_w.insert("key".to_string(), set_map.clone());
        drop(state_w);
        RedisState::unset_key(redis_state.state.clone(), "key".to_string())
            .await
            .unwrap();
        let set_map = RedisState::get_key(redis_state.state.clone(), &"key".to_string()).await;
        assert_eq!(RedisState::get_key(redis_state.state.clone(), &"key".to_string()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_has_key() {
        let mut redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        let set_map = SetMap {
            expiry: None,
            key: "key".to_string(),
            val: "val".to_string(),
            overwrite: None,
            keepttl: None,
            expiry_timestamp: None,
        };
        RedisState::set_key(
            redis_state.state.clone(),
            "key".to_string(),
            set_map.clone(),
        ).await.unwrap();
        assert_eq!(
            RedisState::has_key(redis_state.state.clone(), &"key".to_string())
                .await
                .unwrap(),
            true
        );
        assert_eq!(
            RedisState::has_key(redis_state.state.clone(), &"bar".to_string())
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    async fn test_get_all_keys() {
        let mut redis_state = RedisState::new("/data".to_string(), "dump.rdb".to_string());
        let set_map = SetMap {
            expiry: None,
            key: "foo".to_string(),
            val: "bar".to_string(),
            overwrite: None,
            keepttl: None,
            expiry_timestamp: None,
        };
        RedisState::set_key(
            redis_state.state.clone(),
            "foo".to_string(),
            SetMap {
                expiry: None,
                key: "foo".to_string(),
                val: "bar".to_string(),
                overwrite: None,
                keepttl: None,
                expiry_timestamp: None,
            },
        ).await.unwrap();
        RedisState::set_key(
            redis_state.state.clone(),
            "key".to_string(),
            SetMap {
                expiry: None,
                key: "key".to_string(),
                val: "val".to_string(),
                overwrite: None,
                keepttl: None,
                expiry_timestamp: None,
            },
        ).await.unwrap();
        let keys = RedisState::get_all_keys(redis_state.state.clone()).await.unwrap();
        assert_eq!(keys, vec!["key".to_string(), "foo".to_string()]);
    }
}
