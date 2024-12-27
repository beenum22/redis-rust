use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::env::args;
use std::fs::File;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::RwLock;

use crate::{
    Config, ConfigOperation, ConfigParam, Operation, RDBError, RdbParser, RedisBuffer, RedisError,
    RedisState, RespParser, SetOverwriteArgs,
};

pub(crate) struct RedisServer {
    addr: IpAddr,
    port: u16,
    db: Arc<RedisState>,
}

impl RedisServer {
    pub(crate) fn new(addr: &str, port: u16, dir: String, dbfilename: String) -> Self {
        Self {
            addr: IpAddr::from_str(addr).unwrap(),
            port: port,
            db: Arc::new(RedisState::new(dir, dbfilename)),
        }
    }

    // TODO: Load only if the file exists
    async fn load_rdb(db: Arc<RedisState>) -> Result<(), RedisError> {
        let dir = RedisState::get_config_dir(db.config.clone()).await?.value;
        let dbfilename = RedisState::get_config_dbfilename(db.config.clone())
            .await?
            .value;
        let mut db_file = File::open(format!("{}/{}", dir, dbfilename))
            .map_err(|_| RedisError::RDB(RDBError::DbFileReadError))?;
        match RdbParser::decode(&mut db_file)?.dbs.get(&0) {  
            Some(selectdb) => {
                for i in selectdb.keys.iter() {
                    Self::_action(db.clone(), i.clone()).await?;
                }
            }
            None => (),
        };
        Ok(())
    }

    async fn _action(db: Arc<RedisState>, word: Operation) -> Result<Operation, RedisError> {
        match word {
            Operation::Ping => Ok(Operation::Echo("PONG".to_string())),
            Operation::Echo(_) => Ok(word),
            Operation::Set(set_args) => match &set_args.overwrite {
                Some(val) => {
                    let key_state =
                        RedisState::get_key(db.state.clone(), &set_args.key).await?;
                    match val {
                        SetOverwriteArgs::NX => {
                            if key_state.is_none() {
                                RedisState::set_key(
                                    db.state.clone(),
                                    set_args.key.clone(),
                                    set_args,
                                )
                                .await?;
                                Ok(Operation::Ok)
                            } else {
                                Ok(Operation::Nil)
                            }
                        }
                        SetOverwriteArgs::XX => {
                            if key_state.is_some() {
                                RedisState::set_key(
                                    db.state.clone(),
                                    set_args.key.clone(),
                                    set_args,
                                )
                                .await?;
                                Ok(Operation::Ok)
                            } else {
                                Ok(Operation::Nil)
                            }
                        }
                    }
                }
                None => {
                    RedisState::set_key(db.state.clone(), set_args.key.clone(), set_args).await?;
                    Ok(Operation::Ok)
                }
            },
            Operation::Get(val) => {
                match RedisState::get_key(db.state.clone(), &val).await? {
                    Some(value_map) => {
                        println!("Debug SetMap {:?}", value_map);
                        match value_map.expiry_timestamp {
                            Some(expiry) => {
                                let now = SystemTime::now();
                                match now > expiry {
                                    false => Ok(Operation::Echo(value_map.val.clone())),
                                    true => {
                                        RedisState::get_key(db.state.clone(), &val)
                                            .await?;
                                        Ok(Operation::Nil)
                                    }
                                }
                            }
                            None => Ok(Operation::Echo(value_map.val.clone())),
                        }
                    }
                    None => Ok(Operation::Nil),
                }
            }
            Operation::Config(val_arr) => {
                if val_arr.len() == 0 {
                    return Err(RedisError::UnknownConfig);
                }
                match &val_arr[0] {
                    ConfigOperation::Get(_) => {
                        let mut config_arr: Vec<Operation> = Vec::new();
                        for i in 0..val_arr.len() {
                            match &val_arr[i] {
                                ConfigOperation::Get(config_param) => {
                                    match config_param {
                                        ConfigParam::Dir(_) => {
                                            match RedisState::get_config_dir(db.config.clone()).await {
                                                Ok(value) => {
                                                    config_arr
                                                        .push(Operation::Echo(value.key.to_string()));
                                                    config_arr
                                                        .push(Operation::Echo(value.value.to_string()));
                                                }
                                                Err(_) => (),
                                                
                                            }
                                        }
                                        ConfigParam::DbFileName(_) => {
                                            match RedisState::get_config_dbfilename(db.config.clone()).await {
                                                Ok(value) => {
                                                    config_arr
                                                        .push(Operation::Echo(value.key.to_string()));
                                                    config_arr
                                                        .push(Operation::Echo(value.value.to_string()));
                                                }
                                                Err(_) => (),
                                            }
                                        }
                                        _ => (),
                                    }
                                }
                                _ => (),
                            }
                        }
                        Ok(Operation::EchoArray(config_arr))
                    }
                    ConfigOperation::Set(_) => {
                        for i in 0..val_arr.len() {
                            match &val_arr[i] {
                                ConfigOperation::Set(config_param) => match config_param {
                                    ConfigParam::Dir(val) => {
                                        RedisState::set_config_dir(
                                            db.config.clone(),
                                            val.clone().unwrap().value.clone(),
                                        ).await?;
                                    }
                                    ConfigParam::DbFileName(val) => {
                                        RedisState::set_config_dbfilename(
                                            db.config.clone(),
                                            val.clone().unwrap().value.clone(),
                                        ).await?;
                                    }
                                    _ => (),
                                },
                                _ => (),
                            }
                        }
                        Ok(Operation::Ok)
                    }
                }
            }
            Operation::Keys(key) => {
                let mut arr: Vec<Operation> = Vec::new();
                match key.as_str() {
                    "*" => {
                        for k in RedisState::get_all_keys(db.state.clone()).await? {
                            arr.push(Operation::Echo(k))
                        }
                    }
                    _ => {
                        if RedisState::has_key(db.state.clone(), &key).await? {
                            arr.push(Operation::Echo(key))
                        }
                    }
                }
                Ok(Operation::EchoArray(arr))
            }
            Operation::Unknown => Err(RedisError::UnknownCommand),
            _ => Err(RedisError::UnknownCommand),
        }
    }

    async fn stream_handler(mut stream: TcpStream, addr: SocketAddr, db: Arc<RedisState>) -> () {
        println!("New TCP connection from {:?}", addr);
        tokio::spawn(async move {
            let mut raw_buff: [u8; 512] = [0; 512];
            loop {
                let mut buff = RedisBuffer {
                    buffer: Bytes::new(),
                    index: 0,
                };
                let data = stream.read(&mut raw_buff).await.unwrap();
                buff.buffer = Bytes::copy_from_slice(&raw_buff[..data]);

                if data == 0 {
                    println!("TCP connection from {:?} closed", addr);
                    break;
                }
                let mut resp = RespParser::new(buff);

                match resp.decode() {
                    Ok(decoded_word) => {
                        match Self::_action(db.clone(), decoded_word).await {
                            Ok(action_word) => match resp.encode(action_word) {
                                Ok(reply) => stream.write_all(reply.as_ref()).await.unwrap(),
                                Err(err) => stream
                                    .write_all(format!("-ERR {:?}\r\n", err).as_bytes())
                                    .await
                                    .unwrap(),
                            },
                            // stream.write_all(reply.as_ref()).await.unwrap(),
                            Err(err) => stream
                                .write_all(format!("-ERR {:?}\r\n", err).as_bytes())
                                .await
                                .unwrap(),
                        }
                    }
                    Err(err) => stream
                        .write_all(format!("-ERR {:?}\r\n", err).as_bytes())
                        .await
                        .unwrap(),
                };
            }
        });
    }

    pub(crate) async fn run(&self) -> () {
        let listener = TcpListener::bind(format!("{}:{}", self.addr.to_string(), self.port))
            .await
            .unwrap();
        println!(
            "Redis Server is running on {}:{}",
            self.addr.to_string(),
            self.port
        );
        if let Err(e) = Self::load_rdb(self.db.clone()).await {
            println!("Failed to load RDB: {:?}", e);
        }
        loop {
            let stream = listener.accept().await;
            match stream {
                Ok((stream, addr)) => Self::stream_handler(stream, addr, self.db.clone()).await,
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}
