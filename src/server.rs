use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::env::args;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::RwLock;

use crate::{
    Config, ConfigOperation, ConfigParam, Operation, RedisBuffer, RedisError, RedisState,
    RespParser, SetOverwriteArgs,
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
            db: Arc::new(RedisState::new(
                RwLock::new(HashMap::new()),
                RwLock::new(Config::new(dir, dbfilename)),
            )),
        }
    }

    async fn _action(db: Arc<RedisState>, word: Operation) -> Result<Operation, RedisError> {
        match word {
            Operation::Ping => Ok(Operation::Echo("PONG".to_string())),
            Operation::Echo(_) => Ok(word),
            Operation::Set(set_args) => {
                match &set_args.overwrite {
                    Some(val) => {
                        let db_ro = db.state.read().await; // Get read lock;
                        let key_state = db_ro.get(&set_args.key);
                        match val {
                            SetOverwriteArgs::NX => {
                                if key_state.is_none() {
                                    drop(db_ro);
                                    let mut db_rw = db.state.write().await; // Get write lock
                                    db_rw.insert(set_args.key.clone(), set_args);
                                    drop(db_rw);
                                    Ok(Operation::Ok)
                                } else {
                                    Ok(Operation::Nil)
                                }
                            }
                            SetOverwriteArgs::XX => {
                                if key_state.is_some() {
                                    drop(db_ro);
                                    let mut db_rw = db.state.write().await; // Get write lock
                                    db_rw.insert(set_args.key.clone(), set_args);
                                    drop(db_rw);
                                    Ok(Operation::Ok)
                                } else {
                                    Ok(Operation::Nil)
                                }
                            }
                        }
                    }
                    None => {
                        let mut db_rw = db.state.write().await; // Get write lock
                        db_rw.insert(set_args.key.clone(), set_args);
                        drop(db_rw);
                        Ok(Operation::Ok)
                    }
                }
            }
            Operation::Get(val) => {
                let db_ro = db.state.read().await; // Get read lock
                match db_ro.get(&val) {
                    Some(value_map) => {
                        match value_map.expiry_timestamp {
                            Some(expiry) => {
                                let now = SystemTime::now();
                                match now > expiry {
                                    false => Ok(Operation::Echo(value_map.val.clone())),
                                    true => {
                                        drop(db_ro);
                                        let mut db_rw = db.state.write().await; // Get write lock
                                        db_rw.remove(&val);
                                        drop(db_rw);
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
                                            let config_ro = db.config.read().await; // Get read lock
                                            match &config_ro.dir {
                                                Some(value) => {
                                                    config_arr
                                                        .push(Operation::Echo(value.0.to_string()));
                                                    config_arr
                                                        .push(Operation::Echo(value.1.to_string()));
                                                }
                                                None => (),
                                            }
                                        }
                                        ConfigParam::DbFileName(_) => {
                                            let config_ro = db.config.read().await; // Get read lock
                                            match &config_ro.dbfilename {
                                                Some(value) => {
                                                    config_arr
                                                        .push(Operation::Echo(value.0.to_string()));
                                                    config_arr
                                                        .push(Operation::Echo(value.1.to_string()));
                                                }
                                                None => (),
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
                                        let mut config_rw = db.config.write().await; // Get write lock
                                        config_rw.dir = val.clone();
                                    }
                                    ConfigParam::DbFileName(val) => {
                                        let mut config_rw = db.config.write().await; // Get write lock
                                        config_rw.dbfilename = val.clone();
                                    }
                                    _ => (),
                                },
                                _ => (),
                            }
                        }
                        Ok(Operation::Ok)
                    }
                }
            },
            Operation::Keys(key) => {
                let db_ro = db.state.read().await; // Get read lock
                let mut arr: Vec<Operation> = Vec::new();
                match key.as_str() {
                    "*" => {
                        for k in db_ro.keys() {
                            arr.push(Operation::Echo(k.to_string()))
                        }
                    },
                    _ => {
                        if db_ro.contains_key(&key) {
                            arr.push(Operation::Echo(key))
                        }
                    }
                }
                Ok(Operation::EchoArray(arr))
            },
            Operation::Unknown => Err(RedisError::UnknownCommand),
            _ => Err(RedisError::UnknownCommand),
        }
    }

    async fn stream_handler(mut stream: TcpStream, addr: SocketAddr, db: Arc<RedisState>) {
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
                println!("{:?}", &buff.buffer);
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
