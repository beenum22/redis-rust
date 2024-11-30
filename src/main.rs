#![allow(unused_imports)]
use bytes::{Bytes, BytesMut};
use tokio::sync::{Mutex, RwLock};
use core::str;
use std::char::ToLowercase;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::usize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

mod resp;
use resp::{RedisResp, RedisRespError, RedisRespType, RedisRespWord, SetMap, SetOverwriteArgs};

struct RedisBuffer {
    index: usize,
    buffer: Bytes,
}

struct RedisState {
    state: RwLock<HashMap<String, SetMap>>
}

struct RedisServer {
    addr: IpAddr,
    port: u16,
    db: Arc<RedisState>
}

impl RedisServer {
    fn new(addr: &str, port: u16) -> Self {
        Self {
            addr: IpAddr::from_str(addr).unwrap(),
            port: port,
            db: Arc::new(RedisState {
                state: RwLock::new(HashMap::new())
            })
        }
    }

    async fn _action(db: Arc<RedisState>, word: RedisRespWord) -> Result<RedisRespWord, RedisRespError> {
        match word {
            RedisRespWord::Ping => Ok(RedisRespWord::Echo("PONG".to_string())),
            RedisRespWord::Echo(_) => Ok(word),
            RedisRespWord::Set(set_args) => {
                match &set_args.overwrite {
                    Some(val) => {
                        let mut db_ro = db.state.read().await; // Get read lock;
                        let key_state = db_ro.get(&set_args.key);
                        match val {
                            SetOverwriteArgs::NX => {
                                if key_state.is_none() {
                                    drop(db_ro);
                                    let mut db_rw = db.state.write().await; // Get write lock
                                    db_rw.insert(set_args.key.clone(), set_args);
                                    Ok(RedisRespWord::Ok)
                                } else {
                                    Ok(RedisRespWord::Nil)
                                }
                            },
                            SetOverwriteArgs::XX => {
                                if key_state.is_some() {
                                    drop(db_ro);
                                    let mut db_rw = db.state.write().await; // Get write lock
                                    db_rw.insert(set_args.key.clone(), set_args);
                                    Ok(RedisRespWord::Ok)
                                } else {
                                    Ok(RedisRespWord::Nil)
                                }
                            }
                        }
                    },
                    None => {
                        let mut db_rw = db.state.write().await; // Get write lock
                        db_rw.insert(set_args.key.clone(), set_args);
                        Ok(RedisRespWord::Ok)
                    },
                }
            },
            RedisRespWord::Get(val) => {
                let mut db = db.state.read().await; // Get read lock
                match db.get(&val) {
                    Some(value_map) => Ok(RedisRespWord::Echo(value_map.val.clone())),
                    None => Ok(RedisRespWord::Nil),
                }
            }
            RedisRespWord::Unknown => Err(RedisRespError::UnknownCommand),
            _ => Err(RedisRespError::UnknownCommand),
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
                let mut resp = RedisResp::new(buff);

                match resp.decode() {
                    Ok(decoded_word) => {
                        match Self::_action(db.clone(), decoded_word).await {
                            Ok(action_word) => match resp.encode(action_word) {
                                Ok(reply) => stream.write_all(reply.as_ref()).await.unwrap(),
                                Err(err) => stream
                                    .write_all(format!("-ERR {:?}\r\n", err).as_bytes())
                                    .await
                                    .unwrap(),
                            } 
                            // stream.write_all(reply.as_ref()).await.unwrap(),
                            Err(err) => stream
                                .write_all(format!("-ERR {:?}\r\n", err).as_bytes())
                                .await
                                .unwrap(),
                        }
                    },
                    Err(err) => stream
                        .write_all(format!("-ERR {:?}\r\n", err).as_bytes())
                        .await
                        .unwrap(),
                };
            }
        });
    }

    async fn run(&self) -> () {
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

#[tokio::main]
async fn main() {
    let redis_server = RedisServer::new("127.0.0.1", 6379);
    redis_server.run().await
}
