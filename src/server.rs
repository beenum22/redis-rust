use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::env::args;
use std::fs::File;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use std::vec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::RwLock;
use log::{info, warn, error, debug};

use crate::error::ConnectionError;
use crate::info::{ReplicationInfo, ServerInfo};
use crate::{
    Config, ConfigOperation, ConfigParam, Operation, RDBError, RdbParser, RedisBuffer, RedisError,
    RedisState, RespParser, SetOverwriteArgs, InfoOperation
};

pub(crate) struct RedisServer {
    addr: IpAddr,
    port: u16,
    db: Arc<RedisState>,
    replica_of: Option<SocketAddr> 
}

impl RedisServer {
    pub(crate) fn new(addr: &str, port: u16, dir: String, dbfilename: String, replica_of: Option<String>) -> Self {
        let remote_server: Option<SocketAddr> = match replica_of {
            Some(replica) => {
                let parts: Vec<&str> = replica.split_whitespace().collect();
                let (host_str, port_str) = (parts[0], parts[1]);
                let replica_port = port_str.parse::<u16>().expect("Invalid port number");
                let replica_host = IpAddr::from_str(host_str).unwrap();  // TODO: Handle errors
                Some(SocketAddr::new(replica_host, replica_port))
            }
            None => None
        };

        let role: String = match remote_server {
            Some(_) => "slave".to_string(),
            None => "master".to_string()
        };

        Self {
            addr: IpAddr::from_str(addr).unwrap(),  // TODO: Handle errors
            port: port,
            db: Arc::new(RedisState::new(dir, dbfilename, role)),
            replica_of: remote_server,
        }
    }

    async fn configure_replica(server: SocketAddr) -> Result<(), RedisError>{
        let mut stream = TcpStream::connect(
            format!("{}:{}",
            server.ip(),
            server.port())
        ).await.map_err(|_| RedisError::Connection(ConnectionError::FailedToWriteBytes))?;
        info!("Connected as replica to {}", server);
        let mut buff = RedisBuffer {
            buffer: Bytes::new(),
            index: 0,
        };
        let ping = RespParser::encode(Operation::EchoArray(
            vec![Operation::Echo("PING".to_string())]
        ))?;
        stream.write_all(ping.as_ref()).await.map_err(|_| RedisError::Connection(ConnectionError::FailedToWriteBytes))?;
        let mut raw_buff: [u8; 512] = [0; 512];
        let data = stream.read(&mut raw_buff).await.unwrap();
        buff.buffer = Bytes::copy_from_slice(&raw_buff[..data]);
        match RespParser::decode(&mut buff) {
            Ok(res) => match res {
                Operation::Ok => debug!("Replica Ping response successful"),
                Operation::Error(err) => error!("Replica Ping response failed. {}", err),
                _ => {
                    error!("Replica Ping response failed");
                    return Err(RedisError::UnknownCommand)
                }
            },
            Err(err) => {
                info!("Ping Resp parsing failed. {:?}", err);
                return Err(RedisError::UnknownCommand)
            }
        }
        Ok(())
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
                        // println!("Debug SetMap {:?}", value_map);
                        debug!("Debug SetMap {:?}", value_map);
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
            Operation::Info(val) => {
                let mut arr: Vec<String> = Vec::new();
                let info: crate::info::Info = RedisState::get_info(db.info.clone()).await?;
                for i in 0..val.len() {
                    match &val[i] {
                        InfoOperation::Replication => {
                            arr.push(ReplicationInfo::get_all(info.replication.clone()).join("\n"))
                        }
                        InfoOperation::Server => {
                            arr.push(ServerInfo::get_all(info.server.clone()).join("\n"))
                        },
                    }
                }
                Ok(Operation::Echo(arr.join("\n")))
            }
            Operation::Nil => Ok(Operation::Nil),
            Operation::Unknown => Err(RedisError::UnknownCommand),
            _ => Err(RedisError::UnknownCommand),
        }
    }

    async fn stream_handler(mut stream: TcpStream, addr: SocketAddr, db: Arc<RedisState>) -> () {
        // println!("New TCP connection from {:?}", addr);
        debug!("New TCP connection from {:?}", addr);
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
                    // println!("TCP connection from {:?} closed", addr);
                    debug!("TCP connection from {:?} closed", addr);
                    break;
                }
                debug!("Received: {:?}", buff.buffer);

                match RespParser::decode(&mut buff) {
                    Ok(decoded_word) => {
                        match Self::_action(db.clone(), decoded_word).await {
                            Ok(action_word) => match RespParser::encode(action_word) {
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
        info!(
            "Redis Server is running on {}:{}",
            self.addr.to_string(),
            self.port
        );
        if let Err(e) = Self::load_rdb(self.db.clone()).await {
            // println!("Failed to load RDB: {:?}", e);
            warn!("Failed to load RDB: {:?}", e);
        }
        if self.replica_of.is_some() {
            Self::configure_replica(self.replica_of.unwrap()).await;
        }
 
        loop {
            let stream = listener.accept().await;
            match stream {
                Ok((stream, addr)) => Self::stream_handler(stream, addr, self.db.clone()).await,
                Err(e) => {
                    // println!("error: {}", e);
                    error!("error: {}", e);
                }
            }
        }
    }
}
