use bytes::{Bytes, BytesMut};
use log::{debug, error, info, trace, warn};
use tokio::sync::broadcast::{channel, Receiver, Sender};
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

use crate::error::{ConnectionError, ReplicaError};
use crate::info::{ReplicationInfo, ServerInfo};
use crate::ops::{Psync, ReplicaConfigOperation};
use crate::resp::RespType;
use crate::{
    Config, ConfigParam, InfoOperation, Operation, RDBError, RdbParser,
    RedisBuffer, RedisError, State, RespParser, SetOverwriteArgs,
};

pub(crate) struct Broadcaster {
    sender: Sender<Operation>,
    receiver: Receiver<Operation>,
}

impl Broadcaster {
    pub(crate) fn new(channels: usize) -> Self {
        let (sender, receiver) = channel(channels);
        Self {
            sender,
            receiver,
        }
    }

    pub(crate) fn subscribe(&self) -> Receiver<Operation> {
        self.sender.subscribe()
    }

    pub(crate) fn publish(&self, op: Operation) -> Result<usize, RedisError> {
        self.sender.send(op).map_err(|_| RedisError::Replica(ReplicaError::BroadcastFailed))
    }
}

enum Node {
    Master(SocketAddr),
    Slave(SocketAddr, SocketAddr),
}

impl Node {
    fn new(addr: &str, port: u16, replica_of: Option<String>) -> Self {
        match replica_of {
            Some(replica) => {
                let parts: Vec<&str> = replica.split_whitespace().collect();
                let (host_str, port_str) = (parts[0], parts[1]);
                Self::Slave(
                    format!("{addr}:{port}").to_socket_addrs().expect("Invalid socket address").next().unwrap(),
                    format!("{host_str}:{port_str}").to_socket_addrs().expect("Invalid socket address").next().unwrap()
                )
            }
            None => Self::Master(
                format!("{addr}:{port}").to_socket_addrs().expect("Invalid socket address").next().unwrap(),
            ),
        }
    }

    fn role(&self) -> String {
        match self {
            Node::Master(_) => "master".to_string(),
            Node::Slave(_, _) => "slave".to_string(),
        }
    }

    fn socket(&self) -> SocketAddr {
        match self {
            Node::Master(addr) => addr.clone(),
            Node::Slave(addr, _) => addr.clone(),
        }
    }

    fn ip(&self) -> IpAddr {
        match self {
            Node::Master(addr) => addr.ip(),
            Node::Slave(addr, _) => addr.ip(),
        }
    }

    fn port(&self) -> u16 {
        match self {
            Node::Master(addr) => addr.port(),
            Node::Slave(addr, _) => addr.port(),
        }
    }


}

pub(crate) struct RedisServer {
    node: Node,
    db: Arc<State>,
    broadcast: Arc<Broadcaster>,
}

impl RedisServer {
    pub(crate) fn new(
        addr: &str,
        port: u16,
        dir: String,
        dbfilename: String,
        replica_of: Option<String>,
    ) -> Self {
        let node = Node::new(addr, port, replica_of);
        let role = node.role();
        Self {
            // TODO: Handle Result and Option errors
            node,
            db: Arc::new(State::new(dir, dbfilename, role)),
            broadcast: Arc::new(Broadcaster::new(100)),
        }
    }

    async fn _receive_msg(stream: &mut TcpStream) -> Result<RedisBuffer, RedisError> {
        let mut buff = RedisBuffer {
            buffer: Bytes::new(),
            index: 0,
        };
        let mut raw_buff: [u8; 512] = [0; 512];
        let data = stream
            .read(&mut raw_buff)
            .await
            .map_err(|_| RedisError::Connection(ConnectionError::FailedToReadBytes))?;
        buff.buffer = Bytes::copy_from_slice(&raw_buff[..data]);
        Ok(buff)
    }

    async fn _send_msg(stream: &mut TcpStream, msg: &[u8]) -> Result<(), RedisError> {
        stream
            .write_all(msg.as_ref())
            .await
            .map_err(|_| RedisError::Connection(ConnectionError::FailedToWriteBytes))?;
        Ok(())
    }

    // TODO: All commands are executed serially over one TCP session. Check if separate would be better. You might need to flush the buffer.
    async fn configure_replica(db: Arc<State>, broadcaster: Arc<Broadcaster>, port: u16, server: SocketAddr) -> Result<(), RedisError> {
        let mut stream = TcpStream::connect(server)
            .await
            .map_err(|_| RedisError::Connection(ConnectionError::FailedToWriteBytes))?;
        info!("Connected as replica to {}", server);

        let ping = RespParser::encode(Operation::Ping)?;
        Self::_send_msg(&mut stream, ping.as_ref()).await?;
        let mut buff = Self::_receive_msg(&mut stream).await?;
        match RespParser::decode(&mut buff)? {
            Operation::Ok => debug!("Replica server is reachable"),
            Operation::Error(err) => error!("Replica Ping response failed. {}", err),
            _ => return Err(RedisError::UnknownResponse),
        }

        let replconf_port: Vec<ReplicaConfigOperation> =
            vec![ReplicaConfigOperation::ListeningPort(port)];
        let replconf_port_enc = RespParser::encode(Operation::ReplicaConf(replconf_port))?;
        Self::_send_msg(&mut stream, &replconf_port_enc.as_ref()).await?;
        match RespParser::decode(&mut Self::_receive_msg(&mut stream).await?)? {
            Operation::Ok => debug!("Replica listening port successfully configured"),
            Operation::Error(err) => error!("Failed to configure replica listening port. {}", err),
            _ => return Err(RedisError::UnknownResponse),
        }

        let replconf_capa: Vec<ReplicaConfigOperation> = vec![
            ReplicaConfigOperation::Capabilities("eof".to_string()),
            ReplicaConfigOperation::Capabilities("psync2".to_string()),
        ];
        let replconf_capa_enc = RespParser::encode(Operation::ReplicaConf(replconf_capa))?;
        Self::_send_msg(&mut stream, &replconf_capa_enc.as_ref()).await?;
        match RespParser::decode(&mut Self::_receive_msg(&mut stream).await?)? {
            Operation::Ok => debug!("Replica eof and psync2 capabilities successfully configured"),
            Operation::Error(err) => error!("Failed to configure replica capabilities. {}", err),
            _ => return Err(RedisError::UnknownResponse),
        }

        let psync = RespParser::encode(Operation::Psync(Psync::new("?".to_string(), -1)))?;
        Self::_send_msg(&mut stream, &psync.as_ref()).await?;
        match RespParser::decode(&mut Self::_receive_msg(&mut stream).await?)? {
            Operation::Nil => debug!("Replica state resynchronization details successfully shared"),
            Operation::Error(err) => {
                error!("Failed to share replica resynchronization details. {}", err)
            }
            _ => return Err(RedisError::UnknownResponse),
        }

        match RespParser::decode(&mut Self::_receive_msg(&mut stream).await?)? {
            Operation::RdbFile(val) => {
                debug!("Replica state synchronization successfully initiated")
            }
            Operation::Error(err) => {
                error!("Failed to initiate replica state synchronization. {}", err)
            }
            _ => return Err(RedisError::UnknownResponse),
        }
        
        debug!("Subscribed to the server at {}", server);
        Self::stream_handler(stream, db, broadcaster).await;
        Ok(())
    }

    // TODO: Load only if the file exists
    async fn load_rdb(db: Arc<State>) -> Result<(), RedisError> {
        let dir = State::get_config_dir(db.config.clone()).await?.value;
        let dbfilename = State::get_config_dbfilename(db.config.clone())
            .await?
            .value;
        let mut db_file = File::open(format!("{}/{}", dir, dbfilename))
            .map_err(|_| RedisError::RDB(RDBError::DbFileReadError))?;
        match RdbParser::decode(&mut db_file)?.dbs.get(&0) {
            Some(selectdb) => {
                for i in selectdb.keys.iter() {
                    Self::action(db.clone(), i.clone()).await?;
                }
            }
            None => (),
        };
        Ok(())
    }

    async fn action(db: Arc<State>, word: Operation) -> Result<Vec<Operation>, RedisError> {
        let mut actions: Vec<Operation> = Vec::new();
        let propagation_copy = word.clone();
        match word {
            Operation::Ping => actions.push(Operation::EchoString("PONG".to_string())),
            Operation::Echo(_) => actions.push(word),
            Operation::ReplicaConf(_) => actions.push(Operation::Ok), // TODO: Parse later.
            Operation::Psync(val) => match (val.replication_id.as_str(), val.offset as i16) {
                ("?", -1) => {
                    let info = State::get_info(db.info.clone()).await?;
                    actions.push(Operation::EchoString(format!(
                        "FULLRESYNC {} {}",
                        info.replication.master_replid, info.replication.master_repl_offset
                    )));
                    actions.push(Operation::EchoBytes(Bytes::copy_from_slice(b"REDIS0010\xFA\x03foo\x03bar\xFE\x00\xFD\x61\x56\x4F\x80\x00\x03bar\x03foo\xFF")));
                    actions.push(Operation::Subscribe);
                }
                (_, _) => return Err(RedisError::UnknownConfig),
            },
            Operation::Set(set_args) => {
                actions.push(Operation::Publish(vec![propagation_copy]));
                match &set_args.overwrite {
                    Some(val) => {
                        let key_state = State::get_key(db.state.clone(), &set_args.key).await?;
                        match val {
                            SetOverwriteArgs::NX => {
                                if key_state.is_none() {
                                    State::set_key(
                                        db.state.clone(),
                                        set_args.key.clone(),
                                        set_args,
                                    )
                                    .await?;
                                    actions.push(Operation::Ok)
                                } else {
                                    actions.push(Operation::Nil)
                                }
                            }
                            SetOverwriteArgs::XX => {
                                if key_state.is_some() {
                                    State::set_key(
                                        db.state.clone(),
                                        set_args.key.clone(),
                                        set_args,
                                    )
                                    .await?;
                                    actions.push(Operation::Ok)
                                } else {
                                    actions.push(Operation::Nil)
                                }
                            }
                        }
                    }
                    None => {
                        State::set_key(db.state.clone(), set_args.key.clone(), set_args).await?;
                        actions.push(Operation::Ok)
                    }
                }
            },
            Operation::Get(val) => {
                match State::get_key(db.state.clone(), &val).await? {
                    Some(value_map) => {
                        // println!("Debug SetMap {:?}", value_map);
                        debug!("Debug SetMap {:?}", value_map);
                        match value_map.expiry_timestamp {
                            Some(expiry) => {
                                let now = SystemTime::now();
                                match now > expiry {
                                    false => actions.push(Operation::Echo(value_map.val.clone())),
                                    true => {
                                        State::get_key(db.state.clone(), &val).await?;
                                        actions.push(Operation::Nil)
                                    }
                                }
                            }
                            None => actions.push(Operation::Echo(value_map.val.clone())),
                        }
                    }
                    None => actions.push(Operation::Nil),
                }
            }
            Operation::Config(config_op) => {
                match config_op {
                    crate::config::ConfigOperation::Get(configs) => {
                        if configs.len() == 0 {
                            return Err(RedisError::UnknownConfig);
                        }
                        let mut echos: Vec<Operation> = Vec::new();
                        for config in configs {
                            match config {
                                ConfigParam::Dir(_) => {
                                    match State::get_config_dir(db.config.clone()).await {
                                        Ok(value) => {
                                            echos
                                                .push(Operation::Echo(value.key.to_string()));
                                            echos
                                                .push(Operation::Echo(value.value.to_string()));
                                        }
                                        Err(_) => return Err(RedisError::MissingArgs),
                                    }
                                }
                                ConfigParam::DbFileName(_) => {
                                    match State::get_config_dbfilename(db.config.clone())
                                        .await
                                    {
                                        Ok(value) => {
                                            echos
                                                .push(Operation::Echo(value.key.to_string()));
                                            echos
                                                .push(Operation::Echo(value.value.to_string()));
                                        }
                                        Err(_) => (),
                                    }
                                }
                                _ => (),
                            }
                        };
                        actions.push(Operation::EchoArray(echos))
                    },
                    crate::config::ConfigOperation::Set(configs) => {
                        if configs.len() == 0 {
                            return Err(RedisError::UnknownConfig);
                        }

                        for config in configs {
                            match config {
                                ConfigParam::Dir(val) => {
                                    State::set_config_dir(
                                        db.config.clone(),
                                        val.clone().unwrap().value.clone(),
                                    )
                                    .await?;
                                }
                                ConfigParam::DbFileName(val) => {
                                    State::set_config_dbfilename(
                                        db.config.clone(),
                                        val.clone().unwrap().value.clone(),
                                    )
                                    .await?;
                                }
                                _ => (),
                            }
                        }
                        actions.push(Operation::Ok)
                    },
                }
            },
            Operation::Keys(key) => {
                let mut arr: Vec<Operation> = Vec::new();
                match key.as_str() {
                    "*" => {
                        for k in State::get_all_keys(db.state.clone()).await? {
                            arr.push(Operation::Echo(k))
                        }
                    }
                    _ => {
                        if State::has_key(db.state.clone(), &key).await? {
                            arr.push(Operation::Echo(key))
                        }
                    }
                }
                actions.push(Operation::EchoArray(arr))
            }
            Operation::Info(val) => {
                let mut arr: Vec<String> = Vec::new();
                let info: crate::info::Info = State::get_info(db.info.clone()).await?;
                for i in 0..val.len() {
                    match &val[i] {
                        InfoOperation::Replication => {
                            arr.push(ReplicationInfo::get_all(info.replication.clone()).join("\n"))
                        }
                        InfoOperation::Server => {
                            arr.push(ServerInfo::get_all(info.server.clone()).join("\n"))
                        }
                    }
                }
                actions.push(Operation::Echo(arr.join("\n")))
            }
            Operation::Nil => actions.push(Operation::Nil),
            _ => return Err(RedisError::UnknownCommand),
        }
        Ok(actions)
    }

    async fn encode_and_send(stream: &mut TcpStream, operation: Operation) -> Result<(), RedisError> {
        match RespParser::encode(operation) {
            Ok(reply) => {
                trace!("Bytes to send: {:?}", reply);
                Self::_send_msg(stream, reply.as_ref()).await?;
                Ok(())
            }
            Err(err) => {
                let err_msg = RespParser::encode(Operation::Error(format!("{:?}", err)))?;
                Self::_send_msg(stream, &err_msg.as_ref()).await?;
                Ok(())
            }
        }
    }

    async fn stream_handler(mut stream: TcpStream, db: Arc<State>, broadcaster: Arc<Broadcaster>) -> () {
        tokio::spawn(async move {
            loop {
                let mut buff = match Self::_receive_msg(&mut stream).await {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to receive message from the TCP stream. {:?}", e);
                        continue
                    },
                };

                if buff.buffer.len() == 0 {
                    debug!("TCP connection from {:?} closed", stream.local_addr().unwrap());  // TODO: Handle unwrap() in case of error.
                    break;
                }
                trace!("Received Bytes: {:?}", buff.buffer);

                match RespParser::decode(&mut buff) {
                    Ok(decoded_word) => {
                        let actions = Self::action(db.clone(), decoded_word).await;
                        match actions {
                            Ok(action_word) => {
                                for action in action_word {
                                    trace!("Action to perform: {:?}", action);
                                    match action {
                                        Operation::Publish(replica_ops) => {
                                            for op in replica_ops {
                                                if let Err(e) = broadcaster.publish(op) {
                                                    error!("Failed to broadcast to the replicas. {:?}.", e);
                                                }
                                            }
                                        },
                                        Operation::Subscribe => {
                                            if let Err(e) = State::increment_replicas(db.info.clone()).await {
                                                error!("Failed to update the replica count. {:?}", e);  // TODO: Should I stop further parsing?
                                            }
                                            let mut receiver = broadcaster.subscribe();
                                            while let Ok(replica_op) = receiver.recv().await {
                                                if let Err(e) = Self::encode_and_send(&mut stream, replica_op).await {
                                                    error!("Failed to write message to the TCP stream. {:?}", e);
                                                }
                                            }
                                        }
                                        Operation::Ok => {
                                            // TODO: Handle unwrap error.
                                            if stream.peer_addr().unwrap().port() != 6379 {
                                                if let Err(e) = Self::encode_and_send(&mut stream, action).await {
                                                    error!("Failed to write message to the TCP stream. {:?}", e);
                                                }
                                            }
                                        },
                                        _ => {
                                            if let Err(e) = Self::encode_and_send(&mut stream, action).await {
                                                error!("Failed to write message to the TCP stream. {:?}", e);
                                            }
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                if let Err(e) = Self::encode_and_send(&mut stream, Operation::Error(format!("{:?}", err))).await {
                                    error!("Failed to write message to the TCP stream. {:?}", e);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        if let Err(e) = Self::encode_and_send(&mut stream, Operation::Error(format!("{:?}", err))).await {
                            error!("Failed to write message to the TCP stream. {:?}", e);
                        }
                    }
                };
            }
        });
    }

    pub(crate) async fn run(&self) {
        let listener = TcpListener::bind(self.node.socket()).await.unwrap();
        info!(
            "Redis Server is running on {}:{}",
            self.node.ip(),
            self.node.port(),
        );
        if let Err(e) = Self::load_rdb(self.db.clone()).await {
            warn!("Failed to load RDB: {:?}", e);
        }

        match self.node {
            Node::Slave(addr, remote_addr) => {
                let db = self.db.clone();
                let broadcast = self.broadcast.clone();
                tokio::spawn(async move {
                    if let Err(e) = Self::configure_replica(db, broadcast, addr.port(), remote_addr).await {
                        error!("Failed to configure replica: {:?}", e);
                    }
                });
            },
            _ => ()
        };

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("New TCP connection from {:?}", addr);
                    Self::stream_handler(stream, self.db.clone(), self.broadcast.clone()).await;
                },
                Err(e) => {
                    error!("Failed to accept new TCP connection. {}", e);
                }
            }
        }
    }
}
