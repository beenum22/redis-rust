use futures::{SinkExt, StreamExt};
use bytes::{Bytes, BytesMut};
use log::{debug, error, info, trace, warn};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::broadcast::{self, channel, Receiver, Sender};
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, FramedRead, FramedWrite};
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

use crate::error::{ConnectionError, ReplicaError, ServerError};
use crate::info::{ReplicationInfo, ServerInfo};
use crate::ops::{Operation, Psync, ReplicaConfigOperation};
// use crate::resp::RespType;
use crate::resp::RespParser;
use crate::{
    Config, ConfigParam, InfoOperation, RDBError, RdbParser,
    RedisBuffer, RedisError, State, SetOverwriteArgs,
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
        let (mut ro_tx, mut rx_writer) = stream.into_split();
        let mut reader = FramedRead::new(ro_tx, RespParser::new());
        let mut writer = FramedWrite::new(rx_writer, RespParser::new());
        info!("Connected as replica to {}", server);

        writer.send(Operation::encode(Operation::Ping)?).await?;
        match reader.next().await {
            Some(Ok(msg)) => {
                match Operation::decode(msg)? {
                    Operation::Ok => debug!("Replica server is reachable"),
                    Operation::Error(err) => error!("Replica Ping response failed. {}", err),
                    _ => return Err(RedisError::Replica(ReplicaError::StatusUknown)),
                }
            }
            _ => return Err(RedisError::Replica(ReplicaError::StatusUknown))
        }

        let replconf_port: Vec<ReplicaConfigOperation> =
            vec![ReplicaConfigOperation::ListeningPort(port)];
        writer.send(Operation::encode(Operation::ReplicaConf(replconf_port))?).await?;
        match reader.next().await {
            Some(Ok(msg)) => {
                match Operation::decode(msg)? {
                    Operation::Ok => debug!("Replica listening port successfully configured"),
                    Operation::Error(err) => error!("Failed to configure replica listening port. {}", err),
                    _ => return Err(RedisError::UnknownResponse),
                }
            }
            _ => return Err(RedisError::Replica(ReplicaError::ConfigurationFailed))
        }

        let replconf_capa: Vec<ReplicaConfigOperation> = vec![
            ReplicaConfigOperation::Capabilities("eof".to_string()),
            ReplicaConfigOperation::Capabilities("psync2".to_string()),
        ];
        writer.send(Operation::encode(Operation::ReplicaConf(replconf_capa))?).await?;
        match reader.next().await {
            Some(Ok(msg)) => {
                match Operation::decode(msg)? {
                    Operation::Ok => debug!("Replica eof and psync2 capabilities successfully configured"),
                    Operation::Error(err) => error!("Failed to configure replica capabilities. {}", err),
                    _ => return Err(RedisError::UnknownResponse),
                }
            }
            _ => return Err(RedisError::Replica(ReplicaError::ConfigurationFailed))
        }

        writer.send(Operation::encode(Operation::Psync(Psync::new("?".to_string(), -1)))?).await?;
        match reader.next().await {
            Some(Ok(msg)) => {
                match Operation::decode(msg)? {
                    Operation::Nil => debug!("Replica state resynchronization details successfully shared"),
                    Operation::Error(err) => {
                        error!("Failed to share replica resynchronization details. {}", err)
                    }
                    _ => error!("Failed to share replica resynchronization details. Invalid Resp type received."),
                }
            }
            _ => return Err(RedisError::Replica(ReplicaError::ConfigurationFailed))
        }

        match reader.next().await {
            Some(Ok(msg)) => {
                match Operation::decode(msg)? {
                    Operation::RdbFile(_) => debug!("Replica state synchronization successfully initiated"),
                    Operation::Error(err) => error!("Failed to initiate replica state synchronization. {}", err),
                    _ => error!("Failed to initiate replica state synchronization. Unknown Operation type received.")
                }
            }
            _ => return Err(RedisError::Replica(ReplicaError::ConfigurationFailed))
        }

        match reader.next().await {
            Some(Ok(msg)) => {
                match Operation::decode(msg)? {
                    Operation::ReplicaConf(conf) => {
                        match &conf[0] {
                            ReplicaConfigOperation::GetAck(val) => match val.as_str() {
                                "*" => {
                                    writer.send(Operation::encode(Operation::ReplicaConf(vec![ReplicaConfigOperation::Ack(0)]))?).await?;
                                },
                                _ => error!("Received invalid GETACK argument value for acknowledgement by the master server.")
                            },
                            _ => error!("Received invalid replconf type command for acknowledgement by the master server."),
                        }
                    },
                    _ => error!("Received invalid replconf type command for acknowledgement by the master server."),
                }
            }
            _ => return Err(RedisError::Replica(ReplicaError::ConfigurationFailed))
        }

        debug!("Subscribed to the server at {}", server);
        Self::stream_handler(reader, writer, db, broadcaster).await?;
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
            Operation::ReplicaConf(replconfs) => {
                let mut send_ok = false;
                for conf in replconfs {
                    match conf {
                        ReplicaConfigOperation::GetAck(val) => {
                            if val.as_str() == "*" {
                                actions.push(
                                    Operation::ReplicaConf(vec![ReplicaConfigOperation::Ack(0)])
                                )
                            }
                        },
                        _ => send_ok = true
                    }
                }
                if send_ok {
                    actions.push(Operation::Ok)
                }
            },
            Operation::Psync(val) => match (val.replication_id.as_str(), val.offset as i16) {
                ("?", -1) => {
                    let info = State::get_info(db.info.clone()).await?;
                    actions.push(Operation::Queue);
                    actions.push(Operation::EchoString(format!(
                        "FULLRESYNC {} {}",
                        info.replication.master_replid, info.replication.master_repl_offset
                    )));
                    actions.push(Operation::EchoBytes(Bytes::copy_from_slice(b"REDIS0010\xFA\x03foo\x03bar\xFE\x00\xFD\x61\x56\x4F\x80\x00\x03bar\x03foo\xFF")));
                    actions.push(Operation::RegisterReplica);
                    actions.push(Operation::ReplicaConf(vec![ReplicaConfigOperation::GetAck("*".to_string())]));
                    actions.push(Operation::Subscribe);
                }
                (_, _) => return Err(RedisError::UnknownConfig),
            },
            Operation::Set(set_args) => {
                if State::get_role(db.info.clone()).await? == "master".to_string() {
                    actions.push(Operation::Publish(vec![propagation_copy]));
                };
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
            Operation::Error(_) => actions.push(Operation::Nil),
            _ => return Err(RedisError::UnknownCommand),
        }
        Ok(actions)
    }

    // async fn stream_handler_v2(mut stream: TcpStream, db: Arc<State>, broadcaster: Arc<BroadcasterV2>) -> Result<(), RedisError> {
    async fn stream_handler(mut reader: FramedRead<OwnedReadHalf, RespParser>, mut writer: FramedWrite<OwnedWriteHalf, RespParser>, db: Arc<State>, broadcaster: Arc<Broadcaster>) -> Result<(), RedisError> {
        // let actions: (mpsc::Sender<OperationV2>, mpsc::Receiver<OperationV2>) = mpsc::channel(100);
        // let (req_tx, mut req_rx): (mpsc::Sender<OperationV2>, mpsc::Receiver<OperationV2>) = mpsc::channel(100);
        // let queue: (broadcast::Sender<OperationV2>, broadcast::Receiver<OperationV2>) = broadcast::channel(16);
        let (queue_in, queue_out): (broadcast::Sender<Operation>, broadcast::Receiver<Operation>) = broadcast::channel(16);
        // let queue_in = Arc::new(queue.0);

        let (actions_tx, mut actions_rx): (mpsc::Sender<Operation>, mpsc::Receiver<Operation>) = mpsc::channel(100);

        // let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let reader_db = db.clone();
        tokio::spawn(async move {
            while let Some(result) = reader.next().await {
                trace!("Resp Requested: {:?}-{:?}", result, reader.get_ref().peer_addr());
                match result {
                    Ok(message) => {
                        match Operation::decode(message) {
                            Ok(op) => {
                                trace!("Operation requested: {:?}", op);
                                let actions = Self::action(reader_db.clone(), op).await.unwrap();
                                for act in actions {
                                    if let Err(e) = actions_tx.send(act).await {
                                        error!("Failed to take action for message. {:?}", e);
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to decode Resp message. {:?}", e);
                                if let Err(e) = actions_tx.send(Operation::Error(format!("{:?}", e))).await {
                                    error!("Failed to take action for message. {:?}", e);
                                }
                            },
                        }
                    }
                    Err(err) => {
                        if let Err(e) = actions_tx.send(Operation::Error(format!("{:?}", err))).await {
                            error!("Failed to take action for message. {:?}", e);
                        }
                    }
                }
            }
            // let _ = shutdown_tx.send(()); // Notify writer to shut down
        });

        tokio::spawn(async move {
            while let Some(action) = actions_rx.recv().await {
                trace!("Action to take: {:?}", action);
                match action {
                    Operation::Publish(replica_ops) => {
                        for op in replica_ops {
                            if let Err(e) = broadcaster.publish(op) {
                                error!("Failed to broadcast to the replicas. {:?}.", e);
                            }
                        }
                    },
                    Operation::RegisterReplica => {
                        let db_clone = db.clone();
                        match writer.get_ref().peer_addr().map_err(|_| RedisError::Server(ServerError::TcpStreamFailure)) {
                            Ok(addr) => {
                                if let Err(e) = State::increment_replicas(db_clone.info.clone(), addr).await {
                                    error!("Failed to increment replicas count for {:?} addr. {:?}", addr, e);
                                }
                            },
                            Err(e) => error!("Failed to get stream peer address needed for replica increment. {:?}", e),
                        }
                    },
                    Operation::Queue => {
                        let db_clone = db.clone();
                        let mut broadcast_rx = broadcaster.subscribe();
                        let queue_in_clone = queue_in.clone();
                        match writer.get_ref().peer_addr().map_err(|_| RedisError::Server(ServerError::TcpStreamFailure)) {
                            Ok(addr) => {
                                tokio::spawn(async move {
                                    while let Ok(replica_op) = broadcast_rx.recv().await {
                                        if State::has_replica(db_clone.info.clone(), &addr).await.unwrap() == false {
                                            if let Err(e) = queue_in_clone.send(replica_op) {
                                                error!("Failed to queue published operations for the replica TCP stream. {:?}", e);
                                            }
                                        } else {
                                            break
                                        }
                                    }
                                });
                            },
                            Err(e) => error!("Failed to get stream peer address needed for queue broadcasted operations. {:?}", e),
                        }
                    }
                    Operation::Subscribe => {
                        let db_clone = db.clone();
                        let mut broadcast_rx = broadcaster.subscribe();
                        let queue_in_clone = queue_in.clone();
                        let mut queue_out = queue_in_clone.subscribe();
                        match writer.get_ref().peer_addr().map_err(|_| RedisError::Server(ServerError::TcpStreamFailure)) {
                            Ok(addr) => {
                                while State::has_replica(db_clone.info.clone(), &addr).await.unwrap() == false {
                                    if let Ok(queue_op) = queue_out.recv().await {
                                        if let Err(e) = writer.send(Operation::encode(queue_op).unwrap()).await {
                                            error!("Failed to write queued message to the TCP stream. {:?}", e);
                                        }
                                    }
                                }
                            },
                            Err(e) => error!("Failed to get stream peer address needed for operations subscription. {:?}", e),
                        };
                        while let Ok(replica_op) = broadcast_rx.recv().await {
                            match Operation::encode(replica_op) {
                                Ok(resp) => {
                                    if let Err(e) = writer.send(resp).await {
                                        error!("Failed to write broadcasted message to the TCP stream. {:?}", e);
                                    }
                                },
                                Err(e) => error!("Failed to decode Resp message. {:?}", e),
                            }
                        }
                    },
                    Operation::Ok => {
                        // TODO: Remove hardcoded port
                        if writer.get_ref().peer_addr().map_err(|_| RedisError::Server(ServerError::TcpStreamFailure)).unwrap().port() != 6379 {
                            match Operation::encode(action) {
                                Ok(resp) => {
                                    if let Err(e) = writer.send(resp).await {
                                        error!("Failed to write Ok response to the TCP stream. {:?}", e);
                                    }
                                },
                                Err(e) => error!("Failed to decode Resp message. {:?}", e),
                            }
                        }
                    },
                    _ => {
                        match Operation::encode(action) {
                            Ok(resp) => {
                                if let Err(e) = writer.send(resp).await {
                                    error!("Failed to write message to the TCP stream. {:?}", e);
                                }
                            },
                            Err(e) => error!("Failed to decode Resp message. {:?}", e),
                        }
                        
                    }
                }
                // tokio::select! {
                //     _ = shutdown_rx => {
                //         println!("Writer shutting down.");
                //     }
                // }
                // if let Err(e) = writer.send(OperationV2::encode(action).unwrap()).await {
                //     eprintln!("Error writing to stream: {:?}", e);
                //     break; // Break the loop on error
                // }
            }
        });
        Ok(())
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
                    let db = self.db.clone();
                    let broadcast = self.broadcast.clone();
                    tokio::spawn(async move {
                        let (ro_tx, rx_writer) = stream.into_split();
                        let reader = FramedRead::new(ro_tx, RespParser::new());
                        let mut writer = FramedWrite::new(rx_writer, RespParser::new());
                        Self::stream_handler(reader, writer, db, broadcast).await;
                    });
                },
                Err(e) => {
                    error!("Failed to accept new TCP connection. {}", e);
                }
            }
        }
    }
}
