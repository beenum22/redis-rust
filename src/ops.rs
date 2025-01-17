use bytes::Bytes;
use tokio_util::codec::Decoder;
use std::{
    io::Cursor,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use log::trace;

use crate::{
    config::{ConfigOperation, ConfigPair, ConfigParam}, error::{OperationError, RDBError, RedisError}, info::{InfoOperation, ReplicationInfo}, rdb::RdbParser, resp::RespType, state::{SetExpiryArgs, SetMap, SetOverwriteArgs}, RedisBuffer
};

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum ReplicaConfigOperation {
    ListeningPort(u16),
    Capabilities(String),
    GetAck(String),
    Ack(u16),
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Psync {
    pub(crate) replication_id: String,
    pub(crate) offset: i16,
}

impl Psync {
    pub(crate) fn new(id: String, offset: i16) -> Self {
        Self {
            replication_id: id,
            offset: offset,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum MessageType {
    Read,
    Write,
    Internal,
}

impl MessageType {
    pub(crate) fn get_type(op: &Operation) -> MessageType {
        match &op {
            Operation::Ping => MessageType::Read,
            Operation::ReplicaConf(_) => MessageType::Write,
            Operation::Psync(_) => MessageType::Write,  // Not sure.
            Operation::Echo(_) => MessageType::Read,
            Operation::EchoString(_) => MessageType::Read,
            Operation::EchoBytes(_) => MessageType::Read,
            Operation::Set(_) => MessageType::Write,
            Operation::Get(_) => MessageType::Read,
            Operation::Config(config) => match config {
                ConfigOperation::Get(_) => MessageType::Read,
                ConfigOperation::Set(_) => MessageType::Write,
            },
            Operation::Keys(_) => MessageType::Read,
            Operation::Ok => MessageType::Read,
            Operation::Nil => MessageType::Read,
            Operation::EchoArray(_) => MessageType::Read,
            Operation::Info(_) => MessageType::Read,
            Operation::Error(_) => MessageType::Read,
            _ => MessageType::Internal
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Operation {
    Ping,
    ReplicaConf(Vec<ReplicaConfigOperation>),
    Psync(Psync),
    Publish(Vec<Operation>),  // Internal operation
    Subscribe,  // Internal operation
    Queue,  // Internal operation
    RegisterReplica,  // Internal operation
    Echo(String),
    EchoString(String),
    EchoBytes(Bytes),
    Set(SetMap),
    Get(String),
    Config(ConfigOperation),
    Keys(String),
    Ok,
    Nil,
    EchoArray(Vec<Operation>),
    Info(Vec<InfoOperation>),
    RdbFile(RdbParser),
    Error(String),
    // Unknown,
}

impl Operation {
    fn decode_set(args: &[RespType]) -> Result<SetMap, RedisError> {
        let mut set_args = SetMap {
            expiry: None,
            key: String::new(),
            val: String::new(),
            overwrite: None,
            keepttl: None,
            expiry_timestamp: None,
        };

        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        }
        set_args.key = match &args[0] {
            RespType::BulkString(val) => (*val).to_string(),
            _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
        };
        set_args.val = match &args[1] {
            RespType::BulkString(val) => (*val).to_string(),
            _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
        };
        let mut i: usize = 2;
        while i < args.len() {
            match &args[i] {
                RespType::BulkString(val) => {
                    match val.to_lowercase().as_str() {
                        "ex" => {
                            if set_args.expiry.is_some() {
                                return Err(RedisError::SyntaxError);
                            }
                            i += 1; // Move to next arg value
                            let expiry = args.get(i);
                            match expiry {
                                Some(val) => match val {
                                    RespType::BulkString(arg_val) => {
                                        let expiry_secs: u64 = arg_val.parse().map_err(|err| {
                                            RedisError::InvalidArgValue(
                                                "value is not an integer or out of range"
                                                    .to_string(),
                                            )
                                        })?;
                                        set_args.expiry_timestamp = Some(
                                            SystemTime::now() + Duration::from_secs(expiry_secs),
                                        );
                                        set_args.expiry = Some(SetExpiryArgs::EX(expiry_secs))
                                    }
                                    _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
                                },
                                None => return Err(RedisError::SyntaxError),
                            };
                        }
                        "px" => {
                            if set_args.expiry.is_some() {
                                return Err(RedisError::SyntaxError);
                            }
                            i += 1; // Move to next arg value
                            let expiry = args.get(i);
                            match expiry {
                                Some(val) => match val {
                                    RespType::BulkString(arg_val) => {
                                        let expiry: u128 = arg_val.parse().map_err(|err| {
                                            RedisError::InvalidArgValue(
                                                "value is not an integer or out of range"
                                                    .to_string(),
                                            )
                                        })?;
                                        set_args.expiry_timestamp = Some(
                                            SystemTime::now()
                                                + Duration::from_millis(expiry as u64),
                                        );
                                        set_args.expiry = Some(SetExpiryArgs::PX(expiry))
                                    }
                                    _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
                                },
                                None => return Err(RedisError::SyntaxError),
                            };
                        }
                        "exat" => {
                            if set_args.expiry.is_some() {
                                return Err(RedisError::SyntaxError);
                            }
                            i += 1; // Move to next arg value
                            let expiry = args.get(i);
                            match expiry {
                                Some(val) => match val {
                                    RespType::BulkString(arg_val) => {
                                        let expiry: u64 = arg_val.parse().map_err(|err| {
                                            RedisError::InvalidArgValue(
                                                "value is not an integer or out of range"
                                                    .to_string(),
                                            )
                                        })?;
                                        set_args.expiry_timestamp =
                                            Some(UNIX_EPOCH + Duration::from_secs(expiry));
                                        set_args.expiry = Some(SetExpiryArgs::EXAT(expiry))
                                    }
                                    _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
                                },
                                None => return Err(RedisError::SyntaxError),
                            };
                        }
                        "pxat" => {
                            if set_args.expiry.is_some() {
                                return Err(RedisError::SyntaxError);
                            }
                            i += 1; // Move to next arg value
                            let expiry = args.get(i);
                            match expiry {
                                Some(val) => match val {
                                    RespType::BulkString(arg_val) => {
                                        let mut expiry: u128 = arg_val.parse().map_err(|err| {
                                            RedisError::InvalidArgValue(
                                                "value is not an integer or out of range"
                                                    .to_string(),
                                            )
                                        })?;
                                        set_args.expiry = Some(SetExpiryArgs::PXAT(expiry));
                                        // Temporary fix added to handle timestamps in nanoseconds.
                                        if expiry >= 10_000_000_000_000_000 {
                                            set_args.expiry_timestamp = Some(
                                                UNIX_EPOCH + Duration::from_nanos(expiry as u64),
                                            );
                                        } else {
                                            set_args.expiry_timestamp = Some(
                                                UNIX_EPOCH + Duration::from_millis(expiry as u64),
                                            );
                                        }
                                    }
                                    _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
                                },
                                None => return Err(RedisError::SyntaxError),
                            };
                        }
                        "nx" => set_args.overwrite = Some(SetOverwriteArgs::NX),
                        "xx" => set_args.overwrite = Some(SetOverwriteArgs::XX),
                        "keepttl" => set_args.keepttl = Some(true),
                        _ => return Err(RedisError::SyntaxError),
                    }
                }
                _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
            }
            i += 1;
        }
        Ok(set_args)
    }

    fn encode_set(set_map: SetMap) -> Result<RespType, RedisError> {
        let mut arr: Vec<RespType> = Vec::new();
        arr.push(RespType::BulkString("SET".to_string()));
        arr.push(RespType::BulkString(set_map.key));
        arr.push(RespType::BulkString(set_map.val));
        if set_map.overwrite.is_some() {
            match set_map.overwrite.unwrap() {
                SetOverwriteArgs::NX => arr.push(RespType::BulkString("NX".to_string())),
                SetOverwriteArgs::XX => arr.push(RespType::BulkString("XX".to_string())),
            }
        }
        if set_map.expiry.is_some() {
            match set_map.expiry.unwrap() {
                SetExpiryArgs::EX(val) => {
                    arr.push(RespType::BulkString("EX".to_string()));
                    arr.push(RespType::BulkString(val.to_string()))
                },
                SetExpiryArgs::PX(val) => {
                    arr.push(RespType::BulkString("PX".to_string()));
                    arr.push(RespType::BulkString(val.to_string()))
                },
                SetExpiryArgs::EXAT(val) => {
                    arr.push(RespType::BulkString("EXAT".to_string()));
                    arr.push(RespType::BulkString(val.to_string()))
                },
                SetExpiryArgs::PXAT(val) => {
                    arr.push(RespType::BulkString("PXAT".to_string()));
                    arr.push(RespType::BulkString(val.to_string()))
                },
            }
            if let Some(true) = set_map.keepttl {
                arr.push(RespType::BulkString("KEEPTTL".to_string()));
            }
        }
        Ok(RespType::Array(arr))
    }

    fn _decode_config_param(val: &RespType) -> Result<ConfigParam, RedisError> {
        match val {
            RespType::BulkString(val_str) => match val_str.to_lowercase().as_str() {
                "dir" => Ok(ConfigParam::Dir(None)),
                "dbfilename" => Ok(ConfigParam::DbFileName(None)),
                _ => Ok(ConfigParam::Unknown),
            },
            _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
        }
    }

    fn _decode_keys(args: &[RespType]) -> Result<String, RedisError> {
        if args.len() == 0 || args.len() > 1 {
            return Err(RedisError::MissingArgs);
        }

        match &args[0] {
            RespType::BulkString(val) => Ok(val.to_owned()),
            _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
        }
    }

    fn decode_config(args: &[RespType]) -> Result<ConfigOperation, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        };
        match &args[0] {
            RespType::BulkString(val) => match val.to_lowercase().as_str() {
                "get" => Ok(ConfigOperation::Get(Self::decode_config_get(&args[1..])?)),
                "set" => Ok(ConfigOperation::Set(Self::decode_config_set(&args[1..])?)),
                _ => return Err(RedisError::SyntaxError),
            },
            _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
        }
    }

    fn decode_config_get(args: &[RespType]) -> Result<Vec<ConfigParam>, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        };
        let mut arr: Vec<ConfigParam> = Vec::new();
        for i in 0..args.len() {
            match &args[i] {
                RespType::BulkString(val_str) => match val_str.to_lowercase().as_str() {
                    "dir" => arr.push(ConfigParam::Dir(None)),
                    "dbfilename" => arr.push(ConfigParam::DbFileName(None)),
                    _ => arr.push(ConfigParam::Unknown),
                },
                _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
            }
        }
        Ok(arr)
    }

    fn decode_config_set(args: &[RespType]) -> Result<Vec<ConfigParam>, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        };
        let mut arr: Vec<ConfigParam> = Vec::new();
        for pair in args.windows(2) {
            if pair.len() < 2 {
                return Err(RedisError::SyntaxError);
            }
            match &pair[0] {
                RespType::BulkString(key_str) => match key_str.to_lowercase().as_str() {
                    "dir" => arr.push(ConfigParam::Dir(match &pair[1] {
                        RespType::BulkString(val_str) => {
                            Some(ConfigPair {
                                key: key_str.to_string(),
                                value: val_str.to_string(),
                            })
                        }
                        _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
                    })),
                    "dbfilename" => arr.push(ConfigParam::DbFileName(match &pair[1] {
                        RespType::BulkString(val_str) => Some(ConfigPair {
                            key: key_str.to_string(),
                            value: val_str.to_string(),
                        }),
                        _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
                    })),
                    _ => continue,
                },
                _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
            };
        }
        Ok(arr)
    }

    fn _decode_get(args: &[RespType]) -> Result<String, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        } else if args.len() > 1 {
            return Err(RedisError::SyntaxError);
        }
        match &args[0] {
            RespType::BulkString(val) => Ok((*val).to_string()),
            _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
        }
    }

    fn decode_info(args: &[RespType]) -> Result<Vec<InfoOperation>, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        }
        let mut arr: Vec<InfoOperation> = Vec::new();
        for i in 0..args.len() {
            match &args[i] {
                RespType::BulkString(val_str) => match val_str.to_lowercase().as_str() {
                    "replication" => arr.push(InfoOperation::Replication),
                    "server" => arr.push(InfoOperation::Server),
                    _ => (),
                },
                _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
            }
        }
        Ok(arr)
    }

    fn decode_replconf(args: &[RespType]) -> Result<Vec<ReplicaConfigOperation>, RedisError> {
        let mut replconf: Vec<ReplicaConfigOperation> = Vec::new();
        for chunk in args.chunks(2) {
            if chunk.len() < 2 {
                return Err(RedisError::SyntaxError)
            }
            match (&chunk[0], &chunk[1]) {
                (RespType::BulkString(key), RespType::BulkString(val)) => {
                    if key == "capa" {
                        replconf.push(ReplicaConfigOperation::Capabilities(val.to_owned()));
                    } else if key == "listening-port" {
                        replconf.push(ReplicaConfigOperation::ListeningPort(
                            val.as_str()
                                .parse::<u16>()
                                .map_err(|_| RedisError::ParsingError)?,
                        ));
                    } else if key.to_lowercase().as_str() == "getack" {
                        replconf.push(ReplicaConfigOperation::GetAck(val.to_owned()));
                    }
                }
                _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
            }
        }
        Ok(replconf)
    }

    fn decode_psync(args: &[RespType]) -> Result<Psync, RedisError> {
        match (&args[0], &args[1]) {
            (RespType::BulkString(key), RespType::BulkString(val)) => Ok(Psync::new(
                key.to_string(),
                val.as_str()
                    .parse::<i16>()
                    .map_err(|_| RedisError::ParsingError)?,
            )),
            _ => return Err(RedisError::Operation(OperationError::InvalidRespType)),
        }
    }

    pub(crate) fn decode(word: RespType) -> Result<Self, RedisError> {
        trace!("RESP type to decode: {:?}", word);
        match word {
            RespType::String(res) => match res.to_lowercase().as_str() {
                "ok" => Ok(Self::Ok),
                "pong" => Ok(Self::Ok),
                val if val.starts_with("fullresync") => Ok(Self::Nil),
                _ => Err(RedisError::UnknownCommand),
            },
            RespType::BulkString(res) => Err(RedisError::UnknownCommand),
            RespType::RDB(res) => {
                let mut cursor = Cursor::new(res);
                match RdbParser::decode(&mut cursor) {
                    Ok(parser) => Ok(Self::RdbFile(parser)),
                    Err(err) => {
                        print!("{:?}", err);
                        Err(RedisError::RDB(RDBError::MissingBytes))
                    }
                }
            }
            RespType::Error(err) => Ok(Self::Error(err)),
            RespType::Integer(_) => Err(RedisError::UnknownCommand),
            RespType::Array(res) => match &res[0] {
                RespType::String(val) => todo!(),
                RespType::Error(_) => todo!(),
                RespType::Integer(_) => todo!(),
                RespType::BulkString(val) => match val.to_lowercase().as_str() {
                    "ping" => Ok(Self::Ping),
                    "replconf" => Ok(Self::ReplicaConf(Self::decode_replconf(&res[1..])?)),
                    "psync" => Ok(Self::Psync(Self::decode_psync(&res[1..])?)),
                    "echo" => match &res[1] {
                        RespType::BulkString(val) => Ok(Self::Echo(val.clone())),
                        _ => todo!(),
                    },
                    "set" => Ok(Self::Set(Self::decode_set(&res[1..])?)),
                    "get" => Ok(Self::Get(Self::_decode_get(&res[1..])?)),
                    // "config" => Ok(Self::Config(Self::_decode_config(&res[1..])?)),
                    "config" => Ok(Self::Config(Self::decode_config(&res[1..])?)),
                    "keys" => Ok(Self::Keys(Self::_decode_keys(&res[1..])?)),
                    "info" => Ok(Self::Info(Self::decode_info(&res[1..])?)),
                    _ => Err(RedisError::UnknownCommand),
                },
                _ => Err(RedisError::UnknownCommand),
            },
            RespType::Null => Err(RedisError::UnknownCommand),
        }
    }

    pub(crate) fn encode(word: Self) -> Result<RespType, RedisError> {
        trace!("Operation to encode: {:?}", word);
        match word {
            Operation::Set(val) => Self::encode_set(val),
            Operation::Ping => Ok(RespType::Array(vec![RespType::BulkString(
                "PING".to_string(),
            )])),
            Operation::ReplicaConf(conf) => {
                let mut arr: Vec<RespType> = Vec::new();
                if conf.len() > 0 {
                    arr.push(RespType::BulkString("REPLCONF".to_string()));
                }
                for i in conf {
                    match i {
                        ReplicaConfigOperation::ListeningPort(port) => {
                            arr.push(RespType::BulkString("listening-port".to_string()));
                            arr.push(RespType::BulkString(port.to_string()));
                        }
                        ReplicaConfigOperation::Capabilities(cap) => {
                            arr.push(RespType::BulkString("capa".to_string()));
                            arr.push(RespType::BulkString(cap.to_string()));
                        }
                        ReplicaConfigOperation::GetAck(val) => {
                            arr.push(RespType::BulkString("GETACK".to_string()));
                            arr.push(RespType::BulkString(val.to_string()));
                        },
                        ReplicaConfigOperation::Ack(val) => {
                            arr.push(RespType::BulkString("ACK".to_string()));
                            arr.push(RespType::BulkString(val.to_string()));
                        },
                        _ => return Err(RedisError::UnknownCommand),
                    }
                }
                Ok(RespType::Array(arr))
            }
            Operation::Psync(val) => Ok(RespType::Array(vec![
                RespType::BulkString("PSYNC".to_string()),
                RespType::BulkString(val.replication_id),
                RespType::BulkString(val.offset.to_string()),
            ])),
            Operation::Echo(val) => Ok(RespType::BulkString(val)),
            Operation::EchoString(val) => Ok(RespType::String(val)),
            Operation::EchoBytes(val) => Ok(RespType::RDB(val)),
            Operation::EchoArray(val) => {
                let mut arr: Vec<RespType> = Vec::new();
                for i in 0..val.len() {
                    arr.push(Self::encode(val[i].clone())?);
                }
                Ok(RespType::Array(arr))
            }
            Operation::Ok => Ok(RespType::String("OK".to_string())),
            Operation::Nil => Ok(RespType::Null),
            Operation::Error(val) => Ok(RespType::Error(val)),
            _ => Err(RedisError::UnknownCommand),
        }
    }
}

#[cfg(test)]
mod resp {
    use clap::builder::Str;

    use crate::{error::{OperationError, RedisError}, resp::RespParser, state::{SetExpiryArgs, SetMap, SetOverwriteArgs}};

    use super::{Operation, RespType};

    #[test]
    fn test_decode_set() {
        let set_resp = vec![
            RespType::BulkString(String::from("SET")),
            RespType::BulkString(String::from("foo")),
            RespType::BulkString(String::from("bar")),
            RespType::BulkString(String::from("ex")),
            RespType::BulkString(String::from("15")),
            RespType::BulkString(String::from("nx")),
        ];

        let setmap = Operation::decode_set(&set_resp[1..]);
        assert!(setmap.is_ok());
        assert_eq!(setmap.as_ref().unwrap().expiry, Some(SetExpiryArgs::EX(15)));
        assert_eq!(setmap.as_ref().unwrap().key, String::from("foo"));
        assert_eq!(setmap.as_ref().unwrap().val, String::from("bar"));
        assert_eq!(setmap.as_ref().unwrap().overwrite, Some(SetOverwriteArgs::NX));
        assert!(setmap.as_ref().unwrap().keepttl.is_none());
        assert!(setmap.as_ref().unwrap().expiry_timestamp.is_some());

        let invalid_set_resp = vec![
            RespType::BulkString(String::from("SET")),
            RespType::String(String::from("foo")),
            RespType::BulkString(String::from("bar")),
        ];
        assert!(matches!(Operation::decode_set(&invalid_set_resp[1..]), Err(RedisError::Operation(OperationError::InvalidRespType))));
    }
}
