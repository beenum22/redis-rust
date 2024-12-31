use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{
    config::{ConfigOperation, ConfigPair, ConfigParam},
    error::RedisError, info::InfoOperation, resp::RespType,
    state::{SetExpiryArgs, SetMap, SetOverwriteArgs},
    RedisBuffer
};

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum ReplicaConfigOperation {
    ListeningPort(u16),
    Capabilities(String),
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Operation {
    Ping,
    ReplicaConf(ReplicaConfigOperation),
    Echo(String),
    Set(SetMap),
    Get(String),
    Config(Vec<ConfigOperation>),
    Keys(String),
    Ok,
    Nil,
    EchoArray(Vec<Operation>),
    Info(Vec<InfoOperation>),
    Error(String),
    Unknown,
}

impl Operation {
    fn _decode_set(args: &[RespType]) -> Result<SetMap, RedisError> {
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
            _ => return Err(RedisError::InvalidValueType),
        };
        set_args.val = match &args[1] {
            RespType::BulkString(val) => (*val).to_string(),
            _ => return Err(RedisError::InvalidValueType),
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
                                    _ => return Err(RedisError::InvalidValueType),
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
                                    _ => return Err(RedisError::InvalidValueType),
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
                                    _ => return Err(RedisError::InvalidValueType),
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
                                    _ => return Err(RedisError::InvalidValueType),
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
                _ => return Err(RedisError::InvalidValueType),
            }
            i += 1;
        }
        Ok(set_args)
    }

    fn _decode_config_param(val: &RespType) -> Result<ConfigParam, RedisError> {
        match val {
            RespType::BulkString(val_str) => match val_str.to_lowercase().as_str() {
                "dir" => Ok(ConfigParam::Dir(None)),
                "dbfilename" => Ok(ConfigParam::DbFileName(None)),
                _ => Ok(ConfigParam::Unknown),
            },
            _ => return Err(RedisError::InvalidValueType),
        }
    }

    fn _decode_keys(args: &[RespType]) -> Result<String, RedisError> {
        if args.len() == 0 || args.len() > 1 {
            return Err(RedisError::MissingArgs);
        }

        match &args[0] {
            RespType::BulkString(val) => Ok(val.to_owned()),
            _ => return Err(RedisError::InvalidValueType),
        }
    }

    fn _decode_config_get(args: &[RespType]) -> Result<Vec<ConfigOperation>, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        };
        let mut arr: Vec<ConfigOperation> = Vec::new();
        for i in 0..args.len() {
            match &args[i] {
                RespType::BulkString(val_str) => match val_str.to_lowercase().as_str() {
                    "dir" => arr.push(ConfigOperation::Get(ConfigParam::Dir(None))),
                    "dbfilename" => arr.push(ConfigOperation::Get(ConfigParam::DbFileName(None))),
                    _ => arr.push(ConfigOperation::Get(ConfigParam::Unknown)),
                },
                _ => return Err(RedisError::InvalidValueType),
            }
        }
        Ok(arr)
    }

    fn _decode_config_set(args: &[RespType]) -> Result<Vec<ConfigOperation>, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        };
        let mut arr: Vec<ConfigOperation> = Vec::new();
        for pair in args.windows(2) {
            if pair.len() < 2 {
                return Err(RedisError::SyntaxError);
            }
            arr.push(ConfigOperation::Set(match &pair[0] {
                RespType::BulkString(key_str) => match key_str.to_lowercase().as_str() {
                    "dir" => ConfigParam::Dir(match &pair[1] {
                        RespType::BulkString(val_str) => {
                            // Some((key_str.to_string(), val_str.to_string()))
                            Some(ConfigPair {
                                key: key_str.to_string(),
                                value: val_str.to_string(),
                            })
                        }
                        _ => return Err(RedisError::InvalidValueType),
                    }),
                    "dbfilename" => ConfigParam::DbFileName(match &pair[1] {
                        RespType::BulkString(val_str) => {
                            Some(ConfigPair {
                                key: key_str.to_string(),
                                value: val_str.to_string(),
                            })
                        }
                        _ => return Err(RedisError::InvalidValueType),
                    }),
                    _ => continue,
                },
                _ => return Err(RedisError::InvalidValueType),
            }));
        }
        Ok(arr)
    }

    fn _decode_config(args: &[RespType]) -> Result<Vec<ConfigOperation>, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        };
        match &args[0] {
            RespType::BulkString(val) => match val.to_lowercase().as_str() {
                "get" => Self::_decode_config_get(&args[1..]),
                "set" => Self::_decode_config_set(&args[1..]),
                _ => return Err(RedisError::SyntaxError),
            },
            _ => return Err(RedisError::InvalidValueType),
        }
    }

    fn _decode_get(args: &[RespType]) -> Result<String, RedisError> {
        if args.len() == 0 {
            return Err(RedisError::MissingArgs);
        } else if args.len() > 1 {
            return Err(RedisError::SyntaxError);
        }
        match &args[0] {
            RespType::BulkString(val) => Ok((*val).to_string()),
            _ => return Err(RedisError::InvalidValueType),
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
                _ => return Err(RedisError::InvalidValueType),
            }
        }
        Ok(arr)
    }

    pub(crate) fn decode(raw: &mut RedisBuffer, word: RespType) -> Result<Self, RedisError> {
        match word {
            RespType::String(res) => {
                match res.to_lowercase().as_str() {
                    "ok" => Ok(Self::Ok),
                    "pong" => Ok(Self::Ok),
                    _ => Err(RedisError::UnknownCommand)               }
            },
            RespType::BulkString(res) => Err(RedisError::UnknownCommand),
            RespType::Error(err) => Ok(Self::Error(err)),
            RespType::Integer(_) => Err(RedisError::UnknownCommand),
            RespType::Array(res) => match &res[0] {
                RespType::String(val) => todo!(),
                RespType::Error(_) => todo!(),
                RespType::Integer(_) => todo!(),
                RespType::BulkString(val) => match val.to_lowercase().as_str() {
                    "ping" => Ok(Self::Ping),
                    "echo" => match &res[1] {
                        RespType::BulkString(val) => Ok(Self::Echo(val.clone())),
                        _ => todo!(),
                    },
                    "set" => Ok(Self::Set(Self::_decode_set(&res[1..])?)),
                    "get" => Ok(Self::Get(Self::_decode_get(&res[1..])?)),
                    "config" => Ok(Self::Config(Self::_decode_config(&res[1..])?)),
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
        match word {
            Operation::Ping => Ok(RespType::Array(vec![RespType::BulkString("PING".to_string())])),
            Operation::ReplicaConf(conf) => {
                match conf {
                    ReplicaConfigOperation::ListeningPort(port) => Ok(RespType::Array(vec![
                        RespType::BulkString("REPLCONF".to_string()),
                        RespType::BulkString("listening-port".to_string()),
                        RespType::BulkString(port.to_string()),
                    ])),
                    ReplicaConfigOperation::Capabilities(cap) => Ok(RespType::Array(vec![
                        RespType::BulkString("REPLCONF".to_string()),
                        RespType::BulkString("capa".to_string()),
                        RespType::BulkString(cap),
                    ])),
                }
            },
            Operation::Echo(val) => Ok(RespType::BulkString(val)),
            Operation::EchoArray(val) => {
                let mut arr: Vec<RespType> = Vec::new();
                for i in 0..val.len() {
                    arr.push(Self::encode(val[i].clone())?);
                }
                Ok(RespType::Array(arr))
            }
            Operation::Ok => Ok(RespType::BulkString("OK".to_string())),
            Operation::Nil => Ok(RespType::Null),
            Operation::Error(val) => Ok(RespType::Error(val)),
            _ => Err(RedisError::UnknownCommand),
        }
    }
}

