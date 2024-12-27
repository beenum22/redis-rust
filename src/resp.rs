use bytes::Bytes;
use core::str;
use std::{
    alloc::System,
    collections::HashMap,
    io::Read,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    config::ConfigPair, ConfigOperation, ConfigParam, RedisBuffer, RedisError, SetExpiryArgs, SetMap, SetOverwriteArgs
};

#[derive(Debug)]
struct RespValueIndex {
    start: usize,
    end: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) enum RespType {
    String(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespType>),
    Null,
    //     Boolean(bool),
    //     Double(f32),
    //     BigNumber(i128),
    //     BulkError(String),
    //     VerbatimString(String),
    //     Map(HashMap<String,RedisRespType>),
    //     Attributes(HashMap<String,RedisRespType>),
    //     Set(HashSet<RedisRespType>),
    //     Push(String)
}

impl RespType {
    const CRLF: &[u8; 2] = b"\r\n";

    pub(crate) fn decode(raw: &mut RedisBuffer) -> Result<RespType, RedisError> {
        let type_ch = raw.buffer[raw.index];
        raw.index += 1;
        match type_ch {
            b'+' => Ok(Self::String(Self::decode_string(raw)?)),
            b'-' => Ok(Self::Error(Self::decode_error(raw)?)),
            b'$' => Ok(Self::BulkString(Self::decode_bulk_string(raw)?)),
            b':' => Ok(Self::Integer(Self::decode_integer(raw)?)),
            b'*' => Ok(Self::Array(Self::decode_array(raw)?)),
            b'_' => match Self::decode_null(raw)?.is_none() {
                true => Ok(Self::Null),
                false => Err(RedisError::ParsingError),
            },
            _ => Err(RedisError::ParsingError),
        }
    }

    pub(crate) fn encode(wtype: RespType) -> Result<Bytes, RedisError> {
        match wtype {
            Self::String(val) => Self::encode_string(val),
            Self::Integer(val) => Self::encode_integer(val),
            Self::BulkString(val) => Self::encode_bulk_string(val),
            Self::Array(val) => Self::encode_array(val),
            Self::Null => Self::encode_null(),
            _ => Err(RedisError::ParsingError),
        }
    }

    fn _get_value(raw: &mut RedisBuffer) -> Result<Option<RespValueIndex>, RedisError> {
        let mut val = RespValueIndex {
            start: raw.index,
            end: raw.index,
        };

        match raw.buffer[raw.index..]
            .windows(2)
            .position(|window| window == Self::CRLF)
        {
            Some(index) => {
                if index == 0 {
                    return Ok(None);
                }

                if index + 2 == raw.buffer[raw.index..].len() {
                    // val.end += index + 1;
                    val.end += index - 1;
                    raw.index += index + 1;
                } else {
                    val.end += index - 1;
                    raw.index += index + 2;
                }
                Ok(Some(val))
            }
            None => Err(RedisError::CRLFMissing),
        }
    }

    fn decode_string(raw: &mut RedisBuffer) -> Result<String, RedisError> {
        let mut value_index = Self::_get_value(raw).map_err(|_| RedisError::ParsingError)?;
        match value_index {
            Some(index) => {
                let buff_ref = &raw.buffer[index.start..index.end + 1];
                String::from_utf8(buff_ref.to_vec()).map_err(|_| RedisError::ParsingError)
            }
            None => Err(RedisError::InvalidValue),
        }
    }

    fn encode_string(val: String) -> Result<Bytes, RedisError> {
        Ok(Bytes::from(format!("+{}\r\n", val)))
    }

    fn decode_error(raw: &mut RedisBuffer) -> Result<String, RedisError> {
        Err(RedisError::ParsingError)
    }

    fn decode_integer(raw: &mut RedisBuffer) -> Result<i64, RedisError> {
        let mut value_index = Self::_get_value(raw).map_err(|_| RedisError::ParsingError)?;
        match value_index {
            Some(index) => {
                let value_str = str::from_utf8(&raw.buffer[index.start..index.end + 1])
                    .map_err(|err| RedisError::ParsingError)?;
                value_str
                    .parse::<i64>()
                    .map_err(|_| RedisError::ParsingError)
            }
            None => Err(RedisError::ParsingError),
        }
    }

    fn encode_integer(val: i64) -> Result<Bytes, RedisError> {
        Ok(Bytes::from(format!(":{}\r\n", val.to_string())))
    }

    fn decode_bulk_string(raw: &mut RedisBuffer) -> Result<String, RedisError> {
        // Get Bulk String size
        let count = Self::decode_integer(raw)?;
        let bulk_str_val = Self::_get_value(raw).map_err(|err| RedisError::ParsingError)?;
        match bulk_str_val {
            Some(val) => {
                if val.end - val.start + 1 != count as usize {
                    return Err(RedisError::IncorrectBulkStringSize);
                }
                let buff_ref = &raw.buffer[val.start..val.end + 1];
                String::from_utf8(buff_ref.to_vec()).map_err(|_err| RedisError::ParsingError)
            }
            None => Ok(String::from("")),
        }
    }

    fn encode_bulk_string(val: String) -> Result<Bytes, RedisError> {
        Ok(Bytes::from(format!("${}\r\n{}\r\n", val.len(), val)))
    }

    fn decode_array(raw: &mut RedisBuffer) -> Result<Vec<RespType>, RedisError> {
        let count = Self::decode_integer(raw)?;
        let mut array_val: Vec<RespType> = Vec::with_capacity(count as usize);
        for _i in 0..count as usize {
            let val = Self::decode(raw)?;
            array_val.push(val)
        }
        Ok(array_val)
    }

    fn encode_array(val: Vec<RespType>) -> Result<Bytes, RedisError> {
        let mut enc_val = format!("*{}\r\n", val.len());
        for i in val {
            enc_val.push_str(
                str::from_utf8(&Self::encode(i).unwrap())
                    .map_err(|err| RedisError::ParsingError)?,
            );
        }
        Ok(Bytes::from(enc_val))
    }

    fn decode_null(raw: &mut RedisBuffer) -> Result<Option<String>, RedisError> {
        match Self::_get_value(raw)? {
            Some(_) => Err(RedisError::InvalidValue),
            None => Ok(None),
        }
    }

    fn encode_null() -> Result<Bytes, RedisError> {
        // TODO: Commented out in favor of BulkString Null to support RESP 2 tests. Fix later for RESP 3.
        // Ok(Bytes::from("_\r\n"))
        Ok(Bytes::from("$-1\r\n"))
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Operation {
    Ping,
    Echo(String),
    Set(SetMap),
    Get(String),
    Config(Vec<ConfigOperation>),
    Keys(String),
    Ok,
    Nil,
    EchoArray(Vec<Operation>),
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

    fn decode(raw: &mut RedisBuffer, word: RespType) -> Result<Self, RedisError> {
        match word {
            RespType::String(res) => Err(RedisError::UnknownCommand),
            RespType::BulkString(res) => Err(RedisError::UnknownCommand),
            RespType::Error(_) => Err(RedisError::UnknownCommand),
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
                    _ => Err(RedisError::UnknownCommand),
                },
                _ => Err(RedisError::UnknownCommand),
            },
            RespType::Null => Err(RedisError::UnknownCommand),
        }
    }

    fn encode(word: Self) -> Result<RespType, RedisError> {
        match word {
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

pub(crate) struct RespParser {
    raw: RedisBuffer,
}

impl RespParser {
    pub(crate) fn new(raw_data: RedisBuffer) -> RespParser {
        Self { raw: raw_data }
    }

    pub(crate) fn decode(&mut self) -> Result<Operation, RedisError> {
        let word_type = RespType::decode(&mut self.raw)?;
        Operation::decode(&mut self.raw, word_type)
    }

    pub(crate) fn encode(&mut self, word: Operation) -> Result<Bytes, RedisError> {
        let word_type = Operation::encode(word)?;
        let encoded_bytes = RespType::encode(word_type)?;
        Ok(encoded_bytes)
    }
}

#[cfg(test)]
mod redis_resp_type {
    use bytes::Bytes;

    use crate::resp::{RedisError, RespType};
    use crate::RedisBuffer;

    #[test]
    fn test__get_value() {
        let mut buff = RedisBuffer {
            buffer: Bytes::from("$5\r\nhello\r\n"),
            index: 0,
        };
        buff.index += 1;

        let word_1 = RespType::_get_value(&mut buff);
        assert!(word_1.is_ok());
        assert_eq!(word_1.as_ref().unwrap().as_ref().unwrap().start, 1);
        assert_eq!(word_1.as_ref().unwrap().as_ref().unwrap().end, 1);
        assert_eq!(buff.index, 4);

        let word_2 = RespType::_get_value(&mut buff);
        assert!(word_2.is_ok());
        assert_eq!(word_2.as_ref().unwrap().as_ref().unwrap().start, 4);
        assert_eq!(word_2.as_ref().unwrap().as_ref().unwrap().end, 8);
        assert_eq!(buff.index, 10);

        let mut empty_buff = RedisBuffer {
            buffer: Bytes::from("$0\r\n\r\n"),
            index: 0,
        };
        empty_buff.index += 1;
        let empty_word_1 = RespType::_get_value(&mut empty_buff);
        assert!(empty_word_1.is_ok());
        assert_eq!(empty_word_1.as_ref().unwrap().as_ref().unwrap().start, 1);
        assert_eq!(empty_word_1.as_ref().unwrap().as_ref().unwrap().end, 1);
        assert_eq!(empty_buff.index, 4);

        let empty_word_2 = RespType::_get_value(&mut empty_buff);
        assert!(empty_word_2.is_ok());
        assert!(empty_word_2.as_ref().unwrap().is_none());
        assert_eq!(empty_buff.index, 4);
    }

    #[test]
    fn test_decode_string() {
        let mut buff = RedisBuffer {
            buffer: Bytes::from("+hello\r\n"),
            index: 0,
        };
        buff.index += 1;
        let str_val = RespType::decode_string(&mut buff);
        assert!(str_val.is_ok());
        assert_eq!(str_val.unwrap(), "hello");

        buff.index = 0;

        let mut empty_buff = RedisBuffer {
            buffer: Bytes::from("+\r\n"),
            index: 0,
        };
        empty_buff.index += 1;
        let empty_val = RespType::decode_string(&mut empty_buff);
        assert!(empty_val.is_err());
    }

    #[test]
    fn test_encode_string() {
        let str_val = RespType::encode(RespType::String("hello".to_string()));
        assert!(str_val.is_ok());
        assert_eq!(str_val.unwrap(), Bytes::from("+hello\r\n"));
    }

    #[test]
    fn test_decode_integer() {
        let mut buff = RedisBuffer {
            buffer: Bytes::from(":2\r\n"),
            index: 0,
        };
        buff.index += 1;
        let int_val = RespType::decode_integer(&mut buff);
        assert!(int_val.is_ok());
        assert_eq!(int_val.unwrap(), 2);

        buff.index = 0;

        let mut signed_buff = RedisBuffer {
            buffer: Bytes::from(":-2\r\n"),
            index: 0,
        };
        signed_buff.index += 1;
        let int_val = RespType::decode_integer(&mut signed_buff);
        assert!(int_val.is_ok());
        assert_eq!(int_val.unwrap(), -2);
    }

    #[test]
    fn test_encode_integer() {
        let val = RespType::encode(RespType::Integer(2 as i64));
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Bytes::from(":2\r\n"));

        let signed_val = RespType::encode(RespType::Integer(-2 as i64));
        assert!(signed_val.is_ok());
        assert_eq!(signed_val.unwrap(), Bytes::from(":-2\r\n"));
    }

    #[test]
    fn test_decode_bulk_string() {
        let mut buff = RedisBuffer {
            buffer: Bytes::from("$5\r\nhello\r\n"),
            index: 0,
        };
        buff.index += 1;
        let bulk_str_val = RespType::decode_bulk_string(&mut buff).unwrap();
        assert_eq!(bulk_str_val, "hello");

        buff.index = 0;

        let mut empty_buff = RedisBuffer {
            buffer: Bytes::from("$0\r\n\r\n"),
            index: 0,
        };
        empty_buff.index += 1;
        let empty_val = RespType::decode_bulk_string(&mut empty_buff);
        assert!(empty_val.is_ok());
        assert_eq!(empty_val.unwrap(), "");
    }

    #[test]
    fn test_encode_bulk_string() {
        let val = RespType::encode(RespType::BulkString("hello".to_string()));
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Bytes::from("$5\r\nhello\r\n"));

        let empty_val = RespType::encode(RespType::BulkString("".to_string()));
        assert!(empty_val.is_ok());
        assert_eq!(empty_val.unwrap(), Bytes::from("$0\r\n\r\n"));
    }

    #[test]
    fn test_decode_array() {
        let mut buff = RedisBuffer {
            buffer: Bytes::from("*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n"),
            index: 0,
        };
        let mut array = Vec::with_capacity(3);
        array.push(RespType::BulkString(String::from("foo")));
        array.push(RespType::BulkString(String::from("bar")));
        array.push(RespType::Integer(42));
        buff.index += 1;
        let array_val = RespType::decode_array(&mut buff);
        assert!(array_val.is_ok(),);
        assert_eq!(array_val.unwrap(), array);
    }

    #[test]
    fn test_encode_array() {
        let val = RespType::encode(RespType::Array(vec![
            RespType::BulkString("hello".to_string()),
            RespType::BulkString("world".to_string()),
            RespType::BulkString("foo".to_string()),
        ]));
        assert!(val.is_ok());
        assert_eq!(
            val.unwrap(),
            Bytes::from("*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nfoo\r\n")
        );
    }

    #[test]
    fn test_decode_null() {
        let mut buff = RedisBuffer {
            buffer: Bytes::from("_\r\n"),
            index: 0,
        };
        buff.index += 1;
        let null_val = RespType::decode_null(&mut buff);
        assert!(null_val.is_ok(),);
        assert_eq!(null_val.unwrap(), None);
    }

    #[test]
    fn test_encode_null() {
        let val = RespType::encode(RespType::Null);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Bytes::from("_\r\n"));
    }
}
