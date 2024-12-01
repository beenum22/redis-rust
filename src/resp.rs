use bytes::Bytes;
use core::str;
use std::{
    alloc::System,
    collections::HashMap,
    io::Read,
    str::FromStr,
    time::{Duration, SystemTime},
};

use crate::RedisBuffer;

#[derive(Debug)]
struct RedisRespValue {
    start: usize,
    end: usize,
}

#[derive(Debug)]
pub(crate) enum RedisRespError {
    ParsingError,
    IncorrectBulkStringSize,
    UnknownCommand,
    CRLFMissing,
    InvalidValue,
    InvalidValueType,
    InvalidArraySize,
    InvalidArgValue(String),
    MissingArgs,
    SystemError(String),
    SyntaxError,
    IOError(std::io::Error),
}

#[derive(Debug, PartialEq)]
pub(crate) enum RedisRespType {
    String(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RedisRespType>),
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

impl RedisRespType {
    const CRLF: &[u8; 2] = b"\r\n";

    pub(crate) fn decode(raw: &mut RedisBuffer) -> Result<RedisRespType, RedisRespError> {
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
                false => Err(RedisRespError::ParsingError),
            },
            _ => Err(RedisRespError::ParsingError),
        }
    }

    pub(crate) fn encode(wtype: RedisRespType) -> Result<Bytes, RedisRespError> {
        match wtype {
            Self::String(val) => Self::encode_string(val),
            Self::Integer(val) => Self::encode_integer(val),
            Self::BulkString(val) => Self::encode_bulk_string(val),
            Self::Array(val) => Self::encode_array(val),
            Self::Null => Self::encode_null(),
            _ => Err(RedisRespError::ParsingError),
        }
    }

    fn _get_value(raw: &mut RedisBuffer) -> Result<Option<RedisRespValue>, RedisRespError> {
        let mut val = RedisRespValue {
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
            None => Err(RedisRespError::CRLFMissing),
        }
    }

    fn decode_string(raw: &mut RedisBuffer) -> Result<String, RedisRespError> {
        let mut value_index = Self::_get_value(raw).map_err(|_| RedisRespError::ParsingError)?;
        match value_index {
            Some(index) => {
                let buff_ref = &raw.buffer[index.start..index.end + 1];
                String::from_utf8(buff_ref.to_vec()).map_err(|_| RedisRespError::ParsingError)
            }
            None => Err(RedisRespError::InvalidValue),
        }
    }

    fn encode_string(val: String) -> Result<Bytes, RedisRespError> {
        Ok(Bytes::from(format!("+{}\r\n", val)))
    }

    fn decode_error(raw: &mut RedisBuffer) -> Result<String, RedisRespError> {
        Err(RedisRespError::ParsingError)
    }

    fn decode_integer(raw: &mut RedisBuffer) -> Result<i64, RedisRespError> {
        let mut value_index = Self::_get_value(raw).map_err(|_| RedisRespError::ParsingError)?;
        match value_index {
            Some(index) => {
                let value_str = str::from_utf8(&raw.buffer[index.start..index.end + 1])
                    .map_err(|err| RedisRespError::ParsingError)?;
                value_str
                    .parse::<i64>()
                    .map_err(|_| RedisRespError::ParsingError)
            }
            None => Err(RedisRespError::ParsingError),
        }
    }

    fn encode_integer(val: i64) -> Result<Bytes, RedisRespError> {
        Ok(Bytes::from(format!(":{}\r\n", val.to_string())))
    }

    fn decode_bulk_string(raw: &mut RedisBuffer) -> Result<String, RedisRespError> {
        // Get Bulk String size
        let count = Self::decode_integer(raw)?;
        let bulk_str_val = Self::_get_value(raw).map_err(|err| RedisRespError::ParsingError)?;
        match bulk_str_val {
            Some(val) => {
                if val.end - val.start + 1 != count as usize {
                    return Err(RedisRespError::IncorrectBulkStringSize);
                }
                let buff_ref = &raw.buffer[val.start..val.end + 1];
                String::from_utf8(buff_ref.to_vec()).map_err(|_err| RedisRespError::ParsingError)
            }
            None => Ok(String::from("")),
        }
    }

    fn encode_bulk_string(val: String) -> Result<Bytes, RedisRespError> {
        Ok(Bytes::from(format!("${}\r\n{}\r\n", val.len(), val)))
    }

    fn decode_array(raw: &mut RedisBuffer) -> Result<Vec<RedisRespType>, RedisRespError> {
        let count = Self::decode_integer(raw)?;
        let mut array_val: Vec<RedisRespType> = Vec::with_capacity(count as usize);
        for _i in 0..count as usize {
            let val = Self::decode(raw)?;
            array_val.push(val)
        }
        Ok(array_val)
    }

    fn encode_array(val: Vec<RedisRespType>) -> Result<Bytes, RedisRespError> {
        let mut enc_val = format!("*{}\r\n", val.len());
        for i in val {
            enc_val.push_str(
                str::from_utf8(&Self::encode(i).unwrap())
                    .map_err(|err| RedisRespError::ParsingError)?,
            );
        }
        Ok(Bytes::from(enc_val))
    }

    fn decode_null(raw: &mut RedisBuffer) -> Result<Option<String>, RedisRespError> {
        match Self::_get_value(raw)? {
            Some(_) => Err(RedisRespError::InvalidValue),
            None => Ok(None),
        }
    }

    fn encode_null() -> Result<Bytes, RedisRespError> {
        Ok(Bytes::from("_\r\n"))
    }
}

pub(crate) enum SetOverwriteArgs {
    NX,
    XX,
}

enum SetExpiryArgs {
    EX(u64),
    PX(u128),
    EXAT(u64),
    PXAT(u128),
}

pub(crate) struct SetMap {
    expiry: Option<SetExpiryArgs>,
    pub(crate) key: String,
    pub(crate) val: String,
    pub(crate) overwrite: Option<SetOverwriteArgs>,
    keepttl: Option<bool>,
    pub(crate) expiry_timestamp: Option<SystemTime>,
}

pub(crate) enum RedisRespWord {
    Ping,
    Echo(String),
    Set(SetMap),
    Get(String),
    // SuccessResponse(String),
    // ErrorResponse(String),
    Ok,
    Nil,
    Error(String),
    // CustomString(String),
    // CustomBulkString(String),
    Unknown,
}

impl RedisRespWord {
    fn _decode_set(args: &[RedisRespType]) -> Result<SetMap, RedisRespError> {
        let mut set_args = SetMap {
            expiry: None,
            key: String::new(),
            val: String::new(),
            overwrite: None,
            keepttl: None,
            expiry_timestamp: None,
        };

        if args.len() == 0 {
            return Err(RedisRespError::MissingArgs);
        }
        set_args.key = match &args[0] {
            RedisRespType::BulkString(val) => (*val).to_string(),
            _ => return Err(RedisRespError::InvalidValueType),
        };
        set_args.val = match &args[1] {
            RedisRespType::BulkString(val) => (*val).to_string(),
            _ => return Err(RedisRespError::InvalidValueType),
        };
        let mut i: usize = 2;
        while i < args.len() {
            // for i in 2..args.len() {
            match &args[i] {
                RedisRespType::BulkString(val) => {
                    match val.as_str() {
                        "EX" => {
                            if set_args.expiry.is_some() {
                                return Err(RedisRespError::SyntaxError);
                            }
                            i += 1; // Move to next arg value
                            let expiry = args.get(i);
                            match expiry {
                                Some(val) => match val {
                                    RedisRespType::BulkString(arg_val) => {
                                        let expiry_secs: u64 = arg_val.parse().map_err(|err| {
                                            RedisRespError::InvalidArgValue(
                                                "value is not an integer or out of range"
                                                    .to_string(),
                                            )
                                        })?;
                                        set_args.expiry_timestamp = Some(
                                            SystemTime::now() + Duration::from_secs(expiry_secs),
                                        );
                                        set_args.expiry = Some(SetExpiryArgs::EX(expiry_secs))
                                    }
                                    _ => return Err(RedisRespError::InvalidValueType),
                                },
                                None => return Err(RedisRespError::SyntaxError),
                            };
                        }
                        "PX" => {
                            if set_args.expiry.is_some() {
                                return Err(RedisRespError::SyntaxError);
                            }
                            i += 1; // Move to next arg value
                            let expiry = args.get(i);
                            match expiry {
                                Some(val) => match val {
                                    RedisRespType::BulkString(arg_val) => {
                                        let expiry: u128 = arg_val.parse().map_err(|err| {
                                            RedisRespError::InvalidArgValue(
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
                                    _ => return Err(RedisRespError::InvalidValueType),
                                },
                                None => return Err(RedisRespError::SyntaxError),
                            };
                        }
                        "EXAT" => {
                            if set_args.expiry.is_some() {
                                return Err(RedisRespError::SyntaxError);
                            }
                            i += 1; // Move to next arg value
                            let expiry = args.get(i);
                            match expiry {
                                Some(val) => match val {
                                    RedisRespType::BulkString(arg_val) => {
                                        let expiry: u64 = arg_val.parse().map_err(|err| {
                                            RedisRespError::InvalidArgValue(
                                                "value is not an integer or out of range"
                                                    .to_string(),
                                            )
                                        })?;
                                        let now = SystemTime::now();
                                        set_args.expiry_timestamp =
                                            Some(SystemTime::now() + Duration::from_secs(expiry));
                                        set_args.expiry = Some(SetExpiryArgs::EXAT(expiry))
                                    }
                                    _ => return Err(RedisRespError::InvalidValueType),
                                },
                                None => return Err(RedisRespError::SyntaxError),
                            };
                        }
                        "PXAT" => {
                            if set_args.expiry.is_some() {
                                return Err(RedisRespError::SyntaxError);
                            }
                            i += 1; // Move to next arg value
                            let expiry = args.get(i);
                            match expiry {
                                Some(val) => match val {
                                    RedisRespType::BulkString(arg_val) => {
                                        let expiry: u128 = arg_val.parse().map_err(|err| {
                                            RedisRespError::InvalidArgValue(
                                                "value is not an integer or out of range"
                                                    .to_string(),
                                            )
                                        })?;
                                        set_args.expiry_timestamp = Some(
                                            SystemTime::now()
                                                + Duration::from_millis(expiry as u64),
                                        );
                                        set_args.expiry = Some(SetExpiryArgs::PXAT(expiry))
                                    }
                                    _ => return Err(RedisRespError::InvalidValueType),
                                },
                                None => return Err(RedisRespError::SyntaxError),
                            };
                        }
                        "NX" => set_args.overwrite = Some(SetOverwriteArgs::NX),
                        "XX" => set_args.overwrite = Some(SetOverwriteArgs::XX),
                        "KEEPTTL" => set_args.keepttl = Some(true),
                        _ => return Err(RedisRespError::SyntaxError),
                    }
                }
                _ => return Err(RedisRespError::InvalidValueType),
            }
            i += 1;
        }
        Ok(set_args)
    }

    fn _decode_get(args: &[RedisRespType]) -> Result<String, RedisRespError> {
        if args.len() == 0 {
            return Err(RedisRespError::MissingArgs);
        } else if args.len() > 1 {
            return Err(RedisRespError::SyntaxError);
        }
        match &args[0] {
            RedisRespType::BulkString(val) => Ok((*val).to_string()),
            _ => return Err(RedisRespError::InvalidValueType),
        }
    }

    fn decode(raw: &mut RedisBuffer, word: RedisRespType) -> Result<Self, RedisRespError> {
        match word {
            RedisRespType::String(res) => Err(RedisRespError::UnknownCommand),
            RedisRespType::BulkString(res) => Err(RedisRespError::UnknownCommand),
            RedisRespType::Error(_) => Err(RedisRespError::UnknownCommand),
            RedisRespType::Integer(_) => Err(RedisRespError::UnknownCommand),
            RedisRespType::Array(res) => match &res[0] {
                RedisRespType::String(val) => todo!(),
                RedisRespType::Error(_) => todo!(),
                RedisRespType::Integer(_) => todo!(),
                RedisRespType::BulkString(val) => match val.to_lowercase().as_str() {
                    "ping" => Ok(Self::Ping),
                    "echo" => match &res[1] {
                        RedisRespType::BulkString(val) => Ok(Self::Echo(val.clone())),
                        _ => todo!(),
                    },
                    "set" => Ok(Self::Set(Self::_decode_set(&res[1..])?)),
                    "get" => Ok(Self::Get(Self::_decode_get(&res[1..])?)),
                    _ => Err(RedisRespError::UnknownCommand),
                },
                _ => Err(RedisRespError::UnknownCommand),
            },
            RedisRespType::Null => Err(RedisRespError::UnknownCommand),
        }
    }

    fn encode(word: Self) -> Result<RedisRespType, RedisRespError> {
        match word {
            RedisRespWord::Echo(val) => Ok(RedisRespType::BulkString(val)),
            RedisRespWord::Ok => Ok(RedisRespType::BulkString("OK".to_string())),
            RedisRespWord::Nil => Ok(RedisRespType::Null),
            RedisRespWord::Error(val) => Ok(RedisRespType::Error(val)),
            _ => Err(RedisRespError::UnknownCommand),
        }
    }
}

pub(crate) struct RedisResp {
    raw: RedisBuffer,
}

impl RedisResp {
    pub(crate) fn new(raw_data: RedisBuffer) -> RedisResp {
        Self { raw: raw_data }
    }

    pub(crate) fn decode(&mut self) -> Result<RedisRespWord, RedisRespError> {
        let word_type = RedisRespType::decode(&mut self.raw)?;
        RedisRespWord::decode(&mut self.raw, word_type)
    }

    pub(crate) fn action(&self, word: RedisRespWord) -> Result<RedisRespWord, RedisRespError> {
        match word {
            RedisRespWord::Ping => Ok(RedisRespWord::Echo("PONG".to_string())),
            RedisRespWord::Echo(_) => Ok(word),
            RedisRespWord::Set(set_args) => Ok(RedisRespWord::Ok),
            RedisRespWord::Unknown => todo!(),
            _ => todo!(),
        }
    }

    pub(crate) fn encode(&mut self, word: RedisRespWord) -> Result<Bytes, RedisRespError> {
        let word_type = RedisRespWord::encode(word)?;
        let encoded_bytes = RedisRespType::encode(word_type)?;
        Ok(encoded_bytes)
    }
}

#[cfg(test)]
mod redis_resp_type {
    use bytes::Bytes;

    use crate::resp::{RedisRespError, RedisRespType};
    use crate::RedisBuffer;

    #[test]
    fn test__get_value() {
        let mut buff = RedisBuffer {
            buffer: Bytes::from("$5\r\nhello\r\n"),
            index: 0,
        };
        buff.index += 1;

        let word_1 = RedisRespType::_get_value(&mut buff);
        assert!(word_1.is_ok());
        assert_eq!(word_1.as_ref().unwrap().as_ref().unwrap().start, 1);
        assert_eq!(word_1.as_ref().unwrap().as_ref().unwrap().end, 1);
        assert_eq!(buff.index, 4);

        let word_2 = RedisRespType::_get_value(&mut buff);
        assert!(word_2.is_ok());
        assert_eq!(word_2.as_ref().unwrap().as_ref().unwrap().start, 4);
        assert_eq!(word_2.as_ref().unwrap().as_ref().unwrap().end, 8);
        assert_eq!(buff.index, 10);

        let mut empty_buff = RedisBuffer {
            buffer: Bytes::from("$0\r\n\r\n"),
            index: 0,
        };
        empty_buff.index += 1;
        let empty_word_1 = RedisRespType::_get_value(&mut empty_buff);
        assert!(empty_word_1.is_ok());
        assert_eq!(empty_word_1.as_ref().unwrap().as_ref().unwrap().start, 1);
        assert_eq!(empty_word_1.as_ref().unwrap().as_ref().unwrap().end, 1);
        assert_eq!(empty_buff.index, 4);

        let empty_word_2 = RedisRespType::_get_value(&mut empty_buff);
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
        let str_val = RedisRespType::decode_string(&mut buff);
        assert!(str_val.is_ok());
        assert_eq!(str_val.unwrap(), "hello");

        buff.index = 0;

        let mut empty_buff = RedisBuffer {
            buffer: Bytes::from("+\r\n"),
            index: 0,
        };
        empty_buff.index += 1;
        let empty_val = RedisRespType::decode_string(&mut empty_buff);
        assert!(empty_val.is_err());
    }

    #[test]
    fn test_encode_string() {
        let str_val = RedisRespType::encode(RedisRespType::String("hello".to_string()));
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
        let int_val = RedisRespType::decode_integer(&mut buff);
        assert!(int_val.is_ok());
        assert_eq!(int_val.unwrap(), 2);

        buff.index = 0;

        let mut signed_buff = RedisBuffer {
            buffer: Bytes::from(":-2\r\n"),
            index: 0,
        };
        signed_buff.index += 1;
        let int_val = RedisRespType::decode_integer(&mut signed_buff);
        assert!(int_val.is_ok());
        assert_eq!(int_val.unwrap(), -2);
    }

    #[test]
    fn test_encode_integer() {
        let val = RedisRespType::encode(RedisRespType::Integer(2 as i64));
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Bytes::from(":2\r\n"));

        let signed_val = RedisRespType::encode(RedisRespType::Integer(-2 as i64));
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
        let bulk_str_val = RedisRespType::decode_bulk_string(&mut buff).unwrap();
        assert_eq!(bulk_str_val, "hello");

        buff.index = 0;

        let mut empty_buff = RedisBuffer {
            buffer: Bytes::from("$0\r\n\r\n"),
            index: 0,
        };
        empty_buff.index += 1;
        let empty_val = RedisRespType::decode_bulk_string(&mut empty_buff);
        assert!(empty_val.is_ok());
        assert_eq!(empty_val.unwrap(), "");
    }

    #[test]
    fn test_encode_bulk_string() {
        let val = RedisRespType::encode(RedisRespType::BulkString("hello".to_string()));
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Bytes::from("$5\r\nhello\r\n"));

        let empty_val = RedisRespType::encode(RedisRespType::BulkString("".to_string()));
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
        array.push(RedisRespType::BulkString(String::from("foo")));
        array.push(RedisRespType::BulkString(String::from("bar")));
        array.push(RedisRespType::Integer(42));
        buff.index += 1;
        let array_val = RedisRespType::decode_array(&mut buff);
        assert!(array_val.is_ok(),);
        assert_eq!(array_val.unwrap(), array);
    }

    #[test]
    fn test_encode_array() {
        let val = RedisRespType::encode(RedisRespType::Array(vec![
            RedisRespType::BulkString("hello".to_string()),
            RedisRespType::BulkString("world".to_string()),
            RedisRespType::BulkString("foo".to_string()),
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
        let null_val = RedisRespType::decode_null(&mut buff);
        assert!(null_val.is_ok(),);
        assert_eq!(null_val.unwrap(), None);
    }

    #[test]
    fn test_encode_null() {
        let val = RedisRespType::encode(RedisRespType::Null);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Bytes::from("_\r\n"));
    }
}
