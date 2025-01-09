use bytes::Bytes;
use core::str;
use log::trace;
use std::{
    alloc::System,
    collections::HashMap,
    io::Read,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    config::ConfigPair, error::RespError, info::InfoOperation, rdb::RdbParser, ConfigParam, Operation, RedisBuffer, RedisError, SetExpiryArgs, SetMap, SetOverwriteArgs
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
    Bytes(Bytes),
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
        trace!("RESP Bytes to decode: {:?}", raw.buffer);
        raw.index += 1;
        match type_ch {
            b'+' => Ok(Self::String(Self::decode_string(raw)?)),
            b'-' => Ok(Self::Error(Self::decode_error(raw)?)),
            b'$' => Self::decode_bulk_string(raw),
            b':' => Ok(Self::Integer(Self::decode_integer(raw)?)),
            b'*' => Ok(Self::Array(Self::decode_array(raw)?)),
            b'_' => match Self::decode_null(raw)?.is_none() {
                true => Ok(Self::Null),
                false => Err(RedisError::RESP(RespError::InvalidValue)),
            },
            _ => Err(RedisError::RESP(RespError::UnknownType)),
        }
    }

    pub(crate) fn encode(wtype: RespType) -> Result<Bytes, RedisError> {
        trace!("RESP Type to encode: {:?}", wtype);
        match wtype {
            Self::String(val) => Self::encode_string(val),
            Self::Integer(val) => Self::encode_integer(val),
            Self::BulkString(val) => Self::encode_bulk_string(val),
            Self::Bytes(val) => Self::encode_bulkstring_without_crlf(val),
            Self::Array(val) => Self::encode_array(val),
            Self::Null => Self::encode_null(),
            Self::Error(val) => Self::encode_error(val),
            _ => Err(RedisError::RESP(RespError::UnsupportedType)),
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
            None => Err(RedisError::RESP(RespError::CRLFMissing)),
        }
    }

    // TODO: Refactor and improve parsing here. Too many Vecs in there.
    fn encode_bulkstring_without_crlf(val: Bytes) -> Result<Bytes, RedisError> {
        let mut bytes_len = format!("${}\r\n", val.len()).as_bytes().to_vec();
        bytes_len.extend(val);
        Ok(Bytes::from(bytes_len))
    }

    fn decode_string(raw: &mut RedisBuffer) -> Result<String, RedisError> {
        let value_index = Self::_get_value(raw)?;
        match value_index {
            Some(index) => {
                let buff_ref = &raw.buffer[index.start..index.end + 1];
                String::from_utf8(buff_ref.to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))
            }
            None => Err(RedisError::RESP(RespError::InvalidValue)),
        }
    }

    fn encode_string(val: String) -> Result<Bytes, RedisError> {
        Ok(Bytes::from(format!("+{}\r\n", val)))
    }

    fn decode_error(raw: &mut RedisBuffer) -> Result<String, RedisError> {
        // TODO: It might have multiple CRLFs. Handle them all. Currently, it only fetches till first occurrence.
        match Self::_get_value(raw)? {
            Some(index) => {
                let buff_ref = &raw.buffer[index.start..index.end + 1];
                String::from_utf8(buff_ref.to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))
            }
            None => Err(RedisError::RESP(RespError::InvalidValue)),
        }
    }

    fn encode_error(val: String) -> Result<Bytes, RedisError> {
        Ok(Bytes::from(format!("-Err {}\r\n", val)))
    }

    fn decode_integer(raw: &mut RedisBuffer) -> Result<i64, RedisError> {
        let mut value_index = Self::_get_value(raw)?;
        match value_index {
            Some(index) => {
                let value_str = str::from_utf8(&raw.buffer[index.start..index.end + 1])
                    .map_err(|err| RedisError::RESP(RespError::UTFDecodingFailed))?;
                value_str
                    .parse::<i64>()
                    .map_err(|_| RedisError::RESP(RespError::IntegerParsingFailed))
            }
            None => Err(RedisError::RESP(RespError::InvalidValue)),
        }
    }

    fn encode_integer(val: i64) -> Result<Bytes, RedisError> {
        Ok(Bytes::from(format!(":{}\r\n", val.to_string())))
    }

    fn decode_bulk_string(raw: &mut RedisBuffer) -> Result<Self, RedisError> {
        // Get Bulk String size
        let count = Self::decode_integer(raw)?;
        match Self::_get_value(raw) {
            Ok(Some(val)) => {
                if val.end - val.start + 1 != count as usize {
                    return Err(RedisError::RESP(RespError::IncorrectBulkStringSize));
                }
                let buff_ref = &raw.buffer[val.start..val.end + 1];
                Ok(Self::BulkString(
                    String::from_utf8(buff_ref.to_vec())
                        .map_err(|_err| RedisError::RESP(RespError::UTFDecodingFailed))?,
                ))
            }
            Ok(None) => Ok(Self::BulkString("".to_string())),
            Err(RedisError::RESP(RespError::CRLFMissing)) => {
                if raw.buffer[raw.index..].len() != count as usize {
                    return Err(RedisError::RESP(RespError::IncorrectBulkStringSize));
                }
                Ok(Self::Bytes(Bytes::from(raw.buffer[raw.index..].to_vec())))
            }
            Err(_) => Err(RedisError::ParsingError),
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
                    .map_err(|err| RedisError::RESP(RespError::UTFDecodingFailed))?,
            );
        }
        Ok(Bytes::from(enc_val))
    }

    fn decode_null(raw: &mut RedisBuffer) -> Result<Option<String>, RedisError> {
        match Self::_get_value(raw)? {
            Some(_) => Err(RedisError::RESP(RespError::InvalidValue)),
            None => Ok(None),
        }
    }

    fn encode_null() -> Result<Bytes, RedisError> {
        // TODO: Commented out in favor of BulkString Null to support RESP 2 tests. Fix later for RESP 3.
        // Ok(Bytes::from("_\r\n"))
        Ok(Bytes::from("$-1\r\n"))
    }
}

pub(crate) struct RespParser {
    raw: RedisBuffer,
}

impl RespParser {
    pub(crate) fn new(raw_data: RedisBuffer) -> RespParser {
        Self { raw: raw_data }
    }

    pub(crate) fn decode(raw: &mut RedisBuffer) -> Result<Operation, RedisError> {
        let word_type = RespType::decode(raw)?;
        Operation::decode(raw, word_type)
    }

    pub(crate) fn encode(word: Operation) -> Result<Bytes, RedisError> {
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
        assert_eq!(bulk_str_val, RespType::BulkString("hello".to_string()));

        buff.index = 0;

        let mut empty_buff = RedisBuffer {
            buffer: Bytes::from("$0\r\n\r\n"),
            index: 0,
        };
        empty_buff.index += 1;
        let empty_val = RespType::decode_bulk_string(&mut empty_buff);
        assert!(empty_val.is_ok());
        assert_eq!(empty_val.unwrap(), RespType::BulkString("".to_string()));
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
