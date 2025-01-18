use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::trace;
use tokio_util::codec::{Decoder, Encoder};
use core::str;

use crate::error::{RedisError, RespError};
use crate::rdb::RDB_MAGIC;

#[derive(PartialEq, Debug)]
pub(crate) enum RespType {
    String(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<Self>),
    Null,
    RDB(Bytes),
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
    pub(crate) fn raw_len(&self) -> Result<usize, RedisError> {
        match self {
            RespType::String(val) => Ok(1 + val.len() + 2),
            RespType::Error(val) => Ok(5 + val.len() + 2),
            RespType::Integer(val) => Ok(1 + val.to_string().len() + 2),
            RespType::BulkString(val) => Ok(1 + val.len().to_string().len() + 2 + val.len() + 2),
            RespType::Array(vals) => {
                let mut l: usize = 1 + vals.len().to_string().len() + 2;
                for i in vals {
                    l += i.raw_len()?;
                }
                Ok(l)
            },
            RespType::Null => Ok(4),
            RespType::RDB(bytes) => Ok(1 + bytes.len().to_string().len() + 2 + bytes.len()),
        }
    }
}

pub(crate) struct RespParser {
    // raw: &BytesMut,
}

impl RespParser {
    pub(crate) fn new() -> Self {
        Self { }
    }
}

impl RespParser {
    const CRLF: &[u8; 2] = b"\r\n";

    fn find_crlf(raw: &BytesMut) -> Option<usize> {
        let special: &[u8] = &[b'$', b'*'];
        if let Some(pos) = raw.windows(2).position(|w| w == Self::CRLF) {
            if pos > 0 && raw[1..pos].iter().any(|&b| special.contains(&b)) {
                return None
            }
            return Some(pos);
        }
        None
    }

    fn decode_integer(&mut self, raw: &mut BytesMut) -> Result<Option<RespType>, RedisError> {
        match RespParser::find_crlf(raw) {
            Some(index) => {
                let value_str = String::from_utf8(raw[1..index].to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))?;
                let value_int = value_str
                    .parse::<i64>()
                    .map_err(|_| RedisError::RESP(RespError::IntegerParsingFailed))?;
                raw.advance(index + RespParser::CRLF.len());
                Ok(Some(RespType::Integer(value_int)))
            },
            None => Ok(None),
        }
    }

    fn encode_integer(&mut self, val: i64, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!(":{}\r\n", val.to_string()).as_bytes());
        Ok(())
    }

    fn decode_string(&mut self, raw: &mut BytesMut) -> Result<Option<RespType>, RedisError> {
        match RespParser::find_crlf(raw) {
            Some(index) => {
                let value_str = String::from_utf8(raw[1..index].to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))?;
                raw.advance(index + RespParser::CRLF.len());
                Ok(Some(RespType::String(value_str)))
            },
            None => Ok(None),
        }
    }

    fn encode_string(&mut self, val: String, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!("+{}\r\n", val).as_bytes());
        Ok(())
    }

    fn decode_bulk_string(&mut self, raw: &mut BytesMut) -> Result<Option<RespType>, RedisError> {
        let count = match self.decode_integer(raw)? {
            Some(RespType::Integer(val)) => val,
            Some(_) => return Ok(None),
            None => return Ok(None),
        };
        match RespParser::find_crlf(raw) {
            Some(index) => {
                if index != count as usize {
                    return Err(RedisError::RESP(RespError::IncorrectBulkStringSize));
                }
                let value_str = String::from_utf8(raw[..index].to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))?;
                raw.advance(index + RespParser::CRLF.len());
                Ok(Some(RespType::BulkString(value_str)))
            },
            None => {
                if raw.len() > 4 && &raw[..5] == RDB_MAGIC {
                    let bytes = Bytes::copy_from_slice(&raw[..count as usize]);
                    raw.advance(count as usize);
                    return Ok(Some(RespType::RDB(bytes)))
                }
                Ok(None)
            },
        }
    }

    fn encode_bulk_string(&mut self, val: String, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!("${}\r\n{}\r\n", val.len(), val).as_bytes());
        Ok(())
    }

    fn decode_array(&mut self, raw: &mut BytesMut) -> Result<Option<RespType>, RedisError> {
        let count = match self.decode_integer(raw)? {
            Some(RespType::Integer(val)) => val,
            Some(_) => return Ok(None),
            None => return Ok(None),
        };
        let mut array_val: Vec<RespType> = Vec::with_capacity(count as usize);
        for _i in 0..count as usize {
            // let val = Self::decode(raw)?;
            match self.decode(raw)? {
                Some(val) => array_val.push(val),
                None => return Ok(None),
            }
        }
        Ok(Some(RespType::Array(array_val)))
    }

    fn encode_array(&mut self, val: Vec<RespType>, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!("*{}\r\n", val.len()).as_bytes());
        for i in val {
            self.encode(i, raw)?;
        }
        Ok(())
    }

    fn decode_error(&mut self, raw: &mut BytesMut) -> Result<Option<RespType>, RedisError> {
        // TODO: It might have multiple CRLFs. Handle them all. Currently, it only fetches till first occurrence.

        match RespParser::find_crlf(raw) {
            Some(index) => {
                let value_str = String::from_utf8(raw[1..index].to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))?;
                
                raw.advance(index + RespParser::CRLF.len());
                Ok(Some(RespType::Error(value_str)))
            },
            None => Ok(None),
        }
    }

    fn encode_error(&mut self, val: String, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!("-Err {}\r\n", val).as_bytes());
        Ok(())
    }

    fn decode_null(&mut self, raw: &mut BytesMut) -> Result<Option<RespType>, RedisError> {
        match RespParser::find_crlf(raw) {
            Some(index) => {
                if index != 1 {
                    return Ok(None)
                }
                raw.advance(index + RespParser::CRLF.len());
                Ok(Some(RespType::Null))
            },
            None => Ok(None),
        }
    }

    fn encode_null(&mut self, raw: &mut BytesMut) -> Result<(), RedisError> {
        // TODO: Commented out in favor of BulkString Null to support RESP 2 tests. Fix later for RESP 3.
        // Ok(Bytes::from("_\r\n"))
        raw.put("$-1\r\n".as_bytes());
        Ok(())
    }

    // TODO: Refactor and improve parsing here. Too many Vecs in there.
    fn encode_rdb(&mut self, val: Bytes, raw: &mut BytesMut) -> Result<(), RedisError> {
        let mut bytes_len = format!("${}\r\n", val.len()).as_bytes().to_vec();
        bytes_len.extend(val);
        raw.extend_from_slice(&bytes_len);
        Ok(())
    }
}

impl Decoder for RespParser {
    type Item = RespType;
    type Error = RedisError;

    fn decode(&mut self, raw: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if raw.len() == 0 {
            return Ok(None)
        };
        match raw[0] {
            b'+' => self.decode_string(raw),
            b'-' => self.decode_error(raw),
            b'$' => self.decode_bulk_string(raw),
            b':' => self.decode_integer(raw),
            b'*' => self.decode_array(raw),
            b'_' => self.decode_null(raw),
            _ => Err(RedisError::RESP(RespError::UnknownType)),
        }
    }
}

impl Encoder<RespType> for RespParser {
    type Error = RedisError;

    fn encode(&mut self, item: RespType, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespType::String(val) => self.encode_string(val, dst)?,
            RespType::Integer(val) => self.encode_integer(val, dst)?,
            RespType::BulkString(val) => self.encode_bulk_string(val, dst)?,
            // RespTypeV2::Bytes(val) => Self::encode_bulkstring_without_crlf(val),
            RespType::Array(val) => self.encode_array(val, dst)?,
            RespType::Null => self.encode_null(dst)?,
            RespType::Error(val) => self.encode_error(val, dst)?,
            RespType::RDB(val) => self.encode_rdb(val, dst)?,
            _ => return Err(RedisError::RESP(RespError::UnsupportedType)),
        }
        Ok(())
    }
}


#[cfg(test)]
mod resp {
    use bytes::{Buf, Bytes, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    use crate::{error::{RedisError, RespError}, resp::{RespParser, RespType}};

    #[test]
    fn test_find_crlf() {
        let mut buff = BytesMut::from(":2\r\n:-2\r\n:23");
        assert_eq!(RespParser::find_crlf(&mut buff), Some(2));

        let mut buff = BytesMut::from("hello$5\r\nworld\r\n:-2\r\n:23");
        assert_eq!(RespParser::find_crlf(&mut buff), None);
    }

    #[test]
    fn test_decode_integer() {
        let mut buff = BytesMut::from(":2\r\n:-2\r\n:23");
        let mut parser = RespParser {};

        let unsigned = parser.decode_integer(&mut buff);
        assert!(unsigned.is_ok());
        assert!(unsigned.as_ref().unwrap().is_some());
        assert_eq!(unsigned.unwrap().unwrap(), RespType::Integer(2));

        let signed = parser.decode_integer(&mut buff);
        assert!(signed.is_ok());
        assert!(signed.as_ref().unwrap().is_some());
        assert_eq!(signed.unwrap().unwrap(), RespType::Integer(-2));

        let signed = parser.decode_integer(&mut buff);
        assert!(signed.is_ok());
        assert!(signed.as_ref().unwrap().is_none());
    }

    #[test]
    fn test_encode_integer() {
        let mut buff = BytesMut::new();
        let mut parser = RespParser {};

        let val = parser.encode_integer(42, &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from(":42\r\n"));

        let val = parser.encode_integer(-42, &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from(":42\r\n:-42\r\n"));
    }

    #[test]
    fn test_decode_string() {
        let mut buff = BytesMut::from("+hello\r\n+\r\n");
        let mut parser = RespParser {};

        let str_val = parser.decode_string(&mut buff);
        assert!(str_val.is_ok());
        assert!(str_val.as_ref().unwrap().is_some());
        assert_eq!(str_val.unwrap().unwrap(), RespType::String("hello".to_string()));

        let empty_val = parser.decode_string(&mut buff);
        assert!(empty_val.is_ok());
        assert!(empty_val.as_ref().unwrap().is_some());
        assert_eq!(empty_val.unwrap().unwrap(), RespType::String("".to_string()));
    }

    #[test]
    fn test_encode_string() {
        let mut buff = BytesMut::new();
        let mut parser = RespParser {};
        let str_val = parser.encode_string("hello".to_string(), &mut buff);
        assert!(str_val.is_ok());
        assert_eq!(buff, BytesMut::from("+hello\r\n"));
    }

    #[test]
    fn test_decode_bulk_string() {
        let mut buff = BytesMut::from("$5\r\nhello\r\n$0\r\n\r\n$5\r\nworld");
        let mut parser = RespParser {};

        let bulk_str_val = parser.decode_bulk_string(&mut buff);
        assert!(bulk_str_val.is_ok());
        assert!(bulk_str_val.as_ref().unwrap().is_some());
        assert_eq!(bulk_str_val.unwrap().unwrap(), RespType::BulkString("hello".to_string()));

        let empty_val = parser.decode_bulk_string(&mut buff);
        assert!(empty_val.is_ok());
        assert!(empty_val.as_ref().unwrap().is_some());
        assert_eq!(empty_val.unwrap().unwrap(), RespType::BulkString("".to_string()));
    
        let incomplete_val = parser.decode_bulk_string(&mut buff);
        assert!(incomplete_val.is_ok());
        assert!(incomplete_val.as_ref().unwrap().is_none());

        let mut buff = BytesMut::from("$2\r\nfoo\r\n");
        let invalid_len = parser.decode_bulk_string(&mut buff);
        assert!(invalid_len.is_err());
        assert!(matches!(invalid_len, Err(RedisError::RESP(RespError::IncorrectBulkStringSize))));

        let mut rdb_buff = BytesMut::from("$17\r\nREDISfoobarfoobar");
        let rdb = parser.decode_bulk_string(&mut rdb_buff);
        assert!(rdb.is_ok());
        assert!(rdb.as_ref().unwrap().is_some());
        assert_eq!(rdb.unwrap().unwrap(), RespType::RDB(Bytes::from("REDISfoobarfoobar")));
    }

    #[test]
    fn test_encode_bulk_string() {
        let mut buff = BytesMut::new();
        let mut parser = RespParser {};

        let val = parser.encode_bulk_string(
            "hello".to_string(),
            &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("$5\r\nhello\r\n"));

        let val = parser.encode_bulk_string(
            "".to_string(),
            &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("$5\r\nhello\r\n$0\r\n\r\n"));
    }

    #[test]
    fn test_decode_array() {
        let mut buff = BytesMut::from("*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n");
        let mut parser = RespParser {};

        let mut array = Vec::with_capacity(3);
        array.push(RespType::BulkString(String::from("foo")));
        array.push(RespType::BulkString(String::from("bar")));
        array.push(RespType::Integer(42));

        let array_val = parser.decode_array(&mut buff);
        assert!(array_val.is_ok());
        assert!(array_val.as_ref().unwrap().is_some());
        assert_eq!(array_val.unwrap().unwrap(), RespType::Array(array));
    }

    #[test]
    fn test_encode_array() {
        let mut buff = BytesMut::new();
        let mut parser = RespParser {};

        let val = parser.encode_array(vec![
            RespType::BulkString("hello".to_string()),
            RespType::BulkString("world".to_string()),
            RespType::BulkString("foo".to_string()),
        ], &mut buff);
        assert!(val.is_ok());
        assert_eq!(
            buff,
            BytesMut::from("*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nfoo\r\n")
        );

        let val = parser.encode_array(
            vec![],
            &mut buff
        );
        assert!(val.is_ok());
        assert_eq!(
            buff,
            BytesMut::from("*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nfoo\r\n*0\r\n")
        );
    }

    #[test]
    fn test_decode_error() {
        let mut buff = BytesMut::from("-foo\r\n");
        let mut parser = RespParser {};
        let val = parser.decode_error(&mut buff);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Some(RespType::Error("foo".to_string())));
    }

    #[test]
    fn test_encode_error() {
        let mut buff = BytesMut::new();
        let mut parser = RespParser {};

        let val = parser.encode_error("foo".to_string(), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("-Err foo\r\n"));
    }

    #[test]
    fn test_decode_null() {
        let mut buff = BytesMut::from("_\r\n");
        let mut parser = RespParser {};
        let val = parser.decode_null(&mut buff);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Some(RespType::Null));
    }

    #[test]
    fn test_encode_null() {
        let mut buff = BytesMut::new();
        let mut parser = RespParser {};

        let val = parser.encode_null(&mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("$-1\r\n"));
    }

    #[test]
    fn test_decode() {
        let mut buff = BytesMut::from("+hello\r\n$3\r\nfoo\r\n*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n-foo\r\n_\r\n$11\r\nREDISfoobar$3\r\nfoo\r\n");
        let mut parser = RespParser {};

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap().unwrap(), RespType::String("hello".to_string()));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespType::BulkString("foo".to_string())));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespType::Array(
            vec![
                RespType::BulkString(String::from("foo")),
                RespType::BulkString(String::from("bar")),
                RespType::Integer(42),
            ]
        )));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespType::Error("foo".to_string())));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespType::Null));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespType::RDB(Bytes::from("REDISfoobar"))));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespType::BulkString(String::from("foo"))));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), None);
    }

    #[test]
    fn test_encode() {
        let mut buff = BytesMut::new();
        let mut parser = RespParser {};

        let val = parser.encode(RespType::String("foo".to_string()), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n"));

        let val = parser.encode(RespType::BulkString("bar".to_string()), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n$3\r\nbar\r\n"));

        let val = parser.encode(RespType::Null, &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n$3\r\nbar\r\n$-1\r\n"));

        let val = parser.encode(RespType::Error("err".to_string()), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n$3\r\nbar\r\n$-1\r\n-Err err\r\n"));

        let val = parser.encode(RespType::Integer(42), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n$3\r\nbar\r\n$-1\r\n-Err err\r\n:42\r\n"));


        // let mut buff = BytesMut::from("+hello\r\n$3\r\nfoo\r\n*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n-foo\r\n_\r\n");
        // let mut parser = RespParserV2 {};

        // let decode_next = parser.decode(&mut buff);
        // assert!(decode_next.is_ok());
        // assert_eq!(decode_next.unwrap().unwrap(), RespTypeV2::String("hello".to_string()));

        // let decode_next = parser.decode(&mut buff);
        // assert!(decode_next.is_ok());
        // assert_eq!(decode_next.unwrap(), Some(RespTypeV2::BulkString("foo".to_string())));

        // let decode_next = parser.decode(&mut buff);
        // assert!(decode_next.is_ok());
        // assert_eq!(decode_next.unwrap(), Some(RespTypeV2::Array(
        //     vec![
        //         RespTypeV2::BulkString(String::from("foo")),
        //         RespTypeV2::BulkString(String::from("bar")),
        //         RespTypeV2::Integer(42),
        //     ]
        // )));

        // let decode_next = parser.decode(&mut buff);
        // assert!(decode_next.is_ok());
        // assert_eq!(decode_next.unwrap(), Some(RespTypeV2::Error("foo".to_string())));

        // let decode_next = parser.decode(&mut buff);
        // assert!(decode_next.is_ok());
        // assert_eq!(decode_next.unwrap(), Some(RespTypeV2::Null));
    }
}