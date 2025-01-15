use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::trace;
use tokio_util::codec::{Decoder, Encoder};
use core::str;

use crate::error::{RedisError, RespError};
use crate::rdb::RDB_MAGIC;

#[derive(PartialEq, Debug)]
pub(crate) enum RespTypeV2 {
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

pub(crate) struct RespParserV2 {
    // raw: &BytesMut,
}

impl RespParserV2 {
    pub(crate) fn new() -> Self {
        Self { }
    }
}

impl RespParserV2 {
    const CRLF: &[u8; 2] = b"\r\n";

    fn find_crlf(raw: &BytesMut) -> Option<usize> {
        let special: &[u8] = &[b'$'];
        if let Some(pos) = raw.windows(2).position(|w| w == Self::CRLF) {
            if pos > 0 && raw[1..pos].iter().any(|&b| special.contains(&b)) {
                return None
            }
            return Some(pos);
        }
        None
    }

    fn decode_integer(&mut self, raw: &mut BytesMut) -> Result<Option<RespTypeV2>, RedisError> {
        match RespParserV2::find_crlf(raw) {
            Some(index) => {
                let value_str = String::from_utf8(raw[1..index].to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))?;
                let value_int = value_str
                    .parse::<i64>()
                    .map_err(|_| RedisError::RESP(RespError::IntegerParsingFailed))?;
                raw.advance(index + RespParserV2::CRLF.len());
                Ok(Some(RespTypeV2::Integer(value_int)))
            },
            None => Ok(None),
        }
    }

    fn encode_integer(&mut self, val: i64, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!(":{}\r\n", val.to_string()).as_bytes());
        Ok(())
    }

    fn decode_string(&mut self, raw: &mut BytesMut) -> Result<Option<RespTypeV2>, RedisError> {
        match RespParserV2::find_crlf(raw) {
            Some(index) => {
                let value_str = String::from_utf8(raw[1..index].to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))?;
                raw.advance(index + RespParserV2::CRLF.len());
                Ok(Some(RespTypeV2::String(value_str)))
            },
            None => Ok(None),
        }
    }

    fn encode_string(&mut self, val: String, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!("+{}\r\n", val).as_bytes());
        Ok(())
    }

    fn decode_bulk_string(&mut self, raw: &mut BytesMut) -> Result<Option<RespTypeV2>, RedisError> {
        let count = match self.decode_integer(raw)? {
            Some(RespTypeV2::Integer(val)) => val,
            Some(_) => return Ok(None),
            None => return Ok(None),
        };

        match RespParserV2::find_crlf(raw) {
            Some(index) => {
                if index != count as usize {
                    return Err(RedisError::RESP(RespError::IncorrectBulkStringSize));
                }
                let value_str = String::from_utf8(raw[..index].to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))?;
                raw.advance(index + RespParserV2::CRLF.len());
                Ok(Some(RespTypeV2::BulkString(value_str)))
            },
            None => {
                if raw.len() > 4 && &raw[..5] == RDB_MAGIC {
                    let bytes = Bytes::copy_from_slice(&raw[..count as usize]);
                    raw.advance(count as usize);
                    return Ok(Some(RespTypeV2::RDB(bytes)))
                }
                Ok(None)
            },
        }
    }

    fn encode_bulk_string(&mut self, val: String, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!("${}\r\n{}\r\n", val.len(), val).as_bytes());
        Ok(())
    }

    fn decode_array(&mut self, raw: &mut BytesMut) -> Result<Option<RespTypeV2>, RedisError> {
        let count = match self.decode_integer(raw)? {
            Some(RespTypeV2::Integer(val)) => val,
            Some(_) => return Ok(None),
            None => return Ok(None),
        };
        let mut array_val: Vec<RespTypeV2> = Vec::with_capacity(count as usize);
        for _i in 0..count as usize {
            // let val = Self::decode(raw)?;
            match self.decode(raw)? {
                Some(val) => array_val.push(val),
                None => return Ok(None),
            }
        }
        Ok(Some(RespTypeV2::Array(array_val)))
    }

    fn encode_array(&mut self, val: Vec<RespTypeV2>, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!("*{}\r\n", val.len()).as_bytes());
        for i in val {
            self.encode(i, raw)?;
        }
        Ok(())
    }

    fn decode_error(&mut self, raw: &mut BytesMut) -> Result<Option<RespTypeV2>, RedisError> {
        // TODO: It might have multiple CRLFs. Handle them all. Currently, it only fetches till first occurrence.

        match RespParserV2::find_crlf(raw) {
            Some(index) => {
                let value_str = String::from_utf8(raw[1..index].to_vec())
                    .map_err(|_| RedisError::RESP(RespError::UTFDecodingFailed))?;
                
                raw.advance(index + RespParserV2::CRLF.len());
                Ok(Some(RespTypeV2::Error(value_str)))
            },
            None => Ok(None),
        }
    }

    fn encode_error(&mut self, val: String, raw: &mut BytesMut) -> Result<(), RedisError> {
        raw.put(format!("-Err {}\r\n", val).as_bytes());
        Ok(())
    }

    fn decode_null(&mut self, raw: &mut BytesMut) -> Result<Option<RespTypeV2>, RedisError> {
        match RespParserV2::find_crlf(raw) {
            Some(index) => {
                if index != 1 {
                    return Ok(None)
                }
                raw.advance(index + RespParserV2::CRLF.len());
                Ok(Some(RespTypeV2::Null))
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
}

impl Decoder for RespParserV2 {
    type Item = RespTypeV2;
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

impl Encoder<RespTypeV2> for RespParserV2 {
    type Error = RedisError;

    fn encode(&mut self, item: RespTypeV2, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespTypeV2::String(val) => self.encode_string(val, dst)?,
            RespTypeV2::Integer(val) => self.encode_integer(val, dst)?,
            RespTypeV2::BulkString(val) => self.encode_bulk_string(val, dst)?,
            // RespTypeV2::Bytes(val) => Self::encode_bulkstring_without_crlf(val),
            RespTypeV2::Array(val) => self.encode_array(val, dst)?,
            RespTypeV2::Null => self.encode_null(dst)?,
            RespTypeV2::Error(val) => self.encode_error(val, dst)?,
            _ => return Err(RedisError::RESP(RespError::UnsupportedType)),
        }
        Ok(())
    }
}


#[cfg(test)]
mod resp {
    use bytes::{Buf, Bytes, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    use crate::{error::{RedisError, RespError}, resp_v2::{RespParserV2, RespTypeV2}};

    #[test]
    fn test_find_crlf() {
        let mut buff = BytesMut::from(":2\r\n:-2\r\n:23");
        assert_eq!(RespParserV2::find_crlf(&mut buff), Some(2));

        let mut buff = BytesMut::from("hello$5\r\nworld\r\n:-2\r\n:23");
        assert_eq!(RespParserV2::find_crlf(&mut buff), None);
    }

    #[test]
    fn test_decode_integer() {
        let mut buff = BytesMut::from(":2\r\n:-2\r\n:23");
        let mut parser = RespParserV2 {};

        let unsigned = parser.decode_integer(&mut buff);
        assert!(unsigned.is_ok());
        assert!(unsigned.as_ref().unwrap().is_some());
        assert_eq!(unsigned.unwrap().unwrap(), RespTypeV2::Integer(2));

        let signed = parser.decode_integer(&mut buff);
        assert!(signed.is_ok());
        assert!(signed.as_ref().unwrap().is_some());
        assert_eq!(signed.unwrap().unwrap(), RespTypeV2::Integer(-2));

        let signed = parser.decode_integer(&mut buff);
        assert!(signed.is_ok());
        assert!(signed.as_ref().unwrap().is_none());
    }

    #[test]
    fn test_encode_integer() {
        let mut buff = BytesMut::new();
        let mut parser = RespParserV2 {};

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
        let mut parser = RespParserV2 {};

        let str_val = parser.decode_string(&mut buff);
        assert!(str_val.is_ok());
        assert!(str_val.as_ref().unwrap().is_some());
        assert_eq!(str_val.unwrap().unwrap(), RespTypeV2::String("hello".to_string()));

        let empty_val = parser.decode_string(&mut buff);
        assert!(empty_val.is_ok());
        assert!(empty_val.as_ref().unwrap().is_some());
        assert_eq!(empty_val.unwrap().unwrap(), RespTypeV2::String("".to_string()));
    }

    #[test]
    fn test_encode_string() {
        let mut buff = BytesMut::new();
        let mut parser = RespParserV2 {};
        let str_val = parser.encode_string("hello".to_string(), &mut buff);
        assert!(str_val.is_ok());
        assert_eq!(buff, BytesMut::from("+hello\r\n"));
    }

    #[test]
    fn test_decode_bulk_string() {
        let mut buff = BytesMut::from("$5\r\nhello\r\n$0\r\n\r\n$5\r\nworld");
        let mut parser = RespParserV2 {};

        let bulk_str_val = parser.decode_bulk_string(&mut buff);
        assert!(bulk_str_val.is_ok());
        assert!(bulk_str_val.as_ref().unwrap().is_some());
        assert_eq!(bulk_str_val.unwrap().unwrap(), RespTypeV2::BulkString("hello".to_string()));

        let empty_val = parser.decode_bulk_string(&mut buff);
        assert!(empty_val.is_ok());
        assert!(empty_val.as_ref().unwrap().is_some());
        assert_eq!(empty_val.unwrap().unwrap(), RespTypeV2::BulkString("".to_string()));
    
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
        assert_eq!(rdb.unwrap().unwrap(), RespTypeV2::RDB(Bytes::from("REDISfoobarfoobar")));
    }

    #[test]
    fn test_encode_bulk_string() {
        let mut buff = BytesMut::new();
        let mut parser = RespParserV2 {};

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
        let mut parser = RespParserV2 {};

        let mut array = Vec::with_capacity(3);
        array.push(RespTypeV2::BulkString(String::from("foo")));
        array.push(RespTypeV2::BulkString(String::from("bar")));
        array.push(RespTypeV2::Integer(42));

        let array_val = parser.decode_array(&mut buff);
        assert!(array_val.is_ok());
        assert!(array_val.as_ref().unwrap().is_some());
        assert_eq!(array_val.unwrap().unwrap(), RespTypeV2::Array(array));
    }

    #[test]
    fn test_encode_array() {
        let mut buff = BytesMut::new();
        let mut parser = RespParserV2 {};

        let val = parser.encode_array(vec![
            RespTypeV2::BulkString("hello".to_string()),
            RespTypeV2::BulkString("world".to_string()),
            RespTypeV2::BulkString("foo".to_string()),
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
        let mut parser = RespParserV2 {};
        let val = parser.decode_error(&mut buff);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Some(RespTypeV2::Error("foo".to_string())));
    }

    #[test]
    fn test_encode_error() {
        let mut buff = BytesMut::new();
        let mut parser = RespParserV2 {};

        let val = parser.encode_error("foo".to_string(), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("-Err foo\r\n"));
    }

    #[test]
    fn test_decode_null() {
        let mut buff = BytesMut::from("_\r\n");
        let mut parser = RespParserV2 {};
        let val = parser.decode_null(&mut buff);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), Some(RespTypeV2::Null));
    }

    #[test]
    fn test_encode_null() {
        let mut buff = BytesMut::new();
        let mut parser = RespParserV2 {};

        let val = parser.encode_null(&mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("$-1\r\n"));
    }

    #[test]
    fn test_decode() {
        let mut buff = BytesMut::from("+hello\r\n$3\r\nfoo\r\n*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n-foo\r\n_\r\n$11\r\nREDISfoobar$3\r\nfoo\r\n");
        let mut parser = RespParserV2 {};

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap().unwrap(), RespTypeV2::String("hello".to_string()));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespTypeV2::BulkString("foo".to_string())));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespTypeV2::Array(
            vec![
                RespTypeV2::BulkString(String::from("foo")),
                RespTypeV2::BulkString(String::from("bar")),
                RespTypeV2::Integer(42),
            ]
        )));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespTypeV2::Error("foo".to_string())));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespTypeV2::Null));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespTypeV2::RDB(Bytes::from("REDISfoobar"))));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), Some(RespTypeV2::BulkString(String::from("foo"))));

        let decode_next = parser.decode(&mut buff);
        assert!(decode_next.is_ok());
        assert_eq!(decode_next.unwrap(), None);
    }

    #[test]
    fn test_encode() {
        let mut buff = BytesMut::new();
        let mut parser = RespParserV2 {};

        let val = parser.encode(RespTypeV2::String("foo".to_string()), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n"));

        let val = parser.encode(RespTypeV2::BulkString("bar".to_string()), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n$3\r\nbar\r\n"));

        let val = parser.encode(RespTypeV2::Null, &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n$3\r\nbar\r\n$-1\r\n"));

        let val = parser.encode(RespTypeV2::Error("err".to_string()), &mut buff);
        assert!(val.is_ok());
        assert_eq!(buff, BytesMut::from("+foo\r\n$3\r\nbar\r\n$-1\r\n-Err err\r\n"));

        let val = parser.encode(RespTypeV2::Integer(42), &mut buff);
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