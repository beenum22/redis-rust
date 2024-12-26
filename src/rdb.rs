use std::collections::HashMap;
use std::io::Read;
use std::str;
use std::time::{SystemTime, Duration, UNIX_EPOCH};

use crate::{RDBError, RedisError, Config, Operation, SetMap, SetExpiryArgs};

const RDB_MAGIC: &[u8] = b"REDIS";
const RDB_MIN_VERSION: u32 = 10;
const RDB_MAX_VERSION: u32 = 12;

#[derive(PartialEq, Debug)]
pub(crate) enum RdbOpCode {
    EOF,
    SELECTDB,
    EXPIRETIME,
    EXPIRETIMEMS,
    RESIZEDB,
    AUX,
    Unknown(u8),
}

impl RdbOpCode {
    const RDB_OPCODE_EOF: u8 = 255;
    const RDB_OPCODE_SELECTDB: u8 = 254;
    const RDB_OPCODE_EXPIRETIME: u8 = 253;
    const RDB_OPCODE_EXPIRETIMEMS: u8 = 252;
    const RDB_OPCODE_RESIZEDB: u8 = 251;
    const RDB_OPCODE_AUX: u8 = 250;

    fn parse_opcode(code: u8) -> RdbOpCode {
        match code {
            Self::RDB_OPCODE_EOF => RdbOpCode::EOF,
            Self::RDB_OPCODE_SELECTDB => RdbOpCode::SELECTDB,
            Self::RDB_OPCODE_EXPIRETIME => RdbOpCode::EXPIRETIME,
            Self::RDB_OPCODE_EXPIRETIMEMS => RdbOpCode::EXPIRETIMEMS,
            Self::RDB_OPCODE_RESIZEDB => RdbOpCode::RESIZEDB,
            Self::RDB_OPCODE_AUX => RdbOpCode::AUX,
            _ => RdbOpCode::Unknown(code),
        }
    }
}

#[derive(Debug)]
enum RdbLengthEncoding {
    Length(u32),
    Encoded(u32),
}

#[derive(Debug)]
enum RdbStringInteger {
    Int8(u8),
    Int16(u16),
    Int32(u32),
}

#[derive(Debug)]
enum RdbStringEncoding {
    Length(String),
    Integer(RdbStringInteger),
    Compression(String),
}

impl RdbStringEncoding {
    fn to_string(&self) -> String {
        match self {
            RdbStringEncoding::Length(s) => s.clone(),
            RdbStringEncoding::Integer(i) => match i {
                RdbStringInteger::Int8(val) => val.to_string(),
                RdbStringInteger::Int16(val) => val.to_string(),
                RdbStringInteger::Int32(val) => val.to_string(),
            },
            RdbStringEncoding::Compression(s) => s.clone(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct RdbAuxField {
    key: String,
    value: String,
}

#[derive(Debug)]
pub(crate) struct RdbDbResize {
    db_size: u32,
    expires_size: u32,
}

#[derive(Debug)]
pub(crate) struct RdbSelectDb {
    pub(crate) keys: Vec<Operation>,
}

#[derive(Debug)]
pub(crate) struct RdbParser {
    version: String,
    pub(crate) aux_keys: Vec<RdbAuxField>,
    pub(crate) dbs: HashMap<u8, RdbSelectDb>,
}

impl RdbParser {
    fn _reader_helper<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<(), RedisError> {
        match reader.read_exact(buf) {
            Ok(_) => {
                // println!("Buffer: {:?}", buf);
                Ok(())
            }
            Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
        }
    }

    fn _verify_magic<R: Read>(reader: &mut R) -> Result<(), RedisError> {
        let mut magic = [0u8; 5];
        match reader.read(&mut magic) {
            Ok(5) => {
                if magic != RDB_MAGIC {
                    return Err(RedisError::RDB(RDBError::MissingMagicString));
                }
            }
            _ => return Err(RedisError::RDB(RDBError::MissingBytes)),
        }
        Ok(())
    }

    fn _verify_version<R: Read>(reader: &mut R) -> Result<String, RedisError> {
        let mut buf = [0u8; 4];
        match reader.read(&mut buf) {
            Ok(4) => {
                let version: u32 = std::str::from_utf8(&buf)
                    .map_err(|_| RedisError::RDB(RDBError::InvalidUtf8Encoding))?
                    .parse()
                    .map_err(|_| RedisError::RDB(RDBError::InvalidVersion))?;
                if version < RDB_MIN_VERSION || version > RDB_MAX_VERSION {
                    return Err(RedisError::RDB(RDBError::UnsupportedVersion(version.to_string())));
                }
                Ok(version.to_string())
            },
            _ => return Err(RedisError::RDB(RDBError::MissingBytes)),
        }
    }

    fn _parse_opcode<R: Read>(reader: &mut R) -> Result<RdbOpCode, RedisError> {
        let mut op_buf = [0u8; 1];
        match Self::_reader_helper(reader, &mut op_buf) {
            Ok(_) => Ok(RdbOpCode::parse_opcode(op_buf[0])),
            Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
        }
    }

    fn _parse_db<R: Read>(reader: &mut R) -> Result<u8, RedisError> {
        let mut db_buf = [0u8; 1];
        match Self::_reader_helper(reader, &mut db_buf) {
            Ok(_) => Ok(db_buf[0]),
            Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
        }
    }

    fn decode_length_encoding<R: Read>(reader: &mut R) -> Result<RdbLengthEncoding, RedisError> {
        let mut buf = [0u8; 1];
        match Self::_reader_helper(reader, &mut buf) {
            Ok(_) => {
                let msb = (buf[0]) >> 6;
                match msb {
                    0 => Ok(RdbLengthEncoding::Length((buf[0] & 0x3F) as u32)),
                    1 => {
                        let rest_of_bits = buf[0] & 0x3F;
                        let mut next_byte = [0u8; 1];
                        match Self::_reader_helper(reader, &mut next_byte) {
                            Ok(_) => Ok(RdbLengthEncoding::Length(
                                ((rest_of_bits as u32) << 8) | (next_byte[0] as u32),
                            )),
                            Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
                        }
                    }
                    2 => {
                        let mut next_bytes = [0u8; 4];
                        match Self::_reader_helper(reader, &mut next_bytes) {
                            Ok(_) => Ok(RdbLengthEncoding::Length(next_bytes[0] as u32)),
                            Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
                        }
                    }
                    3 => Ok(RdbLengthEncoding::Encoded((buf[0] & 0x3F) as u32)),
                    _ => unreachable!(),
                }
            }
            Err(_) => return Err(RedisError::RDB(RDBError::MissingBytes)),
        }
    }

    fn decode_string_encoding<R: Read>(reader: &mut R) -> Result<RdbStringEncoding, RedisError> {
        match Self::decode_length_encoding(reader)? {
            RdbLengthEncoding::Length(l) => {
                let mut len_buf = vec![0u8; l as usize];
                match Self::_reader_helper(reader, &mut len_buf) {
                    Ok(_) => Ok(RdbStringEncoding::Length(
                        String::from_utf8(len_buf).map_err(|err| {
                            RedisError::RDB(RDBError::InvalidUtf8Encoding)
                        })?,
                    )),
                    Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
                }
            }
            RdbLengthEncoding::Encoded(l) => match l {
                0 => {
                    let mut int_buf = [0u8; 1];
                    match Self::_reader_helper(reader, &mut int_buf) {
                        Ok(_) => Ok(RdbStringEncoding::Integer(RdbStringInteger::Int8(
                            int_buf[0],
                        ))),
                        Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
                    }
                }
                1 => {
                    let mut int_buf = [0u8; 2];
                    match Self::_reader_helper(reader, &mut int_buf) {
                        Ok(_) => Ok(RdbStringEncoding::Integer(RdbStringInteger::Int16(
                            u16::from_be_bytes(int_buf),
                        ))),
                        Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
                    }
                }
                2 => {
                    let mut int_buf = [0u8; 4];
                    match Self::_reader_helper(reader, &mut int_buf) {
                        Ok(_) => Ok(RdbStringEncoding::Integer(RdbStringInteger::Int32(
                            u32::from_be_bytes(int_buf),
                        ))),
                        Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
                    }
                }
                3 => {
                    let clen = match Self::decode_length_encoding(reader)? {
                        RdbLengthEncoding::Length(l) => l,
                        RdbLengthEncoding::Encoded(_) => {
                            return Err(RedisError::RDB(RDBError::InvalidLengthEncoding))
                        }
                    };
                    let len = match Self::decode_length_encoding(reader)? {
                        RdbLengthEncoding::Length(l) => l,
                        RdbLengthEncoding::Encoded(_) => {
                            return Err(RedisError::RDB(RDBError::InvalidLengthEncoding))
                        }
                    };
                    let mut clen_buf = vec![0u8; clen as usize];
                    match Self::_reader_helper(reader, &mut clen_buf) {
                        Ok(_) => {
                            let data = lzf::decompress(&clen_buf, len as usize).map_err(|err| {
                                RedisError::RDB(RDBError::LzfCompressionError(err))
                            })?;
                            Ok(RdbStringEncoding::Compression(
                                String::from_utf8(data).map_err(|err| {
                                    RedisError::RDB(RDBError::InvalidUtf8Encoding)
                                })?,
                            ))
                        }
                        Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
                    }
                }
                _ => Err(RedisError::RDB(RDBError::InvalidLengthEncoding)),
            },
        }
    }

    fn decode_aux_field<R: Read>(reader: &mut R) -> Result<RdbAuxField, RedisError> {
        let key = match Self::decode_string_encoding(reader)? {
            RdbStringEncoding::Length(s) => s,
            RdbStringEncoding::Integer(i) => match i {
                RdbStringInteger::Int8(val) => val.to_string(),
                RdbStringInteger::Int16(val) => val.to_string(),
                RdbStringInteger::Int32(val) => val.to_string(),
            },
            RdbStringEncoding::Compression(s) => s,
            _ => return Err(RedisError::RDB(RDBError::InvalidStringEncoding)),
        };
        let val = match Self::decode_string_encoding(reader)? {
            RdbStringEncoding::Length(s) => s,
            RdbStringEncoding::Integer(i) => match i {
                RdbStringInteger::Int8(val) => val.to_string(),
                RdbStringInteger::Int16(val) => val.to_string(),
                RdbStringInteger::Int32(val) => val.to_string(),
            },
            RdbStringEncoding::Compression(s) => s,
            _ => return Err(RedisError::RDB(RDBError::InvalidStringEncoding)),
        };
        Ok(RdbAuxField { key, value: val })
    }

    fn decode_resizedb<R: Read>(reader: &mut R) -> Result<RdbDbResize, RedisError> {
        let db_size = match Self::decode_length_encoding(reader)? {
            RdbLengthEncoding::Length(len) => len,
            _ => return Err(RedisError::RDB(RDBError::InvalidLengthEncoding)),
        };
        let expires_size = match Self::decode_length_encoding(reader)? {
            RdbLengthEncoding::Length(len) => len,
            _ => return Err(RedisError::RDB(RDBError::InvalidLengthEncoding)),
        };
        Ok(RdbDbResize {
            db_size,
            expires_size,
        })
    }

    fn decode_expiry<R: Read>(reader: &mut R) -> Result<u32, RedisError> {
        let mut buf = [0u8; 4];
        match Self::_reader_helper(reader, &mut buf) {
            Ok(_) => Ok(u32::from_be_bytes(buf)),
            Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
        }
    }

    fn decode_expiry_ms<R: Read>(reader: &mut R) -> Result<u64, RedisError> {
        let mut buf = [0u8; 8];
        match Self::_reader_helper(reader, &mut buf) {
            Ok(_) => Ok(u64::from_be_bytes(buf)),
            Err(_) => Err(RedisError::RDB(RDBError::MissingBytes)),
        }
    }

    fn decode_key_value<R: Read>(
        reader: &mut R,
        val_type: Option<u8>,
    ) -> Result<SetMap, RedisError> {
        let val_type = if val_type.is_none() {
            let mut buf = [0u8; 1];
            match Self::_reader_helper(reader, &mut buf) {
                Ok(_) => Some(buf[0]),
                Err(_) => return Err(RedisError::RDB(RDBError::MissingBytes)),
            }
        } else {
            val_type
        };
        let key = Self::decode_string_encoding(reader)?.to_string();
        let val = match val_type {
            Some(0) => Self::decode_string_encoding(reader)?.to_string(),
            _ => return Err(RedisError::RDB(RDBError::InvalidEncodingType)),
        };
        Ok(SetMap {
            key,
            val,
            expiry: None,
            overwrite: None,
            keepttl: None,
            expiry_timestamp: None,
        })
    }

    pub fn decode<R: Read>(reader: &mut R) -> Result<RdbParser, RedisError> {
        Self::_verify_magic(reader)?;
        let mut parser = Self {
            version: Self::_verify_version(reader)?,
            aux_keys: Vec::new(),
            dbs: HashMap::new(),
        };

        let mut select_db: Option<u8> = None;
        loop {
            match Self::_parse_opcode(reader) {
                Ok(RdbOpCode::EOF) => break,
                Ok(RdbOpCode::SELECTDB) => {
                    select_db = Some(Self::_parse_db(reader)?);
                    parser.dbs.entry(select_db.unwrap()).or_insert(RdbSelectDb {
                        // aux_keys: Vec::new(),
                        keys: Vec::new(),
                    });
                }
                Ok(RdbOpCode::EXPIRETIME) => {
                    let expiry = Self::decode_expiry(reader)? as u64;
                    match select_db {
                        Some(db) => {
                            let mut key_val = Self::decode_key_value(reader, None)?;
                            key_val.expiry = Some(SetExpiryArgs::EXAT(expiry));
                            key_val.expiry_timestamp =
                                Some(UNIX_EPOCH + Duration::from_secs(expiry));
                            if let Some(db_entry) = parser.dbs.get_mut(&db) {
                                db_entry.keys.push(Operation::Set(key_val));
                            }
                        }
                        None => return Err(RedisError::RDB(RDBError::MissingDbNumber)),
                    }
                }
                Ok(RdbOpCode::EXPIRETIMEMS) => {
                    let mut expiry = Self::decode_expiry_ms(reader)? as u128;
                    match select_db {
                        Some(db) => {
                            let mut key_val = Self::decode_key_value(reader, None)?;
                            key_val.expiry = Some(SetExpiryArgs::PXAT(expiry));
                            // Temporary fix added to handle timestamps in nanoseconds.
                            if expiry >= 10_000_000_000_000_000 {
                                expiry = expiry / 1_000_000
                            }
                            key_val.expiry_timestamp =
                                Some(UNIX_EPOCH + Duration::from_millis(expiry as u64));
                            if let Some(db_entry) = parser.dbs.get_mut(&db) {
                                db_entry.keys.push(Operation::Set(key_val));
                            }
                        }
                        None => return Err(RedisError::RDB(RDBError::MissingDbNumber)),
                    }
                }
                Ok(RdbOpCode::RESIZEDB) => {
                    // Info not used anywhere
                    Self::decode_resizedb(reader)?;
                },
                Ok(RdbOpCode::AUX) => parser.aux_keys.push(Self::decode_aux_field(reader)?),
                Ok(RdbOpCode::Unknown(val)) => match select_db {
                    Some(db) => {
                        let mut key_val = Self::decode_key_value(reader, Some(val))?;
                        if let Some(db_entry) = parser.dbs.get_mut(&db) {
                            db_entry.keys.push(Operation::Set(key_val));
                        }
                    }
                    None => return Err(RedisError::RDB(RDBError::MissingDbNumber)),
                },
                Err(e) => return Err(e),
            }
        }
        Ok(parser)
    }
}

#[cfg(test)]
mod rdb_parser {
    use super::{RdbParser, RdbOpCode, RdbLengthEncoding, RdbStringEncoding, RdbStringInteger, RdbAuxField};
    use crate::{
        RDBError, RedisError, Config, Operation, SetMap, SetExpiryArgs
    };
    // use crate::{
    //     resp::Operation,
    //     state::{
    //         RDBError, RdbAuxField, RdbLengthEncoding, RdbOpCode, RdbSelectDb, RdbStringEncoding,
    //         RdbStringInteger, RedisError, SetExpiryArgs, SetMap,
    //     },
    // };
    use hex;
    use std::io::Cursor;
    use std::time::{Duration, SystemTime};
    

    #[test]
    fn test_parse_opcode() {
        assert_eq!(RdbOpCode::parse_opcode(255), RdbOpCode::EOF);
        assert_eq!(RdbOpCode::parse_opcode(254), RdbOpCode::SELECTDB);
        assert_eq!(RdbOpCode::parse_opcode(253), RdbOpCode::EXPIRETIME);
        assert_eq!(RdbOpCode::parse_opcode(252), RdbOpCode::EXPIRETIMEMS);
        assert_eq!(RdbOpCode::parse_opcode(251), RdbOpCode::RESIZEDB);
        assert_eq!(RdbOpCode::parse_opcode(250), RdbOpCode::AUX);
    }

    #[test]
    fn test_verify_magic() {
        let invalid_data = RdbParser::_verify_magic(&mut Cursor::new(b"RED"));
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        let invalid_magic = RdbParser::_verify_magic(&mut Cursor::new(b"FOOBAR"));
        assert!(invalid_magic.is_err());
        assert_eq!(
            invalid_magic.unwrap_err(),
            RedisError::RDB(RDBError::MissingMagicString)
        );

        let valid_magic = RdbParser::_verify_magic(&mut Cursor::new(b"REDIS0100"));
        assert!(valid_magic.is_ok());
    }

    #[test]
    fn test_verify_version() {
        let invalid_data = RdbParser::_verify_version(&mut Cursor::new(b"RED"));
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        let invalid_ver = RdbParser::_verify_version(&mut Cursor::new(b"0100FOOBAR"));
        assert!(invalid_ver.is_err());
        assert_eq!(
            invalid_ver.unwrap_err(),
            RedisError::RDB(RDBError::UnsupportedVersion("100".to_string()))
        );

        let valid_ver = RdbParser::_verify_version(&mut Cursor::new(b"0003FOOBAR"));
        assert!(valid_ver.is_ok());
        assert_eq!(valid_ver.unwrap(), "0003".to_string())
    }

    #[test]
    fn test_decode_length_encoding() {
        let invalid_data = RdbParser::decode_length_encoding(&mut Cursor::new(b""));
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        // Test case where msb is 0
        let valid_msb_0 = RdbParser::decode_length_encoding(&mut Cursor::new(b"\x1F"));
        assert!(valid_msb_0.is_ok());
        match valid_msb_0.unwrap() {
            RdbLengthEncoding::Length(len) => assert_eq!(len, 31),
            _ => panic!("Unexpected encoding value"),
        }

        // Test case where msb is 1
        let valid_msb_1 = RdbParser::decode_length_encoding(&mut Cursor::new(b"\x43\x0F"));
        assert!(valid_msb_1.is_ok());
        match valid_msb_1.unwrap() {
            RdbLengthEncoding::Length(len) => assert_eq!(len, 783),
            _ => panic!("Unexpected encoding value"),
        }

        // Test case where msb is 2
        let valid_msb_2 =
            RdbParser::decode_length_encoding(&mut Cursor::new(b"\x80\x01\x02\x03\x04"));
        assert!(valid_msb_2.is_ok());
        match valid_msb_2.unwrap() {
            RdbLengthEncoding::Length(len) => assert_eq!(len, 1),
            _ => panic!("Unexpected encoding value"),
        }

        // Test case where msb is 3
        let valid_msb_3 = RdbParser::decode_length_encoding(&mut Cursor::new(b"\xC0"));
        assert!(valid_msb_3.is_ok());
        match valid_msb_3.unwrap() {
            RdbLengthEncoding::Encoded(len) => assert_eq!(len, 0),
            _ => panic!("Unexpected encoding value"),
        }
    }

    #[test]
    fn test_decode_string_encoding() {
        let invalid_data = RdbParser::decode_string_encoding(&mut Cursor::new(b""));
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        // Test case where msb is not 3
        let valid_len_prefix = RdbParser::decode_string_encoding(&mut Cursor::new(b"\x03foo")); // len 3 and string "foo"
        assert!(valid_len_prefix.is_ok());
        match valid_len_prefix.unwrap() {
            RdbStringEncoding::Length(s) => assert_eq!(s, "foo".to_string()),
            _ => panic!("Unexpected encoding value"),
        }

        // Test case where msb is 3 and integer is 0
        let valid_int_u8 = RdbParser::decode_string_encoding(&mut Cursor::new(b"\xC0\x00")); // int 0
        assert!(valid_int_u8.is_ok());
        match valid_int_u8.unwrap() {
            RdbStringEncoding::Integer(RdbStringInteger::Int8(val)) => assert_eq!(val, 0),
            _ => panic!("Unexpected encoding value"),
        }

        // Test case where msb is 3 and integer is 1
        let valid_int_u16 = RdbParser::decode_string_encoding(&mut Cursor::new(b"\xC1\x00\x0F")); // int 15
        assert!(valid_int_u16.is_ok());
        match valid_int_u16.unwrap() {
            RdbStringEncoding::Integer(RdbStringInteger::Int16(val)) => assert_eq!(val, 15),
            _ => panic!("Unexpected encoding value"),
        }

        // Test case where msb is 3 and integer is 2
        let valid_int_u32 =
            RdbParser::decode_string_encoding(&mut Cursor::new(b"\xC2\x00\x00\x00\x01")); // int 1
        assert!(valid_int_u32.is_ok());
        match valid_int_u32.unwrap() {
            RdbStringEncoding::Integer(RdbStringInteger::Int32(val)) => assert_eq!(val, 1),
            _ => panic!("Unexpected encoding value"),
        }

        // Test case where msb is 3
        let valid_msb_3 = RdbParser::decode_string_encoding(&mut Cursor::new(
            b"\xC3\x08\x09\x01\x61\x61\x60\x00\x01\x61\x61",
        )); // compressed "aaaaaaaaa"
            // let valid_msb_3 = RdbParser::decode_string_encoding(&mut Cursor::new(b"3"));
        assert!(valid_msb_3.is_ok());
        match valid_msb_3.unwrap() {
            RdbStringEncoding::Compression(s) => assert_eq!(s, "aaaaaaaaa".to_string()),
            _ => panic!("Unexpected encoding value"),
        }
    }

    #[test]
    fn test_decode_aux_field() {
        let invalid_data = RdbParser::decode_aux_field(&mut Cursor::new(b""));
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        let valid_aux_field =
            RdbParser::decode_aux_field(&mut Cursor::new(b"\x03\x66\x6F\x6F\x03\x66\x6F\x6F")); // key "foo" and value "foo"
        assert!(valid_aux_field.is_ok());
        let aux_field = valid_aux_field.unwrap();
        assert_eq!(aux_field.key, "foo".to_string());
        assert_eq!(aux_field.value, "foo".to_string());
    }

    #[test]
    fn test_decode_resizedb() {
        let invalid_data = RdbParser::decode_resizedb(&mut Cursor::new(b"\xC3\x04"));
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::InvalidLengthEncoding)
        );

        let valid_resizedb = RdbParser::decode_resizedb(&mut Cursor::new(b"\x03\x04")); // resizedb=3, expires=4
        assert!(valid_resizedb.is_ok());
        let resizedb = valid_resizedb.unwrap();
        assert_eq!(resizedb.db_size, 3);
        assert_eq!(resizedb.expires_size, 4);
    }

    #[test]
    fn test_decode_expiry() {
        let invalid_data = RdbParser::decode_expiry(&mut Cursor::new(b""));
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        let valid_expiry = RdbParser::decode_expiry(&mut Cursor::new(b"\x61\x56\x4F\x80")); // expiry=1633046400
        assert!(valid_expiry.is_ok());
        assert_eq!(valid_expiry.unwrap(), 1633046400);
    }

    #[test]
    fn test_decode_expiry_ms() {
        let invalid_data = RdbParser::decode_expiry_ms(&mut Cursor::new(b""));
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        let valid_expiry =
            RdbParser::decode_expiry_ms(&mut Cursor::new(b"\x00\x00\x01\x7C\x39\x26\x8C\00")); // expiry_ms=1633046400000
        assert!(valid_expiry.is_ok());
        assert_eq!(valid_expiry.unwrap(), 1633046400000);
    }

    #[test]
    fn test_decode_key_value() {
        let invalid_data = RdbParser::decode_key_value(&mut Cursor::new(b""), None);
        assert!(invalid_data.is_err());
        assert_eq!(
            invalid_data.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        // Test case where val_type is None
        let valid_key_value = RdbParser::decode_key_value(
            &mut Cursor::new(b"\x00\x03\x66\x6F\x6F\x03\x62\x61\x72"),
            None,
        ); // key="foo", val="bar"
        assert!(valid_key_value.is_ok());
        let key_value = valid_key_value.unwrap();
        assert_eq!(key_value.key, "foo".to_string());
        assert_eq!(key_value.val, "bar".to_string());

        // Test case where val_type is Some(_)
        let valid_key_value = RdbParser::decode_key_value(
            &mut Cursor::new(b"\x03\x66\x6F\x6F\x03\x62\x61\x72"),
            Some(0),
        ); // key="foo", val="bar"
        assert!(valid_key_value.is_ok());
        let key_value = valid_key_value.unwrap();
        assert_eq!(key_value.key, "foo".to_string());
        assert_eq!(key_value.val, "bar".to_string());
    }

    #[test]
    fn test_decode() {
        // Testcase for invalid data
        let invalid_parser = RdbParser::decode(&mut Cursor::new(b"0003FOOBAR"));
        assert!(invalid_parser.is_err());
        assert_eq!(
            invalid_parser.unwrap_err(),
            RedisError::RDB(RDBError::MissingMagicString)
        );

        // Testcase for invalid version
        let invalid_ver =
            RdbParser::decode(&mut Cursor::new(b"REDIS0113\xFE\x01\xFA\x03foo\x03bar\xFF"));
        assert!(invalid_ver.is_err());
        assert_eq!(
            invalid_ver.unwrap_err(),
            RedisError::RDB(RDBError::UnsupportedVersion("100".to_string()))
        );

        // Testcase for missing SelectDB
        let invalid_eof =
            RdbParser::decode(&mut Cursor::new(b"REDIS0003\xFA\x03foo\x03bar\x03bar")); // \x03\x66\x6F\x6F\x03\x66\x6F\x6F
        assert!(invalid_eof.is_err());
        assert_eq!(
            invalid_eof.unwrap_err(),
            RedisError::RDB(RDBError::MissingDbNumber)
        );

        // Testcase for missing bytes or invalid EOF
        let invalid_eof =
            RdbParser::decode(&mut Cursor::new(b"REDIS0003\xFE\x01\xFA\x03foo\x03bar")); // \x03\x66\x6F\x6F\x03\x66\x6F\x6F
        assert!(invalid_eof.is_err());
        assert_eq!(
            invalid_eof.unwrap_err(),
            RedisError::RDB(RDBError::MissingBytes)
        );

        // Testcase for valid RDB with only auxiliary keys
        let valid_aux_parser =
            RdbParser::decode(&mut Cursor::new(b"REDIS0003\xFA\x03foo\x03bar\xFE\x01\xFF")); // \x03\x66\x6F\x6F\x03\x66\x6F\x6F
        assert!(valid_aux_parser.is_ok());
        let aux_parser = valid_aux_parser.unwrap();
        assert_eq!(aux_parser.version, "0003".to_string());
        assert!(aux_parser.dbs.contains_key(&1));
        assert_eq!(
            aux_parser.aux_keys,
            vec![RdbAuxField {
                key: "foo".to_string(),
                value: "bar".to_string()
            }]
        );
        assert_eq!(aux_parser.dbs.get(&1).unwrap().keys, Vec::new());

        // Testcase for valid RDB with keys and expiry in seconds
        let valid_parser_sec = RdbParser::decode(&mut Cursor::new(
            b"REDIS0003\xFA\x03foo\x03bar\xFE\x00\xFD\x61\x56\x4F\x80\x00\x03bar\x03foo\xFF",
        ));
        assert!(valid_parser_sec.is_ok());
        let parser = valid_parser_sec.unwrap();
        assert_eq!(parser.version, "0003".to_string());
        assert!(parser.dbs.contains_key(&0));
        assert_eq!(
            parser.dbs.get(&0).unwrap().keys,
            vec![Operation::Set(SetMap {
                key: "bar".to_string(),
                val: "foo".to_string(),
                expiry: Some(SetExpiryArgs::EXAT(1633046400)),
                overwrite: None,
                keepttl: None,
                expiry_timestamp: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1633046400)),
            })]
        );

        // Testcase for valid RDB with keys and expiry in milliseconds
        let valid_parser_msec = RdbParser::decode(&mut Cursor::new(b"REDIS0003\xFA\x03foo\x03bar\xFE\x00\xFC\x00\x00\x01\x7C\x39\x26\x8C\x00\x00\x03bar\x03foo\xFF"));
        assert!(valid_parser_msec.is_ok());
        let parser_msec = valid_parser_msec.unwrap();
        assert_eq!(parser_msec.version, "0003".to_string());
        assert!(parser_msec.dbs.contains_key(&0));
        assert_eq!(
            parser_msec.dbs.get(&0).unwrap().keys,
            vec![Operation::Set(SetMap {
                key: "bar".to_string(),
                val: "foo".to_string(),
                expiry: Some(SetExpiryArgs::PXAT(1633046400000)),
                overwrite: None,
                keepttl: None,
                expiry_timestamp: Some(
                    SystemTime::UNIX_EPOCH + Duration::from_millis(1633046400000)
                ),
            })]
        );

        // Testcase for valid RDB with keys and no expiries
        let valid_parser = RdbParser::decode(&mut Cursor::new(
            b"REDIS0003\xFA\x03foo\x03bar\xFE\x00\x00\x03bar\x03foo\xFF",
        ));
        assert!(valid_parser.is_ok());
        let parser = valid_parser.unwrap();
        assert_eq!(parser.version, "0003".to_string());
        assert!(parser.dbs.contains_key(&0));
        assert_eq!(
            parser.dbs.get(&0).unwrap().keys,
            vec![Operation::Set(SetMap {
                key: "bar".to_string(),
                val: "foo".to_string(),
                expiry: None,
                overwrite: None,
                keepttl: None,
                expiry_timestamp: None,
            })]
        );
    }
}
