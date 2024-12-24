use lzf::LzfError;

#[derive(Debug, PartialEq)]
pub(crate) enum RedisError {
    ParsingError,
    IncorrectBulkStringSize,
    UnknownCommand,
    UnknownConfig,
    CRLFMissing,
    InvalidValue,
    InvalidValueType,
    InvalidArraySize,
    InvalidArgValue(String),
    MissingArgs,
    SystemError(String),
    SyntaxError,
    RdbHexDecodeError,
    RdbMissingMagicString,
    RdbInvalidVersion,
    RDB(RDBError),
    InvalidUTF,
}

#[derive(Debug, PartialEq)]
pub(crate) enum RDBError {
    MissingMagicString,
    UnsupportedVersion(String),
    InvalidVersion,
    DbFileReadError,
    MissingBytes,
    MissingDbNumber,
    InvalidEOF,
    UnknownOpCode,
    InvalidEncodingType,
    InvalidLengthEncoding,
    InvalidStringEncoding,
    InvalidUtf8Encoding,
    LzfCompressionError(LzfError),
}
