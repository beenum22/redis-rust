use lzf::LzfError;

#[derive(Debug, PartialEq)]
pub(crate) enum RedisError {
    ParsingError,
    UnknownCommand,
    UnknownResponse,
    UnknownConfig,
    InvalidValueType,
    InvalidArgValue(String),
    MissingArgs,
    SyntaxError,
    RDB(RDBError),
    State(StateError),
    Connection(ConnectionError),
    RESP(RespError),
}

#[derive(Debug, PartialEq)]
pub(crate) enum ConnectionError {
    FailedReplicaConnection,
    FailedToWriteBytes,
    FailedToReadBytes,
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

#[derive(Debug, PartialEq)]
pub(crate) enum StateError {
    UnknownConfig,
    UnknownKey,
    LockError,
}

#[derive(Debug, PartialEq)]
pub(crate) enum RespError {
    UnknownType,
    InvalidValue,
    UnsupportedType,
    CRLFMissing,
    UTFDecodingFailed,
    IntegerParsingFailed,
    IncorrectBulkStringSize,
    HexDecodingFailed,
}
