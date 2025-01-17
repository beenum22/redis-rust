use lzf::LzfError;
use std::io::Error;

#[derive(Debug)]
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
    Replica(ReplicaError),
    Server(ServerError),
    Operation(OperationError),
    IOError(Error),
}

impl From<std::io::Error> for RedisError {
    fn from(value: std::io::Error) -> Self {
        RedisError::IOError(value)
    }
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
    InvalidType,
    InvalidValue,
    UnsupportedType,
    CRLFMissing,
    UTFDecodingFailed,
    IntegerParsingFailed,
    IncorrectBulkStringSize,
    HexDecodingFailed,
}

#[derive(Debug, PartialEq)]
pub(crate) enum OperationError {
    InvalidRespType,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ReplicaError {
    BroadcastFailed,
    RegistrationFailed,
    DeregistrationFailed,
    StatusUknown,
    ConfigurationFailed,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ServerError {
    TcpStreamFailure,
    ReplicaRegistrationFailed
}
