#[derive(Debug)]
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
    IOError(std::io::Error),
}