#[derive(Clone)]
pub(crate) enum ConfigParam {
    Dir(Option<(String, String)>),
    DbFileName(Option<(String, String)>),
    Unknown,
}

#[derive(Clone)]
pub(crate) enum ConfigOperation {
    Get(ConfigParam),
    Set(ConfigParam),
}

pub(crate) struct Config {
    pub(crate) dir: Option<(String, String)>,
    pub(crate) dbfilename: Option<(String, String)>,
}

impl Config {
    pub(crate) fn new() -> Self {
        Config {
            dir: Some(("dir".to_string(), "/data".to_string())),
            dbfilename: Some(("dbfilename".to_string(), "dump.rdb".to_string())),
        }
    }
}
