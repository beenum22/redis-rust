#[derive(Clone, PartialEq, Debug)]
pub(crate) enum ConfigParam {
    Dir(Option<ConfigPair>),
    DbFileName(Option<ConfigPair>),
    Unknown,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ConfigPair {
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum ConfigOperation {
    Get(Vec<ConfigParam>),
    Set(Vec<ConfigParam>),
}


#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) dir: Option<ConfigPair>,
    pub(crate) dbfilename: Option<ConfigPair>,
}

impl Config {
    pub(crate) fn new(dir: String, dbfilename: String) -> Self {
        Config {
            dir: Some(ConfigPair {
                key: "dir".to_string(),
                value: dir,
            }),
            dbfilename: Some(ConfigPair {
                key: "dbfilename".to_string(),
                value: dbfilename,
            }),
        }
    }
}
