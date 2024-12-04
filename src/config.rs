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
    pub(crate) fn new(dir: String, dbfilename: String) -> Self {
        Config {
            dir: Some(("dir".to_string(), dir)),
            dbfilename: Some(("dbfilename".to_string(), dbfilename)),
        }
    }
}
