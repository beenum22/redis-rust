#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplicationInfo {
    role: String,
    master_replid: String,
    master_repl_offset: i16,
}

impl ReplicationInfo {
    pub(crate) fn new(role: String) -> Self {
        ReplicationInfo {
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
        }
    }

    pub(crate) fn get_all(info: Self) -> Vec<String> {
        vec![
            "# Replication".to_string(),
            format!("role:{}", info.role),
            format!("master_replid:{}", info.master_replid),
            format!("master_repl_offset:{}", info.master_repl_offset),
            "\n".to_string(),
        ]
    }
    
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ServerInfo {
    pub(crate) redis_version: String,
}

impl ServerInfo {
    pub(crate) fn new() -> Self {
        ServerInfo {
            redis_version: "7.2.6".to_string()
        }
    }

    pub(crate) fn get_all(info: Self) -> Vec<String> {
        vec![
            "# Server".to_string(),
            format!("redis_version:{}", info.redis_version),
            "\n".to_string(),
        ]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum InfoOperation {
    Replication,
    Server,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Info {
    pub(crate) replication: ReplicationInfo,
    pub(crate) server: ServerInfo,
}

impl Info {
    pub(crate) fn new(role: String) -> Self {
        Self {
            replication: ReplicationInfo::new(role),
            server: ServerInfo::new()
        }
    }
}
