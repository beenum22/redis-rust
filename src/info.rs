#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplicationInfo {
    role: String,
}

impl ReplicationInfo {
    pub(crate) fn new(role: String) -> Self {
        ReplicationInfo {
            role
        }
    }

    pub(crate) fn get_all(info: Self) -> Vec<String> {
        vec![
            "# Replication".to_string(),
            format!("role:{}\n", info.role),
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
            format!("redis_version:{}\n", info.redis_version),
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
