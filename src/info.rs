use std::{collections::HashSet, net::SocketAddr};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplicationInfo {
    role: String,
    pub(crate) master_replid: String,
    pub(crate) master_repl_offset: i16,
    pub(crate) connected_slaves: u16,
    pub(crate) slaves: HashSet<SocketAddr>
}

impl ReplicationInfo {
    pub(crate) fn new(role: String) -> Self {
        ReplicationInfo {
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            connected_slaves: 0,
            slaves: HashSet::new(),
        }
    }

    pub(crate) fn get_all(info: Self) -> Vec<String> {
        let mut info_vec = vec!["# Replication".to_string()];
        info_vec.push(format!("role:{}", info.role));
        info_vec.push(format!("master_replid:{}", info.master_replid));
        info_vec.push(format!("master_repl_offset:{}", info.master_repl_offset));
        info_vec.push(format!("connection_slaves:{}", info.connected_slaves));
        let mut i: usize = 0;
        for slave in &info.slaves {
            info_vec.push(format!("slave{}:{}", i, slave));
            i += 1;
        }
        info_vec.push("\n".to_string(),);
        info_vec
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ServerInfo {
    pub(crate) redis_version: String,
}

impl ServerInfo {
    pub(crate) fn new() -> Self {
        ServerInfo {
            redis_version: "7.2.6".to_string(),
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
            server: ServerInfo::new(),
        }
    }
}
