#![allow(unused_imports)]
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::IpAddr;
use std::net::TcpStream;
use std::str::FromStr;

struct RedisServer {
    addr: IpAddr,
    port: u16
}

impl RedisServer {
    fn new(addr: &str, port: u16) -> Self {
        Self {
            addr: IpAddr::from_str(addr).unwrap(),
            port: port
        }
    }

    fn run(&self) -> () {
        let listener = TcpListener::bind(format!("{}:{}", self.addr.to_string(), self.port)).unwrap();
        println!("Redis Server is running on {}:{}", self.addr.to_string(), self.port);
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    // TODO: Read PING specifically later
                    stream.write_all(b"+PONG\r\n").unwrap()
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}


fn main() {    
    let redis_server = RedisServer::new("127.0.0.1", 6379);
    redis_server.run()
}
