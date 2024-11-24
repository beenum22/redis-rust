#![allow(unused_imports)]
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

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

    async fn stream_handler(&self, mut stream: TcpStream, addr: SocketAddr) {
        // TODO: Read PING specifically later
        println!("New TCP connection from {:?}", addr);
        tokio::spawn(async move {
            let mut buffer = [0; 512];
            loop {
                let data = stream.read(&mut buffer).await.unwrap();
                if data == 0 {
                    println!("TCP connection from {:?} closed", addr);
                    break;
                }
                stream.write_all(b"+PONG\r\n").await.unwrap()
            }
        });
    }

    async fn run(&self) -> () {
        let listener = TcpListener::bind(format!("{}:{}", self.addr.to_string(), self.port)).await.unwrap();
        println!("Redis Server is running on {}:{}", self.addr.to_string(), self.port);
        loop {
            let stream = listener.accept().await;
            match stream {
                Ok((stream, addr)) => {
                    self.stream_handler(stream, addr).await
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}


#[tokio::main]
async fn main() {    
    let redis_server = RedisServer::new("127.0.0.1", 6379);
    redis_server.run().await
}
