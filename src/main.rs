#![allow(unused_imports)]
use core::str;
use std::char::ToLowercase;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::usize;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

mod resp;
use resp::{RedisResp, RedisRespError};


struct RedisBuffer {
    index: usize,
    buffer: Bytes,
}

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
        println!("New TCP connection from {:?}", addr);
        tokio::spawn(async move {
            let mut raw_buff: [u8; 512] = [0; 512];
            loop {
                let mut buff = RedisBuffer {
                    buffer: Bytes::new(),
                    index: 0,
                };
                let data = stream.read(&mut raw_buff).await.unwrap();
                buff.buffer = Bytes::copy_from_slice(&raw_buff[..data]);

                if data == 0 {
                    println!("TCP connection from {:?} closed", addr);
                    break;
                }
                let mut resp = RedisResp::new(buff);
                let decoded_word = resp.decode();

                match decoded_word {
                    Ok(word) => {
                        match resp.encode(word) {
                            Ok(reply) => {
                                stream.write_all(reply.as_ref()).await.unwrap()},
                            Err(err) => stream.write_all(format!("-Error {:?}\r\n", err).as_bytes()).await.unwrap(),
                        }
                    },
                    Err(err) => {
                        stream.write_all(format!("-Error {:?}\r\n", err).as_bytes()).await.unwrap()
                    },
                };
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
