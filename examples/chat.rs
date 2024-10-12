use core::fmt;
use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use dashmap::DashMap;
use futures::{stream::SplitStream, SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Sender},
};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{info, level_filters::LevelFilter, warn};
use tracing_subscriber::{fmt::Layer, layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

const MSG_SIZE: usize = 1024;

#[derive(Debug)]
struct ChatState {
    peers: DashMap<SocketAddr, Sender<Arc<Message>>>,
}

#[derive(Debug)]
enum Message {
    Join(String),
    Left(String),
    Text { user: String, content: String },
}

#[derive(Debug)]
struct Peer {
    username: String,
    stream: SplitStream<Framed<TcpStream, LinesCodec>>,
}
#[tokio::main]
async fn main() -> Result<()> {
    let layer = Layer::new().with_filter(LevelFilter::INFO);
    tracing_subscriber::registry().with(layer).init();

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    let state = Arc::new(ChatState::new());
    loop {
        let (stream, addr) = listener.accept().await?;
        let state = Arc::clone(&state);
        info!("New connection from {}", addr);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, addr, state).await {
                warn!("Error handling connection from {}: {}", addr, e);
            };
            info!("Connection closed for {}", addr);
        });
    }

    #[allow(unreachable_code)]
    Ok(())
}
async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    state: Arc<ChatState>,
) -> Result<()> {
    // 将TcpStream使用LinesCodec封装成Framed对象，按行进行数据分割
    let mut stream = Framed::new(stream, LinesCodec::new());

    // send方法是由futures这个crate的SinkExt  trait实现的，可以异步地将数据发送到流中
    stream.send("Enter your name:").await?;
    // next方法返回Option<Result<>>，本来可以使用？？进行错误传播的，但是这个handler的返回类型为Result
    let username = match stream.next().await {
        Some(option) => match option {
            Ok(name) => name,
            Err(e) => {
                warn!("Username codec error:{}", e.to_string());
                return Ok(());
            }
        },
        None => {
            warn!("No username provided");
            return Ok(());
        }
    };

    // 将 用户的信息--sender stream 关联，开启异步task当broadcast时使用sender stream向每个用户client发送消息
    let mut peer = state.add_peer(addr, username, stream);
    let msg = Arc::new(Message::user_join(&peer.username));
    // 广播用户的到来
    state.broadcast(msg, addr).await?;

    while let Some(line) = peer.stream.next().await {
        match line {
            Ok(line) => {
                let msg = Arc::new(Message::new_text(&peer.username, line));
                state.broadcast(msg, addr).await?;
            }
            Err(e) => {
                warn!("Error reading line from stream: {}", e);
                break;
            }
        }
    }

    // 用户退出chat
    let msg = Arc::new(Message::user_left(&peer.username));
    state.broadcast(msg, addr).await?;
    info!("user left:{}", peer.username);
    state.peers.remove(&addr);

    Ok(())
}

impl ChatState {
    fn new() -> Self {
        Self {
            peers: DashMap::new(),
        }
    }
    async fn broadcast(&self, msg: Arc<Message>, addr: SocketAddr) -> Result<()> {
        for peer in self.peers.iter() {
            if peer.key() == &addr {
                continue;
            }
            if let Err(e) = peer.value().send(msg.clone()).await {
                warn!("Error sending message to {}: {}", peer.key(), e);
                self.peers.remove(peer.key());
            };
        }
        Ok(())
    }
    fn add_peer(
        &self,
        addr: SocketAddr,
        username: String,
        stream: Framed<TcpStream, LinesCodec>,
    ) -> Peer {
        let (tx, mut rx) = channel::<Arc<Message>>(MSG_SIZE);
        self.peers.insert(addr, tx);

        let (mut sender, receiver) = stream.split();
        tokio::spawn(async move {
            // rx extract数据之后要修改自身内部状态，所以是mutable
            while let Some(msg) = rx.recv().await {
                let content = format!("{}", msg);
                if let Err(e) = sender.send(content).await {
                    warn!("Error sending message to {}: {}", addr, e);
                }
            }
        });
        // receiver是SplitStream，可以异步地接收数据
        // 不需要mut是因为它是一个异步迭代器，不需要主动修改内部状态
        Peer {
            username,
            stream: receiver,
        }
    }
}
impl Message {
    fn user_join(username: &str) -> Self {
        let username = username.to_string();
        Message::Join(username)
    }
    fn user_left(username: &str) -> Self {
        let username = username.to_string();
        Message::Left(username)
    }
    fn new_text(username: &str, content: String) -> Self {
        let username = username.to_string();
        Message::Text {
            user: username,
            content,
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Join(name) => write!(f, "[{} JOINED]", name),
            Message::Left(name) => write!(f, "[{} LEFT]", name),
            Message::Text { user, content } => write!(f, "[{}]:{}", user, content),
        }
    }
}
