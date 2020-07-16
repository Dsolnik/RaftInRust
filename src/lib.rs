use std::fmt::Display;
use zmq;

pub enum NodeId {
    Id(String),
}

impl NodeId {
    fn new(name: &str) -> NodeId {
        NodeId::Id(String::from(name))
    }

    pub fn send<T>(&self, socket: &zmq::Socket, t: T)
    where
        T: Display,
    {
        let NodeId::Id(name) = self;
        let msg_to_send = format!("{}|{}", name, t);
        socket.send(&msg_to_send[..], 0).unwrap();
    }
}
