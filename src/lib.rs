use serde;
use serde::{Deserialize, Serialize};
use serde_json::{Result as JsonResult, Value};
use zmq;

struct NetworkNode {
    sender: zmq::Socket,
    addr: &'static str,
    id: u32,
}

impl NetworkNode {
    pub fn new(id: u32, addr: &'static str, ctx: &zmq::Context) -> NetworkNode {
        let sender = ctx
            .socket(zmq::PUSH)
            .expect(&format!("Node {}: Error creating PUSH socket", id));

        sender
            .connect(addr)
            .expect(&format!("Node {}: Error connecting to server {}", id, addr));

        NetworkNode { sender, addr, id }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteRPC {
    term: u32,
    candidate_id: String,
    last_log_index: i32,
    last_log_term: i32,
}

#[derive(Serialize, Deserialize, Debug)]
enum Message {
    RequestVote(RequestVoteRPC),
    RequestVoteReply(bool),
}

enum Operation {
    Get,
    Set,
}

struct Entry {
    op: Operation,
    key: String,
    value: String,
}

pub struct RaftNode {
    network_nodes: Vec<NetworkNode>,
    reciever: zmq::Socket,
    node_id: u32,
    // Persistent State
    current_term: u32,
    voted_for: Option<u32>,
    log: Vec<Entry>,
    // Volatile State
    commit_index: u32,
    last_applied: u32,
    // Leader State
    next_index: Vec<u32>,
    match_index: Vec<u32>,
}

impl RaftNode {
    pub fn new(
        node_id: u32,
        node_addr: &'static str,
        other_nodes: Vec<(u32, &'static str)>,
    ) -> RaftNode {
        let ctx = zmq::Context::new();

        let reciever = ctx.socket(zmq::PULL).expect(&format!(
            "Node {}: Error creating PULL Socket at {}",
            node_id, node_addr
        ));

        reciever.bind(node_addr).expect(&format!(
            "Node {}: Error binding PULL Socket at {}",
            node_id, node_addr
        ));

        let network_nodes: Vec<NetworkNode> = other_nodes
            .into_iter()
            .map(|(id, addr)| NetworkNode::new(id, addr, &ctx))
            .collect();

        RaftNode {
            network_nodes,
            reciever,
            node_id,
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: vec![],
            match_index: vec![],
        }
    }

    pub fn start(&mut self) -> zmq::Result<()> {
        let node = &self.network_nodes[0];

        let message = Message::RequestVoteReply(true);
        let message = serde_json::to_string(&message).expect("Error serializing Message to send");

        node.sender.send(&message, 0)?;
        loop {
            if let Ok(msg) = self.reciever.recv_string(0)? {
                let msg: Message =
                    serde_json::from_str(&msg).expect("Error decoding Message in transit");
                println!("Node {}: Got msg {:#?}", self.node_id, msg);
            }
        }
    }
}
