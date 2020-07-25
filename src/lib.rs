use serde;
use serde::{Deserialize, Serialize};
use serde_json;
use zmq;

type NodeId = u32;

struct NetworkNode {
    sender: zmq::Socket,
    addr: &'static str,
    id: NodeId,
}

impl NetworkNode {
    // Create an interface to send messages to the node
    pub fn new(id: u32, addr: &'static str, ctx: &zmq::Context) -> NetworkNode {
        let sender = ctx
            .socket(zmq::PUSH)
            .expect(&format!("Node {}: Error creating PUSH socket", id));

        sender
            .connect(addr)
            .expect(&format!("Node {}: Error connecting to server {}", id, addr));

        NetworkNode { sender, addr, id }
    }

    // Send a message to this node
    fn send_message(&self, message: &Message) {
        let message = serde_json::to_string(message).expect("Error serializing Message to send");

        self.sender
            .send(&message, 0)
            .expect("Error sending Message with zmq");
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteRPC {
    term: u32,
    candidate_id: String,
    last_log_index: u32,
    last_log_term: u32,
}

#[derive(Serialize, Deserialize, Debug)]
//* The first element of each option is the node it is from
enum Message {
    RequestVote(NodeId, RequestVoteRPC),
    RequestVoteReply(NodeId, bool),
}

#[derive(Serialize, Deserialize, Debug)]
enum Operation {
    Get,
    Set,
}

#[derive(Serialize, Deserialize, Debug)]
struct Entry {
    op: Operation,
    key: String,
    value: String,
}

pub struct RaftNode {
    network_nodes: Vec<NetworkNode>,
    reciever: zmq::Socket,
    node_id: NodeId,
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
    fn setup_pull_socket(
        node_id: NodeId,
        node_addr: &'static str,
        ctx: &zmq::Context,
    ) -> zmq::Socket {
        let reciever = ctx.socket(zmq::PULL).expect(&format!(
            "Node {}: Error creating PULL Socket at {}",
            node_id, node_addr
        ));

        reciever.bind(node_addr).expect(&format!(
            "Node {}: Error binding PULL Socket at {}",
            node_id, node_addr
        ));
        reciever
    }

    pub fn new(
        node_id: NodeId,
        node_addr: &'static str,
        other_nodes: Vec<(u32, &'static str)>,
    ) -> RaftNode {
        let ctx = zmq::Context::new();

        let reciever = RaftNode::setup_pull_socket(node_id, node_addr, &ctx);

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

    fn recieve_message(&mut self) -> Option<Message> {
        if let Ok(msg) = self
            .reciever
            .recv_string(0)
            .expect("Error converting recieved Message to string")
        {
            let msg: Message =
                serde_json::from_str(&msg).expect("Error decoding Message in transit");
            Some(msg)
        } else {
            None
        }
    }

    pub fn start(&mut self) {
        let node = &self.network_nodes[0];

        node.send_message(&Message::RequestVoteReply(self.node_id, true));

        loop {
            if let Some(msg) = self.recieve_message() {
                println!("Node {}: Got msg {:?}", self.node_id, msg);
            }
        }
    }
}
