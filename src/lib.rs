use rand::Rng;
use serde;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Instant;
use zmq;

type NodeId = u32;

struct NetworkNode {
    sender: zmq::Socket,
    addr: &'static str,
    node_id: NodeId,
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

        NetworkNode {
            sender,
            addr,
            node_id: id,
        }
    }

    // Send a message to this node
    fn send_message(&self, from: NodeId, msg: Message) -> Result<(), zmq::Error> {
        let message = serde_json::to_string(&NetworkMessage { from, msg })
            .expect("Error serializing Message to send");
        self.sender.send(&message, 0)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
//* The first element of each option is the node it is from
enum Message {
    RequestVote(RequestVoteRPC),
    RequestVoteReply(bool),
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
struct NetworkMessage {
    from: NodeId,
    msg: Message,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
struct RequestVoteRPC {
    term: u32,
    candidate_id: NodeId,
    last_log_index: u32,
    //  TODO: Should be an Entry
    last_log_term: u32,
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
    network_nodes: Vec<Rc<NetworkNode>>,
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
    // timeout
    timeout: i32,
    timeout_instant: Instant,
    // Election State
    voters: Option<HashSet<NodeId>>,
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

    pub fn get_new_timeout_time(bottom: i32, top: i32) -> i32 {
        let mut thread_rng = rand::thread_rng();
        thread_rng.gen_range(bottom, top)
    }

    pub fn new(
        node_id: NodeId,
        node_addr: &'static str,
        other_nodes: Vec<(u32, &'static str)>,
    ) -> RaftNode {
        let ctx = zmq::Context::new();

        let reciever = RaftNode::setup_pull_socket(node_id, node_addr, &ctx);

        let network_nodes: Vec<Rc<NetworkNode>> = other_nodes
            .into_iter()
            .map(|(id, addr)| Rc::new(NetworkNode::new(id, addr, &ctx)))
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
            timeout: RaftNode::get_new_timeout_time(5000, 10000),
            timeout_instant: Instant::now(),
            voters: None,
        }
    }

    fn broadcast_message(&self, msg: &Message) -> Result<(), zmq::Error> {
        for node in self.network_nodes.iter() {
            node.send_message(self.node_id, msg.clone())?;
        }
        Ok(())
    }

    fn recieve_message(&mut self, timeout: i64) -> Result<Option<NetworkMessage>, zmq::Error> {
        let val = self.reciever.poll(zmq::POLLIN, timeout)?;
        if val == 1 {
            let msg = self
                .reciever
                .recv_string(0)
                .expect("Error converting recieved Message to string")
                .expect("Should be read because Poll returned");

            let msg: NetworkMessage =
                serde_json::from_str(&msg).expect("Error decoding Message in transit");
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    fn time_before_timeout(&self) -> i32 {
        self.timeout - self.timeout_instant.elapsed().as_millis() as i32
    }

    fn restart_election_timer(&mut self) {
        self.timeout_instant = Instant::now();
        self.timeout = RaftNode::get_new_timeout_time(5000, 10000);
    }

    fn register_new_vote(&mut self, node: NodeId) {
        println!("Node {}: Recieved a vote from node {}", self.node_id, node);
        let voters = self.voters.as_mut().unwrap();
        voters.insert(node);
    }

    fn start_candidacy(&mut self) -> Result<(), zmq::Error> {
        println!("Node {}: starting candidacy", self.node_id);
        self.current_term += 1;

        // Reset the voters hash set
        self.voters = Some(HashSet::new());

        // Vote for ourself
        self.register_new_vote(self.node_id);
        self.voted_for = Some(self.node_id);

        self.restart_election_timer();

        let message = Message::RequestVote(RequestVoteRPC {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_index: 0,
            last_log_term: 0,
        });
        self.broadcast_message(&message)?;
        Ok(())
    }

    fn handle_msg(&mut self, msg: &Message, node: &NetworkNode) -> Result<(), zmq::Error> {
        match msg {
            Message::RequestVoteReply(vote) => {
                if *vote {
                    self.register_new_vote(node.node_id);
                    if let Some(voters) = &self.voters {
                        if voters.len() >= (self.network_nodes.len() + 1) / 2 {
                            println!("Node {}: Got a majority vote.", self.node_id);
                        }
                    }
                }
            }
            Message::RequestVote(vote_request) => {
                // 1. Reply false if term < currentTerm (§5.1)
                if vote_request.term < self.current_term {
                    node.send_message(self.node_id, Message::RequestVoteReply(false))?;
                } else if self.voted_for.is_none() {
                    // TODO: step 2 conditions (page 4)
                    // 2. If votedFor is null or candidateId, and candidate’s log is at
                    //  least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
                    self.voted_for = Some(node.node_id);
                    node.send_message(self.node_id, Message::RequestVoteReply(true))?;
                } else {
                    node.send_message(self.node_id, Message::RequestVoteReply(false))?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn find_network_node(&self, node_id: NodeId) -> Option<Rc<NetworkNode>> {
        self.network_nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .cloned()
    }

    pub fn start(&mut self) -> Result<(), zmq::Error> {
        println!("Node {} starting...", self.node_id);
        // let node = &self.network_nodes[0];

        // let election_timeout = node.send_message(&Message::RequestVoteReply(self.node_id, true));

        loop {
            // Check if election has timed out
            if self.time_before_timeout() <= 0 {
                self.start_candidacy()?;
                continue;
            }

            let time_left: i64 = self.time_before_timeout() as i64;

            if let Some(msg) = self.recieve_message(time_left)? {
                println!("Node {}: Got msg {:?}", self.node_id, msg);
                if let Some(node) = self.find_network_node(msg.from) {
                    self.handle_msg(&msg.msg, &node)?;
                }
            }
        }
    }
}
