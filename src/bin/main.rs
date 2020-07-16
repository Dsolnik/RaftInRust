use std::thread;
use std::time::Duration;
use zmq;

struct NodeConfig {
    id: &'static str,
    addr: &'static str,
}

const nodes: [NodeConfig; 4] = [
    NodeConfig { id: "1", addr: "" },
    NodeConfig { id: "2", addr: "" },
    NodeConfig { id: "3", addr: "" },
    NodeConfig { id: "4", addr: "" },
];

struct Network {
    current_node: NodeConfig,
    other_nodes: std::vec::Vec<NodeConfig>,
}

impl Network {
    fn send<T>(&self, to: &str, t: T) -> Option<()> {
        None
    }

    fn recv<T>(&self, from: &str, t: T) -> Option<()> {
        None
    }
}

fn raft_node(id: &str) {
    let ctx = zmq::Context::new();
    let requester = ctx.socket(zmq::REQ).unwrap();

    assert!(requester.connect("tcp://localhost:5555").is_ok());
}

fn main() {
    let names = vec!["1", "2"];
    let mut threads = vec![];

    for name in names {
        let handle = thread::spawn(move || raft_node(name));
        threads.push(handle);
    }

    for handle in threads {
        handle.join().unwrap();
    }
}
