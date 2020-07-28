use std::thread;
use Raft::RaftNode;

fn main() {
    let node_configs = vec![
        (1, "tcp://127.0.0.1:5551"),
        (2, "tcp://127.0.0.1:5552"),
        (3, "tcp://127.0.0.1:5553"),
    ];

    let mut threads = vec![];
    for node_config in node_configs.iter() {
        let (node_id, node_addr) = node_config.clone();
        let other_nodes: Vec<(u32, &str)> = node_configs
            .clone()
            .into_iter()
            .filter(|&config| config != (node_id, node_addr))
            .collect();

        let handle = thread::spawn(move || {
            let mut raft_node = RaftNode::new(node_id, node_addr, other_nodes);
            raft_node.start().expect("Error running the Raft Node");
        });
        threads.push(handle);
    }

    threads
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}
