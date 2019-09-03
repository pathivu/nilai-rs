use super::types::Node;
use rand::Rng;
use std::collections::HashMap;
/// k_random_nodes returns random nodes from the given nodes list based on the filtering closure.
pub(crate) fn k_random_nodes<'a, F>(
    nodes: &'a HashMap<String, Node>,
    nodes_ids: &Vec<String>,
    n: usize,
    filter: F,
) -> Vec<&'a Node>
where
    F: Fn(&Node) -> bool,
{
    let mut k_nodes = Vec::new();
    let mut rng = rand::thread_rng();
    let mut i = 0;
    while i < nodes_ids.len() && k_nodes.len() < n {
        let idx = rng.gen_range(0, nodes_ids.len());
        let node_id = nodes_ids.get(idx).unwrap();
        let node = nodes.get(node_id).unwrap();
        // we're unwraping here, I'm confident enough that it won't panic. If it's panic
        // it is good to know, that there is a bug.
        if filter(&node) {
            i = i + 1;
            continue;
        }
        i = i + 1;
        k_nodes.push(node);
    }
    return k_nodes;
}
