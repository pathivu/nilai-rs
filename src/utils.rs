/*
 * Copyright 2019 balajijinnah and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
