use super::types::*;
use super::utils;
use futures::channel::mpsc;
use futures::prelude::*;
use futures::SinkExt;
use futures::StreamExt;
use futures_timer::Delay;
use log::{info, trace, warn};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use futures_timer::Interval;

// TODO: sync the state for name change. 
// not important right now. But if we needed we'll do it in future.

/// NilaiHandler is responsible for handling the state. Basically, it is the heart of the nilai.
/// It receives message from channel and updates it's state. It also probes all the nodes in
/// the cluster for failure detection and send udp message via channel.
pub(crate) struct NilaiHandler {
    pub(crate) msg_rcv: mpsc::Receiver<UdpMessage>,
    pub(crate) msg_sender: mpsc::Sender<UdpMessage>,
    pub(crate) nodes: HashMap<String, Node>,
    pub(crate) seq_no: u32,
    pub(crate) node_ids: Vec<String>,
    pub(crate) ack_checker: HashMap<u32, String>,
    pub(crate) probe_id: usize,
    pub(crate) timeout_sender: mpsc::Sender<UdpMessage>,
    pub(crate) name: String,
    pub(crate) indirect_ack_checker: HashMap<u32, IndirectPing>,
    pub(crate) local_incarnation: u32,
    pub(crate) addr: String,
    pub(crate) indirect_checks: usize,
    pub(crate) gossip_nodes: usize,
    pub(crate) probe_timeout: Duration,
    pub(crate) probe_interval: Duration,
    pub(crate) suspicious_multiplier: u64,
}

impl NilaiHandler {
    /// init  initialize the current node and sends statesync to all the node.
    async fn init(&mut self, peers: Vec<SocketAddr>) {
        // initialize local state.
        self.nodes.insert(
            self.addr.clone(),
            Node {
                addr: self.addr.clone(),
                name: self.name.clone(),
                state: State::Alive,
                incarnation: 0,
            },
        );
        // send state sync message to all the peers.
        self.node_ids.push(self.addr.clone());
        for i in 0..peers.len() {
            let alive_msg = Alive {
                name: self.name.clone(),
                addr: self.addr.clone(),
                incarnation: 0,
            };
            self.send_msg(UdpMessage {
                peer: Some(peers[i]),
                msg: Message::StateSync(alive_msg),
            })
            .await;
        }

        // Initialize the probing.
        let mut sender = self.timeout_sender.clone();
        let probe_interval = self.probe_interval;
        runtime::spawn(async move{
            loop{
                match Delay::new(probe_interval).await{
                    Ok(_) => {
                        if let Err(e) = sender.send(UdpMessage{
                            peer: None,
                            msg: Message::Probe
                        }).await {
                            // need to understand how channel work in rust. will this block?
                            // or we only get here only if the channel is closed. 
                            warn!("I think channel is closed. so breaking the probe {}", e);
                            break;
                        }
                    },
                    Err(e) => {
                        warn!("something went wrong on probing future {}", e);
                    }
                }
            }
        });
    }

    pub(crate) async fn listen(&mut self, peers: Vec<SocketAddr>) {
        // initialize the handlers and send state sync to all the peers.
        self.init(peers).await;

        // listen for incoming messages.
        loop {
            match self.msg_rcv.next().await {
                Some(msg) => {
                    self.handle_msg(msg.peer, msg.msg).await;
                }
                // Channels are closed so simply quit.
                None => break,
            }
        }
    }

    async fn handle_msg(&mut self, from: Option<SocketAddr>, msg: Message) {
        match msg {
            Message::PingMsg(ping_msg) => {
                self.handle_ping(from.unwrap(), ping_msg).await;
            }
            Message::Probe => {
                self.handle_probe().await;
            }
            Message::PingTimeOut(seq_no) => {
                self.handle_timeout(seq_no).await;
            }
            Message::Ack(ack_res) => {
                self.handle_ack(ack_res.seq_no).await;
            }
            Message::IndirectPingTimeout(seq_no) => {
                self.handle_indirect_ping_timeout(seq_no).await;
            }
            Message::IndirectPingMsg(msg) => {
                self.handle_indirect_ping(msg).await;
            }
            Message::SuspectMsg(msg) => {
                self.handle_suspect(msg).await;
            }
            Message::SuspectMsgTimeout(msg) => {
                self.handle_suspect_timeout(msg).await;
            }
            Message::Alive(msg) => {
                self.handle_alive(msg).await;
            }
            Message::Dead(msg) => {
                self.handle_dead(msg).await;
            }
            Message::StateSync(msg) => {
                // It'll update the local state from the message. 
                self.handle_state_sync(msg).await;
            }
            Message::StateSyncRes(msg) => {
                self.handle_state_sync_res(msg);
            }
        }
    }

    fn handle_state_sync_res(&mut self, msg: Alive){
        if let Some(node) = self.nodes.get_mut(&msg.addr){
            if node.incarnation > msg.incarnation{
                // ignore if it is old message
                return
            }
            // update the local state
            node.incarnation = msg.incarnation;
            node.state = State::Alive;
            node.name = msg.name;
            return
        }
        // This is unknown node so updating my local state.
        self.node_ids.push(msg.addr.clone());
        self.nodes.insert(msg.addr.clone(), Node{
            addr: msg.addr,
            incarnation: msg.incarnation,
            name: msg.name,
            state: State::Alive,
        });
    }

    async fn handle_state_sync(&mut self, msg: Alive) {
        if let Some(node) = self.nodes.get(&msg.addr) {
           if node.incarnation != msg.incarnation{
            // so this node is restarted so we'll send dead message to the node. so that it can
            // update it's local state and gossip it around the cluster.
            
            if node.state == State::Dead || node.state == State::Suspect{
                // let the node that it is dead. so that it can refute and update it's state and 
                // gossip alive state.
                self.send_msg(UdpMessage{
                    peer: Some(msg.addr.parse().unwrap()),
                    msg: Message::Dead(Dead{
                        from: self.addr.clone(),
                        node: msg.addr.clone(),
                        incarnation: node.incarnation,
                    })
                }).await; 
            }

           }
        } else {
            self.node_ids.push(msg.addr.clone());
            self.nodes.insert(msg.addr.clone(), Node{
                addr: self.addr.clone(),
                state: State::Alive,
                incarnation: msg.incarnation,
                name: msg.name,
            });
        }
        // any ways we'll send our local state to the peer to update itself.
        self.send_msg(UdpMessage{
            peer: Some(msg.addr.parse().unwrap()),
            msg: Message::StateSyncRes(Alive{
                addr: self.addr.clone(),
                incarnation: self.local_incarnation,
                name: self.name.clone(),
            })
        }).await;
    }

    async fn handle_dead(&mut self, msg: Dead) {
        let dead_node = self.nodes.get_mut(&msg.node);
        match dead_node {
            Some(dead_node) => {
                if dead_node.incarnation > msg.incarnation {
                    // old incarnation num.clone()ber. So, ignore it.
                    return;
                }

                if msg.node == self.addr {
                    // If this is about us then, just refute.
                    self.refute(msg.incarnation).await;
                    return;
                }
                // if the message is new then update and gossip.
                if dead_node.state != State::Dead && dead_node.incarnation != msg.incarnation {
                    dead_node.state = State::Dead;
                    dead_node.incarnation = msg.incarnation;
                    self.gossip(Message::Dead(msg), self.gossip_nodes).await;
                }
            }
            None => {
                // some random node. So, ignoring it.
                return;
            }
        }
    }

    async fn handle_alive(&mut self, msg: Alive) {
        info!("got alive msg from: {}", msg.name);
        let local_state = self.nodes.get_mut(&msg.addr);
        match local_state {
            Some(local_state) => {
                // I've decided to bootstrap with state sync.
                // special case if node is bootstrapping or failed and restarted
                // from a cluster. If the node incarnation number is zero and it is suspect
                // or failed. we'll send dead message to the particular node so that it can
                // get the state.
                // if msg.incarnation == 0
                //     && (local_state.state == State::Dead || local_state.state == State::Suspect)
                // {
                //     let peer = local_state.addr.parse().unwrap();
                //     let incarnation = local_state.incarnation;
                //     let from = self.addr.clone();
                //     let node_id = local_state.addr.to_string();
                //     self.send_msg(UdpMessage {
                //         peer: Some(peer),
                //         msg: Message::Dead(Dead {
                //             from: from,
                //             incarnation: incarnation,
                //             node: node_id,
                //         }),
                //     })
                //     .await;
                //     return;
                // }


                // we'll just ignore if the state matches with the local state
                // so we don't do any thing. So that gossip will get avoided.
                // since we're going any queueing. This will work for us time
                // being.
                if local_state.incarnation >= msg.incarnation {
                    // This is some old message.
                    // May be node crashed and restarted. So send a dead
                    return;
                }

                // suspect may turn into alive on this update.
                local_state.state = State::Alive;
                // let's update the local state and gossip it.
                local_state.incarnation = msg.incarnation;
                self.gossip(Message::Alive(msg), self.gossip_nodes).await;
            }
            None => {
                // This is some new node. So, just insert the
                // node into your list and gossip.
                info!("some new node is joining the cluster yay!!");
                self.nodes.insert(
                    msg.addr.to_string(),
                    Node {
                        addr: msg.addr.clone(),
                        name: msg.name.clone(),
                        state: State::Alive,
                        incarnation: msg.incarnation,
                    },
                );
                self.node_ids.push(msg.addr.clone());
                self.gossip(Message::Alive(msg), self.gossip_nodes).await;
                return;
            }
        }
    }

    async fn handle_suspect_timeout(&mut self, msg: SuspectTimeout) {
        let suspect_node = self.nodes.get_mut(&msg.node_id);
        match suspect_node {
            Some(suspect_node) => {
                if suspect_node.incarnation > msg.incarnation {
                    // okay we got refute message. No need to progress further.
                    return;
                }
                // okay we're confirming as dead.
                suspect_node.state = State::Dead;
                // We find this node as dead so gossip it.
                let dead_msg = Dead {
                    from: self.name.to_string(),
                    node: suspect_node.addr.to_string(),
                    incarnation: msg.incarnation,
                };
                self.gossip(Message::Dead(dead_msg), self.gossip_nodes)
                    .await;
            }
            None => return,
        }
    }

    async fn handle_suspect(&mut self, msg: Suspect) {
        let suspect_node = self.nodes.get(&msg.node);
        if !suspect_node.is_some() {
            // unknown node. So, let's not talk about it.
            return;
        }
        let suspect_node = suspect_node.unwrap();
        match suspect_node.state {
            State::Alive => {
                // we'll send suspect and do the the dead timeout.
            }
            State::Suspect => {
                // already suspected no need to suspect more. when suspect timeout
                // happens we'll gossip dead message. In memeberlist they have timer
                // check for faster gossip. Hope, time will let us to do those features.
                // It is good to have not for now. But, we'll do it.
                return;
            }
            State::Dead => {
                // already dead so no need to suspect.
                return;
            }
        }
        if msg.incarnation < suspect_node.incarnation {
            // old suspect message so just ignore it.
            return;
        }
        if msg.node == self.name {
            // It's me who is been suspected.
            // so refute it.
            self.refute(msg.incarnation).await;
            return;
        }
        // suspect node.
        self.suspect_node(&msg.node).await;
    }

    async fn refute(&mut self, mut incarnation: u32) {
        if self.local_incarnation <= incarnation {
            // advancing incarnation number.
            incarnation = incarnation + 1;
            self.local_incarnation = incarnation;
        }
        let alive_msg = Alive {
            name: self.name.to_string(),
            addr: self.addr.parse().unwrap(),
            incarnation: self.local_incarnation,
        };
        self.gossip(Message::Alive(alive_msg), self.gossip_nodes)
            .await;
    }

    async fn gossip(&mut self, msg: Message, k: usize) {
        let k_nodes = utils::k_random_nodes(&self.nodes, &self.node_ids, k, |n: &Node| {
            match n.state {
                State::Dead => true,
                // we'll gossip to the both suspect and alive nodes.
                _ => false,
            }
        });
        let mut msgs = Vec::new();
        for n in k_nodes {
            // gossip should be a separate queue and have to
            // compound batching based on udp MTU.
            msgs.push(UdpMessage {
                msg: msg.clone(),
                peer: Some(n.addr.parse().unwrap()),
            });
        }
        for msg in msgs {
            self.send_msg(msg).await;
        }
    }

    async fn handle_indirect_ping(&mut self, msg: IndirectPing) {
        self.send_ping(self.seq_no, &msg.to).await;
        self.indirect_ack_checker.insert(self.seq_no, msg);
        self.increment_seq_no();
        // do we need to timeout here to delete the ack checker?
    }

    async fn handle_indirect_ping_timeout(&mut self, seq_no: u32) {
        let s_n_id = self.ack_checker.get(&seq_no);
        if !s_n_id.is_some() {
            // we received ack so simply return.
            return;
        }
        let s_n_id = s_n_id.unwrap().to_string();
        info!("got indirect ping timeout {}", s_n_id);

        self.suspect_node(&s_n_id).await;
    }

    async fn suspect_node(&mut self, suspect_node_id: &String) {
        // choose k random node and gossip that suspect. and also do
        // dead timer.
        let suspect_node = self.nodes.get_mut(suspect_node_id);
        if !suspect_node.is_some() {
            // node may be left.
            // we need some method for leaving the cluster.
            return;
        }
        let suspect_node = suspect_node.unwrap();
        suspect_node.state = State::Suspect;
        let suspect_msg = Suspect {
            from: self.addr.to_string(),
            node: suspect_node.addr.to_string(),
            incarnation: suspect_node.incarnation,
        };
        let suspect_incarnation = suspect_node.incarnation;
        let suspect_node_addr = suspect_node.addr.to_string();
        // here we'll send suspect message to the node it self
        // so that it can refute.
        self.send_msg(UdpMessage {
            peer: Some(suspect_msg.node.parse().unwrap()),
            msg: Message::SuspectMsg(suspect_msg.clone()),
        })
        .await;
        // Now gossip the message across the cluster.
        self.gossip(Message::SuspectMsg(suspect_msg), self.gossip_nodes)
            .await;
        // If we didn't get any alive message with higher incarnation number
        // we'll consider this node as dead.
        let suspect_timeout = Message::SuspectMsgTimeout(SuspectTimeout {
            node_id: suspect_node_addr,
            incarnation: suspect_incarnation,
        });
        self.spawn_timeout_msg(UdpMessage {
            msg: suspect_timeout,
            peer: None,
        }, Duration::from_millis(self.probe_timeout.as_secs() * self.suspicious_multiplier))
        .await;
    }

    async fn handle_ack(&mut self, seq_no: u32) {
        // first you check wether it is indirect ping or our ping.
        // if it is indirect ping send ack res to from node.
        // other wise clear ourself.
        // removing the checker so that when probe timeout come.
        // we know that ack have come so no need to send indirect ping.
        match self.indirect_ack_checker.get(&seq_no) {
            Some(indirect_msg) => {
                let from = indirect_msg.from.parse().unwrap();
                self.send_ack(from, indirect_msg.seq_no)
                    .await;
            }
            None => {
                // here you don't care. You just try to remove
                // it.
                self.ack_checker.remove(&seq_no);
            }
        }
    }
    async fn handle_ping(&mut self, from: SocketAddr, msg: Ping) {
        self.send_ack(from, msg.seq_no).await;
    }

    async fn send_ack(&mut self, from: SocketAddr, seq_no: u32) {
        let res = AckRes { seq_no: seq_no };
        info!(
            "sending ack response to the peer {} with seqno {}",
            from, res.seq_no
        );
        self.send_msg(UdpMessage {
            msg: Message::Ack(res),
            peer: Some(from),
        })
        .await;
    }

    async fn send_msg(&mut self, msg: UdpMessage) {
        match self.msg_sender.send(msg).await {
            Err(err) => warn!("unable to push to channel {}", err),
            _ => info!("pushed to channel successfully"),
        }
    }

    async fn handle_probe(&mut self) {
        let to_be_probed: String;
        loop {
            // we don't probe if there is no nodes to probe.
            if self.nodes.len() == 1 {
                return;
            }

            if self.probe_id >= self.nodes.len() {
                self.probe_id = 0;
                // sanity check since we're unwraping.
                if self.nodes.len() == 1 {
                    return;
                }
            }
            let node_id = self.node_ids.get(self.probe_id).unwrap();
            // sanity check
            match node_id.cmp(&self.addr) {
                Ordering::Equal => {
                    // This is us so no need to probe.
                    self.increment_probe_id();
                    continue;
                }
                _ => {
                    // this is someone else so move forward.
                }
            }
            let node = self.nodes.get(node_id);
            if node.is_some() {
                let node = node.unwrap();
                match node.state {
                    State::Dead => {
                        // no need to send message.
                        self.increment_probe_id();
                        continue;
                    }
                    _ => {
                        // send ping message.
                        to_be_probed = node_id.to_string();
                        break;
                    }
                }
            }
            self.increment_probe_id();
        }

        let seq_no = self.seq_no.clone();
        self.ack_checker.insert(seq_no, to_be_probed.to_string());
        // send ping message and spawn the ping timeout, so that if there is no ack,
        // we can mark this node as suspect.
        self.send_ping(seq_no, &to_be_probed).await;
        let msg = UdpMessage {
            peer: None,
            msg: Message::PingTimeOut(seq_no),
        };
        self.spawn_timeout_msg(msg, self.probe_timeout).await;
        self.increment_seq_no();
    }

    async fn handle_timeout(&mut self, seq_no: u32) {
        let node_id = self.ack_checker.get(&seq_no);
        match node_id {
            Some(id) => {
                let indirect_ping = IndirectPing {
                    seq_no: seq_no,
                    to: id.to_string(),
                    from: self.addr.to_string(),
                };
                // TODO: gossip may send indirect ping
                // to target node.so use k_node here.
                self.gossip(
                    Message::IndirectPingMsg(indirect_ping),
                    self.indirect_checks,
                )
                .await;
            }
            None => {
                // This means, we got ping no need to do any thing.
                return;
            }
        }
        let msg = UdpMessage {
            msg: Message::IndirectPingTimeout(seq_no),
            peer: None,
        };
        self.spawn_timeout_msg(msg, self.probe_timeout).await;
    }

    async fn send_ping(&mut self, seq_no: u32, node_id: &String) {
        let ping = Ping {
            seq_no: seq_no,
            node: node_id.to_string(),
        };
        let to_addr: SocketAddr = node_id.parse().unwrap();
        info!("sending ping to {}", node_id);
        self.send_msg(UdpMessage {
            peer: Some(to_addr),
            msg: Message::PingMsg(ping),
        })
        .await;
    }

    async fn spawn_timeout_msg(&mut self, msg: UdpMessage, duration: Duration) {
        let mut sender = self.timeout_sender.clone();
        runtime::spawn(async move {
            Delay::new(duration).await;
            sender.send(msg).await;
        });
    }

    fn increment_probe_id(&mut self) {
        self.probe_id = self.probe_id + 1;
    }

    fn increment_seq_no(&mut self) {
        self.seq_no = self.seq_no + 1;
    }
}
