use super::delegate;
use super::types::*;
use super::utils;
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::prelude::*;
use futures_timer::Delay;
use log::{info, warn};
use runtime::native::Native;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

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
    pub(crate) alive_delegate: Option<delegate::Handler>,
    pub(crate) dead_delegate: Option<delegate::Handler>,
    pub(crate) closer: oneshot::Receiver<i32>,
}

impl NilaiHandler {
    /// init  initialize the current node and sends statesync to all the node.
    async fn init(&mut self, peers: Vec<SocketAddr>, mut closer: oneshot::Receiver<i32>) {
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
        tokio::spawn(async move {
            loop {
                if let Ok(opt) = closer.try_recv() {
                    if let Some(_) = opt {
                        break;
                    }
                }
                match Delay::new(probe_interval).await {
                    Ok(_) => {
                        if let Err(e) = sender
                            .send(UdpMessage {
                                peer: None,
                                msg: Message::Probe,
                            })
                            .await
                        {
                            // need to understand how channel work in rust. will this block?
                            // or we only get here only if the channel is closed.
                            warn!("I think channel is closed. so breaking the probe {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("something went wrong on probing future {}", e);
                    }
                }
            }
        });
    }

    pub(crate) async fn listen(&mut self, peers: Vec<SocketAddr>) {
        println!("nilai started listening");
        info!("yo");
        // initialize the handlers and send state sync to all the peers.
        let (sender, closer) = oneshot::channel();
        self.init(peers, closer).await;

        // listen for incoming messages.
        loop {
            if let Ok(opt) = self.closer.try_recv() {
                if let Some(_) = opt {
                    if let Err(_) = sender.send(1) {
                        warn!("error while closing probe handler");
                    }
                    break;
                }
            }

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
                info!("got ping message from {}", ping_msg.node);
                self.handle_ping(from.unwrap(), ping_msg).await;
            }
            Message::Probe => {
                info!("got probe interval");
                self.handle_probe().await;
            }
            Message::PingTimeOut(seq_no) => {
                info!("got ping timeout");
                self.handle_timeout(seq_no).await;
            }
            Message::Ack(ack_res) => {
                info!("got ack res for the seq_no {}", ack_res.seq_no);
                self.handle_ack(ack_res.seq_no).await;
            }
            Message::IndirectPingTimeout(seq_no) => {
                info!("got indirect ping timeout {}", seq_no);
                self.handle_indirect_ping_timeout(seq_no).await;
            }
            Message::IndirectPingMsg(msg) => {
                info!("got indirect ping message");
                self.handle_indirect_ping(msg).await;
            }
            Message::SuspectMsg(msg) => {
                info!(
                    "got suspect message for the node {} from {}",
                    msg.node, msg.from
                );
                self.handle_suspect(msg).await;
            }
            Message::SuspectMsgTimeout(msg) => {
                info!("got suspicious timeout for the node {}", msg.node_id);
                self.handle_suspect_timeout(msg).await;
            }
            Message::Alive(msg) => {
                info!("got alive message from node {} {}", msg.name, msg.addr);
                self.handle_alive(msg).await;
            }
            Message::Dead(msg) => {
                info!("got dead message for the node {}", msg.node);
                self.handle_dead(msg).await;
            }
            Message::StateSync(msg) => {
                info!("got state sync request from {} {}", msg.name, msg.addr);
                // It'll update the local state from the message.
                self.handle_state_sync(msg).await;
            }
            Message::StateSyncRes(msg) => {
                info!("got state sync response from {}", msg.addr);
                self.handle_state_sync_res(msg);
            }
        }
    }

    async fn handle_state_sync_res(&mut self, msg: Alive) {
        if let Some(node) = self.nodes.get_mut(&msg.addr) {
            if node.incarnation > msg.incarnation {
                // ignore if it is old message
                return;
            }
            // update the local state
            node.incarnation = msg.incarnation;
            node.state = State::Alive;
            node.name = msg.name;
            return;
        }
        // just update the local state and gossip.
        // actually no need to gossip here.
        // but still okay.
        self.handle_alive(msg).await;
    }

    async fn handle_state_sync(&mut self, msg: Alive) {
        let peer_addr: String = msg.addr.parse().unwrap();
        // any ways we'll send our local state to the peer to update itself.
        self.send_msg(UdpMessage {
            peer: Some(msg.addr.parse().unwrap()),
            msg: Message::StateSyncRes(Alive {
                addr: self.addr.clone(),
                incarnation: self.local_incarnation,
                name: self.name.clone(),
            }),
        })
        .await;
        // gossip this state sync to the rest of the cluster to have faster
        // convergence.
        self.gossip(Message::StateSync(msg.clone()), self.gossip_nodes)
            .await;

        if let Some(node) = self.nodes.get(&peer_addr) {
            if node.state == State::Dead || node.state == State::Suspect {
                // let the node node that it is dead. so that it can refute and update it's
                // state and gossip alive state.
                self.send_msg(UdpMessage {
                    peer: Some(msg.addr.parse().unwrap()),
                    msg: Message::Dead(Dead {
                        from: self.addr.clone(),
                        node: msg.addr.clone(),
                        incarnation: node.incarnation,
                    }),
                })
                .await;
                return;
            }
        }
        // It's not a dead restart so handle this alive
        self.handle_alive(msg).await;
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
                    // send notification to delegate if any.
                    if let Some(delegate) = &self.dead_delegate {
                        delegate(Node {
                            addr: dead_node.addr.to_string(),
                            name: dead_node.name.to_string(),
                            incarnation: dead_node.incarnation,
                            state: State::Dead,
                        });
                    }
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
                // send notification to delegate if any. cuz it's
                // restarted
                if let Some(delegate) = &self.alive_delegate {
                    delegate(Node {
                        addr: msg.addr.clone(),
                        name: msg.name.clone(),
                        incarnation: msg.incarnation,
                        state: State::Alive,
                    });
                }
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
                // send notification if any delegate.
                if let Some(delegate) = &self.alive_delegate {
                    delegate(Node {
                        addr: msg.addr.clone(),
                        name: msg.name.clone(),
                        incarnation: msg.incarnation,
                        state: State::Alive,
                    });
                }
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
                // send notification to delegate if any.
                if let Some(delegate) = &self.dead_delegate {
                    delegate(Node {
                        addr: suspect_node.addr.to_string(),
                        name: suspect_node.name.to_string(),
                        incarnation: suspect_node.incarnation,
                        state: State::Dead,
                    });
                }
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
        if msg.node == self.addr {
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
        println!("indirect ping timeout node {}", s_n_id);
        println!("nodes {:?}", self.nodes);
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
        println!("suspect node {:?}", suspect_node);
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
        self.spawn_timeout_msg(
            UdpMessage {
                msg: suspect_timeout,
                peer: None,
            },
            Duration::from_millis(self.probe_timeout.as_secs() * self.suspicious_multiplier),
        )
        .await;
    }

    async fn handle_ack(&mut self, seq_no: u32) {
        // first you check wether it is indirect ping or our ping.
        // if it is indirect ping send ack res to from node.
        // other wise clear ouetrself.
        // removing the checker so that when probe timeout come.
        // we know that ack have come so no need to send indirect ping.
        match self.indirect_ack_checker.get(&seq_no) {
            Some(indirect_msg) => {
                let from = indirect_msg.from.parse().unwrap();
                self.send_ack(from, indirect_msg.seq_no).await;
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
                return;
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
        tokio::spawn(async move {
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

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use futures::executor::block_on;
    use std::sync::{Once, ONCE_INIT};
    use std::thread;
    use tokio::runtime::Runtime;

    fn get_mock_nilai(
        recv: mpsc::Receiver<UdpMessage>,
        send_udp: mpsc::Sender<UdpMessage>,
        send: mpsc::Sender<UdpMessage>,
    ) -> NilaiHandler {
        let (handler_closer_signal, closer) = oneshot::channel();
        let mut nl = NilaiHandler {
            msg_rcv: recv,
            msg_sender: send_udp,
            nodes: HashMap::default(),
            seq_no: 0,
            node_ids: Vec::default(),
            ack_checker: HashMap::default(),
            probe_id: 0,
            timeout_sender: send,
            name: String::from("yo"),
            indirect_ack_checker: HashMap::default(),
            local_incarnation: 0,
            addr: String::from("127.0.0.0:8000"),
            alive_delegate: None,
            dead_delegate: None,
            indirect_checks: 0,
            gossip_nodes: 0,
            probe_timeout: Duration::from_millis(200),
            probe_interval: Duration::from_millis(200),
            suspicious_multiplier: 2,
            closer: closer,
        };
        nl.nodes.insert(
            String::from("127.0.0.0:8000"),
            Node {
                addr: String::from("127.0.0.0:8000"),
                name: String::from("yo"),
                state: State::Alive,
                incarnation: 0,
            },
        );
        nl.node_ids.push(String::from("127.0.0.0:8000"));
        return nl;
    }
    #[test]
    fn test_handle_alive() {
        let (mut send, recv) = mpsc::channel(100);
        let (send_udp, recv_udp) = mpsc::channel(100);
        let mut nl = get_mock_nilai(recv, send_udp, send.clone());
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 2,
            })
            .await;
            // self and alive node. So, it is two.
            assert_eq!(nl.nodes.len(), 2);

            // checking suspect refute for the suspect.
            let node_2 = nl.nodes.get_mut(&String::from("127.1.1.1:8000")).unwrap();
            node_2.state = State::Suspect;
            //  sending alive with less incarnation number so that
            // it won't update the state
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 1,
            })
            .await;
            let node_2 = nl.nodes.get_mut(&String::from("127.1.1.1:8000")).unwrap();
            assert_eq!(node_2.state, State::Suspect);

            // sending with higher incarnation number so that it'll mark
            // the state as alive
            nl.gossip_nodes = 1;
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 3,
            })
            .await;
            let node_2 = nl.nodes.get_mut(&String::from("127.1.1.1:8000")).unwrap();
            assert_eq!(node_2.state, State::Alive);
        });
    }

    #[test]
    fn test_handle_dead() {
        let (mut send, recv) = mpsc::channel(100);
        let (send_udp, mut recv_udp) = mpsc::channel(100);
        let mut nl = get_mock_nilai(recv, send_udp, send.clone());
        nl.gossip_nodes = 2;
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 2,
            })
            .await;
            assert_eq!(nl.nodes.len(), 2);

            // dead with lower incarnation number so that
            // it won't considered as dead.
            nl.handle_dead(Dead {
                from: String::from("127.0.0.0:8000"),
                node: String::from("127.1.1.1:8000"),
                incarnation: 1,
            })
            .await;
            let node_2 = nl.nodes.get_mut(&String::from("127.1.1.1:8000")).unwrap();
            assert_eq!(node_2.state, State::Alive);

            // dead with high incarnation number so that
            // it will considered as dead.
            nl.handle_dead(Dead {
                from: String::from("127.0.0.0:8000"),
                node: String::from("127.1.1.1:8000"),
                incarnation: 3,
            })
            .await;
            let node_2 = nl.nodes.get_mut(&String::from("127.1.1.1:8000")).unwrap();
            assert_eq!(node_2.state, State::Dead);
            // node is restarted so it is sending alive
            // with 0 incarnation now Nilai should send dead message.
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 0,
            })
            .await;
        });
    }
    #[test]
    fn test_handle_timeout() {
        let (mut send, recv) = mpsc::channel(100);
        let (send_udp, mut recv_udp) = mpsc::channel(100);
        let mut nl = get_mock_nilai(recv, send_udp, send.clone());
        nl.indirect_checks = 2;
        nl.gossip_nodes = 3;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 2,
            })
            .await;
            assert_eq!(nl.nodes.len(), 2);
            nl.handle_probe().await;
            // now probe should send ping message
            let _ = recv_udp.try_next().unwrap().unwrap();
            nl.handle_ack(nl.seq_no - 1).await;
            // now there should not be ack checker
            assert_eq!(nl.ack_checker.len(), 0);
            assert_eq!(
                nl.nodes.get(&String::from("127.1.1.1:8000")).unwrap().state,
                State::Alive
            );
            // add one more node.
            nl.handle_alive(Alive {
                name: String::from("node 3"),
                addr: String::from("127.1.1.3:8000"),
                incarnation: 2,
            })
            .await;
            nl.handle_probe().await;
            // now probe should send ping message
            let _ = recv_udp.try_next().unwrap().unwrap();
            nl.handle_timeout(nl.seq_no - 1).await;
            assert_eq!(nl.ack_checker.len(), 1);
            // need some smart way to test it.
            // rustaceans please help me here.
            let mut indirect_ping_exist = false;
            let mut indirect_ping_node = String::from("");
            loop {
                match recv_udp.try_next() {
                    Ok(msg) => match msg {
                        Some(udp_msg) => match udp_msg.msg {
                            Message::IndirectPingMsg(msg) => {
                                indirect_ping_node = msg.to;
                                indirect_ping_exist = true;
                            }
                            _ => {
                                continue;
                            }
                        },
                        None => {
                            break;
                        }
                    },
                    _ => {
                        break;
                    }
                }
            }
            assert_eq!(indirect_ping_exist, true);
            assert_eq!(nl.ack_checker.len(), 1);
            // so there no indirect ack and we'll do indirect ping timeout.
            nl.handle_indirect_ping_timeout(nl.seq_no - 1).await;
            assert_eq!(
                nl.nodes.get(&indirect_ping_node).unwrap().state,
                State::Suspect
            );
            nl.handle_suspect_timeout(SuspectTimeout {
                incarnation: 0,
                node_id: indirect_ping_node.clone(),
            })
            .await;
            // lesser incarnation number means it'll ignore.
            assert_eq!(
                nl.nodes.get(&indirect_ping_node).unwrap().state,
                State::Suspect
            );

            nl.handle_suspect_timeout(SuspectTimeout {
                incarnation: 2,
                node_id: indirect_ping_node.clone(),
            })
            .await;
            // not refuted, so it'll mark the node as dead.
            assert_eq!(
                nl.nodes.get(&indirect_ping_node).unwrap().state,
                State::Dead
            );
        });
    }
    #[test]
    fn test_suspect_msg() {
        let (mut send, recv) = mpsc::channel(100);
        let (send_udp, mut recv_udp) = mpsc::channel(100);
        let mut nl = get_mock_nilai(recv, send_udp, send.clone());
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 2,
            })
            .await;
            assert_eq!(nl.nodes.len(), 2);

            // send suspect message with less incarnation number.
            nl.handle_suspect(Suspect {
                node: String::from("127.1.1.1:8000"),
                incarnation: 1,
                from: String::from("127.1.1.0:8000"),
            })
            .await;

            // It should not mark the node as suspect, because suspect has less incarnation.
            assert_eq!(
                nl.nodes.get(&String::from("127.1.1.1:8000")).unwrap().state,
                State::Alive
            );

            // send suspect message with same incarnation number.
            nl.handle_suspect(Suspect {
                node: String::from("127.1.1.1:8000"),
                incarnation: 2,
                from: String::from("127.1.1.0:8000"),
            })
            .await;

            // It should mark the node as suspect, because suspect has same incarnation.
            assert_eq!(
                nl.nodes.get(&String::from("127.1.1.1:8000")).unwrap().state,
                State::Suspect
            );

            // now we send a refute message.
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 3,
            })
            .await;
            // after refute it should be alive.
            assert_eq!(
                nl.nodes.get(&String::from("127.1.1.1:8000")).unwrap().state,
                State::Alive
            );

            // send suspect message with higher incarnation number.
            nl.handle_suspect(Suspect {
                node: String::from("127.1.1.1:8000"),
                incarnation: 4,
                from: String::from("127.1.1.0:8000"),
            })
            .await;

            // It should mark the node as suspect, because suspect has msg higher incarnation.
            assert_eq!(
                nl.nodes.get(&String::from("127.1.1.1:8000")).unwrap().state,
                State::Suspect
            );
        });
    }

    #[test]
    fn test_indirect_ping() {
        let (mut send, recv) = mpsc::channel(100);
        let (send_udp, mut recv_udp) = mpsc::channel(100);
        let mut nl = get_mock_nilai(recv, send_udp, send.clone());
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            nl.handle_alive(Alive {
                name: String::from("node 2"),
                addr: String::from("127.1.1.1:8000"),
                incarnation: 2,
            })
            .await;
            nl.handle_alive(Alive {
                name: String::from("node 3"),
                addr: String::from("127.3.3.3:8000"),
                incarnation: 2,
            })
            .await;
            assert_eq!(nl.nodes.len(), 3);

            // send indirect ping from node 3 to node 2.
            nl.handle_indirect_ping(IndirectPing {
                seq_no: 5,
                to: String::from("127.1.1.1:8000"),
                from: String::from("127.3.3.3:8000"),
            })
            .await;

            assert_eq!(nl.indirect_ack_checker.len(), 1);

            // Now you got indirect ack and nl send ack with seq_no 1.
            nl.handle_ack(nl.seq_no - 1).await;

            let mut ack_exist = false;
            let mut ack_seq_no = 0;
            loop {
                match recv_udp.try_next() {
                    Ok(msg) => match msg {
                        Some(udp_msg) => match udp_msg.msg {
                            Message::Ack(msg) => {
                                ack_exist = true;
                                ack_seq_no = msg.seq_no;
                            }
                            _ => {
                                continue;
                            }
                        },
                        None => {
                            break;
                        }
                    },
                    _ => {
                        break;
                    }
                }
            }
            assert!(ack_exist);
            assert_eq!(ack_seq_no, 5);
        });
    }
    #[test]
    fn test_handle_state_sync() {
         let (mut send, recv) = mpsc::channel(100);
        let (send_udp, mut recv_udp) = mpsc::channel(100);
        let mut nl = get_mock_nilai(recv, send_udp, send.clone());
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            nl.handle_state_sync(Alive{
                addr: String::from("127.1.1.1:8000"),
                name: String::from("node 2"),
                incarnation: 0,
            }).await;
            
            // It has to be updated in the local node details.
            assert_eq!(nl.node_ids.len(), 2);
            assert_eq!(nl.node_ids[1], String::from("127.1.1.1:8000"));

            let mut node_2 = nl.nodes.get_mut(&String::from("127.1.1.1:8000")).unwrap();

            assert_eq!(node_2.name, String::from("node 2"));
            assert_eq!(node_2.addr, String::from("127.1.1.1:8000"));
            assert_eq!(node_2.incarnation, 0);
            assert_eq!(node_2.state, State::Alive);

            // let's mark this node as dead and send state sync.
            node_2.state = State::Dead;
            nl.handle_state_sync(Alive{
                addr: String::from("127.1.1.1:8000"),
                name: String::from("node 2"),
                incarnation: 0,
            }).await;
            let mut dead_msg_exist = false;
            // expect dead msg from the nilai.
            loop {
                match recv_udp.try_next() {
                    Ok(msg) => match msg {
                        Some(udp_msg) => match udp_msg.msg {
                            Message::Dead(msg) => {
                                dead_msg_exist = true;
                                assert_eq!(msg.incarnation, 0);
                                assert_eq!(msg.node,String::from("127.1.1.1:8000"));
                                assert_eq!(msg.from, nl.addr);
                            }
                            _ => {
                                continue;
                            }
                        },
                        None => {
                            break;
                        }
                    },
                    _ => {
                        break;
                    }
                }
            }
            assert!(dead_msg_exist);
        });
    }
}
