use super::closer;
use super::delegate;
use super::nilai_handler;
use super::transport;
use failure::Error;
use future::join3;
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
/// NilaiBuilder is used to configure and execute the nilai
pub struct NilaiBuilder {
    indirect_checks: usize,
    gossip_nodes: usize,
    addr: SocketAddr,
    name: String,
    probe_interval: Duration,
    probe_timeout: Duration,
    peers: Vec<SocketAddr>,
    alive_delegate: Option<delegate::Handler>,
    dead_delegate: Option<delegate::Handler>,
    suspicious_multiplier: u64,
}

impl NilaiBuilder {
    /// new is used to construct the nilai builder with default configuration and
    /// returns NilaiBuilder, which will be used to tune the NilaiBuilder and to
    /// run nilai.
    pub fn new(addr: SocketAddr) -> NilaiBuilder {
        return NilaiBuilder {
            addr: addr,
            indirect_checks: 3,
            gossip_nodes: 3,
            name: String::from("nilai node"),
            probe_interval: Duration::from_secs(1),
            probe_timeout: Duration::from_millis(500),
            peers: Vec::new(),
            alive_delegate: None,
            dead_delegate: None,
            suspicious_multiplier: 2,
        };
    }

    /// alive_delegate sets the alive handler. This handler will be called any new node joins
    /// the cluster or the node states changes from dead to alive.
    pub fn alive_delegate(mut self, h: delegate::Handler) -> NilaiBuilder {
        self.alive_delegate = Some(h);
        self
    }

    /// dead_delegate sets the dead handler. This handler will be called if the nilai marks
    /// any node as dead.
    pub fn dead_delegate(mut self, h: delegate::Handler) -> NilaiBuilder {
        self.dead_delegate = Some(h);
        self
    }

    /// indirect_checks sets the number of check indirect pings has to be sent before, considering
    /// the node as dead.
    pub fn indirect_checks(mut self, checks: usize) -> NilaiBuilder {
        self.indirect_checks = checks;
        self
    }

    /// gossip_nodes determine how many nodes the message has to be gossiped.
    pub fn gossip_nodes(mut self, gossip_nodes: usize) -> NilaiBuilder {
        self.gossip_nodes = gossip_nodes;
        self
    }

    /// probe_interval is used to set, how often the nodes has to pinged for health check.
    pub fn probe_interval(mut self, interval: Duration) -> NilaiBuilder {
        self.probe_interval = interval;
        self
    }

    /// probe_timeout is used to determine how long nilai has to wait for ack.
    pub fn probe_timeout(mut self, timeout: Duration) -> NilaiBuilder {
        self.probe_timeout = timeout;
        self
    }

    /// peers is used to set all the peers in the cluster. When nilai is started, nilai will
    /// send request to all the peers to updates it's local state. mean while the peers also
    /// updates about the local node state.
    pub fn peers(mut self, peers: Vec<SocketAddr>) -> NilaiBuilder {
        self.peers = peers;
        self
    }

    /// name is used to set the name of the nilai node.
    pub fn name(mut self, name: String) -> NilaiBuilder {
        self.name = name;
        self
    }

    /// suspicious_multiplier is used to set the multiplier timeout. That is, how long nilai
    /// should take before considering a node as dead.
    /// suspicious timeout will be calculated by probe_timeout * suspicious multiplier.
    pub fn suspicious_multiplier(mut self, multiplier: u64) -> NilaiBuilder {
        self.suspicious_multiplier = multiplier;
        self
    }

    pub fn execute(self) -> Result<closer::NilaiCloser, Error> {
        let socket = block_on(UdpSocket::bind(self.addr))?;
        let (handler_sender, udp_receiver) = mpsc::channel(1000);
        let (udp_sender, handler_receiver) = mpsc::channel(1000);
        let (read_half, send_half) = socket.split();
        let alive_delegate = self.alive_delegate;
        let dead_delegate = self.dead_delegate;
        let (handler_closer_signal, closer) = oneshot::channel();
        let mut handler = nilai_handler::NilaiHandler {
            msg_rcv: udp_receiver,
            msg_sender: udp_sender.clone(),
            nodes: HashMap::new(),
            seq_no: 0,
            node_ids: Vec::new(),
            ack_checker: HashMap::new(),
            probe_id: 0,
            timeout_sender: handler_sender.clone(),
            name: self.name,
            indirect_ack_checker: HashMap::new(),
            local_incarnation: 0,
            addr: self.addr.to_string(),
            indirect_checks: self.indirect_checks,
            gossip_nodes: self.gossip_nodes,
            probe_timeout: self.probe_timeout,
            probe_interval: self.probe_interval,
            suspicious_multiplier: self.suspicious_multiplier,
            alive_delegate: alive_delegate,
            dead_delegate: dead_delegate,
            closer: closer,
        };

        let (tansport_receiver_closer_signal, closer) = oneshot::channel();
        let mut transport_receiver = transport::TransportReceiver {
            handler_ch: handler_sender.clone(),
            udp_socket_receiver: read_half,
            closer: closer,
        };

        let (tansport_sender_closer_signal, closer) = oneshot::channel();
        let mut transport_sender = transport::TransportSender {
            udp_socket_sender: send_half,
            handler_recv_ch: handler_receiver,
            closer: closer,
        };
        let peers = self.peers.clone();
        // now start the state machine.
        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                join3(
                    handler.listen(peers),
                    transport_receiver.listen(),
                    transport_sender.listen(),
                )
                .await;
            });
        });
        let closer = closer::NilaiCloser {
            handler_signal: handler_closer_signal,
            transport_receiver_signal: tansport_receiver_closer_signal,
            transport_sender_signal: tansport_sender_closer_signal,
        };
        Ok(closer)
    }
}
