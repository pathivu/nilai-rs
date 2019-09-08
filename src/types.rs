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

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
#[derive(Debug, PartialEq, Clone)]
pub enum State {
    Alive = 0,
    Suspect = 1,
    Dead = 2,
}

/// Node contains details of the node, which includes address,name, state, incarnation.
#[derive(Debug, Clone)]
pub struct Node {
    pub addr: String,
    pub name: String,
    pub state: State,
    pub incarnation: u32,
}

/// Message enum is used to send and receive message over the channel.
#[derive(Debug, Clone)]
pub(crate) enum Message {
    PingMsg(Ping),
    IndirectPingMsg(IndirectPing),
    Ack(AckRes),
    Probe,
    PingTimeOut(u32),
    IndirectPingTimeout(u32),
    SuspectMsgTimeout(SuspectTimeout),
    SuspectMsg(Suspect),
    Alive(Alive),
    Dead(Dead),
    StateSync(Alive),
    StateSyncRes(Alive),
}

/// SuspectTimeout is triggered after sending suspect message. To check whether we received any
/// alive message
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub(crate) struct SuspectTimeout {
    pub incarnation: u32,
    pub node_id: String,
}

/// Ping is used to probe a node.
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub(crate) struct Ping {
    pub seq_no: u32,
    pub node: String,
}

/// IndirectPing is used to probe the target node, with the help of other nodes in the cluster.
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub(crate) struct IndirectPing {
    pub seq_no: u32,
    pub to: String,
    pub from: String,
}

/// Suspect is used to suspect a node.
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub(crate) struct Suspect {
    pub incarnation: u32,
    pub from: String,
    pub node: String,
}

/// AckRes is used to send response for the ping.
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub(crate) struct AckRes {
    pub seq_no: u32,
}

/// Alive is used to let know the cluster that, I'm Alive.
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub(crate) struct Alive {
    pub name: String,
    pub addr: String,
    pub incarnation: u32,
}

/// Dead is used to determine that the node is dead.
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub(crate) struct Dead {
    pub from: String,
    pub node: String,
    pub incarnation: u32,
}

/// UdpMessage is used to send and receive udp message between nodes in the cluster.
/// Sometimes, UdpMessage is used to send to message to itself, for example timeout.
/// In timeout scenario, we'll keep peer as None and send timeout message to itself.
/// To check wether we got any response before the timeout, If we didn't get any.
/// Then we'll do the necessary steps for the respective timeout.
#[derive(Debug)]
pub(crate) struct UdpMessage {
    pub msg: Message,
    pub peer: Option<SocketAddr>,
}
