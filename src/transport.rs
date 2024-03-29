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

use super::types::*;
use failure::Error;
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::SinkExt;
use futures::StreamExt;
use log::{info, warn};
use num_enum::TryFromPrimitive;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::net::SocketAddr;
use tokio::net::udp::split::{UdpSocketRecvHalf, UdpSocketSendHalf};

// decode_msg is used to decode the buf into message.
fn decode_msg(buf: &Vec<u8>) -> Result<Message, Error> {
    let mut deserializer = Deserializer::new(&buf[1..]);
    match MessageType::try_from(*&buf[0])? {
        MessageType::PingMsg => {
            let msg: Ping = Deserialize::deserialize(&mut deserializer)?;
            return Ok(Message::PingMsg(msg));
        }
        MessageType::IndirectPingMsg => {
            let msg: IndirectPing = Deserialize::deserialize(&mut deserializer)?;
            return Ok(Message::IndirectPingMsg(msg));
        }
        MessageType::AckRespMsg => {
            let msg: AckRes = Deserialize::deserialize(&mut deserializer)?;
            return Ok(Message::Ack(msg));
        }
        MessageType::SuspectMsg => {
            let msg: Suspect = Deserialize::deserialize(&mut deserializer)?;
            return Ok(Message::SuspectMsg(msg));
        }
        MessageType::AliveMsg => {
            let msg: Alive = Deserialize::deserialize(&mut deserializer)?;
            return Ok(Message::Alive(msg));
        }
        MessageType::DeadMsg => {
            let msg: Dead = Deserialize::deserialize(&mut deserializer)?;
            return Ok(Message::Dead(msg));
        }
        MessageType::StateSync => {
            println!("got udp state sync");
            let msg: Alive = Deserialize::deserialize(&mut deserializer)?;
            return Ok(Message::StateSync(msg));
        }
        MessageType::StateSyncRes => {
            let msg: Alive = Deserialize::deserialize(&mut deserializer)?;
            return Ok(Message::StateSyncRes(msg));
        }
    }
}

fn encode_msg(msg: Message, buf: &mut Vec<u8>) -> Result<(), Error> {
    match msg {
        Message::PingMsg(msg) => {
            buf.push(MessageType::PingMsg as u8);
            msg.serialize(&mut Serializer::new(buf))?;
        }
        Message::IndirectPingMsg(msg) => {
            buf.push(MessageType::IndirectPingMsg as u8);
            msg.serialize(&mut Serializer::new(buf))?;
        }
        Message::Ack(msg) => {
            buf.push(MessageType::AckRespMsg as u8);
            msg.serialize(&mut Serializer::new(buf))?;
        }
        Message::SuspectMsg(msg) => {
            buf.push(MessageType::SuspectMsg as u8);
            msg.serialize(&mut Serializer::new(buf))?;
        }
        Message::Alive(msg) => {
            buf.push(MessageType::AliveMsg as u8);
            msg.serialize(&mut Serializer::new(buf))?;
        }
        Message::Dead(msg) => {
            buf.push(MessageType::DeadMsg as u8);
            msg.serialize(&mut Serializer::new(buf))?;
        }
        Message::StateSync(msg) => {
            println!("sending state sync");
            buf.push(MessageType::StateSync as u8);
            msg.serialize(&mut Serializer::new(buf))?;
        }
        Message::StateSyncRes(msg) => {
            buf.push(MessageType::StateSyncRes as u8);
            msg.serialize(&mut Serializer::new(buf))?;
        }
        _ => {
            unimplemented!();
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct TransportReceiver {
    pub handler_ch: mpsc::Sender<UdpMessage>,
    pub udp_socket_receiver: UdpSocketRecvHalf,
    pub closer: oneshot::Receiver<i32>,
}

impl TransportReceiver {
    pub(crate) async fn listen(&mut self) {
        let mut buf = vec![0; 1024];
        loop {
            if let Ok(opt) = self.closer.try_recv() {
                if let Some(_) = opt {
                    info!("stopping transport receiver");
                    break;
                }
            }

            match self.udp_socket_receiver.recv_from(&mut buf).await {
                Ok((read_bytes, from)) => {
                    info!("{} bytes received", read_bytes);
                    if read_bytes == 0 {
                        continue;
                    }
                    match decode_msg(&buf) {
                        Ok(msg) => {
                            self.send_msg(from, msg).await;
                        }
                        Err(err) => {
                            println!("unable to decode");
                            warn!("unable to decode the message {}", err);
                        }
                    }
                }
                Err(err) => {
                    warn!("{} error while receiving the packets", err);
                }
            }
        }
    }

    async fn send_msg(&mut self, from: SocketAddr, msg: Message) {
        if let Err(e) = self
            .handler_ch
            .send(UdpMessage {
                peer: Some(from),
                msg: msg,
            })
            .await
        {
            warn!("unable to send to the nilai handler {}", e);
        }
    }
}

/// MessageType determines type of message. Which is used for encoding and decoding.
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub(crate) enum MessageType {
    PingMsg = 0,
    IndirectPingMsg = 1,
    AckRespMsg = 2,
    SuspectMsg = 3,
    AliveMsg = 4,
    DeadMsg = 5,
    StateSync = 6,
    StateSyncRes = 7,
}

pub(crate) struct TransportSender {
    pub udp_socket_sender: UdpSocketSendHalf,
    pub handler_recv_ch: mpsc::Receiver<UdpMessage>,
    pub closer: oneshot::Receiver<i32>,
}

impl TransportSender {
    pub(crate) async fn listen(&mut self) {
        let mut buf = vec![0; 1024];
        loop {
            if let Ok(opt) = self.closer.try_recv() {
                if let Some(_) = opt {
                    info!("stopping transport sender");
                    break;
                }
            }
            buf.clear();
            match self.handler_recv_ch.next().await {
                Some(udp_msg) => {
                    // every message should have peer.
                    let peer = udp_msg.peer.unwrap();
                    match encode_msg(udp_msg.msg, &mut buf) {
                        Ok(_) => {
                            match self
                                .udp_socket_sender
                                .send_to(&buf[..buf.len()], &peer)
                                .await
                            {
                                Err(e) => {
                                    warn!("error while sending udp message {} {}", e, peer);
                                    continue;
                                }
                                Ok(bytes_sent) => {
                                    info!("bytes sent {}", bytes_sent);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("unable to decode the message {} ", e);
                        }
                    }
                }
                None => {
                    info!("stopping to listen for handler message");
                    break;
                }
            }
        }
    }
}
