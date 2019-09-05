use futures::channel::mpsc;
use futures::SinkExt;
use futures::channel::oneshot;
use super::types;
// NilaiCloser close all the channels which let to close the nilai handler.
pub struct NilaiCloser {
    pub (crate) handler_signal: oneshot::Receiver<i32>,
    pub (crate) transport_receiver_signal: oneshot::Receiver<i32>,
    pub (crate) transport_sender_signal: oneshot::Receiver<i32>,
}

impl NilaiCloser  {
    pub fn close(&mut self){
        // don't ha
    }
}