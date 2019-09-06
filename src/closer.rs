use super::types;
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::SinkExt;
// NilaiCloser close all the channels which let to close the nilai handler.
pub struct NilaiCloser {
    pub(crate) handler_signal: oneshot::Sender<i32>,
    pub(crate) transport_receiver_signal: oneshot::Sender<i32>,
    pub(crate) transport_sender_signal: oneshot::Sender<i32>,
}

impl NilaiCloser {
    pub fn close(&mut self) {
        // don't ha
    }
    pub fn join_handle(&self) {}
}
