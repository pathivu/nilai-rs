use futures::channel::oneshot;
// NilaiCloser close all the channels which let to close the nilai handler.
pub struct NilaiCloser {
    pub(crate) handler_signal: oneshot::Sender<i32>,
    pub(crate) transport_receiver_signal: oneshot::Sender<i32>,
    pub(crate) transport_sender_signal: oneshot::Sender<i32>,
}

impl NilaiCloser {
    /// stop will close the nilai handler. But it won't wait until every resource will
    /// cleaned up. But it'll send signal to the respective resource to stop itself.
    pub fn stop(self) {
        // stop receiving udp packets.
        self.transport_receiver_signal.send(1);

        // stop handler.
        self.handler_signal.send(1);

        // stop sending packets.
        self.transport_sender_signal.send(1);

        // It'll be good to wait here until everything stops.
        // use some golang waitgroup alternative.
    }
}
