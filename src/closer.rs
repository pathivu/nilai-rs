use futures::channel::oneshot;
use failure::Error;
use failure::err_msg;
// NilaiCloser close all the channels which let to close the nilai handler.
pub struct NilaiCloser {
    pub (crate) handler_signal: oneshot::Sender<i32>,
    pub (crate) transport_receiver_signal: oneshot::Sender<i32>,
    pub (crate) transport_sender_signal: oneshot::Sender<i32>,
}

impl NilaiCloser  {
    pub fn close(self)->Result<(), Error>{
        if let Err(_) = self.handler_signal.send(1) {
           return Err(err_msg("unable to close the nilai handler."))
        }
        if let Err(_) = self.transport_receiver_signal.send(1) {
           return Err(err_msg("unable to close transport recevier."))
        } 
        if let Err(_) = self.transport_sender_signal.send(1) {
           return Err(err_msg("unable to close transport sender."))
        }
        // TODO: use golang waitgroup kind of thingy to wait to close
        // all the futures. 
        return Ok(());
    }
}