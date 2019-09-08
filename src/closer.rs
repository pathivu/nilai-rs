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
