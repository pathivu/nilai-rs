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

use nilai::builder;
use nilai::types;
use simplelog::*;
use std::thread;
use std::time::Duration;
fn main() {
    CombinedLogger::init(vec![
        TermLogger::new(LevelFilter::Warn, Config::default(), TerminalMode::Mixed).unwrap(),
        TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed).unwrap(),
    ])
    .unwrap();
    let nilai_builder = builder::NilaiBuilder::new("127.0.0.1:5002".parse().unwrap());
    let closer = nilai_builder
        .alive_delegate(Box::new(|_: types::Node| println!("new node joined")))
        .peers(vec!["127.0.0.1:5001".parse().unwrap()])
        .execute()
        .unwrap();
    // nilai is running so block the current thread.
    thread::sleep(Duration::from_secs(20));
}
