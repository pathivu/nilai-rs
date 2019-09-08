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
