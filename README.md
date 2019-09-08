### Nilai-rs

Nilai is a simple, lightweight embedded failure detection protocol based on SWIM. Nilai detects the failure state by pinging every node in a cluster by round-robin fashion.

It spreads the node state by infection style. Nilai is lightweight because it's built on top of tokio runtime.

Nilai derived from thamizh word (நிலை), which means state.


### Example 

```rust
use failure::Error;
use nilai::builder;
use nilai::types;
use std::thread;
use std::time::Duration;

fn do_main() -> Result<(), Error> {
    let nilai_builder = builder::NilaiBuilder::new("127.0.0.1:5001".parse()?);
    let closer = nilai_builder
        .alive_delegate(Box::new(|_: types::Node| println!("new node joined")))
        .execute()?;
    // nilai is running so block the current thread.
    thread::sleep(Duration::from_secs(5));
    closer.stop();
    Ok(())
}

fn main() {
    match do_main() {
        Err(err) => {
            println!("not able to run nilai handler {:?}", err);
        }
        _ => {}
    }
}


```