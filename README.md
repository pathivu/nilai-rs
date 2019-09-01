### Nilai-rs

Nilai is a simple, lightweight embedded failure detection protocol based on SWIM.

Nilai detects the failure state by pinging every node in a cluster by round-robin fashion.

It spreads the node state by infection style. Nilai is lightweight because it's built on     
top of rust's async runtime.
