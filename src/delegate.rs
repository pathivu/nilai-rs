use super::types;

/// handler is used for giving notification.
/// Ideally, handler is called when new node comes or
/// node is dead. Please, don't have blocking code here
/// If you need blocking operation. Please send the
/// details via channel to another thread and handle
/// it there.
pub type Handler = Box<dyn Fn(types::Node) + Send>;
