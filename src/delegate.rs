use super::types;

/// handler is used for giving notification.
/// Ideally, handler is called when new node comes or
/// node is dead.
pub type Handler = Box<dyn Fn(types::Node)>;
