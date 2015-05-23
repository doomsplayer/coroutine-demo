use deque::Stealer;
use std::sync::mpsc::Sender;
use coroutine::coroutine::Handle;

pub enum SchedMessage {
    NewNeighbor(Sender<SchedMessage>, Stealer<Handle>),
    Shutdown,
}
