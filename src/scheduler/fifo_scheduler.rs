use std::thread;
use std::collections::VecDeque;
use std::convert::From;
use std::sync::atomic::{ATOMIC_BOOL_INIT, AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::mem;

use mio::{EventLoop, Evented, Interest, PollOpt};

use coroutine::coroutine::{State, Handle, Coroutine};
use coroutine::spawn;

use deque::{BufferPool, Stealer, Worker, Stolen};

const MAX_PRIVATE_WORK_NUM: usize = 10;
pub struct Scheduler {
    public_work_queue: Worker<Handle>,
    public_work_stealer: Stealer<Handle>,
    private_work: VecDeque<Handle>,
    
    commchannel: Receiver<SchedMessage>,

    neighbors: Vec<(Sender<SchedMessage>, Stealer<Handle>)>,

    eventloop: EventLoop<EventloopHandler>,
    eventloop_handler: EventloopHandler,
}

impl Scheduler {

    fn new() -> Scheduler {
        let bufpool = BufferPool::new();
        let (worker, stealer) = bufpool.deque();

        let (tx, rx) = channel();

        let scheds = schedulers();
        let mut guard = scheds.lock().unwrap();

        for &(ref rtx, _) in guard.iter() {
            let _ = rtx.send(SchedMessage::NewNeighbor(tx.clone(), stealer.clone()));
        }

        let neighbors = guard.clone();
        guard.push((tx, stealer.clone()));

        Scheduler {
            public_work_queue: worker,
            public_work_stealer: stealer,
            private_work: VecDeque::with_capacity(MAX_PRIVATE_WORK_NUM),
            
            commchannel: rx,

            neighbors: neighbors,

            eventloop: EventLoop::new().unwrap(),
            eventloop_handler: EventloopHandler::new(),
        }
    }

    pub fn current() -> &'static mut Scheduler {
        SCHEDULER.with(|s| unsafe {
            &mut *s.get()
        })
    }

    pub fn spawn<F>(f: F) where F: FnOnce() + Send + 'static {

        let coro = spawn(f);

        let sc = Scheduler::current();
        sc.ready(coro);

        Coroutine::sched();
    }

    pub fn ready(&mut self, work: Handle) {
        if self.private_work.is_empty() && self.private_work.len() < MAX_PRIVATE_WORK_NUM {
            self.public_work_queue.push(work);
        } else {
            self.private_work.push_back(work);
        }
    }

    // this is the start method
    pub fn run<F>(f: F, threads: usize) where F: FnOnce() + Send + 'static {

        assert!(threads >= 1, "Threads must >= 1");
        if SCHEDULER_HAS_STARTED.compare_and_swap(false, true, Ordering::SeqCst) != false {
            panic!("Schedulers are already running!");
        }

        // Start worker threads first
        let counter = Arc::new(AtomicUsize::new(0));
        for tid in 0..threads - 1 {
            let counter = counter.clone();
            thread::Builder::new().name(format!("Thread {}", tid)).spawn(move|| {
                let current = Scheduler::current();
                counter.fetch_add(1, Ordering::SeqCst);
                current.schedule();
            }).unwrap();
        }

        while counter.load(Ordering::SeqCst) != threads - 1 {}

        Scheduler::spawn(|| {
            struct Guard;

            // Send Shutdown to all schedulers
            impl Drop for Guard {
                fn drop(&mut self) {
                    let guard = match schedulers().lock() {
                        Ok(g) => g,
                        Err(poisoned) => poisoned.into_inner()
                    };

                    for &(ref chan, _) in guard.iter() {
                        let _ = chan.send(SchedMessage::Shutdown);
                    }
                }
            }

            let _guard = Guard;

            f();
        });

        Scheduler::current().schedule();

        SCHEDULER_HAS_STARTED.store(false, Ordering::SeqCst);
    }

    #[inline]
    fn resume_coroutine(&mut self, work: Handle) {
        match work.state() {
            State::Suspended | State::Blocked => {
                debug!("Resuming Coroutine: {:?}", work);

                if let Err(err) = work.resume() {
                    let msg = match err.downcast_ref::<&'static str>() {
                        Some(s) => *s,
                        None => match err.downcast_ref::<String>() {
                            Some(s) => &s[..],
                            None => "Box<Any>",
                        }
                    };

                    error!("Coroutine panicked! {:?}", msg);
                }

                match work.state() {
                    State::Normal | State::Running => {
                        unreachable!();
                    },
                    State::Suspended => {
                        debug!("Coroutine suspended, going to be resumed next round");
                        self.ready(work);
                    },
                    State::Blocked => {
                        debug!("Coroutine blocked, maybe waiting for I/O");
                    },
                    State::Finished | State::Panicked => {
                        debug!("Coroutine state: {:?}, will not be resumed automatically", work.state());
                    }
                }
            },
            _ => {
                error!("Trying to resume coroutine {:?}, but its state is {:?}",
                       work, work.state());
            }
        }
    }
    
    #[inline]
    fn recv_msg(&mut self) -> bool {
        match self.commchannel.try_recv() {
            Ok(SchedMessage::NewNeighbor(tx, st)) => {
                self.neighbors.push((tx, st));
                true
            },
            Ok(SchedMessage::Shutdown) => {
                info!("Shutting down");
                false
            },
            Err(TryRecvError::Empty) => {true},
            _ => panic!("Receiving from channel: Unknown message")
        }
    }

    #[inline]
    fn run_eventloop_once(&mut self) {
        if !self.eventloop_handler.slabs.is_empty() {
            self.eventloop.run_once(&mut self.eventloop_handler).unwrap();
        }
    }

    #[inline]
    fn run_one_work_in_private_queue(&mut self) -> bool {
        if let Some(work) = self.private_work.pop_front() {
            self.resume_coroutine(work);
            true
        } else {
            false
        }
    }

    #[inline]
    fn run_one_work_in_public_queue(&mut self) -> bool {
        if let Stolen::Data(work) = self.public_work_stealer.steal() {  // FIFO Scheduler
            self.resume_coroutine(work);
            true
        } else {
            false
        }
    }

    #[inline]
    fn steal_works(&mut self) -> Vec<Handle> {
        debug!("Trying to steal from neighbors: {:?}", thread::current().name());
        
        self.neighbors.iter().filter_map(
            |&(_, ref st)| {
                if let Stolen::Data(w) = st.steal() {
                    Some(w)
                } else {
                    None
                }
            }).collect()
    }

    // no yield allowed here because scheduler's parent is pointing to it self
    // so that yielding would hatch a duplicated new scheduler coroutine and there will be
    // two scheduler coroutines in one thread eventually, which makes no sense.
    fn schedule(&mut self) {
        while self.recv_msg() {
            self.run_eventloop_once();
            
            debug!("Trying to resume all ready coroutines: {:?}", thread::current().name());
            
            // Run all ready coroutines
            // while self.run_one_work_in_public_queue() {}
            
            while self.run_one_work_in_private_queue() {}

            if self.run_one_work_in_public_queue() {
                continue;
            }
            
            if !self.eventloop_handler.slabs.is_empty() {
                debug!("no need for steal or slab is empty");
                continue;
            }

            let stolen_works = self.steal_works();
            let has_stolen = stolen_works.len() != 0;
            for work in stolen_works.into_iter() {
                self.resume_coroutine(work);
            }
            if !has_stolen {
                thread::sleep_ms(100);
            }
        }
    }
}

include!("scheduler_wait_event.rs");
