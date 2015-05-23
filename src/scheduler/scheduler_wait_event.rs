#[cfg(any(target_os = "linux",
          target_os = "android"))]
impl Scheduler {
    pub fn wait_event<E: Evented + AsRawFd>(&mut self, fd: &E, interest: Interest) -> io::Result<()> {
        let token = self.eventloop_handler.slabs.insert((Coroutine::current(), From::from(fd.as_raw_fd()))).unwrap();
        try!(self.eventloop.register_opt(fd, token, interest,
                                         PollOpt::level()|PollOpt::oneshot()));

        debug!("wait_event: Blocked current Coroutine ...; token={:?}", token);
        Coroutine::block();
        debug!("wait_event: Waked up; token={:?}", token);

        Ok(())
    }
}

#[cfg(any(target_os = "macos",
          target_os = "freebsd",
          target_os = "dragonfly",
          target_os = "ios",
          target_os = "bitrig",
          target_os = "openbsd"))]
impl Scheduler {
    pub fn wait_event<E: Evented>(&mut self, fd: &E, interest: Interest) -> io::Result<()> {
        let token = self.handler.slabs.insert(Coroutine::current()).unwrap();
        try!(self.eventloop.register_opt(fd, token, interest,
                                         PollOpt::level()|PollOpt::oneshot()));

        debug!("wait_event: Blocked current Coroutine ...; token={:?}", token);
        Coroutine::block();
        debug!("wait_event: Waked up; token={:?}", token);

        Ok(())
    }
}

