"""Two ways to synchronise the tree: one global mutex, or a read-write lock built on threading.Condition."""
import threading


class GlobalLock:
    """Every operation, read or write, takes the same mutex."""

    name = "global"

    def __init__(self):
        lock = threading.Lock()
        self.r_acquire = self.w_acquire = lock.acquire
        self.r_release = self.w_release = lock.release


class RWLock:
    """Read-write lock on threading.Condition.

    Policy (the same as Go's sync.RWMutex):
    * any number of readers hold the lock together; a writer holds it alone;
    * a waiting writer blocks readers that arrive after it, so a stream of readers cannot starve writers;
    * when a writer releases the lock, the readers that were waiting at that moment are let in before the next
      writer (`granted`), so a stream of writers cannot starve readers either.
    """

    name = "rw"

    def __init__(self):
        self.cond = threading.Condition(threading.Lock())
        self.readers = 0          # readers holding the lock
        self.writer = False       # a writer holds the lock
        self.waiting_writers = 0
        self.waiting_readers = 0
        self.granted = 0          # readers admitted by the last writer release ahead of waiting writers

    def r_acquire(self):
        with self.cond:
            if not self.writer and self.waiting_writers == 0:
                self.readers += 1
                return
            self.waiting_readers += 1
            while self.writer or (self.waiting_writers and not self.granted):
                self.cond.wait()
            self.waiting_readers -= 1
            if self.granted:
                self.granted -= 1
            self.readers += 1

    def r_release(self):
        with self.cond:
            self.readers -= 1
            if self.readers == 0 and self.waiting_writers:
                self.cond.notify_all()

    def w_acquire(self):
        with self.cond:
            self.waiting_writers += 1
            while self.writer or self.readers or self.granted:
                self.cond.wait()
            self.waiting_writers -= 1
            self.writer = True

    def w_release(self):
        with self.cond:
            self.writer = False
            self.granted = self.waiting_readers
            if self.waiting_readers or self.waiting_writers:
                self.cond.notify_all()


LOCKS = {"global": GlobalLock, "rw": RWLock}


class WriterPreferringRWLock(RWLock):
    """Rejected variant: readers wait while any writer waits, and nothing lets them in after a writer releases."""

    name = "rw_writer_pref"

    def r_acquire(self):
        with self.cond:
            while self.writer or self.waiting_writers:
                self.cond.wait()
            self.readers += 1

    def w_release(self):
        with self.cond:
            self.writer = False
            self.cond.notify_all()


LOCKS["rw_writer_pref"] = WriterPreferringRWLock
