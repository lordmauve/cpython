import queue
import sched
import threading
import time
import unittest
from test import support


TIMEOUT = 10


class Timer:
    def __init__(self):
        self._cond = threading.Condition()
        self._time = 0
        self._stop = 0

    def time(self):
        with self._cond:
            return self._time

    # increase the time but not beyond the established limit
    def sleep(self, t):
        assert t >= 0
        with self._cond:
            t += self._time
            while self._stop < t:
                self._time = self._stop
                self._cond.wait()
            self._time = t

    # advance time limit for user code
    def advance(self, t):
        assert t >= 0
        with self._cond:
            self._stop += t
            self._cond.notify_all()


def thread_run(target):
    """Run target in a thread."""
    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t


def assert_thread_stopping(t):
    """Assert that the thread t is stopping."""
    t.join(timeout=1)
    if t.is_alive():
        raise AssertionError("Thread failed to stop")


class TestCase(unittest.TestCase):

    def test_enter(self):
        l = []
        fun = lambda x: l.append(x)
        scheduler = sched.scheduler(time.time, time.sleep)
        for x in [0.5, 0.4, 0.3, 0.2, 0.1]:
            scheduler.enter(x, 1, fun, (x,))
        scheduler.run()
        self.assertEqual(l, [0.1, 0.2, 0.3, 0.4, 0.5])

    def test_enterabs(self):
        l = []
        fun = lambda x: l.append(x)
        scheduler = sched.scheduler(time.time, time.sleep)
        for x in [0.05, 0.04, 0.03, 0.02, 0.01]:
            scheduler.enterabs(x, 1, fun, (x,))
        scheduler.run()
        self.assertEqual(l, [0.01, 0.02, 0.03, 0.04, 0.05])

    def test_enter_concurrent(self):
        q = queue.Queue()
        fun = q.put
        timer = Timer()
        scheduler = sched.scheduler(timer.time, timer.sleep)
        scheduler.enter(1, 1, fun, (1,))
        scheduler.enter(3, 1, fun, (3,))
        t = threading.Thread(target=scheduler.run)
        t.start()
        timer.advance(1)
        self.assertEqual(q.get(timeout=TIMEOUT), 1)
        self.assertTrue(q.empty())
        for x in [4, 5, 2]:
            scheduler.enter(x - 1, 1, fun, (x,))
        timer.advance(2)
        self.assertEqual(q.get(timeout=TIMEOUT), 2)
        self.assertEqual(q.get(timeout=TIMEOUT), 3)
        self.assertTrue(q.empty())
        timer.advance(1)
        self.assertEqual(q.get(timeout=TIMEOUT), 4)
        self.assertTrue(q.empty())
        timer.advance(1)
        self.assertEqual(q.get(timeout=TIMEOUT), 5)
        self.assertTrue(q.empty())
        timer.advance(1000)
        support.join_thread(t, timeout=TIMEOUT)
        self.assertTrue(q.empty())
        self.assertEqual(timer.time(), 5)

    def test_priority(self):
        l = []
        fun = lambda x: l.append(x)
        scheduler = sched.scheduler(time.time, time.sleep)
        for priority in [1, 2, 3, 4, 5]:
            scheduler.enterabs(0.01, priority, fun, (priority,))
        scheduler.run()
        self.assertEqual(l, [1, 2, 3, 4, 5])

    def test_cancel(self):
        l = []
        fun = lambda x: l.append(x)
        scheduler = sched.scheduler(time.time, time.sleep)
        now = time.time()
        event1 = scheduler.enterabs(now + 0.01, 1, fun, (0.01,))
        scheduler.enterabs(now + 0.02, 1, fun, (0.02,))
        scheduler.enterabs(now + 0.03, 1, fun, (0.03,))
        scheduler.enterabs(now + 0.04, 1, fun, (0.04,))
        event5 = scheduler.enterabs(now + 0.05, 1, fun, (0.05,))
        scheduler.cancel(event1)
        scheduler.cancel(event5)
        scheduler.run()
        self.assertEqual(l, [0.02, 0.03, 0.04])

    def test_cancel_concurrent(self):
        q = queue.Queue()
        fun = q.put
        timer = Timer()
        scheduler = sched.scheduler(timer.time, timer.sleep)
        now = timer.time()
        scheduler.enterabs(now + 1, 1, fun, (1,))
        event2 = scheduler.enterabs(now + 2, 1, fun, (2,))
        scheduler.enterabs(now + 4, 1, fun, (4,))
        event5 = scheduler.enterabs(now + 5, 1, fun, (5,))
        scheduler.enterabs(now + 3, 1, fun, (3,))
        t = threading.Thread(target=scheduler.run)
        t.start()
        timer.advance(1)
        self.assertEqual(q.get(timeout=TIMEOUT), 1)
        self.assertTrue(q.empty())
        scheduler.cancel(event2)
        scheduler.cancel(event5)
        timer.advance(1)
        self.assertTrue(q.empty())
        timer.advance(1)
        self.assertEqual(q.get(timeout=TIMEOUT), 3)
        self.assertTrue(q.empty())
        timer.advance(1)
        self.assertEqual(q.get(timeout=TIMEOUT), 4)
        self.assertTrue(q.empty())
        timer.advance(1000)
        support.join_thread(t, timeout=TIMEOUT)
        self.assertTrue(q.empty())
        self.assertEqual(timer.time(), 4)

    def test_empty(self):
        l = []
        fun = lambda x: l.append(x)
        scheduler = sched.scheduler(time.time, time.sleep)
        self.assertTrue(scheduler.empty())
        for x in [0.05, 0.04, 0.03, 0.02, 0.01]:
            scheduler.enterabs(x, 1, fun, (x,))
        self.assertFalse(scheduler.empty())
        scheduler.run()
        self.assertTrue(scheduler.empty())

    def test_queue(self):
        l = []
        fun = lambda x: l.append(x)
        scheduler = sched.scheduler(time.time, time.sleep)
        now = time.time()
        e5 = scheduler.enterabs(now + 0.05, 1, fun)
        e1 = scheduler.enterabs(now + 0.01, 1, fun)
        e2 = scheduler.enterabs(now + 0.02, 1, fun)
        e4 = scheduler.enterabs(now + 0.04, 1, fun)
        e3 = scheduler.enterabs(now + 0.03, 1, fun)
        # queue property is supposed to return an order list of
        # upcoming events
        self.assertEqual(scheduler.queue, [e1, e2, e3, e4, e5])

    def test_args_kwargs(self):
        seq = []
        def fun(*a, **b):
            seq.append((a, b))

        now = time.time()
        scheduler = sched.scheduler(time.time, time.sleep)
        scheduler.enterabs(now, 1, fun)
        scheduler.enterabs(now, 1, fun, argument=(1, 2))
        scheduler.enterabs(now, 1, fun, argument=('a', 'b'))
        scheduler.enterabs(now, 1, fun, argument=(1, 2), kwargs={"foo": 3})
        scheduler.run()
        self.assertCountEqual(seq, [
            ((), {}),
            ((1, 2), {}),
            (('a', 'b'), {}),
            ((1, 2), {'foo': 3})
        ])

    def test_run_non_blocking(self):
        l = []
        fun = lambda x: l.append(x)
        scheduler = sched.scheduler(time.time, time.sleep)
        for x in [10, 9, 8, 7, 6]:
            scheduler.enter(x, 1, fun, (x,))
        scheduler.run(blocking=False)
        self.assertEqual(l, [])


class ConditionWaiterTest(unittest.TestCase):
    """Tests for the ConditionWaiter class."""

    def setUp(self):
        self.w = sched.ConditionWaiter()

    def test_wait(self):
        """We can sleep for at least one ms."""
        start = time.monotonic()
        self.w.sleep(0.001)
        end = time.monotonic()
        self.assertGreaterEqual(end - start, 0.001)

    def test_interrupt(self):
        """We can interrupt the sleep in order to return immediately."""
        l = []

        @thread_run
        def wait_op():
            l.append('started')
            self.w.sleep(100)
            l.append('stopped')

        while not l:
            time.sleep(0.01)
        self.w.interrupt()

        assert_thread_stopping(wait_op)
        self.assertEqual(l, ['started', 'stopped'])


class SchedulerInterruptTest(unittest.TestCase):
    """Test that we can interrupt a sleeping Scheduler when adding new events.

    See bpo-20126 for the issue this is addressing.
    """

    def test_cancel_waited_event(self):
        """If we cancel the only event, the scheduler should return."""
        sch = sched.Scheduler()
        ev = sch.enter(100, 0, lambda: 1 / 0)
        t = thread_run(sch.run)
        time.sleep(0.01)  # let the scheduler get to sleep
        sch.cancel(ev)
        assert_thread_stopping(t)

    def test_clear_waited_event(self):
        """If we clear events, the scheduler should return."""
        sch = sched.Scheduler()
        sch.enter(100, 0, lambda: 1 / 0)
        t = thread_run(sch.run)
        time.sleep(0.01)  # let the scheduler get to sleep
        sch.clear()
        assert_thread_stopping(t)

    def test_interrupt(self):
        """We can interrupt a sleeping scheduler by adding a new event."""
        l = []
        sch = sched.Scheduler()
        ev = sch.enter(100, 0, l.append, (1,))
        t = thread_run(sch.run)

        # schedule something immediately
        sch.enter(0, 0, l.append, (2,))
        time.sleep(0.01)

        contents = l[:]
        sch.cancel(ev)
        self.assertEqual(contents, [2])
        assert_thread_stopping(t)


if __name__ == "__main__":
    unittest.main()
