"""A generally useful event scheduler class.

Each instance of this class manages its own queue.
No multi-threading is implied; you are supposed to hack that
yourself, or use a single instance per application.

Each instance is parametrized with two functions, one that is
supposed to return the current time, one that is supposed to
implement a delay.  You can implement real-time scheduling by
substituting time and sleep from built-in module time, or you can
implement simulated time by writing your own functions.  This can
also be used to integrate scheduling with STDWIN events; the delay
function is allowed to modify the queue.  Time can be expressed as
integers or floating point numbers, as long as it is consistent.

Events are specified by tuples (time, priority, action, argument, kwargs).
As in UNIX, lower priority numbers mean higher priority; in this
way the queue can be maintained as a priority queue.  Execution of the
event means calling the action function, passing it the argument
sequence in "argument" (remember that in Python, multiple function
arguments are be packed in a sequence) and keyword parameters in "kwargs".
The action function may be an instance method so it
has another way to reference private data (besides global variables).
"""
import heapq
import abc
import threading
import warnings
from collections import namedtuple
from time import monotonic as _time
from functools import total_ordering

__all__ = ["scheduler", "Scheduler", "Waiter"]


@total_ordering
class Event(namedtuple('Event', 'time, priority, action, argument, kwargs')):
    __slots__ = ()

    def __eq__(s, o):
        return (s.time, s.priority) == (o.time, o.priority)

    def __lt__(s, o):
        return (s.time, s.priority) < (o.time, o.priority)

    def __call__(self):
        """Execute the event."""
        return self.action(*self.argument, **self.kwargs)


Event.time.__doc__ = ('''Numeric type compatible with the return value of the
timefunc function passed to the constructor.''')
Event.priority.__doc__ = ('''Events scheduled for the same time will be executed
in the order of their priority.''')
Event.action.__doc__ = ('''Executing the event means executing
action(*argument, **kwargs)''')
Event.argument.__doc__ = ('''argument is a sequence holding the positional
arguments for the action.''')
Event.kwargs.__doc__ = ('''kwargs is a dictionary holding the keyword
arguments for the action.''')

_sentinel = object()


class Waiter(abc.ABC):
    @abc.abstractmethod
    def sleep(self, delay: float):
        """Block for delay seconds, unless interrupted."""

    @abc.abstractmethod
    def interrupt(self):
        """Interrupt threads that are waiting in sleep(), if any.

        interrupt() is only needed in multi-threaded applications; it
        may do nothing on platforms where threads are not implemented.
        """


class _ConditionWaiter(Waiter):
    """A timer that uses a threading.Condition to block threads."""

    def __init__(self):
        """Initialize a ConditionWaiter with a new condition variable."""
        self._cond = threading.Condition()

    def sleep(self, delay):
        """Sleep for delay seconds, unless interrupted."""
        with self._cond:
            self._cond.wait(delay)

    def interrupt(self):
        """Interrupt threads currently blocked in sleep()."""
        with self._cond:
            self._cond.notify_all()


def scheduler(timefunc=_time, delayfunc=None):
    """Construct a Scheduler.

    The scheduler uses timefunc to query the current time.

    If delayfunc is given, it will be used to block by the Scheduler
    to block until the next scheduled event.

    """
    if delayfunc:
        return _UninterruptibleScheduler(timefunc, delayfunc)
    return Scheduler(timefunc)


class Scheduler:
    def __init__(self, timefunc=_time, waiter=None):
        """Initialize a new instance.

        If a Waiter instance is given, it will be used for the sleep
        operations, as well as interrupting those operations when
        necessary. Otherwise the waiter used is platform- and
        implementation-dependent.

        """
        self._queue = []
        self._lock = threading.RLock()
        self.timefunc = timefunc
        self.waiter = waiter or _ConditionWaiter()
        self.running = False

    def enterabs(self, time, priority, action, argument=(), kwargs=_sentinel):
        """Enter a new event in the queue at an absolute time.

        Returns an ID for the event which can be used to remove it,
        if necessary.

        Note that the interpretation for the time given must match the
        that of timefunc. Thus if timefunc is time.monotonic() (the
        default), then this function should only be called with time values
        offset from time.monotonic(). It is recommended to use .enter()
        instead in this case.

        """
        if kwargs is _sentinel:
            kwargs = {}
        event = Event(time, priority, action, argument, kwargs)
        q = self._queue
        with self._lock:
            if q and time < q[0].time:
                # We're holding the lock, so we can interrupt now even though
                # we haven't added the new event yet.
                self.waiter.interrupt()
            heapq.heappush(q, event)
        return event  # The ID

    def enter(self, delay, priority, action, argument=(), kwargs=_sentinel):
        """Schedule action in delay seconds, with given priority."""
        time = self.timefunc() + delay
        return self.enterabs(time, priority, action, argument, kwargs)

    def cancel(self, event):
        """Remove an event from the queue.

        This must be presented the ID as returned by enter().
        If the event is not in the queue, this raises ValueError.

        """
        with self._lock:
            self._queue.remove(event)
            if not self.queue:
                # Removing an event can only mean we need to wake up later,
                # never earlier. Waking up with nothing to do is not a
                # concern.
                #
                # However, when we remove the last event, there is nothing
                # later to wait for, so we wake up now in order to
                # terminate.
                self.waiter.interrupt()
            else:
                heapq.heapify(self._queue)

    def clear(self):
        """Unschedule all events from the queue."""
        with self._lock:
            del self._queue[:]
            self.waiter.interrupt()

    def empty(self):
        """Check whether the queue is empty."""
        with self._lock:
            return not self._queue

    def run(self, blocking=True):
        """Execute events until the queue is empty.

        If blocking is False, execute the scheduled events due to
        expire soonest (if any) and then return the deadline of the
        next scheduled call in the scheduler, or None if no further
        events are scheduled.

        When there is a positive delay until the first event, the
        delay function is called and the event is left in the queue;
        otherwise, the event is removed from the queue and executed
        (its action function is called, passing it the argument).  If
        the delay function returns prematurely, it is simply
        restarted.

        It is legal for both the delay function and the action
        function to modify the queue or to raise an exception;
        exceptions are not caught but the scheduler's state remains
        well-defined so run() may be called again.

        A questionable hack is added to allow other threads to run:
        just after an event is executed, a delay of 0 is executed, to
        avoid monopolizing the CPU when other threads are also
        runnable.

        """
        # localize variable access to minimize overhead
        # and to improve thread safety
        lock = self._lock
        q = self._queue
        timefunc = self.timefunc
        delayfunc = self.waiter.sleep
        pop = heapq.heappop

        self.running = True
        while self.running:
            with lock:
                if not q:
                    break

                time = q[0].time
                now = timefunc()

                delay = max(time - now, 0)
                if delay == 0:
                    ev = pop(q)

            if delay:
                if not blocking:
                    return delay
                delayfunc(delay)
            else:
                ev()
                del ev  # don't hold reference to prev event into next sleep

    def run_forever(self):
        """Run the scheduler.

        Even when the queue is empty, do not exit, unless explicitly stopped.

        .. versionadded:: 3.9

        """
        # localize variable access to minimize overhead
        # and to improve thread safety
        lock = self._lock
        q = self._queue
        timefunc = self.timefunc
        delayfunc = self.waiter.sleep
        pop = heapq.heappop

        self.running = True
        while self.running:
            with lock:
                if q:
                    time = q[0].time
                    now = timefunc()

                    delay = max(time - now, 0)
                    if delay == 0:
                        ev = pop(q)
                else:
                    # Nothing in the queue; wait for an arbitrary large
                    # time, expecting to be interrupted
                    delay = 86400

            if delay:
                delayfunc(delay)
            else:
                ev()
                del ev  # don't hold reference to prev event into next sleep

    def stop(self):
        """Stop the running scheduler.

        .. versionadded:: 3.9

        """
        self.running = False
        self.waiter.interrupt()

    @property
    def queue(self):
        """An ordered list of upcoming events.

        Events are named tuples with fields for:
            time, priority, action, arguments, kwargs

        """
        # Use heapq to sort the queue rather than using 'sorted(self._queue)'.
        # With heapq, two events scheduled at the same time will show in
        # the actual order they would be retrieved.
        with self._lock:
            events = self._queue[:]

        return [heapq.heappop(events) for _ in iter(events.__len__, 0)]


class _UninterruptibleWaiter(Waiter):
    """A waiter that doesn't provide the interrupt capability."""

    def __init__(self, delayfunc):
        """Initialise using a custom delay function."""
        self._delayfunc = delayfunc
        self._waiting = set()

    def sleep(self, t):
        """Sleep for t seconds using delayfunc."""
        ident = threading.get_ident()
        self._waiting.add(ident)
        try:
            self._delayfunc(t)
        finally:
            self._waiting.discard(ident)

    def interrupt(self):
        """Interrupt is not implemented for this waiter.

        Warn if we're in a situation where it matters.
        """
        if self._waiting:
            warnings.warn(
                "Schedule changed while running, but interrupt() "
                "is not supported()",
                RuntimeWarning,
                3
            )

    def __repr__(self):
        return f'{type(self).__qualname__}({self.delayfunc!r})'


class _UninterruptibleScheduler(Scheduler):
    """A Scheduler constructed without passing an interruptfunc.

    This scheduler fully implements older APIs but warns about situations
    where they will cause issues.

    """

    def __init__(self, timefunc, delayfunc):
        """Initialize a new instance.

        If a timer instance is given, it will be used for querying time and
        blocking. Otherwise a new ConditionTimer will be used.

        """
        super().__init__(
            timefunc=timefunc,
            waiter=_UninterruptibleWaiter(delayfunc),
        )

    def run_forever(self):
        raise NotImplementedError(
            "Scheduler.run_forever() is not available when using a custom "
            "delayfunc, as this cannot be interrupted."
        )
