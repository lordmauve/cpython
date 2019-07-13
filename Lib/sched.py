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

import time
import heapq
import abc
import threading
from collections import namedtuple
from time import monotonic as _time
from functools import total_ordering

__all__ = ["scheduler", "Scheduler", "Waiter", "ConditionWaiter"]


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
    def block(self, delay: float):
        """Block for delay seconds, unless interrupted."""

    @abc.abstractmethod
    def interrupt(self):
        """Interrupt threads that are waiting in block(), if any."""


def scheduler(timefunc=_time, delayfunc=time.sleep, interruptfunc=None):
    """Construct a Scheduler.

    The scheduler uses timefunc to query the current time, delayfunc to
    block and interruptfunc to interrupt the blocking.

    If interruptfunc is not given then submitting new events will never
    interrupt blocked threads. This can cause events to be run later
    than scheduled when using multiple threads to submit events.

    """
    waiter = type('CustomWaiter', (Waiter,), {
        'block': staticmethod(delayfunc),
        'interrupt': staticmethod(interruptfunc or (lambda: None))
    })()
    return Scheduler(timefunc, waiter=waiter)


class ConditionWaiter(Waiter):
    """A timer that uses a threading.Condition to block threads."""

    def __init__(self):
        """Initialize a ConditionWaiter with a new condition variable."""
        import threading
        self._cond = threading.Condition()

    def block(self, delay):
        """Block for delay seconds."""
        self._cond.wait(delay)

    def interrupt(self):
        """Interrupt currently waiting threads."""
        self._cond.notify_all()


class Scheduler:
    def __init__(self, timefunc=_time, waiter=None):
        """Initialize a new instance.

        If a timer instance is given, it will be used for querying time and
        blocking. Otherwise a new ConditionTimer will be used.

        """
        self._queue = []
        self._lock = threading.Lock()
        self.timefunc = timefunc
        self.waiter = waiter or ConditionWaiter()

    def enterabs(self, time, priority, action, argument=(), kwargs=_sentinel):
        """Enter a new event in the queue at an absolute time.

        Returns an ID for the event which can be used to remove it,
        if necessary.

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
            heapq.heappush(self._queue, event)
        return event  # The ID

    def enter(self, delay, priority, action, argument=(), kwargs=_sentinel):
        """Schedule action with delay and priority."""
        time = self.timefunc() + delay
        return self.enterabs(time, priority, action, argument, kwargs)

    def schedule(
            self, delay, callback,
            args=(),
            kwargs=_sentinel,
            *, priority=0):
        """Schedule callback for delay seconds in the future.

        This method is a convenience over `.enter()` in that `priority`
        is an optional parameter.

        .. versionadded:: 3.9

        """
        return self.enter(delay, priority, callback, args, kwargs)

    def cancel(self, event):
        """Remove an event from the queue.

        This must be presented the ID as returned by enter().
        If the event is not in the queue, this raises ValueError.

        """
        with self._lock:
            self._queue.remove(event)
            heapq.heapify(self._queue)

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
        if blocking:
            for ev in self:
                ev()
                self.waiter.block(0)   # Let other threads run
        else:
            with self._lock:
                delay = self.__next_delay()
                if delay != 0:
                    return delay
                ev = heapq.heappop(self._queue)

            ev()

            with self._lock:
                return self.__next_delay()

    def __iter__(self):
        """Iterate over scheduled events, instead of executing them.

        The returned generator blocks until an event is due and then
        yields the Event object. Calling the Event object dispatches
        the event.

        The iteration terminates when the queue is empty.

        For example,

            sch = scheduler()

            ...

            for ev in sch:
                ev()

        This can be used to capture the return value of events, handle
        exceptions, or perform an action before each event is dispatched:

            rows_modified = 0
            for ev in sch:
                try:
                    rows_modified += ev()
                except Exception:
                    logging.exception("Error dispatching scheduled event")

        """

        # localize variable access to minimize overhead
        # and to improve thread safety
        lock = self._lock
        q = self._queue
        delayfunc = self.waiter.block
        pop = heapq.heappop

        while True:
            with lock:
                delay = self.__next_delay()

                if delay is None:
                    break
                elif delay == 0:
                    ev = pop(q)

            if delay:
                delayfunc(delay)
            else:
                yield ev

    def __next_delay(self):
        """Get the delay until the next Event.

        This function must be called with the lock held; we don't lock it
        here for performance reasons.

        Return 0 exactly if an Event is due now. Return None if there are
        no more events scheduled.

        """
        q = self._queue
        if not q:
            return None

        time = q[0].time
        now = self.timefunc()
        return max(time - now, 0)

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
