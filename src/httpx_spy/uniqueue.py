import asyncio
import threading
from collections import deque
from typing import Generic, TypeVar

T = TypeVar("T")


class QueueEmpty(Exception):
    """Exception raised when you're getting items from the queue and it is
    empty"""


class UniQueue(Generic[T]):
    """
    The UniQueue is similar to threading's or asyncio's queue, except that it
    works safely from any thread or loop (most asyncio things will require to
    stay in the same loop, here we also allow inter-loop communication).
    """

    def __init__(self, max_size: int = 0):
        self._max_size = max_size
        self._queue = deque()

        # For thread safety
        self._mutex = threading.Lock()
        self._not_full = threading.Condition(self._mutex)
        self._not_empty = threading.Condition(self._mutex)

        # For async coordination
        self._waiters: dict[asyncio.AbstractEventLoop, set[asyncio.Future]] = {}
        self._put_waiters: dict[asyncio.AbstractEventLoop, set[asyncio.Future]] = {}

    def _get_waiters(
        self, loop: asyncio.AbstractEventLoop | None = None
    ) -> set[asyncio.Future]:
        """Get waiters for a specific loop"""
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running event loop, create a default dict entry
                loop = object()  # Use a dummy object as key

        if loop not in self._waiters:
            self._waiters[loop] = set()

        return self._waiters[loop]

    def _get_put_waiters(
        self, loop: asyncio.AbstractEventLoop | None = None
    ) -> set[asyncio.Future]:
        """Get put waiters for a specific loop"""
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running event loop, create a default dict entry
                loop = object()  # Use a dummy object as key

        if loop not in self._put_waiters:
            self._put_waiters[loop] = set()

        return self._put_waiters[loop]

    def _notify_waiters(self) -> None:
        """Notify all waiters that an item is available"""
        self._not_empty.notify_all()

        for loop, waiters in list(self._waiters.items()):
            for waiter in list(waiters):
                if not waiter.done():
                    try:
                        if hasattr(loop, "call_soon_threadsafe"):
                            loop.call_soon_threadsafe(waiter.set_result, None)
                        else:
                            waiter.set_result(None)
                    except Exception:  # noqa: S110
                        pass

    def _notify_put_waiters(self) -> None:
        """Notify all put waiters that space is available"""
        self._not_full.notify_all()

        for loop, waiters in list(self._put_waiters.items()):
            for waiter in list(waiters):
                if not waiter.done():
                    try:
                        if hasattr(loop, "call_soon_threadsafe"):
                            loop.call_soon_threadsafe(waiter.set_result, None)
                        else:
                            waiter.set_result(None)
                    except Exception:  # noqa: S110
                        pass
            waiters.clear()

    def sync_put(self, obj: T) -> None:
        """
        Putting the object into the queue. If the queue is full, wait until a
        spot frees up.
        """
        with self._mutex:
            while 0 < self._max_size <= len(self._queue):
                self._not_full.wait()

            self._queue.append(obj)
            self._notify_waiters()

    async def async_put(self, obj: T) -> None:
        """
        Putting the object into the queue. If the queue is full, wait until a
        spot frees up.
        """
        print(f"Putting = {obj}")
        loop = asyncio.get_running_loop()

        with self._mutex:
            if self._max_size <= 0 or len(self._queue) < self._max_size:
                self._queue.append(obj)
                self._notify_waiters()
                return

        # If we're here, queue is full, we need to wait
        while True:
            waiter = loop.create_future()

            with self._mutex:
                if self._max_size <= 0 or len(self._queue) < self._max_size:
                    self._queue.append(obj)
                    self._notify_waiters()
                    return

                # Add ourselves to put waiters
                waiters = self._get_put_waiters(loop)
                waiters.add(waiter)

            try:
                await waiter
            except:
                with self._mutex:
                    waiters.discard(waiter)
                raise

    async def async_get_nowait(self) -> T:
        """Gets an item from the queue, or raise QueueEmpty"""
        with self._mutex:
            if not self._queue:
                msg = "Queue is empty"
                raise QueueEmpty(msg)

            # Get item and remove it from the queue
            item = self._queue.popleft()

            # Notify put waiters that there's space in the queue
            if self._max_size > 0:
                self._notify_put_waiters()

            # Important: Only discard the waiter after successfully getting an item
            loop = asyncio.get_running_loop()
            waiters = self._get_waiters(loop)
            for waiter in list(waiters):
                if waiter.done():
                    waiters.discard(waiter)

            return item
