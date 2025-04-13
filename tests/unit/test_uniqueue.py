import asyncio
import threading
import time

import pytest

from httpx_spy.uniqueue import QueueEmpty, UniQueue


@pytest.fixture
def queue():
    """Create a fresh queue for each test"""
    return UniQueue()


@pytest.fixture
def sized_queue():
    """Create a queue with max_size=2"""
    return UniQueue(max_size=2)


def test_init():
    """Test that queue initializes correctly"""
    q1 = UniQueue()
    assert isinstance(q1, UniQueue)

    q2 = UniQueue(max_size=5)
    assert isinstance(q2, UniQueue)


def test_sync_put_and_async_get(queue):
    """Test basic sync_put and async_get_nowait functionality"""
    queue.sync_put(42)

    async def get_item():
        return await queue.async_get_nowait()

    item = asyncio.run(get_item())
    assert item == 42


def test_empty_queue_raises(queue):
    """Test that getting from an empty queue raises QueueEmpty"""

    async def get_empty():
        return await queue.async_get_nowait()

    with pytest.raises(QueueEmpty):
        asyncio.run(get_empty())


def test_async_put_and_async_get():
    """Test async_put and async_get_nowait"""
    queue = UniQueue()

    async def test():
        await queue.async_put("test")
        item = await queue.async_get_nowait()
        assert item == "test"

    asyncio.run(test())


def test_queue_size_limit(sized_queue):
    """Test that queue respects max_size"""
    sized_queue.sync_put(1)
    sized_queue.sync_put(2)

    # Testing sync put with blocking in another thread
    put_completed = threading.Event()

    def put_item():
        sized_queue.sync_put(3)  # This should block until get is called
        put_completed.set()

    thread = threading.Thread(target=put_item)
    thread.start()

    # Verify that put is blocked (wait a bit to make sure)
    time.sleep(0.1)
    assert not put_completed.is_set()

    # Now get an item, which should unblock the put
    async def get_item():
        return await sized_queue.async_get_nowait()

    item = asyncio.run(get_item())
    assert item == 1

    # Wait for the blocked put to complete
    put_completed.wait(timeout=1)
    assert put_completed.is_set()

    # Verify the queue state
    items = []

    async def get_remaining():
        items.append(await sized_queue.async_get_nowait())
        items.append(await sized_queue.async_get_nowait())

        # Queue should be empty now
        with pytest.raises(QueueEmpty):
            await sized_queue.async_get_nowait()

    asyncio.run(get_remaining())
    assert items == [2, 3]

    thread.join()


def test_async_put_with_size_limit():
    """Test async_put with size limit"""
    queue = UniQueue(max_size=1)

    async def test():
        await queue.async_put(1)

        # Start a task that will be blocked on put
        put_task = asyncio.create_task(queue.async_put(2))

        # Give the task time to start and block
        await asyncio.sleep(0.1)

        # Task should still be running (blocked)
        assert not put_task.done()

        # Get an item to make room
        item = await queue.async_get_nowait()
        assert item == 1

        # Let the put_task complete
        await asyncio.sleep(0.1)

        # Verify the item was put
        item = await queue.async_get_nowait()
        assert item == 2

        # Make sure put_task completed
        await put_task

    asyncio.run(test())


def test_multi_producer_consumer():
    """Test multiple producers and consumers"""
    queue = UniQueue()
    n_items = 100
    results = []

    async def producer(start, count):
        for i in range(start, start + count):
            await queue.async_put(i)
            await asyncio.sleep(0.001)  # Small delay to mix things up

    async def consumer(count):
        collected = 0
        while collected < count:
            try:
                item = await queue.async_get_nowait()
                results.append(item)
                collected += 1
            except QueueEmpty:
                await asyncio.sleep(0.005)

    async def test():
        # Start multiple producers and consumers
        producers = [
            asyncio.create_task(producer(i * 25, 25))
            for i in range(4)  # 4 producers, each putting 25 items
        ]

        consumers = [
            asyncio.create_task(consumer(n_items // 2))
            for _ in range(2)  # 2 consumers, each getting 50 items
        ]

        # Wait for all producers to finish
        await asyncio.gather(*producers)

        # Wait for consumers to finish
        await asyncio.gather(*consumers)

        # Check that all items were consumed
        assert len(results) == n_items
        assert sorted(results) == list(range(n_items))

    asyncio.run(test())


def test_cross_thread_communication():
    """Test communication between threads"""
    queue = UniQueue()
    results = []

    def producer_thread():
        for i in range(10):
            queue.sync_put(f"item-{i}")
            time.sleep(0.01)

    async def consumer():
        expected_items = 10
        collected = 0
        timeout = 0
        while collected < expected_items and timeout < 2.0:  # 2 second max timeout
            try:
                item = await queue.async_get_nowait()
                results.append(item)
                collected += 1
            except QueueEmpty:
                await asyncio.sleep(0.01)
                timeout += 0.01

    # Start producer in a separate thread
    thread = threading.Thread(target=producer_thread)
    thread.start()

    # Run consumer in the main thread
    asyncio.run(consumer())

    thread.join()

    # Check results
    assert len(results) == 10
    assert results == [f"item-{i}" for i in range(10)]


def test_cross_event_loop_communication():
    """Test communication between different event loops"""
    queue = UniQueue()
    results = []

    async def producer_coro():
        for i in range(10):
            await queue.async_put(f"loop-item-{i}")
            await asyncio.sleep(0.01)

    def producer_thread():
        asyncio.run(producer_coro())

    async def consumer():
        expected_items = 10
        collected = 0
        timeout = 0
        while collected < expected_items and timeout < 4.0:
            try:
                item = await queue.async_get_nowait()
                results.append(item)
                collected += 1
            except QueueEmpty:
                await asyncio.sleep(0.005)
                timeout += 0.005

    # Start producer in a separate thread with its own event loop
    thread = threading.Thread(target=producer_thread)
    thread.start()

    # Run consumer in the main thread with different event loop
    asyncio.run(consumer())

    thread.join()

    # Check results
    assert len(results) == 10
    assert results == [f"loop-item-{i}" for i in range(10)]


def test_zero_max_size():
    """Test queue with max_size=0 (unlimited)"""
    queue = UniQueue(max_size=0)

    # Try to put a large number of items (should not block)
    for i in range(1000):
        queue.sync_put(i)

    # Check we can get them all
    async def get_all():
        items = []
        for _ in range(1000):
            items.append(await queue.async_get_nowait())
        return items

    items = asyncio.run(get_all())
    assert len(items) == 1000
    assert items == list(range(1000))


def test_stress_test():  # noqa: C901
    """Stress test with many producers and consumers across threads and loops"""
    queue = UniQueue(max_size=100)
    n_items_per_producer = 100
    n_producers = 5
    n_consumers = 5
    total_items = n_items_per_producer * n_producers

    # Track consumed items
    consumed_items = []
    consumption_lock = threading.Lock()

    # Event to signal consumers to stop if all items produced and consumed
    stop_event = threading.Event()

    def sync_producer(producer_id):
        for i in range(n_items_per_producer):
            item = (producer_id, i)
            queue.sync_put(item)
            time.sleep(0.001)  # Small delay

    async def async_producer(producer_id):
        for i in range(n_items_per_producer):
            item = (producer_id, i)
            await queue.async_put(item)
            await asyncio.sleep(0.001)  # Small delay

    def async_producer_thread(producer_id):
        asyncio.run(async_producer(producer_id))

    async def async_consumer():
        while not stop_event.is_set() or get_queue_size() > 0:
            try:
                item = await queue.async_get_nowait()
                with consumption_lock:
                    consumed_items.append(item)
                    if len(consumed_items) >= total_items:
                        stop_event.set()
            except QueueEmpty:
                await asyncio.sleep(0.01)

    def async_consumer_thread():
        asyncio.run(async_consumer())

    def get_queue_size():
        """Helper to get approximate queue size - only for test monitoring"""
        with queue._mutex:
            return len(queue._queue)

    # Start a mix of sync and async producers
    producer_threads = []
    for i in range(n_producers):
        if i % 2 == 0:
            thread = threading.Thread(target=sync_producer, args=(i,))
        else:
            thread = threading.Thread(target=async_producer_thread, args=(i,))
        producer_threads.append(thread)
        thread.start()

    # Start consumers
    consumer_threads = []
    for _ in range(n_consumers):
        thread = threading.Thread(target=async_consumer_thread)
        consumer_threads.append(thread)
        thread.start()

    # Wait for all producers to finish
    for thread in producer_threads:
        thread.join()

    # Wait for consumers to get all items or timeout
    stop_event.wait(timeout=5)

    # Force stop consumers
    stop_event.set()

    # Wait for consumers to finish
    for thread in consumer_threads:
        thread.join(timeout=1)

    # Verify that all items were consumed
    assert len(consumed_items) == total_items

    # Check that each producer's items were fully consumed
    for producer_id in range(n_producers):
        producer_items = [i for (pid, i) in consumed_items if pid == producer_id]
        assert len(producer_items) == n_items_per_producer
        assert sorted(producer_items) == list(range(n_items_per_producer))
