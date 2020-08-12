"""Test the hyparviewpy.rw_lock module."""

import asyncio
import binascii
import random
import unittest
import sys
import time

from hyparviewpy import rw_lock
from .tools import async_test


class Test_Lock(unittest.TestCase):
    def setUp(self):
        self._rand_bytes = random.getrandbits(32).to_bytes(4, sys.byteorder)
        self._crc = binascii.crc32(self._rand_bytes)

    def test_sync_context(self):
        """Test synchronous context manager exception."""
        lock = rw_lock.RW_Lock()
        with self.assertRaises(RuntimeError):
            lock.reader().__enter__()
        with self.assertRaises(RuntimeError):
            lock.writer().__enter__()

        self.assertIsNone(lock.reader().__exit__(None, None, None))
        self.assertIsNone(lock.writer().__exit__(None, None, None))

    @async_test()
    async def test_lock_state(self):
        """Test basic lock states."""
        lock = rw_lock.RW_Lock()

        # new lock shouldn't be locked
        self.assertFalse(lock.reader().locked())
        self.assertFalse(lock.writer().locked())

        # shouldn't be able to release an unlocked lock
        with self.assertRaises(RuntimeError):
            await lock.reader().release()
        with self.assertRaises(RuntimeError):
            await lock.writer().release()

        # if reader held, should be write locked but not read locked
        async with lock.reader():
            self.assertFalse(lock.reader().locked())
            self.assertTrue(lock.writer().locked())

        # no locks held, should be unlocked
        self.assertFalse(lock.reader().locked())
        self.assertFalse(lock.writer().locked())

        # should be read and write locked if writer held
        async with lock.writer():
            self.assertTrue(lock.reader().locked())
            self.assertTrue(lock.writer().locked())

        # no locks held, should be unlocked
        self.assertFalse(lock.reader().locked())
        self.assertFalse(lock.writer().locked())

    @async_test()
    async def test_single_rw(self):
        """Test a single concurrent reader and writer."""
        lock = rw_lock.RW_Lock()
        await asyncio.gather(self._reader(lock), self._writer(lock))
        await self._reader(lock)

    @async_test(timeout=40)
    async def test_many_rw(self):
        """Test multiple concurrent readers and writers."""
        # start with a single writer
        lock = rw_lock.RW_Lock()
        tasks = [asyncio.create_task(self._writer(lock))]
        await asyncio.sleep(0.1)

        # create 10 readers
        for i in range(10):
            tasks.append(asyncio.create_task(self._reader(lock)))

        # should not take more than 2 seconds
        await asyncio.wait(tasks, timeout=2)
        tasks = []

        # create 10 readers and writers in a random order
        start_time = int(time.time())
        lock_order = ['r' if i < 10 else 'w' for i in range(20)]
        random.shuffle(lock_order)
        for c in lock_order:
            if c == 'r':
                tasks.append(asyncio.create_task(self._reader(lock)))
            else:
                tasks.append(asyncio.create_task(self._writer(lock)))
            await asyncio.sleep(_rand_tenth())

        # total time should be between 10 and 35 seconds
        await asyncio.wait(set(tasks))
        end_time = int(time.time())
        self.assertGreaterEqual(end_time - start_time, 10)
        self.assertLessEqual(end_time - start_time, 35)

    async def _reader(self, lock):
        """Check write task is atomic."""
        async with lock.reader():
            # check that random value matches crc and doesnt change whilst
            # sleeping
            self.assertEqual(binascii.crc32(self._rand_bytes), self._crc)
            old_crc = self._crc
            await asyncio.sleep(_rand_tenth())
            self.assertEqual(binascii.crc32(self._rand_bytes), self._crc)
            self.assertEqual(old_crc, self._crc)

    async def _writer(self, lock):
        """Generate a new random value and corresponding crc."""
        async with lock.writer():
            # generate a random 4 byte value, sleep, then generate crc
            new_bytes = random.getrandbits(32).to_bytes(
                4, sys.byteorder
            )
            self._rand_bytes = new_bytes
            await asyncio.sleep(1)
            self._crc = binascii.crc32(new_bytes)


def _rand_tenth():
    """Get a random number between 0.1 and 1.0 inclusive."""
    return random.randint(1, 10) / 10
