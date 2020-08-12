"""Simple asyncio read write lock. Not thread safe."""

import asyncio


class RW_Lock():
    """The abstraction of a read-write lock pair."""

    def __init__(self):
        self._lock = _Shared_Lock()

    def reader(self):
        """Get an instance of the Reader lock."""
        return Reader(self._lock)

    def writer(self):
        """Get an instance of the Writer lock."""
        return Writer(self._lock)


class Reader():
    """The read lock abstraction.

    Should not be instantiated directly. Instead, create the lock with the
    RW_Lock interface

    Parameters
    ----------
    lock : RW_Lock object
        The associated shared lock instance.
    """

    def __init__(self, lock):
        self._lock = lock

    def __enter__(self):
        """Not applicable. Use asynchronous context manager instead."""
        raise RuntimeError(
            'Asyncronous context manager should be used instead.'
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Not applicable. Use asynchronous context manager instead."""
        pass

    async def __aenter__(self):
        """Acquires the lock on context manager entry."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Releases the lock on context manager exit."""
        await self.release()

    async def acquire(self):
        """Acquire the read lock. Waits until the lock is acquired."""
        await self._lock.acquire_read()

    async def release(self):
        """Release the read lock."""
        await self._lock.release_read()

    def locked(self):
        """Check if read locked.

        Returns
        -------
        boolean
            True if the lock cannot be acquired immediately, False otherwise.
        """
        return self._lock.read_locked()


class Writer():
    """The write lock abstraction.

    Should not be instantiated directly. Instead, create the lock with the
    RW_Lock interface

    Parameters
    ----------
    lock : RW_Lock object
        The associated shared lock instance.
    """

    def __init__(self, lock):
        self._lock = lock

    def __enter__(self):
        """Not applicable. Use asynchronous context manager instead."""
        raise RuntimeError(
            'Asyncronous context manager should be used instead.'
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Not applicable. Use asynchronous context manager instead."""
        pass

    async def __aenter__(self):
        """Acquires the lock on context manager entry."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Releases the lock on context manager exit."""
        await self.release()

    async def acquire(self):
        """Acquire the write lock. Waits until the lock is acquired."""
        await self._lock.acquire_write()

    async def release(self):
        """Release the write lock."""
        await self._lock.release_write()

    def locked(self):
        """Check if write locked.

        Returns
        -------
        boolean
            True if the lock cannot be acquired immediately, False otherwise.
        """
        return self._lock.write_locked()


class _Shared_Lock():
    """Implements read and write lock mechanisms."""

    def __init__(self):
        self._readers = 0
        self._writers = 0
        self._cond = asyncio.Condition()

    def read_locked(self):
        """Check if read locked.

        Returns
        -------
        boolean
            True if the read lock cannot be acquired immediately, False
            otherwise.
        """
        return self._writers > 0

    def write_locked(self):
        """Check if write locked.

        Returns
        -------
        boolean
            True if the write lock cannot be acquired immediately, False
            otherwise.
        """
        return self._readers > 0 or self._writers > 0

    async def acquire_read(self):
        """Acquire the read lock."""
        async with self._cond:
            await self._cond.wait_for(lambda: not self.read_locked())
            self._readers += 1

    async def release_read(self):
        """Release the read lock."""
        async with self._cond:
            self._cond.notify_all()
            if self._readers == 0:
                raise RuntimeError('Releasing a read lock that is not held.')
            self._readers -= 1

    async def acquire_write(self):
        """Acquire the write lock."""
        async with self._cond:
            await self._cond.wait_for(lambda: not self.write_locked())
            self._writers += 1

    async def release_write(self):
        """Release the write lock."""
        async with self._cond:
            self._cond.notify_all()
            if self._writers == 0:
                raise RuntimeError('Releasing a write lock that is not held.')
            self._writers -= 1
