"""General utility tools."""

import asyncio
import inspect
import random


class Log:
    """Debugging log writer.

    Parameters
    ----------
    out_fh : file handle
        Output file/stream.
    debug : boolean
        Log will only be written if True.
    """

    def __init__(self, out_fh, debug):
        self._out_fh = out_fh
        self._debug = debug

    def write(self, data):
        """Write data to the output stream."""
        if self._debug and self._out_fh:
            self._out_fh.write(data)
            self._out_fh.flush()


def unpack_data(data_str):
    """Extract the code and message from a received string.

    Parameters
    ----------
    data_str : str

    Returns
    -------
    code : str
    msg : str
    """
    if data_str:
        data_list = data_str.split(' ', 1)
        code = data_list[0]
        msg = None if len(data_list) < 2 else data_list[1]
        return code, msg
    return None, None


def unpack_nid(nid_str):
    """Unpack a unique id, hostname and port from a network id string.

    Parameters
    ----------
    nid_str : str

    Returns
    -------
    uid : str
    host : str
    port : int
    """
    if nid_str:
        id_list = nid_str.rsplit('@', 1)
        if len(id_list) == 2:
            addr_list = id_list[1].rsplit(':', 1)
            if len(addr_list) == 2:
                return id_list[0], addr_list[0], addr_list[1]
    return None, None, None


def sample_set(target_set, num, evict_sample=False):
    """Sample a list of random elements from a set.

    Parameters
    ----------
    target_set : set
        The set to sample.
    num : int
        The number of elements to select.
    evict_sample : boolean, optional
        Elements will be removed from the set if True.

    Returns
    -------
    list
        A list of the selected elements.
    """
    if len(target_set) == 0:
        return []
    sample_list = random.sample(target_set, min(len(target_set), num))
    if evict_sample:
        target_set.difference_update(sample_list)
    return sample_list


def run_callback(cb, *args, **kwargs):
    """Run a potentially asynchronous callback.

    Parameters
    ----------
    cb : callable
        The callback function.
    *args, **kwargs
        Parameters to pass to the callback.

    Returns
    -------
    asyncio.Task object or callable result
        Returns a asyncio task object if the callable is an awaitable.
        Otherwise, the result of the callable is returned.
    """
    if inspect.isawaitable(cb) or inspect.iscoroutinefunction(cb):
        return asyncio.create_task(cb(*args, **kwargs))
    else:
        return cb(*args, **kwargs)
