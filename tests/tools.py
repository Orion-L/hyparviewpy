import asyncio
import functools


def async_test(timeout=10):
    def do_test(coro):
        @functools.wraps(coro)
        def wrapper(*args, **kwargs):
            loop = asyncio.new_event_loop()
            result = loop.run_until_complete(
                asyncio.wait_for(coro(*args, **kwargs), timeout=timeout)
            )
            loop.close()
            return result
        return wrapper
    return do_test


async def create_server(cb):
    serv = await asyncio.start_server(cb, host='', port=0)
    port = serv.sockets[0].getsockname()[1]
    return serv, port
