"""Unit tests for the hyparviewpy.connect module."""

import asyncio
import unittest

from hyparviewpy import connect, exceptions, constants
from .tools import async_test, create_server


class Test_Connect(unittest.TestCase):
    @async_test()
    async def test_connect_read(self):
        """Test the connect.read_data function."""
        # create a listening server and open a connection
        serv, port = await create_server(self._serv_cb)
        async with serv:
            reader, writer = await asyncio.open_connection('localhost', port)
            await asyncio.sleep(0.2)

            # test that a simple string can be read
            read_task = asyncio.create_task(connect.read_data(reader))
            self._writer.write('hello world\n'.encode())
            await self._writer.drain()
            self.assertEqual(await read_task, 'hello world')

            # test timeout
            with self.assertRaises(exceptions.TimeoutConnectError):
                await connect.read_data(reader, 0.5)

            # test a disconnection whilst reading
            read_task = asyncio.create_task(connect.read_data(reader))
            self._writer.close()
            with self.assertRaises(exceptions.DataConnectError):
                await read_task

            # open a new connection
            writer.close()
            await writer.wait_closed()
            await self._writer.wait_closed()
            reader, writer = await asyncio.open_connection('localhost', port)
            await asyncio.sleep(0.2)

            # test a trauncated message (no newline)
            read_task = asyncio.create_task(connect.read_data(reader))
            self._writer.write('hello world'.encode())
            await self._writer.drain()
            self._writer.close()
            with self.assertRaises(exceptions.DataConnectError):
                await read_task

            writer.close()
            await writer.wait_closed()
            await self._writer.wait_closed()

    @async_test()
    async def test_connect_write(self):
        """Test the connect.send_message and connect.close_writer functions."""
        serv, port = await create_server(self._serv_cb)
        async with serv:
            # test sending a simple string
            await connect.send_message('localhost', port, 'hello world')
            data = await self._reader.readline()
            self.assertEqual(data.decode(), 'hello world\n')
            self._writer.close()
            await self._writer.wait_closed()

            reader, writer = await asyncio.open_connection('localhost', port)
            await asyncio.sleep(0.2)

            # test sending a message and disconnecting
            await connect.close_writer(writer, 'hello world')
            data = await self._reader.readline()
            self.assertEqual(data.decode(), 'hello world\n')
            data = await self._reader.readline()
            self.assertEqual(data.decode(), '')
            self._writer.close()
            await self._writer.wait_closed()

        # test error handling for unavailable destinations
        with self.assertRaises(exceptions.TimeoutConnectError):
            await connect.send_message('localhost', port, 'hello world')

    @async_test(timeout=constants.CONN_TIMEOUT + 10)
    async def test_init_connection(self):
        """Test the connect.init_connection function."""
        serv, port = await create_server(self._init_con_cb)
        async with serv:
            # test a simple connection with a valid response
            p, c = await connect.init_connection(
                'a', 'b', 'localhost', port, 'hello', ['world']
            )
            self.assertEqual(c, 'world')
            self.assertEqual(str(p), 'b@c:d')
            self._writer.close()
            await self._writer.wait_closed()
            p.kill()
            await p.wait_killed()

            # test an invalid code response
            with self.assertRaises(exceptions.ResponseError):
                await connect.init_connection(
                    'a', 'b', 'localhost', port, 'hello', ['hello']
                )
            self._writer.close()
            await self._writer.wait_closed()

            # test an invalid uid response
            with self.assertRaises(exceptions.ResponseError):
                await connect.init_connection(
                    'a', 'c', 'localhost', port, 'hello', ['world']
                )
            self._writer.close()
            await self._writer.wait_closed()

            # test an empty response
            with self.assertRaises(exceptions.ResponseError):
                await connect.init_connection(
                    'a', 'c', 'localhost', port, '42', [None]
                )
            self._writer.close()
            await self._writer.wait_closed()

            # test no response
            with self.assertRaises(exceptions.TimeoutConnectError):
                await connect.init_connection(
                    'a', 'b', 'localhost', port, 'world', ['hello']
                )
            self._writer.close()
            await self._writer.wait_closed()

            # test disconnection
            with self.assertRaises(exceptions.DataConnectError):
                await connect.init_connection(
                    'a', 'b', 'localhost', port, 'hello_world', ['hello']
                )

        # test an unavailable destination
        with self.assertRaises(exceptions.TimeoutConnectError):
            await connect.init_connection(
                'a', 'b', 'localhost', port, 'hello', ['world'])

    async def _serv_cb(self, reader, writer):
        self._reader = reader
        self._writer = writer

    async def _init_con_cb(self, reader, writer):
        """Accompanying callback for test_init_connection."""
        self._reader = reader
        self._writer = writer

        data = await self._reader.readline()
        if data.decode() == 'hello a\n':
            self._writer.write('world b@c:d\n'.encode())
            await self._writer.drain()
        elif data.decode() == '42 a\n':
            self._writer.write('\n'.encode())
            await self._writer.drain()
        elif data.decode() == 'hello_world a\n':
            self._writer.close()
            await self._writer.wait_closed()
