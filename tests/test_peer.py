"""Test the hyparviewpy.peer module."""

import asyncio
import unittest

from hyparviewpy.peer import Peer, ActivePeer
from hyparviewpy import constants as consts, exceptions as excp
from .tools import async_test, create_server


class Test_Peer(unittest.TestCase):
    @async_test()
    async def test_peer_creation(self):
        """Test peer initial state."""
        # test equality, str and hash functions
        pas_p = Peer('a', 'b', 0)
        act_p = pas_p.to_active(None, None)
        act_p2 = ActivePeer('a', 'b', 0, None, None)
        self.assertEqual(pas_p, act_p)
        self.assertEqual(pas_p, act_p2)
        self.assertEqual(pas_p, act_p.to_passive())
        self.assertEqual(pas_p.__hash__(), act_p.__hash__())
        self.assertEqual(str(pas_p), 'a@b:0')
        self.assertEqual(
            str(act_p), f'{pas_p.uid()}@{pas_p.host()}:{pas_p.port()}'
        )

        # exiting the peer shouldn't cause issues
        act_p.kill()
        await act_p.wait_killed()
        act_p2.kill('a')
        await act_p2.wait_killed()

    @async_test()
    async def test_messaging(self):
        """Test the ActivePeer.send_message function."""
        peer = ActivePeer('a', 'b', 0, None, None)
        with self.assertRaises(excp.InvalidActionError):
            await peer.send_message('a')

        serv, port = await create_server(self._serv_cb)
        async with serv:
            peer = await self._create_peer('a', 'localhost', port)
            await peer.send_message('hello world')
            self.assertEqual(
                (await self._reader.readline()).decode(), 'hello world\n'
            )

            peer.kill()
            await peer.wait_killed()
            self._writer.close()
            await self._writer.wait_closed()

    @async_test()
    async def test_invalid_activate(self):
        """Test invalid use cases of ActivePeer.activate."""
        peer = ActivePeer('a', 'b', 0, None, None)
        with self.assertRaises(excp.InvalidActionError):
            peer.activate(None, None, None, None)

        serv, port = await create_server(self._serv_cb)
        async with serv:
            peer = await self._create_peer('a', 'localhost', port)

            # should not be able to activate already active peer, peer that
            # is exiting, or peer that has exited
            with self.assertRaises(excp.InvalidActionError):
                peer.activate(None, None, None, None)
            peer.kill()
            with self.assertRaises(excp.InvalidActionError):
                peer.activate(None, None, None, None)
            await peer.wait_killed()
            with self.assertRaises(excp.InvalidActionError):
                peer.activate(None, None, None, None)

            self._writer.close()
            await self._writer.wait_closed()

    @async_test()
    async def test_active_peer(self):
        """Test the ActivePeer listening loop."""
        serv, port = await create_server(self._serv_cb)
        self._fwd_res = None
        self._shu_res = None
        self._bcast_res = None
        self._dead_res = None

        async with serv:
            peer = await self._create_peer('a', 'localhost', port)

            # send a sequence of valid messages
            self._writer.write(
                f'{consts.JOIN_FOR} c@d:1 {consts.ACT_WALK}\n'.encode()
            )
            self._writer.write((
                f'{consts.SHU_MES} c@d:1 {consts.SHU_WALK}'
                ' e@f:2 g@h:3\n'
            ).encode())
            self._writer.write(
                f'{consts.B_MSG} c@d:1 42 ff hello world\n'.encode()
            )
            self._writer.write(f'{consts.PING}\n'.encode())
            self._writer.write(f'{consts.EXIT}\n'.encode())
            await self._writer.drain()

            # wait for callbacks to finish
            while not (
                self._fwd_res and self._shu_res and self._bcast_res and
                self._dead_res
            ):
                await asyncio.sleep(0.2)

            # check that messages were received correctly
            self.assertEqual(
                self._fwd_res,
                (f'a@localhost:{port}', 'c@d:1', consts.ACT_WALK)
            )
            self.assertEqual(
                self._shu_res, (
                    f'a@localhost:{port}', 'c@d:1', consts.SHU_WALK,
                    'e@f:2 g@h:3'
                )
            )
            self.assertEqual(
                self._bcast_res, (
                    f'a@localhost:{port}', 'c@d:1', 42, 'ff', 'hello world'
                )
            )
            self.assertEqual(
                self._dead_res, (f'a@localhost:{port}', consts.EXIT)
            )

            peer.kill(consts.EXIT)
            await peer.wait_killed()
            self._writer.close()
            await self._writer.wait_closed()

            # test peer exit message
            peer = await self._create_peer('a', 'localhost', port)
            await asyncio.sleep(1)
            peer.kill(consts.DISC)
            await peer.wait_killed()
            data = await self._reader.readline()
            self.assertEqual(data.decode(), f'{consts.DISC}\n')
            self._writer.close()
            await self._writer.wait_closed()

            # test peer getting killed while waiting for read task
            # is_exiting attribute set manually to guarantee execution path
            peer = await self._create_peer('a', 'localhost', port)
            await asyncio.sleep(1)
            peer._is_exiting = True
            self._writer.write(f'{consts.PING}\n'.encode())
            await self._writer.drain()
            await asyncio.sleep(0.5)
            peer._is_exiting = False
            peer.kill(consts.DISC)
            await peer.wait_killed()
            self._writer.close()
            await self._writer.wait_closed()

    @async_test()
    async def test_invalid_messages(self):
        """Test error handling for invalid messages sent to the peer."""
        serv, port = await create_server(self._serv_cb)
        async with serv:
            # test various error cases
            await self._check_inval('a', 'localhost', port, 'hello world')

            await self._check_inval('a', 'localhost', port, consts.JOIN_FOR)
            await self._check_inval(
                'a', 'localhost', port, f'{consts.JOIN_FOR} hello'
            )

            await self._check_inval('a', 'localhost', port, consts.SHU_MES)
            await self._check_inval(
                'a', 'localhost', port, f'{consts.SHU_MES} hello'
            )
            await self._check_inval(
                'a', 'localhost', port, f'{consts.SHU_MES} hello world'
            )

            await self._check_inval('a', 'localhost', port, consts.B_MSG)
            await self._check_inval(
                'a', 'localhost', port, f'{consts.B_MSG} hello'
            )
            await self._check_inval(
                'a', 'localhost', port, f'{consts.B_MSG} hello world'
            )
            await self._check_inval(
                'a', 'localhost', port, f'{consts.B_MSG} hello world again'
            )
            await self._check_inval(
                'a', 'localhost', port, f'{consts.B_MSG} hello 42 world'
            )

            # test an unexpected disconnection
            p = await self._create_peer('a', 'localhost', port)
            self._dead_res = None
            self._writer.close()
            await self._writer.wait_closed()
            while not self._dead_res:
                await asyncio.sleep(0.2)
            self.assertEqual(consts.DATA_ERR, self._dead_res[1])
            p.kill()
            await p.wait_killed()

    async def _create_peer(self, uid, host, port):
        """Create a peer with an active connection to a local test server."""
        self._reader = None
        self._writer = None
        reader, writer = await asyncio.open_connection('localhost', port)
        await asyncio.sleep(0.2)
        peer = ActivePeer('a', 'localhost', port, reader, writer)
        peer.activate(
            self._fwd_cb, self._shu_cb, self._bcast_cb, self._dead_cb
        )
        return peer

    async def _check_inval(self, uid, host, port, send_message):
        """Send a peer an invalid message and check the exit code."""
        self._dead_res = None
        peer = await self._create_peer(uid, host, port)
        self._writer.write(f'{send_message}\n'.encode())
        await self._writer.drain()
        while not self._dead_res:
            await asyncio.sleep(0.2)
        self.assertEqual(consts.INVAL_ERR, self._dead_res[1])
        peer.kill()
        await peer.wait_killed()
        self._writer.close()
        await self._writer.wait_closed()
        self._writer = None
        self._reader = None

    async def _serv_cb(self, reader, writer):
        self._reader = reader
        self._writer = writer

    async def _fwd_cb(self, sender, target, ttl):
        self._fwd_res = (str(sender), target, ttl)

    async def _shu_cb(self, sender, target, ttl, peers):
        self._shu_res = (str(sender), target, ttl, peers)

    async def _bcast_cb(self, sender, origin, ts, uid, msg):
        self._bcast_res = (str(sender), origin, ts, uid, msg)

    async def _dead_cb(self, sender, exit_code):
        self._dead_res = (str(sender), exit_code)
