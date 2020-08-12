"""Test the hyparviewpy.Node module."""

import asyncio
import random
import time
import unittest

from hyparviewpy import Node, exceptions, constants
from .tools import async_test, create_server


class Test_Node(unittest.TestCase):
    @async_test()
    async def test_node_init(self):
        """Test the initial state."""
        n = Node()
        await self._check_exited_node(n)
        self.assertIsNone(n.__exit__(None, None, None))

    @async_test(timeout=20)
    async def test_node_running(self):
        """Test the running state."""
        # test state with context manager and manually running
        n = Node()
        async with n:
            await self._check_started_node(n)
        await self._check_exited_node(n)

        run_task = asyncio.create_task(n.run_node())
        self.assertTrue(await n.wait_running())
        await run_task
        await self._check_started_node(n)
        n.exit_node()
        await n.wait_stopped()
        await self._check_exited_node(n)

        # test sockets are being opened correctly
        n = Node(host='127.0.0.1')
        async with n:
            await self._check_started_node(n)
            self.assertEqual(n.listening_sockets()[0][0], '127.0.0.1')
        await self._check_exited_node(n)

        n = Node(port=45612)
        async with n:
            await self._check_started_node(n)
            self.assertEqual(n.listening_sockets()[0][1], 45612)

        n = Node(host='127.0.0.1', port=37538)
        async with n:
            await self._check_started_node(n)
            self.assertEqual(n.listening_sockets(), [('127.0.0.1', 37538)])

    @async_test()
    async def test_early_exit(self):
        """Test cleanup for nodes that exit before startup has completed."""
        n = Node()
        run_task = asyncio.create_task(n.run_node())
        await asyncio.sleep(0)
        self.assertFalse(n.check_alive())
        n.exit_node()
        await n.wait_running()
        await n.wait_stopped()
        await run_task
        await self._check_exited_node(n)

    @async_test()
    async def test_network_join(self):
        """Test simple network joining."""
        async with Node() as n1:
            n2 = Node()
            async with n2:
                await n2.join_network(n1.nid())
                self.assertEqual(n1.num_active(), 1)
                self.assertEqual(n2.num_active(), 1)
                self.assertEqual(n1.num_passive(), 0)
                self.assertEqual(n2.num_passive(), 0)
                self.assertTrue(n2.uid() in n1.active_peers()[0])
                self.assertTrue(n1.uid() in n2.active_peers()[0])
                self.assertEqual(n1.passive_peers(), [])
                self.assertEqual(n2.passive_peers(), [])

            # outside of n2 context, should have exited network
            self.assertEqual(n2.active_peers(), [])
            self.assertEqual(n2.num_active(), 0)
            self.assertEqual(n2.passive_peers(), [])
            self.assertEqual(n2.num_passive(), 0)

            await asyncio.sleep(1)
            self.assertEqual(n1.active_peers(), [])
            self.assertEqual(n1.passive_peers(), [])
            self.assertEqual(n1.num_active(), 0)
            self.assertEqual(n1.num_passive(), 0)

    @async_test()
    async def test_basic_messaging(self):
        """Test simple direct messaging and broadcasting."""
        async with Node() as n1:
            async with Node() as n2:
                await n2.join_network(n1.nid())

                n1bcast = Network_Message()
                n2bcast = Network_Message()
                n1.attach_broadcast_callback(n1bcast.msg_callback)
                n2.attach_broadcast_callback(n2bcast.msg_callback)

                n1dmsg = Network_Message()
                n2dmsg = Network_Message()
                n1.attach_direct_message_callback(n1dmsg.msg_callback)
                n2.attach_direct_message_callback(n2dmsg.msg_callback)

                await n1.send_broadcast('hello world')
                msg_succ = await n2bcast.wait_msg()
                self.assertTrue(msg_succ)
                self.assertEqual(n2bcast.nid, n1.nid())
                self.assertEqual(n2bcast.msg, 'hello world')

                # nodes shouldnt receive their own broadcast
                msg_succ = await n1bcast.wait_msg(1)
                self.assertFalse(msg_succ)

                await n2.send_message(n1.nid(), '42')
                msg_succ = await n1dmsg.wait_msg()
                self.assertTrue(msg_succ)
                self.assertEqual(n1dmsg.nid, n2.nid())
                self.assertEqual(n1dmsg.msg, '42')

                # nodes shouldnt receive their own direct meessage
                msg_succ = await n2dmsg.wait_msg(1)
                self.assertFalse(msg_succ)

    @async_test()
    async def test_invalid_actions(self):
        """Test error handling for invalid actions."""
        n = Node()

        with self.assertRaises(exceptions.InvalidActionError):
            n.__enter__()

        with self.assertRaises(exceptions.InvalidActionError):
            await n.join_network('hello')

        with self.assertRaises(exceptions.InvalidActionError):
            await n.send_message('hello', 'world')

        with self.assertRaises(exceptions.InvalidActionError):
            await n.send_broadcast('hello')

        async with n:
            with self.assertRaises(exceptions.InvalidActionError):
                await n.run_node()

            with self.assertRaises(exceptions.InvalidActionError):
                await n.join_network(n.nid())

            async with Node() as n2:
                await n.join_network(n2.nid())
                with self.assertRaises(exceptions.InvalidActionError):
                    await n.join_network(n2.nid())

        with self.assertRaises(exceptions.ServerError):
            n = Node(host='hello')
            await n.run_node()
        with self.assertRaises(exceptions.ServerError):
            n = Node(host='127.0.0.1', port=99999)
            await n.run_node()

    @async_test()
    async def test_dead_peer(self):
        """Test a peer exiting unexpectedly."""
        async with Node() as n1:
            async with Node() as n2:
                await n2.join_network(n1.nid())
                peer = next(iter(n1._act_set))
                writer = peer._writer
                peer._writer = None
                await n1.send_broadcast('hello')
                writer.close()
                await writer.wait_closed()

    @async_test()
    async def test_invalid_messages(self):
        """Test error handling for invalid messages sent by peers."""
        async with Node() as n:
            reader, writer = await asyncio.open_connection(
                'localhost', n._port
            )
            writer.write('hello\n'.encode())
            await writer.drain()
            writer.close()
            self.assertTrue(n.check_alive())

        async with Node() as n1:
            async with Node() as n2:
                await n2.join_network(n1.nid())
                peer = next(iter(n2._act_set))
                await peer.send_message(f'{constants.JOIN_FOR} hello 42')
                await peer.send_message(f'{constants.SHU_MES} hello 42 world')
                self.assertEqual(n1.num_active(), 1)
                self.assertEqual(n1.num_passive(), 0)

                await peer.send_message('hello world')
                await asyncio.sleep(2)
                self.assertEqual(n1.num_active(), 0)
                self.assertEqual(n1.num_passive(), 0)

    @async_test()
    async def test_dead_passive(self):
        """Test error handling when node attempts to contact a dead passive."""
        async with Node() as n1:
            async with Node() as n2:
                await n2.join_network(n1.nid())
                peer = next(iter(n2._act_set))

                self._serv, port = await create_server(self._serv_cb)
                async with self._serv:
                    # get  a@localhost into n1's passive set
                    await peer.send_message(
                        f'{constants.SHU_MES} {n2.nid()} 0 a@localhost:{port}'
                    )
                    await asyncio.sleep(2)
                    self.assertEqual(n1.num_active(), 1)
                    self.assertEqual(n1.num_passive(), 1)

                    # get n1 to attempt to contact a@local which should fail
                    await peer.send_message(
                        f'{constants.SHU_MES} a@localhost:{port} 0 {n2.nid()}'
                    )
                    await asyncio.sleep(2)
                    self.assertEqual(n1.num_active(), 1)
                    self.assertEqual(n1.num_passive(), 0)

    @async_test(timeout=600)
    async def test_node_many(self):
        """Test network resilience with many nodes and failures."""
        node_list = [Node() for i in range(100)]

        # check uids are unique
        uid_list = [n.uid() for n in node_list]
        self.assertEqual(len(uid_list), len(set(uid_list)))

        running_list = []
        try:
            for n in node_list:
                # create node and join upon random node in network
                await n.run_node()
                if len(running_list) > 0:
                    ct_n, _ = random.choice(running_list)
                    await n.join_network(ct_n.nid())

                msg = Network_Message()
                n.attach_broadcast_callback(msg.msg_callback)
                n.attach_direct_message_callback(msg.msg_callback)
                running_list.append((n, msg))

            for i in range(0, 3):
                await asyncio.sleep(constants.SHU_TIME + 5)

            while len(running_list) > 1:
                # broadcast and check all nodes received message
                await running_list[0][0].send_broadcast('hello world')
                for node, msg in running_list[1:]:
                    msg_suc = await msg.wait_msg(15)
                    self.assertTrue(msg_suc)
                    self.assertEqual(msg.nid, running_list[0][0].nid())
                    self.assertEqual(msg.msg, 'hello world')

                # kill half of the nodes
                kill_list = running_list[:len(running_list) // 2]
                for n, _ in kill_list:
                    n.exit_node()
                await asyncio.gather(*[n.wait_stopped() for n, _ in kill_list])

                new_list = running_list[len(running_list) // 2:]
                for _, msg in new_list:
                    msg.reset()

                running_list = new_list
                await asyncio.sleep(constants.SHU_TIME + 5)
        finally:
            for n in node_list:
                n.exit_node()
            await asyncio.gather(*[n.wait_stopped() for n in node_list])

    async def _check_exited_node(self, n):
        self.assertFalse(n.check_alive())
        self.assertEqual(n.listening_sockets(), [])
        self.assertEqual(n.active_peers(), [])
        self.assertEqual(n.passive_peers(), [])
        self.assertEqual(n.num_active(), 0)
        self.assertEqual(n.num_passive(), 0)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(n.wait_running(), timeout=0.1)
        await asyncio.wait_for(n.wait_stopped(), timeout=0.5)

    async def _check_started_node(self, n):
        self.assertTrue(n.check_alive())
        self.assertNotEqual(n.listening_sockets(), [])
        self.assertEqual(n.active_peers(), [])
        self.assertEqual(n.passive_peers(), [])
        self.assertEqual(n.num_active(), 0)
        self.assertEqual(n.num_passive(), 0)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(n.wait_stopped(), timeout=0.2)
        await asyncio.wait_for(n.wait_running(), timeout=0.5)

    async def _serv_cb(self, reader, writer):
        self._serv.close()
        await self._serv.wait_closed()
        writer.close()
        await writer.wait_closed()


class Network_Message:
    def __init__(self):
        self.nid = None
        self.msg = None

    def msg_callback(self, nid, msg):
        self.nid = nid
        self.msg = msg

    def reset(self):
        self.nid = None
        self.msg = None

    async def wait_msg(self, timeout=None):
        start = int(time.time())
        while not self.nid:
            if timeout and int(time.time()) - start > timeout:
                return False
            await asyncio.sleep(0.2)
        return True
