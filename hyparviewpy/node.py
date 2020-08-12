"""The HyParView protocol node implementation."""

import ast
import asyncio
import enum
import sys
import time
import uuid
from collections import deque

from . import exceptions as excp
from . import connect as conn
from . import constants as consts
from .peer import Peer
from .rw_lock import RW_Lock
from . import util


class Node:
    """A node that participates in the HyParView protocol.

    Parameters
    ----------
    fanout : int, optional
        The protocol's gossip fanout parameter.
    host : str, optional
        The listening server's hostname.
    port : int, optional
        The listening server's port number.
    debug : boolean, optional
        If True, debugging messages will be written to the log.
    out_fh : file handle, optional
        The log's output file/stream.
    """

    def __init__(
        self, fanout=consts.FAN_DEF, host=None, port=0, debug=False,
        out_fh=sys.stdout
    ):
        self._host = host
        self._port = port
        self._set_host = (host is not None)
        self._set_port = (port != 0)
        self._log = util.Log(out_fh, debug)

        self._socks = []
        self._uid = uuid.uuid1().hex
        self._nid = ''

        self._act_set = set()
        self._pas_set = set()
        self._max_act = fanout * consts.ACT_C
        self._max_pas = self._max_act * consts.PAS_K
        self._shu_pas_sent = None
        self._num_dead = 0
        self._dead_list = []

        self._dmsg_cb = None
        self._bcast_cb = None
        self._bcast_queue = deque(maxlen=consts.MAX_BCAST)

        self._serv_task = None
        self._state = _NodeState.STOPPED
        self._start_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._stop_event.set()

        self._join_lock = asyncio.Lock()
        self._listen_cb_lock = RW_Lock()
        self._msg_job_lock = RW_Lock()

        self._promote_lock = asyncio.Lock()
        self._promote_target = None

        self._peer_callbacks = [
            self._proc_join_for, self._proc_shu_mes, self._proc_bcast,
            self._proc_dead
        ]

    def __enter__(self):
        """Not applicable. Use asynchronous context manager instead."""
        raise excp.InvalidActionError(
            'Asynchronous context manager should be used instead.'
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Not applicable. Use asynchronous context manager instead."""
        pass

    async def __aenter__(self):
        """Run the node on context manager entry."""
        await self.run_node()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Shut down the node on context manager exit."""
        self.exit_node()
        try:
            await self._serv_task
        finally:
            self._serv_task = None

    def uid(self):
        """Get the node's unique id string."""
        return self._uid

    def nid(self):
        """Get the node's network id string."""
        return self._nid

    def check_alive(self):
        """Get a boolean indicating if the node is running."""
        return self._state is _NodeState.RUNNING

    def listening_sockets(self):
        """Get a list of the node's listening server sockets."""
        return self._socks.copy()

    def active_peers(self):
        """Get a list of network id strings for each active peer."""
        return [str(p) for p in self._act_set]

    def passive_peers(self):
        """Get a list of network id strings for each passive peer."""
        return [str(p) for p in self._pas_set]

    def num_active(self):
        """Get the number of peers in the active set."""
        return len(self._act_set)

    def num_passive(self):
        """Get the number of peers in the passive set."""
        return len(self._pas_set)

    def attach_broadcast_callback(self, bcast_cb):
        """Attach a broadcast callback.

        The function will be passed the network id of the message origin (str),
        and the message itself (str). The function should not block
        extensively. Asynchrounous callbacks are scheduled as tasks.
        """
        self._bcast_cb = bcast_cb

    def attach_direct_message_callback(self, dmsg_cb):
        """Attach a broadcast callback.

        The function will be passed the network id of the message origin (str),
        and the message itself (str). The function should not block
        extensively. Asynchrounous callbacks are scheduled as tasks.
        """
        self._dmsg_cb = dmsg_cb

    def exit_node(self):
        """Schedule the node's shutdown."""
        if self._state is not _NodeState.STOPPED:
            self._state = _NodeState.EXITING
            self._start_event.clear()

    async def run_node(self):
        """Initialise the node and run the listening server."""
        if self._state is not _NodeState.STOPPED:
            raise excp.InvalidActionError(
                'Attempted to initialise a node that is already running.'
            )

        self._state = _NodeState.STARTING
        self._stop_event.clear()
        self._serv_task = None
        self._log.write('starting node...\n')
        self._log.write(f' => uid: {self._uid}\n')
        self._log.write(' => starting listening server...\n')

        # run the listening server
        try:
            serv = await asyncio.start_server(
                self._conn_cb, host=self._host, port=self._port,
                start_serving=False
            )
        except Exception as e:
            raise excp.ServerError('Failed to start listening server.') from e

        # get the opened sockets
        self._socks = [s.getsockname() for s in serv.sockets]
        self._log.write(f'  => sockets: {self._socks}\n')

        # get the hostname and port number for the network id
        if not self._set_host and not self._set_port:
            self._host, self._port = self._socks[0][:2]
        else:
            for sock in self._socks:
                if not self._set_host and sock[1] == self._port:
                    self._host = sock[0]
                    break
                elif not self._set_port and sock[0] == self._host:
                    self._port = sock[1]
                    break

        # set the network id
        self._nid = f'{self._uid}@{self._host}:{self._port}'
        self._log.write(f'  => network id: {self._nid}\n')

        if self._state is _NodeState.STARTING:
            # run the listening server loop and finalise
            self._state = _NodeState.RUNNING
            self._serv_task = asyncio.create_task(self._server_coro(serv))
            self._log.write(' => node running\n')
            self._start_event.set()
        else:
            # node was exited early, clean up
            self._log.write(' => node exited while starting, aborting\n')
            self._state = _NodeState.STOPPED

            serv.close()
            await serv.wait_closed()
            self._socks.clear()

            if not self._set_host:
                self._host = None
            if not self._set_port:
                self._port = 0
            self._nid = ''

            self._start_event.set()
            self._start_event.clear()
            self._stop_event.set()

    async def wait_running(self):
        """Wait until the node is running.

        Returns
        -------
        boolean
            True if the server is running, False otherwise.
        """
        await self._start_event.wait()
        return self._state is _NodeState.RUNNING

    async def wait_stopped(self):
        """Wait until the node has stopped running."""
        await self._stop_event.wait()

    async def join_network(self, nid):
        """Join a network.

        Parameters
        ----------
        nid : str
            The network id of the network node to join upon.
        """
        uid, host, port = util.unpack_nid(nid)
        if uid == self._uid:
            raise excp.InvalidActionError('Cannot join upon self.')

        if self._state is not _NodeState.RUNNING:
            raise excp.InvalidActionError(
                'Cannot join network before node is running.'
            )

        # acquire join lock in case node exits or multiple joins attempted
        async with self._join_lock:
            if len(self._act_set) > 0 or len(self._pas_set) > 0:
                raise excp.InvalidActionError(
                    'Cannot join network whilst already connected to peers.'
                )

            self._log.write(f'initiating connection to {nid}...\n')
            peer, _ = await conn.init_connection(
                self._nid, uid, host, port, consts.JOIN_REQ, (consts.JOIN_SUC)
            )
            self._add_active(peer)
            self._log.write(' => connected\n')

    async def send_message(self, nid, msg):
        """Send a direct message to a node.

        Parameters
        ----------
        nid : str
            The network id of the node to message.
        msg : str
            The message to send. A newline will be appended to the end of the
            string.
        """
        # grab a read lock in case node shuts down while sending
        async with self._msg_job_lock.reader():
            if self._state is not _NodeState.RUNNING:
                raise excp.InvalidActionError('Node is not running.')

            _, host, port = util.unpack_nid(nid)
            await conn.send_message(
                host, port, f'{consts.D_MSG} {self._nid} {repr(str(msg))}'
            )

    async def send_broadcast(self, msg):
        """Broadcast a message."""
        # grab a read lock in case node shuts down while sending
        async with self._msg_job_lock.reader():
            if self._state is not _NodeState.RUNNING:
                raise excp.InvalidActionError('Node is not running.')

            # generate message uuid and add to seen queue
            self._log.write('generating message...\n')
            await asyncio.sleep(0.2)
            msg_uid = uuid.uuid1().hex
            self._log.write(f' => uid: {msg_uid}\n')
            self._bcast_queue.append(msg_uid)

            # format message and send to peers
            await self._message_peers(' '.join(map(str, [
                consts.B_MSG, self._nid, int(time.time()), msg_uid,
                repr(str(msg))
            ])))
            self._log.write(' => broadcast sent\n')

    def _add_passive(self, new_peer):
        """Add a peer to the passive set.

        Parameters
        ----------
        new_peer : hyparview.peer.Peer object
        """
        if new_peer not in self._pas_set:
            # discard a random passive peer if the set is full
            if len(self._pas_set) >= self._max_pas:
                discarded = util.sample_set(
                    self._pas_set, len(self._pas_set) - self._max_pas + 1, True
                )
                self._log.write(
                    f'discarded passives: [{", ".join(map(str, discarded))}]\n'
                )

            self._pas_set.add(new_peer)
            self._log.write(f'new passive: {new_peer}\n')

    def _add_active(self, new_peer):
        """Add a peer to the active set.

        Parameters
        ----------
        new_peer : hyparview.peer.ActivePeer object
        """
        # kill random active peers if max set size exceeded
        peers = []
        if len(self._act_set) >= self._max_act:
            peers = util.sample_set(
                self._act_set, len(self._act_set) - self._max_act + 1, True
            )
            self._log.write('pruning peers:\n')
            for p in peers:
                self._log.write(f' => {p}\n')
                p.kill(consts.DISC)
                self._dead_list.append(p)
                self._add_passive(p.to_passive())

        # activate peer and add to set
        new_peer.activate(*self._peer_callbacks)
        self._act_set.add(new_peer)

    def _proc_shu_rep(self, recv_nids):
        """Process a shuffle reply message.

        Parameters
        ----------
        recv_nids : list of str
            The received list of network ids.
        """
        # integrate the received nodes and clear the list of sent passive peers
        self._integrate_shu(recv_nids, self._shu_pas_sent)
        self._shu_pas_sent = None

    def _integrate_shu(self, recv_nids, sent_peers):
        """Integrate peers received from shuffle into the passive set.

        Parameters
        ----------
        recv_nids : list of str
            The received list of network ids.
        sent_peers : list of hyparview.peer.Peer objects
            List of passive peers sent in the original shuffle message or the
            response.
        """
        discard_index = 0
        for r_nid in recv_nids:
            # generate a passive peer instance
            r_uid, r_host, r_port = util.unpack_nid(r_nid)
            if not r_uid or r_uid == self._uid:
                continue
            r_peer = Peer(r_uid, r_host, r_port)

            # add to passive set if not already in a set
            if (
                r_peer not in self._act_set and r_peer not in self._pas_set
            ):
                # prioritise removing sent peers from passive set if full
                while (
                    sent_peers and len(self._pas_set) >= self._max_pas and
                    discard_index < len(sent_peers)
                ):
                    self._pas_set.discard(sent_peers[discard_index])
                    self._log.write(
                        f'discarded passive: {sent_peers[discard_index]}\n'
                    )
                    discard_index += 1

                self._add_passive(r_peer)
                self._log.write(f'integrated passive: {r_peer}\n')

    async def _server_coro(self, serv):
        """Run the listening server.

        Parameters
        ----------
        serv : asyncio.Server object
            The server instance created when starting the node.
        """
        try:
            async with serv:
                await serv.start_serving()
                last_shu = int(time.time())
                while self._state is _NodeState.RUNNING:
                    # promote passive peers if necessary
                    await self._replace_dead()

                    # do shuffle if enough time has passed since last shuffle
                    if int(time.time()) - last_shu >= consts.SHU_TIME:
                        await self._do_shuffle()
                        last_shu = int(time.time())

                    # cleanup dead nodes
                    if len(self._dead_list) > 0:
                        temp_list = self._dead_list.copy()
                        self._dead_list.clear()
                        await asyncio.gather(
                            *[p.wait_killed() for p in temp_list]
                        )

                    await asyncio.sleep(consts.SERV_SLEEP)
        finally:
            # wait for pending connection, join, and message jobs to finish
            async with self._listen_cb_lock.writer():
                async with self._join_lock:
                    async with self._msg_job_lock.writer():
                        # kill all peers
                        for p in self._act_set:
                            p.kill(consts.EXIT)
                            self._dead_list.append(p)
                        self._act_set.clear()
                        self._pas_set.clear()

            try:
                await asyncio.gather(
                    *[p.wait_killed() for p in self._dead_list]
                )
            finally:
                # reset attributes
                self._dead_list.clear()
                self._num_dead = 0
                self._shu_pas_sent = None
                self._bcast_queue.clear()

                self._socks.clear()
                if not self._set_host:
                    self._host = None
                if not self._set_port:
                    self._port = 0
                self._nid = ''

                self._state = _NodeState.STOPPED
                self._stop_event.set()

    async def _conn_cb(self, reader, writer):
        """Process new clients connecting to server.

        Parameters
        ----------
        reader : asyncio.StreamReader
            The client's associated read stream.
        writer : asyncio.StreamWriter
            The client's associated write stream.
        """
        # grab read lock in case node shuts down while handling the client
        async with self._listen_cb_lock.reader():
            # process the new connection
            try:
                new_peer, code, msg = await conn.proc_new_connection(
                    reader, writer, (
                        consts.JOIN_REQ, consts.NB_LOW, consts.NB_HIG,
                        consts.SHU_REP, consts.D_MSG, consts.PING
                    )
                )
            except Exception as e:
                host, port = writer.get_extra_info('peername')[:2]
                self._log.write((
                    '** failed to process connection: '
                    f'{host}:{port}\n => {e}\n'
                ))
                return

            # handle the request appropriately
            if code == consts.JOIN_REQ:
                await self._proc_join_req(new_peer)
            elif code in (consts.NB_HIG, consts.NB_LOW):
                await self._proc_nb_req(new_peer, code)
            else:
                if code == consts.SHU_REP:
                    self._proc_shu_rep(msg.split(' '))
                elif code == consts.D_MSG and self._dmsg_cb:
                    util.run_callback(
                        self._dmsg_cb, str(new_peer), ast.literal_eval(msg)
                    )

                # dont need peer if shuffle reply, dmsg, or ping
                new_peer.kill()
                self._dead_list.append(new_peer)

    async def _proc_join_req(self, new_peer):
        """Process a join request.

        Parameters
        ----------
        new_peer : hyparview.peer.ActivePeer object
            The peer that sent the request.
        """
        # add to active and send success message
        await new_peer.send_message(f'{consts.JOIN_SUC} {self._nid}')
        self._add_active(new_peer)

        # forward the join
        fwd_msg = f'{consts.JOIN_FOR} {new_peer} {consts.ACT_WALK}'
        await self._message_peers(fwd_msg, {new_peer})

        self._log.write(f'new peer joined {new_peer}\n')

    async def _proc_nb_req(self, new_peer, code):
        """Process a neighbour request.

        Parameters
        ----------
        new_peer : hyparview.peer.ActivePeer object
            The peer that sent the request.
        code : str
            The request priority.
        """
        # add to active if there's space in active set or request is high
        # priority
        # if nodes send requests to each other (new peer is the promote
        # target), the node with the lower uid will accept the request.
        if (
            new_peer not in self._act_set and (
                code == consts.NB_HIG or
                (code == consts.NB_LOW and len(self._act_set) < self._max_act)
            ) and (
                new_peer != self._promote_target or new_peer.uid() > self._uid
            )
        ):
            self._pas_set.discard(new_peer)
            await new_peer.send_message(f'{consts.NB_ACC} {self._nid}')
            self._add_active(new_peer)
            self._log.write(f'neighbour accepted: {new_peer}\n')
        else:
            self._log.write(f'neighbour rejected: {new_peer}\n')
            new_peer.kill(f'{consts.NB_REJ} {self._nid}')
            self._dead_list.append(new_peer)

    async def _proc_join_for(self, sender_peer, target_nid, ttl):
        """Process a join forward message.

        Parameters
        ----------
        sender_peer : hyparview.peer.ActivePeer object
            The peer that sent the message.
        target_nid : str
            The join request target's network id.
        ttl : int
            The message's time to live.
        """
        # generate a peer object for the target
        t_uid, t_host, t_port = util.unpack_nid(target_nid)
        if not t_uid:
            return
        t_peer = Peer(t_uid, t_host, t_port)

        # reduce the ttl unless self is target and ttl is 0
        next_ttl = ttl - 1 if not (t_uid == self._uid and ttl == 0) else ttl

        if (
            self._uid != t_uid and t_peer not in self._act_set and
            t_peer != self._promote_target
        ):
            # target is not self, not active, and not involved in neighbour
            # request - add to active or passive if appropriate
            if ttl == 0 or len(self._act_set) < 2:
                self._pas_set.discard(t_peer)
                await self._promote_peer(t_peer, consts.NB_HIG)
                return
            elif (ttl == consts.PAS_WALK and t_peer not in self._pas_set):
                self._add_passive(t_peer)

        # forward message if ttl not expired
        if next_ttl >= 0:
            fwd_msg = f'{consts.JOIN_FOR} {target_nid} {next_ttl}'
            await self._message_random(fwd_msg, 1, {sender_peer})

    async def _proc_shu_mes(self, sender, target_nid, ttl, peers_str):
        """Process a shuffle request message.

        Parameters
        ----------
        sender : hyparvier.peer.ActivePeer object
            The peer that sent the message
        target_nid : str
            The shuffle target's network id.
        ttl : int
            The message's time to live.
        peers_str : str
            The shuffled peers as a list of space separated network ids.
        """
        t_uid, t_host, t_port = util.unpack_nid(target_nid)
        if not t_uid:
            return

        # reduce the ttl unless self is target and ttl is at 0
        next_ttl = ttl - 1 if not (t_uid == self._uid and ttl == 0) else ttl

        if t_uid != self._uid and (ttl == 0 or len(self._act_set) < 2):
            # self is not target and ttl is 0 or only 1 active peer - integrate
            # received peers and send shuffle reply
            act_shu_list = util.sample_set(self._act_set, consts.ACT_SHU)
            pas_shu_list = await self._generate_pas_shu_list()
            shu_msg = '{0} {1} {2}'.format(
                consts.SHU_REP, self._nid,
                ' '.join(map(str, act_shu_list + pas_shu_list)),
            )

            if peers_str:
                self._integrate_shu(peers_str.split(' '), pas_shu_list)

            try:
                await conn.send_message(t_host, t_port, shu_msg)
            except excp.TimeoutConnectError as e:
                self._log.write((
                    f'** failed to send shuffle reply to {target_nid}\n'
                    f' => {e}\n'
                ))
                t_peer = Peer(t_uid, t_host, t_port)
                if t_peer in self._pas_set:
                    self._pas_set.discard(t_peer)
        elif next_ttl >= 0:
            # ttl not expired - forward message to a random peer
            shu_msg = f'{consts.SHU_MES} {target_nid} {next_ttl} {peers_str}'
            await self._message_random(shu_msg, 1, {sender})

    async def _proc_bcast(
        self, sender_peer, origin_nid, msg_ts, msg_uid, msg
    ):
        """Process a broadcast message.

        Parameters
        ----------
        sender_peer : hyparview.peer.ActivePeer object
            The message sender.
        origin_nid : str
            Network id of the broadcast origin.
        msg_ts : int
            The message's timestamp in seconds.
        msg_uid : str
            The unique id of the message.
        msg : str
            The message itself.
        """
        # only process message if not in seen queue
        if msg_uid not in self._bcast_queue:
            # add to seen queue and run callback
            self._bcast_queue.append(msg_uid)
            if self._bcast_cb:
                util.run_callback(
                    self._bcast_cb, origin_nid, ast.literal_eval(msg)
                )

            # forward broadcast if message has not timed out
            now = int(time.time())
            if now >= msg_ts and now - msg_ts < consts.MSG_TIMEOUT:
                await self._message_peers(' '.join(map(str, [
                    consts.B_MSG, origin_nid, msg_ts, msg_uid, msg
                ])), {sender_peer})

    async def _proc_dead(self, peer, exit_code):
        """Process a peer that has disconnected or unexpectedly died.

        Parameters
        ----------
            peer : hyparview.peer.ActivePeer object
                The peer that has died.
            exit_code : str
                The peer's exit code.
        """
        # remove from active set, increment number of dead nodes, and add to
        # passive if peer is still connected to network
        self._act_set.discard(peer)
        self._num_dead += 1
        if exit_code == consts.DISC:
            self._add_passive(peer.to_passive())
            self._log.write(f'peer disconnected: {peer}\n')
        elif exit_code == consts.EXIT:
            self._log.write(f'peer left network: {peer}\n')
        else:
            self._log.write(
                f'** peer unexpectedly died: {peer}, code: {exit_code}\n'
            )

        # schedule cleanup and add to dead list
        peer.kill()
        self._dead_list.append(peer)

    async def _promote_peer(self, peer, req_code=None):
        """Perform a neighbour request.

        Parameters
        ----------
        peer : hyparview.peer.Peer object
            The passive peer instance.
        req_code : str, optional
            The neighbour request code. If not supplied, priority will be
            determined by active set size.

        Returns
        -------
        boolean
            True if the promotion succeeded, False otherwise.
        """
        # generate req code if needed
        if not req_code:
            req_code = consts.NB_HIG if len(self._act_set) == 0 \
                else consts.NB_LOW

        success = False
        await self._promote_lock.acquire()
        try:
            if peer not in self._act_set:
                # initiate a connection
                self._promote_target = peer
                try:
                    new_active, code = await conn.init_connection(
                        self._nid, peer.uid(), peer.host(), peer.port(),
                        req_code, (consts.NB_ACC, consts.NB_REJ)
                    )
                except (excp.ConnectError) as e:
                    self._log.write(f'** failed to promote {peer}\n => {e}\n')
                    new_active = None

                if new_active and code == consts.NB_ACC:
                    # neighbour request accepted - remove from passive set and
                    # add to active set
                    self._pas_set.discard(peer)
                    self._add_active(new_active)
                    self._log.write(f'promoted peer {peer}\n')
                    success = True
                elif new_active:
                    # neighbour request denied - cleanup resources
                    new_active.kill()
                    self._dead_list.append(new_active)
                else:
                    # connection error - remove from passive set
                    self._pas_set.discard(peer)
        finally:
            self._promote_lock.release()
            self._promote_target = None

        return success

    async def _do_shuffle(self):
        """Perform a round of shuffling."""
        if not self._shu_pas_sent or len(self._act_set) > 0:
            # generate shuffle lists and send message to a random peer
            act_shu_list = util.sample_set(self._act_set, consts.ACT_SHU)
            pas_shu_list = await self._generate_pas_shu_list()
            self._shu_pas_sent = pas_shu_list
            shu_msg = '{0} {1} {2} {3}'.format(
                consts.SHU_MES, self._nid, consts.SHU_WALK,
                ' '.join(map(str, act_shu_list + pas_shu_list))
            )
            await self._message_random(shu_msg, 1)

    async def _generate_pas_shu_list(self):
        """Generate a list of passive peers and ensure they are alive.

        Returns
        -------
        list of hyparview.peer.Peer objects
        """
        pas_shu_list = []
        while (
            len(pas_shu_list) < consts.PAS_SHU and
            len(pas_shu_list) < len(self._pas_set)
        ):
            # generate a list of candidates to fill remaining slots
            tentative_list = util.sample_set(
                self._pas_set, consts.PAS_SHU - len(pas_shu_list)
            )

            # ping every candidate and discard dead peers
            for peer in tentative_list:
                try:
                    await conn.send_message(
                        peer.host(), peer.port(), f'{consts.PING} {self._nid}'
                    )
                    pas_shu_list.append(peer)
                except excp.ConnectError:
                    self._pas_set.discard(peer)

        return pas_shu_list

    async def _replace_dead(self):
        """Replace dead active peers with passive peers."""
        # promote passive peers if peers have died recently or there are no
        # active peers
        if (
            (self._num_dead > 0 or len(self._act_set) == 0) and
            len(self._act_set) < self._max_act and len(self._pas_set) > 0
        ):
            peer_set = self._pas_set.copy()
            while (
                len(peer_set) > 0 and self._num_dead > 0 and
                len(self._act_set) < self._max_act
            ):
                peers = util.sample_set(peer_set, self._num_dead, True)
                for p in peers:
                    res = await self._promote_peer(p)
                    if res and self._num_dead > 0:
                        self._num_dead -= 1

    async def _message_peers(self, msg, exclude_set=None):
        """Send a message to all active peers.

        Parameters
        ----------
        msg : str
            The message to send. A newline will be appended.
        exclude_set : list of hyparview.peer.ActivePeer objects, optional
            List of peers to avoid messaging.
        """
        await self._message_random(msg, len(self._act_set), exclude_set)

    async def _message_random(self, msg, num, exclude_set=None):
        """Send a message to a random set of active peers.

        Parameters
        ----------
        msg : str
            The message to send. A newline will be appended.
        num : int
            The number of peers to send the message to.
        exclude_set : list of hyparview.peer.ActivePeer objects, optional
            List of peers to avoid messaging.
        """
        peer_set = self._act_set.copy()
        if exclude_set:
            peer_set.difference_update(exclude_set)
        sent_peers = 0
        failed_peers = []
        while sent_peers < num and len(peer_set) > 0:
            # get a random selection of peers and send message
            peers = util.sample_set(peer_set, num, True)
            res = await asyncio.gather(
                *[p.send_message(msg) for p in peers], return_exceptions=True
            )

            # count number of successful sends and track failed peers
            for p, r in zip(peers, res):
                if not r:
                    sent_peers += 1
                else:
                    failed_peers.append((p, r))
                    self._num_dead += 1

        # kill and remove failed peers from active set
        if len(failed_peers) > 0:
            self._log.write('** failed to send message to:\n')
        for p, r in failed_peers:
            self._log.write(f' => {p}, {r}\n')
            self._act_set.discard(p)
            p.kill()
            self._dead_list.append(p)


class _NodeState(enum.Enum):
    """Enumeration of node states."""

    STOPPED = enum.auto()
    STARTING = enum.auto()
    RUNNING = enum.auto()
    EXITING = enum.auto()
