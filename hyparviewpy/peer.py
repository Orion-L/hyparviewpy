"""Encapsulates a network peer."""

import asyncio

from . import connect as conn
from . import constants as consts
from . import exceptions as excp
from . import util


class Peer:
    """Represents a passive peer.

    Parameters
    ----------
    uid : str
        The peer's unique id.
    host : str
        The peer's listening server hostname.
    port : int
        The peer's listening server port number.
    """

    def __init__(self, uid, host, port):
        self._uid = uid
        self._host = host
        self._port = port

    def __str__(self):
        """Get a representation of the form '[uid]@[hostname]:[port]'."""
        return f'{self._uid}@{self._host}:{self._port}'

    def __hash__(self):
        """Hash the string representation."""
        return hash(str(self))

    def __eq__(self, other):
        """Check for equality.

        Two peers are considered equal if they share the same unique id,
        hostname and port number.
        """
        return (
            isinstance(other, Peer) and
            self._uid == other._uid and
            self._host == other._host and
            self._port == other._port
        )

    def uid(self):
        """Get the peer's uid."""
        return self._uid

    def host(self):
        """Get the peer's hostname."""
        return self._host

    def port(self):
        """Get the peer's port number."""
        return self._port

    def to_active(self, reader, writer):
        """Convert the peer to an ActivePeer instance.

        Parameters
        ----------
        reader : asyncio.StreamReader object
            The stream reader that is associated with the active connection to
            the peer.
        writer : asyncio.StreamWriter object
            The stream writer that is associated with the active connection to
            the peer.

        Returns
        -------
        ActivePeer object
            An ActivePeer instance that encapsulates the peer.
        """
        return ActivePeer(self._uid, self._host, self._port, reader, writer)


class ActivePeer(Peer):
    """Represents an active peer and manages communication with the peer.

    Parameters
    ----------
    uid : str
        The peer's unique id.
    host : str
        The peer's listening server hostname.
    port : int
        The peer's listening server port number.
    reader : asyncio.StreamReader object
        The stream reader associated with the connection to the peer.
    writer : asyncio.StreamWriter object
        The stream writer associated with the connection to the peer.
    """

    def __init__(self, uid, host, port, reader, writer):
        super().__init__(uid, host, port)
        self._reader = reader
        self._writer = writer
        self._active = False
        self._is_exiting = False
        self._exit_task = None
        self._loop_task = None
        self._read_task = None

    def __str__(self):
        """Inherited from Peer."""
        return super().__str__()

    def __hash__(self):
        """Inherited from Peer."""
        return super().__hash__()

    def __eq__(self, other):
        """Inherited from Peer."""
        return super().__eq__(other)

    def to_passive(self):
        """Get a corresponding passive peer instance."""
        return Peer(self._uid, self._host, self._port)

    def activate(self, fwd_cb, shu_cb, bcast_cb, dead_cb):
        """Run the peer's listening loop.

        Parameters
        ----------
        fwd_cb : callable
            A callback that is run when a forward join message is received. The
            function will be passed the caller's instance (ActivePeer),
            the target's network id (str), and the message's time to live
            (int).
        shu_cb : callable
            A callback that is run when a shuffle message is received. The
            function will be passed the caller's instance, (ActivePeer), the
            target's network id (str), the message's to live (int), and the
            shuffled peers' network ids (str).
        bcast_cb : callable
            A callback that is run when a broadcast message is received. The
            function will be passed the caller's instance, (ActivePeer), the
            origin's network id (str), the message's timestamp in seconds
            (int), the message's unique id (str), and the message itself (str).
        dead_cb : callable
            A callback that is run when the peer disconnects or exits
            unexpectedly. The function will be passed the caller's instance
            (ActivePeer) and an error code (str).

        Raises
        ------
        hyparviewpy.exceptions.InvalidActionError
            Raised if the instance is in an invalid state.
        """
        if self._active:
            raise excp.InvalidActionError('Peer is already active.')
        if self._is_exiting:
            raise excp.InvalidActionError('Peer is still exiting.')
        if not (self._reader and self._writer):
            raise excp.InvalidActionError('Asyncio streams are not set.')

        self._fwd_cb = fwd_cb
        self._shu_cb = shu_cb
        self._bcast_cb = bcast_cb
        self._dead_cb = dead_cb

        self._active = True
        self._is_exiting = False
        self._exit_task = None
        self._loop_task = asyncio.create_task(self._recv_loop())
        self._read_task = None

    async def send_message(self, msg):
        """Send a message to the peer.

        Parameters
        ----------
        msg : str
            The message to send. A newline is appended to the end of the
            string.
        """
        if not self._writer:
            raise excp.InvalidActionError('Writer is not set.')
        self._writer.write(f'{msg}\n'.encode())
        await self._writer.drain()

    def kill(self, msg=None):
        """Schedule the peer's cleanup task.

        Parameters
        ----------
        msg : str, optional
            Message to send before terminating the connection.
        """
        if not self._is_exiting:
            self._active = False
            self._is_exiting = True
            self._exit_task = asyncio.create_task(self._cleanup(msg))

    async def wait_killed(self):
        """Await on the peer's cleanup task."""
        if self._exit_task:
            try:
                await self._exit_task
            finally:
                self._exit_task = None

    async def _recv_loop(self):
        """Continuously receive data and run the appropriate callback."""
        err = None
        while self._active:
            # start a task to read from the stream
            try:
                self._read_task = asyncio.create_task(
                    conn.read_data(self._reader)
                )
                data = await self._read_task
            except excp.DataConnectError:
                err = consts.DATA_ERR
                break
            except asyncio.CancelledError:
                break
            finally:
                self._read_task = None

            # peer could be killed unexpectedly before task end
            if self._is_exiting:
                break

            code, msg = util.unpack_data(data)
            if code in (consts.JOIN_FOR, consts.SHU_MES, consts.B_MSG):
                # join forwards, shuffles, and broadcasts should contain a
                # message field
                if not msg:
                    err = consts.INVAL_ERR
                    break

                # extract the associated nid and time/ttl fields
                msg_list = msg.split(' ', 2)
                if (
                    (code != consts.JOIN_FOR and len(msg_list) < 3) or
                    len(msg_list) < 2
                ):
                    err = consts.INVAL_ERR
                    break
                msg_id = msg_list[0]
                try:
                    msg_time = int(msg_list[1])
                except ValueError:
                    err = consts.INVAL_ERR
                    break

                # invoke appropriate callback
                if code == consts.JOIN_FOR:
                    await util.run_callback(
                        self._fwd_cb, self, msg_id, msg_time
                    )
                elif code == consts.SHU_MES:
                    await util.run_callback(
                        self._shu_cb, self, msg_id, msg_time, msg_list[2]
                    )
                else:
                    # extract additional broadcast message id field
                    bcast_msg_list = msg_list[2].split(' ', 1)
                    if len(bcast_msg_list) < 2:
                        err = consts.INVAL_ERR
                        break
                    bcast_origin = msg_id
                    bcast_id = bcast_msg_list[0]

                    await util.run_callback(
                        self._bcast_cb, self, bcast_origin, msg_time, bcast_id,
                        bcast_msg_list[1]
                    )
            elif code == consts.PING:
                # ignore pings
                pass
            elif code in (consts.DISC, consts.EXIT):
                err = code
                break
            else:
                err = consts.INVAL_ERR
                break

        # peer dead - close writer, cleanup and invoke callback unless already
        # exiting
        self._active = False
        if not self._is_exiting:
            self._is_exiting = True
            try:
                if err in (consts.DISC, consts.EXIT):
                    await conn.close_writer(self._writer)
                else:
                    await conn.close_writer(self._writer, err)
                await util.run_callback(self._dead_cb, self, err)
            finally:
                self._writer = None
                self._reader = None
                self._is_exiting = False

    async def _cleanup(self, exit_msg):
        # cancel pending read task, wait for loop to exit, and close connection
        try:
            if self._read_task:
                self._read_task.cancel()
            if self._loop_task:
                await self._loop_task
        finally:
            await conn.close_writer(self._writer, exit_msg)
            self._loop_task = None
            self._writer = None
            self._reader = None
            self._is_exiting = False
