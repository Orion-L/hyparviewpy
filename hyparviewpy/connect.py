"""Manage connections between network nodes."""

import asyncio

from . import constants as consts
from . import exceptions as excp
from .peer import ActivePeer
from . import util


async def init_connection(own_nid, uid, host, port, code, expected_codes):
    """Initiate a new connection to a network peer.

    Parameters
    ----------
    own_nid : str
        The caller node's network id.
    uid : str
        The peer's unique id.
    host : str
        The peer's listening server hostname.
    port : int
        The peer's listening server port number.
    code : str
        The code that will be sent when initiating the connection.
    expected_codes : list of str
        The codes that the caller node is expecting to receive.

    Returns
    -------
    new_peer : hyparviewpy.peer.ActivePeer object
        An ActivePeer instance associated with the newly connected peer.
    code : str
        The received code.

    Raises
    ------
    hyparviewpy.exceptions.ConnectError
        Raises a subclass of ConnectError if the connection fails.
    """
    # setup the connection
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=consts.CONN_TIMEOUT
        )
    except Exception as e:
        raise excp.TimeoutConnectError(
            'Attempt to initiate connection to peer timed out.'
        ) from e

    # send the initial message
    writer.write(f'{code} {own_nid}\n'.encode())
    await writer.drain()

    # await response
    new_peer, code, _ = await proc_new_connection(
        reader, writer, expected_codes, uid
    )
    return new_peer, code


async def proc_new_connection(reader, writer, valid_codes, expected_uid=None):
    """Process a new connection and create an associated ActivePeer object.

    Parameters
    ----------
    reader : asyncio.StreamReader object
        The reader stream associated with the peer.
    writer : asyncio.StreamWriter object
        The writer stream associated with the peer.
    valid_codes : list of str
        The codes expected to be sent by the peer.
    expected_uid : str, optional
        The peer's expected unique id.

    Returns
    -------
    new_peer : hyparviewpy.peer.ActivePeer object
        An ActivePeer instance associated with the newly connected peer.
    code : str
        The code sent by the peer.
    msg : str
        The message sent by the peer.

    Raises
    ------
    hyparviewpy.exceptions.ConnectError
        Raises a subclass of ConnectError if the connection fails or is
        invalid.
    """
    # try to receive data
    try:
        data = await read_data(reader, read_timeout=consts.CONN_TIMEOUT)
    except excp.TimeoutConnectError:
        await close_writer(writer, consts.TIMEOUT_ERR)
        raise
    except excp.DataConnectError:
        await close_writer(writer, consts.DATA_ERR)
        raise

    # parse and check code
    code, msg_data = util.unpack_data(data)
    if code not in valid_codes:
        await close_writer(writer, consts.INVAL_ERR)
        raise excp.ResponseError((
            'New connection did not send an expected code.\n'
            f' => received code: {code}'
        ))

    # extract nid and message and check if uid is expected
    try:
        msg_list = msg_data.split(' ', 1)
    except Exception:
        await close_writer(writer, consts.INVAL_ERR)
        raise excp.ResponseError((
            'New connection did not send expected message data.\n'
            f' => received code: {code}\n'
            f' => received data: {msg_data}'
        ))

    uid, host, port = util.unpack_nid(msg_list[0])
    msg = '' if len(msg_list) != 2 else msg_list[1]
    if expected_uid:
        if uid != expected_uid:
            await close_writer(writer, consts.INVAL_ERR)
            raise excp.ResponseError((
                'New connection did not send an expected uid.\n'
                f' => received uid: {uid}, expected: {expected_uid}'
            ))

    return ActivePeer(uid, host, port, reader, writer), code, msg


async def read_data(reader, read_timeout=None):
    """Get a message string from a reader stream.

    Parameters
    ----------
    reader : asyncio.StreamReader object
        The stream to read from.
    read_timeout : int, optional
        Timeout in seconds.

    Returns
    -------
    str
        The received string. Leading and trailing whitespace is stripped.

    Raises
    ------
    hyparviewpy.exceptions.ConnectError
        Raises a subclass of ConnectError if reading timed out or failed.
    """
    # create a read task and wait for it
    try:
        data = await asyncio.wait_for(reader.readline(), timeout=read_timeout)
    except asyncio.TimeoutError:
        raise excp.TimeoutConnectError('Read from reader stream timed out.')

    # check the data and convert to a string
    if not data or data == b'':
        raise excp.DataConnectError('No data received from stream.')
    data_str = data.decode()
    if not data_str.endswith('\n'):
        raise excp.DataConnectError('Received data was truncated.')

    return data_str.strip()


async def send_message(host, port, msg):
    """Send a single message directly to a specified node.

    Parameters
    ----------
    host : str
        The node's listening server hostname.
    port : int
        The node's listening server port number.
    msg : str
        The message to send. A newline will be appended to the string.

    Raises
    ------
    hyparviewpy.exceptions.TimeoutConnectError
        If the attempted connection to the peer timed out.
    """
    # setup the connection
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=consts.CONN_TIMEOUT
        )
    except Exception as e:
        raise excp.TimeoutConnectError(
            'Message not sent. Attempt to connect to peer timed out.'
        ) from e

    # send the message and close the writer
    await close_writer(writer, msg)


async def close_writer(writer, msg=None):
    """Close a writer stream.

    Parameters
    ----------
    writer : asyncio.StreamWriter object
        The writer stream to close.
    msg : str, optional
        A message to send before closing the stream.
    """
    if writer and not writer.is_closing():
        try:
            if msg:
                writer.write(f'{msg}\n'.encode())
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
