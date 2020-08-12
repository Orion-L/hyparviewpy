#!/usr/bin/python

import argparse
import asyncio
import cmd
import time

import hyparviewpy


class Shell(cmd.Cmd):
    intro = (
        'Welcome to the HyParView test shell. '
        'Type help or ? for a list of commands.\n'
    )

    def __init__(self, node, loop):
        self._node = node
        self._loop = loop
        self._alive = True

        super().__init__()

    def do_info(self, args):
        """Print the node's information."""
        print('\n'.join([
            f'uid: {self._node.uid()}', f'nid: {self._node.nid()}',
            'sockets: [',
        ]))
        for s in self._node.listening_sockets():
            print(f'  {s}')
        print('\n'.join([
            ']', f'active peers: {self._node.num_active()}',
            f'passive peers: {self._node.num_passive()}'
        ]))

    def do_active_peers(self, args):
        """Print a list of active peers."""
        print('\n'.join(self._node.active_peers()))

    def do_passive_peers(self, args):
        """Print a list of passive peers."""
        print('\n'.join(self._node.passive_peers()))

    def do_join(self, args):
        args_list = args.split()
        if args == '' or (len(args_list) != 3 and len(args_list) != 1):
            print('** invalid number of arguments')
        else:
            if len(args_list) == 1:
                nid = args_list[0]
            else:
                nid = f'{args_list[0]}@{args_list[1]}:{args_list[2]}'

            fut = asyncio.run_coroutine_threadsafe(
                self._node.join_network(nid), self._loop
            )
            while not fut.done():
                time.sleep(0.5)
            if fut.exception():
                print(f'** failed to join: {fut.exception()}')

    def help_join(self):
        print((
            'Join a network.\n'
            'join [uid] [host] [port]\n'
            '  uid: The contact node\'s uid.\n'
            '  host: The host name of the contact node.\n'
            '  port: The port number of the contact node.\n'
            'join [nid]\n'
            '  nid: The contact node\'s network id.'
        ))

    def do_message(self, args):
        args_list = args.split(' ', 1)
        if args == '' or len(args_list) != 2:
            print('** invalid number of arguments')
        else:
            fut = asyncio.run_coroutine_threadsafe(
                self._node.send_message(args_list[0], args_list[1]), self._loop
            )
            while not fut.done():
                time.sleep(0.5)
            if fut.exception():
                print(f'** failed to send message: {fut.exception()}')

    def help_message(self):
        print((
            'Send a message to a node.\n'
            'message [nid] [message...]\n'
            '  nid: The node\'s network id.\n'
            '  message: The message string to send.'
        ))

    def do_broadcast(self, message):
        if message == '':
            print('** invalid number of arguments')
        else:
            fut = asyncio.run_coroutine_threadsafe(
                self._node.send_broadcast(message), self._loop
            )
            while not fut.done():
                time.sleep(0.5)
            if fut.exception():
                print(f'** failed to send broadcast: {fut.exception()}')

    def help_broadcast(self):
        print((
            'Send a broadcast.\n'
            'broadcast [message...]\n'
            '  message: The message string to send.'
        ))

    def do_EOF(self, args):
        """Exit."""
        return True

    def emptyline(self):
        pass

    def precmd(self, line):
        return line if self._alive else 'EOF'


def recv_msg(sender, msg):
    print(f'[{sender}]: {msg}')


def recv_bcast(sender, msg):
    print(f'[BCAST - {sender}]: {msg}')


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--host', type=str, help='Optional host name.'
    )
    parser.add_argument(
        '--port', type=int, help='Optional port number.'
    )
    return parser.parse_args()


async def main():
    args = setup_args()
    port = 0 if not args.port else args.port
    async with hyparviewpy.Node(
        host=args.host, port=port, debug=False
    ) as n:
        n.attach_direct_message_callback(recv_msg)
        n.attach_broadcast_callback(recv_bcast)
        loop = asyncio.get_running_loop()
        shell = Shell(n, loop)
        shell_fut = loop.run_in_executor(None, shell.cmdloop)
        node_task = asyncio.create_task(n.wait_stopped())
        _, pending = await asyncio.wait(
            {shell_fut, node_task}, return_when=asyncio.FIRST_COMPLETED
        )

        if shell_fut in pending:
            shell._alive = False
            await shell_fut
        if node_task.done():
            excp = node_task.exception()
            if excp is not None:
                raise excp


if __name__ == '__main__':
    asyncio.run(main())
