hyparviewpy
-----------
An asynchronous Python implementation of the HyParView P2P network protocol
(https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf).

Example
-------
```Python
import asyncio
from hyparviewpy import Node

async def fun():
    async with Node() as n1:
        n1.attach_broadcast_callback(broadcast_cb)
        async with Node() as n2:
            n2.attach_broadcast_callback(broadcast_cb)

            # alternative to context manager
            n3 = Node()
            await n3.run_node()

            await n2.join_network(n1.nid())
            await n3.join_network(n2.nid())

            await n3.send_broadcast('hello world')
            n3.exit_node()
            await n3.wait_stopped()

def broadcast_cb(sender, msg):
    print(f'[BCAST - {sender}]: {msg}')

asyncio.run(fun())
```

See [examples/simple_cli.py](examples/simple_cli.py) for a further example of
usage.

API Reference
-------------
Available methods can be displayed with `python -m pydoc hyparviewpy`

References
----------
J. Leitao, J. Pereira and L. Rodrigues, "HyParView: A Membership Protocol for
Reliable Gossip-Based Broadcast," 37th Annual IEEE/IFIP International
Conference on Dependable Systems and Networks (DSN'07), Edinburgh, 2007, pp.
419-429, doi: 10.1109/DSN.2007.56.
