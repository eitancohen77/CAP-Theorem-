import asyncio
import random

class Node:
    # We share the network because we would have 2 different system and network: 
    # Availability system and Consistency System
    def __init__(self, node_id, network):
        self.node_id = node_id
        self.network = network #
        self.store = {}

    # Does a very simple key value storage that allows read and writing operations
    # We also use asyncio.sleep in order to replicate a request taking time to be completed.
    async def local_write(self, key, value):
        await asyncio.sleep(random.uniform(0.01, 0.05))
        self.store[key] = value

    async def local_read(self, key):
        await asyncio.sleep(random.uniform(0.01, 0.05))
        return self.store[key]

    def __repr__(self):
        return f"Node({self.node_id}, data={self.store})"