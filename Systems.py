"""
Two distributed systems built on top of the exact same nodes/network.

ConsistentSystem (CP)
    On a partition, it refuses to serve a request rather than risk
    returning/storing a value that isn't agreed on by a majority of
    nodes ("quorum"). It picks Consistency, sacrifices Availability.

AvailableSystem (AP)
    On a partition, it always answers using whatever node it can reach,
    even if that node is cut off from the rest of the cluster and may
    hold stale or divergent data. It picks Availability, sacrifices
    Consistency.

Both systems talk to the coordinator node the client happens to contact,
then try to reach the other nodes through the shared Network object.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Result:
    success: bool
    value: Optional[object] = None
    acknowledged: list = field(default_factory=list)
    unreachable: list = field(default_factory=list)
    message: str = ""


class DistributedSystem:
    """Shared system that both consistent system and available system will use"""

    def __init__(self, nodes, name):
        self.nodes = {n.node_id: n for n in nodes}
        self.name = name

    # This function checks if a given node is able to connect with other nodes. 
    # If it is able to connect with other nodes, then it is placed in a reachable
    # bucket. If not its placed in a unreachable bucket
    def _reachable_from(self, coordinator_id):
        coordinator = self.nodes[coordinator_id]
        reachable, unreachable = [], []
        for node_id, node in self.nodes.items():
            if coordinator.network.connected(coordinator_id, node_id):
                reachable.append(node)
            else:
                unreachable.append(node)
        return reachable, unreachable


class ConsistentSystem(DistributedSystem):
    """CP: requires a quorum of nodes before it will accept a write or
    trust a read. During a partition, the minority side becomes
    completely unavailable rather than risk inconsistency."""

    def __init__(self, nodes):
        super().__init__(nodes, "CP System (Consistency-first)")
        # Have a quorum to decide what the amount of nodes needed for a system to run
        self.quorum = len(nodes) // 2 + 1

    async def write(self, coordinator_id, key, value):
        reachable, unreachable = self._reachable_from(coordinator_id)

        if len(reachable) < self.quorum:
            return Result(
                success=False,
                acknowledged=[],
                unreachable=[n.node_id for n in unreachable],
                message=(
                    f"REJECTED - only {len(reachable)}/{len(self.nodes)} nodes "
                    f"reachable, need a quorum of {self.quorum}. Refusing to "
                    f"write so we never return a value the rest of the cluster "
                    f"disagrees with."
                ),
            )

        for node in reachable:
            await node.local_write(key, value)

        return Result(
            success=True,
            value=value,
            acknowledged=[n.node_id for n in reachable],
            unreachable=[n.node_id for n in unreachable],
            message=f"OK - quorum of {len(reachable)}/{len(self.nodes)} nodes acknowledged the write.",
        )

    async def read(self, coordinator_id, key):
        reachable, unreachable = self._reachable_from(coordinator_id)

        if len(reachable) < self.quorum:
            return Result(
                success=False,
                unreachable=[n.node_id for n in unreachable],
                message=(
                    f"REJECTED - only {len(reachable)}/{len(self.nodes)} nodes "
                    f"reachable, need a quorum of {self.quorum}. Refusing to "
                    f"read a possibly-stale value."
                ),
            )

        values = [await node.local_read(key) for node in reachable]
        return Result(
            success=True,
            value=values[0],
            acknowledged=[n.node_id for n in reachable],
            unreachable=[n.node_id for n in unreachable],
            message=f"OK - value confirmed by quorum of {len(reachable)}/{len(self.nodes)} nodes.",
        )


class AvailableSystem(DistributedSystem):
    """AP: always serves the request using whatever node the client
    reached, and best-effort replicates to whoever else it can see.
    During a partition every side keeps working, but the sides can
    drift out of sync with each other."""

    def __init__(self, nodes):
        super().__init__(nodes, "AP System (Availability-first)")

    async def write(self, coordinator_id, key, value):
        coordinator = self.nodes[coordinator_id]
        reachable, unreachable = self._reachable_from(coordinator_id)

        await coordinator.local_write(key, value)
        for node in reachable:
            if node.node_id != coordinator_id:
                await node.local_write(key, value)

        msg = f"OK - accepted immediately by node {coordinator_id}"
        if unreachable:
            msg += (
                f" and replicated to {[n.node_id for n in reachable if n.node_id != coordinator_id]}. "
                f"Could NOT reach {[n.node_id for n in unreachable]} - those nodes are now stale."
            )
        else:
            msg += " and replicated to all nodes."

        return Result(
            success=True,
            value=value,
            acknowledged=[n.node_id for n in reachable],
            unreachable=[n.node_id for n in unreachable],
            message=msg,
        )

    async def read(self, coordinator_id, key):
        coordinator = self.nodes[coordinator_id]
        value = await coordinator.local_read(key)
        return Result(
            success=True,
            value=value,
            acknowledged=[coordinator_id],
            message=f"OK - answered instantly from node {coordinator_id} (not verified against other nodes).",
        )