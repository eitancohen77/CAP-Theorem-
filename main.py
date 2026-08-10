import asyncio
from Network import Network
from Node import Node
from Systems import ConsistentSystem, AvailableSystem


"""
CAP Theorem Demo: Consistency vs Availability during a network partition.

Run it with:
    python3 demo.py

What happens:
  1. A 5-node cluster is created. Two systems are built on top of the
     SAME nodes/network: one CP (consistency-first), one AP
     (availability-first).
  2. Everything works normally -- both systems write/read fine.
  3. We simulate a network PARTITION: nodes split into a minority
     group {0, 1} and a majority group {2, 3, 4}.
  4. A client talks to node 0 (in the minority/cut-off side) and sends
     the SAME write to both systems at the same time.
       - CP refuses the write (can't reach a quorum) -> stays consistent,
         becomes unavailable.
       - AP accepts the write immediately on node 0 -> stays available,
         but node 0 is now out of sync with nodes 2/3/4.
  5. We read the same key from the majority side to prove the two
     systems diverged.
  6. The partition heals and we show the AP system's lingering
     inconsistency (nothing reconciles it automatically -- that's the
     point).
"""

def banner(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def show_result(system_name, op, result):
    status = "SUCCESS" if result.success else "FAILED"
    print(f"[{system_name}] {op} -> {status:<7} | {result.message} | Value read = {result.value}")


async def testSystems():
    node_ids = [0, 1, 2, 3, 4]
    
    network_cp = Network(node_ids)
    network_ap = Network(node_ids)

    cp_nodes = [Node(i, network_cp) for i in node_ids]
    ap_nodes = [Node(i, network_ap) for i in node_ids]


    # Two independent systems, that have their own nodes.
    cp = ConsistentSystem(cp_nodes)
    ap = AvailableSystem(ap_nodes)

    banner("STEP 1: Normal operation (no partition)")
    r_cp = await cp.write(coordinator_id=0, key="balance", value=100)
    r_ap = await ap.write(coordinator_id=0, key="balance", value=100)
    show_result("CP", "WRITE", r_cp)
    show_result("AP", "WRITE", r_ap)

    print("\nCONSISTENT PARTITION SYSTEM")
    for n in cp.nodes.items():
        print(f"TESTING {n}")
        #print(f"  node {n.node_id}: {n.store}")
    print("\nAVAILABLE PARTITION SYSTEM")
    for n in ap.nodes.items():
        print(f"TESTING {n}")
        #print(f"  node {n.node_id}: {n.store}")
    print("\nBoth systems agree: everyone has balance = 100.")

    banner("STEP 2: Network partition happens!  {0,1} <-!-> {2,3,4}")
    
    network_cp.partition(group_a=[0, 1], group_b=[2, 3, 4])
    network_ap.partition(group_a=[0, 1], group_b=[2, 3, 4])
    print(f"Can Node 0 talk to Node 3 in Consistent System? {network_cp.connected(0, 3)}")
    print(f"Can Node 1 talk to Node 4 in Available System? {network_ap.connected(1, 4)}")
    print("Node 0 and 1 can no longer reach nodes 2, 3, 4.")

    banner("STEP 3: Client sends a write to node 0 (minority side) on BOTH systems")
    write_r_cp, write_r_ap, read_r_cp, read_r_ap = await asyncio.gather(
        cp.write(coordinator_id=0, key="balance", value=250),
        ap.write(coordinator_id=0, key="balance", value=250),
        cp.read(coordinator_id=0, key="balance"),
        ap.read(coordinator_id=0, key="balance"),
    )
    
    show_result("CP", "WRITE", write_r_cp)
    show_result("AP", "WRITE", write_r_ap)
    show_result("CP", "READ", read_r_cp)
    show_result("AP", "READ", read_r_ap)

    banner("STEP 4: Reading 'balance' from a MAJORITY node (node 2) on both systems")
    r_cp, r_ap = await asyncio.gather(
        cp.read(coordinator_id=2, key="balance"),
        ap.read(coordinator_id=2, key="balance"),
    )
    show_result("CP", "READ", r_cp)
    show_result("AP", "READ", r_ap)

    print(
        "\nNotice: CP's read from node 2 is still successful (2,3,4 are still a "
        "quorum among themselves) and correctly shows the OLD value 100 -- "
        "it never saw the write to node 0 because that write was rejected. "
        "No inconsistency is ever exposed.\n"
        "AP's read from node 2 also shows 100, but AP's node 0 is silently "
        "sitting on 250. Two clients asking two different nodes right now "
        "get two different answers -- that's the inconsistency AP accepted "
        "in exchange for never saying 'unavailable'."
    )

    banner("STEP 5: Partition heals")
    network_ap.heal()
    network_cp.heal()
    print("Nodes 0-4 can all reach each other again.\n")
    print(f"Can Node 0 talk to Node 3 in Consistent System? {network_cp.connected(0, 3)}")
    print(f"Can Node 1 talk to Node 4 in Available System? {network_ap.connected(1, 4)}")


    banner("STEP 6: Final state of every node's local store")
    print("\nCONSISTENT PARTITION SYSTEM")
    for n in cp.nodes.items():
        print(f"TESTING {n}")
        #print(f"  node {n.node_id}: {n.store}")
    print("\nAVAILABLE PARTITION SYSTEM")
    for n in ap.nodes.items():
        print(f"TESTING {n}")
        #print(f"  node {n.node_id}: {n.store}")
    print(
        "\nThe AP nodes are still divergent (node 0/1 = 250, node 2/3/4 = 100). "
        "Healing the network doesn't fix this by itself -- an AP system needs "
        "an explicit reconciliation strategy to converge. That reconciliation step is "
        "intentionally left out here so the divergence is easy to see."
    )

    banner("SUMMARY")
    print(
        "CP system : during the partition it REJECTED the write and the read\n"
        "            request whenever a quorum wasn't reachable. Every answer\n"
        "            you got from it was guaranteed correct, but sometimes it\n"
        "            gave you no answer at all.\n\n"
        "AP system : during the partition it ACCEPTED every request instantly,\n"
        "            no matter which node you happened to hit. It was always\n"
        "            available, but two different nodes could give\n"
        "            you two different answers to the same question even IFF"
        "            there is only one node available with the others partitioned\n\n"
        "That trade-off -- give up availability to keep consistency, or give\n"
        "up consistency to keep availability, whenever a partition happens --\n"
        "is exactly what the CAP theorem describes."
    )

async def testNetwork():
    banner("STEP 1. INITIALIZING CLUSTER")
    node_ids = [1, 2, 3]
    network = Network(node_ids)
    
    # Instantiate nodes with references to the shared network. 
    # We then map each node id to the nodes.
    nodes = {}
    for n_id in node_ids:
        nodes[n_id] = Node(n_id, network)
    print(f"Created nodes: {list(nodes.keys())}")
    print(f"Is network partitioned? {network.is_partitioned}\n")

    banner("STEP 2. LOCAL READ / WRITE TESTS")
    await nodes[1].local_write("user", "Alice")
    await nodes[2].local_write("user", "Bob")

    val_1 = await nodes[1].local_read("user")
    val_2 = await nodes[2].local_read("user")
    print(f"Node 1 store read: {val_1}")
    print(f"Node 2 store read: {val_2}\n")

    banner("STEP 3. CREATING A NETWORK PARTITION")
    # Here we create a partition between 2 node groups 
    # group_a = {1} group_b = {2, 3}
    network.partition(group_a=[1], group_b=[2, 3])
    print(f"Is network partitioned? {network.is_partitioned}")
    print(f"Can Node 1 talk to Node 2? {network.connected(1, 2)}")
    print(f"Can Node 2 talk to Node 3? {network.connected(2, 3)}")
    print(f"Can Node 1 talk to itself? {network.connected(1, 1)}\n")

    banner("STEP 4. SIMULATING A REPLICATION ATTEMPT")
    # Node 1 tries to replicate data to Node 2 across the partition
    node1_id, node2_id = 1, 2
    key, val = "system_status", "maintenance" # Some random unimportant key value 

    if network.connected(node1_id, node2_id): # We check if the connection is there
        await nodes[node2_id].local_write(key, val) # If it is we can add information
        print(f"Replication succeeded from Node {node1_id} to Node {node2_id}") 
    else:
        print(f"REPLICATION BLOCKED: Connection broken between Node {node1_id} and Node {node2_id}!") # If not error message

    print(f"Node 2 state: {nodes[2]}\n") # Then we print node2 to see if any changes happened.

    # Getting rid of the partition.
    banner("STEP 5. HEALING THE NETWORK")
    network.heal()
    print(f"Is network partitioned? {network.is_partitioned}")
    print(f"Can Node 1 talk to Node 2 now? {network.connected(1, 2)}\n")

    banner("STEP 6. ASSERTION SAFETY CHECK")
    try:
        # Invalid partition: forgot Node 3
        print("Testing invalid partition (forgetting Node 3)...")
        network.partition(group_a=[1], group_b=[2])
    except AssertionError as e:
        print(f"Assertion caught successfully: '{e}'")

async def main():
    await testSystems()


if __name__ == "__main__":
    asyncio.run(main())