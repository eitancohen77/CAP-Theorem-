import asyncio
from Network import Network
from Node import Node


async def networkTest():
    print("=== 1. INITIALIZING CLUSTER ===")
    node_ids = [1, 2, 3]
    network = Network(node_ids)
    
    # Instantiate nodes with references to the shared network. 
    # We then map each node id to the nodes.
    nodes = {}
    for n_id in node_ids:
        nodes[n_id] = Node(n_id, network)
    print(f"Created nodes: {list(nodes.keys())}")
    print(f"Is network partitioned? {network.is_partitioned}\n")

    print("=== 2. LOCAL READ / WRITE TESTS ===")
    await nodes[1].local_write("user", "Alice")
    await nodes[2].local_write("user", "Bob")

    val_1 = await nodes[1].local_read("user")
    val_2 = await nodes[2].local_read("user")
    print(f"Node 1 store read: {val_1}")
    print(f"Node 2 store read: {val_2}\n")

    print("=== 3. CREATING A NETWORK PARTITION ===")
    # Here we create a partition between 2 node groups 
    # group_a = {1} group_b = {2, 3}
    network.partition(group_a=[1], group_b=[2, 3])
    print(f"Is network partitioned? {network.is_partitioned}")
    print(f"Can Node 1 talk to Node 2? {network.connected(1, 2)}")
    print(f"Can Node 2 talk to Node 3? {network.connected(2, 3)}")
    print(f"Can Node 1 talk to itself? {network.connected(1, 1)}\n")

    print("=== 4. SIMULATING A REPLICATION ATTEMPT ===")
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
    print("=== 5. HEALING THE NETWORK ===")
    network.heal()
    print(f"Is network partitioned? {network.is_partitioned}")
    print(f"Can Node 1 talk to Node 2 now? {network.connected(1, 2)}\n")

    print("=== 6. ASSERTION SAFETY CHECK ===")
    try:
        # Invalid partition: forgot Node 3
        print("Testing invalid partition (forgetting Node 3)...")
        network.partition(group_a=[1], group_b=[2])
    except AssertionError as e:
        print(f"Assertion caught successfully: '{e}'")

async def main():
    networkTest()


if __name__ == "__main__":
    asyncio.run(main())