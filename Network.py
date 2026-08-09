""" 
This is the network that connects all our nodes at Node.py file creating our distributed system.

Usually every node can reach and propagate information to every other node, but here we 
have a "partition" that can block communication between 2 groups of nodes. 
"""


class Network:
    def __init__(self, node_ids):
        # We turn it into a set because 
        # 1) We dont want duplicate node ids in our set
        # 2) we dont care about the order the nodes are in. We just care about the value. 
        # This allows us to do O(1) operation for == comparisson. Instead of having to loop 
        # through every element in 2 lists to check if they match. It also allows us to use 
        # Intersection and Union.

        # When partition_groups = None it means there are no partitions in our system. Otherwise
        # if we do have a partition in our system then partition_groups = the 2 set groups that cant 
        # propagate with each other. They can only talk with other nodes in their set group.
        self.node_ids = set(node_ids)
        self.partition_groups = None

    def partition(self, group_a, group_b):
        # Turning lists to sets to make it faster.
        # In sets {1, 2, 3} == {3, 1, 2} returns True
        group_a, group_b = set(group_a), set(group_b)
        # Error statements to make sure A U B == all_nodes
        assert group_a | group_b == self.node_ids, "gropus must cover all nodes"
        # Error statement making sure the 2 groups are divided with Intersection
        assert not (group_a & group_b), "groups must not overlap"
        self.partition_groups = (group_a, group_b)

    def heal(self):
        # If the network parition is resolved we can heal it
        self.partition_groups = None

    # Function to make sure 2 nodes are allowed to communicate with eachother
    def  connected(self, node_a, node_b):
        if node_a == node_b:
            return True

        if self.partition_groups == None:
            return True

        for group in self.partition_groups:
            if node_a in group and node_b in group:
                return True

        return False

    @property
    def is_partitioned(self):
        return self.partition_groups is not None