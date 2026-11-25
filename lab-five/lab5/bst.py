"""C8: binary search tree of record keys 0..K-1. remove() only marks a node deleted (lazy deletion).

The tree itself is not thread-safe; ThreadSafeBST in structure.py wraps every operation into a lock.
"""


class Node:
    __slots__ = ("key", "left", "right", "deleted")

    def __init__(self, key):
        self.key = key
        self.left = None
        self.right = None
        self.deleted = False


class BST:
    def __init__(self):
        self.root = None
        self.size = 0      # keys present: nodes not marked deleted
        self.nodes = 0     # all nodes, marked ones included
        self.version = 0   # bumped by every write operation; reads never touch it

    # writes ---------------------------------------------------------------------------------------------------
    def insert(self, key):
        """Adds the key; a node marked deleted is revived. False if the key is already present."""
        self.version += 1
        node = self.root
        if node is None:
            self.root = Node(key)
            self.nodes += 1
            self.size += 1
            return True
        while True:
            if key < node.key:
                if node.left is None:
                    node.left = Node(key)
                    break
                node = node.left
            elif key > node.key:
                if node.right is None:
                    node.right = Node(key)
                    break
                node = node.right
            else:
                if node.deleted:
                    node.deleted = False
                    self.size += 1
                    return True
                return False
        self.nodes += 1
        self.size += 1
        return True

    def remove(self, key):
        """Marks the key deleted. False if it is absent or already marked."""
        self.version += 1
        node = self.root
        while node is not None:
            if key < node.key:
                node = node.left
            elif key > node.key:
                node = node.right
            else:
                if node.deleted:
                    return False
                node.deleted = True
                self.size -= 1
                return True
        return False

    # reads: no assignment to any field of the tree or of a node ------------------------------------------------
    def contains(self, key):
        node = self.root
        while node is not None:
            if key < node.key:
                node = node.left
            elif key > node.key:
                node = node.right
            else:
                return not node.deleted
        return False

    def range_count(self, lo, hi):
        """Number of present keys k with lo <= k <= hi; subtrees outside the range are skipped."""
        count = 0
        stack = [self.root] if self.root is not None else []
        while stack:
            node = stack.pop()
            key = node.key
            if key > lo and node.left is not None:
                stack.append(node.left)
            if key < hi and node.right is not None:
                stack.append(node.right)
            if lo <= key <= hi and not node.deleted:
                count += 1
        return count

    # checks ---------------------------------------------------------------------------------------------------
    def check_invariants(self):
        """Left subtree keys are smaller, right ones larger (strict, so a key occurs at most once);
        the node and key counters match the tree."""
        nodes = present = 0
        depth_max = 0
        stack = [(self.root, -1, None, 1)] if self.root is not None else []
        errors = []
        while stack:
            node, lo, hi, depth = stack.pop()
            if not (node.key > lo and (hi is None or node.key < hi)):
                errors.append(f"key {node.key} outside ({lo}, {hi})")
            nodes += 1
            present += not node.deleted
            depth_max = max(depth_max, depth)
            if node.left is not None:
                stack.append((node.left, lo, node.key, depth + 1))
            if node.right is not None:
                stack.append((node.right, node.key, hi, depth + 1))
        if nodes != self.nodes:
            errors.append(f"nodes counter {self.nodes} != {nodes} nodes in the tree")
        if present != self.size:
            errors.append(f"size counter {self.size} != {present} present keys")
        return {"ok": not errors, "errors": errors[:10], "nodes": nodes, "present": present, "depth": depth_max}

    def fingerprint(self):
        """Every field of every node in key order plus the counters: equal fingerprints mean nothing changed."""
        out = []
        stack, node = [], self.root
        while stack or node is not None:
            while node is not None:
                stack.append(node)
                node = node.left
            node = stack.pop()
            out.append((node.key, node.deleted, id(node.left), id(node.right)))
            node = node.right
        return hash((tuple(out), id(self.root), self.size, self.nodes, self.version)), len(out)
