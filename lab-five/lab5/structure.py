"""Thread-safe facade of the tree: writes take the lock in write mode, reads in read mode.

Every method gets a two-slot list `st` and stores there the moment the lock was acquired (st[0]) and the moment
right before it was released (st[1]); the caller takes t_call before the call and t_ret after it.
"""
from time import perf_counter_ns as now

from lab5.bst import BST

RANGE_WIDTH = 100  # range_count(lo, lo + 99): about 50 present keys with the tree half full


class ThreadSafeBST:
    def __init__(self, lock, tree=None):
        self.tree = tree or BST()
        self.lock = lock
        self._w_acq, self._w_rel = lock.w_acquire, lock.w_release
        self._r_acq, self._r_rel = lock.r_acquire, lock.r_release

    def insert(self, key, st):
        self._w_acq()
        st[0] = now()
        try:
            return self.tree.insert(key)
        finally:
            st[1] = now()
            self._w_rel()

    def remove(self, key, st):
        self._w_acq()
        st[0] = now()
        try:
            return self.tree.remove(key)
        finally:
            st[1] = now()
            self._w_rel()

    def contains(self, key, st):
        self._r_acq()
        st[0] = now()
        try:
            return self.tree.contains(key)
        finally:
            st[1] = now()
            self._r_rel()

    def range_count(self, lo, hi, st):
        self._r_acq()
        st[0] = now()
        try:
            return self.tree.range_count(lo, hi)
        finally:
            st[1] = now()
            self._r_rel()

    # used only by the read-only check: the version must not move while a read holds the lock
    def contains_checked(self, key):
        self._r_acq()
        try:
            before = self.tree.version
            self.tree.contains(key)
            self.tree.range_count(key, key + RANGE_WIDTH - 1)
            return before == self.tree.version
        finally:
            self._r_rel()
