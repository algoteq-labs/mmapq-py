"""Smoke test for an installed mmapq (sdist or registry install), not the repo tree."""
import importlib.metadata
import sys
import tempfile

import mmapq
from mmapq import IndexedBatchQueue, IndexedQueue, NextMove

want = sys.argv[1]
assert mmapq.__version__ == want, (mmapq.__version__, want)
assert importlib.metadata.version("mmapq") == want
assert "site-packages" in mmapq.__file__, mmapq.__file__

got = []


def handler(index, appender_id, data):
    got.append((index, bytes(data)))
    return NextMove.ADVANCE


with tempfile.TemporaryDirectory() as d:
    with IndexedQueue.create(d, "smoke") as q:
        with q.create_appender() as a:
            assert (a.append(b"one"), a.append(b"two")) == (0, 1)
        with q.create_poller() as p:
            p.poll(handler)
            p.poll(handler)
    assert got == [(0, b"one"), (1, b"two")], got
    with IndexedBatchQueue.create(d, "smokeb") as q:
        with q.create_appender() as a:
            with a.start_batch() as b:
                b.append(b"e1")
                b.append(b"e2")
                bi = b.commit()
        with q.create_reader() as r:
            assert r.read(bi) == [b"e1", b"e2"]
print("smoke ok", mmapq.__version__)
