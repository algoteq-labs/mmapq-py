"""Single-message echo server — echoes messages back to a response queue.

Simplified version of batch_echo_server.py using single-message queues.
Demonstrates a single-process producer/consumer pair.

Usage:
    python single_message_echo.py <directory> <queue_name> [count]

Example:
    python single_message_echo.py /tmp demo 5
"""

import os
import sys
import shutil

from mmapq import MmapqIndexedQueue, MmapqIndexedReader, MmapqNextMove


def main():
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} <directory> <queue_name> [count]", file=sys.stderr)
        sys.exit(1)

    directory = sys.argv[1]
    queue_name = sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 5

    queue_path = os.path.join(directory, queue_name)
    if os.path.exists(queue_path):
        shutil.rmtree(queue_path)

    # Create queue, appender, and poller
    q = MmapqIndexedQueue.create(directory, queue_name)
    appender = q.create_appender()
    poller = q.create_poller()

    # Produce messages
    print(f"Producing {count} messages...")
    for i in range(count):
        msg = f"msg-{i}".encode()
        appender.append(msg)
    print(f"  produced {count} messages")

    # Consume via sequential poll
    print("Consuming...")
    messages = []

    def handler(_index, _appender_id, data):
        messages.append(data)
        print(f"  received: {data!r}")
        return MmapqNextMove.ADVANCE

    for _ in range(count):
        poller.poll(handler)

    # Verify via random-access reader
    print("\nVerifying via random-access reader...")
    reader = MmapqIndexedReader.create(directory, queue_name)
    for i in range(count):
        data = reader.read(i)
        expected = f"msg-{i}".encode()
        assert data == expected, f"mismatch at {i}: {data!r} != {expected!r}"
    print(f"  all {count} messages verified ({reader.last_index() + 1} entries)")

    # Cleanup
    reader.close()
    poller.close()
    appender.close()
    q.close()
    shutil.rmtree(queue_path)

    print("\nOK — single-message demo completed successfully")


if __name__ == "__main__":
    main()
