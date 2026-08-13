"""Batch echo server — echoes entire batches back to the response queue.

Usage:
    python batch_echo_server.py <directory> <request_queue> <response_queue>
                               [cpu_core] [--sync]
"""

import os
import sys
import struct

from mmapq import MmapqIndexedBatchQueue, MmapqNextMove

SHUTDOWN_SEQ = 0
LATENCY_HEADER_FMT = ">HI"  # uint16 instance_id, uint32 sequence


def pin_thread(cpu):
    """Pin the current thread to a specific CPU core."""
    if cpu < 0:
        return
    import ctypes
    libc = ctypes.CDLL("libc.so.6")
    # cpu_set_t is 1024 bits = 128 bytes on Linux
    CPU_SETSIZE = 1024
    size = CPU_SETSIZE // 8  # 128 bytes
    mask = ctypes.c_ulong * (size // ctypes.sizeof(ctypes.c_ulong))
    cpuset = mask()
    cpuset[cpu // ctypes.sizeof(ctypes.c_ulong)] = 1 << (cpu % ctypes.sizeof(ctypes.c_ulong))
    libc.pthread_setaffinity_np(libc.pthread_self(), size, ctypes.byref(cpuset))


def main():
    if len(sys.argv) < 4:
        print(f"usage: {sys.argv[0]} <directory> <request_queue> <response_queue> "
              f"[cpu_core] [--sync]", file=sys.stderr)
        sys.exit(1)

    directory = sys.argv[1]
    req_name = sys.argv[2]
    res_name = sys.argv[3]
    cpu_core = int(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] != "--sync" else -1
    use_sync = "--sync" in sys.argv

    if cpu_core >= 0:
        pin_thread(cpu_core)
        print(f"  Core: {cpu_core}")

    q_req = MmapqIndexedBatchQueue.create(directory, req_name)
    q_res = MmapqIndexedBatchQueue.create(directory, res_name)
    poller = q_req.create_poller()
    appender = q_res.create_appender()

    shutdown = False

    def echo_handler(batch_index, messages):
        nonlocal shutdown

        if not messages:
            return MmapqNextMove.ADVANCE

        msg0 = messages[0]
        if len(msg0) >= 6:
            (_, seq) = struct.unpack(LATENCY_HEADER_FMT, msg0[:6])
            if seq == SHUTDOWN_SEQ:
                shutdown = True

        # Echo: all messages go to a new batch
        with appender.start_batch() as writer:
            for msg in messages:
                writer.append(msg)
            writer.commit()

        return MmapqNextMove.ADVANCE

    print(f"Batch Echo Server started ({'SYNC' if use_sync else 'ASYNC'})...")
    print(f"  Queues: {directory}/{req_name} -> {directory}/{res_name}")
    print("Waiting for requests...")

    while not shutdown:
        poller.poll(echo_handler)

    print("Shutdown requested. Closing...")
    appender.close()
    poller.close()
    q_req.close()
    q_res.close()


if __name__ == "__main__":
    main()
