"""Batch echo server — echoes entire batches back to the response queue.

Usage:
    python batch_echo_server.py <directory> <request_queue> <response_queue>
                               [cpu_core] [--sync]
"""

import os
import sys
import struct

from mmapq import IndexedBatchQueue, NextMove, ffi, lib

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

    q_req = IndexedBatchQueue.create(directory, req_name)
    q_res = IndexedBatchQueue.create(directory, res_name)
    poller = q_req.create_poller()
    appender = q_res.create_appender()

    shutdown = False

    def echo_handler(batch_index, ctx):
        nonlocal shutdown

        # Advance to first message, check for shutdown
        it = ffi.addressof(ctx.batch_iterator)
        if not lib.batch_iterator_advance(it):
            return NextMove.ADVANCE

        length = lib.batch_iterator_current_length_ffi(it)
        data_ptr = lib.batch_iterator_current_data_ffi(it)
        data = bytes(ffi.buffer(data_ptr, length))

        if len(data) >= 6:
            (_, seq) = struct.unpack(LATENCY_HEADER_FMT, data[:6])
            if seq == SHUTDOWN_SEQ:
                shutdown = True

        # Echo: manually append msg 0, then append_remaining_from for msg 1..N
        with appender.start_batch() as writer:
            writer.append(data)
            writer.append_remaining_from(it)
            writer.commit()

        return NextMove.ADVANCE

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
