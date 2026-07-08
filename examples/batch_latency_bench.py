"""Batch round-trip latency benchmark.

Measures one-way latency by sending a single-message batch through a request
queue to an echo server and waiting for the echoed response.

Usage:
    # Internal server (auto-starts echo server thread)
    python batch_latency_bench.py <iterations> <warmup> <msg_size>
                                  [client_cpu] [server_cpu]

    # External server (requires batch_echo_server.py running)
    python batch_latency_bench.py <iterations> <warmup> <msg_size>
                                  [client_cpu] [--external] [--sync]
"""

import os
import sys
import threading
import time
import struct

from mmapq import IndexedBatchQueue, NextMove, PollResult, ffi, lib

SHUTDOWN_SEQ = 0
LATENCY_HEADER_FMT = ">HI"  # uint16 instance_id, uint32 sequence

# Defaults
DEFAULT_ITERATIONS = 100000
DEFAULT_WARMUP = 10000
DEFAULT_MSG_SIZE = 64


def monotonic_nanos():
    """Return monotonic timestamp in nanoseconds."""
    import ctypes
    libc = ctypes.CDLL("libc.so.6")
    class timespec(ctypes.Structure):
        _fields_ = [("tv_sec", ctypes.c_long), ("tv_nsec", ctypes.c_long)]
    ts = timespec()
    libc.clock_gettime(1, ctypes.byref(ts))  # CLOCK_MONOTONIC = 1
    return ts.tv_sec * 1_000_000_000 + ts.tv_nsec


def format_duration(ns):
    if ns < 1000:
        return f"{ns} ns"
    elif ns < 1_000_000:
        return f"{ns/1000:.2f} µs"
    else:
        return f"{ns/1_000_000:.2f} ms"


def print_stats(results, elapsed_ns, label):
    if not results:
        return
    sorted_vals = sorted(results)
    n = len(sorted_vals)
    total = sum(sorted_vals)
    mean = total / n
    throughput = n / (elapsed_ns / 1_000_000_000)

    def p(frac):
        return sorted_vals[int(n * frac)]

    print(f"\n{label}")
    print(f"  {n:>6} samples")
    print(f"  Throughput: {throughput:.0f} ops/sec")
    print(f"  Latency:")
    print(f"    Mean:    {format_duration(mean)}")
    print(f"    Min:     {format_duration(sorted_vals[0])}")
    print(f"    P50:     {format_duration(p(0.50))}")
    print(f"    P90:     {format_duration(p(0.90))}")
    print(f"    P99:     {format_duration(p(0.99))}")
    print(f"    P99.9:   {format_duration(p(0.999))}")
    print(f"    P99.99:  {format_duration(p(0.9999))}")
    print(f"    Max:     {format_duration(sorted_vals[-1])}")


class BatchLatencyProbe:
    """Client-side probe that sends a request and waits for the echo."""

    def __init__(self, instance_id, msg_size, directory,
                 request_name="request", response_name="response"):
        self.instance_id = instance_id
        self.msg_size = msg_size
        self.current_seq = 0
        self.response_received = False

        q_req = IndexedBatchQueue.create(directory, request_name)
        q_res = IndexedBatchQueue.create(directory, response_name)
        self.request_appender = q_req.create_appender()
        self.response_poller = q_res.create_poller()
        self.response_poller.move_to_end()
        self._q_req = q_req
        self._q_res = q_res

        self.msg_buffer = bytearray(msg_size)

    def execute(self):
        self.response_received = False
        self.current_seq += 1

        struct.pack_into(LATENCY_HEADER_FMT, self.msg_buffer, 0,
                         self.instance_id, self.current_seq)

        start = monotonic_nanos()

        with self.request_appender.start_batch() as writer:
            writer.append(bytes(self.msg_buffer))
            writer.commit()

        my_id = self.instance_id
        my_seq = self.current_seq

        def response_handler(_bi, ctx):
            it = ffi.addressof(ctx.batch_iterator)
            if not lib.batch_iterator_advance(it):
                return NextMove.ADVANCE
            length = lib.batch_iterator_current_length_ffi(it)
            if length >= 6:
                data_ptr = lib.batch_iterator_current_data_ffi(it)
                data = bytes(ffi.buffer(data_ptr, length))
                (rid, rseq) = struct.unpack(LATENCY_HEADER_FMT, data[:6])
                if rid == my_id and rseq == my_seq:
                    self.response_received = True
            return NextMove.ADVANCE

        while not self.response_received:
            self.response_poller.poll(response_handler)

        return monotonic_nanos() - start

    def close(self):
        self.request_appender.close()
        self.response_poller.close()
        self._q_req.close()
        self._q_res.close()

    def send_shutdown(self):
        """Send a shutdown signal and wait for echo."""
        self.response_received = False
        saved_seq = self.current_seq
        self.current_seq = SHUTDOWN_SEQ

        struct.pack_into(LATENCY_HEADER_FMT, self.msg_buffer, 0,
                         self.instance_id, SHUTDOWN_SEQ)

        with self.request_appender.start_batch() as writer:
            writer.append(bytes(self.msg_buffer))
            writer.commit()

        timeout_at = monotonic_nanos() + 2_000_000_000

        def shutdown_handler(_bi, ctx):
            it = ffi.addressof(ctx.batch_iterator)
            if not lib.batch_iterator_advance(it):
                return NextMove.ADVANCE
            length = lib.batch_iterator_current_length_ffi(it)
            if length >= 6:
                data_ptr = lib.batch_iterator_current_data_ffi(it)
                data = bytes(ffi.buffer(data_ptr, length))
                (_, rseq) = struct.unpack(LATENCY_HEADER_FMT, data[:6])
                if rseq == SHUTDOWN_SEQ:
                    self.response_received = True
            return NextMove.ADVANCE

        while not self.response_received and monotonic_nanos() < timeout_at:
            self.response_poller.poll(shutdown_handler)

        self.current_seq = saved_seq


def internal_server(directory,
                    request_name="request",
                    response_name="response",
                    server_cpu=-1):
    """Internal echo server thread."""
    if server_cpu >= 0:
        _pin_thread(server_cpu)

    q_req = IndexedBatchQueue.create(directory, request_name)
    q_res = IndexedBatchQueue.create(directory, response_name)
    poller = q_req.create_poller()
    appender = q_res.create_appender()

    shutdown = False

    def echo_handler(batch_index, ctx):
        nonlocal shutdown
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

        with appender.start_batch() as writer:
            writer.append(data)
            writer.append_remaining_from(it)
            writer.commit()

        return NextMove.ADVANCE

    while not shutdown:
        result = poller.poll(echo_handler)
        if result == PollResult.END_OF_STREAM:
            # Spin briefly when empty
            pass

    appender.close()
    poller.close()
    q_req.close()
    q_res.close()


def _pin_thread(cpu):
    if cpu < 0:
        return
    import ctypes
    libc = ctypes.CDLL("libc.so.6")
    CPU_SETSIZE = 1024
    size = CPU_SETSIZE // 8
    mask_t = ctypes.c_ulong * (size // ctypes.sizeof(ctypes.c_ulong))
    cpuset = mask_t()
    cpuset[cpu // ctypes.sizeof(ctypes.c_ulong)] = 1 << (cpu % ctypes.sizeof(ctypes.c_ulong))
    libc.pthread_setaffinity_np(libc.pthread_self(), size, ctypes.byref(cpuset))


def main():
    args = sys.argv[1:]
    if not args:
        print(f"usage: {sys.argv[0]} <iterations> [warmup] [msg_size] [client_cpu] "
              f"[server_cpu] [--external] [--sync] [--dir <path>]", file=sys.stderr)
        sys.exit(1)

    pos_args = []
    external_server = False
    use_sync = False
    directory = "/tmp"

    i = 0
    while i < len(args):
        if args[i] == "--external":
            external_server = True
        elif args[i] == "--sync":
            use_sync = True
        elif args[i] == "--dir" and i + 1 < len(args):
            directory = args[i + 1]
            i += 1
        else:
            pos_args.append(args[i])
        i += 1

    iterations = int(pos_args[0]) if len(pos_args) > 0 else DEFAULT_ITERATIONS
    warmup = int(pos_args[1]) if len(pos_args) > 1 else DEFAULT_WARMUP
    msg_size = int(pos_args[2]) if len(pos_args) > 2 else DEFAULT_MSG_SIZE
    client_cpu = int(pos_args[3]) if len(pos_args) > 3 else -1
    server_cpu = int(pos_args[4]) if len(pos_args) > 4 else -1

    print(f"Batch Round-Trip Latency Benchmark")
    print(f"  Iterations={iterations}, Warmup={warmup}, MsgSize={msg_size}")
    print(f"  Mode: {'EXTERNAL' if external_server else 'INTERNAL'} "
          f"({'SYNC' if use_sync else 'ASYNC'})")

    server_thread = None
    if not external_server:
        server_thread = threading.Thread(
            target=internal_server,
            args=(directory, "request", "response", server_cpu),
            daemon=True,
        )
        server_thread.start()
        time.sleep(0.2)
        print(f"  Internal server started (core {server_cpu})")

    if client_cpu >= 0:
        _pin_thread(client_cpu)

    probe = BatchLatencyProbe(
        1, msg_size, directory,
        request_name="request", response_name="response",
    )

    print("Warming up...")
    for _ in range(warmup):
        probe.execute()

    print("Measuring...")
    results = []
    start_bench = monotonic_nanos()
    for _ in range(iterations):
        results.append(probe.execute())
    end_bench = monotonic_nanos()

    print_stats(results, end_bench - start_bench,
                "Batch Round-Trip Latency Statistics (nanoseconds):")

    probe.send_shutdown()
    probe.close()

    if server_thread:
        server_thread.join(timeout=5)
        print("  Internal server stopped.")


if __name__ == "__main__":
    main()
