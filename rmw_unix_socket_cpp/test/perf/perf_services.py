#!/usr/bin/env python3
"""
Service round-trip latency benchmark for rmw_unix_socket_cpp.

Spawns N service/client pairs in a single process and measures
client-side request -> response latency.

Usage:
    RMW_IMPLEMENTATION=rmw_unix_socket_cpp python3 perf_services.py [N_PAIRS] [N_CALLS]
"""

import os
import statistics
import sys
import time

import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node
from example_interfaces.srv import AddTwoInts


def now_ns() -> int:
    return time.monotonic_ns()


class ServicePair:
    def __init__(self, node: Node, name: str):
        self.name = name
        self.samples_ns: list[int] = []
        self._send_ns: int | None = None
        self.srv = node.create_service(AddTwoInts, name, self._handle)
        self.cli = node.create_client(AddTwoInts, name)
        self._future = None

    def _handle(self, request, response):
        response.sum = request.a + request.b
        return response

    def call_async(self):
        req = AddTwoInts.Request()
        req.a = 1
        req.b = 2
        self._send_ns = now_ns()
        self._future = self.cli.call_async(req)

    def poll(self) -> bool:
        if self._future is None:
            return True
        if self._future.done():
            recv_ns = now_ns()
            assert self._send_ns is not None
            self.samples_ns.append(recv_ns - self._send_ns)
            self._future = None
            self._send_ns = None
            return True
        return False


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def report(name: str, samples_us: list[float]):
    if not samples_us:
        print(f"  {name}: NO SAMPLES")
        return
    print(
        f"  {name}: n={len(samples_us)}  "
        f"min={min(samples_us):.1f}us  "
        f"med={statistics.median(samples_us):.1f}us  "
        f"mean={statistics.fmean(samples_us):.1f}us  "
        f"p95={percentile(samples_us, 0.95):.1f}us  "
        f"p99={percentile(samples_us, 0.99):.1f}us  "
        f"max={max(samples_us):.1f}us"
    )


def main():
    n_pairs = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    n_calls = int(sys.argv[2]) if len(sys.argv) > 2 else 200

    rmw = os.environ.get("RMW_IMPLEMENTATION", "<default>")
    print(f"=== service round-trip latency benchmark ===")
    print(f"  rmw         : {rmw}")
    print(f"  pairs       : {n_pairs}")
    print(f"  calls/pair  : {n_calls}")
    print()

    rclpy.init()
    node = rclpy.create_node("perf_services")
    executor = SingleThreadedExecutor()
    executor.add_node(node)

    pairs = [ServicePair(node, f"/perf/svc_{i}") for i in range(n_pairs)]

    # Wait for all services to be available.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        executor.spin_once(timeout_sec=0.05)
        if all(p.cli.service_is_ready() for p in pairs):
            break
    if not all(p.cli.service_is_ready() for p in pairs):
        not_ready = sum(1 for p in pairs if not p.cli.service_is_ready())
        print(f"WARNING: {not_ready}/{n_pairs} services never became ready")

    t0 = time.monotonic()
    for round_idx in range(n_calls):
        for p in pairs:
            p.call_async()
        round_deadline = time.monotonic() + 10.0
        while time.monotonic() < round_deadline:
            executor.spin_once(timeout_sec=0.001)
            if all(p.poll() for p in pairs):
                break
    elapsed = time.monotonic() - t0

    all_samples_us = [s / 1000.0 for p in pairs for s in p.samples_ns]
    received = len(all_samples_us)
    expected = n_pairs * n_calls
    print(f"completed {received}/{expected} round-trips in {elapsed:.2f}s "
          f"({received/elapsed:.0f} call/s aggregate)")
    print()
    report("aggregate (all pairs)", all_samples_us)

    pair_p99s_us = [percentile([s / 1000.0 for s in p.samples_ns], 0.99) for p in pairs if p.samples_ns]
    if pair_p99s_us:
        print(f"\n  per-pair p99: min={min(pair_p99s_us):.1f}us  "
              f"med={statistics.median(pair_p99s_us):.1f}us  "
              f"max={max(pair_p99s_us):.1f}us")

    node.destroy_node()
    rclpy.shutdown()
    sys.exit(0 if received == expected else 2)


if __name__ == "__main__":
    main()
