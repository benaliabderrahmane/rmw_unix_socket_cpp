#!/usr/bin/env python3
"""
Pub/sub latency benchmark for rmw_unix_socket_cpp.

Spawns N publisher/subscriber pairs in a single process, publishes
timestamped messages, and measures end-to-end (publish -> take) latency.

Usage:
    RMW_IMPLEMENTATION=rmw_unix_socket_cpp python3 perf_pubsub.py [N_PAIRS] [N_MESSAGES] [PAYLOAD_BYTES]
"""

import os
import statistics
import sys
import time

import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import String


# 20 ASCII digits — wide enough for any monotonic_ns() value (max int64 ~= 9.2e18).
HEADER_DIGITS = 20


def now_ns() -> int:
    return time.monotonic_ns()


class LatencyPair:
    """One publisher + one subscriber on a unique topic."""

    def __init__(self, node: Node, topic: str, payload_size: int):
        self.topic = topic
        self.payload_size = payload_size
        self.samples_ns: list[int] = []
        self._pending_send_ns: int | None = None

        qos = QoSProfile(
            depth=10,
            reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST,
        )

        self.pub = node.create_publisher(String, topic, qos)
        self.sub = node.create_subscription(String, topic, self._on_msg, qos)

    def _on_msg(self, msg: String):
        recv_ns = now_ns()
        send_ns = int(msg.data[:HEADER_DIGITS])
        self.samples_ns.append(recv_ns - send_ns)
        self._pending_send_ns = None

    def send(self):
        send_ns = now_ns()
        header = f"{send_ns:0{HEADER_DIGITS}d}"
        body = "x" * max(0, self.payload_size - HEADER_DIGITS)
        msg = String()
        msg.data = header + body
        self._pending_send_ns = send_ns
        self.pub.publish(msg)


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
    n_pairs = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    n_messages = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    payload_size = int(sys.argv[3]) if len(sys.argv) > 3 else 64

    rmw = os.environ.get("RMW_IMPLEMENTATION", "<default>")
    print(f"=== pub/sub latency benchmark ===")
    print(f"  rmw          : {rmw}")
    print(f"  pairs        : {n_pairs}")
    print(f"  messages/pair: {n_messages}")
    print(f"  payload bytes: {payload_size}")
    print()

    rclpy.init()
    node = rclpy.create_node("perf_pubsub")
    executor = SingleThreadedExecutor()
    executor.add_node(node)

    pairs = [LatencyPair(node, f"/perf/pubsub_{i}", payload_size) for i in range(n_pairs)]

    # Warm up: let discovery settle.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        executor.spin_once(timeout_sec=0.05)

    t0 = time.monotonic()
    for round_idx in range(n_messages):
        for p in pairs:
            p.send()
        # Drain until every pair has received this round.
        round_deadline = time.monotonic() + 5.0
        target_count = round_idx + 1
        while time.monotonic() < round_deadline:
            executor.spin_once(timeout_sec=0.001)
            if all(len(p.samples_ns) >= target_count for p in pairs):
                break
    elapsed = time.monotonic() - t0

    # Aggregate across all pairs.
    all_samples_us = [s / 1000.0 for p in pairs for s in p.samples_ns]
    received = len(all_samples_us)
    expected = n_pairs * n_messages
    print(f"received {received}/{expected} samples in {elapsed:.2f}s "
          f"({received/elapsed:.0f} msg/s aggregate)")
    print()
    report("aggregate (all pairs)", all_samples_us)

    # Per-pair worst case.
    pair_p99s_us = [percentile([s / 1000.0 for s in p.samples_ns], 0.99) for p in pairs if p.samples_ns]
    if pair_p99s_us:
        print(f"\n  per-pair p99: min={min(pair_p99s_us):.1f}us  "
              f"med={statistics.median(pair_p99s_us):.1f}us  "
              f"max={max(pair_p99s_us):.1f}us")

    node.destroy_node()
    rclpy.shutdown()

    # Exit code: nonzero if we lost messages.
    sys.exit(0 if received == expected else 2)


if __name__ == "__main__":
    main()
