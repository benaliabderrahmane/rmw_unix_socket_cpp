#!/usr/bin/env python3
"""
Scale test: launch 100 separate processes and measure cross-process
pub/sub latency under rmw_unix_socket_cpp.

Architecture:
  - 1 measurer process    (this script's child) — owns N subscribers + a /perf/start trigger publisher
  - N talker processes    — each subscribes to /perf/start, then publishes timestamped
                            messages on /perf/topic_<i> for a fixed duration

This exercises real cross-process IPC (the in-process pub/sub test does not).

Usage:
    RMW_IMPLEMENTATION=rmw_unix_socket_cpp python3 scale_100_nodes.py [N_TALKERS] [DURATION_S] [RATE_HZ]
"""

import os
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

# ----- Talker child program (executed via `python3 -c`) ------------------

TALKER_SRC = r"""
import os, sys, time
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import String, Bool

idx = int(sys.argv[1])
duration_s = float(sys.argv[2])
rate_hz = float(sys.argv[3])
HEADER_DIGITS = 20

rclpy.init()
node = rclpy.create_node(f"perf_talker_{idx}")

qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.RELIABLE, history=HistoryPolicy.KEEP_LAST)
pub = node.create_publisher(String, f"/perf/topic_{idx}", qos)

started = [False]
def on_start(_msg):
    started[0] = True
node.create_subscription(Bool, "/perf/start", on_start, qos)

# Wait for the start trigger (or 30s timeout).
deadline = time.monotonic() + 30.0
while time.monotonic() < deadline and not started[0]:
    rclpy.spin_once(node, timeout_sec=0.05)
if not started[0]:
    print(f"talker {idx}: never received start", file=sys.stderr)
    rclpy.shutdown()
    sys.exit(1)

period = 1.0 / rate_hz
end = time.monotonic() + duration_s
seq = 0
while time.monotonic() < end:
    msg = String()
    send_ns = time.monotonic_ns()
    msg.data = f"{send_ns:0{HEADER_DIGITS}d}"
    pub.publish(msg)
    seq += 1
    # Sleep until next tick.
    next_tick = time.monotonic() + period
    while time.monotonic() < next_tick:
        rclpy.spin_once(node, timeout_sec=0.001)

node.destroy_node()
rclpy.shutdown()
"""

# ----- Measurer child program ---------------------------------------------

MEASURER_SRC = r"""
import json, os, sys, time
import rclpy
from rclpy.node import Node
from rclpy.executors import SingleThreadedExecutor
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import String, Bool

n_talkers = int(sys.argv[1])
duration_s = float(sys.argv[2])
out_path = sys.argv[3]
HEADER_DIGITS = 20

rclpy.init()
node = rclpy.create_node("perf_measurer")
executor = SingleThreadedExecutor()
executor.add_node(node)

qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.RELIABLE, history=HistoryPolicy.KEEP_LAST)

samples_per_topic = [[] for _ in range(n_talkers)]

def make_cb(i):
    def cb(msg):
        recv_ns = time.monotonic_ns()
        send_ns = int(msg.data[:HEADER_DIGITS])
        samples_per_topic[i].append(recv_ns - send_ns)
    return cb

for i in range(n_talkers):
    node.create_subscription(String, f"/perf/topic_{i}", make_cb(i), qos)

start_pub = node.create_publisher(Bool, "/perf/start", qos)

# Let subscriptions register in shared-memory registry.
deadline = time.monotonic() + 5.0
while time.monotonic() < deadline:
    executor.spin_once(timeout_sec=0.05)

# Trigger talkers. Send several times to be robust against drops.
for _ in range(20):
    start_pub.publish(Bool(data=True))
    executor.spin_once(timeout_sec=0.05)

# Spin for duration + grace period.
end = time.monotonic() + duration_s + 2.0
while time.monotonic() < end:
    executor.spin_once(timeout_sec=0.01)

with open(out_path, "w") as f:
    json.dump(samples_per_topic, f)

node.destroy_node()
rclpy.shutdown()
"""


def percentile(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def report(name, samples_us):
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
    n_talkers = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    duration_s = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0
    rate_hz = float(sys.argv[3]) if len(sys.argv) > 3 else 50.0

    rmw = os.environ.get("RMW_IMPLEMENTATION", "<default>")
    print(f"=== {n_talkers}-process scale benchmark ===")
    print(f"  rmw       : {rmw}")
    print(f"  talkers   : {n_talkers} (separate processes)")
    print(f"  duration  : {duration_s}s")
    print(f"  rate/talker: {rate_hz} Hz")
    print(f"  expected  : ~{int(n_talkers * duration_s * rate_hz)} total messages")
    print()

    out_path = f"/tmp/perf_scale_{os.getpid()}.json"
    env = os.environ.copy()

    # Launch the measurer first.
    measurer = subprocess.Popen(
        [sys.executable, "-c", MEASURER_SRC, str(n_talkers), str(duration_s), out_path],
        env=env,
    )

    # Brief delay so the measurer can register its subscribers before talkers start.
    time.sleep(2.0)

    print(f"launching {n_talkers} talker processes...")
    t_launch = time.monotonic()
    talkers = []
    for i in range(n_talkers):
        p = subprocess.Popen(
            [sys.executable, "-c", TALKER_SRC, str(i), str(duration_s), str(rate_hz)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        talkers.append(p)
    launch_elapsed = time.monotonic() - t_launch
    print(f"all talkers launched in {launch_elapsed:.2f}s")

    # Wait for everyone.
    try:
        for p in talkers:
            p.wait(timeout=duration_s + 60.0)
        measurer.wait(timeout=30.0)
    except subprocess.TimeoutExpired:
        print("WARNING: timeout waiting for child processes; killing")
        for p in talkers + [measurer]:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)

    # Report any talker stderr noise.
    failed = [p for p in talkers if p.returncode != 0]
    if failed:
        print(f"WARNING: {len(failed)}/{n_talkers} talker processes had nonzero exit")
        for p in failed[:3]:
            err = p.stderr.read().decode(errors="replace") if p.stderr else ""
            if err.strip():
                print(f"  sample stderr: {err.strip()[:200]}")

    # Load samples.
    if not Path(out_path).exists():
        print(f"ERROR: measurer produced no output at {out_path}")
        sys.exit(1)
    import json
    with open(out_path) as f:
        samples_per_topic = json.load(f)
    Path(out_path).unlink(missing_ok=True)

    all_samples_us = [s / 1000.0 for topic_samples in samples_per_topic for s in topic_samples]
    received_per_topic = [len(t) for t in samples_per_topic]
    received = sum(received_per_topic)
    topics_with_no_msgs = sum(1 for c in received_per_topic if c == 0)

    print()
    print(f"received {received} messages from {n_talkers - topics_with_no_msgs}/{n_talkers} topics")
    if topics_with_no_msgs:
        print(f"  WARNING: {topics_with_no_msgs} topics produced zero messages "
              f"(discovery or trigger missed?)")
    print()
    report("aggregate (all talkers)", all_samples_us)

    if received_per_topic:
        per_topic_p99s_us = [
            percentile([s / 1000.0 for s in t], 0.99) for t in samples_per_topic if t
        ]
        if per_topic_p99s_us:
            print(f"\n  per-topic p99: min={min(per_topic_p99s_us):.1f}us  "
                  f"med={statistics.median(per_topic_p99s_us):.1f}us  "
                  f"max={max(per_topic_p99s_us):.1f}us")
        print(f"  per-topic msg count: min={min(received_per_topic)}  "
              f"med={statistics.median(received_per_topic)}  "
              f"max={max(received_per_topic)}")

    sys.exit(0 if topics_with_no_msgs == 0 else 2)


if __name__ == "__main__":
    main()
