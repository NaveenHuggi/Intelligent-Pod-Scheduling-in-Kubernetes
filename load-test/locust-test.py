#!/usr/bin/env python3
"""
Locust load test for comparing default vs intelligent scheduler under HTTP load.
Usage:
    locust -f load-test/locust-test.py --host http://<NODE-IP>:<PORT> \
           --users 50 --spawn-rate 5 --run-time 60s --headless
"""

# pyrefly: ignore [missing-import]
from locust import HttpUser, task, between, events
import random
import time

class SchedulerLoadUser(HttpUser):
    """Simulates concurrent HTTP requests to the scheduled app pods."""
    wait_time = between(0.1, 0.5)

    @task(3)
    def get_root(self):
        """Hit the root endpoint — most common request."""
        with self.client.get("/", catch_response=True, name="GET /") as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Unexpected status: {resp.status_code}")

    @task(1)
    def get_health(self):
        """Health-check style request."""
        with self.client.get("/", catch_response=True, name="GET /health") as resp:
            if resp.status_code in (200, 404):
                resp.success()

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("\n=== Load Test Started ===")
    print(f"Target host: {environment.host}")
    print("Testing scheduler performance under HTTP load...\n")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats.total
    print("\n=== Load Test Results ===")
    print(f"Total requests  : {stats.num_requests}")
    print(f"Failures        : {stats.num_failures}")
    print(f"Avg latency (ms): {stats.avg_response_time:.1f}")
    print(f"p95 latency (ms): {stats.get_response_time_percentile(0.95):.1f}")
    print(f"p99 latency (ms): {stats.get_response_time_percentile(0.99):.1f}")
    print(f"Req/sec         : {stats.total_rps:.2f}")
