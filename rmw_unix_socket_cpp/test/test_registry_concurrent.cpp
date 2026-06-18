// Copyright 2026 Abderahmane BENALI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Concurrency stress tests for the lock-free registry.
//
// These tests verify that the per-slot seqlock + atomic-CAS protocol holds up
// under realistic contention: many threads adding/removing entries while many
// other threads scan for them, without external synchronization.
//
// What we're guarding against:
//   - Torn reads: a query observing a half-written entry (e.g., a node_name
//     with bytes from two different concurrent writers spliced together).
//   - Lost adds: two writers claiming the same slot.
//   - Use-after-free: a reader resolving a slot whose payload was concurrently
//     torn down.
//   - Generation counter regression: any observed generation value must
//     monotonically increase.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <sys/mman.h>
#include <unistd.h>

#include "../src/registry.hpp"

class RegistryConcurrentTest : public ::testing::Test
{
protected:
  void * registry_ptr = nullptr;
  size_t registry_size = 0;
  int registry_fd = -1;
  // Distinct domain to avoid colliding with the single-threaded test_registry.
  size_t domain_id = 197;

  void SetUp() override
  {
    char name[64];
    std::snprintf(name, sizeof(name), "/ros2_uds_%zu", domain_id);
    shm_unlink(name);

    registry_fd = rmw_uds::registry_open(domain_id, &registry_ptr, &registry_size);
    ASSERT_GE(registry_fd, 0);
    ASSERT_NE(nullptr, registry_ptr);
  }

  void TearDown() override
  {
    rmw_uds::registry_close(registry_fd, registry_ptr, registry_size);
    char name[64];
    std::snprintf(name, sizeof(name), "/ros2_uds_%zu", domain_id);
    shm_unlink(name);
  }
};

// N writer threads each add UNIQUE entries (different node names) and then
// remove them. After all threads join, the registry must be empty and the
// total number of generation bumps must be exactly 2 * N * iterations
// (each add and each remove bumps the counter once).
TEST_F(RegistryConcurrentTest, ConcurrentAddRemoveLeavesRegistryEmpty)
{
  auto * header = rmw_uds::registry_header(registry_ptr);
  const int num_threads = 16;
  const int iterations_per_thread = 200;

  uint64_t gen_before = rmw_uds::registry_generation(header);

  auto worker = [&](int tid) {
      for (int iter = 0; iter < iterations_per_thread; ++iter) {
        rmw_uds::RegistryEntry e;
        std::memset(&e, 0, sizeof(e));
        e.type = rmw_uds::ENTRY_NODE;
        e.pid = getpid();
        std::snprintf(e.node_name, sizeof(e.node_name), "t%d_i%d", tid, iter);
        std::strncpy(e.node_namespace, "/concurrent", sizeof(e.node_namespace) - 1);
        int32_t idx = rmw_uds::registry_add(header, e);
        ASSERT_GE(idx, 0) << "add failed at tid=" << tid << " iter=" << iter;
        rmw_uds::registry_remove(header, idx);
      }
    };

  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker, i);
  }
  for (auto & t : threads) {
    t.join();
  }

  // Registry should be empty.
  auto remaining = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_NODE, nullptr, nullptr, "/concurrent");
  EXPECT_EQ(0u, remaining.size())
    << remaining.size() << " entries leaked under concurrent add/remove";

  // Each successful add and remove bumps generation by 1.
  uint64_t gen_after = rmw_uds::registry_generation(header);
  EXPECT_EQ(
    gen_before + 2ull * num_threads * iterations_per_thread,
    gen_after);
}

// One writer thread continuously cycles an entry (add, then remove, then
// add a *different* entry, ...). Multiple reader threads continuously query
// the registry. Every result the reader sees must be one of the entries
// the writer actually committed — never a torn / half-written entry.
TEST_F(RegistryConcurrentTest, ReadersNeverSeeTornEntries)
{
  auto * header = rmw_uds::registry_header(registry_ptr);
  std::atomic<bool> stop{false};
  std::atomic<int> torn_observations{0};
  std::atomic<int> total_observations{0};

  // The writer publishes entries whose node_name is "alpha_<N>" or
  // "beta_<N>". The topic_name encodes a discriminator: "/torn_test/A" or
  // "/torn_test/B". A reader that sees node_name starting with "alpha"
  // MUST see topic ending with "/A" — otherwise the slot was torn.
  auto writer = [&]() {
      int counter = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        rmw_uds::RegistryEntry e;
        std::memset(&e, 0, sizeof(e));
        e.type = rmw_uds::ENTRY_NODE;
        e.pid = getpid();
        std::strncpy(e.node_namespace, "/torn_test", sizeof(e.node_namespace) - 1);
        if (counter % 2 == 0) {
          std::snprintf(e.node_name, sizeof(e.node_name), "alpha_%d", counter);
          std::snprintf(e.topic_name, sizeof(e.topic_name), "/torn_test/A");
        } else {
          std::snprintf(e.node_name, sizeof(e.node_name), "beta_%d", counter);
          std::snprintf(e.topic_name, sizeof(e.topic_name), "/torn_test/B");
        }
        int32_t idx = rmw_uds::registry_add(header, e);
        if (idx >= 0) {
          // Yield so the entry is visible for at least one scheduling slice;
          // otherwise on a heavily contended 2-core CI runner readers can
          // miss every single add/remove window and observation count = 0.
          std::this_thread::yield();
          rmw_uds::registry_remove(header, idx);
        }
        ++counter;
      }
    };

  auto reader = [&]() {
      while (!stop.load(std::memory_order_relaxed)) {
        auto results = rmw_uds::registry_query(
          header, rmw_uds::ENTRY_NODE, nullptr, nullptr, "/torn_test");
        for (const auto & r : results) {
          total_observations.fetch_add(1, std::memory_order_relaxed);
          bool name_alpha = r.node_name.rfind("alpha", 0) == 0;
          bool name_beta = r.node_name.rfind("beta", 0) == 0;
          bool topic_a = r.topic_name == "/torn_test/A";
          bool topic_b = r.topic_name == "/torn_test/B";
          // node_name and topic_name must agree on which "phase" we're in.
          if ((name_alpha && !topic_a) || (name_beta && !topic_b) ||
            (!name_alpha && !name_beta))
          {
            torn_observations.fetch_add(1, std::memory_order_relaxed);
          }
        }
      }
    };

  std::thread w(writer);
  std::vector<std::thread> readers;
  for (int i = 0; i < 4; ++i) {
    readers.emplace_back(reader);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  stop.store(true, std::memory_order_relaxed);
  w.join();
  for (auto & r : readers) {
    r.join();
  }

  EXPECT_GT(total_observations.load(), 0) << "test did not exercise the path";
  EXPECT_EQ(0, torn_observations.load())
    << "observed " << torn_observations.load()
    << " torn snapshots out of " << total_observations.load();
}

// Many threads racing on registry_add simultaneously: each must get a
// distinct slot index. No two writers may share a slot.
TEST_F(RegistryConcurrentTest, ConcurrentAllocateDistinctSlots)
{
  auto * header = rmw_uds::registry_header(registry_ptr);
  const int num_threads = 32;
  const int adds_per_thread = 50;

  std::vector<std::vector<int32_t>> per_thread_indices(num_threads);

  auto worker = [&](int tid) {
      for (int i = 0; i < adds_per_thread; ++i) {
        rmw_uds::RegistryEntry e;
        std::memset(&e, 0, sizeof(e));
        e.type = rmw_uds::ENTRY_PUBLISHER;
        e.pid = getpid();
        std::snprintf(e.topic_name, sizeof(e.topic_name), "/alloc_%d_%d", tid, i);
        int32_t idx = rmw_uds::registry_add(header, e);
        ASSERT_GE(idx, 0);
        per_thread_indices[tid].push_back(idx);
      }
    };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker, i);
  }
  for (auto & t : threads) {t.join();}

  // Verify all returned indices are unique across all threads.
  std::set<int32_t> seen;
  for (const auto & v : per_thread_indices) {
    for (int32_t idx : v) {
      EXPECT_TRUE(seen.insert(idx).second)
        << "slot index " << idx << " was handed out twice";
    }
  }
  EXPECT_EQ(static_cast<size_t>(num_threads * adds_per_thread), seen.size());

  // Each entry must be queryable by its unique topic.
  auto all = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_PUBLISHER, nullptr, nullptr, nullptr);
  EXPECT_EQ(static_cast<size_t>(num_threads * adds_per_thread), all.size());

  // Clean up.
  for (const auto & v : per_thread_indices) {
    for (int32_t idx : v) {
      rmw_uds::registry_remove(header, idx);
    }
  }
}

// Two threads racing to remove the same slot: exactly one must succeed,
// generation must be bumped exactly once.
TEST_F(RegistryConcurrentTest, ConcurrentRemoveOfSameSlotIsIdempotent)
{
  auto * header = rmw_uds::registry_header(registry_ptr);

  // Run many trials because the race window is tiny.
  for (int trial = 0; trial < 100; ++trial) {
    rmw_uds::RegistryEntry e;
    std::memset(&e, 0, sizeof(e));
    e.type = rmw_uds::ENTRY_NODE;
    e.pid = getpid();
    std::snprintf(e.node_name, sizeof(e.node_name), "race_%d", trial);
    int32_t idx = rmw_uds::registry_add(header, e);
    ASSERT_GE(idx, 0);

    uint64_t gen_before = rmw_uds::registry_generation(header);

    std::atomic<int> ready{0};
    auto remover = [&]() {
        // Spin to maximize the chance both threads CAS at the same time.
        ready.fetch_add(1);
        while (ready.load() < 2) {/* spin */}
        rmw_uds::registry_remove(header, idx);
      };
    std::thread t1(remover);
    std::thread t2(remover);
    t1.join();
    t2.join();

    uint64_t gen_after = rmw_uds::registry_generation(header);
    EXPECT_EQ(gen_before + 1, gen_after)
      << "trial " << trial
      << ": generation bumped " << (gen_after - gen_before)
      << " times for a single removed slot (expected exactly 1)";

    auto leftover = rmw_uds::registry_query(
      header, rmw_uds::ENTRY_NODE, nullptr, e.node_name, nullptr);
    EXPECT_EQ(0u, leftover.size());
  }
}

// Generation must never decrease from the perspective of a single observer,
// even under chaotic concurrent mutation. Each observer tracks its own local
// "last seen" to avoid false positives from observers racing on a shared one.
TEST_F(RegistryConcurrentTest, GenerationCounterMonotonicUnderChaos)
{
  auto * header = rmw_uds::registry_header(registry_ptr);
  std::atomic<bool> stop{false};
  std::atomic<int> regressions{0};

  auto mutator = [&](int tid) {
      int n = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        rmw_uds::RegistryEntry e;
        std::memset(&e, 0, sizeof(e));
        e.type = rmw_uds::ENTRY_SUBSCRIPTION;
        e.pid = getpid();
        std::snprintf(e.topic_name, sizeof(e.topic_name), "/chaos_%d_%d", tid, n++);
        int32_t idx = rmw_uds::registry_add(header, e);
        if (idx >= 0) {
          rmw_uds::registry_remove(header, idx);
        }
      }
    };

  auto observer = [&]() {
      uint64_t local_last = rmw_uds::registry_generation(header);
      while (!stop.load(std::memory_order_relaxed)) {
        uint64_t g = rmw_uds::registry_generation(header);
        if (g < local_last) {
          regressions.fetch_add(1, std::memory_order_relaxed);
        }
        local_last = g;
      }
    };

  std::vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {threads.emplace_back(mutator, i);}
  for (int i = 0; i < 4; ++i) {threads.emplace_back(observer);}

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  stop.store(true, std::memory_order_relaxed);
  for (auto & t : threads) {t.join();}

  EXPECT_EQ(0, regressions.load())
    << "generation counter regressed " << regressions.load() << " times";
}
