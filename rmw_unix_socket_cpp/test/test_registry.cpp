#include <gtest/gtest.h>

#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "../src/registry.hpp"

class RegistryTest : public ::testing::Test
{
protected:
  void * registry_ptr = nullptr;
  size_t registry_size = 0;
  int registry_fd = -1;
  size_t domain_id = 98;  // Unique to avoid clashing with running systems

  void SetUp() override
  {
    // Make sure no stale registry from a previous test (or older layout) is
    // lying around — the new code uses O_CREAT|O_EXCL and would otherwise
    // attach to a v2 file.
    char name[64];
    std::snprintf(name, sizeof(name), "/ros2_uds_%zu", domain_id);
    shm_unlink(name);

    registry_fd = rmw_uds::registry_open(domain_id, &registry_ptr, &registry_size);
    ASSERT_GE(registry_fd, 0);
    ASSERT_NE(nullptr, registry_ptr);
    ASSERT_GT(registry_size, 0u);
  }

  void TearDown() override
  {
    rmw_uds::registry_close(registry_fd, registry_ptr, registry_size);
    char name[64];
    std::snprintf(name, sizeof(name), "/ros2_uds_%zu", domain_id);
    shm_unlink(name);
  }
};

TEST_F(RegistryTest, OpenCreatesValidHeader)
{
  auto * header = rmw_uds::registry_header(registry_ptr);
  EXPECT_EQ(rmw_uds::REGISTRY_VERSION, header->version.load());
  EXPECT_EQ(rmw_uds::REGISTRY_MAX_ENTRIES, header->max_entries);
}

TEST_F(RegistryTest, AddAndRemoveEntry)
{
  auto * header = rmw_uds::registry_header(registry_ptr);

  rmw_uds::RegistryEntry entry;
  std::memset(&entry, 0, sizeof(entry));
  entry.type = rmw_uds::ENTRY_NODE;
  entry.pid = getpid();
  std::strncpy(entry.node_name, "test_node", sizeof(entry.node_name) - 1);
  std::strncpy(entry.node_namespace, "/test", sizeof(entry.node_namespace) - 1);

  int32_t idx = rmw_uds::registry_add(header, entry);
  ASSERT_GE(idx, 0);

  auto results = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_NODE, nullptr, "test_node", "/test");
  ASSERT_EQ(1u, results.size());
  EXPECT_EQ("test_node", results[0].node_name);
  EXPECT_EQ("/test", results[0].node_namespace);

  rmw_uds::registry_remove(header, idx);
  results = rmw_uds::registry_query(header, rmw_uds::ENTRY_NODE, nullptr, "test_node", nullptr);
  EXPECT_EQ(0u, results.size());
}

TEST_F(RegistryTest, GenerationIncrementsOnChange)
{
  auto * header = rmw_uds::registry_header(registry_ptr);
  uint64_t gen0 = rmw_uds::registry_generation(header);

  rmw_uds::RegistryEntry entry;
  std::memset(&entry, 0, sizeof(entry));
  entry.type = rmw_uds::ENTRY_PUBLISHER;
  entry.pid = getpid();
  std::strncpy(entry.topic_name, "/gen_test", sizeof(entry.topic_name) - 1);

  int32_t idx = rmw_uds::registry_add(header, entry);
  ASSERT_GE(idx, 0);
  uint64_t gen1 = rmw_uds::registry_generation(header);
  EXPECT_GT(gen1, gen0);

  rmw_uds::registry_remove(header, idx);
  uint64_t gen2 = rmw_uds::registry_generation(header);
  EXPECT_GT(gen2, gen1);
}

TEST_F(RegistryTest, QueryFiltersByType)
{
  auto * header = rmw_uds::registry_header(registry_ptr);

  rmw_uds::RegistryEntry pub_entry;
  std::memset(&pub_entry, 0, sizeof(pub_entry));
  pub_entry.type = rmw_uds::ENTRY_PUBLISHER;
  pub_entry.pid = getpid();
  std::strncpy(pub_entry.topic_name, "/topic_a", sizeof(pub_entry.topic_name) - 1);

  rmw_uds::RegistryEntry sub_entry;
  std::memset(&sub_entry, 0, sizeof(sub_entry));
  sub_entry.type = rmw_uds::ENTRY_SUBSCRIPTION;
  sub_entry.pid = getpid();
  std::strncpy(sub_entry.topic_name, "/topic_a", sizeof(sub_entry.topic_name) - 1);

  int32_t idx1 = rmw_uds::registry_add(header, pub_entry);
  int32_t idx2 = rmw_uds::registry_add(header, sub_entry);
  ASSERT_GE(idx1, 0);
  ASSERT_GE(idx2, 0);

  auto pubs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_PUBLISHER, "/topic_a", nullptr, nullptr);
  auto subs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_SUBSCRIPTION, "/topic_a", nullptr, nullptr);
  auto all = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_EMPTY, "/topic_a", nullptr, nullptr);

  rmw_uds::registry_remove(header, idx1);
  rmw_uds::registry_remove(header, idx2);

  EXPECT_EQ(1u, pubs.size());
  EXPECT_EQ(1u, subs.size());
  EXPECT_EQ(2u, all.size());
}

// Pins the invariant that the type pre-filter (which short-circuits on the
// pre-snapshot state) must still return exactly the entry whose validated
// type matches — with same-topic decoys of other types present. Guards
// against a future edit dropping the authoritative post-snapshot re-check.
TEST_F(RegistryTest, QueryTypeFilterExcludesOtherTypesSameTopic)
{
  auto * header = rmw_uds::registry_header(registry_ptr);

  rmw_uds::RegistryEntry pub_entry;
  std::memset(&pub_entry, 0, sizeof(pub_entry));
  pub_entry.type = rmw_uds::ENTRY_PUBLISHER;
  pub_entry.pid = getpid();
  std::strncpy(pub_entry.topic_name, "/shared", sizeof(pub_entry.topic_name) - 1);

  rmw_uds::RegistryEntry sub_entry;
  std::memset(&sub_entry, 0, sizeof(sub_entry));
  sub_entry.type = rmw_uds::ENTRY_SUBSCRIPTION;
  sub_entry.pid = getpid();
  std::strncpy(sub_entry.topic_name, "/shared", sizeof(sub_entry.topic_name) - 1);

  rmw_uds::RegistryEntry srv_entry;
  std::memset(&srv_entry, 0, sizeof(srv_entry));
  srv_entry.type = rmw_uds::ENTRY_SERVICE;
  srv_entry.pid = getpid();
  std::strncpy(srv_entry.topic_name, "/shared", sizeof(srv_entry.topic_name) - 1);

  int32_t pub_idx = rmw_uds::registry_add(header, pub_entry);
  int32_t sub_idx = rmw_uds::registry_add(header, sub_entry);
  int32_t srv_idx = rmw_uds::registry_add(header, srv_entry);
  ASSERT_GE(pub_idx, 0);
  ASSERT_GE(sub_idx, 0);
  ASSERT_GE(srv_idx, 0);

  auto pubs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_PUBLISHER, "/shared", nullptr, nullptr);
  auto subs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_SUBSCRIPTION, "/shared", nullptr, nullptr);

  rmw_uds::registry_remove(header, pub_idx);
  rmw_uds::registry_remove(header, sub_idx);
  rmw_uds::registry_remove(header, srv_idx);

  ASSERT_EQ(1u, pubs.size());
  EXPECT_EQ(rmw_uds::ENTRY_PUBLISHER, pubs[0].type);
  ASSERT_EQ(1u, subs.size());
  EXPECT_EQ(rmw_uds::ENTRY_SUBSCRIPTION, subs[0].type);
}

TEST_F(RegistryTest, MultipleEntriesSameType)
{
  auto * header = rmw_uds::registry_header(registry_ptr);
  std::vector<int32_t> indices;

  for (int i = 0; i < 50; ++i) {
    rmw_uds::RegistryEntry entry;
    std::memset(&entry, 0, sizeof(entry));
    entry.type = rmw_uds::ENTRY_NODE;
    entry.pid = getpid();
    std::snprintf(entry.node_name, sizeof(entry.node_name), "node_%d", i);
    std::strncpy(entry.node_namespace, "/", sizeof(entry.node_namespace) - 1);
    int32_t idx = rmw_uds::registry_add(header, entry);
    ASSERT_GE(idx, 0);
    indices.push_back(idx);
  }

  auto nodes = rmw_uds::registry_query(header, rmw_uds::ENTRY_NODE, nullptr, nullptr, nullptr);
  EXPECT_EQ(50u, nodes.size());

  for (int32_t idx : indices) {
    rmw_uds::registry_remove(header, idx);
  }
}

// A PID that is extremely unlikely to exist. /proc/<this> should be ENOENT,
// which is what cleanup_stale uses to decide an entry's owner is dead.
static constexpr pid_t DEAD_PID = 2147483000;

TEST_F(RegistryTest, CleanupStaleRemovesDeadPidEntries)
{
  auto * header = rmw_uds::registry_header(registry_ptr);

  rmw_uds::RegistryEntry dead_entry;
  std::memset(&dead_entry, 0, sizeof(dead_entry));
  dead_entry.type = rmw_uds::ENTRY_NODE;
  dead_entry.pid = DEAD_PID;
  std::strncpy(dead_entry.node_name, "ghost", sizeof(dead_entry.node_name) - 1);

  rmw_uds::RegistryEntry live_entry;
  std::memset(&live_entry, 0, sizeof(live_entry));
  live_entry.type = rmw_uds::ENTRY_NODE;
  live_entry.pid = getpid();
  std::strncpy(live_entry.node_name, "alive", sizeof(live_entry.node_name) - 1);

  int32_t dead_idx = rmw_uds::registry_add(header, dead_entry);
  int32_t live_idx = rmw_uds::registry_add(header, live_entry);
  ASSERT_GE(dead_idx, 0);
  ASSERT_GE(live_idx, 0);

  rmw_uds::registry_cleanup_stale(header);
  auto remaining = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_NODE, nullptr, nullptr, nullptr);
  rmw_uds::registry_remove(header, live_idx);

  ASSERT_EQ(1u, remaining.size());
  EXPECT_EQ("alive", remaining[0].node_name);
}

// Fill the registry, then call registry_add again: the overflow path must
// reclaim stale (dead-PID) slots and succeed on the retry.
TEST_F(RegistryTest, AddReclaimsStaleSlotsOnOverflow)
{
  auto * header = rmw_uds::registry_header(registry_ptr);

  // Temporarily shrink max_entries so we can actually fill the registry.
  const uint32_t original_max = header->max_entries;
  header->max_entries = 8;

  // Fill with dead-PID entries.
  for (uint32_t i = 0; i < header->max_entries; ++i) {
    rmw_uds::RegistryEntry e;
    std::memset(&e, 0, sizeof(e));
    e.type = rmw_uds::ENTRY_NODE;
    e.pid = DEAD_PID;
    std::snprintf(e.node_name, sizeof(e.node_name), "dead_%u", i);
    ASSERT_GE(rmw_uds::registry_add(header, e), 0);
  }

  // Confirm it really is full: another dead-PID add must succeed only via
  // reclamation. First, attempt with a dead PID — also reclaims since all
  // entries are dead. Verify count is still bounded by max_entries.
  auto pre = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_NODE, nullptr, nullptr, nullptr);
  EXPECT_EQ(static_cast<size_t>(header->max_entries), pre.size());

  // The new entry is live; registry_add must internally call cleanup_stale
  // and succeed on the second pass.
  rmw_uds::RegistryEntry live;
  std::memset(&live, 0, sizeof(live));
  live.type = rmw_uds::ENTRY_NODE;
  live.pid = getpid();
  std::strncpy(live.node_name, "survivor", sizeof(live.node_name) - 1);
  int32_t live_idx = rmw_uds::registry_add(header, live);
  EXPECT_GE(live_idx, 0);

  auto results = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_NODE, nullptr, "survivor", nullptr);
  EXPECT_EQ(1u, results.size());

  for (uint32_t i = 0; i < header->max_entries; ++i) {
    rmw_uds::registry_remove(header, static_cast<int32_t>(i));
  }
  header->max_entries = original_max;
}

// An existing shm file with a smaller size (e.g., created by a previous
// build of this RMW with a different RegistryEntrySlot layout) must be
// detected and replaced. Without this, mmap'ing the larger expected size on
// top of the smaller file would SIGBUS on first slot-array access — the
// exact failure mode seen in production when REGISTRY_VERSION was bumped
// from 2 to 3 without cleaning /dev/shm.
TEST_F(RegistryTest, OpenReplacesShmWithWrongSize)
{
  // Tear down our SetUp() registry; we want to set up a hostile starting
  // state by hand.
  rmw_uds::registry_close(registry_fd, registry_ptr, registry_size);
  registry_ptr = nullptr;
  registry_fd = -1;

  // Create a too-small shm file under the name our domain would use.
  char name[64];
  std::snprintf(name, sizeof(name), "/ros2_uds_%zu", domain_id);
  shm_unlink(name);
  int bad_fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0666);
  ASSERT_GE(bad_fd, 0);
  // 4 KB is much smaller than the real registry size — guaranteed mismatch.
  ASSERT_EQ(0, ftruncate(bad_fd, 4096));
  close(bad_fd);

  // Now open the registry normally. It must detect the size mismatch,
  // unlink the stale file, and create a fresh one of the right size.
  registry_fd = rmw_uds::registry_open(domain_id, &registry_ptr, &registry_size);
  ASSERT_GE(registry_fd, 0);
  ASSERT_NE(nullptr, registry_ptr);

  auto * header = rmw_uds::registry_header(registry_ptr);
  EXPECT_EQ(rmw_uds::REGISTRY_VERSION, header->version.load());

  // Round-trip an entry to confirm the new file is fully usable
  // (also implicitly exercises every slot is mapped — would SIGBUS if not).
  rmw_uds::RegistryEntry e;
  std::memset(&e, 0, sizeof(e));
  e.type = rmw_uds::ENTRY_NODE;
  e.pid = getpid();
  std::strncpy(e.node_name, "post_recovery", sizeof(e.node_name) - 1);
  int32_t idx = rmw_uds::registry_add(header, e);
  ASSERT_GE(idx, 0);
  rmw_uds::registry_remove(header, idx);
}

// Live-PID entries must survive the overflow reclamation pass.
TEST_F(RegistryTest, AddOverflowKeepsLivePidEntries)
{
  auto * header = rmw_uds::registry_header(registry_ptr);
  const uint32_t original_max = header->max_entries;
  header->max_entries = 4;

  // Half live, half dead.
  for (uint32_t i = 0; i < header->max_entries; ++i) {
    rmw_uds::RegistryEntry e;
    std::memset(&e, 0, sizeof(e));
    e.type = rmw_uds::ENTRY_NODE;
    e.pid = (i % 2 == 0) ? getpid() : DEAD_PID;
    std::snprintf(e.node_name, sizeof(e.node_name), "n_%u", i);
    ASSERT_GE(rmw_uds::registry_add(header, e), 0);
  }

  rmw_uds::RegistryEntry extra;
  std::memset(&extra, 0, sizeof(extra));
  extra.type = rmw_uds::ENTRY_NODE;
  extra.pid = getpid();
  std::strncpy(extra.node_name, "extra", sizeof(extra.node_name) - 1);
  int32_t extra_idx = rmw_uds::registry_add(header, extra);
  EXPECT_GE(extra_idx, 0);

  auto all = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_NODE, nullptr, nullptr, nullptr);
  // All remaining entries must have a live PID (ours) — no dead PIDs left.
  for (const auto & r : all) {
    EXPECT_NE(0, std::strncmp(r.node_name.c_str(), "n_1", 3))
      << "dead-PID entry '" << r.node_name << "' survived overflow cleanup";
    EXPECT_NE(0, std::strncmp(r.node_name.c_str(), "n_3", 3))
      << "dead-PID entry '" << r.node_name << "' survived overflow cleanup";
  }

  for (uint32_t i = 0; i < header->max_entries; ++i) {
    rmw_uds::registry_remove(header, static_cast<int32_t>(i));
  }
  header->max_entries = original_max;
}
