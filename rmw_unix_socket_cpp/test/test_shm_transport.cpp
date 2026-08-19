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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/shm_transport.hpp"
#include "../src/types.hpp"  // WireHeader, for the tl_ring latched cache

class ShmTransportTest : public ::testing::Test
{
protected:
  size_t domain_id = 95;  // unique per test binary (96 is test_rmw_init's)
  rmw_uds::ShmRingWriter ring;
  rmw_uds::ShmReaderCache cache;

  void TearDown() override
  {
    rmw_uds::shm_writer_close(ring);
    rmw_uds::shm_reader_close(cache);
  }

  static std::vector<uint8_t> pattern(size_t n, uint8_t salt)
  {
    std::vector<uint8_t> v(n);
    for (size_t i = 0; i < n; ++i) {
      v[i] = static_cast<uint8_t>((i * 31 + salt) & 0xFF);
    }
    return v;
  }

  // Stage + build the descriptor datagram payload the way the wire delivers it.
  std::vector<uint8_t> stage(const std::vector<uint8_t> & payload)
  {
    rmw_uds::ShmPayloadDescriptor desc{};
    EXPECT_TRUE(
      rmw_uds::shm_stage_payload(
        ring, domain_id, payload.data(), payload.size(), desc));
    std::vector<uint8_t> wire(sizeof(desc));
    std::memcpy(wire.data(), &desc, sizeof(desc));
    return wire;
  }
};

TEST_F(ShmTransportTest, StageAndFetchRoundTrip)
{
  const auto payload = pattern(100 * 1024, 7);
  auto wire = stage(payload);

  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, wire));
  EXPECT_EQ(payload, wire);
}

TEST_F(ShmTransportTest, ManyMessagesEachFetchable)
{
  // Interleave staging and fetching the way a drain loop would see it.
  for (int i = 0; i < 20; ++i) {
    const auto payload = pattern(64 * 1024 + i, static_cast<uint8_t>(i));
    auto wire = stage(payload);
    ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, wire)) << "message " << i;
    EXPECT_EQ(payload, wire) << "message " << i;
  }
}

TEST_F(ShmTransportTest, LappedRecordIsCleanDrop)
{
  // 3 MB records force a ring of 4 records; the 5th stage laps the 1st.
  const auto first = pattern(3 * 1024 * 1024, 1);
  auto first_wire = stage(first);

  for (int i = 0; i < 4; ++i) {
    stage(pattern(3 * 1024 * 1024, static_cast<uint8_t>(10 + i)));
  }

  EXPECT_FALSE(rmw_uds::shm_fetch_payload(cache, domain_id, first_wire))
    << "a lapped record must be dropped, not delivered";
}

TEST_F(ShmTransportTest, RingGrowthInvalidatesOldSegmentName)
{
  const auto small = pattern(64 * 1024, 3);
  auto small_wire = stage(small);
  const std::string old_name = ring.shm_name;

  // A payload too big for the current ring forces a new, larger segment.
  const auto big = pattern(4 * 1024 * 1024, 4);
  auto big_wire = stage(big);
  EXPECT_NE(old_name, ring.shm_name);

  // The old segment name is unlinked, so a reader that never mapped it can't
  // resolve the old descriptor any more — a clean drop...
  EXPECT_FALSE(rmw_uds::shm_fetch_payload(cache, domain_id, small_wire));

  // ...while the new segment serves the new descriptor.
  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, big_wire));
  EXPECT_EQ(big, big_wire);
}

TEST_F(ShmTransportTest, MappedSegmentSurvivesWriterClose)
{
  // A reader that already mapped the segment keeps reading it after the
  // publisher is destroyed: unlink only removes the name, not the mapping.
  const auto payload = pattern(128 * 1024, 5);
  auto wire = stage(payload);
  auto wire_again = wire;

  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, wire));
  rmw_uds::shm_writer_close(ring);

  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, wire_again));
  EXPECT_EQ(payload, wire_again);
}

TEST_F(ShmTransportTest, HostileDescriptorRejected)
{
  const auto payload = pattern(64 * 1024, 6);
  auto wire = stage(payload);

  rmw_uds::ShmPayloadDescriptor desc;
  std::memcpy(&desc, wire.data(), sizeof(desc));

  // Offset beyond the ring: must be rejected by the bounds check, not read.
  auto bad = desc;
  bad.offset = 1ULL << 40;
  std::vector<uint8_t> bad_wire(sizeof(bad));
  std::memcpy(bad_wire.data(), &bad, sizeof(bad));
  EXPECT_FALSE(rmw_uds::shm_fetch_payload(cache, domain_id, bad_wire));

  // Size that overflows past the ring end from a valid offset.
  bad = desc;
  bad.payload_size = 0xFFFFFFFF;
  std::memcpy(bad_wire.data(), &bad, sizeof(bad));
  EXPECT_FALSE(rmw_uds::shm_fetch_payload(cache, domain_id, bad_wire));

  // Misaligned offset.
  bad = desc;
  bad.offset += 1;
  std::memcpy(bad_wire.data(), &bad, sizeof(bad));
  EXPECT_FALSE(rmw_uds::shm_fetch_payload(cache, domain_id, bad_wire));

  // A datagram that isn't descriptor-sized at all.
  std::vector<uint8_t> runt = {1, 2, 3};
  EXPECT_FALSE(rmw_uds::shm_fetch_payload(cache, domain_id, runt));
}

TEST_F(ShmTransportTest, StaleSeqIsCleanDrop)
{
  const auto payload = pattern(64 * 1024, 8);
  auto wire = stage(payload);

  rmw_uds::ShmPayloadDescriptor desc;
  std::memcpy(&desc, wire.data(), sizeof(desc));
  desc.seq += 2;  // pretend the descriptor is from a lapped generation
  std::memcpy(wire.data(), &desc, sizeof(desc));

  EXPECT_FALSE(rmw_uds::shm_fetch_payload(cache, domain_id, wire));
}

TEST_F(ShmTransportTest, CleanupRemovesDeadPidSegments)
{
  // Shape a segment name like a dead process left it behind (PID far above
  // pid_max) and one owned by us; cleanup must remove only the dead one.
  // Our own PID in the owner field keeps concurrent suite runs on one host
  // from racing each other on a shared fixed name.
  char dead_name[96];
  std::snprintf(dead_name, sizeof(dead_name),
    "/ros2_uds_data_%zu_2147483000_%d_1", domain_id, getpid());
  int fd = shm_open(dead_name, O_CREAT | O_RDWR, 0666);
  ASSERT_GE(fd, 0);
  close(fd);

  const auto payload = pattern(64 * 1024, 9);
  stage(payload);  // creates a live segment owned by this process

  rmw_uds::shm_cleanup_orphan_segments(domain_id);

  EXPECT_LT(shm_open(dead_name, O_RDONLY, 0), 0)
    << "dead-PID segment should have been unlinked";
  int live = shm_open(ring.shm_name.c_str(), O_RDONLY, 0);
  EXPECT_GE(live, 0) << "live segment must survive cleanup";
  if (live >= 0) {
    close(live);
  }
}

// --- reserve/commit/abort: the two-phase staging behind serialize-into-ring ---

TEST_F(ShmTransportTest, ReserveCommitRoundTrip)
{
  const auto payload = pattern(100 * 1024, 21);
  uint8_t * dst = rmw_uds::shm_stage_reserve(ring, domain_id, payload.size());
  ASSERT_NE(nullptr, dst);
  std::memcpy(dst, payload.data(), payload.size());

  rmw_uds::ShmPayloadDescriptor desc{};
  ASSERT_TRUE(rmw_uds::shm_stage_commit(ring, payload.size(), desc));

  std::vector<uint8_t> wire(sizeof(desc));
  std::memcpy(wire.data(), &desc, sizeof(desc));
  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, wire));
  EXPECT_EQ(payload, wire);
}

TEST_F(ShmTransportTest, CommitSmallerThanReservedPacksTight)
{
  // A size-walk overestimate must waste nothing: commit with the ACTUAL size
  // advances the cursor by the actual record, and the payload round-trips at
  // its actual length.
  const size_t reserved = 256 * 1024;
  const auto payload = pattern(100 * 1024, 22);  // actual << reserved

  uint8_t * dst = rmw_uds::shm_stage_reserve(ring, domain_id, reserved);
  ASSERT_NE(nullptr, dst);
  std::memcpy(dst, payload.data(), payload.size());
  const uint64_t offset_before = ring.next_offset;

  rmw_uds::ShmPayloadDescriptor desc{};
  ASSERT_TRUE(rmw_uds::shm_stage_commit(ring, payload.size(), desc));
  EXPECT_EQ(payload.size(), desc.payload_size);
  // Cursor advanced by the aligned ACTUAL record, not the reservation.
  EXPECT_LT(ring.next_offset - offset_before, reserved);

  std::vector<uint8_t> wire(sizeof(desc));
  std::memcpy(wire.data(), &desc, sizeof(desc));
  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, wire));
  EXPECT_EQ(payload, wire);
}

TEST_F(ShmTransportTest, AbortLeavesSlotReusableAndUnobservable)
{
  // Publish one good record, then reserve-and-abort at the next slot, then
  // publish again: the aborted slot is reused and the good records round-trip.
  const auto first = pattern(64 * 1024, 23);
  auto first_wire = stage(first);

  uint8_t * dst = rmw_uds::shm_stage_reserve(ring, domain_id, 64 * 1024);
  ASSERT_NE(nullptr, dst);
  const uint64_t aborted_offset = ring.next_offset;
  std::memset(dst, 0xEE, 1024);  // half-written garbage
  rmw_uds::shm_stage_abort(ring);
  EXPECT_EQ(aborted_offset, ring.next_offset) << "abort must not advance the cursor";

  const auto second = pattern(64 * 1024, 24);
  auto second_wire = stage(second);

  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, first_wire));
  EXPECT_EQ(first, first_wire);
  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, second_wire));
  EXPECT_EQ(second, second_wire);
}

TEST_F(ShmTransportTest, ReservedUncommittedSlotFailsStaleDescriptor)
{
  // A reader holding a descriptor for a record that was later overwritten by
  // an in-flight (reserved, uncommitted) writer must get a clean drop: the
  // slot's seqlock is odd for the whole reserve..commit window.
  const auto first = pattern(3 * 1024 * 1024, 25);
  auto first_wire = stage(first);

  // Fill the ring so the next reserve lands back on the first record's slot.
  for (int i = 0; i < 3; ++i) {
    stage(pattern(3 * 1024 * 1024, static_cast<uint8_t>(30 + i)));
  }
  uint8_t * dst = rmw_uds::shm_stage_reserve(ring, domain_id, 3 * 1024 * 1024);
  ASSERT_NE(nullptr, dst);

  EXPECT_FALSE(rmw_uds::shm_fetch_payload(cache, domain_id, first_wire))
    << "a descriptor into a reserved-but-uncommitted slot must drop cleanly";
  rmw_uds::shm_stage_abort(ring);
}

TEST_F(ShmTransportTest, CommitOverReservationIsRejected)
{
  uint8_t * dst = rmw_uds::shm_stage_reserve(ring, domain_id, 64 * 1024);
  ASSERT_NE(nullptr, dst);
  const uint64_t offset_before = ring.next_offset;
  const uint32_t index_before = ring.next_index;

  rmw_uds::ShmPayloadDescriptor desc{};
  EXPECT_FALSE(rmw_uds::shm_stage_commit(ring, 65 * 1024, desc))
    << "committing more than the reservation must be rejected, not published";
  // The rejection behaves like an abort: nothing advanced, nothing published.
  EXPECT_EQ(offset_before, ring.next_offset);
  EXPECT_EQ(index_before, ring.next_index);

  // The ring stays usable afterwards (the slot is reused).
  const auto payload = pattern(64 * 1024, 26);
  auto wire = stage(payload);
  ASSERT_TRUE(rmw_uds::shm_fetch_payload(cache, domain_id, wire));
  EXPECT_EQ(payload, wire);
}

// --- TRANSIENT_LOCAL latched cache (tl_ring) ---

static rmw_uds::WireHeader make_tl_header(int64_t seq)
{
  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  hdr.gid[0] = 0xAB;
  hdr.gid[8] = 0xCD;  // context-id byte: part of the full-GID validation
  hdr.sequence_number = seq;
  hdr.msg_type = 0;
  return hdr;
}

TEST_F(ShmTransportTest, TlRingLatchAndPullRoundTrip)
{
  rmw_uds::TlRingWriter ring;
  uint8_t gid[16] = {0xAB, 0, 0, 0, 0, 0, 0, 0, 0xCD, 0, 0, 0, 0, 0, 0, 0};
  ASSERT_TRUE(rmw_uds::tl_ring_create(ring, domain_id, gid, 5));

  std::unique_ptr<rmw_uds::DurableShmSegment> evicted;
  for (int64_t i = 1; i <= 8; ++i) {  // 8 > depth 5: oldest three lap out
    auto hdr = make_tl_header(i);
    std::vector<uint8_t> payload(64, static_cast<uint8_t>(i));
    hdr.payload_size = static_cast<uint32_t>(payload.size());
    ASSERT_TRUE(rmw_uds::tl_ring_latch(
        ring, hdr, payload.data(), payload.size(), nullptr, nullptr, evicted));
    EXPECT_EQ(nullptr, evicted);  // embedded records own no durable segment
  }

  std::vector<rmw_uds::TlPulledRecord> records;
  int64_t max_seq = 0;
  bool overlapped = true;
  ASSERT_TRUE(rmw_uds::tl_ring_pull(
      ring.shm_name, gid, 10, records, max_seq, &overlapped));
  EXPECT_FALSE(overlapped);  // no concurrent writer in this test
  EXPECT_EQ(8, max_seq);
  ASSERT_EQ(5u, records.size());
  for (size_t i = 0; i < records.size(); ++i) {
    EXPECT_EQ(static_cast<int64_t>(4 + i), records[i].sequence_number);
    EXPECT_EQ(static_cast<uint8_t>(4 + i), records[i].payload.at(0));
  }

  // Wrong expected GID (stale-segment defense): the pull must refuse.
  uint8_t wrong_gid[16] = {0xAB, 0, 0, 0, 0, 0, 0, 0, 0xEE, 0, 0, 0, 0, 0, 0, 0};
  EXPECT_FALSE(rmw_uds::tl_ring_pull(
      ring.shm_name, wrong_gid, 10, records, max_seq, nullptr));

  // Capture the name BEFORE close (close clears it): this drives the pull
  // through shm_open ENOENT — the destroyed-publisher path — instead of the
  // empty-name early return, verifying close actually unlinked the segment.
  const std::string name = ring.shm_name;
  rmw_uds::tl_ring_close(ring);
  EXPECT_FALSE(rmw_uds::tl_ring_pull(name, gid, 10, records, max_seq, nullptr));
}

TEST_F(ShmTransportTest, TlRingPullSkipsPoisonedSlotAndReturnsPromptly)
{
  // A publisher killed mid-latch leaves one slot's seqlock odd forever. The
  // pull must skip exactly that slot after bounded retries — never spin, and
  // never discard the rest of the history.
  rmw_uds::TlRingWriter ring;
  uint8_t gid[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  ASSERT_TRUE(rmw_uds::tl_ring_create(ring, domain_id, gid, 4));

  std::unique_ptr<rmw_uds::DurableShmSegment> evicted;
  for (int64_t i = 1; i <= 4; ++i) {
    auto hdr = make_tl_header(i);
    std::vector<uint8_t> payload(32, static_cast<uint8_t>(i));
    hdr.payload_size = static_cast<uint32_t>(payload.size());
    ASSERT_TRUE(rmw_uds::tl_ring_latch(
        ring, hdr, payload.data(), payload.size(), nullptr, nullptr, evicted));
  }

  // Poison slot 1 (record seq 2) the way a SIGKILL mid-write would: odd seq.
  auto * record = reinterpret_cast<rmw_uds::ShmRecordHeader *>(
    ring.base + 64 /* header area */ + 1 * ring.slot_bytes);
  record->seq.store(2 * 2 - 1, std::memory_order_release);

  const auto t0 = std::chrono::steady_clock::now();
  std::vector<rmw_uds::TlPulledRecord> records;
  int64_t max_seq = 0;
  bool overlapped = true;
  ASSERT_TRUE(rmw_uds::tl_ring_pull(
      ring.shm_name, gid, 10, records, max_seq, &overlapped));
  EXPECT_FALSE(overlapped) << "a dead writer's poisoned slot is not overlap";
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  EXPECT_LT(
    std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500)
    << "pull must not spin on a dead writer's odd seqlock";
  ASSERT_EQ(3u, records.size()) << "only the poisoned slot may be skipped";
  EXPECT_EQ(1, records[0].sequence_number);
  EXPECT_EQ(3, records[1].sequence_number);
  EXPECT_EQ(4, records[2].sequence_number);
  EXPECT_EQ(4, max_seq);

  rmw_uds::tl_ring_close(ring);
}

TEST_F(ShmTransportTest, TlRingPullFlagsOverlapWhenASkippedSlotIsFilled)
{
  // The dedup watermark is sound only if a sequence gap in the pulled result
  // implies overlapped: on overlap the subscriber keeps just the contiguous
  // prefix, and without it extends the watermark across the gap — dropping
  // the missing sample's in-flight datagram. Slots the scan SKIPS (never
  // written, or given up on after the bounded retries) produce no record, so
  // a writer filling one during the scan has to be detected all the same.
  //
  // The interleaving is forced, not raced: both threads share one CPU, so the
  // latching thread runs only when the scan yields — which it does at the
  // poisoned slot, after it has already passed the empty slots 2-5.
  cpu_set_t original;
  CPU_ZERO(&original);
  if (sched_getaffinity(0, sizeof(original), &original) != 0) {
    GTEST_SKIP() << "CPU affinity unavailable";
  }
  cpu_set_t single;
  CPU_ZERO(&single);
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &original)) {
      CPU_SET(cpu, &single);
      break;
    }
  }
  if (sched_setaffinity(0, sizeof(single), &single) != 0) {
    GTEST_SKIP() << "cannot pin to a single CPU";
  }

  uint8_t gid[16] = {0x5A, 1, 2, 3, 4, 5, 6, 7, 0xC3, 9, 10, 11, 12, 13, 14, 15};
  bool exercised = false;
  for (int attempt = 0; attempt < 20 && !exercised; ++attempt) {
    rmw_uds::TlRingWriter latched;
    ASSERT_TRUE(rmw_uds::tl_ring_create(latched, domain_id, gid, 8));
    std::unique_ptr<rmw_uds::DurableShmSegment> evicted;
    for (int64_t i = 1; i <= 2; ++i) {  // slots 0,1 — the cursor stops at 2
      auto hdr = make_tl_header(i);
      std::vector<uint8_t> payload(32, static_cast<uint8_t>(i));
      hdr.payload_size = static_cast<uint32_t>(payload.size());
      ASSERT_TRUE(rmw_uds::tl_ring_latch(
          latched, hdr, payload.data(), payload.size(), nullptr, nullptr, evicted));
    }
    // Slot 6 odd, exactly as sample 7 mid-write leaves it: the scan stalls
    // there, having already passed slots 2-5 as never-written.
    auto * poisoned = reinterpret_cast<rmw_uds::ShmRecordHeader *>(
      latched.base + 64 /* header area */ + 6 * latched.slot_bytes);
    poisoned->seq.store(7 * 2 - 1, std::memory_order_release);

    std::atomic<bool> go{false};
    std::thread latcher([&latched, &go]() {
        while (!go.load(std::memory_order_acquire)) {
        }
        std::unique_ptr<rmw_uds::DurableShmSegment> ev;
        for (int64_t i = 3; i <= 8; ++i) {  // slots 2-7; sample 7 clears slot 6
          auto hdr = make_tl_header(i);
          std::vector<uint8_t> payload(32, static_cast<uint8_t>(i));
          hdr.payload_size = static_cast<uint32_t>(payload.size());
          (void)rmw_uds::tl_ring_latch(
            latched, hdr, payload.data(), payload.size(), nullptr, nullptr, ev);
        }
      });

    std::vector<rmw_uds::TlPulledRecord> records;
    int64_t max_seq = 0;
    bool overlapped = false;
    go.store(true, std::memory_order_release);
    const bool pulled = rmw_uds::tl_ring_pull(
      latched.shm_name, gid, 10, records, max_seq, &overlapped);
    latcher.join();

    bool gap = false;
    for (size_t i = 1; i < records.size(); ++i) {
      if (records[i].sequence_number != records[i - 1].sequence_number + 1) {
        gap = true;
        break;
      }
    }
    if (pulled && gap) {
      exercised = true;
      EXPECT_TRUE(overlapped)
        << "a gap opened by skipping a slot the writer then filled must read "
        << "as overlap, or the watermark swallows the missing sample";
    }
    rmw_uds::tl_ring_close(latched);
  }
  (void)sched_setaffinity(0, sizeof(original), &original);
  if (!exercised) {
    GTEST_SKIP() << "the scan never observed a writer-created gap";
  }
}

TEST_F(ShmTransportTest, TlRingDepthClampAndNewestSuffix)
{
  // A depth beyond the byte cap clamps to the computed slot count (the stock
  // rosout depth of 1000 must fit un-clamped — that is what sized the cap),
  // and a fully-lapped ring replays exactly the newest slot-count suffix.
  const size_t stride = 64;  // SHM_RECORD_ALIGN
  const size_t slot_bytes =
    (sizeof(rmw_uds::ShmRecordHeader) + 37 + rmw_uds::TL_EMBED_CAP + stride - 1) &
    ~(stride - 1);
  const size_t expect_max = rmw_uds::TL_RING_MAX_BYTES / slot_bytes;
  ASSERT_GE(expect_max, 1000u) << "stock rosout depth must fit the byte cap";

  rmw_uds::TlRingWriter ring;
  uint8_t gid[16] = {9, 9, 9, 9, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};
  ASSERT_TRUE(rmw_uds::tl_ring_create(ring, domain_id, gid, 10000));
  EXPECT_EQ(expect_max, ring.slots) << "depth 10000 must clamp to the byte cap";

  // rosout-shaped depth: no clamp.
  rmw_uds::TlRingWriter rosout_ring;
  ASSERT_TRUE(rmw_uds::tl_ring_create(rosout_ring, domain_id, gid, 1000));
  EXPECT_EQ(1000u, rosout_ring.slots);
  rmw_uds::tl_ring_close(rosout_ring);

  std::unique_ptr<rmw_uds::DurableShmSegment> evicted;
  const int64_t total = static_cast<int64_t>(ring.slots) + 5;  // lap by 5
  for (int64_t i = 1; i <= total; ++i) {
    auto hdr = make_tl_header(i);
    uint8_t byte = static_cast<uint8_t>(i & 0xFF);
    hdr.payload_size = 1;
    ASSERT_TRUE(rmw_uds::tl_ring_latch(ring, hdr, &byte, 1, nullptr, nullptr, evicted));
  }
  std::vector<rmw_uds::TlPulledRecord> records;
  int64_t max_seq = 0;
  ASSERT_TRUE(rmw_uds::tl_ring_pull(
      ring.shm_name, gid, total + 10, records, max_seq, nullptr));
  EXPECT_EQ(total, max_seq);
  ASSERT_EQ(static_cast<size_t>(ring.slots), records.size());
  EXPECT_EQ(total - static_cast<int64_t>(ring.slots) + 1,
    records.front().sequence_number);
  EXPECT_EQ(total, records.back().sequence_number);
  rmw_uds::tl_ring_close(ring);
}

TEST_F(ShmTransportTest, TlRingEmbedCapBoundary)
{
  // Exactly TL_EMBED_CAP embeds; one byte over must be pre-staged durably by
  // the caller. An off-by-one here would make tl_ring_pull treat the record
  // length as torn and silently drop the latched sample.
  rmw_uds::TlRingWriter ring;
  uint8_t gid[16] = {7, 7, 7, 7, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0};
  ASSERT_TRUE(rmw_uds::tl_ring_create(ring, domain_id, gid, 4));

  std::unique_ptr<rmw_uds::DurableShmSegment> evicted;
  std::vector<uint8_t> at_cap(rmw_uds::TL_EMBED_CAP, 0x5A);
  auto hdr1 = make_tl_header(1);
  hdr1.payload_size = static_cast<uint32_t>(at_cap.size());
  ASSERT_TRUE(rmw_uds::tl_ring_latch(
      ring, hdr1, at_cap.data(), at_cap.size(), nullptr, nullptr, evicted));

  std::vector<uint8_t> over_cap(rmw_uds::TL_EMBED_CAP + 1, 0xA5);
  rmw_uds::ShmPayloadDescriptor desc;
  auto seg = rmw_uds::shm_stage_durable(
    domain_id, over_cap.data(), over_cap.size(), desc);
  ASSERT_NE(nullptr, seg);
  auto hdr2 = make_tl_header(2);
  hdr2.payload_size = static_cast<uint32_t>(over_cap.size());
  ASSERT_TRUE(rmw_uds::tl_ring_latch(
      ring, hdr2, over_cap.data(), over_cap.size(), &desc, std::move(seg), evicted));

  std::vector<rmw_uds::TlPulledRecord> records;
  int64_t max_seq = 0;
  ASSERT_TRUE(rmw_uds::tl_ring_pull(ring.shm_name, gid, 10, records, max_seq, nullptr));
  ASSERT_EQ(2u, records.size());
  EXPECT_EQ(at_cap, records[0].payload);  // embedded, byte-equal
  // Over-cap record carries the 32-byte descriptor + SHM flag; resolve it
  // through the same fetch path the subscription pull uses.
  EXPECT_EQ(sizeof(rmw_uds::ShmPayloadDescriptor), records[1].payload.size());
  rmw_uds::WireHeader hdr_out;
  std::memcpy(&hdr_out, records[1].wire_header, sizeof(hdr_out));
  EXPECT_TRUE(hdr_out.msg_type & rmw_uds::SHM_PAYLOAD_FLAG);
  rmw_uds::ShmReaderCache local_cache;
  std::vector<uint8_t> resolved = records[1].payload;
  ASSERT_TRUE(rmw_uds::shm_fetch_payload(local_cache, domain_id, resolved));
  EXPECT_EQ(over_cap, resolved);
  rmw_uds::shm_reader_close(local_cache);
  rmw_uds::tl_ring_close(ring);
}
