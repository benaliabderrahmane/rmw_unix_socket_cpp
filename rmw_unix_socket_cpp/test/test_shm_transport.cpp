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

#include <cstring>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/shm_transport.hpp"

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
