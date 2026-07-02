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

#include <cstdio>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "../src/transport.hpp"
#include "../src/types.hpp"

class TransportTest : public ::testing::Test
{
protected:
  size_t domain_id = 97;

  void SetUp() override
  {
    rmw_uds::ensure_socket_dir(domain_id);
  }
};

TEST_F(TransportTest, CreateBoundSocket)
{
  auto path = rmw_uds::make_socket_path(domain_id, "test");
  int fd = rmw_uds::create_bound_socket(path);
  ASSERT_GE(fd, 0);
  rmw_uds::close_socket(fd, path);
}

TEST_F(TransportTest, CreateSendSocket)
{
  int fd = rmw_uds::create_send_socket();
  ASSERT_GE(fd, 0);
  close(fd);
}

TEST_F(TransportTest, SendAndReceive)
{
  auto recv_path = rmw_uds::make_socket_path(domain_id, "recv");
  int recv_fd = rmw_uds::create_bound_socket(recv_path);
  ASSERT_GE(recv_fd, 0);

  int send_fd = rmw_uds::create_send_socket();
  ASSERT_GE(send_fd, 0);

  // Build a message
  rmw_uds::WireHeader send_hdr;
  std::memset(&send_hdr, 0, sizeof(send_hdr));
  send_hdr.sequence_number = 42;
  send_hdr.source_timestamp_ns = 123456789;
  send_hdr.msg_type = 0;

  std::vector<uint8_t> payload = {1, 2, 3, 4, 5};
  send_hdr.payload_size = static_cast<uint32_t>(payload.size());

  ASSERT_EQ(
    rmw_uds::SendResult::Ok,
    rmw_uds::send_to(
      send_fd, recv_path, send_hdr, payload.data(), payload.size()));

  // Receive
  rmw_uds::WireHeader recv_hdr;
  std::vector<uint8_t> recv_payload;
  ASSERT_TRUE(rmw_uds::recv_from(recv_fd, recv_hdr, recv_payload));

  EXPECT_EQ(42, recv_hdr.sequence_number);
  EXPECT_EQ(123456789, recv_hdr.source_timestamp_ns);
  EXPECT_EQ(0, recv_hdr.msg_type);
  EXPECT_EQ(payload, recv_payload);

  close(send_fd);
  rmw_uds::close_socket(recv_fd, recv_path);
}

TEST_F(TransportTest, RecvFromEmptyReturnsF)
{
  auto path = rmw_uds::make_socket_path(domain_id, "empty");
  int fd = rmw_uds::create_bound_socket(path);
  ASSERT_GE(fd, 0);

  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;
  EXPECT_FALSE(rmw_uds::recv_from(fd, hdr, payload));

  rmw_uds::close_socket(fd, path);
}

TEST_F(TransportTest, MultipleMessages)
{
  auto path = rmw_uds::make_socket_path(domain_id, "multi");
  int recv_fd = rmw_uds::create_bound_socket(path);
  int send_fd = rmw_uds::create_send_socket();
  ASSERT_GE(recv_fd, 0);
  ASSERT_GE(send_fd, 0);

  constexpr int N = 10;
  for (int i = 0; i < N; ++i) {
    rmw_uds::WireHeader hdr;
    std::memset(&hdr, 0, sizeof(hdr));
    hdr.sequence_number = i;
    hdr.payload_size = sizeof(int);
    hdr.msg_type = 0;
    ASSERT_EQ(
      rmw_uds::SendResult::Ok,
      rmw_uds::send_to(
        send_fd, path, hdr, reinterpret_cast<const uint8_t *>(&i), sizeof(i)));
  }

  for (int i = 0; i < N; ++i) {
    rmw_uds::WireHeader recv_hdr;
    std::vector<uint8_t> recv_payload;
    ASSERT_TRUE(rmw_uds::recv_from(recv_fd, recv_hdr, recv_payload));
    EXPECT_EQ(i, recv_hdr.sequence_number);
    int val;
    std::memcpy(&val, recv_payload.data(), sizeof(val));
    EXPECT_EQ(i, val);
  }

  close(send_fd);
  rmw_uds::close_socket(recv_fd, path);
}

TEST_F(TransportTest, LargePayloadRoundTrip)
{
  // 100 KB — a payload spanning many pages so the scatter-read is exercised
  // well past the header iovec, yet small enough to fit the default
  // net.core.wmem_max datagram cap so the test passes on stock kernels.
  auto path = rmw_uds::make_socket_path(domain_id, "large");
  int recv_fd = rmw_uds::create_bound_socket(path);
  int send_fd = rmw_uds::create_send_socket();
  ASSERT_GE(recv_fd, 0);
  ASSERT_GE(send_fd, 0);

  std::vector<uint8_t> payload(100 * 1024);
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<uint8_t>((i * 31 + 7) & 0xFF);
  }

  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  hdr.sequence_number = 7;
  hdr.payload_size = static_cast<uint32_t>(payload.size());
  ASSERT_EQ(
    rmw_uds::SendResult::Ok,
    rmw_uds::send_to(send_fd, path, hdr, payload.data(), payload.size()));

  rmw_uds::WireHeader recv_hdr;
  std::vector<uint8_t> recv_payload;
  ASSERT_TRUE(rmw_uds::recv_from(recv_fd, recv_hdr, recv_payload));
  EXPECT_EQ(7, recv_hdr.sequence_number);
  EXPECT_EQ(payload, recv_payload);

  close(send_fd);
  rmw_uds::close_socket(recv_fd, path);
}

TEST_F(TransportTest, EmptyPayloadHeaderOnly)
{
  // A header-only datagram (payload_size == 0) takes the one-entry iovec
  // branch on both the send and the receive side.
  auto path = rmw_uds::make_socket_path(domain_id, "hdronly");
  int recv_fd = rmw_uds::create_bound_socket(path);
  int send_fd = rmw_uds::create_send_socket();
  ASSERT_GE(recv_fd, 0);
  ASSERT_GE(send_fd, 0);

  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  hdr.sequence_number = 21;
  hdr.payload_size = 0;
  ASSERT_EQ(
    rmw_uds::SendResult::Ok,
    rmw_uds::send_to(send_fd, path, hdr, nullptr, 0));

  rmw_uds::WireHeader recv_hdr;
  std::vector<uint8_t> recv_payload = {0xAA};  // must come back empty
  ASSERT_TRUE(rmw_uds::recv_from(recv_fd, recv_hdr, recv_payload));
  EXPECT_EQ(21, recv_hdr.sequence_number);
  EXPECT_TRUE(recv_payload.empty());

  close(send_fd);
  rmw_uds::close_socket(recv_fd, path);
}

// Helper: send a raw datagram (no WireHeader) to a bound socket path.
static void send_raw(const std::string & dest, const void * data, size_t len)
{
  int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
  ASSERT_GE(fd, 0);
  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, dest.c_str(), sizeof(addr.sun_path) - 1);
  ASSERT_EQ(
    static_cast<ssize_t>(len),
    sendto(fd, data, len, 0, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)));
  close(fd);
}

TEST_F(TransportTest, EmptyDatagramConsumedNotStuck)
{
  // A zero-length datagram from a rogue local sender must be consumed, not
  // left at the head of the queue. SOCK_DGRAM is FIFO: if recv_from only
  // peeks it and returns false, every later message is stuck behind it and
  // a level-triggered epoll wakes the waiter forever.
  auto path = rmw_uds::make_socket_path(domain_id, "emptygram");
  int recv_fd = rmw_uds::create_bound_socket(path);
  ASSERT_GE(recv_fd, 0);

  send_raw(path, "", 0);

  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;
  EXPECT_FALSE(rmw_uds::recv_from(recv_fd, hdr, payload));

  // A valid message queued behind the empty datagram must still get through.
  int send_fd = rmw_uds::create_send_socket();
  ASSERT_GE(send_fd, 0);
  rmw_uds::WireHeader send_hdr;
  std::memset(&send_hdr, 0, sizeof(send_hdr));
  send_hdr.sequence_number = 11;
  std::vector<uint8_t> body = {9, 8, 7};
  send_hdr.payload_size = static_cast<uint32_t>(body.size());
  ASSERT_EQ(
    rmw_uds::SendResult::Ok,
    rmw_uds::send_to(send_fd, path, send_hdr, body.data(), body.size()));

  ASSERT_TRUE(rmw_uds::recv_from(recv_fd, hdr, payload));
  EXPECT_EQ(11, hdr.sequence_number);
  EXPECT_EQ(body, payload);

  close(send_fd);
  rmw_uds::close_socket(recv_fd, path);
}

TEST_F(TransportTest, RuntDatagramConsumedNotStuck)
{
  // Same property for a datagram shorter than WireHeader: dropped and
  // consumed, so the valid message behind it is still delivered.
  auto path = rmw_uds::make_socket_path(domain_id, "runt");
  int recv_fd = rmw_uds::create_bound_socket(path);
  ASSERT_GE(recv_fd, 0);

  const char junk[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  send_raw(path, junk, sizeof(junk));

  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;
  EXPECT_FALSE(rmw_uds::recv_from(recv_fd, hdr, payload));

  int send_fd = rmw_uds::create_send_socket();
  ASSERT_GE(send_fd, 0);
  rmw_uds::WireHeader send_hdr;
  std::memset(&send_hdr, 0, sizeof(send_hdr));
  send_hdr.sequence_number = 13;
  std::vector<uint8_t> body = {1, 2};
  send_hdr.payload_size = static_cast<uint32_t>(body.size());
  ASSERT_EQ(
    rmw_uds::SendResult::Ok,
    rmw_uds::send_to(send_fd, path, send_hdr, body.data(), body.size()));

  ASSERT_TRUE(rmw_uds::recv_from(recv_fd, hdr, payload));
  EXPECT_EQ(13, hdr.sequence_number);
  EXPECT_EQ(body, payload);

  close(send_fd);
  rmw_uds::close_socket(recv_fd, path);
}

// Helper: write an empty file with a specific name under the test domain dir.
// Returns the full path written.
static std::string write_fake_sock(size_t domain_id, const char * name)
{
  char path[256];
  std::snprintf(path, sizeof(path), "/tmp/ros2_uds/%zu/%s", domain_id, name);
  FILE * f = std::fopen(path, "w");
  if (f) {std::fclose(f);}
  return std::string(path);
}

static bool file_exists(const std::string & path)
{
  struct stat st;
  return stat(path.c_str(), &st) == 0;
}

TEST_F(TransportTest, CleanupOrphanFilesRemovesDeadPidFiles)
{
  // Create files shaped like make_socket_path() output:
  //   sub_<pid>_<hex>.sock
  // One with a dead PID (should be unlinked), one with our own PID (kept).
  const std::string dead = write_fake_sock(domain_id, "sub_2147483000_deadbeef.sock");
  char live_name[64];
  std::snprintf(live_name, sizeof(live_name), "cli_%d_cafebabe.sock", getpid());
  const std::string live = write_fake_sock(domain_id, live_name);

  ASSERT_TRUE(file_exists(dead));
  ASSERT_TRUE(file_exists(live));

  rmw_uds::cleanup_orphan_socket_files(domain_id);

  EXPECT_FALSE(file_exists(dead)) << "dead-PID socket file should have been unlinked";
  EXPECT_TRUE(file_exists(live)) << "live-PID socket file must not be unlinked";

  // Clean up the live one we created.
  unlink(live.c_str());
}

TEST_F(TransportTest, CleanupOrphanFilesIgnoresUnparsableNames)
{
  // A file that doesn't match our <prefix>_<pid>_<unique>.sock pattern
  // must be left alone. We don't want to unlink random files placed in
  // /tmp/ros2_uds/<N>/ by third parties.
  const std::string weird1 = write_fake_sock(domain_id, "noprefix");
  const std::string weird2 = write_fake_sock(domain_id, "prefix_notanumber_x.sock");
  const std::string weird3 = write_fake_sock(domain_id, "only_one_underscore.sock");

  ASSERT_TRUE(file_exists(weird1));
  ASSERT_TRUE(file_exists(weird2));
  ASSERT_TRUE(file_exists(weird3));

  rmw_uds::cleanup_orphan_socket_files(domain_id);

  EXPECT_TRUE(file_exists(weird1));
  EXPECT_TRUE(file_exists(weird2));
  EXPECT_TRUE(file_exists(weird3));

  unlink(weird1.c_str());
  unlink(weird2.c_str());
  unlink(weird3.c_str());
}

TEST_F(TransportTest, CleanupOrphanFilesMissingDirIsNoop)
{
  // Non-existent domain directory must not crash or error.
  rmw_uds::cleanup_orphan_socket_files(99999);
  SUCCEED();
}
