#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <sys/stat.h>
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

  ASSERT_TRUE(rmw_uds::send_to(
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
    ASSERT_TRUE(rmw_uds::send_to(
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
