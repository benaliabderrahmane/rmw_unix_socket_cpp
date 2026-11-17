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

#include "test_base.hpp"

#include <cstring>

#include "test_msgs/msg/basic_types.hpp"
#include "test_msgs/msg/strings.hpp"
#include "test_msgs/msg/unbounded_sequences.hpp"

#include "rmw/event.h"
#include "rmw/events_statuses/events_statuses.h"
#include "rmw/qos_profiles.h"
#include "rmw/serialized_message.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "../src/types.hpp"  // UdsSubscription + WireHeader layouts only (no linked symbols)

class PubSubTest : public RmwUdsNodeTest
{
protected:
  rmw_publisher_t * pub = nullptr;
  rmw_subscription_t * sub = nullptr;
  const rosidl_message_type_support_t * ts = nullptr;
  rmw_qos_profile_t qos;

  void SetUp() override
  {
    RmwUdsNodeTest::SetUp();
    ts = rosidl_typesupport_cpp::get_message_type_support_handle<
      test_msgs::msg::BasicTypes>();
    std::memset(&qos, 0, sizeof(qos));
    qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    qos.depth = 10;
    qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
    qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
  }

  void TearDown() override
  {
    if (sub) { auto _r [[maybe_unused]] = rmw_destroy_subscription(node, sub); }
    if (pub) { auto _r [[maybe_unused]] = rmw_destroy_publisher(node, pub); }
    RmwUdsNodeTest::TearDown();
  }
};

TEST_F(PubSubTest, CreateDestroyPublisher)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/test_topic", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);
  EXPECT_STREQ("/test_topic", pub->topic_name);
  EXPECT_EQ(uds_id(), pub->implementation_identifier);
  EXPECT_FALSE(pub->can_loan_messages);
}

TEST_F(PubSubTest, CreateDestroySubscription)
{
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);
  EXPECT_STREQ("/test_topic", sub->topic_name);
  EXPECT_EQ(uds_id(), sub->implementation_identifier);
}

TEST_F(PubSubTest, PublishAndTake)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/roundtrip", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/roundtrip", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  // Publish
  test_msgs::msg::BasicTypes send_msg;
  send_msg.int32_value = 42;
  send_msg.float64_value = 3.14;
  send_msg.bool_value = true;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  // Take
  test_msgs::msg::BasicTypes recv_msg;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(42, recv_msg.int32_value);
  EXPECT_DOUBLE_EQ(3.14, recv_msg.float64_value);
  EXPECT_TRUE(recv_msg.bool_value);
}

TEST_F(PubSubTest, TakeWithInfo)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/info_topic", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/info_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::BasicTypes send_msg;
  send_msg.int32_value = 99;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  test_msgs::msg::BasicTypes recv_msg;
  bool taken = false;
  rmw_message_info_t info = rmw_get_zero_initialized_message_info();
  EXPECT_EQ(RMW_RET_OK, rmw_take_with_info(sub, &recv_msg, &taken, &info, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(99, recv_msg.int32_value);
  EXPECT_GT(info.source_timestamp, 0);
  EXPECT_GT(info.received_timestamp, 0);
  EXPECT_GE(info.publication_sequence_number, 1u);
}

TEST_F(PubSubTest, TakeEmptyReturnsFalse)
{
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/empty_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::BasicTypes recv_msg;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  EXPECT_FALSE(taken);
}

TEST_F(PubSubTest, MultipleMessages)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/multi", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/multi", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  constexpr int N = 5;
  for (int i = 0; i < N; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  for (int i = 0; i < N; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_EQ(i, recv.int32_value);
  }
}

TEST_F(PubSubTest, PublisherCountMatchedSubscriptions)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/count_test", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  size_t count = 0;
  EXPECT_EQ(RMW_RET_OK, rmw_publisher_count_matched_subscriptions(pub, &count));
  EXPECT_EQ(0u, count);

  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/count_test", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  EXPECT_EQ(RMW_RET_OK, rmw_publisher_count_matched_subscriptions(pub, &count));
  EXPECT_EQ(1u, count);
}

TEST_F(PubSubTest, PublisherGetGid)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/gid_test", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  rmw_gid_t gid;
  EXPECT_EQ(RMW_RET_OK, rmw_get_gid_for_publisher(pub, &gid));
  EXPECT_EQ(uds_id(), gid.implementation_identifier);

  // GID should not be all zeros
  bool all_zero = true;
  for (size_t i = 0; i < RMW_GID_STORAGE_SIZE; ++i) {
    if (gid.data[i] != 0) { all_zero = false; break; }
  }
  EXPECT_FALSE(all_zero);
}

TEST_F(PubSubTest, LargeMessageViaShmRing)
{
  // A payload well above SHM_PAYLOAD_THRESHOLD travels through the
  // publisher's /dev/shm ring: the datagram carries only a descriptor and
  // the subscriber copies the bytes out of the ring. This exercises the
  // full path — stage on publish, descriptor resolve on drain, deserialize
  // on take — through the public API.
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, seq_ts, "/large_topic", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, seq_ts, "/large_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::UnboundedSequences send_msg;
  send_msg.uint8_values.resize(300 * 1024);  // ~300 KB serialized
  for (size_t i = 0; i < send_msg.uint8_values.size(); ++i) {
    send_msg.uint8_values[i] = static_cast<uint8_t>((i * 13 + 5) & 0xFF);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  // The publisher must actually have created its ring segment.
  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  EXPECT_NE(nullptr, pub_data->shm_ring.base);

  test_msgs::msg::UnboundedSequences recv_msg;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(send_msg.uint8_values, recv_msg.uint8_values);
}

// Issue #84: a take hands its payload buffer back to the subscription and the
// next drain receives into it, instead of allocating and freeing one per
// message (glibc trimmed a large freed buffer, and the next message
// page-faulted it back in). Each message must still come out byte-exact at its
// own size, with nothing left over from the larger one before it. A ring
// payload of 512 KiB or more is read in place (#86): its descriptor lands in
// the drain's buffer, which then stays with the drain, so one buffer still
// cycles. (A smaller ring payload is copied into a second, reused buffer.)
TEST_F(PubSubTest, TakeReusesPayloadBufferAcrossMessages)
{
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, seq_ts, "/payload_reuse", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, seq_ts, "/payload_reuse", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(sub->data);

  const uint8_t * buffer = nullptr;
  // 60 KB and 1 KB ride inline datagrams, 600 KB the ring.
  for (size_t size : {60 * 1024, 1024, 600 * 1024, 60 * 1024}) {
    SCOPED_TRACE(size);
    test_msgs::msg::UnboundedSequences send_msg;
    send_msg.uint8_values.resize(size);
    for (size_t i = 0; i < size; ++i) {
      send_msg.uint8_values[i] = static_cast<uint8_t>((i * 7 + size) & 0xFF);
    }
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

    test_msgs::msg::UnboundedSequences recv_msg;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_EQ(send_msg.uint8_values, recv_msg.uint8_values);

    // The endpoint owns one buffer, the same every time: the taken message's,
    // now the one spare, or the drain's own after a ring message.
    std::lock_guard<std::mutex> drain_lock(sub_data->drain_mutex);
    std::lock_guard<std::mutex> lock(sub_data->queue_mutex);
    ASSERT_EQ(
      1u, sub_data->spare_payloads.size() + (sub_data->recv_payload.capacity() > 0 ? 1 : 0));
    const auto & held = sub_data->spare_payloads.empty() ?
      sub_data->recv_payload : sub_data->spare_payloads.back();
    EXPECT_GE(held.capacity(), 60u * 1024);
    if (!buffer) {
      buffer = held.data();
    }
    EXPECT_EQ(buffer, held.data());
  }
}

namespace
{
// A message whose bytes are a function of (tag, size), so a reused buffer that
// leaked bytes of an earlier message, or a message cut to the wrong size, shows.
test_msgs::msg::UnboundedSequences patterned(uint32_t tag, size_t size)
{
  test_msgs::msg::UnboundedSequences m;
  m.uint8_values.resize(size);
  for (size_t i = 0; i < size; ++i) {
    m.uint8_values[i] = static_cast<uint8_t>((i * 7 + tag * 31) & 0xFF);
  }
  if (size >= sizeof(tag)) {
    std::memcpy(m.uint8_values.data(), &tag, sizeof(tag));
  }
  return m;
}

// Every buffer the subscription owns outside a take: queued, spare, and the
// drain's receive buffer.
size_t owned_buffers(rmw_uds::UdsSubscription * s)
{
  std::lock_guard<std::mutex> drain_lock(s->drain_mutex);
  std::lock_guard<std::mutex> queue_lock(s->queue_mutex);
  return s->message_queue.size() + s->spare_payloads.size() +
         (s->recv_payload.capacity() > 0 ? 1 : 0);
}
}  // namespace

// Issue #84, KEEP_LAST overflow: the drain hands the dropped entry's buffer
// back and receives the next datagram into it, so a subscription that falls
// behind cycles through depth + 1 buffers instead of allocating one per
// message. Six messages are drained at depth 2 by rmw_wait without a take, so
// four are dropped; the two kept must come out byte-exact at their own sizes.
TEST_F(PubSubTest, KeepLastOverflowRecyclesDroppedPayload)
{
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  qos.depth = 2;
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, seq_ts, "/payload_overflow", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, seq_ts, "/payload_overflow", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(sub->data);

  // Ring and inline sizes mixed; under the 1 MiB ring floor in total, so none laps.
  const size_t sizes[6] = {200 * 1024, 1024, 100 * 1024, 2048, 150 * 1024, 70 * 1024};
  rmw_wait_set_t * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  for (uint32_t round = 0; round < 20; ++round) {
    SCOPED_TRACE(round);
    for (uint32_t i = 0; i < 6; ++i) {
      auto m = patterned(round * 6 + i, sizes[i]);
      ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
    }
    // One rmw_wait drains all six (the socket holds them all) without taking.
    rmw_subscriptions_t subs;
    void * sub_array[1] = {sub->data};
    subs.subscribers = sub_array;
    subs.subscriber_count = 1;
    rmw_time_t timeout{5, 0};
    ASSERT_EQ(RMW_RET_OK, rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &timeout));
    {
      // The last overflow handed its buffer back and the drain took it for
      // the next datagram: nothing spare, one buffer ready to receive into.
      std::lock_guard<std::mutex> drain_lock(sub_data->drain_mutex);
      std::lock_guard<std::mutex> queue_lock(sub_data->queue_mutex);
      EXPECT_EQ(2u, sub_data->message_queue.size());
      EXPECT_TRUE(sub_data->spare_payloads.empty());
      EXPECT_GT(sub_data->recv_payload.capacity(), 0u);
    }
    for (uint32_t i = 4; i < 6; ++i) {
      test_msgs::msg::UnboundedSequences recv_msg;
      bool taken = false;
      ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
      ASSERT_TRUE(taken);
      EXPECT_EQ(patterned(round * 6 + i, sizes[i]).uint8_values, recv_msg.uint8_values);
    }
    std::lock_guard<std::mutex> lock(sub_data->queue_mutex);
    EXPECT_EQ(2u, sub_data->spare_payloads.size());
  }
  EXPECT_EQ(3u, owned_buffers(sub_data));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
}

// A take that fails to deserialize still hands its buffer back: the recycle
// runs on every return path, not only the successful one.
TEST_F(PubSubTest, FailedTakeRecyclesPayload)
{
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, seq_ts, "/payload_corrupt", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(sub->data);

  int send_fd = ::socket(AF_UNIX, SOCK_DGRAM, 0);
  ASSERT_GE(send_fd, 0);
  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, sub_data->socket_path.c_str(), sizeof(addr.sun_path) - 1);
  // An inline datagram whose payload is not CDR (bad encapsulation header).
  std::vector<uint8_t> payload(40 * 1024, 0xFF);
  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  hdr.payload_size = static_cast<uint32_t>(payload.size());
  hdr.msg_type = 0;
  std::vector<uint8_t> dgram(sizeof(hdr) + payload.size());
  std::memcpy(dgram.data(), &hdr, sizeof(hdr));
  std::memcpy(dgram.data() + sizeof(hdr), payload.data(), payload.size());

  for (int with_info = 0; with_info < 2; ++with_info) {
    SCOPED_TRACE(with_info);
    ASSERT_EQ(
      static_cast<ssize_t>(dgram.size()),
      ::sendto(send_fd, dgram.data(), dgram.size(), 0,
        reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)));
    test_msgs::msg::UnboundedSequences recv_msg;
    bool taken = false;
    rmw_message_info_t info;
    if (with_info) {
      EXPECT_NE(RMW_RET_OK, rmw_take_with_info(sub, &recv_msg, &taken, &info, nullptr));
    } else {
      EXPECT_NE(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
    }
    EXPECT_FALSE(taken);
    rcutils_reset_error();
    std::lock_guard<std::mutex> lock(sub_data->queue_mutex);
    ASSERT_EQ(1u, sub_data->spare_payloads.size());
    EXPECT_GE(sub_data->spare_payloads.back().capacity(), payload.size());
  }
  ::close(send_fd);
}

// The listener thread drains while three threads take through three take
// paths, against a publisher of mixed inline and ring sizes at depth 4. Spares
// move between the drain (drain_mutex + queue_mutex) and the takes
// (queue_mutex) on four threads: every message taken must be byte-exact at its
// own size, and the buffers owned stay within the bound DESIGN.md gives.
// Run under TSan by the sanitizer job.
TEST_F(PubSubTest, ConcurrentTakesAndListenerDrainRecycleSafely)
{
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  qos.depth = 4;
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, seq_ts, "/payload_stress", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, seq_ts, "/payload_stress", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(sub->data);
  auto no_op = [](const void *, size_t) {};
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_set_on_new_message_callback(sub, no_op, nullptr));

  const size_t sizes[5] = {100, 70 * 1024, 1024, 300 * 1024, 10 * 1024};
  std::atomic<bool> stop{false};
  std::atomic<size_t> good{0};
  std::atomic<size_t> bad{0};
  auto check = [&](const test_msgs::msg::UnboundedSequences & got) {
      uint32_t tag = 0;
      if (got.uint8_values.size() >= sizeof(tag)) {
        std::memcpy(&tag, got.uint8_values.data(), sizeof(tag));
      }
      if (got.uint8_values == patterned(tag, sizes[tag % 5]).uint8_values) {
        ++good;
      } else {
        ++bad;
      }
    };
  std::vector<std::thread> takers;
  takers.emplace_back([&] {
      while (!stop) {
        test_msgs::msg::UnboundedSequences m;
        bool taken = false;
        if (rmw_take(sub, &m, &taken, nullptr) == RMW_RET_OK && taken) {check(m);}
      }
    });
  takers.emplace_back([&] {
      while (!stop) {
        test_msgs::msg::UnboundedSequences m;
        rmw_message_info_t info;
        bool taken = false;
        if (rmw_take_with_info(sub, &m, &taken, &info, nullptr) == RMW_RET_OK && taken) {
          check(m);
        }
      }
    });
  takers.emplace_back([&] {
      rcutils_allocator_t allocator = rcutils_get_default_allocator();
      rmw_serialized_message_t ser = rmw_get_zero_initialized_serialized_message();
      if (rmw_serialized_message_init(&ser, 0, &allocator) != RMW_RET_OK) {
        ++bad;
        return;
      }
      while (!stop) {
        bool taken = false;
        if (rmw_take_serialized_message(sub, &ser, &taken, nullptr) == RMW_RET_OK && taken) {
          test_msgs::msg::UnboundedSequences m;
          if (rmw_deserialize(&ser, seq_ts, &m) == RMW_RET_OK) {check(m);} else {++bad;}
        }
      }
      auto r [[maybe_unused]] = rmw_serialized_message_fini(&ser);
    });

  for (uint32_t tag = 0; tag < 500; ++tag) {
    auto m = patterned(tag, sizes[tag % 5]);
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));  // not ASSERT: takers must be joined
    if (tag % 50 == 49) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop = true;
  for (auto & t : takers) {t.join();}
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_set_on_new_message_callback(sub, nullptr, nullptr));

  EXPECT_EQ(0u, bad.load());
  EXPECT_GT(good.load(), 0u);
  // Queue depth, plus one per thread that took at once, plus the drain's own.
  EXPECT_LE(owned_buffers(sub_data), qos.depth + takers.size() + 1);
}

// ignore_local_publications on the shm path. drain_endpoint() tests
// is_same_context() before it resolves the descriptor, because the sender's
// context id is already in WireHeader::gid and an ignored payload should never
// be mapped and copied out of the ring. The small-payload ignore_local tests
// never reach that branch, since an inline payload has no descriptor to skip.
TEST_F(PubSubTest, IgnoreLocalPublicationsDropsLargeSameContextMessage)
{
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, seq_ts, "/large_ignore_local", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub_opts.ignore_local_publications = true;
  sub = rmw_create_subscription(node, seq_ts, "/large_ignore_local", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::UnboundedSequences send_msg;
  send_msg.uint8_values.resize(300 * 1024);  // over SHM_PAYLOAD_THRESHOLD
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  // It really did go through the ring, so the drop below is the shm path.
  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  EXPECT_NE(nullptr, pub_data->shm_ring.base);

  test_msgs::msg::UnboundedSequences recv_msg;
  bool taken = true;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  EXPECT_FALSE(taken) <<
    "ignore_local_publications=true must drop a same-context shm payload";
}

TEST_F(PubSubTest, StringMessages)
{
  auto str_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::Strings>();
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, str_ts, "/str_topic", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, str_ts, "/str_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::Strings send_msg;
  send_msg.string_value = "Hello from Unix sockets!";
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  test_msgs::msg::Strings recv_msg;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ("Hello from Unix sockets!", recv_msg.string_value);
}

// Regression for the rmw_take_sequence cursor bug: a deserialize failure in the
// middle of a batch must not leave an uninitialized hole or miscount size — each
// success is written at the *taken cursor, not the loop index. We inject
// [good, corrupt, good] straight into the subscription's datagram socket via raw
// POSIX sendto (a single SOCK_DGRAM sender preserves order, so the corrupt one
// lands between the two good ones). Before the fix the second good message was
// written at data[2] and lost, while size == *taken == 2 exposed data[1] (the
// hole from the failed deserialize) to the consumer as a valid message.
TEST_F(PubSubTest, TakeSequenceSkipsMidBatchCorruptContiguously)
{
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/take_seq_corrupt", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(sub->data);
  ASSERT_FALSE(sub_data->socket_path.empty());

  // A valid wire payload via the public rmw_serialize API (identical CDR bytes
  // to what a publisher emits, so the take path deserializes it cleanly).
  test_msgs::msg::BasicTypes good;
  good.int32_value = 4242;
  good.bool_value = true;
  rcutils_allocator_t allocator = rcutils_get_default_allocator();
  rmw_serialized_message_t good_ser = rmw_get_zero_initialized_serialized_message();
  ASSERT_EQ(RMW_RET_OK, rmw_serialized_message_init(&good_ser, 0, &allocator));
  ASSERT_EQ(RMW_RET_OK, rmw_serialize(&good, ts, &good_ser));

  int send_fd = ::socket(AF_UNIX, SOCK_DGRAM, 0);
  ASSERT_GE(send_fd, 0);
  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, sub_data->socket_path.c_str(), sizeof(addr.sun_path) - 1);

  // Each datagram is the packed WireHeader followed by the payload (recv_from's framing).
  auto inject = [&](const uint8_t * payload, size_t len) {
    rmw_uds::WireHeader hdr;
    std::memset(&hdr, 0, sizeof(hdr));
    hdr.payload_size = static_cast<uint32_t>(len);
    hdr.msg_type = 0;  // topic message
    std::vector<uint8_t> dgram(sizeof(hdr) + len);
    std::memcpy(dgram.data(), &hdr, sizeof(hdr));
    if (len > 0) {std::memcpy(dgram.data() + sizeof(hdr), payload, len);}
    ASSERT_EQ(
      static_cast<ssize_t>(dgram.size()),
      ::sendto(send_fd, dgram.data(), dgram.size(), 0,
        reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)));
  };

  const uint8_t corrupt[4] = {0xDE, 0xAD, 0xBE, 0xEF};  // too short to deserialize
  inject(good_ser.buffer, good_ser.buffer_length);
  inject(corrupt, sizeof(corrupt));
  inject(good_ser.buffer, good_ser.buffer_length);

  std::this_thread::sleep_for(std::chrono::milliseconds(50));  // defensive; datagrams already buffered

  test_msgs::msg::BasicTypes out[3];
  void * out_ptrs[3] = {&out[0], &out[1], &out[2]};
  rmw_message_sequence_t seq;
  seq.data = out_ptrs;
  seq.size = 0;
  seq.capacity = 3;
  seq.allocator = nullptr;

  rmw_message_info_t infos[3];
  std::memset(infos, 0, sizeof(infos));
  rmw_message_info_sequence_t info_seq;
  info_seq.data = infos;
  info_seq.size = 0;
  info_seq.capacity = 3;
  info_seq.allocator = nullptr;

  size_t taken = 99;
  EXPECT_EQ(RMW_RET_OK, rmw_take_sequence(sub, 3, &seq, &info_seq, &taken, nullptr));

  EXPECT_EQ(2u, taken);
  EXPECT_EQ(2u, seq.size);
  EXPECT_EQ(2u, info_seq.size);
  EXPECT_EQ(4242, out[0].int32_value);
  EXPECT_EQ(4242, out[1].int32_value);  // the bug left this a hole and lost it

  ::close(send_fd);
  EXPECT_EQ(RMW_RET_OK, rmw_serialized_message_fini(&good_ser));
}

// subscription_options.ignore_local_publications is enforced in
// drain_endpoint() (drain.cpp), which drops any message whose
// sender context id — embedded in WireHeader::gid by UdsGid::generate() —
// matches the subscription's owning rmw_context_t, before it reaches the queue.
TEST_F(PubSubTest, IgnoreLocalPublicationsDropsSameProcessMessage)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/ignore_local", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Subscriber that wants to ignore messages from publishers in this process.
  auto ignoring_sub_opts = rmw_get_default_subscription_options();
  ignoring_sub_opts.ignore_local_publications = true;
  sub = rmw_create_subscription(node, ts, "/ignore_local", &qos, &ignoring_sub_opts);
  ASSERT_NE(nullptr, sub);

  // Baseline subscriber with default options, same topic, same process: it
  // must still receive the message so we know the publish itself worked.
  auto default_sub_opts = rmw_get_default_subscription_options();
  rmw_subscription_t * baseline_sub = rmw_create_subscription(
    node, ts, "/ignore_local", &qos, &default_sub_opts);
  ASSERT_NE(nullptr, baseline_sub);

  test_msgs::msg::BasicTypes send_msg;
  send_msg.int32_value = 123;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  test_msgs::msg::BasicTypes recv_msg;
  bool taken = false;

  EXPECT_EQ(RMW_RET_OK, rmw_take(baseline_sub, &recv_msg, &taken, nullptr));
  EXPECT_TRUE(taken) << "baseline subscriber (ignore_local_publications=false) "
                        "should still receive the same-process publication";

  taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  EXPECT_FALSE(taken) << "ignore_local_publications=true must drop messages "
                          "published from within the same process/context";

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, baseline_sub));
}

// Issue #86: a subscription queues a large payload's descriptor and its take
// deserializes straight from the publisher's ring record. A record the
// publisher overwrote before the take is reported as message_lost, never
// delivered.
class RingTakeTest : public PubSubTest
{
protected:
  const rosidl_message_type_support_t * seq_ts =
    rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  std::vector<rmw_subscription_t *> extra_subs;
  std::vector<rmw_event_t> events;

  void TearDown() override
  {
    for (auto & ev : events) {(void)rmw_event_fini(&ev);}
    for (auto * s : extra_subs) {(void)rmw_destroy_subscription(node, s);}
    PubSubTest::TearDown();
  }

  void make_pub(const char * topic)
  {
    auto pub_opts = rmw_get_default_publisher_options();
    pub = rmw_create_publisher(node, seq_ts, topic, &qos, &pub_opts);
    ASSERT_NE(nullptr, pub);
  }

  // A subscription with its message_lost event, which owns both.
  rmw_subscription_t * make_sub(const char * topic)
  {
    auto sub_opts = rmw_get_default_subscription_options();
    rmw_subscription_t * s = rmw_create_subscription(node, seq_ts, topic, &qos, &sub_opts);
    if (s) {
      extra_subs.push_back(s);
      events.reserve(8);  // the handles must not move once initialised
      events.push_back(rmw_get_zero_initialized_event());
      if (rmw_subscription_event_init(&events.back(), s, RMW_EVENT_MESSAGE_LOST) != RMW_RET_OK) {
        return nullptr;
      }
    }
    return s;
  }

  size_t lost(size_t index)
  {
    rmw_message_lost_status_t st{};
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take_event(&events[index], &st, &taken));
    return st.total_count;
  }

  // Drain the subscriptions into their queues, as an executor's rmw_wait does.
  void drain_all()
  {
    std::vector<void *> arr;
    for (auto * s : extra_subs) {arr.push_back(s->data);}
    rmw_wait_set_t * ws = rmw_create_wait_set(&context, arr.size());
    ASSERT_NE(nullptr, ws);
    rmw_subscriptions_t subs{arr.size(), arr.data()};
    rmw_time_t timeout{1, 0};
    EXPECT_EQ(RMW_RET_OK, rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &timeout));
    EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  }

  static test_msgs::msg::UnboundedSequences make_msg(size_t size, uint8_t seed)
  {
    test_msgs::msg::UnboundedSequences m;
    m.uint8_values.resize(size);
    for (size_t i = 0; i < size; ++i) {
      m.uint8_values[i] = static_cast<uint8_t>((i * 31 + seed) & 0xFF);
    }
    return m;
  }

  void publish(const test_msgs::msg::UnboundedSequences & m)
  {
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
  }

  static bool queued_in_ring(rmw_subscription_t * s)
  {
    auto * data = static_cast<rmw_uds::UdsSubscription *>(s->data);
    std::lock_guard<std::mutex> lock(data->queue_mutex);
    return !data->message_queue.empty() &&
           (data->message_queue.back().header.msg_type & rmw_uds::SHM_PAYLOAD_FLAG) &&
           data->message_queue.back().payload.capacity() == 0;
  }

  bool take_serialized(rmw_subscription_t * s, test_msgs::msg::UnboundedSequences & out)
  {
    rmw_serialized_message_t ser = rmw_get_zero_initialized_serialized_message();
    rcutils_allocator_t alloc = rcutils_get_default_allocator();
    EXPECT_EQ(RMW_RET_OK, rmw_serialized_message_init(&ser, 0, &alloc));
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take_serialized_message(s, &ser, &taken, nullptr));
    if (taken) {
      EXPECT_EQ(RMW_RET_OK, rmw_deserialize(&ser, seq_ts, &out));
    }
    EXPECT_EQ(RMW_RET_OK, rmw_serialized_message_fini(&ser));
    return taken;
  }
};

TEST_F(RingTakeTest, LargePayloadsAreByteExactFrom64KiBTo8MiB)
{
  make_pub("/ring_take_sizes");
  rmw_subscription_t * deser = make_sub("/ring_take_sizes");
  rmw_subscription_t * ser = make_sub("/ring_take_sizes");
  ASSERT_NE(nullptr, deser);
  ASSERT_NE(nullptr, ser);

  uint8_t seed = 1;
  for (size_t size : {64 * 1024, 256 * 1024, 1024 * 1024, 3 * 1024 * 1024 + 17,
      8 * 1024 * 1024})
  {
    SCOPED_TRACE(size);
    const auto sent = make_msg(size, seed++);
    publish(sent);
    drain_all();
    // Queued as a descriptor, the bytes still in the publisher's ring.
    EXPECT_TRUE(queued_in_ring(deser));
    EXPECT_TRUE(queued_in_ring(ser));

    test_msgs::msg::UnboundedSequences got;
    rmw_message_info_t info = rmw_get_zero_initialized_message_info();
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take_with_info(deser, &got, &taken, &info, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_TRUE(sent.uint8_values == got.uint8_values);

    test_msgs::msg::UnboundedSequences got_ser;
    ASSERT_TRUE(take_serialized(ser, got_ser));
    EXPECT_TRUE(sent.uint8_values == got_ser.uint8_values);
  }
  EXPECT_EQ(0u, lost(0));
  EXPECT_EQ(0u, lost(1));
}

// The publisher laps its ring (four records of 1 MiB) while ten messages sit
// queued. Every take path delivers only the records still intact, newest
// last and byte-exact, and reports the rest through message_lost.
TEST_F(RingTakeTest, ALapWhileQueuedIsReportedLostNeverDelivered)
{
  make_pub("/ring_take_lap");
  rmw_subscription_t * one = make_sub("/ring_take_lap");
  rmw_subscription_t * seq = make_sub("/ring_take_lap");
  rmw_subscription_t * ser = make_sub("/ring_take_lap");
  ASSERT_NE(nullptr, one);
  ASSERT_NE(nullptr, seq);
  ASSERT_NE(nullptr, ser);

  constexpr size_t kSize = 1024 * 1024;
  constexpr int kCount = 10;
  for (int i = 1; i <= kCount; ++i) {
    publish(make_msg(kSize, static_cast<uint8_t>(i)));
    drain_all();  // each descriptor is queued while its record is intact
  }

  auto check_tail = [&](const std::vector<test_msgs::msg::UnboundedSequences> & got,
      size_t lost_count) {
      EXPECT_GE(got.size(), 4u) << "the ring keeps at least four records";
      EXPECT_LT(got.size(), static_cast<size_t>(kCount)) << "the publisher lapped the ring";
      EXPECT_EQ(static_cast<size_t>(kCount) - got.size(), lost_count);
      for (size_t k = 0; k < got.size(); ++k) {
        const int i = kCount - static_cast<int>(got.size()) + 1 + static_cast<int>(k);
        EXPECT_TRUE(make_msg(kSize, static_cast<uint8_t>(i)).uint8_values == got[k].uint8_values)
          << "message " << i;
      }
    };

  std::vector<test_msgs::msg::UnboundedSequences> got;
  for (;;) {
    test_msgs::msg::UnboundedSequences m;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(one, &m, &taken, nullptr));
    if (!taken) {break;}
    got.push_back(std::move(m));
  }
  check_tail(got, lost(0));

  {
    test_msgs::msg::UnboundedSequences msgs[kCount];
    void * ptrs[kCount];
    for (int k = 0; k < kCount; ++k) {ptrs[k] = &msgs[k];}
    rmw_message_sequence_t mseq{ptrs, 0, kCount};
    rmw_message_info_t infos[kCount];
    rmw_message_info_sequence_t iseq{infos, 0, kCount};
    size_t taken = 0;
    ASSERT_EQ(RMW_RET_OK, rmw_take_sequence(seq, kCount, &mseq, &iseq, &taken, nullptr));
    EXPECT_EQ(taken, mseq.size);
    for (size_t k = 1; k < taken; ++k) {
      EXPECT_EQ(infos[k - 1].publication_sequence_number + 1, infos[k].publication_sequence_number);
    }
    check_tail(std::vector<test_msgs::msg::UnboundedSequences>(msgs, msgs + taken), lost(1));
  }

  got.clear();
  for (;;) {
    test_msgs::msg::UnboundedSequences m;
    if (!take_serialized(ser, m)) {break;}
    got.push_back(std::move(m));
  }
  check_tail(got, lost(2));

  // Nothing is left pinned: every mapping is still the publisher's live ring.
  for (auto * s : extra_subs) {
    auto * data = static_cast<rmw_uds::UdsSubscription *>(s->data);
    std::lock_guard<std::mutex> lock(data->shm_cache.mutex);
    for (auto & entry : data->shm_cache.mappings) {
      EXPECT_EQ(0u, entry.second.pins);
    }
  }
}

// The ring grows (new segment, old one unlinked) while descriptors into the
// old segment are queued. The publisher never writes the old segment again,
// so the subscription keeps it mapped until they are taken, delivers them,
// and then unmaps it.
TEST_F(RingTakeTest, QueuedMessagesSurviveARingRegrowth)
{
  make_pub("/ring_take_regrow");
  rmw_subscription_t * s = make_sub("/ring_take_regrow");
  ASSERT_NE(nullptr, s);
  auto * data = static_cast<rmw_uds::UdsSubscription *>(s->data);
  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);

  const auto a = make_msg(256 * 1024, 11);
  const auto b = make_msg(256 * 1024, 12);
  const auto c = make_msg(2 * 1024 * 1024, 13);  // outgrows the 1 MiB ring
  publish(a);
  drain_all();
  publish(b);
  drain_all();
  const uint64_t first_segment = pub_data->shm_ring.segment_id;
  publish(c);
  ASSERT_NE(first_segment, pub_data->shm_ring.segment_id) << "the ring did not grow";
  drain_all();
  {
    std::lock_guard<std::mutex> lock(data->shm_cache.mutex);
    EXPECT_EQ(2u, data->shm_cache.mappings.size()) << "the old segment is still held";
  }

  for (const auto * sent : {&a, &b, &c}) {
    test_msgs::msg::UnboundedSequences got;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(s, &got, &taken, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_TRUE(sent->uint8_values == got.uint8_values);
  }
  EXPECT_EQ(0u, lost(0));
  std::lock_guard<std::mutex> lock(data->shm_cache.mutex);
  ASSERT_EQ(1u, data->shm_cache.mappings.size()) << "the old segment is released";
  EXPECT_EQ(pub_data->shm_ring.segment_id, data->shm_cache.mappings.begin()->first.segment_id);
}

// Nested types deserialized in place: a sequence of structs and of strings.
TEST_F(RingTakeTest, ASequenceOfStructsDeserializesFromTheRing)
{
  make_pub("/ring_take_structs");
  rmw_subscription_t * s = make_sub("/ring_take_structs");
  ASSERT_NE(nullptr, s);

  test_msgs::msg::UnboundedSequences sent;
  for (int i = 0; i < 3000; ++i) {
    test_msgs::msg::BasicTypes b;
    b.bool_value = (i % 2) == 0;
    b.char_value = static_cast<uint8_t>(i);
    b.float64_value = i * 0.5;
    b.int16_value = static_cast<int16_t>(-i);
    b.uint32_value = static_cast<uint32_t>(i) * 7u;
    b.int64_value = -static_cast<int64_t>(i) * 1000003;
    sent.basic_types_values.push_back(b);
    sent.string_values.push_back(std::string(static_cast<size_t>(i % 37), 'a' + (i % 26)));
  }
  publish(sent);
  drain_all();
  EXPECT_TRUE(queued_in_ring(s));

  test_msgs::msg::UnboundedSequences got;
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take(s, &got, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_TRUE(sent == got);
}

// KEEP_ALL is outside the ring-bounded semantics: such a subscription copies
// a large payload out when it drains it, so a record the publisher overwrites
// later is still delivered.
TEST_F(RingTakeTest, AKeepAllSubscriptionLosesNothingItDrained)
{
  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_ALL;
  qos.depth = 0;  // as rclcpp's KeepAll; resolves to 10
  make_pub("/ring_take_keep_all");
  rmw_subscription_t * s = make_sub("/ring_take_keep_all");
  ASSERT_NE(nullptr, s);

  constexpr size_t kSize = 1024 * 1024;
  constexpr int kCount = 10;
  for (int i = 1; i <= kCount; ++i) {
    publish(make_msg(kSize, static_cast<uint8_t>(i)));
    drain_all();
    EXPECT_FALSE(queued_in_ring(s)) << "copied out at the drain";
  }
  for (int i = 1; i <= kCount; ++i) {
    test_msgs::msg::UnboundedSequences got;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(s, &got, &taken, nullptr));
    ASSERT_TRUE(taken) << "message " << i;
    EXPECT_TRUE(make_msg(kSize, static_cast<uint8_t>(i)).uint8_values == got.uint8_values);
  }
  EXPECT_EQ(0u, lost(0));
}

// The on_new_message count is a take credit (the EventsExecutor takes once per
// event). Records the publisher laps while queued are dropped at the drain, so
// a lapping stream never credits more takes than can deliver a message.
TEST_F(RingTakeTest, NewMessageEventsNeverOutnumberDeliverableTakes)
{
  make_pub("/ring_take_credits");
  rmw_subscription_t * s = make_sub("/ring_take_credits");
  ASSERT_NE(nullptr, s);
  std::atomic<size_t> credits{0};
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_set_on_new_message_callback(
      s, [](const void * user, size_t n) {
        *static_cast<std::atomic<size_t> *>(const_cast<void *>(user)) += n;
      }, &credits));

  constexpr size_t kSize = 1024 * 1024;
  constexpr int kCount = 10;
  for (int i = 1; i <= kCount; ++i) {
    publish(make_msg(kSize, static_cast<uint8_t>(i)));
    drain_all();
  }
  size_t delivered = 0;
  for (;;) {
    test_msgs::msg::UnboundedSequences got;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(s, &got, &taken, nullptr));
    if (!taken) {break;}
    ++delivered;
  }
  EXPECT_GE(delivered, 4u) << "the ring keeps at least four records";
  EXPECT_LT(delivered, static_cast<size_t>(kCount)) << "the publisher lapped the ring";
  EXPECT_LE(credits.load(), delivered);
  EXPECT_EQ(static_cast<size_t>(kCount) - delivered, lost(0));
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_set_on_new_message_callback(s, nullptr, nullptr));
}

// A KEEP_LAST overflow drops queued descriptors: their pins go with them.
TEST_F(RingTakeTest, AKeepLastOverflowReleasesItsPins)
{
  qos.depth = 2;
  make_pub("/ring_take_overflow");
  rmw_subscription_t * s = make_sub("/ring_take_overflow");
  ASSERT_NE(nullptr, s);
  for (int i = 1; i <= 6; ++i) {
    publish(make_msg(256 * 1024, static_cast<uint8_t>(i)));
    drain_all();
  }
  for (int i = 5; i <= 6; ++i) {
    test_msgs::msg::UnboundedSequences got;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(s, &got, &taken, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_TRUE(make_msg(256 * 1024, static_cast<uint8_t>(i)).uint8_values == got.uint8_values);
  }
  EXPECT_EQ(0u, lost(0)) << "a KEEP_LAST overflow is not a lost message";
  auto * data = static_cast<rmw_uds::UdsSubscription *>(s->data);
  std::lock_guard<std::mutex> lock(data->shm_cache.mutex);
  for (auto & entry : data->shm_cache.mappings) {
    EXPECT_EQ(0u, entry.second.pins);
  }
}

// A publisher destroyed while its descriptors are queued unlinks its ring. The
// sweep that eight more mappings trigger must keep the pinned mapping (marked
// superseded), so the queued messages are still delivered.
TEST_F(RingTakeTest, QueuedMessagesSurviveTheSweepOfADestroyedPublisher)
{
  make_pub("/ring_take_sweep");
  rmw_subscription_t * s = make_sub("/ring_take_sweep");
  ASSERT_NE(nullptr, s);
  auto * data = static_cast<rmw_uds::UdsSubscription *>(s->data);
  const auto a = make_msg(512 * 1024, 21);
  const auto b = make_msg(512 * 1024, 22);
  publish(a);
  publish(b);
  drain_all();
  const uint32_t gone_owner = static_cast<rmw_uds::UdsPublisher *>(pub->data)->shm_ring.owner_id;
  ASSERT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub));
  pub = nullptr;

  std::vector<rmw_publisher_t *> others;
  auto pub_opts = rmw_get_default_publisher_options();
  for (int i = 0; i < 8; ++i) {
    others.push_back(rmw_create_publisher(node, seq_ts, "/ring_take_sweep", &qos, &pub_opts));
    ASSERT_NE(nullptr, others.back());
    const auto m = make_msg(64 * 1024, static_cast<uint8_t>(i));
    ASSERT_EQ(RMW_RET_OK, rmw_publish(others.back(), &m, nullptr));
  }
  drain_all();  // the queue keeps the depth-10 newest: a, b and the eight

  for (const auto * sent : {&a, &b}) {
    test_msgs::msg::UnboundedSequences got;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(s, &got, &taken, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_TRUE(sent->uint8_values == got.uint8_values);
  }
  {
    std::lock_guard<std::mutex> lock(data->shm_cache.mutex);
    for (auto & entry : data->shm_cache.mappings) {
      EXPECT_NE(gone_owner, entry.first.owner_id) << "unmapped by its last take";
    }
  }
  for (auto * p : others) {EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, p));}
  EXPECT_EQ(0u, lost(0));
}

// A ring first mapped once its record seq has passed 2^31 still tracks the
// newest seq (the take's lap heuristic), instead of staying at 0.
TEST_F(RingTakeTest, TheNewestSeqIsSeededPastHalfTheCounter)
{
  make_pub("/ring_take_seq_seed");
  rmw_subscription_t * s = make_sub("/ring_take_seq_seed");
  ASSERT_NE(nullptr, s);
  static_cast<rmw_uds::UdsPublisher *>(pub->data)->shm_ring.next_index = 0x40000001u;
  const auto sent = make_msg(1024 * 1024, 31);
  publish(sent);
  drain_all();
  auto * data = static_cast<rmw_uds::UdsSubscription *>(s->data);
  {
    std::lock_guard<std::mutex> lock(data->shm_cache.mutex);
    ASSERT_EQ(1u, data->shm_cache.mappings.size());
    EXPECT_EQ(0x80000002u, data->shm_cache.mappings.begin()->second.newest_seq);
  }
  test_msgs::msg::UnboundedSequences got;
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take(s, &got, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_TRUE(sent.uint8_values == got.uint8_values);
}

// Below 512 KiB the take copies the record into a reused buffer before it
// deserializes (in place was slower there); from 512 KiB it reads in place.
TEST_F(RingTakeTest, OnlyPayloadsOf512KiBAndUpAreReadInPlace)
{
  make_pub("/ring_take_threshold");
  rmw_subscription_t * s = make_sub("/ring_take_threshold");
  ASSERT_NE(nullptr, s);
  auto * data = static_cast<rmw_uds::UdsSubscription *>(s->data);
  for (size_t size : {256 * 1024, 1024 * 1024}) {
    SCOPED_TRACE(size);
    {
      std::lock_guard<std::mutex> lock(data->queue_mutex);
      data->spare_payloads.clear();
    }
    const auto sent = make_msg(size, 41);
    publish(sent);
    drain_all();
    EXPECT_TRUE(queued_in_ring(s));
    test_msgs::msg::UnboundedSequences got;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(s, &got, &taken, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_TRUE(sent.uint8_values == got.uint8_values);
    std::lock_guard<std::mutex> lock(data->queue_mutex);
    EXPECT_EQ(size < 512 * 1024 ? 1u : 0u, data->spare_payloads.size()) << "the copy's buffer";
  }
}
