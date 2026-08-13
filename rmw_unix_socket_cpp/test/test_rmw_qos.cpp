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

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <chrono>

#include <sys/socket.h>
#include <unistd.h>

#include "test_msgs/msg/basic_types.hpp"
#include "test_msgs/msg/unbounded_sequences.hpp"
#include "test_msgs/srv/basic_types.hpp"

#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"
#include "rosidl_typesupport_cpp/service_type_support.hpp"

#include <sys/un.h>

#include "types.hpp"

class QosTest : public RmwUdsNodeTest
{
protected:
  const rosidl_message_type_support_t * ts = nullptr;

  void SetUp() override
  {
    RmwUdsNodeTest::SetUp();
    ts = rosidl_typesupport_cpp::get_message_type_support_handle<
      test_msgs::msg::BasicTypes>();
  }

  rmw_qos_profile_t make_qos(
    rmw_qos_reliability_policy_e rel,
    rmw_qos_durability_policy_e dur,
    size_t depth = 10)
  {
    rmw_qos_profile_t qos;
    std::memset(&qos, 0, sizeof(qos));
    qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    qos.depth = depth;
    qos.reliability = rel;
    qos.durability = dur;
    return qos;
  }
};

// --- TRANSIENT_LOCAL (latched) tests ---

TEST_F(QosTest, TransientLocalLateJoiner)
{
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  // Create publisher with TRANSIENT_LOCAL
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/latched", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Publish 3 messages BEFORE subscriber exists
  for (int i = 1; i <= 3; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i * 10;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  // Now create subscriber (late joiner)
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/latched", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  // Publish one more message — this triggers cache replay to the new subscriber
  test_msgs::msg::BasicTypes trigger_msg;
  trigger_msg.int32_value = 40;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &trigger_msg, nullptr));

  // Subscriber should receive all 4 messages (3 cached + 1 current)
  for (int i = 1; i <= 4; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    ASSERT_TRUE(taken) << "Failed to take message " << i;
    EXPECT_EQ(i * 10, recv.int32_value);
  }

  // No more messages
  test_msgs::msg::BasicTypes recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  EXPECT_FALSE(taken);

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalLargeMessageUsesDurableShm)
{
  // Large latched messages are staged into a dedicated *durable* shm segment
  // owned by the cache entry — never the publisher's cycling ring, which would
  // lap and corrupt a record still awaited by a late joiner. So the cached
  // entry carries a descriptor (SHM_PAYLOAD_FLAG set), shm_ring stays untouched,
  // and replay resolves the descriptor out of the durable segment. This also
  // pins the ordering in rmw_publish: the TL branch must stage durably rather
  // than fall through to the ring fork. 100 KB: over SHM_PAYLOAD_THRESHOLD,
  // under the stock-kernel datagram cap. (See TransientLocalHugeMessageLateJoiner
  // for the case that exceeds the cap.)
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, seq_ts, "/latched_large", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  test_msgs::msg::UnboundedSequences msg;
  msg.uint8_values.resize(100 * 1024);
  for (size_t i = 0; i < msg.uint8_values.size(); ++i) {
    msg.uint8_values[i] = static_cast<uint8_t>((i * 7 + 1) & 0xFF);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  EXPECT_EQ(nullptr, pub_data->shm_ring.base)
    << "TRANSIENT_LOCAL must use a durable segment, never the cycling ring";

  // A late joiner must get the cached message via durable-shm replay.
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, seq_ts, "/latched_large", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::UnboundedSequences trigger;
  trigger.uint8_values = {1, 2, 3};
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &trigger, nullptr));
  EXPECT_EQ(nullptr, pub_data->shm_ring.base);

  test_msgs::msg::UnboundedSequences recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(msg.uint8_values, recv.uint8_values);

  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(trigger.uint8_values, recv.uint8_values);

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalHugeMessageLateJoiner)
{
  // Regression for the reported bug: a latched message larger than the kernel's
  // ~4 MB single-datagram cap must still reach a late joiner. On the old inline
  // path the 5 MB replay datagram was rejected by the kernel (EMSGSIZE without
  // the raised sysctls, ENOBUFS with them) and silently dropped, so the late
  // joiner got nothing. Through the durable shm segment only a ~69-byte
  // descriptor crosses the socket, so replay works regardless of sysctl tuning.
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, seq_ts, "/latched_huge", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Publish a 5 MB message BEFORE any subscriber exists — it is cached durably.
  test_msgs::msg::UnboundedSequences big;
  big.uint8_values.resize(5 * 1024 * 1024);
  for (size_t i = 0; i < big.uint8_values.size(); ++i) {
    big.uint8_values[i] = static_cast<uint8_t>((i * 131 + 7) & 0xFF);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &big, nullptr));

  // Late joiner, then a small publish to trigger replay of the cached 5 MB msg.
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, seq_ts, "/latched_huge", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::UnboundedSequences trigger;
  trigger.uint8_values = {9, 8, 7};
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &trigger, nullptr));

  test_msgs::msg::UnboundedSequences recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken) << "late joiner must receive the cached 5 MB message";
  EXPECT_EQ(big.uint8_values, recv.uint8_values);

  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(trigger.uint8_values, recv.uint8_values);

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalPublishReturnsErrorOnEMSGSIZE)
{
  // A latched publish whose live send is rejected by the kernel size cap must
  // return RMW_RET_ERROR, not a lying RMW_RET_OK. Before the fix the TL path
  // ignored send_to's result entirely. Shrink SO_SNDBUF so a sub-threshold
  // (inline) latched message hits EMSGSIZE deterministically, independent of
  // the machine's net.core.wmem_max.
  auto * ctx_impl = reinterpret_cast<rmw_uds::UdsContext *>(context.impl);
  int small_buf = 2048;
  ASSERT_EQ(
    0,
    setsockopt(
      ctx_impl->send_socket_fd, SOL_SOCKET, SO_SNDBUF,
      &small_buf, sizeof(small_buf)));

  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, seq_ts, "/tl_emsgsize", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, seq_ts, "/tl_emsgsize", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  // 32 KiB: above the shrunken buffer, below SHM_PAYLOAD_THRESHOLD so it stays
  // on the inline datagram path (a durable-staged payload would bypass the cap).
  static_assert(32 * 1024 < rmw_uds::SHM_PAYLOAD_THRESHOLD, "must stay inline");
  test_msgs::msg::UnboundedSequences msg;
  msg.uint8_values.resize(32 * 1024);
  EXPECT_EQ(RMW_RET_ERROR, rmw_publish(pub, &msg, nullptr));

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalLargeMessageNotLatchedWhenShmUnavailable)
{
  // When shm staging is unavailable, a large latched payload cannot enter the
  // pull cache (there is nowhere durable to put it): the publish still
  // returns OK, an EXISTING subscriber still receives the sample live as an
  // inline datagram, and a late joiner gets nothing — the documented
  // degradation of pull-based replay, replacing the old inline-heap fallback
  // that the deleted push machinery could still replay.
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, seq_ts, "/tl_fallback", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Existing subscriber: must receive the sample live despite shm being down.
  auto sub_opts = rmw_get_default_subscription_options();
  auto * live_sub = rmw_create_subscription(node, seq_ts, "/tl_fallback", &qos, &sub_opts);
  ASSERT_NE(nullptr, live_sub);

  setenv("RMW_UDS_TEST_FORCE_SHM_FAILURE", "1", 1);
  test_msgs::msg::UnboundedSequences msg;
  msg.uint8_values.resize(100 * 1024);
  for (size_t i = 0; i < msg.uint8_values.size(); ++i) {
    msg.uint8_values[i] = static_cast<uint8_t>((i * 5 + 2) & 0xFF);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  unsetenv("RMW_UDS_TEST_FORCE_SHM_FAILURE");  // reset before it leaks to other tests

  test_msgs::msg::UnboundedSequences recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(live_sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken) << "existing subscriber must receive the inline live send";
  EXPECT_EQ(msg.uint8_values, recv.uint8_values);

  // Late joiner: the sample was never latched, so nothing is replayed.
  auto * late_sub = rmw_create_subscription(node, seq_ts, "/tl_fallback", &qos, &sub_opts);
  ASSERT_NE(nullptr, late_sub);
  taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(late_sub, &recv, &taken, nullptr));
  EXPECT_FALSE(taken) <<
    "a sample that could not be latched must not reach a late joiner";

  auto _r0 [[maybe_unused]] = rmw_destroy_subscription(node, late_sub);
  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, live_sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, VolatileLargeMessageInlineFallbackWhenShmUnavailable)
{
  // Ring-path counterpart of the TL test above: when shm staging is
  // unavailable, a large VOLATILE payload is serialized into the inline
  // fallback (no ring, no SHM_PAYLOAD_FLAG) and still delivered byte-equal.
  // Pins shm_serialize_prepare_send's reserve-failure branch, including the
  // contained resize on the extern "C" boundary.
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE, 5);
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, seq_ts, "/vol_fallback", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, seq_ts, "/vol_fallback", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  setenv("RMW_UDS_TEST_FORCE_SHM_FAILURE", "1", 1);
  test_msgs::msg::UnboundedSequences msg;
  msg.uint8_values.resize(100 * 1024);  // over the threshold, under the datagram cap
  for (size_t i = 0; i < msg.uint8_values.size(); ++i) {
    msg.uint8_values[i] = static_cast<uint8_t>((i * 11 + 3) & 0xFF);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  unsetenv("RMW_UDS_TEST_FORCE_SHM_FAILURE");  // reset before it leaks to other tests

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  EXPECT_EQ(nullptr, pub_data->shm_ring.base)
    << "shm forced unavailable — no ring may be created";

  test_msgs::msg::UnboundedSequences recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken) << "the inline-fallback message must be delivered";
  EXPECT_EQ(msg.uint8_values, recv.uint8_values);

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalSerializedLargeMessageLateJoiner)
{
  // rmw_publish_serialized_message now feeds the TL replay cache and stages
  // large latched payloads durably, so a >cap serialized latched message
  // reaches a late joiner. Previously that entry point never cached, so the
  // message was sent inline once and dropped for anyone who joined later.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/tl_serialized_huge", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  constexpr size_t big_size = 5 * 1024 * 1024;
  rmw_serialized_message_t serialized;
  serialized.buffer_capacity = big_size;
  serialized.buffer_length = big_size;
  serialized.buffer = static_cast<uint8_t *>(std::malloc(big_size));
  ASSERT_NE(nullptr, serialized.buffer);
  for (size_t i = 0; i < big_size; ++i) {
    serialized.buffer[i] = static_cast<uint8_t>((i * 131 + 7) & 0xFF);
  }
  serialized.allocator = rcutils_get_default_allocator();

  // Publish BEFORE any subscriber — must be cached in a durable segment.
  EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &serialized, nullptr));

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/tl_serialized_huge", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  uint8_t trig_bytes[] = {1, 2, 3, 4};
  rmw_serialized_message_t trig;
  trig.buffer = trig_bytes;
  trig.buffer_length = sizeof(trig_bytes);
  trig.buffer_capacity = sizeof(trig_bytes);
  trig.allocator = rcutils_get_default_allocator();
  EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &trig, nullptr));

  rmw_serialized_message_t received = rmw_get_zero_initialized_serialized_message();
  ASSERT_EQ(RMW_RET_OK, rmw_serialized_message_init(&received, 0, &serialized.allocator));
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_serialized_message(sub, &received, &taken, nullptr));
  ASSERT_TRUE(taken) << "late joiner must receive the cached 5 MB serialized message";
  ASSERT_EQ(big_size, received.buffer_length);
  EXPECT_EQ(0, std::memcmp(serialized.buffer, received.buffer, big_size));

  EXPECT_EQ(RMW_RET_OK, rmw_take_serialized_message(sub, &received, &taken, nullptr));
  ASSERT_TRUE(taken);
  ASSERT_EQ(sizeof(trig_bytes), received.buffer_length);

  auto _f [[maybe_unused]] = rmw_serialized_message_fini(&received);
  std::free(serialized.buffer);
  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalCacheDepthEnforced)
{
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 3);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/depth_test", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Publish 5 messages (cache depth=3, so oldest 2 should be evicted)
  for (int i = 1; i <= 5; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  // Late-joining subscriber
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/depth_test", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  // Trigger replay with one more publish
  test_msgs::msg::BasicTypes trigger_msg;
  trigger_msg.int32_value = 6;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &trigger_msg, nullptr));

  // Should get messages 4, 5, 6 (cache had 3,4,5; then 6 added, 3 evicted;
  // replay sends 4,5 then current 6)
  std::vector<int> received;
  for (int i = 0; i < 10; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (!taken) {break;}
    received.push_back(recv.int32_value);
  }

  // We should have received 4, 5, 6 (the 3 most recent in cache + current)
  ASSERT_GE(received.size(), 3u);
  // The last received should be 6 (current message)
  EXPECT_EQ(6, received.back());

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// --- Transport error propagation ---

TEST_F(QosTest, PublishReturnsErrorOnEMSGSIZE)
{
  // Shrink SO_SNDBUF so any send hits EMSGSIZE — publish must return ERROR.
  auto * ctx_impl = reinterpret_cast<rmw_uds::UdsContext *>(context.impl);
  int small_buf = 2048;
  ASSERT_EQ(
    0,
    setsockopt(
      ctx_impl->send_socket_fd, SOL_SOCKET, SO_SNDBUF,
      &small_buf, sizeof(small_buf)));

  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/emsgsize", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/emsgsize", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  // 32 KB — well above SOCK_MIN_SNDBUF the kernel will clamp us to, but
  // below SHM_PAYLOAD_THRESHOLD so the message stays on the inline datagram
  // path (payloads at or above the threshold bypass the socket buffer
  // entirely via the shm ring; see the test below).
  constexpr size_t big_size = 32 * 1024;
  static_assert(big_size < rmw_uds::SHM_PAYLOAD_THRESHOLD, "must stay inline");
  rmw_serialized_message_t serialized;
  serialized.buffer_capacity = big_size;
  serialized.buffer_length = big_size;
  serialized.buffer = static_cast<uint8_t *>(std::malloc(big_size));
  ASSERT_NE(nullptr, serialized.buffer);
  std::memset(serialized.buffer, 0x42, big_size);
  serialized.allocator = rcutils_get_default_allocator();

  EXPECT_EQ(RMW_RET_ERROR, rmw_publish_serialized_message(pub, &serialized, nullptr));

  std::free(serialized.buffer);
  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, LargePayloadBypassesSendBuffer)
{
  // Same shrunken SO_SNDBUF, but a payload above SHM_PAYLOAD_THRESHOLD:
  // the bytes travel through the publisher's shm ring and only a small
  // descriptor crosses the socket, so the publish succeeds and the message
  // arrives intact where it previously died with EMSGSIZE.
  auto * ctx_impl = reinterpret_cast<rmw_uds::UdsContext *>(context.impl);
  int small_buf = 2048;
  ASSERT_EQ(
    0,
    setsockopt(
      ctx_impl->send_socket_fd, SOL_SOCKET, SO_SNDBUF,
      &small_buf, sizeof(small_buf)));

  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/shm_bypass", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/shm_bypass", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  constexpr size_t big_size = 128 * 1024;
  static_assert(big_size >= rmw_uds::SHM_PAYLOAD_THRESHOLD, "must use the ring");
  rmw_serialized_message_t serialized;
  serialized.buffer_capacity = big_size;
  serialized.buffer_length = big_size;
  serialized.buffer = static_cast<uint8_t *>(std::malloc(big_size));
  ASSERT_NE(nullptr, serialized.buffer);
  for (size_t i = 0; i < big_size; ++i) {
    serialized.buffer[i] = static_cast<uint8_t>((i * 17 + 3) & 0xFF);
  }
  serialized.allocator = rcutils_get_default_allocator();

  EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &serialized, nullptr));

  rmw_serialized_message_t received = rmw_get_zero_initialized_serialized_message();
  ASSERT_EQ(
    RMW_RET_OK,
    rmw_serialized_message_init(&received, 0, &serialized.allocator));
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_serialized_message(sub, &received, &taken, nullptr));
  ASSERT_TRUE(taken);
  ASSERT_EQ(big_size, received.buffer_length);
  EXPECT_EQ(0, std::memcmp(serialized.buffer, received.buffer, big_size));

  auto _f [[maybe_unused]] = rmw_serialized_message_fini(&received);
  std::free(serialized.buffer);
  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalReplayOnWaitNoSubsequentPublish)
{
  // Late sub must receive cached msgs from rmw_wait alone — no fresh publish.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/wait_replay", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  for (int i = 1; i <= 3; ++i) {
    test_msgs::msg::BasicTypes m;
    m.int32_value = i;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
  }

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/wait_replay", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  rmw_subscriptions_t subscriptions;
  void * sub_array[1] = {sub->data};
  subscriptions.subscribers = sub_array;
  subscriptions.subscriber_count = 1;

  rmw_time_t timeout;
  timeout.sec = 0;
  timeout.nsec = 500 * 1000 * 1000;  // 500 ms

  EXPECT_EQ(RMW_RET_OK, rmw_wait(
      &subscriptions, nullptr, nullptr, nullptr, nullptr, ws, &timeout));
  EXPECT_NE(nullptr, subscriptions.subscribers[0]);

  std::vector<int> received;
  for (int i = 0; i < 10; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (!taken) {break;}
    received.push_back(recv.int32_value);
  }

  ASSERT_EQ(3u, received.size());
  EXPECT_EQ(1, received[0]);
  EXPECT_EQ(2, received[1]);
  EXPECT_EQ(3, received[2]);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, PublishStillReturnsOkOnSoftDropPeerGone)
{
  // ENOENT on a vanished peer must stay RET_OK — only EMSGSIZE escalates.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/peer_gone", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/peer_gone", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  // Warm pub's path cache, then unlink the sub's socket → next sendmsg = ENOENT.
  test_msgs::msg::BasicTypes m;
  m.int32_value = 1;
  ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));

  auto * sub_impl = static_cast<rmw_uds::UdsSubscription *>(sub->data);
  unlink(sub_impl->socket_path.c_str());

  m.int32_value = 2;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalReplayOnWaitDepthOne)
{
  // depth=1 + late-join: the single cached msg must reach the new sub via wait.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 1);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/depth1_wait", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  test_msgs::msg::BasicTypes m;
  m.int32_value = 42;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/depth1_wait", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  rmw_subscriptions_t subscriptions;
  void * sub_array[1] = {sub->data};
  subscriptions.subscribers = sub_array;
  subscriptions.subscriber_count = 1;

  rmw_time_t timeout;
  timeout.sec = 0;
  timeout.nsec = 500 * 1000 * 1000;
  EXPECT_EQ(RMW_RET_OK, rmw_wait(
      &subscriptions, nullptr, nullptr, nullptr, nullptr, ws, &timeout));
  EXPECT_NE(nullptr, subscriptions.subscribers[0]);

  test_msgs::msg::BasicTypes recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(42, recv.int32_value);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, VolatileNoCache)
{
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/volatile_test", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Publish before subscriber
  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 99;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  // Late-joining subscriber should NOT get the old message
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/volatile_test", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::BasicTypes recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  EXPECT_FALSE(taken);

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// --- QoS depth enforcement ---

TEST_F(QosTest, QueueDepthEnforced)
{
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE, 3);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/qdepth", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/qdepth", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  // Publish 5 messages into depth=3 queue
  for (int i = 1; i <= 5; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  // Should only get last 3 (depth=3, oldest dropped)
  std::vector<int> received;
  for (int i = 0; i < 10; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (!taken) {break;}
    received.push_back(recv.int32_value);
  }

  ASSERT_EQ(3u, received.size());
  EXPECT_EQ(3, received[0]);
  EXPECT_EQ(4, received[1]);
  EXPECT_EQ(5, received[2]);

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// --- QoS compatibility ---

TEST_F(QosTest, QosCompatibilityCheck)
{
  auto reliable = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);
  auto best_effort = make_qos(
    RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);
  auto transient = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL);
  auto volatile_dur = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);

  rmw_qos_compatibility_type_t compat;
  char reason[256];

  // Reliable pub + Reliable sub = OK
  EXPECT_EQ(RMW_RET_OK, rmw_qos_profile_check_compatible(
      reliable, reliable, &compat, reason, sizeof(reason)));
  EXPECT_EQ(RMW_QOS_COMPATIBILITY_OK, compat);

  // Best effort pub + Reliable sub = Warning
  EXPECT_EQ(RMW_RET_OK, rmw_qos_profile_check_compatible(
      best_effort, reliable, &compat, reason, sizeof(reason)));
  EXPECT_EQ(RMW_QOS_COMPATIBILITY_WARNING, compat);

  // Volatile pub + Transient local sub = Warning
  EXPECT_EQ(RMW_RET_OK, rmw_qos_profile_check_compatible(
      volatile_dur, transient, &compat, reason, sizeof(reason)));
  EXPECT_EQ(RMW_QOS_COMPATIBILITY_WARNING, compat);

  // Transient local pub + Transient local sub = OK
  EXPECT_EQ(RMW_RET_OK, rmw_qos_profile_check_compatible(
      transient, transient, &compat, reason, sizeof(reason)));
  EXPECT_EQ(RMW_QOS_COMPATIBILITY_OK, compat);
}

// --- Multiple subscribers ---

TEST_F(QosTest, MultipleSubscribersOnePublisher)
{
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/multi_sub", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub1 = rmw_create_subscription(node, ts, "/multi_sub", &qos, &sub_opts);
  auto * sub2 = rmw_create_subscription(node, ts, "/multi_sub", &qos, &sub_opts);
  auto * sub3 = rmw_create_subscription(node, ts, "/multi_sub", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub1);
  ASSERT_NE(nullptr, sub2);
  ASSERT_NE(nullptr, sub3);

  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 42;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  // All 3 subscribers should receive the message
  for (auto * sub : {sub1, sub2, sub3}) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_EQ(42, recv.int32_value);
  }

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub3);
  auto _r2 [[maybe_unused]] = rmw_destroy_subscription(node, sub2);
  auto _r3 [[maybe_unused]] = rmw_destroy_subscription(node, sub1);
  auto _r4 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// --- Multiple services and clients ---

TEST_F(QosTest, MultipleClientsOneService)
{
  auto srv_ts = rosidl_typesupport_cpp::get_service_type_support_handle<
    test_msgs::srv::BasicTypes>();

  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_VOLATILE);

  auto * srv = rmw_create_service(node, srv_ts, "/multi_cli_srv", &qos);
  ASSERT_NE(nullptr, srv);

  auto * cli1 = rmw_create_client(node, srv_ts, "/multi_cli_srv", &qos);
  auto * cli2 = rmw_create_client(node, srv_ts, "/multi_cli_srv", &qos);
  ASSERT_NE(nullptr, cli1);
  ASSERT_NE(nullptr, cli2);

  // Both clients should see the service
  bool avail = false;
  EXPECT_EQ(RMW_RET_OK, rmw_service_server_is_available(node, cli1, &avail));
  EXPECT_TRUE(avail);
  EXPECT_EQ(RMW_RET_OK, rmw_service_server_is_available(node, cli2, &avail));
  EXPECT_TRUE(avail);

  auto _r1 [[maybe_unused]] = rmw_destroy_client(node, cli2);
  auto _r2 [[maybe_unused]] = rmw_destroy_client(node, cli1);
  auto _r3 [[maybe_unused]] = rmw_destroy_service(node, srv);
}

TEST_F(QosTest, TransientLocalReplayReachesLateJoinerWhileWaitBlocked)
{
  // The subscriber joins AFTER the publisher's executor is already blocked in
  // rmw_wait. A joining subscriber only bumps the shm generation counter, which
  // signals no fd, so an idle rmw_wait(infinite) would block in epoll forever
  // and never re-run the top-of-wait replay. The latched message must still
  // reach the late joiner within a bounded time.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  // A service anchors ctx resolution inside rmw_wait, mirroring an idle node
  // whose wait set holds only its services.
  auto srv_ts = rosidl_typesupport_cpp::get_service_type_support_handle<
    test_msgs::srv::BasicTypes>();
  auto svc_qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE, RMW_QOS_POLICY_DURABILITY_VOLATILE);
  auto * srv = rmw_create_service(node, srv_ts, "/idle_anchor", &svc_qos);
  ASSERT_NE(nullptr, srv);

  // Guard condition only unblocks the executor thread on teardown so the test
  // never hangs when the message never arrives (the failing case).
  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/idle_replay", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Publish before any subscriber exists; then the node goes idle.
  test_msgs::msg::BasicTypes m;
  m.int32_value = 7;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));

  auto * ws = rmw_create_wait_set(&context, 4);
  ASSERT_NE(nullptr, ws);

  // Executor thread: spin rmw_wait with an INFINITE timeout, like an idle node.
  std::atomic<bool> stop{false};
  std::thread executor(
    [&] {
      while (!stop.load()) {
        void * srv_array[1] = {srv->data};
        rmw_services_t services;
        services.services = srv_array;
        services.service_count = 1;
        void * gc_array[1] = {gc->data};
        rmw_guard_conditions_t gcs;
        gcs.guard_conditions = gc_array;
        gcs.guard_condition_count = 1;
        auto _r [[maybe_unused]] = rmw_wait(
          nullptr, &gcs, &services, nullptr, nullptr, ws, nullptr);
      }
    });

  // Let the executor reach epoll and block before the subscriber joins.
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  // Late joiner — created after the executor is already blocked in rmw_wait.
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/idle_replay", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  bool got = false;
  for (int i = 0; i < 300 && !got; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    if (rmw_take(sub, &recv, &taken, nullptr) == RMW_RET_OK && taken &&
      recv.int32_value == 7)
    {
      got = true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  stop.store(true);
  auto _t [[maybe_unused]] = rmw_trigger_guard_condition(gc);
  executor.join();

  EXPECT_TRUE(got) << "late joiner never received the latched message while the "
    "publisher's executor was blocked in rmw_wait";

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_wait_set(ws);
  auto _r3 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
  auto _r4 [[maybe_unused]] = rmw_destroy_guard_condition(gc);
  auto _r5 [[maybe_unused]] = rmw_destroy_service(node, srv);
}

TEST_F(QosTest, TransientLocalLateJoinerWhilePublisherProcessIdle)
{
  // Scenario: a latched (TRANSIENT_LOCAL) publisher lives in a process that
  // is completely idle — its only executor thread is parked in an unbounded
  // rmw_wait that contains no subscriptions, services, or clients. A
  // subscriber that joins later must still receive the retained message.
  // This is the user-visible bug: a latched topic on a quiet node never
  // reaching late subscribers, however the process happens to be waiting.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/idle_replay_gc_only", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  test_msgs::msg::BasicTypes m;
  m.int32_value = 9;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));

  auto * ws = rmw_create_wait_set(&context, 4);
  ASSERT_NE(nullptr, ws);

  std::atomic<bool> stop{false};
  std::thread executor(
    [&] {
      while (!stop.load()) {
        void * gc_array[1] = {gc->data};
        rmw_guard_conditions_t gcs;
        gcs.guard_conditions = gc_array;
        gcs.guard_condition_count = 1;
        auto _r [[maybe_unused]] = rmw_wait(
          nullptr, &gcs, nullptr, nullptr, nullptr, ws, nullptr);
      }
    });

  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/idle_replay_gc_only", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  bool got = false;
  for (int i = 0; i < 300 && !got; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    if (rmw_take(sub, &recv, &taken, nullptr) == RMW_RET_OK && taken &&
      recv.int32_value == 9)
    {
      got = true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  stop.store(true);
  auto _t [[maybe_unused]] = rmw_trigger_guard_condition(gc);
  executor.join();

  EXPECT_TRUE(got) << "late joiner never received the latched message while the "
    "publisher's process was idle";

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_wait_set(ws);
  auto _r3 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
  auto _r4 [[maybe_unused]] = rmw_destroy_guard_condition(gc);
}

// --- Pull-based TRANSIENT_LOCAL replay (latched-cache pull) ---

TEST_F(QosTest, TransientLocalPullDeliversWithNoWaitAnywhere)
{
  // The strongest form of the idle-publisher scenario: NO thread in this
  // process ever enters rmw_wait, so there is no doorbell drain and no
  // wait-side replay. A late joiner must still receive the latched history,
  // immediately, because it pulls the publisher's latched cache itself at
  // rmw_create_subscription.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/pull_no_wait", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  for (int i = 1; i <= 3; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i * 10;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/pull_no_wait", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  // No sleeps, no rmw_wait: the history must already be in the queue.
  int got = 0;
  for (int i = 0; i < 5; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (!taken) {break;}
    EXPECT_EQ((got + 1) * 10, recv.int32_value);  // oldest-first, in seq order
    ++got;
  }
  EXPECT_EQ(3, got) <<
    "late joiner did not receive the latched history without a wait cycle";

  auto _p1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalPullNoDuplicateWithLivePublish)
{
  // Publish latched history, join, then publish one live sample. The
  // subscriber must see each sample exactly once: the pull covers the history,
  // the datagram covers the live sample, and the per-publisher sequence
  // watermark dedups any overlap.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 10);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/pull_dedup", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  for (int i = 1; i <= 3; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/pull_dedup", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::BasicTypes live;
  live.int32_value = 4;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &live, nullptr));

  // Drain with retries: the live datagram needs a moment to land.
  std::vector<int32_t> seen;
  for (int i = 0; i < 200 && seen.size() < 4; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (taken) {
      seen.push_back(recv.int32_value);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
  ASSERT_EQ(4u, seen.size()) << "expected the 3 latched + 1 live samples";
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(i + 1, seen[i]);  // exactly once each, in order
  }
  // And nothing further: no duplicate from replay-vs-datagram overlap.
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    EXPECT_FALSE(taken) << "duplicate sample delivered";
  }

  auto _p1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalPullRespectsIgnoreLocal)
{
  // Same context, ignore_local_publications=true: the pulled history must be
  // filtered exactly like the datagram path filters live samples, or a
  // transform/republish node feeds back its own latched output.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/pull_ignore_local", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 42;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  auto sub_opts = rmw_get_default_subscription_options();
  sub_opts.ignore_local_publications = true;
  auto * sub = rmw_create_subscription(node, ts, "/pull_ignore_local", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  test_msgs::msg::BasicTypes recv;
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  EXPECT_FALSE(taken) <<
    "same-context latched history delivered despite ignore_local_publications";

  auto _p1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalPullSubscriberChurnRedelivers)
{
  // Destroy + recreate a subscription on the same topic: the NEW subscription
  // is a new endpoint and must receive the full latched history again (its own
  // pull), exactly once. This replaces the old known_subscriber_paths pruning
  // tests, which pinned the deleted push-side machinery.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/pull_churn", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 5;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  auto sub_opts = rmw_get_default_subscription_options();
  for (int round = 0; round < 3; ++round) {
    auto * sub = rmw_create_subscription(node, ts, "/pull_churn", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub);

    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    EXPECT_TRUE(taken) << "round " << round << ": latched history not redelivered";
    if (taken) {
      EXPECT_EQ(5, recv.int32_value);
    }
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    EXPECT_FALSE(taken) << "round " << round << ": history delivered twice";

    auto _p [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  }

  auto _p2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalConcurrentPublishChurnStress)
{
  // Merge gate for the pull design: subscriptions churn while a publisher
  // thread latches continuously. Every subscription must observe its
  // publisher's samples exactly once (no pull/datagram duplicate, no
  // watermark-swallowed sample) and in sequence order — the pull runs
  // concurrently with ring overwrites, so this exercises the per-record
  // seqlock snapshot and the store-buffering fence pair under real load.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 10);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/pull_stress", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  std::atomic<bool> stop{false};
  std::atomic<int32_t> published{0};
  std::thread publisher_thread(
    [&] {
      int32_t v = 0;
      while (!stop.load()) {
        test_msgs::msg::BasicTypes m;
        m.int32_value = ++v;
        if (rmw_publish(pub, &m, nullptr) != RMW_RET_OK) {
          break;
        }
        published.store(v);
      }
    });
  // A fatal assertion below returns from the test with publisher_thread
  // still joinable, which would std::terminate the whole process instead of
  // reporting the failure — stop and join on every exit path.
  struct JoinGuard
  {
    std::atomic<bool> & stop_flag;
    std::thread & thread;
    ~JoinGuard()
    {
      stop_flag.store(true);
      if (thread.joinable()) {
        thread.join();
      }
    }
  } join_guard{stop, publisher_thread};

  // Gate round 0 on the pipeline being live: with zero publishes completed,
  // the first churn round's pull finds an empty ring and its take loop could
  // expire before the first sample lands — a scheduler flake, not a defect.
  for (int i = 0; i < 2000 && published.load() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_GT(published.load(), 0) << "publisher thread never published";

  // Churn: each round joins mid-stream, drains for a moment, and must see a
  // strictly increasing, duplicate-free value sequence.
  auto sub_opts = rmw_get_default_subscription_options();
  for (int round = 0; round < 20; ++round) {
    auto * sub = rmw_create_subscription(node, ts, "/pull_stress", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub);
    int32_t last = 0;
    int received = 0;
    for (int i = 0; i < 40; ++i) {
      test_msgs::msg::BasicTypes recv;
      bool taken = false;
      ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
      if (!taken) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      EXPECT_GT(recv.int32_value, last)
        << "round " << round << ": duplicate or out-of-order sample "
        << recv.int32_value << " after " << last;
      last = recv.int32_value;
      ++received;
    }
    EXPECT_GT(received, 0) << "round " << round << ": no samples at all";
    auto _r [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  }

  stop.store(true);
  if (publisher_thread.joinable()) {
    publisher_thread.join();  // JoinGuard then finds nothing left to do
  }

  // Quiesced late joiner: must receive exactly the newest depth samples, in
  // order — the latched history and nothing else.
  const int32_t total = published.load();
  ASSERT_GE(total, 20);
  auto * final_sub = rmw_create_subscription(node, ts, "/pull_stress", &qos, &sub_opts);
  ASSERT_NE(nullptr, final_sub);
  std::vector<int32_t> tail;
  for (int i = 0; i < 15; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(final_sub, &recv, &taken, nullptr));
    if (!taken) {break;}
    tail.push_back(recv.int32_value);
  }
  ASSERT_EQ(10u, tail.size()) << "expected exactly depth latched samples";
  for (size_t i = 0; i < tail.size(); ++i) {
    EXPECT_EQ(total - 9 + static_cast<int32_t>(i), tail[i])
      << "latched history is not the newest-depth suffix in order";
  }

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, final_sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalWatermarkDropsForgedDuplicate)
{
  // Deterministic dedup coverage: after the pull sets the watermark, a
  // datagram carrying the publisher's GID with a sequence number at or below
  // the watermark must be dropped by the drain — and a genuine live sample
  // must still get through, proving the drop is not vacuous.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 10);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/wm_forge", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);
  for (int i = 1; i <= 3; ++i) {
    test_msgs::msg::BasicTypes m;
    m.int32_value = i;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
  }

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/wm_forge", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);
  // Drain the pulled history (seqs 1..3, watermark = 3).
  for (int i = 0; i < 3; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    ASSERT_TRUE(taken);
  }

  // Forge a duplicate the way the pull/live overlap would produce one: the
  // publisher's GID, sequence 2, sent straight to the subscription socket.
  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(sub->data);
  rmw_uds::WireHeader forged;
  std::memset(&forged, 0, sizeof(forged));
  std::memcpy(forged.gid, pub_data->gid.data, sizeof(forged.gid));
  forged.sequence_number = 2;
  forged.msg_type = 0;
  uint8_t junk[8] = {0};
  forged.payload_size = sizeof(junk);
  // Raw sendto (send_to is not exported from the shared library): one
  // datagram of WireHeader + payload to the subscription's bound socket,
  // exactly what a publisher's fan-out produces.
  {
    struct sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, sub_data->socket_path.c_str(),
      sizeof(addr.sun_path) - 1);
    std::vector<uint8_t> dgram(sizeof(forged) + sizeof(junk));
    std::memcpy(dgram.data(), &forged, sizeof(forged));
    std::memcpy(dgram.data() + sizeof(forged), junk, sizeof(junk));
    ASSERT_EQ(static_cast<ssize_t>(dgram.size()),
      sendto(sub_data->context->send_socket_fd, dgram.data(), dgram.size(),
        0, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  test_msgs::msg::BasicTypes recv;
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  EXPECT_FALSE(taken) << "forged duplicate (seq <= watermark) was delivered";

  // The watermark must not eat genuine live traffic.
  test_msgs::msg::BasicTypes live;
  live.int32_value = 44;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &live, nullptr));
  bool got_live = false;
  for (int i = 0; i < 100 && !got_live; ++i) {
    taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (taken && recv.int32_value == 44) {got_live = true;}
    if (!taken) {std::this_thread::sleep_for(std::chrono::milliseconds(2));}
  }
  EXPECT_TRUE(got_live) << "live sample after the watermark was not delivered";

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalTwoPublishersBothHistoriesPulled)
{
  // Two latched publishers on one topic: a late joiner must receive both
  // histories exactly once (independent per-GID watermarks), then one live
  // sample from each.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 10);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub_a = rmw_create_publisher(node, ts, "/two_pubs", &qos, &pub_opts);
  auto * pub_b = rmw_create_publisher(node, ts, "/two_pubs", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub_a);
  ASSERT_NE(nullptr, pub_b);

  for (int i = 1; i <= 2; ++i) {
    test_msgs::msg::BasicTypes m;
    m.int32_value = 100 + i;  // A: 101, 102
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub_a, &m, nullptr));
    m.int32_value = 200 + i;  // B: 201, 202
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub_b, &m, nullptr));
  }

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/two_pubs", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  std::vector<int32_t> seen;
  for (int i = 0; i < 6; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (!taken) {break;}
    seen.push_back(recv.int32_value);
  }
  std::sort(seen.begin(), seen.end());
  const std::vector<int32_t> expect = {101, 102, 201, 202};
  EXPECT_EQ(expect, seen) << "both publishers' histories, exactly once";

  test_msgs::msg::BasicTypes m;
  m.int32_value = 103;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub_a, &m, nullptr));
  m.int32_value = 203;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub_b, &m, nullptr));
  seen.clear();
  for (int i = 0; i < 200 && seen.size() < 2; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (taken) {
      seen.push_back(recv.int32_value);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }
  std::sort(seen.begin(), seen.end());
  const std::vector<int32_t> expect_live = {103, 203};
  EXPECT_EQ(expect_live, seen) << "one live sample from each publisher";

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub_a);
  auto _r3 [[maybe_unused]] = rmw_destroy_publisher(node, pub_b);
}

TEST_F(QosTest, TransientLocalMidBandPayloadLatchedAndLive)
{
  // The mid band — above the embed cap (1 KiB), below the datagram shm
  // threshold (64 KiB) — rides a durable descriptor in the latched slot but
  // an inline datagram on the wire. Both consumers must get byte-equal data.
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, seq_ts, "/mid_band", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  auto sub_opts = rmw_get_default_subscription_options();
  auto * live_sub = rmw_create_subscription(node, seq_ts, "/mid_band", &qos, &sub_opts);
  ASSERT_NE(nullptr, live_sub);

  test_msgs::msg::UnboundedSequences msg;
  msg.uint8_values.resize(4 * 1024);  // squarely in the mid band
  for (size_t i = 0; i < msg.uint8_values.size(); ++i) {
    msg.uint8_values[i] = static_cast<uint8_t>((i * 13 + 3) & 0xFF);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  test_msgs::msg::UnboundedSequences recv;
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take(live_sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken) << "live mid-band sample not delivered inline";
  EXPECT_EQ(msg.uint8_values, recv.uint8_values);

  auto * late_sub = rmw_create_subscription(node, seq_ts, "/mid_band", &qos, &sub_opts);
  ASSERT_NE(nullptr, late_sub);
  taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take(late_sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken) << "mid-band latched sample not pulled by the late joiner";
  EXPECT_EQ(msg.uint8_values, recv.uint8_values);

  auto _r0 [[maybe_unused]] = rmw_destroy_subscription(node, late_sub);
  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, live_sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}
