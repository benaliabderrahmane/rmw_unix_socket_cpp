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
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    ASSERT_EQ(1u, pub_data->message_cache.size());
    const auto & cm = pub_data->message_cache.back();
    EXPECT_NE(nullptr, cm.shm_seg)
      << "a large TRANSIENT_LOCAL message must be cached in a durable shm segment";
    EXPECT_TRUE(cm.header.msg_type & rmw_uds::SHM_PAYLOAD_FLAG);
  }
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

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    ASSERT_EQ(1u, pub_data->message_cache.size());
    EXPECT_NE(nullptr, pub_data->message_cache.back().shm_seg);
  }

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

TEST_F(QosTest, TransientLocalLargeMessageInlineFallbackWhenShmUnavailable)
{
  // When shm staging is unavailable, a large latched payload falls back to
  // caching inline (no durable segment, no SHM_PAYLOAD_FLAG) and is still
  // delivered byte-equal to a late joiner. The test seam forces the failure.
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, seq_ts, "/tl_fallback", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  setenv("RMW_UDS_TEST_FORCE_SHM_FAILURE", "1", 1);
  test_msgs::msg::UnboundedSequences msg;
  msg.uint8_values.resize(100 * 1024);
  for (size_t i = 0; i < msg.uint8_values.size(); ++i) {
    msg.uint8_values[i] = static_cast<uint8_t>((i * 5 + 2) & 0xFF);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  unsetenv("RMW_UDS_TEST_FORCE_SHM_FAILURE");  // reset before it leaks to other tests

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    ASSERT_EQ(1u, pub_data->message_cache.size());
    const auto & cm = pub_data->message_cache.back();
    EXPECT_EQ(nullptr, cm.shm_seg)
      << "shm forced unavailable — the large payload must be cached inline";
    EXPECT_FALSE(cm.header.msg_type & rmw_uds::SHM_PAYLOAD_FLAG);
  }

  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, seq_ts, "/tl_fallback", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);
  test_msgs::msg::UnboundedSequences trigger;
  trigger.uint8_values = {5, 6, 7};
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &trigger, nullptr));

  test_msgs::msg::UnboundedSequences recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  ASSERT_TRUE(taken) << "late joiner must receive the inline-fallback message";
  EXPECT_EQ(msg.uint8_values, recv.uint8_values);

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
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
  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    ASSERT_EQ(1u, pub_data->message_cache.size());
    EXPECT_NE(nullptr, pub_data->message_cache.back().shm_seg);
  }

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

TEST_F(QosTest, KnownSubscriberPathsPrunedOnChurn)
{
  // The publisher's known_subscriber_paths must not accumulate dead entries as
  // subscribers churn: each create/destroy bumps the registry generation, and a
  // restarted subscriber gets a brand-new unique socket path. Without pruning on
  // refresh the set is insert-only and grows by one per churned subscriber.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/churn", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Seed the cache so there is something to replay.
  test_msgs::msg::BasicTypes seed;
  seed.int32_value = 1;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &seed, nullptr));

  auto sub_opts = rmw_get_default_subscription_options();
  constexpr int kChurn = 8;
  for (int i = 0; i < kChurn; ++i) {
    // New sub bumps generation -> next publish refreshes + records this sub.
    auto * sub = rmw_create_subscription(node, ts, "/churn", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub);
    test_msgs::msg::BasicTypes m;
    m.int32_value = i + 2;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
    // Destroy bumps generation again; next publish refreshes + prunes the gone sub.
    auto _r [[maybe_unused]] = rmw_destroy_subscription(node, sub);
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
  }

  // After the loop every churned sub is destroyed, so known should be empty.
  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  size_t known_size = 0;
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    known_size = pub_data->known_subscriber_paths.size();
  }
  // With the prune: tracks only live subs (0 here). Without it: grows to kChurn.
  EXPECT_LE(known_size, 1u)
    << "known_subscriber_paths leaked dead entries: size=" << known_size;

  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalSerializedKnownSubscriberPathsPrunedOnChurn)
{
  // Same prune guarantee as KnownSubscriberPathsPrunedOnChurn, but driven
  // through rmw_publish_serialized_message, which carries its own copy of the
  // prune-on-refresh logic. Guards against that copy silently diverging: without
  // the prune the serialized path's known_subscriber_paths grows by one per
  // churned subscriber.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/churn_serialized", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  uint8_t bytes[] = {1, 2, 3, 4, 5, 6, 7, 8};
  rmw_serialized_message_t msg;
  msg.buffer = bytes;
  msg.buffer_length = sizeof(bytes);
  msg.buffer_capacity = sizeof(bytes);
  msg.allocator = rcutils_get_default_allocator();

  // Seed the cache so there is something to replay.
  EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &msg, nullptr));

  auto sub_opts = rmw_get_default_subscription_options();
  constexpr int kChurn = 8;
  for (int i = 0; i < kChurn; ++i) {
    auto * sub = rmw_create_subscription(node, ts, "/churn_serialized", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub);
    EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &msg, nullptr));
    auto _r [[maybe_unused]] = rmw_destroy_subscription(node, sub);
    EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &msg, nullptr));
  }

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  size_t known_size = 0;
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    known_size = pub_data->known_subscriber_paths.size();
  }
  EXPECT_LE(known_size, 1u)
    << "serialized-path known_subscriber_paths leaked dead entries: size=" << known_size;

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
