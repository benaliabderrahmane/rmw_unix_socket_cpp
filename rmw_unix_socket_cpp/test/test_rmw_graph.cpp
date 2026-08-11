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
#include <string>

#include "../src/registry.hpp"
#include "../src/types.hpp"

#include "test_msgs/msg/basic_types.hpp"

#include "rmw/get_topic_names_and_types.h"
#include "rmw/names_and_types.h"
#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"

TEST_F(RmwUdsNodeTest, GetNodeNames)
{
  rcutils_string_array_t names = rcutils_get_zero_initialized_string_array();
  rcutils_string_array_t namespaces = rcutils_get_zero_initialized_string_array();

  ASSERT_EQ(RMW_RET_OK, rmw_get_node_names(node, &names, &namespaces));
  ASSERT_GE(names.size, 1u);

  // Our test_node should be in the list
  bool found = false;
  for (size_t i = 0; i < names.size; ++i) {
    if (std::string(names.data[i]) == "test_node") {
      found = true;
      EXPECT_STREQ("/test_ns", namespaces.data[i]);
    }
  }
  EXPECT_TRUE(found);

  auto _r1 [[maybe_unused]] = rcutils_string_array_fini(&names);
  auto _r2 [[maybe_unused]] = rcutils_string_array_fini(&namespaces);
}

// registry_cleanup_stale walks every live slot and stats /proc/<pid> for each
// one. That is garbage collection, and it must not run on every graph query.
TEST_F(RmwUdsNodeTest, GraphQueryThrottlesStaleCleanup)
{
  auto * nd = static_cast<rmw_uds::UdsNode *>(node->data);
  auto * header = rmw_uds::registry_header(nd->context->registry_ptr);

  // A PID that cannot be running, so cleanup_stale sees the entry as dead.
  constexpr pid_t DEAD_PID = 2147483000;
  auto add_ghost = [&](const char * name) {
      rmw_uds::RegistryEntry e;
      std::memset(&e, 0, sizeof(e));
      e.type = rmw_uds::ENTRY_NODE;
      e.pid = DEAD_PID;
      std::strncpy(e.node_name, name, sizeof(e.node_name) - 1);
      return rmw_uds::registry_add(header, e);
    };

  auto graph_lists = [&](const char * name) {
      rcutils_string_array_t names = rcutils_get_zero_initialized_string_array();
      rcutils_string_array_t namespaces = rcutils_get_zero_initialized_string_array();
      EXPECT_EQ(RMW_RET_OK, rmw_get_node_names(node, &names, &namespaces));
      bool found = false;
      for (size_t i = 0; i < names.size; ++i) {
        if (std::string(names.data[i]) == name) {
          found = true;
        }
      }
      auto _r1 [[maybe_unused]] = rcutils_string_array_fini(&names);
      auto _r2 [[maybe_unused]] = rcutils_string_array_fini(&namespaces);
      return found;
    };

  // The first query after a quiet period does sweep, so this one is reclaimed.
  ASSERT_GE(add_ghost("ghost_first"), 0);
  EXPECT_FALSE(graph_lists("ghost_first")) <<
    "the first graph query should still reclaim dead slots";

  // A second dead slot added immediately must survive: the sweep is throttled,
  // so the very next query does not pay for another full stat() pass.
  const int32_t second = add_ghost("ghost_second");
  ASSERT_GE(second, 0);
  EXPECT_TRUE(graph_lists("ghost_second")) <<
    "cleanup_stale ran again right away; the throttle is not in effect";

  rmw_uds::registry_remove(header, second);
}

TEST_F(RmwUdsNodeTest, CountPublishersAndSubscribers)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  rmw_qos_profile_t qos;
  std::memset(&qos, 0, sizeof(qos));
  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  qos.depth = 10;
  qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;

  // Initially zero
  size_t count = 99;
  EXPECT_EQ(RMW_RET_OK, rmw_count_publishers(node, "/graph_topic", &count));
  EXPECT_EQ(0u, count);

  EXPECT_EQ(RMW_RET_OK, rmw_count_subscribers(node, "/graph_topic", &count));
  EXPECT_EQ(0u, count);

  // Create publisher
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/graph_topic", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  EXPECT_EQ(RMW_RET_OK, rmw_count_publishers(node, "/graph_topic", &count));
  EXPECT_EQ(1u, count);

  // Create subscription
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/graph_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  EXPECT_EQ(RMW_RET_OK, rmw_count_subscribers(node, "/graph_topic", &count));
  EXPECT_EQ(1u, count);

  // Cleanup
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub));

  // Counts should be zero again
  EXPECT_EQ(RMW_RET_OK, rmw_count_publishers(node, "/graph_topic", &count));
  EXPECT_EQ(0u, count);
}

TEST_F(RmwUdsNodeTest, GetTopicNamesAndTypes)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  rmw_qos_profile_t qos;
  std::memset(&qos, 0, sizeof(qos));
  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  qos.depth = 10;
  qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/typed_topic", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  rcutils_allocator_t alloc = rcutils_get_default_allocator();
  rmw_names_and_types_t names_types = rmw_get_zero_initialized_names_and_types();
  ASSERT_EQ(RMW_RET_OK, rmw_get_topic_names_and_types(node, &alloc, false, &names_types));

  bool found = false;
  for (size_t i = 0; i < names_types.names.size; ++i) {
    if (std::string(names_types.names.data[i]) == "/typed_topic") {
      found = true;
      ASSERT_GE(names_types.types[i].size, 1u);
    }
  }
  EXPECT_TRUE(found);

  auto _r3 [[maybe_unused]] = rmw_names_and_types_fini(&names_types);
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub));
}

TEST_F(RmwUdsNodeTest, CompareGidsEqual)
{
  rmw_gid_t gid1, gid2;
  std::memset(&gid1, 0, sizeof(gid1));
  std::memset(&gid2, 0, sizeof(gid2));
  gid1.data[0] = 1;
  gid2.data[0] = 1;

  bool result = false;
  EXPECT_EQ(RMW_RET_OK, rmw_compare_gids_equal(&gid1, &gid2, &result));
  EXPECT_TRUE(result);

  gid2.data[0] = 2;
  EXPECT_EQ(RMW_RET_OK, rmw_compare_gids_equal(&gid1, &gid2, &result));
  EXPECT_FALSE(result);
}
