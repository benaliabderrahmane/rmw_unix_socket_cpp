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

#include "test_msgs/srv/basic_types.hpp"

#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/service_type_support.hpp"

#include "../src/types.hpp"  // UdsService/UdsClient shm_ring layout only (no linked symbols)

class ServiceClientTest : public RmwUdsNodeTest
{
protected:
  rmw_service_t * srv = nullptr;
  rmw_client_t * cli = nullptr;
  const rosidl_service_type_support_t * ts = nullptr;
  rmw_qos_profile_t qos;

  void SetUp() override
  {
    RmwUdsNodeTest::SetUp();
    ts = rosidl_typesupport_cpp::get_service_type_support_handle<
      test_msgs::srv::BasicTypes>();
    std::memset(&qos, 0, sizeof(qos));
    qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    qos.depth = 10;
    qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
    qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
  }

  void TearDown() override
  {
    if (cli) { auto _r [[maybe_unused]] = rmw_destroy_client(node, cli); }
    if (srv) { auto _r [[maybe_unused]] = rmw_destroy_service(node, srv); }
    RmwUdsNodeTest::TearDown();
  }
};

TEST_F(ServiceClientTest, CreateDestroyService)
{
  srv = rmw_create_service(node, ts, "/test_srv", &qos);
  ASSERT_NE(nullptr, srv);
  EXPECT_STREQ("/test_srv", srv->service_name);
  EXPECT_EQ(uds_id(), srv->implementation_identifier);
}

TEST_F(ServiceClientTest, CreateDestroyClient)
{
  cli = rmw_create_client(node, ts, "/test_srv", &qos);
  ASSERT_NE(nullptr, cli);
  EXPECT_STREQ("/test_srv", cli->service_name);
  EXPECT_EQ(uds_id(), cli->implementation_identifier);
}

TEST_F(ServiceClientTest, ServiceServerIsAvailable)
{
  cli = rmw_create_client(node, ts, "/avail_srv", &qos);
  ASSERT_NE(nullptr, cli);

  bool available = false;
  EXPECT_EQ(RMW_RET_OK, rmw_service_server_is_available(node, cli, &available));
  EXPECT_FALSE(available);

  srv = rmw_create_service(node, ts, "/avail_srv", &qos);
  ASSERT_NE(nullptr, srv);

  EXPECT_EQ(RMW_RET_OK, rmw_service_server_is_available(node, cli, &available));
  EXPECT_TRUE(available);

  // Same generation (no graph change): the cached short-circuit must return
  // the cached value, not a stale default.
  available = false;
  EXPECT_EQ(RMW_RET_OK, rmw_service_server_is_available(node, cli, &available));
  EXPECT_TRUE(available);
}

TEST_F(ServiceClientTest, RequestResponseRoundTrip)
{
  srv = rmw_create_service(node, ts, "/roundtrip_srv", &qos);
  cli = rmw_create_client(node, ts, "/roundtrip_srv", &qos);
  ASSERT_NE(nullptr, srv);
  ASSERT_NE(nullptr, cli);

  // Send request
  test_msgs::srv::BasicTypes::Request request;
  request.bool_value = true;
  request.int32_value = 42;
  request.float64_value = 3.14;

  int64_t seq_id = 0;
  EXPECT_EQ(RMW_RET_OK, rmw_send_request(cli, &request, &seq_id));
  EXPECT_GE(seq_id, 1);

  // Take request on service side
  test_msgs::srv::BasicTypes::Request recv_request;
  rmw_service_info_t request_header;
  std::memset(&request_header, 0, sizeof(request_header));
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_request(srv, &request_header, &recv_request, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(42, recv_request.int32_value);
  EXPECT_TRUE(recv_request.bool_value);
  EXPECT_DOUBLE_EQ(3.14, recv_request.float64_value);

  // Send response
  test_msgs::srv::BasicTypes::Response response;
  response.bool_value = true;
  response.int32_value = 84;
  response.string_value = "success";
  EXPECT_EQ(RMW_RET_OK, rmw_send_response(srv, &request_header.request_id, &response));

  // Take response on client side
  test_msgs::srv::BasicTypes::Response recv_response;
  rmw_service_info_t response_header;
  std::memset(&response_header, 0, sizeof(response_header));
  taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_response(cli, &response_header, &recv_response, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(84, recv_response.int32_value);
  EXPECT_TRUE(recv_response.bool_value);
  EXPECT_EQ("success", recv_response.string_value);
}

TEST_F(ServiceClientTest, LargeRequestAndResponseViaShm)
{
  // A >4 MB request and a >4 MB response must round-trip. Both exceed the
  // kernel's ~4 MB single-datagram cap (buffer-independent), so success proves
  // the client's and service's shm rings carried the payloads: on the inline
  // path (pre-change) the datagrams would be rejected and never delivered.
  srv = rmw_create_service(node, ts, "/large_srv", &qos);
  cli = rmw_create_client(node, ts, "/large_srv", &qos);
  ASSERT_NE(nullptr, srv);
  ASSERT_NE(nullptr, cli);

  std::string big_req(5 * 1024 * 1024, '\0');
  for (size_t i = 0; i < big_req.size(); ++i) {
    big_req[i] = static_cast<char>('A' + (i % 26));
  }
  std::string big_resp(5 * 1024 * 1024, '\0');
  for (size_t i = 0; i < big_resp.size(); ++i) {
    big_resp[i] = static_cast<char>('a' + (i % 26));
  }

  test_msgs::srv::BasicTypes::Request request;
  request.int32_value = 7;
  request.string_value = big_req;
  int64_t seq_id = 0;
  EXPECT_EQ(RMW_RET_OK, rmw_send_request(cli, &request, &seq_id));

  auto * cli_data = static_cast<rmw_uds::UdsClient *>(cli->data);
  EXPECT_NE(nullptr, cli_data->shm_ring.base)
    << "a >4 MB request must be staged into the client's shm ring";

  test_msgs::srv::BasicTypes::Request recv_request;
  rmw_service_info_t request_header;
  std::memset(&request_header, 0, sizeof(request_header));
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_request(srv, &request_header, &recv_request, &taken));
  ASSERT_TRUE(taken) << "service must receive the 5 MB request";
  EXPECT_EQ(7, recv_request.int32_value);
  EXPECT_EQ(big_req, recv_request.string_value);

  test_msgs::srv::BasicTypes::Response response;
  response.int32_value = 9;
  response.string_value = big_resp;
  EXPECT_EQ(RMW_RET_OK, rmw_send_response(srv, &request_header.request_id, &response));

  auto * srv_data = static_cast<rmw_uds::UdsService *>(srv->data);
  EXPECT_NE(nullptr, srv_data->shm_ring.base)
    << "a >4 MB response must be staged into the service's shm ring";

  test_msgs::srv::BasicTypes::Response recv_response;
  rmw_service_info_t response_header;
  std::memset(&response_header, 0, sizeof(response_header));
  taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_response(cli, &response_header, &recv_response, &taken));
  ASSERT_TRUE(taken) << "client must receive the 5 MB response";
  EXPECT_EQ(9, recv_response.int32_value);
  EXPECT_EQ(big_resp, recv_response.string_value);
}

TEST_F(ServiceClientTest, LargeRequestDeliveredThroughWaitDrain)
{
  // rmw_wait drains service sockets into the request queue; it must resolve a
  // large-request shm descriptor there too, not only in rmw_take_request. Real
  // executors always wait before taking, so if the wait drain dropped the
  // descriptor the request would be lost before take ever ran.
  srv = rmw_create_service(node, ts, "/large_wait_srv", &qos);
  cli = rmw_create_client(node, ts, "/large_wait_srv", &qos);
  ASSERT_NE(nullptr, srv);
  ASSERT_NE(nullptr, cli);

  std::string big(5 * 1024 * 1024, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>('A' + (i % 26));
  }
  test_msgs::srv::BasicTypes::Request request;
  request.int32_value = 5;
  request.string_value = big;
  int64_t seq_id = 0;
  EXPECT_EQ(RMW_RET_OK, rmw_send_request(cli, &request, &seq_id));

  // Drain via rmw_wait (not a direct take) to exercise the wait-side path.
  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  rmw_services_t services;
  void * srv_array[1] = {srv->data};
  services.services = srv_array;
  services.service_count = 1;
  rmw_time_t timeout;
  timeout.sec = 1;
  timeout.nsec = 0;
  EXPECT_EQ(
    RMW_RET_OK,
    rmw_wait(nullptr, nullptr, &services, nullptr, nullptr, ws, &timeout));

  test_msgs::srv::BasicTypes::Request recv_request;
  rmw_service_info_t request_header;
  std::memset(&request_header, 0, sizeof(request_header));
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_request(srv, &request_header, &recv_request, &taken));
  ASSERT_TRUE(taken) << "a 5 MB request drained by rmw_wait must be delivered";
  EXPECT_EQ(big, recv_request.string_value);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
}

TEST_F(ServiceClientTest, SendResponseToGoneClientReturnsOk)
{
  // A service that responds after its client has shut down must NOT return an
  // error: rclcpp throws inside the executor on any non-OK/non-TIMEOUT ret.
  // Matches rmw_cyclonedds (client GONE -> RMW_RET_OK); the response is dropped.
  srv = rmw_create_service(node, ts, "/gone_client_srv", &qos);
  ASSERT_NE(nullptr, srv);

  // No client is registered for this service, so the GID lookup in
  // rmw_send_response finds no match — a true client-gone miss.
  rmw_request_id_t request_id;
  std::memset(&request_id, 0, sizeof(request_id));
  request_id.sequence_number = 1;  // writer_guid left all-zero: matches no client

  test_msgs::srv::BasicTypes::Response response;
  response.bool_value = true;
  response.int32_value = 7;
  EXPECT_EQ(RMW_RET_OK, rmw_send_response(srv, &request_id, &response));
}
