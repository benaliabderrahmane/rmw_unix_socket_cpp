cmake_minimum_required(VERSION 3.14)
project(rmw_unix_socket_cpp)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(rcutils REQUIRED)
find_package(rcpputils REQUIRED)
find_package(rmw REQUIRED)
find_package(rosidl_runtime_c REQUIRED)
find_package(fastcdr CONFIG REQUIRED)
find_package(rosidl_typesupport_fastrtps_c REQUIRED)
find_package(rosidl_typesupport_fastrtps_cpp REQUIRED)

# rmw_init_options_t::localhost_only was removed in Kilted (rmw >= 7.8).
# Probe so we can keep setting it on older distros without breaking newer ones.
include(CheckCXXSourceCompiles)
set(_rmw_probe_saved_libs ${CMAKE_REQUIRED_LIBRARIES})
set(CMAKE_REQUIRED_LIBRARIES rmw::rmw rcutils::rcutils)
check_cxx_source_compiles("
#include <rmw/init_options.h>
int main() {
  rmw_init_options_t o{};
  o.localhost_only = RMW_LOCALHOST_ONLY_DEFAULT;
  return 0;
}
" RMW_HAS_LOCALHOST_ONLY)
set(CMAKE_REQUIRED_LIBRARIES ${_rmw_probe_saved_libs})

add_library(rmw_unix_socket_cpp SHARED
  src/registry.cpp
  src/serialization.cpp
  src/transport.cpp
  src/rmw_init.cpp
  src/rmw_node.cpp
  src/rmw_publisher.cpp
  src/rmw_subscription.cpp
  src/rmw_service.cpp
  src/rmw_client.cpp
  src/rmw_guard_condition.cpp
  src/rmw_wait.cpp
  src/rmw_graph.cpp
  src/rmw_event.cpp
  src/rmw_serialize.cpp
  src/rmw_features.cpp
  src/rmw_unsupported.cpp
)

target_include_directories(rmw_unix_socket_cpp PRIVATE src)

if(RMW_HAS_LOCALHOST_ONLY)
  target_compile_definitions(rmw_unix_socket_cpp PRIVATE RMW_HAS_LOCALHOST_ONLY=1)
endif()

target_link_libraries(rmw_unix_socket_cpp PUBLIC
  rmw::rmw
)

target_link_libraries(rmw_unix_socket_cpp PRIVATE
  rcutils::rcutils
  rcpputils::rcpputils
  rosidl_runtime_c::rosidl_runtime_c
  fastcdr
  rosidl_typesupport_fastrtps_c::rosidl_typesupport_fastrtps_c
  rosidl_typesupport_fastrtps_cpp::rosidl_typesupport_fastrtps_cpp
  pthread
  rt
)

configure_rmw_library(rmw_unix_socket_cpp)

register_rmw_implementation(
  "c:rosidl_typesupport_c:rosidl_typesupport_fastrtps_c"
  "cpp:rosidl_typesupport_cpp:rosidl_typesupport_fastrtps_cpp"
)

install(
  TARGETS rmw_unix_socket_cpp
  EXPORT export_rmw_unix_socket_cpp
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

ament_export_targets(export_rmw_unix_socket_cpp)

if(BUILD_TESTING)
  find_package(ament_cmake_gtest REQUIRED)
  # rosidl_buffer was introduced in Rolling and is referenced by test_msgs'
  # exported imported targets, but test_msgsConfig.cmake doesn't load it
  # transitively. Pull it in first (QUIET so the call is a no-op on Jazzy /
  # Kilted where the package doesn't exist).
  find_package(rosidl_buffer QUIET)
  find_package(test_msgs REQUIRED)
  find_package(rosidl_typesupport_cpp REQUIRED)

  # Common includes for unit tests that compile sources directly
  set(_unit_test_deps rmw::rmw
    fastcdr
    rosidl_typesupport_fastrtps_c::rosidl_typesupport_fastrtps_c
    rosidl_typesupport_fastrtps_cpp::rosidl_typesupport_fastrtps_cpp
    pthread rt)

  # Imported targets for tests that pull in IDL-generated message types.
  # ament_target_dependencies() was removed in Rolling, so link directly to
  # ${test_msgs_TARGETS} (set by find_package(test_msgs)) and the
  # rosidl_typesupport_cpp imported target. Same pattern works on Jazzy/Kilted.
  set(_test_msg_deps
    ${test_msgs_TARGETS}
    rosidl_typesupport_cpp::rosidl_typesupport_cpp)

  # Unit test: registry
  ament_add_gtest(test_registry test/test_registry.cpp src/registry.cpp)
  target_link_libraries(test_registry ${_unit_test_deps})
  target_include_directories(test_registry PRIVATE src)

  # Unit test: registry concurrency / lock-free seqlock stress
  ament_add_gtest(test_registry_concurrent
    test/test_registry_concurrent.cpp src/registry.cpp)
  target_link_libraries(test_registry_concurrent ${_unit_test_deps})
  target_include_directories(test_registry_concurrent PRIVATE src)

  # Unit test: serialization round-trips
  ament_add_gtest(test_serialization test/test_serialization.cpp src/serialization.cpp)
  target_link_libraries(test_serialization ${_unit_test_deps} ${_test_msg_deps})
  target_include_directories(test_serialization PRIVATE src)

  # Unit test: transport
  ament_add_gtest(test_transport test/test_transport.cpp src/transport.cpp)
  target_link_libraries(test_transport ${_unit_test_deps})
  target_include_directories(test_transport PRIVATE src)

  # Common link/include for all RMW API tests (they include types.hpp which
  # needs fastrtps headers; linking the shared lib pulls them in).
  set(_rmw_test_libs rmw_unix_socket_cpp
    rosidl_typesupport_fastrtps_cpp::rosidl_typesupport_fastrtps_cpp)

  # RMW API: init/shutdown lifecycle
  ament_add_gtest(test_rmw_init test/test_rmw_init.cpp)
  target_link_libraries(test_rmw_init ${_rmw_test_libs})
  target_include_directories(test_rmw_init PRIVATE src)

  # RMW API: node management
  ament_add_gtest(test_rmw_node test/test_rmw_node.cpp)
  target_link_libraries(test_rmw_node ${_rmw_test_libs})
  target_include_directories(test_rmw_node PRIVATE src)

  # RMW API: guard conditions
  ament_add_gtest(test_rmw_guard_condition test/test_rmw_guard_condition.cpp)
  target_link_libraries(test_rmw_guard_condition ${_rmw_test_libs})
  target_include_directories(test_rmw_guard_condition PRIVATE src)

  # RMW API: publish/subscribe round-trip
  ament_add_gtest(test_rmw_pub_sub test/test_rmw_pub_sub.cpp)
  target_link_libraries(test_rmw_pub_sub ${_rmw_test_libs} ${_test_msg_deps})
  target_include_directories(test_rmw_pub_sub PRIVATE src)

  # RMW API: service/client round-trip
  ament_add_gtest(test_rmw_service_client test/test_rmw_service_client.cpp)
  target_link_libraries(test_rmw_service_client ${_rmw_test_libs} ${_test_msg_deps})
  target_include_directories(test_rmw_service_client PRIVATE src)

  # RMW API: wait set
  ament_add_gtest(test_rmw_wait test/test_rmw_wait.cpp)
  target_link_libraries(test_rmw_wait ${_rmw_test_libs} ${_test_msg_deps})
  target_include_directories(test_rmw_wait PRIVATE src)

  # RMW API: graph introspection
  ament_add_gtest(test_rmw_graph test/test_rmw_graph.cpp)
  target_link_libraries(test_rmw_graph ${_rmw_test_libs} ${_test_msg_deps})
  target_include_directories(test_rmw_graph PRIVATE src)

  # RMW API: QoS, TRANSIENT_LOCAL, multi-subscriber/client
  ament_add_gtest(test_rmw_qos test/test_rmw_qos.cpp)
  target_link_libraries(test_rmw_qos ${_rmw_test_libs} ${_test_msg_deps})
  target_include_directories(test_rmw_qos PRIVATE src)
endif()

ament_package()
