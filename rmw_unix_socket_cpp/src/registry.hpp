#ifndef RMW_UNIX_SOCKET_CPP__REGISTRY_HPP_
#define RMW_UNIX_SOCKET_CPP__REGISTRY_HPP_

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include <pthread.h>
#include <sys/types.h>

#include "rmw/types.h"

namespace rmw_uds
{

static constexpr uint32_t REGISTRY_MAX_ENTRIES = 32768;
static constexpr uint32_t REGISTRY_VERSION = 2;

enum RegistryEntryType : uint8_t
{
  ENTRY_EMPTY = 0,
  ENTRY_NODE,
  ENTRY_PUBLISHER,
  ENTRY_SUBSCRIPTION,
  ENTRY_SERVICE,
  ENTRY_CLIENT,
};

struct RegistryEntry
{
  RegistryEntryType type;
  uint8_t pad[3];
  pid_t pid;
  uint8_t gid[16];
  char node_name[256];
  char node_namespace[256];
  char topic_name[256];
  char type_name[256];
  char socket_path[108];
  uint8_t qos_reliability;
  uint8_t qos_durability;
  uint8_t qos_history;
  uint8_t pad2;
  uint32_t qos_depth;
};

struct RegistryHeader
{
  uint32_t version;
  uint32_t max_entries;
  uint64_t generation;
  pthread_mutex_t mutex;
  // Entries follow immediately after the header in shared memory.
  // Access via registry_entries() helper function.
};

// Open or create the shared memory registry for the given domain_id.
// Returns fd >= 0 on success, sets *out_ptr and *out_size.
int registry_open(
  size_t domain_id, void ** out_ptr, size_t * out_size);

// Close the shared memory registry.
void registry_close(int fd, void * ptr, size_t size);

// Lock the registry mutex (handles EOWNERDEAD).
// Returns 0 on success.
int registry_lock(RegistryHeader * header);

// Unlock the registry mutex.
void registry_unlock(RegistryHeader * header);

// Add an entry. Returns slot index or -1 on failure.
// Caller must fill the RegistryEntry fields before calling.
int32_t registry_add(RegistryHeader * header, const RegistryEntry & entry);

// Remove an entry by index.
void registry_remove(RegistryHeader * header, int32_t index);

// Clean up stale entries (dead PIDs).
void registry_cleanup_stale(RegistryHeader * header);

// Get current generation counter.
uint64_t registry_generation(RegistryHeader * header);

// Query helpers - return matching entries

struct RegistryQueryResult
{
  std::string node_name;
  std::string node_namespace;
  std::string topic_name;
  std::string type_name;
  std::string socket_path;
  uint8_t gid[16];
  RegistryEntryType type;
  rmw_qos_profile_t qos;
};

std::vector<RegistryQueryResult> registry_query(
  RegistryHeader * header,
  RegistryEntryType type_filter,          // ENTRY_EMPTY means match all
  const char * topic_filter,              // nullptr means match all
  const char * node_name_filter,          // nullptr means match all
  const char * node_namespace_filter);    // nullptr means match all

inline RegistryHeader * registry_header(void * ptr)
{
  return static_cast<RegistryHeader *>(ptr);
}

inline RegistryEntry * registry_entries(RegistryHeader * header)
{
  return reinterpret_cast<RegistryEntry *>(
    reinterpret_cast<uint8_t *>(header) + sizeof(RegistryHeader));
}

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__REGISTRY_HPP_
