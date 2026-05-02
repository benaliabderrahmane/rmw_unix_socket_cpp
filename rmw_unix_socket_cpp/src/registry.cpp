#include "registry.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>

#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "rmw/qos_profiles.h"

namespace rmw_uds
{

static size_t compute_registry_size()
{
  return sizeof(RegistryHeader) + REGISTRY_MAX_ENTRIES * sizeof(RegistryEntry);
}

int registry_open(size_t domain_id, void ** out_ptr, size_t * out_size)
{
  char name[64];
  std::snprintf(name, sizeof(name), "/ros2_uds_%zu", domain_id);

  size_t total_size = compute_registry_size();
  bool created = false;

  // Try to open existing first
  int fd = shm_open(name, O_RDWR, 0666);
  if (fd < 0) {
    // Create new
    fd = shm_open(name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
      return -1;
    }
    if (ftruncate(fd, static_cast<off_t>(total_size)) != 0) {
      close(fd);
      return -1;
    }
    created = true;
  }

  void * ptr = mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    close(fd);
    return -1;
  }

  auto * header = static_cast<RegistryHeader *>(ptr);

  if (created || header->version == 0) {
    // Initialize the header
    std::memset(ptr, 0, total_size);
    header->version = REGISTRY_VERSION;
    header->max_entries = REGISTRY_MAX_ENTRIES;
    header->generation = 0;

    // Initialize process-shared robust mutex
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&header->mutex, &attr);
    pthread_mutexattr_destroy(&attr);
  }

  *out_ptr = ptr;
  *out_size = total_size;
  return fd;
}

void registry_close(int fd, void * ptr, size_t size)
{
  if (ptr && ptr != MAP_FAILED) {
    munmap(ptr, size);
  }
  if (fd >= 0) {
    close(fd);
  }
}

int registry_lock(RegistryHeader * header)
{
  int ret = pthread_mutex_lock(&header->mutex);
  if (ret == EOWNERDEAD) {
    // Previous owner died — make the mutex consistent and clean up
    pthread_mutex_consistent(&header->mutex);
    registry_cleanup_stale(header);
    return 0;
  }
  return ret;
}

void registry_unlock(RegistryHeader * header)
{
  pthread_mutex_unlock(&header->mutex);
}

int32_t registry_add(RegistryHeader * header, const RegistryEntry & entry)
{
  auto * entries = registry_entries(header);

  for (uint32_t i = 0; i < header->max_entries; ++i) {
    if (entries[i].type == ENTRY_EMPTY) {
      entries[i] = entry;
      header->generation++;
      return static_cast<int32_t>(i);
    }
  }

  // Registry full — reclaim entries whose owner PID is dead, then retry once.
  // This covers the common case of processes killed without calling destroy.
  registry_cleanup_stale(header);
  for (uint32_t i = 0; i < header->max_entries; ++i) {
    if (entries[i].type == ENTRY_EMPTY) {
      entries[i] = entry;
      header->generation++;
      return static_cast<int32_t>(i);
    }
  }
  return -1;  // Registry full
}

void registry_remove(RegistryHeader * header, int32_t index)
{
  if (index < 0 || static_cast<uint32_t>(index) >= header->max_entries) {
    return;
  }

  auto * entries = registry_entries(header);
  auto & e = entries[index];

  // Unlink socket file if present
  if (e.socket_path[0] != '\0') {
    unlink(e.socket_path);
  }

  std::memset(&e, 0, sizeof(RegistryEntry));
  e.type = ENTRY_EMPTY;
  header->generation++;
}

void registry_cleanup_stale(RegistryHeader * header)
{
  auto * entries = registry_entries(header);

  for (uint32_t i = 0; i < header->max_entries; ++i) {
    if (entries[i].type == ENTRY_EMPTY) {
      continue;
    }

    // Check if the owning process is still alive using /proc/<pid>.
    // This is Docker-safe: /proc only shows PIDs visible in our PID namespace.
    // If /proc/<pid> doesn't exist, the PID is either dead or in another
    // PID namespace — either way, we can't reach it.
    // If /proc/<pid> exists but the process is in another container with
    // --pid=host, it's alive and we leave it alone.
    char proc_path[32];
    std::snprintf(proc_path, sizeof(proc_path), "/proc/%d", entries[i].pid);
    struct stat st;
    if (stat(proc_path, &st) == -1 && errno == ENOENT) {
      // PID doesn't exist in our namespace — clean up
      if (entries[i].socket_path[0] != '\0') {
        unlink(entries[i].socket_path);
      }
      std::memset(&entries[i], 0, sizeof(RegistryEntry));
      entries[i].type = ENTRY_EMPTY;
      header->generation++;
    }
  }
}

uint64_t registry_generation(RegistryHeader * header)
{
  return __atomic_load_n(&header->generation, __ATOMIC_ACQUIRE);
}

static rmw_qos_profile_t entry_to_qos(const RegistryEntry & e)
{
  rmw_qos_profile_t qos;
  std::memset(&qos, 0, sizeof(qos));
  qos.reliability = static_cast<rmw_qos_reliability_policy_e>(e.qos_reliability);
  qos.durability = static_cast<rmw_qos_durability_policy_e>(e.qos_durability);
  qos.history = static_cast<rmw_qos_history_policy_e>(e.qos_history);
  qos.depth = e.qos_depth;
  return qos;
}

std::vector<RegistryQueryResult> registry_query(
  RegistryHeader * header,
  RegistryEntryType type_filter,
  const char * topic_filter,
  const char * node_name_filter,
  const char * node_namespace_filter)
{
  std::vector<RegistryQueryResult> results;
  auto * entries = registry_entries(header);

  for (uint32_t i = 0; i < header->max_entries; ++i) {
    auto & e = entries[i];
    if (e.type == ENTRY_EMPTY) {
      continue;
    }

    // Filter by type
    if (type_filter != ENTRY_EMPTY && e.type != type_filter) {
      continue;
    }

    // Filter by topic
    if (topic_filter && std::strcmp(e.topic_name, topic_filter) != 0) {
      continue;
    }

    // Filter by node name
    if (node_name_filter && std::strcmp(e.node_name, node_name_filter) != 0) {
      continue;
    }

    // Filter by node namespace
    if (node_namespace_filter && std::strcmp(e.node_namespace, node_namespace_filter) != 0) {
      continue;
    }

    RegistryQueryResult r;
    r.node_name = e.node_name;
    r.node_namespace = e.node_namespace;
    r.topic_name = e.topic_name;
    r.type_name = e.type_name;
    r.socket_path = e.socket_path;
    std::memcpy(r.gid, e.gid, sizeof(r.gid));
    r.type = e.type;
    r.qos = entry_to_qos(e);
    results.push_back(r);
  }

  return results;
}

}  // namespace rmw_uds
