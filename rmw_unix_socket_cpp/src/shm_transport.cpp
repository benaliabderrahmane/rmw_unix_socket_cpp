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

#include "shm_transport.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "logging.hpp"

namespace rmw_uds
{

// Per-process counter so every publisher ring in a process gets its own name.
static std::atomic<uint32_t> g_shm_owner_counter{1};

// Records are cache-line aligned: keeps the record seq atomic naturally
// aligned at any cursor position and avoids the payload of one record
// false-sharing with the header of the next.
static constexpr size_t SHM_RECORD_ALIGN = 64;

static size_t align_up(size_t v, size_t a)
{
  return (v + a - 1) & ~(a - 1);
}

static std::string make_segment_name(
  size_t domain_id, int32_t pid, uint32_t owner_id, uint64_t segment_id)
{
  char name[96];
  std::snprintf(name, sizeof(name), "/ros2_uds_data_%zu_%d_%u_%llu",
    domain_id, pid, owner_id,
    static_cast<unsigned long long>(segment_id));
  return std::string(name);
}

// Create and map a fresh segment sized for record_area_bytes. Returns false
// (with the writer left closed) on any failure; the caller sends inline.
static bool create_segment(
  ShmRingWriter & ring, size_t domain_id, size_t record_area_bytes)
{
  if (ring.owner_pid < 0) {
    ring.owner_pid = getpid();
    // Mix a time component into the id, exactly like make_socket_path does
    // for socket names: the OS recycles PIDs, and if a new incarnation could
    // regenerate a dead publisher's segment name, a subscriber's cached
    // mapping of the dead ring would silently alias the new one.
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    uint32_t time_component = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(now).count() & 0xFFFF);
    ring.owner_id =
      (g_shm_owner_counter.fetch_add(1, std::memory_order_relaxed) << 16) |
      time_component;
  }
  const uint64_t new_segment_id = ring.segment_id + 1;
  std::string name = make_segment_name(
    domain_id, ring.owner_pid, ring.owner_id, new_segment_id);

  // O_EXCL: the name encodes pid + per-process ids, so an existing segment
  // can only be an orphan from a recycled PID — unlink it and retry once.
  // 0644, not the registry's 0666: the ring is single-writer. Subscribers in
  // other accounts map it read-only, and nothing else may scribble on it.
  int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
  if (fd < 0 && errno == EEXIST) {
    shm_unlink(name.c_str());
    fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
  }
  if (fd < 0) {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "shm_open('%s') failed: %s — sending payload inline",
      name.c_str(), std::strerror(errno));
    return false;
  }

  // posix_fallocate rather than a bare ftruncate: tmpfs sizes files lazily,
  // and on a full /dev/shm the deferred page allocation would surface as
  // SIGBUS inside the staging memcpy. Reserving the pages up front turns
  // out-of-space into a clean inline fallback here instead.
  const size_t total = sizeof(ShmRingHeader) + record_area_bytes;
  const int alloc_err = posix_fallocate(fd, 0, static_cast<off_t>(total));
  if (alloc_err != 0) {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "posix_fallocate('%s', %zu) failed: %s — sending payload inline",
      name.c_str(), total, std::strerror(alloc_err));
    close(fd);
    shm_unlink(name.c_str());
    return false;
  }

  void * base = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (base == MAP_FAILED) {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "mmap('%s', %zu) failed: %s — sending payload inline",
      name.c_str(), total, std::strerror(errno));
    close(fd);
    shm_unlink(name.c_str());
    return false;
  }

  // The new segment is ready before the old one goes away, so a descriptor
  // in flight against the old segment keeps working for subscribers that
  // already mapped it; unlinking only removes the name.
  shm_writer_close(ring);

  auto * header = reinterpret_cast<ShmRingHeader *>(base);
  header->magic = SHM_RING_MAGIC;
  header->version = SHM_RING_VERSION;
  header->ring_bytes = record_area_bytes;

  ring.fd = fd;
  ring.base = static_cast<uint8_t *>(base);
  ring.ring_bytes = record_area_bytes;
  ring.segment_id = new_segment_id;
  ring.next_offset = 0;
  ring.shm_name = std::move(name);
  return true;
}

bool shm_stage_payload(
  ShmRingWriter & ring,
  size_t domain_id,
  const uint8_t * payload,
  size_t payload_size,
  ShmPayloadDescriptor & desc_out)
{
  // The record header and the wire descriptor carry 32-bit lengths.
  if (payload_size > UINT32_MAX) {
    return false;  // inline path; the send will fail loudly with EMSGSIZE
  }

  const size_t record_bytes =
    align_up(sizeof(ShmRecordHeader) + payload_size, SHM_RECORD_ALIGN);

  // Lazily create the ring, and recreate it bigger when a payload outgrows
  // the four-records-of-the-largest guarantee. The ring only ever grows.
  if (!ring.base || record_bytes * 4 > ring.ring_bytes) {
    const size_t page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    size_t area = record_bytes * 4;
    if (area < SHM_RING_MIN_BYTES) {
      area = SHM_RING_MIN_BYTES;
    }
    if (!create_segment(ring, domain_id, align_up(area, page))) {
      return false;
    }
  }

  // Records never straddle the end of the ring: wrap the cursor instead so a
  // record is always one contiguous span readers can bounds-check.
  if (ring.next_offset + record_bytes > ring.ring_bytes) {
    ring.next_offset = 0;
  }

  auto * record = reinterpret_cast<ShmRecordHeader *>(
    ring.base + sizeof(ShmRingHeader) + ring.next_offset);
  const uint32_t seq_stable = ring.next_index * 2;

  // Seqlock write, same protocol as the registry slots: odd while the bytes
  // are in flux, even (and equal to the descriptor) once stable. exchange is
  // an acq_rel RMW so the payload copy cannot be reordered ahead of it; the
  // slot may hold a stale record's seq or raw payload bytes, hence an
  // absolute exchange rather than the registry's fetch_add.
  record->seq.exchange(seq_stable - 1, std::memory_order_acq_rel);
  record->payload_len = static_cast<uint32_t>(payload_size);
  std::memcpy(record + 1, payload, payload_size);
  record->seq.store(seq_stable, std::memory_order_release);

  desc_out.segment_id = ring.segment_id;
  desc_out.offset = ring.next_offset;
  desc_out.seq = seq_stable;
  desc_out.payload_size = static_cast<uint32_t>(payload_size);
  desc_out.owner_pid = ring.owner_pid;
  desc_out.owner_id = ring.owner_id;

  ring.next_offset += record_bytes;
  ring.next_index += 1;
  return true;
}

std::unique_ptr<DurableShmSegment> shm_stage_durable(
  size_t domain_id,
  const uint8_t * payload,
  size_t payload_size,
  ShmPayloadDescriptor & desc_out)
{
  // The record header and the wire descriptor carry 32-bit lengths.
  if (payload_size > UINT32_MAX) {
    return nullptr;
  }
  // Test seam (cold path, large latched publishes only): when this env var is
  // set, behave as if shared memory were unavailable so tests can exercise the
  // inline fallback deterministically. Never set in production.
  if (std::getenv("RMW_UDS_TEST_FORCE_SHM_FAILURE") != nullptr) {
    return nullptr;
  }

  // One record at offset 0; the segment holds nothing else and never grows.
  const size_t record_bytes =
    align_up(sizeof(ShmRecordHeader) + payload_size, SHM_RECORD_ALIGN);
  const size_t page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
  const size_t record_area = align_up(record_bytes, page);
  const size_t total = sizeof(ShmRingHeader) + record_area;

  // Each durable segment gets its own owner_id, so its name never collides
  // with the publisher's ring or another cached message (and the reader's
  // per-owner stale-mapping sweep treats each as a distinct ring). Mix in a
  // time component for the same recycled-PID reason as create_segment.
  const int32_t pid = getpid();
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  uint32_t time_component = static_cast<uint32_t>(
    std::chrono::duration_cast<std::chrono::microseconds>(now).count() & 0xFFFF);
  const uint32_t owner_id =
    (g_shm_owner_counter.fetch_add(1, std::memory_order_relaxed) << 16) |
    time_component;
  const uint64_t segment_id = 1;
  std::string name = make_segment_name(domain_id, pid, owner_id, segment_id);

  int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
  if (fd < 0 && errno == EEXIST) {
    shm_unlink(name.c_str());
    fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
  }
  if (fd < 0) {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "shm_open('%s') failed: %s — caching payload inline",
      name.c_str(), std::strerror(errno));
    return nullptr;
  }

  const int alloc_err = posix_fallocate(fd, 0, static_cast<off_t>(total));
  if (alloc_err != 0) {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "posix_fallocate('%s', %zu) failed: %s — caching payload inline",
      name.c_str(), total, std::strerror(alloc_err));
    close(fd);
    shm_unlink(name.c_str());
    return nullptr;
  }

  void * base = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);  // the mapping keeps the segment alive; the fd is not needed
  if (base == MAP_FAILED) {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "mmap('%s', %zu) failed: %s — caching payload inline",
      name.c_str(), total, std::strerror(errno));
    shm_unlink(name.c_str());
    return nullptr;
  }

  auto * header = reinterpret_cast<ShmRingHeader *>(base);
  header->magic = SHM_RING_MAGIC;
  header->version = SHM_RING_VERSION;
  header->ring_bytes = record_area;

  // Single write-once record. No reader can reach it before the descriptor is
  // sent, so the payload copy followed by a release store of the stable (even)
  // seq is all the ordering the reader's acquire load needs.
  auto * record = reinterpret_cast<ShmRecordHeader *>(
    static_cast<uint8_t *>(base) + sizeof(ShmRingHeader));
  const uint32_t seq_stable = 2;  // index 1 * 2; matches shm_stage_payload
  record->payload_len = static_cast<uint32_t>(payload_size);
  std::memcpy(record + 1, payload, payload_size);
  record->seq.store(seq_stable, std::memory_order_release);

  desc_out.segment_id = segment_id;
  desc_out.offset = 0;
  desc_out.seq = seq_stable;
  desc_out.payload_size = static_cast<uint32_t>(payload_size);
  desc_out.owner_pid = pid;
  desc_out.owner_id = owner_id;

  auto seg = std::unique_ptr<DurableShmSegment>(new DurableShmSegment());
  seg->base = static_cast<uint8_t *>(base);
  seg->map_size = total;
  seg->shm_name = std::move(name);
  return seg;
}

DurableShmSegment::~DurableShmSegment()
{
  if (base) {
    munmap(base, map_size);
    base = nullptr;
  }
  if (!shm_name.empty()) {
    shm_unlink(shm_name.c_str());
    shm_name.clear();
  }
}

// Map (or find the cached mapping of) the segment a descriptor names.
// Returns nullptr when the segment is gone or fails validation.
static const ShmReaderCache::Mapping * map_segment(
  ShmReaderCache & cache, size_t domain_id, const ShmPayloadDescriptor & desc)
{
  const std::string name = make_segment_name(
    domain_id, desc.owner_pid, desc.owner_id, desc.segment_id);

  auto it = cache.mappings.find(name);
  if (it != cache.mappings.end()) {
    return &it->second;
  }

  // Publisher churn accumulates mappings of rings whose names are gone
  // (each incarnation gets a fresh name). Sweep them out when the cache has
  // grown past a handful of publishers, so steady state pays nothing.
  if (cache.mappings.size() >= 8) {
    for (auto stale = cache.mappings.begin(); stale != cache.mappings.end(); ) {
      int probe = shm_open(stale->first.c_str(), O_RDONLY, 0);
      if (probe < 0 && errno == ENOENT) {
        munmap(stale->second.base, stale->second.size);
        stale = cache.mappings.erase(stale);
      } else {
        if (probe >= 0) {
          close(probe);
        }
        ++stale;
      }
    }
  }

  int fd = shm_open(name.c_str(), O_RDONLY, 0);
  if (fd < 0) {
    // Publisher gone (or it already replaced this segment generation and we
    // never mapped it) — the message is lost. For ring payloads that matches an
    // exiting publisher on the inline path. For a durable TRANSIENT_LOCAL
    // segment it is a small divergence from the pre-durable code: a 64 KiB–4 MiB
    // latched datagram used to survive in the subscriber's socket buffer, but an
    // unlinked durable segment can no longer be mapped after the fact.
    return nullptr;
  }

  struct stat st;
  if (fstat(fd, &st) != 0 ||
    static_cast<size_t>(st.st_size) < sizeof(ShmRingHeader))
  {
    close(fd);
    return nullptr;
  }

  void * base = mmap(
    nullptr, static_cast<size_t>(st.st_size), PROT_READ, MAP_SHARED, fd, 0);
  close(fd);  // the mapping keeps the segment alive; the fd is not needed
  if (base == MAP_FAILED) {
    return nullptr;
  }

  // Validate the header before trusting any descriptor offset against it.
  // The subtraction form cannot wrap (st_size >= sizeof(ShmRingHeader) was
  // checked above), unlike sizeof + ring_bytes, which a hostile header
  // could overflow past the file size.
  const auto * header = reinterpret_cast<const ShmRingHeader *>(base);
  if (header->magic != SHM_RING_MAGIC || header->version != SHM_RING_VERSION ||
    header->ring_bytes >
    static_cast<size_t>(st.st_size) - sizeof(ShmRingHeader))
  {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "shm segment '%s' failed validation — dropping message", name.c_str());
    munmap(base, static_cast<size_t>(st.st_size));
    return nullptr;
  }

  // A new segment generation supersedes older ones from the same ring:
  // drop stale mappings so a long-lived subscription doesn't accumulate one
  // mapping per growth step of every publisher it ever listened to.
  char prefix[96];
  std::snprintf(prefix, sizeof(prefix), "/ros2_uds_data_%zu_%d_%u_",
    domain_id, desc.owner_pid, desc.owner_id);
  for (auto stale = cache.mappings.begin(); stale != cache.mappings.end(); ) {
    if (stale->first.rfind(prefix, 0) == 0) {
      munmap(stale->second.base, stale->second.size);
      stale = cache.mappings.erase(stale);
    } else {
      ++stale;
    }
  }

  ShmReaderCache::Mapping mapping;
  mapping.base = static_cast<uint8_t *>(base);
  mapping.size = static_cast<size_t>(st.st_size);
  // Snapshot the validated capacity: every later bounds check uses this
  // copy, never the live header, which the segment's owner keeps writable.
  mapping.ring_bytes = static_cast<size_t>(header->ring_bytes);
  return &cache.mappings.emplace(name, mapping).first->second;
}

bool shm_fetch_payload(
  ShmReaderCache & cache,
  size_t domain_id,
  std::vector<uint8_t> & payload_io)
{
  if (payload_io.size() != sizeof(ShmPayloadDescriptor)) {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "shm descriptor datagram has wrong size (%zu) — dropped",
      payload_io.size());
    return false;
  }
  ShmPayloadDescriptor desc;
  std::memcpy(&desc, payload_io.data(), sizeof(desc));

  std::lock_guard<std::mutex> lock(cache.mutex);

  const ShmReaderCache::Mapping * mapping = map_segment(cache, domain_id, desc);
  if (!mapping) {
    return false;
  }

  // Bounds-check the descriptor against the capacity snapshotted at map
  // time (never the live header, which the owner keeps writable): a corrupt
  // or hostile descriptor must not be able to read outside the mapping.
  const uint64_t ring_bytes = mapping->ring_bytes;
  if (desc.offset % SHM_RECORD_ALIGN != 0 ||
    desc.offset > ring_bytes ||
    sizeof(ShmRecordHeader) + static_cast<uint64_t>(desc.payload_size) >
    ring_bytes - desc.offset)
  {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000, "shm descriptor out of bounds (offset %llu, size %u) — dropped",
      static_cast<unsigned long long>(desc.offset), desc.payload_size);
    return false;
  }

  const auto * record = reinterpret_cast<const ShmRecordHeader *>(
    mapping->base + sizeof(ShmRingHeader) + desc.offset);

  // Seqlock read (registry protocol): the record must carry the expected
  // stable seq before the copy and still carry it after. Anything else means
  // the publisher lapped the ring and reused the slot — a clean drop.
  if (record->seq.load(std::memory_order_acquire) != desc.seq ||
    record->payload_len != desc.payload_size)
  {
    return false;
  }
  payload_io.assign(
    reinterpret_cast<const uint8_t *>(record + 1),
    reinterpret_cast<const uint8_t *>(record + 1) + desc.payload_size);
  // The fence keeps the payload loads above from sinking below the re-check
  // on weakly-ordered CPUs; a bare acquire load only pins later accesses.
  std::atomic_thread_fence(std::memory_order_acquire);
  if (record->seq.load(std::memory_order_relaxed) != desc.seq) {
    return false;  // overwritten mid-copy; payload_io holds torn bytes
  }
  return true;
}

void shm_writer_close(ShmRingWriter & ring)
{
  if (ring.base) {
    munmap(ring.base, sizeof(ShmRingHeader) + ring.ring_bytes);
    ring.base = nullptr;
  }
  if (ring.fd >= 0) {
    close(ring.fd);
    ring.fd = -1;
  }
  if (!ring.shm_name.empty()) {
    shm_unlink(ring.shm_name.c_str());
    ring.shm_name.clear();
  }
  ring.ring_bytes = 0;
  ring.next_offset = 0;
}

void shm_reader_close(ShmReaderCache & cache)
{
  std::lock_guard<std::mutex> lock(cache.mutex);
  for (auto & entry : cache.mappings) {
    munmap(entry.second.base, entry.second.size);
  }
  cache.mappings.clear();
}

void shm_cleanup_orphan_segments(size_t domain_id)
{
  // Name format (see make_segment_name, without the leading '/'):
  //   ros2_uds_data_<domain>_<pid>_<owner>_<segment>
  // Same dead-PID test as registry_cleanup_stale / cleanup_orphan_socket_files:
  // /proc only shows PIDs visible in our namespace, and a PID we cannot see
  // cannot be reached by our sockets either, so unlinking is safe.
  char prefix[64];
  std::snprintf(prefix, sizeof(prefix), "ros2_uds_data_%zu_", domain_id);
  const size_t prefix_len = std::strlen(prefix);

  DIR * dir = opendir("/dev/shm");
  if (!dir) {
    return;
  }
  struct dirent * ent;
  while ((ent = readdir(dir)) != nullptr) {
    if (std::strncmp(ent->d_name, prefix, prefix_len) != 0) {
      continue;
    }
    char * end = nullptr;
    long pid = std::strtol(ent->d_name + prefix_len, &end, 10);
    if (end == ent->d_name + prefix_len || *end != '_' || pid <= 0) {
      continue;
    }
    char proc_path[32];
    std::snprintf(proc_path, sizeof(proc_path), "/proc/%ld", pid);
    struct stat st;
    if (stat(proc_path, &st) == -1 && errno == ENOENT) {
      char shm_name[NAME_MAX + 2];
      std::snprintf(shm_name, sizeof(shm_name), "/%s", ent->d_name);
      shm_unlink(shm_name);
    }
  }
  closedir(dir);
}

}  // namespace rmw_uds
