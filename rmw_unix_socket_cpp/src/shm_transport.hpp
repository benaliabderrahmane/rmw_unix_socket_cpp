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

#ifndef RMW_UNIX_SOCKET_CPP__SHM_TRANSPORT_HPP_
#define RMW_UNIX_SOCKET_CPP__SHM_TRANSPORT_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <sys/types.h>

namespace rmw_uds
{

// Shared-memory payload path for large topic messages.
//
// A payload at or above SHM_PAYLOAD_THRESHOLD does not travel inside the UDS
// datagram. The publisher writes the serialized bytes once into its own
// /dev/shm ring and fans out a fixed-size ShmPayloadDescriptor instead (the
// WireHeader msg_type gains the SHM_PAYLOAD_FLAG bit). Subscribers map the
// ring on first use and copy the payload out under a per-record seqlock —
// the same odd/even protocol the discovery registry uses — so a publisher
// that laps the ring is detected as a clean drop, never delivered corrupt.
// This removes the two kernel copies (and the net.core.wmem_max datagram
// cap) from the large-message path; the datagram itself stays tiny.

// Payloads >= this many bytes are staged in shared memory. Below it the
// inline datagram is both simpler and faster (no page faults, no seqlock).
static constexpr size_t SHM_PAYLOAD_THRESHOLD = 64 * 1024;

// Smallest ring a publisher creates. A ring is always sized to hold at least
// four records of the largest payload staged so far, so a subscriber that is
// one wait-wakeup behind still finds the record intact (KEEP_LAST depths
// above that are bounded by the socket queue, not the ring).
static constexpr size_t SHM_RING_MIN_BYTES = 8 * 1024 * 1024;

static constexpr uint32_t SHM_RING_MAGIC = 0x52534455;  // "UDSR"
static constexpr uint32_t SHM_RING_VERSION = 1;

// WireHeader::msg_type high bit: the datagram payload is a
// ShmPayloadDescriptor, not CDR bytes. The low bits keep their meaning
// (0 = topic message; the flag is only ever set on topic messages).
static constexpr uint8_t SHM_PAYLOAD_FLAG = 0x80;

// Datagram payload when SHM_PAYLOAD_FLAG is set. Blitted onto the wire, so
// the packed layout is a cross-process contract like WireHeader.
struct __attribute__((packed)) ShmPayloadDescriptor
{
  uint64_t segment_id;    // ring segment generation (part of the shm name)
  uint64_t offset;        // byte offset of the record inside the ring area
  uint32_t seq;           // expected (even) seqlock value of the record
  uint32_t payload_size;  // CDR payload length
  int32_t owner_pid;      // with owner_id: names the publisher's segment
  uint32_t owner_id;      // per-process unique ring id
};

static_assert(sizeof(ShmPayloadDescriptor) == 32, "descriptor wire layout changed");
static_assert(offsetof(ShmPayloadDescriptor, segment_id) == 0, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, offset) == 8, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, seq) == 16, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, payload_size) == 20, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, owner_pid) == 24, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, owner_id) == 28, "descriptor layout changed");

// First bytes of every ring segment. Readers validate it before trusting any
// offset out of a descriptor.
struct ShmRingHeader
{
  uint32_t magic;
  uint32_t version;
  uint64_t ring_bytes;  // capacity of the record area that follows
};

static_assert(sizeof(ShmRingHeader) == 16, "ring header shm layout changed");

// One record in the ring: header immediately followed by the payload bytes.
// Records are 64-byte aligned. seq carries an absolute value from the
// writer's monotonic counter (2*n while record n is stable, 2*n-1 while it
// is being written), so a reader comparing against the descriptor's expected
// value detects both "still being written" and "already overwritten".
struct ShmRecordHeader
{
  std::atomic<uint32_t> seq;
  uint32_t payload_len;
};

static_assert(sizeof(ShmRecordHeader) == 8, "record header shm layout changed");

// Publisher-side ring state (process-local; the mapped segment is the shared
// part). Guarded by UdsPublisher::shm_mutex — only the owning process writes.
struct ShmRingWriter
{
  int fd = -1;
  uint8_t * base = nullptr;   // mapping of ShmRingHeader + record area
  size_t ring_bytes = 0;      // record-area capacity
  uint64_t segment_id = 0;    // bumped when the ring is recreated bigger
  uint64_t next_offset = 0;   // ring cursor into the record area
  uint32_t next_index = 1;    // monotonic record counter (drives seq values)
  int32_t owner_pid = -1;
  uint32_t owner_id = 0;      // assigned from a process-global counter
  std::string shm_name;
};

// Subscriber-side cache of mapped publisher rings, keyed by shm name. One
// entry per (publisher ring, segment generation) seen; internally locked
// because rmw_wait and rmw_take may drain the same subscription concurrently.
struct ShmReaderCache
{
  struct Mapping
  {
    uint8_t * base = nullptr;
    size_t size = 0;
    // Ring capacity as validated at map time. Bounds checks use this
    // snapshot, never the live (owner-writable) header in the mapping.
    size_t ring_bytes = 0;
  };
  std::mutex mutex;
  std::unordered_map<std::string, Mapping> mappings;
};

// A dedicated shm segment holding exactly one immutable payload record, for
// TRANSIENT_LOCAL cached messages. Unlike the cycling ShmRingWriter, this
// segment is written once and never reused, so a late-joining subscriber can
// map and replay it at any time until the cache entry is evicted. Owns its
// mapping and shm name; the destructor unmaps and unlinks. Move-only, so a
// CachedMessage holding one can be shuffled through the replay deque without
// double-freeing the segment.
struct DurableShmSegment
{
  uint8_t * base = nullptr;
  size_t map_size = 0;
  std::string shm_name;

  DurableShmSegment() = default;
  ~DurableShmSegment();
  DurableShmSegment(const DurableShmSegment &) = delete;
  DurableShmSegment & operator=(const DurableShmSegment &) = delete;
};

// Stage a payload into the publisher's ring, creating the ring lazily and
// recreating it larger (new segment_id, old segment unlinked) when the
// payload outgrows it. Fills desc_out on success. Returns false when shared
// memory is unavailable (shm_open/ftruncate/mmap failure) — the caller falls
// back to sending the payload inline. Caller holds UdsPublisher::shm_mutex.
bool shm_stage_payload(
  ShmRingWriter & ring,
  size_t domain_id,
  const uint8_t * payload,
  size_t payload_size,
  ShmPayloadDescriptor & desc_out);

// Stage a payload into a fresh, dedicated, immutable segment and fill desc_out.
// The returned segment stays valid (its record never overwritten) until the
// handle is destroyed, so a TRANSIENT_LOCAL publisher can cache the descriptor
// and replay it to late joiners. Returns nullptr when shared memory is
// unavailable — the caller falls back to caching the payload inline. The
// resulting descriptor is read by the same shm_fetch_payload path as ring
// payloads; readers cannot tell the two apart.
std::unique_ptr<DurableShmSegment> shm_stage_durable(
  size_t domain_id,
  const uint8_t * payload,
  size_t payload_size,
  ShmPayloadDescriptor & desc_out);

// Replace payload_io (a ShmPayloadDescriptor as received off the wire) with
// the payload bytes it describes, copied out under the record seqlock.
// Returns false when the descriptor is malformed, the segment cannot be
// mapped (publisher gone), or the record was overwritten (publisher lapped
// the ring) — all of which the caller treats as a dropped message.
bool shm_fetch_payload(
  ShmReaderCache & cache,
  size_t domain_id,
  std::vector<uint8_t> & payload_io);

// Unmap + unlink the publisher's current segment (publisher destruction).
void shm_writer_close(ShmRingWriter & ring);

// Unmap every cached segment (subscription destruction).
void shm_reader_close(ShmReaderCache & cache);

// Scan /dev/shm for ring segments of this domain whose owner PID (encoded in
// the name) is no longer alive in our PID namespace, and unlink them. Same
// contract and reasoning as cleanup_orphan_socket_files.
void shm_cleanup_orphan_segments(size_t domain_id);

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__SHM_TRANSPORT_HPP_
