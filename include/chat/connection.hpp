#pragma once
// Connection: one accepted TCP client in the epoll server.
//
// Ownership / thread rules (this is how two threads never touch a socket at
// once):
//   * Only the REACTOR thread ever reads, writes or closes the fd, and only it
//     touches `decoder`, `wbuf_` and the interest flags.
//   * Worker threads never use the fd. They (a) drain the per-connection inbox
//     and (b) append reply frames to the out queue with enqueue().
//   * Per-connection ordering: at most one worker drains the inbox at a time
//     (the `scheduled` flag), so a client's messages are processed in order.
//   * Shared state is behind in_mu_ / out_mu_, each a leaf lock (never nested,
//     never held across a syscall or while calling into ChatHub).
//   * Workers hold shared_ptr<Connection>; the registry holds the sessions'
//     send callbacks with weak_ptr, so a closed connection is simply skipped.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include "chat/chat_hub.hpp"
#include "chat/codec.hpp"
#include "chat/fd.hpp"

namespace chat {

class Connection {
 public:
  enum class FlushResult { Drained, Blocked, Error };
  enum class PushResult { Queued, NeedSchedule, Overflow };

  Connection(std::uint64_t id, FileDescriptor fd, std::string peer, std::size_t max_frame,
             std::size_t max_out_bytes, std::size_t max_inbox);

  std::uint64_t id() const { return id_; }
  const std::string& peer() const { return peer_; }
  int fd() const { return fd_.get(); }                // reactor thread only
  void close_fd() { fd_.reset(); }                    // reactor thread only

  // ---- reactor thread only
  FrameDecoder decoder;
  bool want_out = false;  // currently registered for EPOLLOUT
  void touch();           // note client activity (idle timer)
  std::chrono::milliseconds idle_for() const;
  // Write as much queued output as the socket accepts.
  FlushResult flush();

  // ---- any thread
  // Append a frame to the out queue. Returns false if the connection is closed
  // or the queue would exceed max_out_bytes (slow client / backpressure).
  enum class EnqueueResult { Ok, Closed, Overflow };
  EnqueueResult enqueue(const std::string& frame);

  // Inbox handed from the reactor to workers.
  PushResult push_inbox(std::string msg);
  bool pop_inbox(std::string& msg);  // false => inbox empty and `scheduled` cleared

  std::atomic<bool> closed{false};
  std::atomic<bool> closing{false};         // close once the out queue drains
  std::atomic<bool> flush_pending{false};   // a flush task is already posted
  std::atomic<bool> overflow_reported{false};

  std::size_t loop_index = 0;  // which I/O loop owns the fd; set once before sharing
  SessionPtr session;  // set once, right after construction, before sharing

 private:
  const std::uint64_t id_;
  FileDescriptor fd_;
  const std::string peer_;
  const std::size_t max_out_bytes_;
  const std::size_t max_inbox_;
  std::atomic<std::int64_t> last_active_ms_;

  std::mutex in_mu_;
  std::deque<std::string> inbox_;
  bool scheduled_ = false;

  std::mutex out_mu_;
  std::deque<std::string> out_;
  std::atomic<std::size_t> queued_bytes_{0};  // out_ + unsent part of wbuf_

  std::string wbuf_;        // reactor thread only: batch currently being written
  std::size_t woff_ = 0;
};

}  // namespace chat
