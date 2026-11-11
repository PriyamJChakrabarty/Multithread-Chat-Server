#include "chat/connection.hpp"

#include <sys/socket.h>

#include <cerrno>

namespace chat {

namespace {
std::int64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}
}  // namespace

Connection::Connection(std::uint64_t id, FileDescriptor fd, std::string peer, std::size_t max_frame,
                       std::size_t max_out_bytes, std::size_t max_inbox)
    : decoder(max_frame),
      id_(id),
      fd_(std::move(fd)),
      peer_(std::move(peer)),
      max_out_bytes_(max_out_bytes),
      max_inbox_(max_inbox),
      last_active_ms_(now_ms()) {}

void Connection::touch() { last_active_ms_.store(now_ms(), std::memory_order_relaxed); }

std::chrono::milliseconds Connection::idle_for() const {
  return std::chrono::milliseconds(now_ms() - last_active_ms_.load(std::memory_order_relaxed));
}

Connection::EnqueueResult Connection::enqueue(const std::string& frame) {
  if (closed.load()) return EnqueueResult::Closed;
  // Reserve first so concurrent producers can't jointly blow past the limit.
  const std::size_t prev = queued_bytes_.fetch_add(frame.size());
  if (prev + frame.size() > max_out_bytes_) {
    queued_bytes_.fetch_sub(frame.size());
    return EnqueueResult::Overflow;
  }
  std::lock_guard<std::mutex> lk(out_mu_);
  out_.push_back(frame);
  return EnqueueResult::Ok;
}

Connection::FlushResult Connection::flush() {
  for (;;) {
    if (woff_ >= wbuf_.size()) {
      wbuf_.clear();
      woff_ = 0;
      {
        // Take everything queued in one go; the syscall below runs unlocked.
        std::lock_guard<std::mutex> lk(out_mu_);
        for (auto& f : out_) wbuf_ += f;
        out_.clear();
      }
      if (wbuf_.empty()) return FlushResult::Drained;
    }
    ssize_t n = ::send(fd_.get(), wbuf_.data() + woff_, wbuf_.size() - woff_, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) return FlushResult::Blocked;  // partial write
      return FlushResult::Error;  // EPIPE / ECONNRESET: peer vanished mid-write
    }
    woff_ += static_cast<std::size_t>(n);
    queued_bytes_.fetch_sub(static_cast<std::size_t>(n));
  }
}

Connection::PushResult Connection::push_inbox(std::string msg) {
  std::lock_guard<std::mutex> lk(in_mu_);
  if (inbox_.size() >= max_inbox_) return PushResult::Overflow;
  inbox_.push_back(std::move(msg));
  if (scheduled_) return PushResult::Queued;
  scheduled_ = true;
  return PushResult::NeedSchedule;
}

bool Connection::pop_inbox(std::string& msg) {
  std::lock_guard<std::mutex> lk(in_mu_);
  if (inbox_.empty()) {
    scheduled_ = false;
    return false;
  }
  msg = std::move(inbox_.front());
  inbox_.pop_front();
  return true;
}

}  // namespace chat
