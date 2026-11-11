#pragma once
// EpollReactor: thin wrapper over epoll_create1/ctl/wait with one callback per
// fd and a thread-safe post() that runs a closure on the reactor thread
// (woken through an eventfd). add/modify/remove must be called from the
// reactor thread (or before run()); post()/stop() may be called from anywhere.
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "chat/fd.hpp"

namespace chat {

class EpollReactor {
 public:
  using Handler = std::function<void(std::uint32_t events)>;

  EpollReactor();
  EpollReactor(const EpollReactor&) = delete;
  EpollReactor& operator=(const EpollReactor&) = delete;

  void add(int fd, std::uint32_t events, Handler h);
  void modify(int fd, std::uint32_t events);
  void remove(int fd);  // safe to call from inside that fd's own handler

  void post(std::function<void()> fn);  // thread-safe
  void run();                           // blocks until stop()
  void stop();                          // thread-safe, idempotent

 private:
  void drain_posted();

  FileDescriptor epfd_;
  FileDescriptor wake_;  // eventfd
  std::atomic<bool> stop_{false};
  std::unordered_map<int, std::shared_ptr<Handler>> handlers_;  // reactor thread only
  std::mutex post_mu_;
  std::vector<std::function<void()>> posted_;
};

}  // namespace chat
