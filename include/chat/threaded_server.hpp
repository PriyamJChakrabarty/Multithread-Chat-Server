#pragma once
// ThreadedServer: the thread-per-client baseline. Same protocol and ChatHub as
// the epoll server (TCP only), but each client gets a dedicated blocking
// thread. Exists so the benchmarks compare architectures, not feature sets.
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "chat/chat_hub.hpp"
#include "chat/epoll_server.hpp"  // ServerConfig / ServerStats
#include "chat/fd.hpp"

namespace chat {

class ThreadedServer {
 public:
  explicit ThreadedServer(ServerConfig cfg) : cfg_(std::move(cfg)) {}
  ~ThreadedServer();

  void start();
  void run();  // accept loop on the calling thread
  void request_stop();
  std::uint16_t port() const { return port_; }
  ServerStats stats() const;

 private:
  struct Client {
    FileDescriptor fd;
    std::mutex write_mu;  // serialises writers (the owner thread and broadcasters)
  };
  void serve(std::shared_ptr<Client> c, std::uint64_t id);

  ServerConfig cfg_;
  std::uint16_t port_ = 0;
  ChatHub hub_;
  FileDescriptor listener_;
  std::atomic<bool> stopping_{false};

  mutable std::mutex mu_;  // guards clients_
  std::condition_variable cv_;
  std::unordered_map<std::uint64_t, std::shared_ptr<Client>> clients_;
  std::uint64_t next_id_ = 1;

  std::atomic<std::uint64_t> accepted_{0}, rejected_{0}, messages_{0};
};

}  // namespace chat
