#pragma once
// ChatServer: epoll reactor (I/O only) + thread pool (message processing).
//
//   reactor thread : accept, read bytes, frame them, flush write queues,
//                    idle timer, signals, UDP receive.
//   worker threads : parse commands, route/broadcast through ChatHub.
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "chat/chat_hub.hpp"
#include "chat/codec.hpp"
#include "chat/connection.hpp"
#include "chat/epoll_reactor.hpp"
#include "chat/thread_pool.hpp"

namespace chat {

struct ServerConfig {
  std::string host = "0.0.0.0";
  std::uint16_t port = 9000;  // 0 = pick an ephemeral port
  std::size_t workers = 4;
  std::size_t io_threads = 1;           // reactor loops; each has its own SO_REUSEPORT listener
  std::size_t max_connections = 20000;
  unsigned idle_timeout_s = 300;        // 0 disables
  std::size_t max_frame = 64 * 1024;    // largest accepted message payload
  std::size_t max_out_bytes = 1 << 20;  // per-client write backlog before disconnect
  std::size_t max_inbox = 1024;         // per-client unprocessed messages before disconnect
  bool edge_triggered = true;           // false = level-triggered (for comparison)
  bool enable_udp = true;
  bool handle_signals = false;          // SIGINT/SIGTERM => graceful stop via signalfd
  int backlog = 4096;
};

struct ServerStats {
  std::uint64_t accepted = 0, rejected = 0;
  std::uint64_t closed_idle = 0, closed_overflow = 0, closed_protocol = 0;
  std::uint64_t messages = 0;
  std::uint64_t udp_received = 0, udp_invalid = 0, udp_lost = 0, udp_reordered = 0,
                udp_duplicates = 0;
  std::size_t connections = 0;
};

class ChatServer {
 public:
  explicit ChatServer(ServerConfig cfg);
  ~ChatServer();
  ChatServer(const ChatServer&) = delete;
  ChatServer& operator=(const ChatServer&) = delete;

  // Bind sockets and spin up workers. Throws std::system_error on failure.
  // Call before run(); afterwards port() is valid.
  void start();
  // Run the reactor on the calling thread until request_stop() or a signal.
  void run();
  // Thread-safe; makes run() return after closing all connections.
  void request_stop();

  std::uint16_t port() const { return port_; }
  ServerStats stats() const;
  ChatHub& hub() { return hub_; }

 private:
  // One I/O loop = one thread + one epoll instance + the connections it owns.
  // A connection never migrates, so only its loop's thread touches its fd.
  struct Loop {
    EpollReactor reactor;
    FileDescriptor listener, timer, reserve_fd;  // reserve_fd: spare fd released to survive EMFILE
    std::unordered_map<std::uint64_t, std::shared_ptr<Connection>> conns;  // loop thread only
    std::thread thread;
    std::size_t index = 0;
  };
  Loop& loop_of(const std::shared_ptr<Connection>& c) { return *loops_[c->loop_index]; }

  // loop-thread handlers
  void on_accept(Loop& L);
  void on_conn_event(Loop& L, const std::shared_ptr<Connection>& c, std::uint32_t events);
  void on_readable(Loop& L, const std::shared_ptr<Connection>& c);
  void on_writable(Loop& L, const std::shared_ptr<Connection>& c);
  void on_udp();    // loop 0 only
  void on_timer(Loop& L);
  void close_connection(Loop& L, const std::shared_ptr<Connection>& c, const char* reason);
  void update_interest(Loop& L, const std::shared_ptr<Connection>& c);
  bool shed_one_connection(Loop& L);  // EMFILE recovery

  // any thread
  SendFn make_sender(std::weak_ptr<Connection> w);
  void request_flush(const std::shared_ptr<Connection>& c);
  void process(const std::shared_ptr<Connection>& c);  // worker: drain inbox

  struct Counters {
    std::atomic<std::uint64_t> accepted{0}, rejected{0}, closed_idle{0}, closed_overflow{0},
        closed_protocol{0}, messages{0}, udp_received{0}, udp_invalid{0}, udp_reordered{0},
        udp_duplicates{0};
    std::atomic<std::int64_t> udp_lost{0};
  };

  ServerConfig cfg_;
  std::uint16_t port_ = 0;
  Counters ctr_;
  ChatHub hub_;
  std::unique_ptr<ThreadPool> pool_;
  std::vector<std::unique_ptr<Loop>> loops_;

  FileDescriptor udp_, sigfd_;  // both live on loop 0
  std::atomic<std::uint64_t> next_conn_id_{1};
  std::unordered_map<std::string, SequenceTracker> udp_peers_;  // loop 0 only
  std::atomic<std::size_t> conn_count_{0};
};

}  // namespace chat
