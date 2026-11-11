#include "chat/epoll_server.hpp"

#include <fcntl.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <system_error>

#include "chat/logger.hpp"
#include "chat/net.hpp"

namespace chat {

namespace {
constexpr std::size_t kReadChunk = 16 * 1024;
constexpr int kWorkerBudget = 32;           // messages per worker turn (fairness)
constexpr std::size_t kMaxUdpPeers = 65536;  // cap on per-peer loss trackers

}  // namespace

ChatServer::ChatServer(ServerConfig cfg) : cfg_(std::move(cfg)) {}

ChatServer::~ChatServer() {
  if (pool_) pool_->stop();
}

void ChatServer::start() {
  const std::size_t want = cfg_.max_connections + 128;
  const std::size_t have = net::raise_nofile_limit(want);
  if (have < want)
    LOG_WARN("RLIMIT_NOFILE is " << have << ", below max_connections+128 (" << want
                                 << "); accept may hit EMFILE (handled by shedding)");

  const std::size_t nloops = cfg_.io_threads ? cfg_.io_threads : 1;
  const bool reuseport = nloops > 1;
  for (std::size_t i = 0; i < nloops; ++i) {
    auto L = std::make_unique<Loop>();
    L->index = i;
    // Loop 0 picks the port (possibly ephemeral); the others join it via SO_REUSEPORT
    // and the kernel spreads incoming connections across the listeners.
    L->listener = net::listen_tcp(cfg_.host, i == 0 ? cfg_.port : port_, cfg_.backlog, true, reuseport);
    if (i == 0) port_ = net::local_port(L->listener.get());
    L->reserve_fd.reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));
    loops_.push_back(std::move(L));
  }
  if (cfg_.enable_udp) udp_ = net::bind_udp(cfg_.host, port_, true);

  // Block SIGINT/SIGTERM BEFORE any thread exists so workers inherit the mask
  // and the signals are only ever consumed through the signalfd.
  if (cfg_.handle_signals) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &set, nullptr) != 0)
      throw std::system_error(errno, std::generic_category(), "pthread_sigmask");
    sigfd_.reset(::signalfd(-1, &set, SFD_NONBLOCK | SFD_CLOEXEC));
    if (!sigfd_) throw std::system_error(errno, std::generic_category(), "signalfd");
  }

  pool_ = std::make_unique<ThreadPool>(cfg_.workers);

  const std::uint32_t et = (cfg_.edge_triggered ? static_cast<std::uint32_t>(EPOLLET) : 0u);
  for (auto& lp : loops_) {
    Loop* L = lp.get();
    L->reactor.add(L->listener.get(), EPOLLIN | et, [this, L](std::uint32_t) { on_accept(*L); });
    if (cfg_.idle_timeout_s > 0) {
      L->timer.reset(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC));
      if (!L->timer) throw std::system_error(errno, std::generic_category(), "timerfd_create");
      itimerspec its{};
      its.it_interval.tv_sec = 1;
      its.it_value.tv_sec = 1;
      ::timerfd_settime(L->timer.get(), 0, &its, nullptr);
      L->reactor.add(L->timer.get(), EPOLLIN, [this, L](std::uint32_t) { on_timer(*L); });
    }
  }
  Loop& L0 = *loops_[0];
  if (udp_) L0.reactor.add(udp_.get(), EPOLLIN | et, [this](std::uint32_t) { on_udp(); });
  if (sigfd_) {
    L0.reactor.add(sigfd_.get(), EPOLLIN, [this](std::uint32_t) {
      signalfd_siginfo si;
      while (::read(sigfd_.get(), &si, sizeof si) == static_cast<ssize_t>(sizeof si)) {
        LOG_INFO("signal " << si.ssi_signo << " received, shutting down");
      }
      request_stop();
    });
  }
  LOG_INFO("listening on " << cfg_.host << ":" << port_ << " tcp" << (udp_ ? "+udp" : "") << ", "
                           << loops_.size() << " io threads, " << cfg_.workers << " workers, "
                           << (cfg_.edge_triggered ? "edge" : "level") << "-triggered");
}

void ChatServer::request_stop() {
  for (auto& L : loops_) L->reactor.stop();
}

void ChatServer::run() {
  for (std::size_t i = 1; i < loops_.size(); ++i) {
    Loop* L = loops_[i].get();
    L->thread = std::thread([L] { L->reactor.run(); });
  }
  loops_[0]->reactor.run();  // loop 0 runs on the calling thread
  request_stop();            // make sure the others stop too
  for (std::size_t i = 1; i < loops_.size(); ++i) loops_[i]->thread.join();

  // Graceful shutdown (all loops are quiescent now): close every client,
  // then let queued worker tasks finish.
  LOG_INFO("shutting down (" << conn_count_.load() << " connections open)");
  for (auto& L : loops_) {
    L->reactor.remove(L->listener.get());
    L->listener.reset();
    std::vector<std::shared_ptr<Connection>> all;
    for (auto& [id, c] : L->conns) all.push_back(c);
    for (auto& c : all) close_connection(*L, c, "server shutdown");
  }
  pool_->stop();
  auto s = stats();
  LOG_INFO("stats: accepted=" << s.accepted << " rejected=" << s.rejected << " messages="
                              << s.messages << " idle_closed=" << s.closed_idle
                              << " overflow_closed=" << s.closed_overflow
                              << " udp_rx=" << s.udp_received << " udp_lost=" << s.udp_lost);
}

ServerStats ChatServer::stats() const {
  ServerStats s;
  s.accepted = ctr_.accepted;
  s.rejected = ctr_.rejected;
  s.closed_idle = ctr_.closed_idle;
  s.closed_overflow = ctr_.closed_overflow;
  s.closed_protocol = ctr_.closed_protocol;
  s.messages = ctr_.messages;
  s.udp_received = ctr_.udp_received;
  s.udp_invalid = ctr_.udp_invalid;
  s.udp_reordered = ctr_.udp_reordered;
  s.udp_duplicates = ctr_.udp_duplicates;
  s.udp_lost = static_cast<std::uint64_t>(std::max<std::int64_t>(0, ctr_.udp_lost.load()));
  s.connections = conn_count_.load();
  return s;
}

// ------------------------------------------------------------------- accept
bool ChatServer::shed_one_connection(Loop& L) {
  // Out of fds: free the spare, accept the pending connection just to close it
  // (otherwise a level-triggered listener spins), then re-arm the spare.
  L.reserve_fd.reset();
  int fd = ::accept4(L.listener.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
  bool got = fd >= 0;
  if (got) {
    ::close(fd);
    ++ctr_.rejected;
  }
  L.reserve_fd.reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));
  return got;
}

void ChatServer::on_accept(Loop& L) {
  for (;;) {
    sockaddr_in peer{};
    socklen_t plen = sizeof peer;
    int fd = ::accept4(L.listener.get(), reinterpret_cast<sockaddr*>(&peer), &plen,
                       SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EINTR || errno == ECONNABORTED) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) return;  // drained
      if (errno == EMFILE || errno == ENFILE) {
        LOG_WARN("accept: " << net::errno_str(errno) << " - shedding a connection");
        if (shed_one_connection(L)) continue;
        return;
      }
      LOG_ERROR("accept: " << net::errno_str(errno));
      return;
    }
    FileDescriptor cfd(fd);
    if (conn_count_.load() >= cfg_.max_connections) {
      ++ctr_.rejected;  // cfd closes on scope exit
      continue;
    }
    try {
      net::set_tcp_nodelay(fd);
      net::set_keepalive(fd);
    } catch (const std::system_error& e) {
      LOG_WARN("socket option failed: " << e.what());
    }

    auto conn = std::make_shared<Connection>(next_conn_id_.fetch_add(1), std::move(cfd),
                                             net::addr_to_string(peer), cfg_.max_frame,
                                             cfg_.max_out_bytes, cfg_.max_inbox);
    conn->loop_index = L.index;
    conn->session = hub_.add_session(make_sender(conn));
    L.conns[conn->id()] = conn;
    ++conn_count_;
    ++ctr_.accepted;
    L.reactor.add(fd, EPOLLIN | EPOLLRDHUP | ((cfg_.edge_triggered ? static_cast<std::uint32_t>(EPOLLET) : 0u)),
                 [this, &L, conn](std::uint32_t ev) { on_conn_event(L, conn, ev); });
    LOG_DEBUG("accepted #" << conn->id() << " from " << conn->peer());
  }
}

// ------------------------------------------------------------ connection I/O
void ChatServer::on_conn_event(Loop& L, const std::shared_ptr<Connection>& c, std::uint32_t ev) {
  if (c->closed.load()) return;
  if (ev & EPOLLERR) return close_connection(L, c, "socket error");
  if (ev & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) {
    on_readable(L, c);
    if (c->closed.load()) return;
  }
  if (ev & EPOLLOUT) on_writable(L, c);
}

void ChatServer::on_readable(Loop& L, const std::shared_ptr<Connection>& c) {
  char buf[kReadChunk];
  for (;;) {
    ssize_t n = ::recv(c->fd(), buf, sizeof buf, 0);
    if (n > 0) {
      c->touch();
      if (c->closing.load()) continue;  // saying goodbye: discard further input
      c->decoder.feed(buf, static_cast<std::size_t>(n));
      while (auto msg = c->decoder.next()) {
        switch (c->push_inbox(std::move(*msg))) {
          case Connection::PushResult::Overflow:
            ++ctr_.closed_overflow;
            return close_connection(L, c, "inbox flood");
          case Connection::PushResult::NeedSchedule:
            pool_->submit([this, c] { process(c); });
            break;
          case Connection::PushResult::Queued:
            break;
        }
      }
      if (c->decoder.failed()) {
        ++ctr_.closed_protocol;
        return close_connection(L, c, "oversized frame");
      }
      // Edge-triggered: keep reading until EAGAIN or the edge is lost.
      // Level-triggered: epoll re-reports leftover data, one read is enough.
      if (!cfg_.edge_triggered) return;
      continue;
    }
    if (n == 0) return close_connection(L, c, "peer closed");
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return;  // drained
    return close_connection(L, c, net::errno_str(errno).c_str());
  }
}

void ChatServer::update_interest(Loop& L, const std::shared_ptr<Connection>& c) {
  std::uint32_t ev = EPOLLIN | EPOLLRDHUP | ((cfg_.edge_triggered ? static_cast<std::uint32_t>(EPOLLET) : 0u));
  if (c->want_out) ev |= EPOLLOUT;
  L.reactor.modify(c->fd(), ev);
}

void ChatServer::on_writable(Loop& L, const std::shared_ptr<Connection>& c) {
  if (c->closed.load()) return;
  switch (c->flush()) {
    case Connection::FlushResult::Error:
      return close_connection(L, c, "write error");
    case Connection::FlushResult::Blocked:  // partial write: wait for EPOLLOUT
      if (!c->want_out) {
        c->want_out = true;
        update_interest(L, c);
      }
      return;
    case Connection::FlushResult::Drained:
      if (c->want_out) {
        c->want_out = false;
        update_interest(L, c);
      }
      if (c->closing.load()) close_connection(L, c, "quit");
      return;
  }
}

void ChatServer::close_connection(Loop& L, const std::shared_ptr<Connection>& c, const char* reason) {
  if (c->closed.exchange(true)) return;
  LOG_DEBUG("closing #" << c->id() << " (" << c->peer() << "): " << reason);
  L.reactor.remove(c->fd());
  c->close_fd();  // after this no thread can touch the socket
  L.conns.erase(c->id());
  --conn_count_;
  if (c->session) hub_.remove_session(c->session);
}

// --------------------------------------------------------- worker-side bits
SendFn ChatServer::make_sender(std::weak_ptr<Connection> w) {
  return [this, w](std::string_view line) {
    auto c = w.lock();
    if (!c) return;  // connection already gone
    switch (c->enqueue(encode_frame(line))) {
      case Connection::EnqueueResult::Ok:
        request_flush(c);
        break;
      case Connection::EnqueueResult::Overflow:
        // Slow consumer: cut it loose instead of buffering without bound.
        if (!c->overflow_reported.exchange(true)) {
          loop_of(c).reactor.post([this, c] {
            ++ctr_.closed_overflow;
            close_connection(loop_of(c), c, "write backlog too large");
          });
        }
        break;
      case Connection::EnqueueResult::Closed:
        break;
    }
  };
}

void ChatServer::request_flush(const std::shared_ptr<Connection>& c) {
  // Coalesce: one posted task flushes everything queued by then.
  if (c->flush_pending.exchange(true)) return;
  loop_of(c).reactor.post([this, c] {
    c->flush_pending.store(false);
    on_writable(loop_of(c), c);
  });
}

void ChatServer::process(const std::shared_ptr<Connection>& c) {
  std::string msg;
  for (int i = 0; i < kWorkerBudget; ++i) {
    if (!c->pop_inbox(msg)) return;  // empty: `scheduled` cleared, a new push reschedules
    ++ctr_.messages;
    if (c->closed.load() || c->closing.load()) continue;
    if (hub_.handle_line(c->session, msg) == ChatHub::Result::Quit) {
      c->closing.store(true);
      request_flush(c);  // flush the farewell, then the reactor closes
    }
  }
  // Budget used up with work left: requeue behind other connections' tasks.
  pool_->submit([this, c] { process(c); });
}

// ---------------------------------------------------------------------- UDP
void ChatServer::on_udp() {
  char buf[2048];
  const int max_batch = cfg_.edge_triggered ? 1 << 30 : 64;
  for (int i = 0; i < max_batch; ++i) {
    sockaddr_in from{};
    socklen_t flen = sizeof from;
    ssize_t n = ::recvfrom(udp_.get(), buf, sizeof buf, 0, reinterpret_cast<sockaddr*>(&from), &flen);
    if (n < 0) {
      if (errno == EINTR) continue;
      return;  // EAGAIN: drained
    }
    auto dg = decode_datagram(buf, static_cast<std::size_t>(n));
    if (!dg) {
      ++ctr_.udp_invalid;
      continue;
    }
    ++ctr_.udp_received;

    // Loss / reorder detection from the per-datagram sequence number.
    if (udp_peers_.size() >= kMaxUdpPeers) udp_peers_.clear();
    auto& tracker = udp_peers_[net::addr_to_string(from)];
    auto before = tracker.stats();
    tracker.on_packet(dg->header.seq);
    auto after = tracker.stats();
    ctr_.udp_lost += static_cast<std::int64_t>(after.lost) - static_cast<std::int64_t>(before.lost);
    ctr_.udp_reordered += after.reordered - before.reordered;
    ctr_.udp_duplicates += after.duplicates - before.duplicates;

    switch (dg->header.type) {
      case DatagramType::Ping: {  // presence ping: echo seq + timestamp so the sender can compute RTT
        DatagramHeader h = dg->header;
        h.type = DatagramType::Pong;
        std::string reply = encode_datagram(h, dg->payload);
        ::sendto(udp_.get(), reply.data(), reply.size(), MSG_DONTWAIT,
                 reinterpret_cast<sockaddr*>(&from), flen);
        break;
      }
      case DatagramType::Message: {  // payload: "<nick> <text>"; routed by a worker
        std::string_view p = dg->payload;
        auto sp = p.find(' ');
        if (sp == std::string_view::npos || sp == 0) break;
        std::string nick(p.substr(0, sp)), text(p.substr(sp + 1));
        pool_->submit([this, nick = std::move(nick), text = std::move(text)] {
          hub_.deliver_udp(nick, text);
        });
        break;
      }
      case DatagramType::Pong:
        break;
    }
  }
}

// -------------------------------------------------------------------- timer
void ChatServer::on_timer(Loop& L) {
  std::uint64_t expirations;
  ssize_t r = ::read(L.timer.get(), &expirations, sizeof expirations);
  (void)r;
  const auto limit = std::chrono::seconds(cfg_.idle_timeout_s);
  std::vector<std::shared_ptr<Connection>> idle;
  for (auto& [id, c] : L.conns)
    if (c->idle_for() > limit) idle.push_back(c);
  for (auto& c : idle) {
    ++ctr_.closed_idle;
    close_connection(L, c, "idle timeout");
  }
}

}  // namespace chat
