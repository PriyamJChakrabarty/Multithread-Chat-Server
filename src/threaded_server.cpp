#include "chat/threaded_server.hpp"

#include <sys/socket.h>
#include <sys/time.h>

#include <cerrno>
#include <thread>

#include "chat/codec.hpp"
#include "chat/logger.hpp"
#include "chat/net.hpp"

namespace chat {

namespace {
void set_timeout(int fd, int opt, unsigned seconds) {
  timeval tv{};
  tv.tv_sec = static_cast<time_t>(seconds);
  ::setsockopt(fd, SOL_SOCKET, opt, &tv, sizeof tv);
}
}  // namespace

ThreadedServer::~ThreadedServer() { request_stop(); }

void ThreadedServer::start() {
  net::raise_nofile_limit(cfg_.max_connections + 128);
  listener_ = net::listen_tcp(cfg_.host, cfg_.port, cfg_.backlog, /*nonblocking=*/false);
  port_ = net::local_port(listener_.get());
  LOG_INFO("threaded baseline listening on " << cfg_.host << ":" << port_);
}

void ThreadedServer::request_stop() {
  if (stopping_.exchange(true)) return;
  if (listener_) ::shutdown(listener_.get(), SHUT_RDWR);  // wakes the blocked accept()
}

void ThreadedServer::run() {
  while (!stopping_.load()) {
    int fd = ::accept4(listener_.get(), nullptr, nullptr, SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EINTR || errno == ECONNABORTED) continue;
      if (errno == EMFILE || errno == ENFILE) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }
      break;  // listener shut down (or fatal)
    }
    auto c = std::make_shared<Client>();
    c->fd.reset(fd);
    std::uint64_t id;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (clients_.size() >= cfg_.max_connections) {
        ++rejected_;
        continue;  // c closes the fd
      }
      id = next_id_++;
      clients_[id] = c;
    }
    ++accepted_;
    try {
      net::set_tcp_nodelay(fd);
      net::set_keepalive(fd);
    } catch (const std::system_error&) {}
    // Bound how long a stuck/slow peer can hold a thread (read idle, write stall).
    if (cfg_.idle_timeout_s > 0) set_timeout(fd, SO_RCVTIMEO, cfg_.idle_timeout_s);
    set_timeout(fd, SO_SNDTIMEO, 10);

    try {
      std::thread([this, c, id] { serve(c, id); }).detach();
    } catch (const std::system_error& e) {  // out of threads
      LOG_ERROR("cannot spawn thread: " << e.what());
      std::lock_guard<std::mutex> lk(mu_);
      clients_.erase(id);
    }
  }

  // Shutdown: kick every blocked reader awake, then wait for threads to leave.
  std::unique_lock<std::mutex> lk(mu_);
  for (auto& [id, c] : clients_) ::shutdown(c->fd.get(), SHUT_RDWR);
  cv_.wait(lk, [this] { return clients_.empty(); });
}

void ThreadedServer::serve(std::shared_ptr<Client> c, std::uint64_t id) {
  const int fd = c->fd.get();
  auto sess = hub_.add_session([c](std::string_view line) {
    std::string frame = encode_frame(line);
    std::lock_guard<std::mutex> lk(c->write_mu);
    // A blocking write under write_mu is fine here: it is a per-client leaf
    // lock and SO_SNDTIMEO bounds the stall. This is the baseline's weakness.
    net::write_all(c->fd.get(), frame.data(), frame.size());
  });

  FrameDecoder dec(cfg_.max_frame);
  char buf[16 * 1024];
  bool quit = false;
  while (!quit && !stopping_.load()) {
    ssize_t n = ::recv(fd, buf, sizeof buf, 0);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;  // EOF, error, or idle timeout (EAGAIN from SO_RCVTIMEO)
    dec.feed(buf, static_cast<std::size_t>(n));
    while (auto m = dec.next()) {
      ++messages_;
      if (hub_.handle_line(sess, *m) == ChatHub::Result::Quit) {
        quit = true;
        break;
      }
    }
    if (dec.failed()) break;
  }
  hub_.remove_session(sess);
  ::shutdown(fd, SHUT_RDWR);
  {
    std::lock_guard<std::mutex> lk(mu_);
    clients_.erase(id);
    cv_.notify_all();  // under the lock: run() may destroy the server right after it wakes
  }
}

ServerStats ThreadedServer::stats() const {
  ServerStats s;
  s.accepted = accepted_;
  s.rejected = rejected_;
  s.messages = messages_;
  std::lock_guard<std::mutex> lk(mu_);
  s.connections = clients_.size();
  return s;
}

}  // namespace chat
