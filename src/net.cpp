#include "chat/net.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/resource.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace chat::net {

namespace {
[[noreturn]] void fail(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

sockaddr_in make_addr(const std::string& host, uint16_t port) {
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  if (host.empty() || host == "0.0.0.0") {
    a.sin_addr.s_addr = htonl(INADDR_ANY);
  } else if (inet_pton(AF_INET, host.c_str(), &a.sin_addr) != 1) {
    throw std::invalid_argument("bad IPv4 address: " + host);
  }
  return a;
}

void set_int_opt(int fd, int level, int name, int value, const char* what) {
  if (::setsockopt(fd, level, name, &value, sizeof value) < 0) fail(what);
}
}  // namespace

std::string errno_str(int err) { return std::strerror(err); }

void set_nonblocking(int fd) {
  int fl = ::fcntl(fd, F_GETFL, 0);
  if (fl < 0 || ::fcntl(fd, F_SETFL, fl | O_NONBLOCK) < 0) fail("fcntl(O_NONBLOCK)");
}

void set_tcp_nodelay(int fd) { set_int_opt(fd, IPPROTO_TCP, TCP_NODELAY, 1, "TCP_NODELAY"); }
void set_keepalive(int fd) { set_int_opt(fd, SOL_SOCKET, SO_KEEPALIVE, 1, "SO_KEEPALIVE"); }

FileDescriptor listen_tcp(const std::string& host, uint16_t port, int backlog,
                          bool nonblocking, bool reuseport) {
  int type = SOCK_STREAM | SOCK_CLOEXEC | (nonblocking ? SOCK_NONBLOCK : 0);
  FileDescriptor fd(::socket(AF_INET, type, 0));
  if (!fd) fail("socket");
  set_int_opt(fd.get(), SOL_SOCKET, SO_REUSEADDR, 1, "SO_REUSEADDR");
  if (reuseport) set_int_opt(fd.get(), SOL_SOCKET, SO_REUSEPORT, 1, "SO_REUSEPORT");
  sockaddr_in a = make_addr(host, port);
  if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&a), sizeof a) < 0) fail("bind");
  if (::listen(fd.get(), backlog) < 0) fail("listen");
  return fd;
}

FileDescriptor bind_udp(const std::string& host, uint16_t port, bool nonblocking) {
  int type = SOCK_DGRAM | SOCK_CLOEXEC | (nonblocking ? SOCK_NONBLOCK : 0);
  FileDescriptor fd(::socket(AF_INET, type, 0));
  if (!fd) fail("socket(udp)");
  set_int_opt(fd.get(), SOL_SOCKET, SO_REUSEADDR, 1, "SO_REUSEADDR");
  // A bigger receive buffer absorbs bursts between epoll wakeups.
  set_int_opt(fd.get(), SOL_SOCKET, SO_RCVBUF, 4 * 1024 * 1024, "SO_RCVBUF");
  sockaddr_in a = make_addr(host, port);
  if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&a), sizeof a) < 0) fail("bind(udp)");
  return fd;
}

FileDescriptor connect_tcp(const std::string& host, uint16_t port) {
  FileDescriptor fd(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (!fd) fail("socket");
  sockaddr_in a = make_addr(host, port);
  int rc;
  do {
    rc = ::connect(fd.get(), reinterpret_cast<sockaddr*>(&a), sizeof a);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0) fail("connect");
  return fd;
}

uint16_t local_port(int fd) {
  sockaddr_in a{};
  socklen_t len = sizeof a;
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len) < 0) fail("getsockname");
  return ntohs(a.sin_port);
}

std::string addr_to_string(const sockaddr_in& a) {
  char buf[INET_ADDRSTRLEN] = {};
  ::inet_ntop(AF_INET, &a.sin_addr, buf, sizeof buf);
  return std::string(buf) + ":" + std::to_string(ntohs(a.sin_port));
}

std::size_t raise_nofile_limit(std::size_t want) {
  rlimit rl{};
  if (::getrlimit(RLIMIT_NOFILE, &rl) < 0) return 0;
  rlim_t target = static_cast<rlim_t>(want);
  if (rl.rlim_max != RLIM_INFINITY && target > rl.rlim_max) target = rl.rlim_max;
  if (target > rl.rlim_cur) {
    rl.rlim_cur = target;
    if (::setrlimit(RLIMIT_NOFILE, &rl) < 0) ::getrlimit(RLIMIT_NOFILE, &rl);
  }
  return static_cast<std::size_t>(rl.rlim_cur);
}

bool write_all(int fd, const char* data, std::size_t len) {
  while (len > 0) {
    ssize_t n = ::send(fd, data, len, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;  // EPIPE, ECONNRESET, EAGAIN on SO_SNDTIMEO, ...
    }
    data += n;
    len -= static_cast<std::size_t>(n);
  }
  return true;
}

}  // namespace chat::net
