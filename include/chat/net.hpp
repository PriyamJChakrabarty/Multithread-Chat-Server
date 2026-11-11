#pragma once
// Thin helpers over the BSD socket API (IPv4 only).
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "chat/fd.hpp"

namespace chat::net {

std::string errno_str(int err);

// All of these throw std::system_error on failure.
void set_nonblocking(int fd);
void set_tcp_nodelay(int fd);
void set_keepalive(int fd);

// Bound + listening TCP socket (SO_REUSEADDR always, SO_REUSEPORT optional).
FileDescriptor listen_tcp(const std::string& host, uint16_t port, int backlog,
                          bool nonblocking, bool reuseport = false);
// Bound UDP socket.
FileDescriptor bind_udp(const std::string& host, uint16_t port, bool nonblocking);
// Blocking TCP connect (used by the client and tests).
FileDescriptor connect_tcp(const std::string& host, uint16_t port);

uint16_t local_port(int fd);
std::string addr_to_string(const sockaddr_in& a);

// Raise RLIMIT_NOFILE soft limit toward `want` (capped by the hard limit).
// Returns the resulting soft limit.
std::size_t raise_nofile_limit(std::size_t want);

// Blocking send of the whole buffer; retries on EINTR/partial writes and uses
// MSG_NOSIGNAL so a dead peer yields EPIPE instead of SIGPIPE.
bool write_all(int fd, const char* data, std::size_t len);

}  // namespace chat::net
