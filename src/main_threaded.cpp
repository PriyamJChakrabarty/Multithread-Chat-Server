// chat_server_threaded: thread-per-client baseline for benchmarking.
#include <signal.h>

#include <iostream>
#include <thread>

#include "chat/cli.hpp"
#include "chat/logger.hpp"
#include "chat/threaded_server.hpp"

namespace {
chat::ThreadedServer* g_server = nullptr;
// Handler only sets an atomic flag and shutdown()s the listener: both are
// async-signal-safe.
void on_signal(int) {
  if (g_server) g_server->request_stop();
}
}  // namespace

int main(int argc, char** argv) {
  chat::Args a(argc, argv);
  chat::LogLevel lvl;
  if (chat::Logger::parse_level(a.get("log-level", "info"), lvl)) chat::Logger::instance().set_level(lvl);

  chat::ServerConfig cfg;
  cfg.port = static_cast<std::uint16_t>(a.get_int("port", 9000));
  cfg.max_connections = static_cast<std::size_t>(a.get_int("max-conns", 20000));
  cfg.idle_timeout_s = static_cast<unsigned>(a.get_int("idle", 300));
  cfg.max_frame = static_cast<std::size_t>(a.get_int("max-frame", 64 * 1024));

  signal(SIGPIPE, SIG_IGN);
  try {
    chat::ThreadedServer server(cfg);
    g_server = &server;
    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    server.start();
    server.run();
    g_server = nullptr;
  } catch (const std::exception& e) {
    LOG_ERROR("fatal: " << e.what());
    return 1;
  }
  return 0;
}
