#include "chat/logger.hpp"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>

namespace chat {

Logger& Logger::instance() {
  static Logger inst;
  return inst;
}

bool Logger::parse_level(std::string_view s, LogLevel& out) {
  if (s == "debug") out = LogLevel::Debug;
  else if (s == "info") out = LogLevel::Info;
  else if (s == "warn") out = LogLevel::Warn;
  else if (s == "error") out = LogLevel::Error;
  else if (s == "off") out = LogLevel::Off;
  else return false;
  return true;
}

void Logger::log(LogLevel l, std::string_view msg) {
  static std::mutex mu;
  static const char* names[] = {"DEBUG", "INFO ", "WARN ", "ERROR"};
  using namespace std::chrono;
  auto now = system_clock::now();
  std::time_t t = system_clock::to_time_t(now);
  int ms = static_cast<int>(duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000);
  std::tm tm{};
  localtime_r(&t, &tm);
  char ts[32];
  std::strftime(ts, sizeof ts, "%H:%M:%S", &tm);

  std::lock_guard<std::mutex> lk(mu);
  std::fprintf(stderr, "%s.%03d [%s] %.*s\n", ts, ms, names[static_cast<int>(l)],
               static_cast<int>(msg.size()), msg.data());
}

}  // namespace chat
