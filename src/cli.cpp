#include "chat/cli.hpp"

#include <cstdlib>

namespace chat {

Args::Args(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--", 0) == 0) {
      std::string key = a.substr(2);
      auto eq = key.find('=');
      if (eq != std::string::npos) {
        opts_[key.substr(0, eq)] = key.substr(eq + 1);
      } else if (i + 1 < argc && std::string(argv[i + 1]).rfind("--", 0) != 0) {
        opts_[key] = argv[++i];
      } else {
        opts_[key] = "1";
      }
    } else {
      pos_.push_back(a);
    }
  }
}

std::string Args::get(const std::string& key, const std::string& def) const {
  auto it = opts_.find(key);
  return it == opts_.end() ? def : it->second;
}

long Args::get_int(const std::string& key, long def) const {
  auto it = opts_.find(key);
  if (it == opts_.end()) return def;
  char* end = nullptr;
  long v = std::strtol(it->second.c_str(), &end, 10);
  return (end && *end == '\0') ? v : def;
}

}  // namespace chat
