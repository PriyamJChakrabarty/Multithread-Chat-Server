#pragma once
// Tiny `--key value` / `--flag` argument parser shared by the executables.
#include <map>
#include <string>
#include <vector>

namespace chat {

class Args {
 public:
  Args(int argc, char** argv);
  bool has(const std::string& key) const { return opts_.count(key) > 0; }
  std::string get(const std::string& key, const std::string& def = "") const;
  long get_int(const std::string& key, long def) const;
  const std::vector<std::string>& positional() const { return pos_; }

 private:
  std::map<std::string, std::string> opts_;
  std::vector<std::string> pos_;
};

}  // namespace chat
