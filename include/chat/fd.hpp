#pragma once
// RAII owner of a POSIX file descriptor. Move-only: no leaks, no double close.
#include <unistd.h>

#include <utility>

namespace chat {

class FileDescriptor {
 public:
  FileDescriptor() noexcept = default;
  explicit FileDescriptor(int fd) noexcept : fd_(fd) {}
  ~FileDescriptor() { reset(); }

  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;
  FileDescriptor(FileDescriptor&& o) noexcept : fd_(o.release()) {}
  FileDescriptor& operator=(FileDescriptor&& o) noexcept {
    if (this != &o) reset(o.release());
    return *this;
  }

  int get() const noexcept { return fd_; }
  bool valid() const noexcept { return fd_ >= 0; }
  explicit operator bool() const noexcept { return valid(); }

  // Give up ownership without closing.
  int release() noexcept { return std::exchange(fd_, -1); }
  // Close the current fd (if any) and optionally adopt another.
  void reset(int fd = -1) noexcept {
    if (fd_ >= 0) ::close(fd_);
    fd_ = fd;
  }

 private:
  int fd_ = -1;
};

}  // namespace chat
