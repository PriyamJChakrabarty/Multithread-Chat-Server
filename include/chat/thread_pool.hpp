#pragma once
// Fixed-size worker pool: a mutex-protected FIFO task queue plus a condition
// variable. stop() lets workers drain what is already queued, then joins.
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace chat {

class ThreadPool {
 public:
  using Task = std::function<void()>;

  explicit ThreadPool(std::size_t threads);
  ~ThreadPool() { stop(); }
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  // Returns false (and drops the task) once stop() has been called.
  bool submit(Task task);
  // Idempotent. Waits for queued tasks to finish, then joins the workers.
  void stop();

  std::size_t pending() const;
  std::size_t size() const { return workers_.size(); }

 private:
  void worker_loop();

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Task> queue_;
  bool stopping_ = false;
  std::vector<std::thread> workers_;
};

}  // namespace chat
