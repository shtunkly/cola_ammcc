#ifndef simple_threadworker_HH
#define simple_threadworker_HH

#include "COLA.hh"

#include <atomic>
#include <thread>
#include <vector>

namespace cola {
  class SimpleThreadworker {
    std::atomic<size_t> task_counter_;
    size_t total_tasks_;
    size_t write_buffer_size_;
    std::vector<std::jthread> threads_;
    void ThreadJob(FilterEnsemble ensemble);

   public:
    SimpleThreadworker(std::vector<FilterEnsemble>&& filter_ensembles, size_t total_tasks,
                       size_t write_buffer_size = 100);
  };
}  // namespace cola

#endif