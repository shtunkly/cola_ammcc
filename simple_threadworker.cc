#include "simple_threadworker.hh"

#include "COLA.hh"
#include "EventData.hh"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

namespace cola {
  void SimpleThreadworker::ThreadJob(FilterEnsemble ensemble) {
    std::vector<std::unique_ptr<EventData>> event_buffer;

    while (true) {
      size_t current = task_counter_.fetch_add(1);
      if (current >= total_tasks_) {
        break;
      }

      auto event = (*(ensemble.generator))();
      for (const auto& converter : ensemble.converters) {
        event = std::move(event) | converter;
      }
      if (event_buffer.size() < write_buffer_size_) {
        event_buffer.emplace_back(std::move(event));
        continue;
      }
      event_buffer.emplace_back(std::move(event));
      for (auto& even : event_buffer) {
        std::move(even) | ensemble.writer;
      }
      event_buffer.clear();
    }
    for (auto& even : event_buffer) {
      std::move(even) | ensemble.writer;
    }
    event_buffer.clear();
  }
  SimpleThreadworker::SimpleThreadworker(std::vector<FilterEnsemble>&& filter_ensembles, size_t total_tasks,
                                         size_t write_buffer_size)
      : total_tasks_(total_tasks), write_buffer_size_(write_buffer_size), task_counter_(0) {
    size_t threads_count = filter_ensembles.size();
    std::cout << "Startinng " << threads_count << " threads" << '\n';
    for (size_t i = 0; i < threads_count; i++) {
      threads_.emplace_back(&SimpleThreadworker::ThreadJob, this, std::move(filter_ensembles[i]));
    }
  }
}  // namespace cola