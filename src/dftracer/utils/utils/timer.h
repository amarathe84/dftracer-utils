#ifndef DFTRACER_UTILS_UTILS_TIMER_H
#define DFTRACER_UTILS_UTILS_TIMER_H

#include <chrono>
#include <string>
#include <unordered_map>

namespace dftracer::utils {

class Timer {
   public:
    Timer(bool autostart = false, bool verbose = false);
    Timer(const std::string& name, bool autostart = false,
          bool verbose = false);
    ~Timer();
    void start();
    void stop();
    double elapsed() const;

   private:
    bool verbose_ = false;
    bool running_ = false;
    std::string name_;
    using Clock = std::chrono::high_resolution_clock;
    Clock::time_point start_time;
    Clock::time_point end_time;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_UTILS_TIMER_H
