#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/utils/timer.h>

#include <cstdio>

namespace dftracer::utils {

Timer::Timer(bool autostart, bool verbose)
    : verbose_(verbose), running_(false) {
    if (autostart) {
        start();
    }
}

Timer::Timer(const std::string& name, bool autostart, bool verbose)
    : verbose_(verbose), running_(false), name_(name) {
    if (autostart) {
        start();
    }
}

Timer::~Timer() {
    stop();
    if (verbose_) {
        if (name_.empty()) {
          printf("Elapsed time: %.3f ms\n", elapsed());
            // DFTRACER_UTILS_LOG_INFO("Elapsed time: %.3f ms", elapsed());
            // DFTRACER_UTILS_LOG_DEBUG("Elapsed time: %.3f ms", elapsed());
        } else {
          printf("[%s] Elapsed time: %.3f ms\n", name_.c_str(), elapsed());
            // DFTRACER_UTILS_LOG_INFO("[%s] Elapsed time: %.3f ms", name_.c_str(),
            //                         elapsed());
            // DFTRACER_UTILS_LOG_DEBUG("[%s] Elapsed time: %.3f ms",
            //                         name_.c_str(), elapsed());
        }
    }
}

void Timer::start() {
    start_time = Clock::now();
    running_ = true;
}

void Timer::stop() {
    if (running_) {
        end_time = Clock::now();
        running_ = false;
    }
}

double Timer::elapsed() const {
    if (running_) {
        return std::chrono::duration<double, std::milli>(Clock::now() -
                                                         start_time)
            .count();
    } else {
        return std::chrono::duration<double, std::milli>(end_time - start_time)
            .count();
    }
}

}  // namespace dftracer::utils
