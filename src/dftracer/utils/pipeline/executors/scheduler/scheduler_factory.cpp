#include <dftracer/utils/pipeline/executors/scheduler/scheduler_interface.h>
#include <dftracer/utils/pipeline/executors/thread/scheduler.h>

namespace dftracer::utils {

std::unique_ptr<SchedulerInterface> SchedulerFactory::create(Type type, std::size_t num_threads) {
    switch (type) {
        case Type::THREAD_POOL: {
            auto scheduler = std::make_unique<GlobalScheduler>();
            scheduler->initialize(num_threads);
            return scheduler;
        }
        case Type::SEQUENTIAL: {
            // For now, use thread scheduler with 1 thread for sequential
            // Later we can create a dedicated SequentialScheduler
            auto scheduler = std::make_unique<GlobalScheduler>();
            scheduler->initialize(1);
            return scheduler;
        }
        default:
            throw std::invalid_argument("Unknown scheduler type");
    }
}

} // namespace dftracer::utils