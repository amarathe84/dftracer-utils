#ifndef DFTRACER_UTILS_PIPELINE_BUILDER_H
#define DFTRACER_UTILS_PIPELINE_BUILDER_H

#include <dftracer/utils/pipeline/executors/mpi_executor.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/factory.h>

#include <any>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

namespace dftracer::utils {

template <typename InputType>
class PipelineBuilder {
   public:
    template <typename>
    friend class PipelineBuilder;

    explicit PipelineBuilder(std::any input) : input_data_(std::move(input)) {}
    PipelineBuilder(const PipelineBuilder&) = delete;
    PipelineBuilder& operator=(const PipelineBuilder&) = delete;
    PipelineBuilder(PipelineBuilder&&) = default;
    PipelineBuilder& operator=(PipelineBuilder&&) = default;

    // Filter operation
    template <typename F>
    PipelineBuilder&& filter(F predicate) && {
        tasks_.push_back(Tasks::filter<InputType>(std::move(predicate)));

        // Add dependency if this isn't the first task
        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }

        return std::move(*this);
    }

    // Map operation
    template <typename OutputType, typename F>
    PipelineBuilder<OutputType> map(F func) && {
        PipelineBuilder<OutputType> next_builder(std::move(input_data_));

        next_builder.tasks_ = std::move(tasks_);
        next_builder.dependencies_ = std::move(dependencies_);

        next_builder.tasks_.push_back(
            Tasks::map<InputType, OutputType>(std::move(func)));

        // Add dependency if this isn't the first task
        if (next_builder.tasks_.size() > 1) {
            next_builder.dependencies_.emplace_back(
                next_builder.tasks_.size() - 1,  // current task
                next_builder.tasks_.size() - 2   // previous task
            );
        }

        return next_builder;
    }

    // Reduce operations
    PipelineBuilder&& sum() && {
        static_assert(std::is_arithmetic_v<InputType>,
                      "Sum requires arithmetic type");
        tasks_.push_back(Tasks::sum<InputType>());

        // Add dependency if this isn't the first task
        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1,  // current task
                                       tasks_.size() - 2   // previous task
            );
        }
        return std::move(*this);
    }

    PipelineBuilder&& product() && {
        static_assert(std::is_arithmetic_v<InputType>,
                      "Product requires arithmetic type");
        tasks_.push_back(Tasks::product<InputType>());

        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }
        return std::move(*this);
    }

    PipelineBuilder&& max(
        InputType initial = std::numeric_limits<InputType>::lowest()) && {
        tasks_.push_back(Tasks::max<InputType>(initial));

        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }
        return std::move(*this);
    }

    PipelineBuilder&& min(
        InputType initial = std::numeric_limits<InputType>::max()) && {
        tasks_.push_back(Tasks::min<InputType>(initial));

        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }
        return std::move(*this);
    }

    // Take/Limit operations
    PipelineBuilder&& take(size_t count) && {
        tasks_.push_back(Tasks::take<InputType>(count));

        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }
        return std::move(*this);
    }

    PipelineBuilder&& limit(size_t count) && {
        tasks_.push_back(Tasks::limit<InputType>(count));

        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }
        return std::move(*this);
    }

    // Skip/Drop operations
    PipelineBuilder&& skip(size_t count) && {
        tasks_.push_back(Tasks::skip<InputType>(count));

        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }
        return std::move(*this);
    }

    PipelineBuilder&& drop(size_t count) && {
        tasks_.push_back(Tasks::drop<InputType>(count));

        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }
        return std::move(*this);
    }

    // Distinct operation
    PipelineBuilder&& distinct() && {
        tasks_.push_back(Tasks::distinct<InputType>());

        if (tasks_.size() > 1) {
            dependencies_.emplace_back(tasks_.size() - 1, tasks_.size() - 2);
        }
        return std::move(*this);
    }

    // Execution methods
    std::any execute_sequential() && {
        SequentialExecutor executor;
        return execute_with_executor(executor);
    }

    std::any execute_threaded() && {
        ThreadExecutor executor;
        return execute_with_executor(executor);
    }

    std::any execute_mpi() && {
        MPIExecutor executor;
        return execute_with_executor(executor);
    }

   private:
    std::any execute_with_executor(Executor& executor) {
        Pipeline pipeline;

        // Add all tasks to pipeline
        std::vector<TaskIndex> task_ids;
        for (auto& task : tasks_) {
            task_ids.push_back(pipeline.add_task(std::move(task)));
        }

        // Add dependencies
        for (const auto& [dependent_idx, dependency_idx] : dependencies_) {
            pipeline.add_dependency(task_ids[dependency_idx],
                                    task_ids[dependent_idx]);
        }

        return executor.execute(pipeline, input_data_);
    }

   private:
    std::any input_data_;
    std::vector<std::unique_ptr<Task>> tasks_;
    std::vector<std::pair<size_t, size_t>> dependencies_;
};

// Factory functions
template <typename T>
PipelineBuilder<T> from(const std::vector<T>& data) {
    return PipelineBuilder<T>(std::any(data));
}

template <typename T>
PipelineBuilder<T> from(std::vector<T>&& data) {
    return PipelineBuilder<T>(std::any(std::move(data)));
}

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_BUILDER_H
