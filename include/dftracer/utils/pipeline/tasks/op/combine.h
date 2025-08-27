#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_COMBINE_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_COMBINE_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/task.h>

#include <any>
#include <functional>
#include <string>
#include <vector>

namespace dftracer::utils {

template <typename Result, typename... InputTypes>
class CombineTask : public Task {
   private:
    std::function<Result(InputTypes...)> combiner_;

   public:
    explicit CombineTask(std::function<Result(InputTypes...)> combiner)
        : Task(TaskType::COMBINE), combiner_(std::move(combiner)) {}

    std::any execute(std::any input) override {
        std::vector<std::any> inputs;

        try {
            inputs = std::any_cast<std::vector<std::any>>(input);
        } catch (const std::bad_any_cast&) {
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "CombineTask expects vector<std::any> input");
        }

        if (inputs.size() != sizeof...(InputTypes)) {
            throw PipelineError(
                PipelineError::VALIDATION_ERROR,
                "Wrong number of inputs for CombineTask. Expected: " +
                    std::to_string(sizeof...(InputTypes)) +
                    ", Got: " + std::to_string(inputs.size()));
        }

        return std::any(
            unpack_and_call(inputs, std::index_sequence_for<InputTypes...>{}));
    }

   private:
    template <size_t... Is>
    Result unpack_and_call(const std::vector<std::any>& inputs,
                           std::index_sequence<Is...>) {
        try {
            return combiner_(std::any_cast<InputTypes>(inputs[Is])...);
        } catch (const std::bad_any_cast& e) {
            throw PipelineError(
                PipelineError::TYPE_MISMATCH_ERROR,
                "Type mismatch in CombineTask input unpacking: " +
                    std::string(e.what()));
        }
    }
};

namespace stream_ops {

template <typename F>
struct Combine {
    F func;
    explicit Combine(F f) : func(std::move(f)) {}
};

}  // namespace stream_ops

namespace ops {

template <typename F>
stream_ops::Combine<F> combine(F func) {
    return stream_ops::Combine<F>(std::move(func));
}

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_COMBINE_H
