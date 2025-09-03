#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/executors/executor_factory.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

using namespace dftracer::utils;

int main() {
    DFTRACER_UTILS_LOGGER_INIT();
    DFTRACER_UTILS_LOG_INFO("=== Task Emission Race Condition Solutions ===");

    Pipeline pipeline;

    // ❌ UNSAFE: Race condition potential
    auto unsafe_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("UNSAFE: Processing input %d", input);

        auto subtask = [](int x, TaskContext& ctx2) -> int {
            // Simulate some work
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return x * 2;
        };

        // ❌ RACE CONDITION: subtask might start before add_dependency is
        // called!
        TaskIndex emitted = ctx.emit<int, int>(subtask, Input{input});
        ctx.add_dependency(ctx.current(), emitted);  // Too late!

        return input + 10;
    };

    // ✅ SAFE SOLUTION 1: Use current() helper (most convenient)
    auto safe_current_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("SAFE1: Processing input %d", input);

        auto subtask = [](int x, TaskContext& ctx2) -> int {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return x * 3;
        };

        // ✅ ATOMIC: dependency set immediately using current() helper
        TaskIndex dependent =
            ctx.emit<int, int>(subtask, Input{input}, DependsOn{ctx.current()});
        DFTRACER_UTILS_LOG_INFO("SAFE1: Emitted dependent task %d", dependent);

        return input + 20;
    };

    // ✅ SAFE SOLUTION 2: Pass dependency parameter explicitly
    auto safe_param_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("SAFE2: Processing input %d", input);

        auto subtask = [](int x, TaskContext& ctx2) -> int {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return x * 4;
        };

        // ✅ ATOMIC: dependency passed directly to emit
        TaskIndex dependent =
            ctx.emit<int, int>(subtask, Input{input}, DependsOn{ctx.current()});
        DFTRACER_UTILS_LOG_INFO("SAFE2: Emitted dependent task %d", dependent);

        return input + 30;
    };

    // ✅ SAFE SOLUTION 3: Independent tasks (no dependency needed)
    auto independent_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("INDEPENDENT: Processing input %d", input);

        auto parallel_task = [](int x, TaskContext& ctx2) -> int {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return x * 5;
        };

        // ✅ SAFE: No dependency, can run in parallel
        TaskIndex parallel = ctx.emit<int, int>(parallel_task, Input{input});
        DFTRACER_UTILS_LOG_INFO("INDEPENDENT: Emitted parallel task %d",
                                parallel);

        return input + 40;
    };

    // ✅ SAFE SOLUTION 4: Multiple tasks with complex dependencies
    auto complex_deps_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("COMPLEX: Processing input %d", input);

        // Create multiple interdependent tasks
        auto task_a = [](int x, TaskContext& ctx2) -> int {
            DFTRACER_UTILS_LOG_INFO("Task A processing %d", x);
            return x + 100;
        };

        auto task_b = [](int x, TaskContext& ctx2) -> int {
            DFTRACER_UTILS_LOG_INFO("Task B processing %d", x);
            return x + 200;
        };

        auto task_c = [](int x, TaskContext& ctx2) -> int {
            DFTRACER_UTILS_LOG_INFO("Task C processing %d", x);
            return x + 300;
        };

        // ✅ SAFE: All dependencies set atomically
        TaskIndex a_id = ctx.emit<int, int>(
            task_a, Input{input},
            DependsOn{ctx.current()});  // A depends on current
        TaskIndex b_id = ctx.emit<int, int>(task_b, Input{input},
                                            DependsOn{a_id});  // B depends on A
        TaskIndex c_id = ctx.emit<int, int>(task_c, Input{input},
                                            DependsOn{b_id});  // C depends on B

        DFTRACER_UTILS_LOG_INFO("COMPLEX: Created chain %d -> %d -> %d -> %d",
                                ctx.current(), a_id, b_id, c_id);

        return input + 50;
    };

    // Add tasks to pipeline
    TaskIndex unsafe_id = pipeline.add_task<int, int>(unsafe_task);
    TaskIndex safe1_id = pipeline.add_task<int, int>(safe_current_task);
    TaskIndex safe2_id = pipeline.add_task<int, int>(safe_param_task);
    TaskIndex indep_id = pipeline.add_task<int, int>(independent_task);
    TaskIndex complex_id = pipeline.add_task<int, int>(complex_deps_task);

    // Set up pipeline dependencies
    pipeline.add_dependency(unsafe_id, safe1_id);
    pipeline.add_dependency(safe1_id, safe2_id);
    pipeline.add_dependency(safe2_id, indep_id);
    pipeline.add_dependency(indep_id, complex_id);

    // Execute the pipeline using factory (hides implementation details)
    try {
        auto executor = ExecutorFactory::create_sequential();

        DFTRACER_UTILS_LOG_INFO("=== Executing pipeline with input 42 ===");
        std::any result = executor->execute(pipeline, 42);
        int final_result = std::any_cast<int>(result);
        DFTRACER_UTILS_LOG_INFO("Final result: %d", final_result);

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Pipeline execution failed: %s", e.what());
        return 1;
    }

    DFTRACER_UTILS_LOG_INFO("=== Race condition examples completed ===");

    // Summary of approaches:
    std::cout << "\n=== SUMMARY OF SAFE APPROACHES ===\n";
    std::cout << "1. emit(func, input, ctx.current()) - Most convenient\n";
    std::cout << "2. emit(func, input, dependency_id) - Explicit control\n";
    std::cout << "3. emit(func, input, -1) - Independent parallel tasks\n";
    std::cout << "4. Chain dependencies with previous task IDs\n";
    std::cout << "\n❌ NEVER DO: emit() + add_dependency() - RACE CONDITION!\n";
    std::cout
        << "❌ ALSO NEVER: separate add_task() + add_dependency() calls\n";
    std::cout << "✅ ALWAYS USE: atomic dependency parameter in emit()\n";

    return 0;
}
