#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <doctest/doctest.h>

#include <any>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

using namespace dftracer::utils;

TEST_CASE("Pipeline - Basic functionality") {
    Pipeline pipeline;

    auto double_task = [](int input, TaskContext& ctx) -> int {
        return input * 2;
    };

    TaskIndex task_id = pipeline.add_task<int, int>(double_task);
    CHECK(task_id == 0);
}

TEST_CASE("Pipeline - Sequential execution") {
    Pipeline pipeline;

    auto double_task = [](int input, TaskContext& ctx) -> int {
        return input * 2;
    };

    pipeline.add_task<int, int>(double_task);

    SequentialExecutor executor;
    std::any result = executor.execute(pipeline, 21);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 42);
}

TEST_CASE("Pipeline - Thread execution") {
    Pipeline pipeline;

    auto double_task = [](int input, TaskContext& ctx) -> int {
        return input * 2;
    };

    pipeline.add_task<int, int>(double_task);

    ThreadExecutor executor(2);
    std::any result = executor.execute(pipeline, 21);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 42);
}

TEST_CASE("Pipeline - Task dependencies") {
    Pipeline pipeline;

    auto add_task = [](int input, TaskContext& ctx) -> int {
        return input + 10;
    };

    auto multiply_task = [](int input, TaskContext& ctx) -> int {
        return input * 2;
    };

    TaskIndex t1 = pipeline.add_task<int, int>(add_task);
    TaskIndex t2 = pipeline.add_task<int, int>(multiply_task);
    pipeline.add_dependency(t1, t2);

    SequentialExecutor executor;
    std::any result = executor.execute(pipeline, 5);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 30);
}

TEST_CASE("Pipeline - Task emission") {
    Pipeline pipeline;

    auto emitting_task = [](int input, TaskContext& ctx) -> int {
        auto child_task = [](int x, TaskContext& ctx2) -> int { return x * 3; };

        ctx.emit<int, int>(child_task, Input{input * 2},
                           DependsOn{ctx.current()});
        return input + 5;
    };

    pipeline.add_task<int, int>(emitting_task);

    SequentialExecutor executor;
    std::any result = executor.execute(pipeline, 10);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 15);
}

TEST_CASE("Pipeline - String processing") {
    Pipeline pipeline;

    auto string_task = [](std::string input, TaskContext& ctx) -> std::string {
        return "Processed: " + input;
    };

    pipeline.add_task<std::string, std::string>(string_task);

    SequentialExecutor executor;
    std::string input = "test";
    std::any result = executor.execute(pipeline, input);
    std::string final_result = std::any_cast<std::string>(result);

    CHECK(final_result == "Processed: test");
}

TEST_CASE("Pipeline - Vector processing") {
    Pipeline pipeline;

    auto vector_task = [](std::vector<int> input, TaskContext& ctx) -> int {
        int sum = 0;
        for (const auto& elem : input) {
            sum += elem;
        }
        return sum;
    };

    pipeline.add_task<std::vector<int>, int>(vector_task);

    SequentialExecutor executor;
    std::vector<int> input = {1, 2, 3, 4, 5};
    std::any result = executor.execute(pipeline, input);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 15);
}

TEST_CASE("Pipeline - Deterministic execution") {
    Pipeline pipeline;

    std::atomic<int> counter{0};

    auto deterministic_task = [&counter](int input, TaskContext& ctx) -> int {
        for (int i = 0; i < 5; ++i) {
            auto work_task = [&counter, i, input](int work_amount,
                                                  TaskContext& ctx2) -> int {
                counter++;
                int result = input;
                for (int j = 0; j < work_amount * 10; ++j) {
                    result = (result * 3 + 7) % 1000;
                }
                return result + i;
            };

            ctx.emit<int, int>(work_task, Input{i + 1});
        }
        return input * 2;
    };

    pipeline.add_task<int, int>(deterministic_task);

    SequentialExecutor seq_executor;
    counter = 0;
    std::any seq_result = seq_executor.execute(pipeline, 42);
    int seq_final = std::any_cast<int>(seq_result);
    int seq_counter = counter.load();

    ThreadExecutor thread_executor(2);
    counter = 0;
    std::any thread_result = thread_executor.execute(pipeline, 42);
    int thread_final = std::any_cast<int>(thread_result);
    int thread_counter = counter.load();

    CHECK(seq_final == thread_final);
    CHECK(seq_counter == thread_counter);
}

TEST_CASE("Pipeline - Multiple task chains") {
    Pipeline pipeline;

    auto task1 = [](int input, TaskContext&) -> int { return input + 1; };

    auto task2 = [](int input, TaskContext&) -> int { return input * 2; };

    auto task3 = [](int input, TaskContext&) -> int { return input - 5; };

    TaskIndex t1 = pipeline.add_task<int, int>(task1);
    TaskIndex t2 = pipeline.add_task<int, int>(task2);
    TaskIndex t3 = pipeline.add_task<int, int>(task3);

    pipeline.add_dependency(t1, t2);
    pipeline.add_dependency(t2, t3);

    SequentialExecutor executor;
    std::any result = executor.execute(pipeline, 10);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 17);
}

TEST_CASE("Pipeline - Complex task emission") {
    Pipeline pipeline;

    auto generator = [](std::vector<int> input, TaskContext& ctx) -> int {
        int sum = 0;
        for (size_t i = 0; i < input.size(); ++i) {
            auto element_processor = [i](int element,
                                         TaskContext& ctx2) -> int {
                return element * element;
            };

            ctx.emit<int, int>(element_processor, Input{input[i]},
                               DependsOn{ctx.current()});
            sum += input[i];
        }
        return sum;
    };

    pipeline.add_task<std::vector<int>, int>(generator);

    SequentialExecutor executor;
    std::vector<int> input = {2, 3, 4};
    std::any result = executor.execute(pipeline, input);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 9);
}

TEST_CASE("Pipeline - Thread safety") {
    Pipeline pipeline;

    std::atomic<int> shared_counter{0};

    auto thread_safe_task = [&shared_counter](int input,
                                              TaskContext& ctx) -> int {
        for (int i = 0; i < 10; ++i) {
            auto atomic_task = [&shared_counter, i](int x,
                                                    TaskContext& ctx2) -> int {
                shared_counter++;
                return x + i;
            };

            ctx.emit<int, int>(atomic_task, Input{input});
        }
        return input;
    };

    pipeline.add_task<int, int>(thread_safe_task);

    ThreadExecutor executor(4);
    std::any result = executor.execute(pipeline, 5);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 5);
    CHECK(shared_counter.load() == 10);
}

TEST_CASE("Pipeline - Error handling") {
    Pipeline pipeline;

    auto error_task = [](int input, TaskContext& ctx) -> int {
        if (input < 0) {
            return -1;
        }
        return input * 2;
    };

    pipeline.add_task<int, int>(error_task);

    SequentialExecutor executor;

    SUBCASE("Valid input") {
        std::any result = executor.execute(pipeline, 5);
        int final_result = std::any_cast<int>(result);
        CHECK(final_result == 10);
    }

    SUBCASE("Invalid input") {
        std::any result = executor.execute(pipeline, -5);
        int final_result = std::any_cast<int>(result);
        CHECK(final_result == -1);
    }
}

TEST_CASE("Pipeline - Empty pipeline") {
    Pipeline pipeline;
    SequentialExecutor executor;

    CHECK_THROWS(executor.execute(pipeline, 42));
}

TEST_CASE("Pipeline - Different executor thread counts") {
    Pipeline pipeline;

    auto simple_task = [](int input, TaskContext& ctx) -> int {
        return input * 3;
    };

    pipeline.add_task<int, int>(simple_task);

    std::vector<int> thread_counts = {1, 2, 4, 8};

    for (int thread_count : thread_counts) {
        ThreadExecutor executor(thread_count);
        std::any result = executor.execute(pipeline, 7);
        int final_result = std::any_cast<int>(result);
        CHECK(final_result == 21);
    }
}

TEST_CASE("Pipeline - Cyclic dependency detection") {
    Pipeline pipeline;

    auto task1 = [](int input, TaskContext& ctx) -> int { return input + 1; };

    auto task2 = [](int input, TaskContext& ctx) -> int { return input * 2; };

    TaskIndex t1 = pipeline.add_task<int, int>(task1);
    TaskIndex t2 = pipeline.add_task<int, int>(task2);

    pipeline.add_dependency(t1, t2);
    pipeline.add_dependency(t2, t1);

    SequentialExecutor executor;
    CHECK_THROWS_AS(executor.execute(pipeline, 5), PipelineError);
}

TEST_CASE("Pipeline - Type mismatch validation") {
    Pipeline pipeline;

    auto string_task = [](int input, TaskContext& ctx) -> std::string {
        return std::to_string(input);
    };

    auto int_task = [](int input, TaskContext& ctx) -> int {
        return input * 2;
    };

    TaskIndex t1 = pipeline.add_task<int, std::string>(string_task);
    TaskIndex t2 = pipeline.add_task<int, int>(int_task);

    pipeline.add_dependency(t1, t2);

    SequentialExecutor executor;
    CHECK_THROWS_AS(executor.execute(pipeline, 5), PipelineError);
}

TEST_CASE("Pipeline - Multiple dependencies") {
    Pipeline pipeline;

    auto task1 = [](int input, TaskContext& ctx) -> int { return input + 10; };

    auto task2 = [](int input, TaskContext& ctx) -> int { return input * 2; };

    auto combiner_task = [](std::vector<std::any> inputs,
                            TaskContext& ctx) -> int {
        int sum = 0;
        for (const auto& input : inputs) {
            sum += std::any_cast<int>(input);
        }
        return sum;
    };

    TaskIndex t1 = pipeline.add_task<int, int>(task1);
    TaskIndex t2 = pipeline.add_task<int, int>(task2);
    TaskIndex t3 = pipeline.add_task<std::vector<std::any>, int>(combiner_task);

    pipeline.add_dependency(t1, t3);
    pipeline.add_dependency(t2, t3);

    SequentialExecutor executor;
    std::any result = executor.execute(pipeline, 5);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 25);
}

TEST_CASE("Pipeline - Multiple dependencies type mismatch") {
    Pipeline pipeline;

    auto task1 = [](int input, TaskContext& ctx) -> int { return input + 10; };

    auto task2 = [](int input, TaskContext& ctx) -> int { return input * 2; };

    auto bad_combiner = [](int input, TaskContext& ctx) -> int {
        return input;
    };

    TaskIndex t1 = pipeline.add_task<int, int>(task1);
    TaskIndex t2 = pipeline.add_task<int, int>(task2);
    TaskIndex t3 = pipeline.add_task<int, int>(bad_combiner);

    pipeline.add_dependency(t1, t3);
    pipeline.add_dependency(t2, t3);

    SequentialExecutor executor;
    CHECK_THROWS_AS(executor.execute(pipeline, 5), PipelineError);
}

TEST_CASE("Pipeline - Complex dependency graph") {
    Pipeline pipeline;

    auto add_task = [](int input, TaskContext& ctx) -> int {
        return input + 1;
    };

    auto multiply_task = [](int input, TaskContext& ctx) -> int {
        return input * 2;
    };

    auto combiner_task = [](std::vector<std::any> inputs,
                            TaskContext& ctx) -> int {
        int product = 1;
        for (const auto& input : inputs) {
            product *= std::any_cast<int>(input);
        }
        return product;
    };

    TaskIndex t1 = pipeline.add_task<int, int>(add_task);
    TaskIndex t2 = pipeline.add_task<int, int>(multiply_task);
    TaskIndex t3 = pipeline.add_task<int, int>(add_task);
    TaskIndex t4 = pipeline.add_task<int, int>(multiply_task);
    TaskIndex t5 = pipeline.add_task<std::vector<std::any>, int>(combiner_task);

    pipeline.add_dependency(t1, t2);
    pipeline.add_dependency(t1, t3);
    pipeline.add_dependency(t2, t5);
    pipeline.add_dependency(t3, t4);
    pipeline.add_dependency(t4, t5);

    SequentialExecutor executor;
    std::any result = executor.execute(pipeline, 2);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 48);
}

TEST_CASE("Pipeline - Task context usage") {
    Pipeline pipeline;

    std::vector<TaskIndex> emitted_tasks;

    auto context_task = [&emitted_tasks](int input, TaskContext& ctx) -> int {
        auto child_task = [input](int multiplier, TaskContext& ctx2) -> int {
            return input * multiplier;
        };

        TaskIndex child_id = ctx.emit<int, int>(child_task, Input{3});
        emitted_tasks.push_back(child_id);

        return input + 5;
    };

    pipeline.add_task<int, int>(context_task);

    SequentialExecutor executor;
    std::any result = executor.execute(pipeline, 10);
    int final_result = std::any_cast<int>(result);

    CHECK(final_result == 15);
    CHECK(emitted_tasks.size() == 1);
}

TEST_CASE("Pipeline - Empty pipeline validation") {
    Pipeline empty_pipeline;

    SequentialExecutor seq_executor;
    ThreadExecutor thread_executor(2);

    CHECK_THROWS_AS(seq_executor.execute(empty_pipeline, 42), PipelineError);
    CHECK_THROWS_AS(thread_executor.execute(empty_pipeline, 42), PipelineError);
}

TEST_CASE("Pipeline - Large pipeline stress test") {
    Pipeline pipeline;

    std::vector<TaskIndex> tasks;

    for (int i = 0; i < 100; ++i) {
        auto task = [i](int input, TaskContext& ctx) -> int {
            return input + i;
        };

        TaskIndex task_id = pipeline.add_task<int, int>(task);
        tasks.push_back(task_id);

        if (i > 0) {
            pipeline.add_dependency(tasks[i - 1], tasks[i]);
        }
    }

    SequentialExecutor executor;
    std::any result = executor.execute(pipeline, 0);
    int final_result = std::any_cast<int>(result);

    int expected = 0;
    for (int i = 0; i < 100; ++i) {
        expected += i;
    }

    CHECK(final_result == expected);
}
