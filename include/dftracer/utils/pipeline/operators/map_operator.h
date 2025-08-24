#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_OPERATOR_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_OPERATOR_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <cstddef>
#include <cstdint>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {

// map(std::vector<In> input) -> std::vector<Out> {
//   foreach (x in input) {

//   }
// }


class TaskFactory

/*
Create a tree of factory tasks

- manage execution of tasks
- only have one function "execute"
- goes to top of the tree until the leaves to execute

child class of ThreadPipeline, MPIPipeline, SequentialPipeline
  - it will use commonality from one task to others from pipeline class

Pipeline factory

*/
typedef TaskID ...;
class Pipeline {
private:
  // Tree<Task> tasks_;
  std::vector<Task> tasks_;
  std::vector<std::pair<TaskID, TaskID>> links_;

public:
  TaskID add_task(...) { ... }
  void add_links(TaskID from, TaskID to) { ... }

  void execute() {
    ...
  }

protected:
  virtual void submit(Task& task) = 0;
  virtual void wait();
  virtual void barrier();
  virtual void gather();
};

struct Output {

};

struct Input {

};

enum class TaskType {
  MAP,
  REDUCE,
  FILTER
};

class Task {
private:
  TaskType type_;
protected:
  Task(TaskType t) : type_(t) {}
public:
  virtual Output execute(Input) = 0;
};

template<typename T>
struct MapInput : public Input {
  std::vector<T> data;
};

template<typename T>
struct MapOutput : public Output {
  std::vector<T> data;
};


template<typename Input, typename Output>
class MapTask : public Task {
public:
  MapTask(MapInput<Input> in, MapOutput<Output> out) : Task(TaskType::MAP) {}

  inline MapOutput<Output> execute(MapInput<Input>) {

  }
};

auto pipeline = PipelineFactory::get(MPI);
pipeline.add_task(IOOperator<int, int>::create(

));

template<typename In, typename Out>
class MapOperator : public Operator<In, Out> {
 public:
  using Fn = void (*)(const void* in_elem, void* out_elem);
  using FnWithState = void (*)(const void* in_elem, void* out_elem,
                               void* state);

  std::size_t in_size;
  std::size_t out_size;
  Fn fn = nullptr;
  FnWithState fn_with_state = nullptr;
  void* state = nullptr;

  MapOperator(std::size_t in_sz, std::size_t out_sz, Fn stateless_fn = nullptr,
              const char* op_name = nullptr, std::uint64_t op_id = 0)
      : Operator(Op::MAP, op_name, op_id),
        in_size(in_sz),
        out_size(out_sz),
        fn(stateless_fn),
        fn_with_state(nullptr),
        state(nullptr) {}
};

}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_OPERATOR_H
