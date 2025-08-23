#include <dftracer/utils/pipeline/collection.h>
#include <dftracer/utils/pipeline/execution_context/sequential.h>
#include <dftracer/utils/pipeline/lazy_collection.h>

#include <iostream>
#include <vector>

using namespace dftracer::utils::pipeline;

int main(int argc, char** argv) {
  (void)argc;
  (void)argv;

  // Simple data
  std::vector<int> input{1, 2, 3, 4, 5};

  // Execution context (sequential for now)
  context::SequentialContext ctx;

  // --- Eager path: Collection<T> ---
  auto c0 = Collection<int>::from_sequence(input);
  Collection<int> c1 =
      c0.map([](const int& x) { return x * x; }, ctx);  // square

  std::cout << "Eager/Collection result: ";
  for (auto v : c1.data()) std::cout << v << ' ';
  std::cout << "\n";

  // --- Lazy path: LazyCollection<T> ---
  auto l0 = LazyCollection<int>::from_vector(input);
  auto l1 =
      l0.map<int>([](const int& in, int& out) { out = in * 2; });  // times 2
  auto lres = l1.collect_local(ctx);

  std::cout << "Lazy/LazyCollection result: ";
  for (auto v : lres) std::cout << v << ' ';
  std::cout << "\n";

  return 0;
}
