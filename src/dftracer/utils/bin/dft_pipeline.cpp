#include <dftracer/utils/pipeline/collection.h>
#include <dftracer/utils/pipeline/execution_context/sequential.h>
#include <dftracer/utils/pipeline/lazy_collection.h>

#include <iostream>
#include <vector>

using namespace dftracer::utils::pipeline;

int main(int argc, char** argv) {
  (void)argc;
  (void)argv;

  std::vector<int> input{1, 2, 3, 4, 5};

  context::SequentialContext ctx;
  {
    std::cout << "=== Map example ===\n";
    auto c0 = Collection<int>::from_sequence(input);
    Collection<int> c1 =
        c0.map([](const int& x) { return x * x; }, ctx);  // square

    std::cout << "Eager/Collection result: ";
    for (auto v : c1.data()) std::cout << v << ' ';
    std::cout << "\n";

    auto l0 = LazyCollection<int>::from_sequence(input);
    auto l1 =
        l0.map<int>([](const int& in, int& out) { out = in * 2; });  // times 2
    auto lres = l1.collect_local(ctx);

    std::cout << "Lazy/LazyCollection result: ";
    for (auto v : lres) std::cout << v << ' ';
    std::cout << "\n";
    std::cout << "\n";
  }

  {
    std::cout << "=== Filter example ===\n";

    auto f0 = Collection<int>::from_sequence(input);
    auto f1 = f0.filter([](int x) { return x % 2 == 1; }, ctx);
    std::cout << "Eager/Collection filter result: ";
    for (auto v : f1.data()) std::cout << v << ' ';
    std::cout << "\n";

    auto lf0 = LazyCollection<int>::from_sequence(input);
    auto lf1 = lf0.filter([](int x) { return x % 2 == 0; });
    auto lfres = lf1.collect_local(ctx);
    std::cout << "Lazy/LazyCollection filter result: ";
    for (auto v : lfres) std::cout << v << ' ';
    std::cout << "\n";
    std::cout << "\n";
  }

  return 0;
}
