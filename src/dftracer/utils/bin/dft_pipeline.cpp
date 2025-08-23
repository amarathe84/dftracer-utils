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

  {
    std::cout << "=== FlatMap example ===\n";

    auto fm0 = Collection<int>::from_sequence(input);
    auto fm1 = fm0.flatmap<int>(
        [](int x) {
          if (x % 2 == 0) return std::vector<int>{x, x * 10};
          return std::vector<int>{};
        },
        ctx);
    std::cout << "Eager/Collection flatmap result: ";
    for (auto v : fm1.data()) std::cout << v << ' ';
    std::cout << "\n";

    auto lfm0 = LazyCollection<int>::from_sequence(input);
    auto lfm1 = lfm0.flatmap<int>([](int x, auto emit) {
      if (x % 2 == 1) emit(x);
      emit(x * 100);
    });
    auto lfmres = lfm1.collect_local(ctx);
    std::cout << "Lazy/LazyCollection flatmap result: ";
    for (auto v : lfmres) std::cout << v << ' ';
    std::cout << "\n";
    std::cout << "\n";

    std::cout << "Collection flatmap (vector-of-vector): ";

    // FlatMap from vector-of-vectors
    std::vector<std::vector<int>> nested{{1, 2}, {3, 4, 5}, {}, {6}};
    auto fmv0 = Collection<std::vector<int>>::from_sequence(nested);
    auto fmv1 = fmv0.flatmap<int>(
        [](const std::vector<int>& xs) {
          return xs;  // returning vector<int> flattens
        },
        ctx);
    std::cout << "Eager/Collection flatmap (vector-of-vector) result: ";
    for (auto v : fmv1.data()) std::cout << v << ' ';
    std::cout << "\n";

    auto lfmv0 = LazyCollection<std::vector<int>>::from_sequence(nested);
    auto lfmv1 =
        lfmv0.flatmap<int>([](const std::vector<int>& xs) { return xs; });
    auto lfmvres = lfmv1.collect_local(ctx);
    std::cout << "Lazy/LazyCollection flatmap (vector-of-vector) result: ";
    for (auto v : lfmvres) std::cout << v << ' ';
    std::cout << "\n";
    std::cout << "\n";
  }
  return 0;
}
