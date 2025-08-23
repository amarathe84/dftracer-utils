#ifndef __DFTRACER_UTILS_UTILS_RESULT_H
#define __DFTRACER_UTILS_UTILS_RESULT_H

#include <functional>
#include <stdexcept>
#include <type_traits>
#include <variant>

template <typename T, typename E>
class Result {
 private:
  std::variant<T, E> data;

 public:
  Result(const T& value) : data(value) {}
  Result(T&& value) : data(std::move(value)) {}
  Result(const E& error) : data(error) {}
  Result(E&& error) : data(std::move(error)) {}

  static Result Ok(const T& value) { return Result(value); }
  static Result Ok(T&& value) { return Result(std::move(value)); }
  static Result Err(const E& error) { return Result(error); }
  static Result Err(E&& error) { return Result(std::move(error)); }

  bool is_ok() const { return std::holds_alternative<T>(data); }
  bool is_err() const { return std::holds_alternative<E>(data); }

  const T& value() const {
    if (is_err()) {
      throw std::runtime_error("Attempted to get value from error Result");
    }
    return std::get<T>(data);
  }

  T& value() {
    if (is_err()) {
      throw std::runtime_error("Attempted to get value from error Result");
    }
    return std::get<T>(data);
  }

  const E& error() const {
    if (is_ok()) {
      throw std::runtime_error("Attempted to get error from ok Result");
    }
    return std::get<E>(data);
  }

  E& error() {
    if (is_ok()) {
      throw std::runtime_error("Attempted to get error from ok Result");
    }
    return std::get<E>(data);
  }

  T value_or(const T& default_value) const {
    return is_ok() ? value() : default_value;
  }

  template <typename F>
  auto map(F&& func) -> Result<std::invoke_result_t<F, T>, E> {
    if (is_ok()) {
      return Result<std::invoke_result_t<F, T>, E>::Ok(func(value()));
    }
    return Result<std::invoke_result_t<F, T>, E>::Err(error());
  }

  template <typename F>
  auto and_then(F&& func) -> std::invoke_result_t<F, T> {
    if (is_ok()) {
      return func(value());
    }
    return std::invoke_result_t<F, T>::Err(error());
  }

  template <typename F>
  Result<T, E> or_else(F&& func) {
    if (is_err()) {
      return func(error());
    }
    return *this;
  }

  template <typename OkFunc, typename ErrFunc>
  auto match(OkFunc&& ok_func, ErrFunc&& err_func) {
    if (is_ok()) {
      return ok_func(value());
    } else {
      return err_func(error());
    }
  }

  explicit operator bool() const { return is_ok(); }

  bool operator==(const Result& other) const { return data == other.data; }

  bool operator!=(const Result& other) const { return !(*this == other); }
};

#endif  // __DFTRACER_UTILS_UTILS_RESULT_H
