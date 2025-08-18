#ifndef __DFTRACER_UTILS_UTILS_DFT_EVENTS_H
#define __DFTRACER_UTILS_UTILS_DFT_EVENTS_H

#include <unordered_map>
#include <any>
#include <string>
#include <vector>
#include <iosfwd>

namespace dftracer {
namespace utils {
namespace json {

using Any      = std::any;
using AnyMap   = std::unordered_map<std::string, Any>;
using AnyArray = std::vector<Any>;

std::vector<AnyMap> parse_json_lines(const char* data, size_t size);
AnyMap parse_json(const char* data, size_t size);

std::ostream& operator<<(std::ostream& os, const Any& value);
std::ostream& operator<<(std::ostream& os, const AnyMap& map);
std::ostream& operator<<(std::ostream& os, const std::vector<AnyMap>& maps);

}
}
}

#endif
