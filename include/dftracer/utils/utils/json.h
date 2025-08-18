#ifndef __DFTRACER_UTILS_UTILS_DFT_EVENTS_H
#define __DFTRACER_UTILS_UTILS_DFT_EVENTS_H

#include <unordered_map>
#include <string>
#include <vector>
#include <iosfwd>
#include <simdjson.h>

namespace dftracer {
namespace utils {
namespace json {

// High-performance JSON types using simdjson DOM directly - no conversion
using JsonDocument = simdjson::dom::element;
using JsonDocuments = std::vector<JsonDocument>;

JsonDocuments parse_json_lines(const char* data, size_t size);
JsonDocument parse_json(const char* data, size_t size);

std::ostream& operator<<(std::ostream& os, const JsonDocument& doc);
std::ostream& operator<<(std::ostream& os, const JsonDocuments& docs);

}
}
}

#endif
