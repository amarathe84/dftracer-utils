#include <dftracer/utils/utils/json.h>
#include <simdjson.h>
#include <thread>

namespace dftracer {
namespace utils {
namespace json {

namespace {

size_t find_last_newline(const char* data, size_t n) {
    for (size_t i = n; i > 0; --i) {
        char c = data[i-1];
        if (c=='\n' || c=='\r') return i;
    }
    return 0;
}

} // anonymous namespace

// Thread-local DOM parser for better performance
thread_local simdjson::dom::parser tl_parser;

JsonDocument parse_json(const char* data, size_t size) {
    auto doc = tl_parser.parse(data, size);
    if (doc.error()) {
        // Return null element on error
        return simdjson::dom::element{};
    }
    
    return doc.value();
}

JsonDocuments parse_json_lines(const char* data, size_t size) {
    JsonDocuments out;
    
    // Quick estimation for better memory allocation
    size_t estimated_lines = size / 80 + 16;
    out.reserve(estimated_lines);

    // Trim trailing partial line
    size_t parse_len = find_last_newline(data, size);
    if (parse_len == 0) parse_len = size;

    // Parse each line using DOM API - no conversion!
    const char* start = data;
    const char* end = data + parse_len;
    
    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n' && *line_end != '\r') {
            ++line_end;
        }
        
        if (line_end > start) {
            size_t line_size = static_cast<size_t>(line_end - start);
            
            // Parse line with DOM parser - store directly without conversion
            auto doc = tl_parser.parse(start, line_size);
            
            if (!doc.error()) {
                out.emplace_back(doc.value());
            }
        }
        
        // Skip newline characters
        while (line_end < end && (*line_end == '\n' || *line_end == '\r')) {
            ++line_end;
        }
        start = line_end;
    }
    
    return out;
}

std::ostream& operator<<(std::ostream& os, const JsonDocument& doc) {
    // Use simdjson's built-in serialization for maximum efficiency
    os << simdjson::minify(doc);
    return os;
}

std::ostream& operator<<(std::ostream& os, const JsonDocuments& docs) {
    for (size_t i = 0; i < docs.size(); ++i) {
        if (i > 0) os << "\n";
        dftracer::utils::json::operator<<(os, docs[i]);
    }
    return os;
}

}
}
}