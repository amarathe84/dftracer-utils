#include <dftracer/utils/utils/json.h>
#include <simdjson.h>

namespace dftracer {
namespace utils {
namespace json {

namespace {

Any convert_value(simdjson::ondemand::value v) {
    auto t = v.type();
    if (t.error() != simdjson::SUCCESS) {
        return Any(std::string(std::string_view(v.raw_json_token())));
    }
    
    switch (t.value()) {
        case simdjson::ondemand::json_type::null:
            return Any{};
        case simdjson::ondemand::json_type::boolean:
            return Any(bool(v.get_bool().value()));
        case simdjson::ondemand::json_type::number: {
            auto i = v.get_int64();
            if (i.error() == simdjson::SUCCESS) {
                return Any(i.value());
            }
            return Any(double(v.get_double().value()));
        }
        case simdjson::ondemand::json_type::string:
            return Any(std::string(std::string_view(v.get_string().value())));
        case simdjson::ondemand::json_type::array: {
            AnyArray out;
            for (auto x : v.get_array()) {
                out.emplace_back(convert_value(x.value()));
            }
            return Any(out);
        }
        case simdjson::ondemand::json_type::object: {
            AnyMap out;
            for (auto f : v.get_object()) {
                std::string k{std::string_view(f.unescaped_key().value())};
                out.emplace(std::move(k), convert_value(f.value()));
            }
            return Any(out);
        }
    }
    return Any{};
}

size_t find_last_newline(const char* data, size_t n) {
    for (size_t i = n; i > 0; --i) {
        char c = data[i-1];
        if (c=='\n' || c=='\r') return i;
    }
    return 0;
}

} // anonymous namespace

AnyMap parse_json(const char* data, size_t size) {
    simdjson::padded_string padded(data, size);
    simdjson::ondemand::parser parser;
    auto doc = parser.iterate(padded);
    if (doc.error()) {
        return AnyMap{};
    }
    
    auto t = doc.type();
    if (t.error() == simdjson::SUCCESS && t.value() == simdjson::ondemand::json_type::object) {
        AnyMap out;
        for (auto f : doc.get_object()) {
            std::string k{std::string_view(f.unescaped_key().value())};
            out.emplace(std::move(k), convert_value(f.value()));
        }
        return out;
    } else {
        // Non-object: store under "_" key  
        AnyMap m;
        m.emplace("_", convert_value(doc.value()));
        return m;
    }
}

std::vector<AnyMap> parse_json_lines(const char* data, size_t size) {
    std::vector<AnyMap> out;

    // Trim trailing partial line (avoid errors when the last line is incomplete)
    size_t parse_len = find_last_newline(data, size);
    if (parse_len == 0) parse_len = size;

    // Split by lines and parse each one
    const char* start = data;
    const char* end = data + parse_len;
    
    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n' && *line_end != '\r') {
            line_end++;
        }
        
        if (line_end > start) {
            out.emplace_back(parse_json(start, static_cast<size_t>(line_end - start)));
        }
        
        // Skip newline characters
        while (line_end < end && (*line_end == '\n' || *line_end == '\r')) {
            line_end++;
        }
        start = line_end;
    }
    
    return out;
}

std::ostream& operator<<(std::ostream& os, const Any& value) {
    try {
        if (!value.has_value()) {
            return os << "null";
        }
        
        const std::type_info& type = value.type();
        
        if (type == typeid(bool)) {
            return os << (std::any_cast<bool>(value) ? "true" : "false");
        } else if (type == typeid(int64_t)) {
            return os << std::any_cast<int64_t>(value);
        } else if (type == typeid(uint64_t)) {
            return os << std::any_cast<uint64_t>(value);
        } else if (type == typeid(double)) {
            return os << std::any_cast<double>(value);
        } else if (type == typeid(std::string)) {
            return os << '"' << std::any_cast<std::string>(value) << '"';
        } else if (type == typeid(AnyArray)) {
            const auto& arr = std::any_cast<AnyArray>(value);
            os << "[";
            for (size_t i = 0; i < arr.size(); ++i) {
                if (i > 0) os << ",";
                os << arr[i];
            }
            return os << "]";
        } else if (type == typeid(AnyMap)) {
            return os << std::any_cast<AnyMap>(value);
        }
        return os << "\"<unknown type>\"";
    } catch (...) {
        return os << "\"<error>\"";
    }
}

std::ostream& operator<<(std::ostream& os, const AnyMap& map) {
    os << "{";
    bool first = true;
    for (const auto& pair : map) {
        if (!first) os << ",";
        first = false;
        os << '"' << pair.first << "\":" << pair.second;
    }
    return os << "}";
}

std::ostream& operator<<(std::ostream& os, const std::vector<AnyMap>& maps) {
    for (size_t i = 0; i < maps.size(); ++i) {
        if (i > 0) os << "\n";
        os << maps[i];
    }
    return os;
}

}
}
}
