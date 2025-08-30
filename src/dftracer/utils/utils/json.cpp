#include <dftracer/utils/utils/json.h>
#include <simdjson.h>

#include <thread>

namespace dftracer::utils::json {

namespace {

std::size_t find_last_newline(const char* data, std::size_t n) {
    for (std::size_t i = n; i > 0; --i) {
        char c = data[i - 1];
        if (c == '\n' || c == '\r') return i;
    }
    return 0;
}
}  // anonymous namespace

std::ostream& operator<<(std::ostream& os, const JsonDocument& doc) {
    os << simdjson::minify(doc);
    return os;
}

std::ostream& operator<<(std::ostream& os, const JsonDocuments& docs) {
    for (std::size_t i = 0; i < docs.size(); ++i) {
        if (i > 0) os << "\n";
        dftracer::utils::json::operator<<(os, docs[i]);
    }
    return os;
}

JsonDocuments parse_json_lines(JsonParser& parser, const char* data,
                               std::size_t size) {
    JsonDocuments out;

    // Quick estimation for better memory allocation
    std::size_t estimated_lines = size / 80 + 16;
    out.reserve(estimated_lines);

    // Trim trailing partial line
    std::size_t parse_len = find_last_newline(data, size);
    if (parse_len == 0) parse_len = size;

    const char* start = data;
    const char* end = data + parse_len;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n' && *line_end != '\r') {
            ++line_end;
        }

        if (line_end > start) {
            std::size_t line_size = static_cast<std::size_t>(line_end - start);

            // Create a fresh parser for each line to avoid buffer reuse issues
            JsonParser line_parser;
            auto el = line_parser.parse(start, line_size);
            if (!el.error()) {
                out.emplace_back(std::move(el.value()));
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

JsonDocument parse_json(JsonParser& parser, const char* data,
                        std::size_t size) {
    auto doc = parser.parse(data, size);
    if (doc.error()) {
        return JsonDocument();
    }
    return doc.value();
}

std::string get_string_field(const JsonDocument& doc, const std::string& key) {
    if (!doc.is_object()) return "";

    auto obj_result = doc.get_object();
    if (obj_result.error()) return "";

    auto obj = obj_result.value();
    for (auto field : obj) {
        std::string field_key = std::string(field.key);
        if (field_key == key) {
            if (field.value.is_string()) {
                auto str_result = field.value.get_string();
                if (!str_result.error()) {
                    return std::string(str_result.value());
                }
            }
        }
    }
    return "";
}

double get_double_field(const JsonDocument& doc, const std::string& key) {
    if (!doc.is_object()) return 0.0;

    auto obj_result = doc.get_object();
    if (obj_result.error()) return 0.0;

    auto obj = obj_result.value();
    for (auto field : obj) {
        std::string field_key = std::string(field.key);
        if (field_key == key) {
            if (field.value.is_double()) {
                auto val_result = field.value.get_double();
                if (!val_result.error()) {
                    return val_result.value();
                }
            } else if (field.value.is_int64()) {
                auto val_result = field.value.get_int64();
                if (!val_result.error()) {
                    return static_cast<double>(val_result.value());
                }
            } else if (field.value.is_uint64()) {
                auto val_result = field.value.get_uint64();
                if (!val_result.error()) {
                    return static_cast<double>(val_result.value());
                }
            } else if (field.value.is_string()) {
                auto str_result = field.value.get_string();
                if (!str_result.error()) {
                    try {
                        return std::stod(std::string(str_result.value()));
                    } catch (...) {
                        // Invalid number string, return 0.0
                    }
                }
            }
        }
    }
    return 0.0;
}

std::uint64_t get_uint64_field(const JsonDocument& doc,
                               const std::string& key) {
    if (!doc.is_object()) return 0;

    auto obj_result = doc.get_object();
    if (obj_result.error()) return 0;

    auto obj = obj_result.value();
    for (auto field : obj) {
        std::string field_key = std::string(field.key);
        if (field_key == key) {
            if (field.value.is_uint64()) {
                auto val_result = field.value.get_uint64();
                if (!val_result.error()) {
                    return val_result.value();
                }
            } else if (field.value.is_int64()) {
                auto val_result = field.value.get_int64();
                if (!val_result.error()) {
                    return static_cast<uint64_t>(val_result.value());
                }
            } else if (field.value.is_double()) {
                auto val_result = field.value.get_double();
                if (!val_result.error()) {
                    return static_cast<uint64_t>(val_result.value());
                }
            } else if (field.value.is_string()) {
                auto str_result = field.value.get_string();
                if (!str_result.error()) {
                    try {
                        return std::stoull(std::string(str_result.value()));
                    } catch (...) {
                        // Invalid number string, return 0
                    }
                }
            }
        }
    }
    return 0;
}

std::string get_args_string_field(const JsonDocument& doc,
                                  const std::string& key) {
    if (!doc.is_object()) return "";

    auto obj_result = doc.get_object();
    if (obj_result.error()) return "";

    auto obj = obj_result.value();
    for (auto field : obj) {
        std::string field_key = std::string(field.key);
        if (field_key == "args" && field.value.is_object()) {
            auto args_result = field.value.get_object();
            if (!args_result.error()) {
                auto args = args_result.value();
                for (auto arg_field : args) {
                    std::string arg_key = std::string(arg_field.key);
                    if (arg_key == key && arg_field.value.is_string()) {
                        auto str_result = arg_field.value.get_string();
                        if (!str_result.error()) {
                            return std::string(str_result.value());
                        }
                    }
                }
            }
        }
    }
    return "";
}
}  // namespace dftracer::utils::json
