#include <algorithm>
#include <spdlog/spdlog.h>
#include <string>

#include <dft_utils/utils/logger.h>

extern "C"
{

    /**
     * Convert string log level to spdlog level enum
     */
    static spdlog::level::level_enum string_to_log_level_internal(const char *level_str)
    {
        std::string lower_level = level_str;
        std::transform(lower_level.begin(), lower_level.end(), lower_level.begin(), ::tolower);

        if (lower_level == "trace")
            return spdlog::level::trace;
        if (lower_level == "debug")
            return spdlog::level::debug;
        if (lower_level == "info")
            return spdlog::level::info;
        if (lower_level == "warn" || lower_level == "warning")
            return spdlog::level::warn;
        if (lower_level == "err" || lower_level == "error")
            return spdlog::level::err;
        if (lower_level == "critical")
            return spdlog::level::critical;
        if (lower_level == "off")
            return spdlog::level::off;

        // Default to info if unrecognized
        return spdlog::level::info;
    }

    int dft_utils_set_log_level(const char *level_str)
    {
        if (!level_str)
        {
            return -1;
        }

        spdlog::level::level_enum level = string_to_log_level_internal(level_str);
        spdlog::set_level(level);
        return 0;
    }

    int dft_utils_set_log_level_int(int level)
    {
        if (level < 0 || level > 6)
        {
            return -1;
        }

        spdlog::level::level_enum spdlog_level = static_cast<spdlog::level::level_enum>(level);
        spdlog::set_level(spdlog_level);
        return 0;
    }

    const char *dft_utils_get_log_level_string()
    {
        spdlog::level::level_enum current_level = spdlog::get_level();

        switch (current_level)
        {
        case spdlog::level::trace:
            return "trace";
        case spdlog::level::debug:
            return "debug";
        case spdlog::level::info:
            return "info";
        case spdlog::level::warn:
            return "warn";
        case spdlog::level::err:
            return "error";
        case spdlog::level::critical:
            return "critical";
        case spdlog::level::off:
            return "off";
        default:
            return "info";
        }
    }

    int dft_utils_get_log_level_int()
    {
        return static_cast<int>(spdlog::get_level());
    }

    // Backward compatibility aliases
    int dft_set_log_level(const char *level_str)
    {
        return dft_utils_set_log_level(level_str);
    }

    int dft_set_log_level_int(int level)
    {
        return dft_utils_set_log_level_int(level);
    }

    const char *dft_get_log_level_string()
    {
        return dft_utils_get_log_level_string();
    }

    int dft_get_log_level_int()
    {
        return dft_utils_get_log_level_int();
    }

} // extern "C"

namespace dft
{
namespace utils
{
int set_log_level(const std::string &level_str)
{
    return dft_utils_set_log_level(level_str.c_str());
}

int set_log_level_int(int level)
{
    return dft_utils_set_log_level_int(level);
}

std::string get_log_level_string()
{
    return std::string(dft_utils_get_log_level_string());
}

int get_log_level_int()
{
    return dft_utils_get_log_level_int();
}
} // namespace utils
} // namespace dft
