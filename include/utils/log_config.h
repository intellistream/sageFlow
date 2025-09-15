#pragma once
#include <string>
#include <spdlog/spdlog.h>

namespace candy {
struct LogConfig {
    spdlog::level::level_enum level = spdlog::level::info;
};

// parse level string (case-insensitive)
inline spdlog::level::level_enum parse_log_level(const std::string &s) {
    std::string v; v.reserve(s.size());
    for (char c: s) v.push_back(static_cast<char>(::tolower(static_cast<unsigned char>(c))));
    if (v=="trace") return spdlog::level::trace;
    if (v=="debug") return spdlog::level::debug;
    if (v=="info")  return spdlog::level::info;
    if (v=="warn" || v=="warning")  return spdlog::level::warn;
    if (v=="err" || v=="error")  return spdlog::level::err;
    if (v=="critical" || v=="fatal") return spdlog::level::critical;
    if (v=="off") return spdlog::level::off;
    return spdlog::level::info;
}

// Apply log level to global logger
void apply_log_level(spdlog::level::level_enum lvl);

// Load from env (CANDY_LOG_LEVEL) or passed string; env overrides.
void init_log_level(const std::string &level_from_config);
}
