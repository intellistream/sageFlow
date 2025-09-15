#include "utils/log_config.h"
#include "utils/logger.h"
#include <cstdlib>

namespace candy {

void apply_log_level(spdlog::level::level_enum lvl) {
    auto lg = get_logger();
    lg->set_level(lvl);
    // 同时设置 sink 最低等级（spdlog 会根据 logger 过滤，但保险）
    for (auto &s : lg->sinks()) {
        s->set_level(lvl);
    }
    CANDY_LOG_INFO("LOG", "log_level_applied level={} ", spdlog::level::to_string_view(lvl));
}

void init_log_level(const std::string &level_from_config) {
    const char* env = std::getenv("CANDY_LOG_LEVEL");
    std::string chosen = env && *env ? std::string(env) : level_from_config;
    auto lvl = parse_log_level(chosen);
    apply_log_level(lvl);
}

} // namespace candy
