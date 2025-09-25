#pragma once
// Logger utility (color & sequence)
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <memory>
#include <atomic>
#include <cstdlib>
#include "utils/log_config.h"
// 仅按照日志等级着色，取消不同 phase 自定义颜色

// Windows 控制台 ANSI 支持：由用户外部启用，避免在头文件内直接调用 WinAPI 造成编译器解析问题。

namespace candy {
inline std::atomic<uint64_t> g_log_seq{0};



inline std::shared_ptr<spdlog::logger> get_logger() {
    static std::shared_ptr<spdlog::logger> logger = [](){
    auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    // 强制开启颜色（即使非 TTY 环境）
    sink->set_color_mode(spdlog::color_mode::always);
    // 使用默认的等级颜色（spdlog 已内置），仅强制启用颜色

    auto lg = std::make_shared<spdlog::logger>("candy", sink);
    // Pattern：时间 线程 等级(带色) [PHASE] seq=N msg
    lg->set_pattern("[%H:%M:%S.%e] [tid=%t] [%^%l%$] %v");
    // 初始日志等级：从环境变量 CANDY_LOG_LEVEL 读取；若未设置则默认为 info。
    // 注意：不调用 init_log_level()/apply_log_level 以避免静态初始化期间的递归。
    spdlog::level::level_enum initial_lvl = spdlog::level::info;
    if (const char* env = std::getenv("CANDY_LOG_LEVEL"); env && *env) {
        initial_lvl = candy::parse_log_level(env);
    }
    lg->set_level(initial_lvl);
    for (auto &s : lg->sinks()) {
        s->set_level(initial_lvl);
    }
        return lg;
    }();
    return logger;
}

// 基础宏：带相位着色 + 递增序号
// 统一格式：[PHASE] seq=N message
// 新增 DEBUG 级别，便于将高频诊断从 INFO 下沉
#define CANDY_LOG_DEBUG(phase, fmt, ...) \
    get_logger()->debug("[{}] seq={} " fmt, phase, candy::g_log_seq.fetch_add(1, std::memory_order_relaxed), ##__VA_ARGS__)
#define CANDY_LOG_INFO(phase, fmt, ...) \
    get_logger()->info("[{}] seq={} " fmt, phase, candy::g_log_seq.fetch_add(1, std::memory_order_relaxed), ##__VA_ARGS__)

#define CANDY_LOG_WARN(phase, fmt, ...) \
    get_logger()->warn("[{}] seq={} " fmt, phase, candy::g_log_seq.fetch_add(1, std::memory_order_relaxed), ##__VA_ARGS__)

#define CANDY_LOG_ERROR(phase, fmt, ...) \
    get_logger()->error("[{}] seq={} " fmt, phase, candy::g_log_seq.fetch_add(1, std::memory_order_relaxed), ##__VA_ARGS__)

} // namespace candy
