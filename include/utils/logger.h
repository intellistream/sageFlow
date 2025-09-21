#pragma once
// Logger utility (color & sequence)
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <memory>
#include <atomic>
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
    lg->set_level(spdlog::level::trace); // 允许输出所有等级，具体过滤可后续封装
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
