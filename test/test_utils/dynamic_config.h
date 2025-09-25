#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <variant>
#include <cstdint>
#include "toml++/toml.hpp"

namespace candy { namespace test {

using ConfigValue = std::variant<int,int64_t,uint32_t,double,std::string,std::vector<int>,std::vector<std::string>>;

class DynamicConfig {
public:
    void set(const std::string& key, const ConfigValue& value);
    template<typename T> T get(const std::string& key) const;
    template<typename T> T get(const std::string& key, const T& default_value) const;
    bool has(const std::string& key) const;
    std::vector<std::string> keys() const;
    void dump(std::ostream& os) const;
private:
    std::unordered_map<std::string, ConfigValue> config_map_;
};

class DynamicConfigManager {
public:
    static bool loadConfig(const std::string& config_path, const std::string& section, DynamicConfig& config);
    static bool loadConfigs(const std::string& config_path, const std::string& section, std::vector<DynamicConfig>& configs);
    static bool loadRootConfig(const std::string& config_path, DynamicConfig& config);
private:
    static void extractConfig(const toml::table& tbl, DynamicConfig& config, const std::string& prefix = "");
    static ConfigValue convertTomlValue(const toml::node& node);
    static toml::table parseFileWithFallback(const std::string& path);
};

template<typename T>
T DynamicConfig::get(const std::string& key) const {
    auto it = config_map_.find(key);
    if (it == config_map_.end()) throw std::runtime_error("Config key not found: " + key);
    try { return std::get<T>(it->second); } catch (...) { throw std::runtime_error("Config key '"+key+"' has wrong type"); }
}

template<typename T>
T DynamicConfig::get(const std::string& key, const T& default_value) const {
    auto it = config_map_.find(key);
    if (it == config_map_.end()) return default_value;
    try { return std::get<T>(it->second); } catch (...) { return default_value; }
}

}} // namespace
