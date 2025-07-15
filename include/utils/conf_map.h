//
// Created by Pygon on 25-2-23.
//

#ifndef CONF_MAP_H
#define CONF_MAP_H
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>

namespace candy {
enum class ConfigType { STRING, I64, DOUBLE };

using ConfigValue = std::variant<std::string, int64_t, double>;

class ConfigMap {
  std::unordered_map<std::string, std::pair<ConfigType, ConfigValue> > config_map_;

 public:
  ConfigMap() = default;
  explicit ConfigMap(const std::map<std::string, std::string> &config) {
    for (const auto &pair : config) {
      config_map_[pair.first] = std::make_pair(ConfigType::STRING, pair.second);
    }
  }

  auto getValue(const std::string &key) const -> ConfigValue {
    if (!config_map_.contains(key)) {
      throw std::invalid_argument("Key not found in configuration map.");
    }
    return config_map_.at(key).second;
  }

  auto getString(const std::string &key) const -> std::string {
    if (config_map_.at(key).first != ConfigType::STRING) {
      throw std::invalid_argument("Key is not of type string.");
    }
    return std::get<std::string>(config_map_.at(key).second);
  }

  auto getI64(const std::string &key) const -> int64_t {
    if (config_map_.at(key).first != ConfigType::I64) {
      throw std::invalid_argument("Key is not of type int64_t.");
    }
    return std::get<int64_t>(config_map_.at(key).second);
  }

  auto getDouble(const std::string &key) const -> double {
    if (config_map_.at(key).first != ConfigType::DOUBLE) {
      throw std::invalid_argument("Key is not of type double.");
    }
    return std::get<double>(config_map_.at(key).second);
  }

  void setValue(const std::string &key, ConfigType type, const ConfigValue &value) {
    config_map_[key] = std::make_pair(type, value);
  }

  void setValue(const std::string_view &key, ConfigType type, ConfigValue &&value) {
    config_map_.emplace(key, std::make_pair(type, std::move(value)));
  }

  auto exist(const std::string &key) const -> bool { return config_map_.contains(key); }
};
}  // namespace candy

#endif  // CONF_MAP_H
