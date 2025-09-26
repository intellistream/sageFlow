#include "test_utils/dynamic_config.h"
#include <iostream>
#include <filesystem>

namespace candy {
namespace test {

void DynamicConfig::set(const std::string& key, const ConfigValue& value) { config_map_[key]=value; }
bool DynamicConfig::has(const std::string& key) const { return config_map_.count(key)>0; }
std::vector<std::string> DynamicConfig::keys() const { std::vector<std::string> r; r.reserve(config_map_.size()); for(auto &p:config_map_) r.push_back(p.first); return r; }
void DynamicConfig::dump(std::ostream& os) const {
	os << "[DynamicConfig] {\n";
	for (const auto& [k,v] : config_map_) {
		os << "  " << k << " = ";
		std::visit([&](auto const& x){
			using T=std::decay_t<decltype(x)>;
			if constexpr (std::is_same_v<T,std::vector<int>>) {
				os << "["; for(size_t i=0;i<x.size();++i){ if(i) os<<","; os<<x[i]; } os<<"]";
			} else if constexpr (std::is_same_v<T,std::vector<std::string>>) {
				os << "["; for(size_t i=0;i<x.size();++i){ if(i) os<<","; os<<'"'<<x[i]<<'"'; } os<<"]";
			} else { os << x; }
		}, v);
		os << "\n";
	}
	os << "}\n";
}

toml::table DynamicConfigManager::parseFileWithFallback(const std::string& path) {
	std::vector<std::string> candidates{path};
#ifdef PROJECT_DIR
	try { candidates.push_back((std::filesystem::path(PROJECT_DIR)/path).string()); } catch(...) {}
#endif
	for (auto &p : candidates) {
		try { auto result = toml::parse_file(p); const toml::table& root = result; return root; } catch(const std::exception&) {}
	}
	throw std::runtime_error("Failed to open config: "+path);
}

ConfigValue DynamicConfigManager::convertTomlValue(const toml::node& node) {
	if (auto v=node.value<int>()) return *v;
	if (auto v=node.value<int64_t>()) return *v;
	if (auto v=node.value<uint32_t>()) return *v;
	if (auto v=node.value<double>()) return *v;
	if (auto v=node.value<std::string>()) return *v;
	if (auto arr=node.as_array()) {
		std::vector<int> ints; bool all=true; for(auto &e:*arr){ if(auto x=e.value<int>()) ints.push_back(*x); else { all=false; break; } }
		if(all && !ints.empty()) return ints;
		std::vector<std::string> strs; for(auto &e:*arr){ if(auto x=e.value<std::string>()) strs.push_back(*x); else throw std::runtime_error("Mixed array types"); }
		return strs;
	}
	throw std::runtime_error("Unsupported TOML value type");
}

void DynamicConfigManager::extractConfig(const toml::table& tbl, DynamicConfig& config, const std::string& prefix) {
	for (auto& [key, value] : tbl) {
		std::string k = prefix.empty() ? std::string(key.str()) : (prefix+"."+std::string(key.str()));
		if (auto* subtbl=value.as_table()) { extractConfig(*subtbl, config, k); continue; }
		try { config.set(k, convertTomlValue(value)); }
		catch(const std::exception& e){ std::cerr << "[DynamicConfig] Warn convert '"<<k<<"': "<<e.what()<<std::endl; }
	}
}

bool DynamicConfigManager::loadConfig(const std::string& config_path, const std::string& section, DynamicConfig& config) {
	try { const toml::table& root = parseFileWithFallback(config_path);
		if(section.empty()){ extractConfig(root, config, ""); config.dump(std::cerr); return true; }
		if(auto* sect=root[section].as_table()){ extractConfig(*sect, config, ""); config.dump(std::cerr); return true; }
		std::cerr<<"[DynamicConfig] Section '"<<section<<"' not found or not a table"<<std::endl; return false;
	} catch(const std::exception& e){ std::cerr<<"[DynamicConfig] Failed load "<<config_path<<": "<<e.what()<<std::endl; return false; }
}

bool DynamicConfigManager::loadConfigs(const std::string& config_path, const std::string& section, std::vector<DynamicConfig>& configs) {
	try { const toml::table& root = parseFileWithFallback(config_path);
		if(section.empty()){ DynamicConfig c; extractConfig(root,c,""); c.dump(std::cerr); configs.push_back(std::move(c)); return true; }
		if(auto* arr=root[section].as_array()){
			for(auto &node:*arr){ if(auto* t=node.as_table()){ DynamicConfig c; extractConfig(*t,c,""); c.dump(std::cerr); configs.push_back(std::move(c)); }}
			return true;
		}
		std::cerr<<"[DynamicConfig] Section '"<<section<<"' not found or not an array"<<std::endl; return false;
	} catch(const std::exception& e){ std::cerr<<"[DynamicConfig] Failed load configs "<<config_path<<": "<<e.what()<<std::endl; return false; }
}

bool DynamicConfigManager::loadRootConfig(const std::string& config_path, DynamicConfig& config) { return loadConfig(config_path, "", config); }

} // namespace test
} // namespace candy