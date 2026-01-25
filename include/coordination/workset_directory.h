#pragma once

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <vector>
#include <atomic>

namespace sageFlow {

struct WorksetProfile {
    uint64_t id;
    int owner;
    double load;
};

/**
 * @brief Interface for managing Workset ownership and Load info.
 */
class WorksetDirectory {
public:
    virtual ~WorksetDirectory() = default;

    virtual std::optional<int> getOwner(uint64_t workset_id) const = 0;
    virtual void setOwner(uint64_t workset_id, int worker_id) = 0;
    
    // Report load for a specific workset (accumulative or absolute? let's say absolute rate)
    virtual void reportWorksetLoad(uint64_t workset_id, double load) = 0;
    
    // Get global view for rebalancing
    virtual std::vector<WorksetProfile> getAllWorksetProfiles() const = 0;
};

class LocalWorksetDirectory : public WorksetDirectory {
public:
    std::optional<int> getOwner(uint64_t workset_id) const override {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = owners_.find(workset_id);
        if (it != owners_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    void setOwner(uint64_t workset_id, int worker_id) override {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        owners_[workset_id] = worker_id;
    }
    
    void reportWorksetLoad(uint64_t workset_id, double load) override {
        std::lock_guard<std::mutex> lock(load_mutex_);
        loads_[workset_id] = load;
    }
    
    std::vector<WorksetProfile> getAllWorksetProfiles() const override {
        std::shared_lock<std::shared_mutex> owner_lock(mutex_);
        std::lock_guard<std::mutex> load_lock(load_mutex_);
        
        std::vector<WorksetProfile> profiles;
        profiles.reserve(owners_.size());
        
        for (const auto& kv : owners_) {
            double load = 0.0;
            if (loads_.count(kv.first)) {
                load = loads_.at(kv.first);
            }
            profiles.push_back({kv.first, kv.second, load});
        }
        return profiles;
    }

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<uint64_t, int> owners_;
    
    mutable std::mutex load_mutex_;
    std::unordered_map<uint64_t, double> loads_;
};

} // namespace sageFlow
