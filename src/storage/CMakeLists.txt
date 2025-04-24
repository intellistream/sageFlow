add_library(
    storage
    storage_manager.cpp
)
target_link_libraries(
        storage
        compute_engine
)