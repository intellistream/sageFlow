add_lib(
        core
        compute_engine/compute_engine.cpp
        state_interface.cpp
)

target_link_libraries(
        core
        utils
)