add_lib(
        core
        compute_engine/compute_engine.cpp
        utils/monitoring.cpp
        state_interface.cpp
)

target_link_libraries(
        core
        ${PROFILER_LIB}
        ${TCMALLOC_LIB}
)