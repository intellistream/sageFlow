# coordination module CMakeLists.txt
# 边界追踪和协调相关组件

add_library(coordination STATIC
    boundary_tracker.cpp
    late_arrival_handler.cpp
    partition_coordinator.cpp
)

target_include_directories(coordination
    PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(coordination
    PUBLIC
    externalRuntimeLibs
    execution
)
