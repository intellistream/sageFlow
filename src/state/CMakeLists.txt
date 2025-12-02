# CMakeLists.txt for state module

add_lib(
        state
        partitioned_window_state.cpp
        partitioned_vector_state.cpp
        shared_window_state.cpp
        two_tier_window_state.cpp
        window_state_factory.cpp
)
target_link_libraries(
        state
        PRIVATE
        common
        execution
        coordination
)

# window_state_factory 需要 operator 模块中的 JoinStrategyConfig
# 但由于 operator 模块本身有编译问题，暂时通过设置 include 路径解决
target_include_directories(
        state
        PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)
