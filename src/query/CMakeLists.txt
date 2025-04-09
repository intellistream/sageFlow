add_subdirectory(
        executor
)
add_subdirectory(
        optimizer
)
add_library(
        query INTERFACE
)
target_link_libraries(
        query
        INTERFACE
        executor
        optimizer
)