add_library(candy INTERFACE)

target_include_directories(candy INTERFACE ${PROJECT_SOURCE_DIR}/include)

add_subdirectory(
        utils
)
add_subdirectory(
        index
)
add_subdirectory(
        common
)
add_subdirectory(
        compute_engine
)
add_subdirectory(
        concurrency
)
add_subdirectory(
        function
)
add_subdirectory(
        operator
)
add_subdirectory(
        query
)
add_subdirectory(
       storage
)
add_subdirectory(
        stream
)

add_library(candy_lib STATIC
    candy/candy.cpp
)

target_link_libraries(candy_lib PUBLIC
    common
    concurrency
    function
    index
    operator
    query
    storage
    stream
    compute_engine
    utils
    fmt::fmt
)

target_include_directories(candy_lib PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)