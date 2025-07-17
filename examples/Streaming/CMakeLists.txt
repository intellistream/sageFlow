add_executable(
        Streaming_example
        main.cpp
)
# add_executable(
#         topk
#         topk.cpp
# )
# add_executable(
#         itopk
#         itopk.cpp
# )

# add_executable(
#         aggregate
#         aggregate.cpp
# )

target_link_libraries(
        Streaming_example
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
# target_link_libraries(
#         topk
#         candy_lib
# )

# target_link_libraries(
#         itopk
#         candy_lib
# )

# target_link_libraries(
#         aggregate
#         candy_lib
# )
target_include_directories(
        Streaming_example PRIVATE
        ${PROJECT_SOURCE_DIR}/third-party/tomlplusplus/include
)
# target_include_directories(
#         topk PRIVATE
#         ${PROJECT_SOURCE_DIR}/third-party/tomlplusplus/include
# )
# target_include_directories(
#         itopk PRIVATE
#         ${PROJECT_SOURCE_DIR}/third-party/tomlplusplus/include
# )
# target_include_directories(
#         aggregate PRIVATE
#         ${PROJECT_SOURCE_DIR}/third-party/tomlplusplus/include
# )
# macro(add_test name)
#     add_executable(${name} ${name}.cpp)
#     target_link_libraries(${name} candy_lib)
#     target_include_directories(
#         ${name} PRIVATE
#         ${PROJECT_SOURCE_DIR}/third-party/tomlplusplus/include
#     )
# endmacro()
# add_test(test1)
# add_test(test2)
# add_test(index_perf)
# add_test(buffer_test)