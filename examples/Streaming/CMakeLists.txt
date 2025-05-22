add_executable(
        Streaming_example
        main.cpp
)
add_executable(
        topk
        topk.cpp
)
add_executable(
        itopk
        itopk.cpp
)

add_executable(
        aggregate
        aggregate.cpp
)

target_link_libraries(
        Streaming_example
        candy
)
target_link_libraries(
        topk
        candy
)

target_link_libraries(
        itopk
        candy
)

target_link_libraries(
        aggregate
        candy
)
macro(add_test name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} candy)
endmacro()
add_test(test1)
add_test(test2)
add_test(index_perf)
add_test(buffer_test)