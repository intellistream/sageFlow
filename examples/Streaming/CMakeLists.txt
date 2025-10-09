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
        PRIVATE
        sageflow
        externalRuntimeLibs
)
target_link_libraries(
        topk
        PRIVATE
        sageflow
        externalRuntimeLibs
)

target_link_libraries(
        itopk
        PRIVATE
        sageflow
        externalRuntimeLibs
)

target_link_libraries(
        aggregate
        PRIVATE
        sageflow
        externalRuntimeLibs
)
macro(add_streaming_example name)
        add_executable(${name} ${name}.cpp)
        target_link_libraries(${name} PRIVATE sageflow externalRuntimeLibs)
endmacro()
add_streaming_example(test1)
add_streaming_example(test2)
add_streaming_example(index_perf)
add_streaming_example(buffer_test)