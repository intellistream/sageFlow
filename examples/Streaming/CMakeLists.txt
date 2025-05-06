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
        sift_windowed_stream
        sift_windowed_stream.cpp
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
        sift_windowed_stream
        candy
)
