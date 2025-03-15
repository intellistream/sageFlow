add_lib(
        streaming
        stream_environment.cpp
        stream.cpp
        data_stream_source/data_stream_source.cpp
        data_stream_source/file_stream_source.cpp
        planner.cpp

)
target_link_libraries(
        streaming
        runtime
)