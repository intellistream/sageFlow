add_gtest(
    unit_tests test_compute_engine.cpp test_file_stream_source.cpp
)
target_link_libraries(
    unit_tests
    candy
)