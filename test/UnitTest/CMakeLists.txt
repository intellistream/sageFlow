add_gtest(
    unit_tests test_compute_engine.cpp test_file_stream.cpp
)
target_link_libraries(
    unit_tests
    core
    streaming
    proto
)