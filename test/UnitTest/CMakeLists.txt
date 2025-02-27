add_gtest(
    unit_tests test_compute_engine.cpp
)
target_link_libraries(
    unit_tests
    core
)