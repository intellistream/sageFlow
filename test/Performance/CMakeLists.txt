add_executable(
        IndexTest
        IndexTest.cpp
)


add_executable(
        test_window_pipeline
        test_window_pipeline.cpp
)

target_link_libraries(
    IndexTest
    candy
)

target_link_libraries(
    test_window_pipeline
    candy
    gtest
    gtest_main
)