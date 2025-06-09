add_executable(
        IndexTest
        IndexTest.cpp
)


add_executable(
        test_window_pipeline
        test_window_pipeline.cpp
)

# 添加join方法性能测试
add_executable(
        test_join_performance_methods
        test_join_performance_methods.cpp
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

# join方法性能测试的链接库
target_link_libraries(
    test_join_performance_methods
    candy
    gtest
    gtest_main
)