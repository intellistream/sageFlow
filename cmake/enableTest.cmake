# enableTest.cmake
# 负责定义 add_gtest 宏，保持与 test/CMakeLists.txt 现有调用兼容

# 可选包含 GoogleTest.cmake 以支持 gtest_discover_tests
include(GoogleTest OPTIONAL)

# 封装: add_gtest(<target> <source>)
macro(add_gtest TARGET_NAME SOURCE_FILE)
    add_executable(${TARGET_NAME} ${SOURCE_FILE})

    # 统一使用关键字签名，避免与其他地方混用
    if(TARGET gtest_main)
        target_link_libraries(${TARGET_NAME} PRIVATE gtest_main)
    elseif(TARGET GTest::gtest_main)
        target_link_libraries(${TARGET_NAME} PRIVATE GTest::gtest_main)
    endif()
    if(TARGET gtest)
        target_link_libraries(${TARGET_NAME} PRIVATE gtest)
    elseif(TARGET GTest::gtest)
        target_link_libraries(${TARGET_NAME} PRIVATE GTest::gtest)
    endif()

    add_test(NAME ${TARGET_NAME} COMMAND ${TARGET_NAME})

    # 让 CLion 能枚举单测用例
    if(COMMAND gtest_discover_tests)
        gtest_discover_tests(${TARGET_NAME}
            WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
            DISCOVERY_TIMEOUT 30
        )
    endif()
endmacro()

message(STATUS "enableTest.cmake loaded: add_gtest macro (keyword signature) ready")