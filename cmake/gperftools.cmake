if(NOT ENABLE_GPERFTOOLS)
    message(STATUS "gperftools profiling support disabled by option")
    set(PROFILER_LIB "" CACHE FILEPATH "" FORCE)
    set(TCMALLOC_LIB "" CACHE FILEPATH "" FORCE)
    return()
endif()

# 查找 gperftools 库
find_library(PROFILER_LIB profiler)
find_library(TCMALLOC_LIB tcmalloc)

# 检查是否找到库
if(NOT PROFILER_LIB)
    message(WARNING "Profiler library not found. Performance profiling will be disabled.")
    set(ENABLE_GPERFTOOLS OFF CACHE BOOL "Enable gperftools profiling support" FORCE)
endif()

if(ENABLE_GPERFTOOLS AND NOT TCMALLOC_LIB)
    message(WARNING "TCMalloc library not found. Memory allocation profiling will be disabled.")
endif()