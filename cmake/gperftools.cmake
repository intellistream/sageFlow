# 查找 gperftools 库
find_library(PROFILER_LIB profiler)
find_library(TCMALLOC_LIB tcmalloc)

# 检查是否找到库
if(NOT PROFILER_LIB)
    message(FATAL_ERROR "Profiler library not found.")
endif()
if(NOT TCMALLOC_LIB)
    message(FATAL_ERROR "TCMalloc library not found.")
endif()