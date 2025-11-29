add_lib(
        compute_engine
        compute_engine.cpp
        pca.cpp
        simd_distance.cpp
)

# Enable SIMD optimizations based on CPU architecture
include(CheckCXXCompilerFlag)

# Check and enable AVX support
check_cxx_compiler_flag("-mavx" COMPILER_SUPPORTS_AVX)
if(COMPILER_SUPPORTS_AVX)
    target_compile_options(compute_engine PRIVATE -mavx)
endif()

# Check and enable AVX2 support
check_cxx_compiler_flag("-mavx2" COMPILER_SUPPORTS_AVX2)
if(COMPILER_SUPPORTS_AVX2)
    target_compile_options(compute_engine PRIVATE -mavx2)
endif()

# Check and enable FMA support
check_cxx_compiler_flag("-mfma" COMPILER_SUPPORTS_FMA)
if(COMPILER_SUPPORTS_FMA)
    target_compile_options(compute_engine PRIVATE -mfma)
endif()

# Check and enable AVX-512 support (optional, for newer CPUs)
check_cxx_compiler_flag("-mavx512f" COMPILER_SUPPORTS_AVX512)
if(COMPILER_SUPPORTS_AVX512)
    target_compile_options(compute_engine PRIVATE -mavx512f)
endif()
