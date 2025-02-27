add_subdirectory(
		argparse
)
add_subdirectory(
		fmt
)
set(
	BUILD_GMOCK OFF
)
add_subdirectory(
		googletest
)
set(
		SPDLOG_FMT_EXTERNAL ON
)
add_subdirectory(
		spdlog
)
add_subdirectory(
	tomlplusplus
)
add_library(
	externalLibs INTERFACE
)
target_link_libraries(
	externalLibs INTERFACE
	argparse
	fmt
	spdlog
	tomlplusplus::tomlplusplus
)