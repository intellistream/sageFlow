macro(add_lib library_name)
add_library(
    ${library_name}
    ${ARGN}
)
target_link_libraries(
    ${library_name}
    externalLibs
)
endmacro(add_lib library_name)
