add_lib(
        utils
        monitoring.cpp
        log_config.cpp
)
if(ENABLE_GPERFTOOLS AND DEFINED SAGE_GPERFTOOLS_LIBS AND SAGE_GPERFTOOLS_LIBS)
        target_link_libraries(
                        utils
                        PRIVATE
                        ${SAGE_GPERFTOOLS_LIBS}
        )
endif()