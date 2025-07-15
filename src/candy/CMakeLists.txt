add_library(candy_src
        candy.cpp
        )

target_link_libraries(candy_src PUBLIC
        stream
        )
