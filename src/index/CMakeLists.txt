add_library(
        index
        knn.cpp
)
target_link_libraries(
        index
        storage
)