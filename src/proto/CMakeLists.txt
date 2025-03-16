find_package(Protobuf REQUIRED)

add_lib(
        proto
        message.pb.cc
)

# target_include_directories(proto ${Protobuf_INCLUDE_DIRS})
target_link_libraries(proto ${Protobuf_LIBRARIES})
