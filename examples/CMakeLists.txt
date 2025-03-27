add_executable(hello_world hello_world.cc)
target_link_libraries(hello_world PRIVATE DataFrame Threads::Threads)
target_compile_options(hello_world PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>)
# add_test(NAME hello_world COMMAND hello_world)

