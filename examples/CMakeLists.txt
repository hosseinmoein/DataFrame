add_executable(hello_world hello_world.cc)
target_link_libraries(hello_world PRIVATE DataFrame Threads::Threads)
target_compile_options(
    hello_world
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)
# add_test(NAME hello_world COMMAND hello_world)

