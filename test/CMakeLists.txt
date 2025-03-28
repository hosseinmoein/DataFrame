add_executable(dataframe_tester dataframe_tester.cc)
target_link_libraries(dataframe_tester PRIVATE DataFrame Threads::Threads)
add_test(NAME dataframe_tester COMMAND dataframe_tester)
target_compile_options(
    dataframe_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(dataframe_tester_2 dataframe_tester_2.cc)
target_link_libraries(dataframe_tester_2 PRIVATE DataFrame)
add_test(NAME dataframe_tester_2 COMMAND dataframe_tester_2)
target_compile_options(
    dataframe_tester_2
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(dataframe_tester_3 dataframe_tester_3.cc)
target_link_libraries(dataframe_tester_3 PRIVATE DataFrame)
add_test(NAME dataframe_tester_3 COMMAND dataframe_tester_3)
target_compile_options(
    dataframe_tester_3
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(dataframe_tester_4 dataframe_tester_4.cc)
add_test(NAME dataframe_tester_4 COMMAND dataframe_tester_4)
target_link_libraries(dataframe_tester_4 PRIVATE DataFrame)
target_compile_options(
    dataframe_tester_4
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(dataframe_tester_schema dataframe_tester_schema.cc)
target_link_libraries(dataframe_tester_schema PRIVATE DataFrame)
add_test(NAME dataframe_tester_schema COMMAND dataframe_tester_schema)
target_compile_options(
    dataframe_tester_schema
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(vectors_tester vectors_tester.cc)
target_link_libraries(vectors_tester PRIVATE DataFrame)
add_test(NAME vectors_tester COMMAND vectors_tester)
target_compile_options(
    vectors_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(matrix_tester matrix_tester.cc)
target_link_libraries(matrix_tester PRIVATE DataFrame)
add_test(NAME matrix_tester COMMAND matrix_tester)
target_compile_options(
    matrix_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(iso_tree_tester iso_tree_tester.cc)
target_link_libraries(iso_tree_tester PRIVATE DataFrame)
add_test(NAME iso_tree_tester COMMAND iso_tree_tester)
target_compile_options(
    iso_tree_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(allocator_tester allocator_tester.cc)
target_link_libraries(allocator_tester PRIVATE DataFrame)
add_test(NAME allocator_tester COMMAND allocator_tester)
target_compile_options(
    allocator_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(vector_ptr_view_tester vector_ptr_view_tester.cc)
target_link_libraries(vector_ptr_view_tester PRIVATE DataFrame)
add_test(NAME vector_ptr_view_tester COMMAND vector_ptr_view_tester)
target_compile_options(
    vector_ptr_view_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(meta_prog_tester meta_prog_tester.cc)
target_link_libraries(meta_prog_tester PRIVATE DataFrame)
add_test(NAME meta_prog_tester COMMAND meta_prog_tester)
target_compile_options(
    meta_prog_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(date_time_tester date_time_tester.cc)
target_link_libraries(date_time_tester PRIVATE DataFrame)
add_test(NAME date_time_tester COMMAND date_time_tester)
target_compile_options(
    date_time_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(gen_rand_tester gen_rand_tester.cc)
target_link_libraries(gen_rand_tester PRIVATE DataFrame)
add_test(NAME gen_rand_tester COMMAND gen_rand_tester)
target_compile_options(
    gen_rand_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)

add_executable(dataframe_thread_safety dataframe_thread_safety.cc)
target_link_libraries(dataframe_thread_safety
    PRIVATE DataFrame Threads::Threads
)
add_test(NAME dataframe_thread_safety COMMAND dataframe_thread_safety)
target_compile_options(
    dataframe_thread_safety
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
            $<$<CXX_COMPILER_ID:MSVC>:/wd4251>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5030>
            $<$<CXX_COMPILER_ID:MSVC>:/wd5222>
    # TODO: fix these warnings
    PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/wd4244>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4267>
           $<$<CXX_COMPILER_ID:MSVC>:/wd4996>
)
