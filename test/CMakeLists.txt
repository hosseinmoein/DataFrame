add_executable(dataframe_tester dataframe_tester.cc)
target_link_libraries(dataframe_tester PRIVATE DataFrame Threads::Threads)
target_compile_options(dataframe_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)

add_test(NAME dataframe_tester COMMAND dataframe_tester)
if(MSVC)
    # For some unknown reason to me, this test cannot open data files in
    # AppVeyor
    set_tests_properties(dataframe_tester PROPERTIES DISABLED TRUE)
endif()

add_executable(dataframe_tester_2 dataframe_tester_2.cc)
target_link_libraries(dataframe_tester_2 PRIVATE DataFrame)
target_compile_options(dataframe_tester_2
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)

add_test(NAME dataframe_tester_2 COMMAND dataframe_tester_2)
# if(MSVC)
#     # For some unknown reason to me, this test sigfaults in AppVeyor
#     set_tests_properties(dataframe_tester_2 PROPERTIES DISABLED TRUE)
# endif()

add_executable(dataframe_tester_3 dataframe_tester_3.cc)
target_link_libraries(dataframe_tester_3 PRIVATE DataFrame)
target_compile_options(dataframe_tester_3
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
add_test(NAME dataframe_tester_3 COMMAND dataframe_tester_3)

add_executable(dataframe_tester_4 dataframe_tester_4.cc)
target_link_libraries(dataframe_tester_4 PRIVATE DataFrame)
target_compile_options(dataframe_tester_4
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
add_test(NAME dataframe_tester_4 COMMAND dataframe_tester_4)

add_executable(dataframe_tester_schema dataframe_tester_schema.cc)
target_link_libraries(dataframe_tester_schema PRIVATE DataFrame)
add_test(NAME dataframe_tester_schema COMMAND dataframe_tester_schema)

add_executable(vectors_tester vectors_tester.cc)
target_link_libraries(vectors_tester PRIVATE DataFrame)
target_compile_options(vectors_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
add_test(NAME vectors_tester COMMAND vectors_tester)
if(MSVC)
    # For some unknown reason to me, this test sigfaults in AppVeyor
    set_tests_properties(vectors_tester PROPERTIES DISABLED TRUE)
endif()

add_executable(matrix_tester matrix_tester.cc)
target_link_libraries(matrix_tester PRIVATE DataFrame)
target_compile_options(matrix_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
add_test(NAME matrix_tester COMMAND matrix_tester)

if(NOT MSVC)
    # MSVC compiler craps out on immediately executing lambdas
    add_executable(allocator_tester allocator_tester.cc)
    target_link_libraries(allocator_tester PRIVATE DataFrame)
    add_test(NAME allocator_tester COMMAND allocator_tester)
endif()

add_executable(vector_ptr_view_tester vector_ptr_view_tester.cc)
target_link_libraries(vector_ptr_view_tester PRIVATE DataFrame)
add_test(NAME vector_ptr_view_tester COMMAND vector_ptr_view_tester)

add_executable(meta_prog_tester meta_prog_tester.cc)
target_link_libraries(meta_prog_tester PRIVATE DataFrame)
add_test(NAME meta_prog_tester COMMAND meta_prog_tester)

add_executable(date_time_tester date_time_tester.cc)
target_link_libraries(date_time_tester PRIVATE DataFrame)
add_test(NAME date_time_tester COMMAND date_time_tester)

add_executable(gen_rand_tester gen_rand_tester.cc)
target_link_libraries(gen_rand_tester PRIVATE DataFrame)
add_test(NAME gen_rand_tester COMMAND gen_rand_tester)

add_executable(dataframe_thread_safety dataframe_thread_safety.cc)
target_link_libraries(dataframe_thread_safety PRIVATE DataFrame Threads::Threads)
add_test(NAME dataframe_thread_safety COMMAND dataframe_thread_safety)
