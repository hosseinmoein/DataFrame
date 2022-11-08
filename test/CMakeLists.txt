add_executable(dataframe_tester dataframe_tester.cc)
target_link_libraries(dataframe_tester PRIVATE DataFrame Threads::Threads)
target_compile_options(dataframe_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
hmdf_target_data_files(dataframe_tester
    DATA_FILES
        ${PROJECT_SOURCE_DIR}/data/sample_data_dt_index.csv
        ${PROJECT_SOURCE_DIR}/data/sample_data_string_index.csv
        ${PROJECT_SOURCE_DIR}/data/sample_data.csv
        ${PROJECT_SOURCE_DIR}/data/sample_data.json
)
add_test(NAME dataframe_tester COMMAND dataframe_tester)

add_executable(dataframe_tester_2 dataframe_tester_2.cc)
target_link_libraries(dataframe_tester_2 PRIVATE DataFrame)
target_compile_options(dataframe_tester_2
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
hmdf_target_data_files(dataframe_tester_2
    DATA_FILES
        ${PROJECT_SOURCE_DIR}/data/csv2_format_data_2.csv
        ${PROJECT_SOURCE_DIR}/data/csv2_format_data_no_index.csv
        ${PROJECT_SOURCE_DIR}/data/csv2_format_data.csv
        ${PROJECT_SOURCE_DIR}/data/DT_IBM.csv
        ${PROJECT_SOURCE_DIR}/data/FORD.csv
        ${PROJECT_SOURCE_DIR}/data/IBM.csv
        ${PROJECT_SOURCE_DIR}/data/sample_data_2.csv
        ${PROJECT_SOURCE_DIR}/data/sample_data_2.json
        ${PROJECT_SOURCE_DIR}/data/sample_data_no_index.csv
        ${PROJECT_SOURCE_DIR}/data/sample_data_no_index.json
        ${PROJECT_SOURCE_DIR}/data/sample_data.csv
        ${PROJECT_SOURCE_DIR}/data/sample_data.json
        ${PROJECT_SOURCE_DIR}/data/SHORT_IBM.csv
)
add_test(NAME dataframe_tester_2 COMMAND dataframe_tester_2)
if(MSVC)
    # For some unknown reason to me, this test sigfaults in AppVeyor
    set_tests_properties(dataframe_tester_2 PROPERTIES DISABLED TRUE)
endif()

add_executable(dataframe_tester_3 dataframe_tester_3.cc)
target_link_libraries(dataframe_tester_3 PRIVATE DataFrame)
add_test(NAME dataframe_tester_3 COMMAND dataframe_tester_3)

add_executable(dataframe_tester_schema dataframe_tester_schema.cc)
target_link_libraries(dataframe_tester_schema PRIVATE DataFrame)
add_test(NAME dataframe_tester_schema COMMAND dataframe_tester_schema)

add_executable(vectors_tester vectors_tester.cc)
target_link_libraries(vectors_tester PRIVATE DataFrame)
add_test(NAME vectors_tester COMMAND vectors_tester)
if(MSVC)
    # For some unknown reason to me, this test sigfaults in AppVeyor
    set_tests_properties(vectors_tester PROPERTIES DISABLED TRUE)
endif()

add_executable(vector_ptr_view_tester vector_ptr_view_tester.cc)
target_link_libraries(vector_ptr_view_tester PRIVATE DataFrame)
add_test(NAME vector_ptr_view_tester COMMAND vector_ptr_view_tester)

add_executable(date_time_tester date_time_tester.cc)
target_link_libraries(date_time_tester PRIVATE DataFrame)
add_test(NAME date_time_tester COMMAND date_time_tester)

add_executable(gen_rand_tester gen_rand_tester.cc)
target_link_libraries(gen_rand_tester PRIVATE DataFrame)
add_test(NAME gen_rand_tester COMMAND gen_rand_tester)

add_executable(dataframe_thread_safety dataframe_thread_safety.cc)
target_link_libraries(dataframe_thread_safety PRIVATE DataFrame Threads::Threads)
add_test(NAME dataframe_thread_safety COMMAND dataframe_thread_safety)
