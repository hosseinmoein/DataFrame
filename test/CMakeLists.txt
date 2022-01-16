# Tests
add_executable(dataframe_tester dataframe_tester.cc)
target_link_libraries(dataframe_tester PRIVATE DataFrame Threads::Threads)
target_compile_options(dataframe_tester
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
dataframe_target_data_files(
    TARGET dataframe_tester
    DATA_FILES
        data/sample_data_dt_index.csv
        data/sample_data_string_index.csv
        data/sample_data.csv
        data/sample_data.json
)
add_test(NAME dataframe_tester COMMAND $<TARGET_FILE:dataframe_tester>)

add_executable(dataframe_tester_2 dataframe_tester_2.cc)
target_link_libraries(dataframe_tester_2 PRIVATE DataFrame)
target_compile_options(dataframe_tester_2
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
dataframe_target_data_files(
    TARGET dataframe_tester_2
    DATA_FILES
        data/csv2_format_data_2.csv
        data/csv2_format_data_no_index.csv
        data/csv2_format_data.csv
        data/DT_IBM.csv
        data/FORD.csv
        data/IBM.csv
        data/sample_data_2.csv
        data/sample_data_2.json
        data/sample_data_no_index.csv
        data/sample_data_no_index.json
        data/sample_data.csv
        data/sample_data.json
        data/SHORT_IBM.csv
)
if(NOT MSVC)
    # For some unknown reason to me, this test sigfaults in AppVeyor
    add_test(NAME dataframe_tester_2 COMMAND $<TARGET_FILE:dataframe_tester_2>)
endif()

add_executable(dataframe_tester_3 dataframe_tester_3.cc)
target_link_libraries(dataframe_tester_3 PRIVATE DataFrame)
add_test(NAME dataframe_tester_3 COMMAND $<TARGET_FILE:dataframe_tester_3>)

add_executable(vectors_tester vectors_tester.cc)
target_link_libraries(vectors_tester PRIVATE DataFrame)
if(NOT MSVC)
    # For some unknown reason to me, this test sigfaults in AppVeyor
    add_test(NAME vectors_tester COMMAND $<TARGET_FILE:vectors_tester>)
endif()

add_executable(vector_ptr_view_tester vector_ptr_view_tester.cc)
target_link_libraries(vector_ptr_view_tester PRIVATE DataFrame)
add_test(NAME vector_ptr_view_tester COMMAND $<TARGET_FILE:vector_ptr_view_tester>)

add_executable(date_time_tester date_time_tester.cc)
target_link_libraries(date_time_tester PRIVATE DataFrame)
add_test(NAME date_time_tester COMMAND $<TARGET_FILE:date_time_tester>)

add_executable(gen_rand_tester gen_rand_tester.cc)
target_link_libraries(gen_rand_tester PRIVATE DataFrame)
add_test(NAME gen_rand_tester COMMAND $<TARGET_FILE:gen_rand_tester>)

add_executable(dataframe_thread_safety dataframe_thread_safety.cc)
target_link_libraries(dataframe_thread_safety PRIVATE DataFrame Threads::Threads)
add_test(NAME dataframe_thread_safety COMMAND $<TARGET_FILE:dataframe_thread_safety>)

# TODO: move example & benchmarks in a more appropriate location?

# Example
add_executable(hello_world hello_world.cc)
target_link_libraries(hello_world PRIVATE DataFrame)
dataframe_target_data_files(TARGET hello_world DATA_FILES data/SHORT_IBM.csv)

# Benchmarks
add_executable(dataframe_performance dataframe_performance.cc)
target_link_libraries(dataframe_performance PRIVATE DataFrame)

add_executable(dataframe_performance_2 dataframe_performance_2.cc)
target_link_libraries(dataframe_performance_2 PRIVATE DataFrame)
