include_directories(${PROJECT_SOURCE_DIR}/DataFrame/include)

# Build the test binary
add_executable(dataframe_tester dataframe_tester.cc)
add_executable(vectors_tester vectors_tester.cc)
add_executable(date_time_tester date_time_tester.cc)

add_dependencies(dataframe_tester Dataframe)
add_dependencies(vectors_tester Dataframe)
add_dependencies(date_time_tester Dataframe)

target_link_libraries(dataframe_tester Dataframe) # Link the Dataframe library to the test binary
target_link_libraries(vectors_tester Dataframe) # Link the Dataframe library to the test binary
target_link_libraries(date_time_tester Dataframe) # Link the Dataframe library to the test binary
if (UNIX)
    # Find pthreads library
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    target_link_libraries(dataframe_tester Threads::Threads)
    target_link_libraries(vectors_tester Threads::Threads)
    target_link_libraries(date_time_tester Threads::Threads)
endif (UNIX)

# Enable ctest, testing so we can see if unit tests pass or fail in CI
enable_testing()
add_test(NAME dataframe_tester
        COMMAND dataframe_tester
        WORKING_DIRECTORY $<TARGET_FILE_DIR:dataframe_tester>)
add_test(NAME vectors_tester
        COMMAND vectors_tester
        WORKING_DIRECTORY $<TARGET_FILE_DIR:vectors_tester>)
add_test(NAME date_time_tester
        COMMAND date_time_tester
        WORKING_DIRECTORY $<TARGET_FILE_DIR:date_time_tester>)

message("-- Copying files for testing")
# Ctest require this files in the build dir, on all platforms
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data_dt_index.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data_string_index.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
if (MSVC)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data_dt_index.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data_dt_index.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data_string_index.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Debug)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/sample_data_string_index.csv DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/Release)
endif(MSVC)
message("-- Copying files for testing - done")
