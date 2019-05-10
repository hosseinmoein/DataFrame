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
