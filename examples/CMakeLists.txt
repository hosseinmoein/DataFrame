add_executable(hello_world hello_world.cc)
target_link_libraries(hello_world PRIVATE DataFrame)
hmdf_target_data_files(hello_world
    DATA_FILES ${PROJECT_SOURCE_DIR}/data/SHORT_IBM.csv
)
