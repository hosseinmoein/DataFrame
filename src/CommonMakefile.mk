## Hossein Moein
## September 12 2017

LOCAL_LIB_DIR = ../lib/$(BUILD_PLATFORM)
LOCAL_BIN_DIR = ../bin/$(BUILD_PLATFORM)
LOCAL_OBJ_DIR = ../obj/$(BUILD_PLATFORM)
LOCAL_INCLUDE_DIR = ../include
PROJECT_LIB_DIR = ../../lib/$(BUILD_PLATFORM)
PROJECT_INCLUDE_DIR = ../../include

# -----------------------------------------------------------------------------

SRCS = ../test/dataframe_tester.cc \
       ../test/dataframe_tester_2.cc \
       ../test/dataframe_tester_3.cc \
       ../test/dataframe_tester_4.cc \
       ../test/dataframe_tester_5.cc \
       ../examples/hello_world.cc \
       ../test/dataframe_thread_safety.cc \
       ../test/dataframe_tester_schema.cc \
       ../benchmarks/dataframe_performance.cc \
       ../benchmarks/dataframe_performance_2.cc \
       ../test/vectors_tester.cc \
       ../test/vector_ptr_view_tester.cc \
       ../test/meta_prog_tester.cc \
       ../test/date_time_tester.cc \
       ../test/gen_rand_tester.cc \
       ../test/allocator_tester.cc \
       ../test/linkedin_benchmark.cc \
       ../test/dataframe_read_large_file.cc \
       Utils/DateTime.cc

HEADERS = $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroVector.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroVector.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroView.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroView.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroConstView.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroConstView.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroPtrView.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroPtrView.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroConstPtrView.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroConstPtrView.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/DataFrame.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_misc.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_standalone.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_set.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_remove.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_get.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_slice.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_visit.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_read.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_write.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_opt.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_join.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_shift.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_sort.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_private_decl.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_functors.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/RandGen.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/DataFrameStatsVisitors.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/DataFrameMLVisitors.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/DataFrameFinancialVisitors.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/DataFrameTransformVisitors.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/DataFrameTypes.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/RandGen.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/DataFrameOperators.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/VectorView.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/VectorPtrView.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Threads/ThreadGranularity.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Threads/SharedQueue.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Threads/SharedQueue.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Threads/ThreadPool.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Threads/ThreadPool.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/DateTime.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Utils.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/MetaProg.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Concepts.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/FixedSizeString.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/FixedSizePriorityQueue.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/AlignedAllocator.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/FixedSizeAllocator.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Endianness.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Matrix.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Matrix.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/IsolationTree.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/IsolationTree.tcc

LIB_NAME = DataFrame
TARGET_LIB = $(LOCAL_LIB_DIR)/lib$(LIB_NAME).a

TARGETS += $(TARGET_LIB) \
           $(LOCAL_BIN_DIR)/dataframe_tester \
           $(LOCAL_BIN_DIR)/dataframe_tester_2 \
           $(LOCAL_BIN_DIR)/dataframe_tester_3 \
           $(LOCAL_BIN_DIR)/dataframe_tester_4 \
           $(LOCAL_BIN_DIR)/dataframe_tester_5 \
           $(LOCAL_BIN_DIR)/hello_world \
           $(LOCAL_BIN_DIR)/dataframe_thread_safety \
           $(LOCAL_BIN_DIR)/dataframe_tester_schema \
           $(LOCAL_BIN_DIR)/dataframe_performance \
           $(LOCAL_BIN_DIR)/dataframe_performance_2 \
           $(LOCAL_BIN_DIR)/vectors_tester \
           $(LOCAL_BIN_DIR)/allocator_tester \
           $(LOCAL_BIN_DIR)/linkedin_benchmark \
           $(LOCAL_BIN_DIR)/dataframe_read_large_file \
           $(LOCAL_BIN_DIR)/vector_ptr_view_tester \
           $(LOCAL_BIN_DIR)/meta_prog_tester \
           $(LOCAL_BIN_DIR)/date_time_tester \
           $(LOCAL_BIN_DIR)/gen_rand_tester \
           $(LOCAL_BIN_DIR)/matrix_tester \
           $(LOCAL_BIN_DIR)/iso_tree_tester

# -----------------------------------------------------------------------------

LFLAGS += -Bstatic -L$(LOCAL_LIB_DIR) -L$(PROJECT_LIB_DIR)

LIBS = $(LFLAGS) -l$(LIB_NAME) $(PLATFORM_LIBS)
INCLUDES += -I. -I$(LOCAL_INCLUDE_DIR) -I$(PROJECT_INCLUDE_DIR)
DEFINES = -Wall -DHMDF_HAVE_CLOCK_GETTIME -DDMS_$(BUILD_DEFINE)__

# -----------------------------------------------------------------------------

# object file
#
LIB_OBJS = $(LOCAL_OBJ_DIR)/DateTime.o

# -----------------------------------------------------------------------------

# set up C++ suffixes and relationship between .cc and .o files
#
.SUFFIXES: .cc

$(LOCAL_OBJ_DIR)/%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@
$(LOCAL_OBJ_DIR)/%.o: ../benchmarks/%.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@
$(LOCAL_OBJ_DIR)/%.o: ../examples/%.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@
$(LOCAL_OBJ_DIR)/%.o: ../test/%.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@
$(LOCAL_OBJ_DIR)/%.o: Vectors/%.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@
$(LOCAL_OBJ_DIR)/%.o: Utils/%.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

.cc :
	$(CXX) $(CXXFLAGS) $< -o $@ -lm $(TLIB) -lg++

# -----------------------------------------------------------------------------

all: PRE_BUILD $(TARGETS)

PRE_BUILD:
	mkdir -p $(LOCAL_LIB_DIR)
	mkdir -p $(LOCAL_BIN_DIR)
	mkdir -p $(LOCAL_OBJ_DIR)
	mkdir -p $(PROJECT_LIB_DIR)
	mkdir -p $(PROJECT_INCLUDE_DIR)/DataFrame

$(TARGET_LIB): $(LIB_OBJS)
	ar -rcs $(TARGET_LIB) $(LIB_OBJS)

DATAFRAME_TESTER_OBJ = $(LOCAL_OBJ_DIR)/dataframe_tester.o
$(LOCAL_BIN_DIR)/dataframe_tester: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ) $(LIBS)

DATAFRAME_TESTER_OBJ_2 = $(LOCAL_OBJ_DIR)/dataframe_tester_2.o
$(LOCAL_BIN_DIR)/dataframe_tester_2: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ_2)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ_2) $(LIBS)

DATAFRAME_TESTER_OBJ_3 = $(LOCAL_OBJ_DIR)/dataframe_tester_3.o
$(LOCAL_BIN_DIR)/dataframe_tester_3: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ_3)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ_3) $(LIBS)

DATAFRAME_TESTER_OBJ_4 = $(LOCAL_OBJ_DIR)/dataframe_tester_4.o
$(LOCAL_BIN_DIR)/dataframe_tester_4: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ_4)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ_4) $(LIBS)

DATAFRAME_TESTER_OBJ_5 = $(LOCAL_OBJ_DIR)/dataframe_tester_5.o
$(LOCAL_BIN_DIR)/dataframe_tester_5: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ_5)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ_5) $(LIBS)

HELLO_WORLD_OBJ = $(LOCAL_OBJ_DIR)/hello_world.o
$(LOCAL_BIN_DIR)/hello_world: $(TARGET_LIB) $(HELLO_WORLD_OBJ)
	$(CXX) -o $@ $(HELLO_WORLD_OBJ) $(LIBS)

DATAFRAME_THREAD_SAFTY_OBJ = $(LOCAL_OBJ_DIR)/dataframe_thread_safety.o
$(LOCAL_BIN_DIR)/dataframe_thread_safety: $(TARGET_LIB) $(DATAFRAME_THREAD_SAFTY_OBJ)
	$(CXX) -o $@ $(DATAFRAME_THREAD_SAFTY_OBJ) $(LIBS)

DATAFRAME_TESTER_SCHEMA_OBJ = $(LOCAL_OBJ_DIR)/dataframe_tester_schema.o
$(LOCAL_BIN_DIR)/dataframe_tester_schema: $(TARGET_LIB) $(DATAFRAME_TESTER_SCHEMA_OBJ)
	$(CXX) -o $@ $(DATAFRAME_TESTER_SCHEMA_OBJ) $(LIBS)

DATAFRAME_PERFORMANCE_OBJ = $(LOCAL_OBJ_DIR)/dataframe_performance.o
$(LOCAL_BIN_DIR)/dataframe_performance: $(TARGET_LIB) $(DATAFRAME_PERFORMANCE_OBJ)
	$(CXX) -o $@ $(DATAFRAME_PERFORMANCE_OBJ) $(LIBS)

DATAFRAME_PERFORMANCE_2_OBJ = $(LOCAL_OBJ_DIR)/dataframe_performance_2.o
$(LOCAL_BIN_DIR)/dataframe_performance_2: $(TARGET_LIB) $(DATAFRAME_PERFORMANCE_2_OBJ)
	$(CXX) -o $@ $(DATAFRAME_PERFORMANCE_2_OBJ) $(LIBS)

VECTORS_TESTER_OBJ = $(LOCAL_OBJ_DIR)/vectors_tester.o
$(LOCAL_BIN_DIR)/vectors_tester: $(TARGET_LIB) $(VECTORS_TESTER_OBJ)
	$(CXX) -o $@ $(VECTORS_TESTER_OBJ) $(LIBS)

ALLOCATOR_TESTER_OBJ = $(LOCAL_OBJ_DIR)/allocator_tester.o
$(LOCAL_BIN_DIR)/allocator_tester: $(TARGET_LIB) $(ALLOCATOR_TESTER_OBJ)
	$(CXX) -o $@ $(ALLOCATOR_TESTER_OBJ) $(LIBS)

LINKEDIN_BENCHMARK_OBJ = $(LOCAL_OBJ_DIR)/linkedin_benchmark.o
$(LOCAL_BIN_DIR)/linkedin_benchmark: $(TARGET_LIB) $(LINKEDIN_BENCHMARK_OBJ)
	$(CXX) -o $@ $(LINKEDIN_BENCHMARK_OBJ) $(LIBS)

DATAFRAME_READ_LARGE_FILE_OBJ = $(LOCAL_OBJ_DIR)/dataframe_read_large_file.o
$(LOCAL_BIN_DIR)/dataframe_read_large_file: $(TARGET_LIB) $(DATAFRAME_READ_LARGE_FILE_OBJ)
	$(CXX) -o $@ $(DATAFRAME_READ_LARGE_FILE_OBJ) $(LIBS)

VECTOR_PTR_VIEW_TESTER_OBJ = $(LOCAL_OBJ_DIR)/vector_ptr_view_tester.o
$(LOCAL_BIN_DIR)/vector_ptr_view_tester: $(TARGET_LIB) $(VECTOR_PTR_VIEW_TESTER_OBJ)
	$(CXX) -o $@ $(VECTOR_PTR_VIEW_TESTER_OBJ) $(LIBS)

META_PROG_OBJ = $(LOCAL_OBJ_DIR)/meta_prog_tester.o
$(LOCAL_BIN_DIR)/meta_prog_tester: $(TARGET_LIB) $(META_PROG_OBJ)
	$(CXX) -o $@ $(META_PROG_OBJ) $(LIBS)

DATE_TIME_TESTER_OBJ = $(LOCAL_OBJ_DIR)/date_time_tester.o
$(LOCAL_BIN_DIR)/date_time_tester: $(TARGET_LIB) $(DATE_TIME_TESTER_OBJ)
	$(CXX) -o $@ $(DATE_TIME_TESTER_OBJ) $(LIBS)

GEN_RAND_TESTER_OBJ = $(LOCAL_OBJ_DIR)/gen_rand_tester.o
$(LOCAL_BIN_DIR)/gen_rand_tester: $(TARGET_LIB) $(GEN_RAND_TESTER_OBJ)
	$(CXX) -o $@ $(GEN_RAND_TESTER_OBJ) $(LIBS)

MATRIX_TESTER_OBJ = $(LOCAL_OBJ_DIR)/matrix_tester.o
$(LOCAL_BIN_DIR)/matrix_tester: $(TARGET_LIB) $(MATRIX_TESTER_OBJ)
	$(CXX) -o $@ $(MATRIX_TESTER_OBJ) $(LIBS)

ISO_TREE_TESTER_OBJ = $(LOCAL_OBJ_DIR)/iso_tree_tester.o
$(LOCAL_BIN_DIR)/iso_tree_tester: $(TARGET_LIB) $(ISO_TREE_TESTER_OBJ)
	$(CXX) -o $@ $(ISO_TREE_TESTER_OBJ) $(LIBS)

# -----------------------------------------------------------------------------

depend:
	makedepend $(CXXFLAGS) -Y $(SRCS)

clean:
	rm -f $(LIB_OBJS) $(TARGETS) $(DATAFRAME_TESTER_OBJ) $(VECTORS_TESTER_OBJ) \
          $(DATE_TIME_TESTER_OBJ) $(VECTOR_PTR_VIEW_TESTER_OBJ) \
          $(GEN_RAND_TESTER_OBJ) $(META_PROG_OBJ) \
          $(DATAFRAME_PERFORMANCE_OBJ) $(DATAFRAME_TESTER_OBJ_2) \
          $(DATAFRAME_TESTER_OBJ_3) $(DATAFRAME_TESTER_OBJ_4) \
          $(HELLO_WORLD_OBJ) $(DATAFRAME_PERFORMANCE_2_OBJ) \
          $(DATAFRAME_THREAD_SAFTY_OBJ) $(DATAFRAME_TESTER_SCHEMA_OBJ) \
          $(ALLOCATOR_TESTER_OBJ) $(LINKEDIN_BENCHMARK_OBJ) \
          $(DATAFRAME_READ_LARGE_FILE_OBJ) $(MATRIX_TESTER_OBJ) \
          $(ISO_TREE_TESTER_OBJ) $(DATAFRAME_TESTER_OBJ_5)

clobber:
	rm -f $(LIB_OBJS) $(TARGETS) $(DATAFRAME_TESTER_OBJ) $(VECTORS_TESTER_OBJ) \
          $(DATE_TIME_TESTER_OBJ) $(VECTOR_PTR_VIEW_TESTER_OBJ) \
          $(GEN_RAND_TESTER_OBJ) $(DATAFRAME_PERFORMACE_OBJ) \
          $(DATAFRAME_TESTER_OBJ_2) $(DATAFRAME_THREAD_SAFTY_OBJ) \
          $(DATAFRAME_TESTER_OBJ_3) $(DATAFRAME_TESTER_OBJ_4) \
          $(HELLO_WORLD_OBJ) $(DATAFRAME_TESTER_SCHEMA_OBJ) \
          $(ALLOCATOR_TESTER_OBJ) $(DATAFRAME_PERFORMANCE_OBJ) \
          $(DATAFRAME_PERFORMANCE_2_OBJ) \
          $(META_PROG_OBJ) $(LINKEDIN_BENCHMARK_OBJ) \
          $(DATAFRAME_READ_LARGE_FILE_OBJ) $(MATRIX_TESTER_OBJ) \
          $(ISO_TREE_TESTER_OBJ) $(DATAFRAME_TESTER_OBJ_5)

install_lib:
	cp -pf $(TARGET_LIB) $(PROJECT_LIB_DIR)/.

install_hdr:
	cp -rpf $(LOCAL_INCLUDE_DIR)/* $(PROJECT_INCLUDE_DIR)/.

# -----------------------------------------------------------------------------

## Local Variables:
## mode:Makefile
## tab-width:4
## End:
