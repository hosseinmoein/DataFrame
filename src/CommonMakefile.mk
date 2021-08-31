## Hossein Moein
## September 12 2017

LOCAL_LIB_DIR = ../lib/$(BUILD_PLATFORM)
LOCAL_BIN_DIR = ../bin/$(BUILD_PLATFORM)
LOCAL_OBJ_DIR = ../obj/$(BUILD_PLATFORM)
LOCAL_INCLUDE_DIR = ../include
PROJECT_LIB_DIR = ../../lib/$(BUILD_PLATFORM)
PROJECT_INCLUDE_DIR = ../../include

# -----------------------------------------------------------------------------

SRCS = Vectors/HeteroVector.cc \
       ../test/dataframe_tester.cc \
       ../test/dataframe_tester_2.cc \
       ../test/dataframe_tester_3.cc \
       ../test/hello_world.cc \
       ../test/dataframe_thread_safety.cc \
       ../test/dataframe_performance.cc \
       ../test/dataframe_performance_2.cc \
       Vectors/HeteroView.cc \
       Vectors/HeteroPtrView.cc \
       ../test/vectors_tester.cc \
       ../test/vector_ptr_view_tester.cc \
       ../test/date_time_tester.cc \
       ../test/gen_rand_tester.cc \
       Utils/DateTime.cc

HEADERS = $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroVector.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroVector.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroView.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroView.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroPtrView.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Vectors/HeteroPtrView.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/DataFrame.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_misc.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_standalone.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_set.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_get.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_visit.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_read.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_write.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_opt.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_join.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Internals/DataFrame_shift.tcc \
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
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/ThreadGranularity.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/DateTime.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/Utils.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/FixedSizeString.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame/Utils/FixedSizePriorityQueue.h

LIB_NAME = DataFrame
TARGET_LIB = $(LOCAL_LIB_DIR)/lib$(LIB_NAME).a

TARGETS += $(TARGET_LIB) \
           $(LOCAL_BIN_DIR)/dataframe_tester \
           $(LOCAL_BIN_DIR)/dataframe_tester_2 \
           $(LOCAL_BIN_DIR)/dataframe_tester_3 \
           $(LOCAL_BIN_DIR)/hello_world \
           $(LOCAL_BIN_DIR)/dataframe_thread_safety \
           $(LOCAL_BIN_DIR)/dataframe_performance \
           $(LOCAL_BIN_DIR)/dataframe_performance_2 \
           $(LOCAL_BIN_DIR)/vectors_tester \
           $(LOCAL_BIN_DIR)/vector_ptr_view_tester \
           $(LOCAL_BIN_DIR)/date_time_tester \
           $(LOCAL_BIN_DIR)/gen_rand_tester

# -----------------------------------------------------------------------------

LFLAGS += -Bstatic -L$(LOCAL_LIB_DIR) -L$(PROJECT_LIB_DIR)

LIBS = $(LFLAGS) -l$(LIB_NAME) $(PLATFORM_LIBS)
INCLUDES += -I. -I$(LOCAL_INCLUDE_DIR) -I$(PROJECT_INCLUDE_DIR)
DEFINES = -Wall -D_REENTRANT -DHMDF_HAVE_CLOCK_GETTIME \
          -DP_THREADS -D_POSIX_PTHREAD_SEMANTICS -DDMS_$(BUILD_DEFINE)__

# -----------------------------------------------------------------------------

# object file
#
LIB_OBJS = $(LOCAL_OBJ_DIR)/HeteroVector.o \
           $(LOCAL_OBJ_DIR)/HeteroView.o \
           $(LOCAL_OBJ_DIR)/HeteroPtrView.o \
           $(LOCAL_OBJ_DIR)/DateTime.o

# -----------------------------------------------------------------------------

# set up C++ suffixes and relationship between .cc and .o files
#
.SUFFIXES: .cc

$(LOCAL_OBJ_DIR)/%.o: %.cc
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
	ar -clrs $(TARGET_LIB) $(LIB_OBJS)

DATAFRAME_TESTER_OBJ = $(LOCAL_OBJ_DIR)/dataframe_tester.o
$(LOCAL_BIN_DIR)/dataframe_tester: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ) $(LIBS)

DATAFRAME_TESTER_OBJ_2 = $(LOCAL_OBJ_DIR)/dataframe_tester_2.o
$(LOCAL_BIN_DIR)/dataframe_tester_2: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ_2)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ_2) $(LIBS)

DATAFRAME_TESTER_OBJ_3 = $(LOCAL_OBJ_DIR)/dataframe_tester_3.o
$(LOCAL_BIN_DIR)/dataframe_tester_3: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ_3)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ_3) $(LIBS)

HELLO_WORLD_OBJ = $(LOCAL_OBJ_DIR)/hello_world.o
$(LOCAL_BIN_DIR)/hello_world: $(TARGET_LIB) $(HELLO_WORLD_OBJ)
	$(CXX) -o $@ $(HELLO_WORLD_OBJ) $(LIBS)

DATAFRAME_THREAD_SAFTY_OBJ = $(LOCAL_OBJ_DIR)/dataframe_thread_safety.o
$(LOCAL_BIN_DIR)/dataframe_thread_safety: $(TARGET_LIB) $(DATAFRAME_THREAD_SAFTY_OBJ)
	$(CXX) -o $@ $(DATAFRAME_THREAD_SAFTY_OBJ) $(LIBS)

DATAFRAME_PERFORMANCE_OBJ = $(LOCAL_OBJ_DIR)/dataframe_performance.o
$(LOCAL_BIN_DIR)/dataframe_performance: $(TARGET_LIB) $(DATAFRAME_PERFORMANCE_OBJ)
	$(CXX) -o $@ $(DATAFRAME_PERFORMANCE_OBJ) $(LIBS)

DATAFRAME_PERFORMANCE_2_OBJ = $(LOCAL_OBJ_DIR)/dataframe_performance_2.o
$(LOCAL_BIN_DIR)/dataframe_performance_2: $(TARGET_LIB) $(DATAFRAME_PERFORMANCE_2_OBJ)
	$(CXX) -o $@ $(DATAFRAME_PERFORMANCE_2_OBJ) $(LIBS)

VECTORS_TESTER_OBJ = $(LOCAL_OBJ_DIR)/vectors_tester.o
$(LOCAL_BIN_DIR)/vectors_tester: $(TARGET_LIB) $(VECTORS_TESTER_OBJ)
	$(CXX) -o $@ $(VECTORS_TESTER_OBJ) $(LIBS)

VECTOR_PTR_VIEW_TESTER_OBJ = $(LOCAL_OBJ_DIR)/vector_ptr_view_tester.o
$(LOCAL_BIN_DIR)/vector_ptr_view_tester: $(TARGET_LIB) $(VECTOR_PTR_VIEW_TESTER_OBJ)
	$(CXX) -o $@ $(VECTOR_PTR_VIEW_TESTER_OBJ) $(LIBS)

DATE_TIME_TESTER_OBJ = $(LOCAL_OBJ_DIR)/date_time_tester.o
$(LOCAL_BIN_DIR)/date_time_tester: $(TARGET_LIB) $(DATE_TIME_TESTER_OBJ)
	$(CXX) -o $@ $(DATE_TIME_TESTER_OBJ) $(LIBS)

GEN_RAND_TESTER_OBJ = $(LOCAL_OBJ_DIR)/gen_rand_tester.o
$(LOCAL_BIN_DIR)/gen_rand_tester: $(TARGET_LIB) $(GEN_RAND_TESTER_OBJ)
	$(CXX) -o $@ $(GEN_RAND_TESTER_OBJ) $(LIBS)

# -----------------------------------------------------------------------------

depend:
	makedepend $(CXXFLAGS) -Y $(SRCS)

clean:
	rm -f $(LIB_OBJS) $(TARGETS) $(DATAFRAME_TESTER_OBJ) $(VECTORS_TESTER_OBJ) \
          $(DATE_TIME_TESTER_OBJ) $(VECTOR_PTR_VIEW_TESTER_OBJ) \
          $(GEN_RAND_TESTER_OBJ) \
          $(DATAFRAME_PERFORMANCE_OBJ) $(DATAFRAME_TESTER_OBJ_2) \
          $(DATAFRAME_TESTER_OBJ_3) $(HELLO_WORLD_OBJ) \
          $(DATAFRAME_PERFORMANCE_2_OBJ) $(DATAFRAME_THREAD_SAFTY_OBJ)

clobber:
	rm -f $(LIB_OBJS) $(TARGETS) $(DATAFRAME_TESTER_OBJ) $(VECTORS_TESTER_OBJ) \
          $(DATE_TIME_TESTER_OBJ) $(VECTOR_PTR_VIEW_TESTER_OBJ) \
          $(GEN_RAND_TESTER_OBJ) $(DATAFRAME_PERFORMACE_OBJ) \
          $(DATAFRAME_TESTER_OBJ_2) $(DATAFRAME_THREAD_SAFTY_OBJ) \
          $(DATAFRAME_TESTER_OBJ_3) $(HELLO_WORLD_OBJ)

install_lib:
	cp -pf $(TARGET_LIB) $(PROJECT_LIB_DIR)/.

install_hdr:
	cp -rpf $(LOCAL_INCLUDE_DIR)/* $(PROJECT_INCLUDE_DIR)/.

# -----------------------------------------------------------------------------

## Local Variables:
## mode:Makefile
## tab-width:4
## End:
