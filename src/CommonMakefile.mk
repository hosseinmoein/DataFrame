## Hossein Moein
## September 12 2017

LOCAL_LIB_DIR = ../lib/$(BUILD_PLATFORM)
LOCAL_BIN_DIR = ../bin/$(BUILD_PLATFORM)
LOCAL_OBJ_DIR = ../obj/$(BUILD_PLATFORM)
LOCAL_INCLUDE_DIR = ../include
PROJECT_LIB_DIR = ../../lib/$(BUILD_PLATFORM)
PROJECT_INCLUDE_DIR = ../../include

# -----------------------------------------------------------------------------

SRCS = HeteroVector.cc \
       dataframe_tester.cc \
       HeteroView.cc \
       vectors_tester.cc \
       date_time_tester.cc \
       ThreadGranularity.cc \
       DateTime.cc

HEADERS = $(LOCAL_INCLUDE_DIR)/HeteroVector.h \
          $(LOCAL_INCLUDE_DIR)/HeteroVector.tcc \
          $(LOCAL_INCLUDE_DIR)/HeteroView.h \
          $(LOCAL_INCLUDE_DIR)/HeteroView.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame.h \
          $(LOCAL_INCLUDE_DIR)/DataFrame.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame_misc.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame_set.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame_get.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame_read.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame_opt.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame_join.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame_shift.tcc \
          $(LOCAL_INCLUDE_DIR)/DataFrame_functors.h \
          $(LOCAL_INCLUDE_DIR)/DataFrameVisitors.h \
          $(LOCAL_INCLUDE_DIR)/VectorView.h \
          $(LOCAL_INCLUDE_DIR)/ThreadGranularity.h \
          $(LOCAL_INCLUDE_DIR)/DateTime.h

LIB_NAME = DataSci
TARGET_LIB = $(LOCAL_LIB_DIR)/lib$(LIB_NAME).a

TARGETS += $(TARGET_LIB) $(LOCAL_BIN_DIR)/dataframe_tester \
	   $(LOCAL_BIN_DIR)/vectors_tester $(LOCAL_BIN_DIR)/date_time_tester

# -----------------------------------------------------------------------------

LFLAGS += -Bstatic -L$(LOCAL_LIB_DIR) -L$(PROJECT_LIB_DIR)

LIBS = $(LFLAGS) -l$(LIB_NAME) -lDMScu $(PLATFORM_LIBS)
INCLUDES += -I. -I$(LOCAL_INCLUDE_DIR) -I$(PROJECT_INCLUDE_DIR)
DEFINES = -D_REENTRANT -DDMS_INCLUDE_SOURCE \
          -DP_THREADS -D_POSIX_PTHREAD_SEMANTICS -DDMS_$(BUILD_DEFINE)__

# -----------------------------------------------------------------------------

# object file
#
LIB_OBJS = $(LOCAL_OBJ_DIR)/HeteroVector.o \
           $(LOCAL_OBJ_DIR)/HeteroView.o \
           $(LOCAL_OBJ_DIR)/ThreadGranularity.o \
           $(LOCAL_OBJ_DIR)/DateTime.o

# -----------------------------------------------------------------------------

# set up C++ suffixes and relationship between .cc and .o files
#
.SUFFIXES: .cc

$(LOCAL_OBJ_DIR)/%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

.cc :
	$(CXX) $(CXXFLAGS) $< -o $@ -lm $(TLIB) -lg++
#	$(CXX) $(CXXFLAGS) $< -o $@ -lm $(TLIB)

# -----------------------------------------------------------------------------

all: PRE_BUILD $(TARGETS)

PRE_BUILD:
	mkdir -p $(LOCAL_LIB_DIR)
	mkdir -p $(LOCAL_BIN_DIR)
	mkdir -p $(LOCAL_OBJ_DIR)
	mkdir -p $(PROJECT_LIB_DIR)
	mkdir -p $(PROJECT_INCLUDE_DIR)

$(TARGET_LIB): $(LIB_OBJS)
	ar -clrs $(TARGET_LIB) $(LIB_OBJS)

DATAFRAME_TESTER_OBJ = $(LOCAL_OBJ_DIR)/dataframe_tester.o
$(LOCAL_BIN_DIR)/dataframe_tester: $(TARGET_LIB) $(DATAFRAME_TESTER_OBJ)
	$(CXX) -o $@ $(DATAFRAME_TESTER_OBJ) $(LIBS)

VECTORS_TESTER_OBJ = $(LOCAL_OBJ_DIR)/vectors_tester.o
$(LOCAL_BIN_DIR)/vectors_tester: $(TARGET_LIB) $(VECTORS_TESTER_OBJ)
	$(CXX) -o $@ $(VECTORS_TESTER_OBJ) $(LIBS)

DATE_TIME_TESTER_OBJ = $(LOCAL_OBJ_DIR)/date_time_tester.o
$(LOCAL_BIN_DIR)/date_time_tester: $(TARGET_LIB) $(DATE_TIME_TESTER_OBJ)
	$(CXX) -o $@ $(DATE_TIME_TESTER_OBJ) $(LIBS)

# -----------------------------------------------------------------------------

depend:
	makedepend $(CXXFLAGS) -Y $(SRCS)

clean:
	rm -f $(LIB_OBJS) $(TARGETS) $(DATAFRAME_TESTER_OBJ) $(VECTORS_TESTER_OBJ) \
          $(DATE_TIME_TESTER_OBJ)

clobber:
	rm -f $(LIB_OBJS) $(TARGETS) $(DATAFRAME_TESTER_OBJ) $(VECTORS_TESTER_OBJ) \
          $(DATE_TIME_TESTER_OBJ)

install_lib:
	cp -pf $(TARGET_LIB) $(PROJECT_LIB_DIR)/.

install_hdr:
	cp -pf $(HEADERS) $(PROJECT_INCLUDE_DIR)/.

# -----------------------------------------------------------------------------

## Local Variables:
## mode:Makefile
## tab-width:4
## End:
