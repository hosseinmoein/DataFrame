## Hossein Moein
## September 12 2017

LOCAL_LIB_DIR = ../lib/$(BUILD_PLATFORM)
LOCAL_BIN_DIR = ../bin/$(BUILD_PLATFORM)
LOCAL_OBJ_DIR = ../obj/$(BUILD_PLATFORM)
LOCAL_INCLUDE_DIR = ../include
PROJECT_LIB_DIR = ../../lib/$(BUILD_PLATFORM)
PROJECT_INCLUDE_DIR = ../../include

# -----------------------------------------------------------------------------

SRCS = HeteroVector.cc datasci_tester.cc HeteroView.cc

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
          $(LOCAL_INCLUDE_DIR)/DFVisitors.h

LIB_NAME = DataSci
TARGET_LIB = $(LOCAL_LIB_DIR)/lib$(LIB_NAME).a

TARGETS += $(TARGET_LIB) $(LOCAL_BIN_DIR)/datasci_tester

# -----------------------------------------------------------------------------

LFLAGS += -Bstatic -L$(LOCAL_LIB_DIR) -L$(PROJECT_LIB_DIR)

LIBS = $(LFLAGS) -l$(LIB_NAME) -lDMScu $(PLATFORM_LIBS)
INCLUDES += -I. -I$(LOCAL_INCLUDE_DIR) -I$(PROJECT_INCLUDE_DIR)
DEFINES = -D_REENTRANT -DDMS_INCLUDE_SOURCE \
          -DP_THREADS -D_POSIX_PTHREAD_SEMANTICS -DDMS_$(BUILD_DEFINE)__

# -----------------------------------------------------------------------------

# object file
#
LIB_OBJS = $(LOCAL_OBJ_DIR)/HeteroVector.o $(LOCAL_OBJ_DIR)/HeteroView.o

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

DATASCI_TESTER_OBJ = $(LOCAL_OBJ_DIR)/datasci_tester.o
$(LOCAL_BIN_DIR)/datasci_tester: $(TARGET_LIB) $(DATASCI_TESTER_OBJ)
	$(CXX) -o $@ $(DATASCI_TESTER_OBJ) $(LIBS)

# -----------------------------------------------------------------------------

depend:
	makedepend $(CXXFLAGS) -Y $(SRCS)

clean:
	rm -f $(LIB_OBJS) $(TARGETS) $(DATASCI_TESTER_OBJ)

clobber:
	rm -f $(LIB_OBJS) $(TARGETS) $(DATASCI_TESTER_OBJ)

install_lib:
	cp -pf $(TARGET_LIB) $(PROJECT_LIB_DIR)/.

install_hdr:
	cp -pf $(HEADERS) $(PROJECT_INCLUDE_DIR)/.

# -----------------------------------------------------------------------------

## Local Variables:
## mode:Makefile
## tab-width:4
## End:
