## Hossein Moein
## July 17 2009

LOCAL_LIB_DIR = ../lib/$(BUILD_PLATFORM)
LOCAL_BIN_DIR = ../bin/$(BUILD_PLATFORM)
LOCAL_OBJ_DIR = ../obj/$(BUILD_PLATFORM)
LOCAL_INCLUDE_DIR = ../include
PROJECT_LIB_DIR = ../../../lib/$(BUILD_PLATFORM)
PROJECT_INCLUDE_DIR = ../../../include

# -----------------------------------------------------------------------------

SRCS =

HEADERS = $(LOCAL_INCLUDE_DIR)/DMScu_FixedSizeString.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_Exception.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_FileBase.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_FileDef.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_MMapBase.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_MMapFile.h

LIB_NAME = DMScu
TARGET_LIB = $(LOCAL_LIB_DIR)/lib$(LIB_NAME).a

TARGETS += $(TARGET_LIB) \
           $(LOCAL_BIN_DIR)/fixsizestr_tester \
           $(LOCAL_BIN_DIR)/filebase_tester \
           $(LOCAL_BIN_DIR)/mmfile_tester

# -----------------------------------------------------------------------------

LFLAGS += -Bstatic -L$(LOCAL_LIB_DIR) -L$(PROJECT_LIB_DIR)

LIBS = $(LFLAGS) -l$(LIB_NAME) $(PLATFORM_LIBS)
INCLUDES += -I. -I$(LOCAL_INCLUDE_DIR) -I$(PROJECT_INCLUDE_DIR)
DEFINES = -D_REENTRANT -DDMS_INCLUDE_SOURCE \
          -DP_THREADS -D_POSIX_PTHREAD_SEMANTICS -DDMS_$(BUILD_DEFINE)__

# -----------------------------------------------------------------------------

# object file
#
LIB_OBJS = $(LOCAL_OBJ_DIR)/DMScu_FileBase.o \
           $(LOCAL_OBJ_DIR)/DMScu_MMapBase.o \
           $(LOCAL_OBJ_DIR)/DMScu_MMapFile.o

# -----------------------------------------------------------------------------

# set up C++ suffixes and relationship between .cc and .o files
#
.SUFFIXES: .cc

$(LOCAL_OBJ_DIR)/%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

.cc :
	$(CXX) $(CXXFLAGS) $< -o $@ -lm $(TLIB) -lg++

# -----------------------------------------------------------------------------

all: $(TARGETS)


$(TARGET_LIB): $(LIB_OBJS)
	ar -clrs $(TARGET_LIB) $(LIB_OBJS)

FIXSIZESTR_TESTER_OBJ = $(LOCAL_OBJ_DIR)/fixsizestr_tester.o
$(LOCAL_BIN_DIR)/fixsizestr_tester: $(TARGET_LIB) $(FIXSIZESTR_TESTER_OBJ)
	$(CXX) -o $@ $(FIXSIZESTR_TESTER_OBJ) $(LIBS)

FILEBASE_TESTER_OBJ = $(LOCAL_OBJ_DIR)/filebase_tester.o
$(LOCAL_BIN_DIR)/filebase_tester: $(TARGET_LIB) $(FILEBASE_TESTER_OBJ)
	$(CXX) -o $@ $(FILEBASE_TESTER_OBJ) $(LIBS)

MMFILE_TESTER_OBJ = $(LOCAL_OBJ_DIR)/mmfile_tester.o
$(LOCAL_BIN_DIR)/mmfile_tester: $(TARGET_LIB) $(MMFILE_TESTER_OBJ)
	$(CXX) -o $@ $(MMFILE_TESTER_OBJ) $(LIBS)

# -----------------------------------------------------------------------------

depend:
	makedepend $(CXXFLAGS) -Y $(SRCS)

clean:
	rm -f $(LIB_OBJS)

clobber:
	rm -f $(LIB_OBJS) $(TARGETS) $(FIXSIZESTR_TESTER_OBJ) \
          $(FILEBASE_TESTER_OBJ) $(MMFILE_TESTER_OBJ)

install_lib:
	cp -pf $(TARGET_LIB) $(PROJECT_LIB_DIR)/.

install_hdr:
	cp -pf $(HEADERS) $(PROJECT_INCLUDE_DIR)/.

# -----------------------------------------------------------------------------

## Local Variables:
## mode:Makefile
## tab-width:4
## End:
