## Hossein Moein
## July 17 2009

LOCAL_LIB_DIR = ../lib/$(BUILD_PLATFORM)
LOCAL_BIN_DIR = ../bin/$(BUILD_PLATFORM)
LOCAL_OBJ_DIR = ../obj/$(BUILD_PLATFORM)
LOCAL_INCLUDE_DIR = ../include
PROJECT_LIB_DIR = ../../../lib/$(BUILD_PLATFORM)
PROJECT_INCLUDE_DIR = ../../../include

# -----------------------------------------------------------------------------

SRCS = DMScu_CoreManager.cc \
       DMScu_NotificationManager.cc \
       DMScu_StrTokenizer.cc \
       DMScu_ConfigFileReader.cc \
       DMScu_String.cc \
       DMScu_PthreadAttrSettings.cc \
       DMScu_MutexManager.cc \
       DMScu_ThreadManagerBase.cc

HEADERS = $(LOCAL_INCLUDE_DIR)/DMScu_CompareOperators.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_MathOperators.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_CoreManager.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_FixedSizeString.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_String.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_HashMap.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_HashMap.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_HashSet.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_HashSet.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_HashUtils.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_NotificationManager.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_VectorRange.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_StepVectorRange.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_PtrVector.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_PtrVector.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_StrTokenizer.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_ConfigFileReader.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_SyncList.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_SyncList.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_Exception.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_ValueDescr.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_PthreadAttrSettings.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_MutexManager.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_ThreadManagerBase.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_ThreadManager.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_ThreadManager.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_SharedQueue.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_SharedQueue.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_TimerAlarm.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_TimerAlarm.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_LockFreeQueue.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_FASTProtocolUtilities.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_LockFreeQueue.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_ThreadPool.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_ThreadPool.tcc \
          $(LOCAL_INCLUDE_DIR)/DMScu_FileBase.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_FileDef.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_FixedSizeQueue.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_MMapBase.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_MMapFile.h \
          $(LOCAL_INCLUDE_DIR)/DMScu_MMapSharedMem.h

LIB_NAME = DMScu
TARGET_LIB = $(LOCAL_LIB_DIR)/lib$(LIB_NAME).a

TARGETS += $(TARGET_LIB) \
           $(LOCAL_BIN_DIR)/hmap_tester \
           $(LOCAL_BIN_DIR)/hset_tester \
           $(LOCAL_BIN_DIR)/core_tester \
           $(LOCAL_BIN_DIR)/fixsizestr_tester \
           $(LOCAL_BIN_DIR)/string_tester \
           $(LOCAL_BIN_DIR)/vec_range_tester \
           $(LOCAL_BIN_DIR)/ptrvec_tester \
           $(LOCAL_BIN_DIR)/strtoken_tester \
           $(LOCAL_BIN_DIR)/compareopt_tester \
           $(LOCAL_BIN_DIR)/mathopt_tester \
           $(LOCAL_BIN_DIR)/hash_perf_tester \
           $(LOCAL_BIN_DIR)/config_file_tester \
           $(LOCAL_BIN_DIR)/mutex_tester \
           $(LOCAL_BIN_DIR)/thread_tester \
           $(LOCAL_BIN_DIR)/sharedq_tester \
           $(LOCAL_BIN_DIR)/timer_tester \
           $(LOCAL_BIN_DIR)/lockfreeq_tester \
           $(LOCAL_BIN_DIR)/thrpool_tester \
           $(LOCAL_BIN_DIR)/filebase_tester \
           $(LOCAL_BIN_DIR)/fixedsizeq_tester \
           $(LOCAL_BIN_DIR)/mmfile_tester \
           $(LOCAL_BIN_DIR)/sharedmem_tester

# -----------------------------------------------------------------------------

LFLAGS += -Bstatic -L$(LOCAL_LIB_DIR) -L$(PROJECT_LIB_DIR)

LIBS = $(LFLAGS) -l$(LIB_NAME) $(PLATFORM_LIBS)
INCLUDES += -I. -I$(LOCAL_INCLUDE_DIR) -I$(PROJECT_INCLUDE_DIR)
DEFINES = -D_REENTRANT -DDMS_INCLUDE_SOURCE \
          -DP_THREADS -D_POSIX_PTHREAD_SEMANTICS -DDMS_$(BUILD_DEFINE)__

# -----------------------------------------------------------------------------

# object file
#
LIB_OBJS = $(LOCAL_OBJ_DIR)/DMScu_CoreManager.o \
           $(LOCAL_OBJ_DIR)/DMScu_NotificationManager.o \
           $(LOCAL_OBJ_DIR)/DMScu_StrTokenizer.o \
           $(LOCAL_OBJ_DIR)/DMScu_ConfigFileReader.o \
           $(LOCAL_OBJ_DIR)/DMScu_String.o \
           $(LOCAL_OBJ_DIR)/DMScu_PthreadAttrSettings.o \
           $(LOCAL_OBJ_DIR)/DMScu_MutexManager.o \
           $(LOCAL_OBJ_DIR)/DMScu_ThreadManagerBase.o \
           $(LOCAL_OBJ_DIR)/DMScu_FileBase.o \
           $(LOCAL_OBJ_DIR)/DMScu_MMapBase.o \
           $(LOCAL_OBJ_DIR)/DMScu_MMapFile.o \
           $(LOCAL_OBJ_DIR)/DMScu_MMapSharedMem.o

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

all: $(TARGETS)


$(TARGET_LIB): $(LIB_OBJS)
	ar -clrs $(TARGET_LIB) $(LIB_OBJS)

HMAP_TESTER_OBJ = $(LOCAL_OBJ_DIR)/hmap_tester.o
$(LOCAL_BIN_DIR)/hmap_tester: $(TARGET_LIB) $(HMAP_TESTER_OBJ)
	$(CXX) -o $@ $(HMAP_TESTER_OBJ) $(LIBS)

HSET_TESTER_OBJ = $(LOCAL_OBJ_DIR)/hset_tester.o
$(LOCAL_BIN_DIR)/hset_tester: $(TARGET_LIB) $(HSET_TESTER_OBJ)
	$(CXX) -o $@ $(HSET_TESTER_OBJ) $(LIBS)

CORE_TESTER_OBJ = $(LOCAL_OBJ_DIR)/core_tester.o
$(LOCAL_BIN_DIR)/core_tester: $(TARGET_LIB) $(CORE_TESTER_OBJ)
	$(CXX) -o $@ $(CORE_TESTER_OBJ) $(LIBS)

FIXSIZESTR_TESTER_OBJ = $(LOCAL_OBJ_DIR)/fixsizestr_tester.o
$(LOCAL_BIN_DIR)/fixsizestr_tester: $(TARGET_LIB) $(FIXSIZESTR_TESTER_OBJ)
	$(CXX) -o $@ $(FIXSIZESTR_TESTER_OBJ) $(LIBS)

STRING_TESTER_OBJ = $(LOCAL_OBJ_DIR)/string_tester.o
$(LOCAL_BIN_DIR)/string_tester: $(TARGET_LIB) $(STRING_TESTER_OBJ)
	$(CXX) -o $@ $(STRING_TESTER_OBJ) $(LIBS)

VEC_RANGE_TESTER_OBJ = $(LOCAL_OBJ_DIR)/vec_range_tester.o
$(LOCAL_BIN_DIR)/vec_range_tester: $(TARGET_LIB) $(VEC_RANGE_TESTER_OBJ)
	$(CXX) -o $@ $(VEC_RANGE_TESTER_OBJ) $(LIBS)

PTRVEC_TESTER_OBJ = $(LOCAL_OBJ_DIR)/ptrvec_tester.o
$(LOCAL_BIN_DIR)/ptrvec_tester: $(TARGET_LIB) $(PTRVEC_TESTER_OBJ)
	$(CXX) -o $@ $(PTRVEC_TESTER_OBJ) $(LIBS)

STRTOKEN_TESTER_OBJ = $(LOCAL_OBJ_DIR)/strtoken_tester.o
$(LOCAL_BIN_DIR)/strtoken_tester: $(TARGET_LIB) $(STRTOKEN_TESTER_OBJ)
	$(CXX) -o $@ $(STRTOKEN_TESTER_OBJ) $(LIBS)

COMPAREOPT_TESTER_OBJ = $(LOCAL_OBJ_DIR)/compareopt_tester.o
$(LOCAL_BIN_DIR)/compareopt_tester: $(TARGET_LIB) $(COMPAREOPT_TESTER_OBJ)
	$(CXX) -o $@ $(COMPAREOPT_TESTER_OBJ) $(LIBS)

MATHOPT_TESTER_OBJ = $(LOCAL_OBJ_DIR)/mathopt_tester.o
$(LOCAL_BIN_DIR)/mathopt_tester: $(TARGET_LIB) $(MATHOPT_TESTER_OBJ)
	$(CXX) -o $@ $(MATHOPT_TESTER_OBJ) $(LIBS)

HASH_PERF_TESTER_OBJ = $(LOCAL_OBJ_DIR)/hash_perf_tester.o
$(LOCAL_BIN_DIR)/hash_perf_tester: $(TARGET_LIB) $(HASH_PERF_TESTER_OBJ)
	$(CXX) -o $@ $(HASH_PERF_TESTER_OBJ) $(LIBS)

CONFIG_FILE_TESTER_OBJ = $(LOCAL_OBJ_DIR)/config_file_tester.o
$(LOCAL_BIN_DIR)/config_file_tester: $(TARGET_LIB) $(CONFIG_FILE_TESTER_OBJ)
	$(CXX) -o $@ $(CONFIG_FILE_TESTER_OBJ) $(LIBS)

MUTEX_TESTER_OBJ = $(LOCAL_OBJ_DIR)/mutex_tester.o
$(LOCAL_BIN_DIR)/mutex_tester: $(TARGET_LIB) $(MUTEX_TESTER_OBJ)
	$(CXX) -o $@ $(MUTEX_TESTER_OBJ) $(LIBS)

THREAD_TESTER_OBJ = $(LOCAL_OBJ_DIR)/thread_tester.o
$(LOCAL_BIN_DIR)/thread_tester: $(TARGET_LIB) $(THREAD_TESTER_OBJ)
	$(CXX) -o $@ $(THREAD_TESTER_OBJ) $(LIBS)

SHAREDQ_TESTER_OBJ = $(LOCAL_OBJ_DIR)/sharedq_tester.o
$(LOCAL_BIN_DIR)/sharedq_tester: $(TARGET_LIB) $(SHAREDQ_TESTER_OBJ)
	$(CXX) -o $@ $(SHAREDQ_TESTER_OBJ) $(LIBS)

TIMER_TESTER_OBJ = $(LOCAL_OBJ_DIR)/timer_tester.o
$(LOCAL_BIN_DIR)/timer_tester: $(TARGET_LIB) $(TIMER_TESTER_OBJ)
	$(CXX) -o $@ $(TIMER_TESTER_OBJ) $(LIBS)

LOCKFREEQ_TESTER_OBJ = $(LOCAL_OBJ_DIR)/lockfreeq_tester.o
$(LOCAL_BIN_DIR)/lockfreeq_tester: $(TARGET_LIB) $(LOCKFREEQ_TESTER_OBJ)
	$(CXX) -o $@ $(LOCKFREEQ_TESTER_OBJ) $(LIBS)

THRPOOL_TESTER_OBJ = $(LOCAL_OBJ_DIR)/thrpool_tester.o
$(LOCAL_BIN_DIR)/thrpool_tester: $(TARGET_LIB) $(THRPOOL_TESTER_OBJ)
	$(CXX) -o $@ $(THRPOOL_TESTER_OBJ) $(LIBS)

FILEBASE_TESTER_OBJ = $(LOCAL_OBJ_DIR)/filebase_tester.o
$(LOCAL_BIN_DIR)/filebase_tester: $(TARGET_LIB) $(FILEBASE_TESTER_OBJ)
	$(CXX) -o $@ $(FILEBASE_TESTER_OBJ) $(LIBS)

FIXEDSIZEQ_TESTER_OBJ = $(LOCAL_OBJ_DIR)/fixedsizeq_tester.o
$(LOCAL_BIN_DIR)/fixedsizeq_tester: $(TARGET_LIB) $(FIXEDSIZEQ_TESTER_OBJ)
	$(CXX) -o $@ $(FIXEDSIZEQ_TESTER_OBJ) $(LIBS)

MMFILE_TESTER_OBJ = $(LOCAL_OBJ_DIR)/mmfile_tester.o
$(LOCAL_BIN_DIR)/mmfile_tester: $(TARGET_LIB) $(MMFILE_TESTER_OBJ)
	$(CXX) -o $@ $(MMFILE_TESTER_OBJ) $(LIBS)

SHAREDMEM_TESTER_OBJ = $(LOCAL_OBJ_DIR)/sharedmem_tester.o
$(LOCAL_BIN_DIR)/sharedmem_tester: $(TARGET_LIB) $(SHAREDMEM_TESTER_OBJ)
	$(CXX) -o $@ $(SHAREDMEM_TESTER_OBJ) $(LIBS)

# -----------------------------------------------------------------------------

depend:
	makedepend $(CXXFLAGS) -Y $(SRCS)

clean:
	rm -f $(LIB_OBJS)

clobber:
	rm -f $(LIB_OBJS) $(TARGETS) $(HMAP_TESTER_OBJ) $(HSET_TESTER_OBJ) \
          $(CORE_TESTER_OBJ) $(FIXSIZESTR_TESTER_OBJ) \
          $(STRING_TESTER_OBJ) $(VEC_RANGE_TESTER_OBJ)  \
          $(PTRVEC_TESTER_OBJ) $(STRSTR_TESTER_OBJ) \
          $(STRTOKEN_TESTER_OBJ) $(COMPAREOPT_TESTER_OBJ) \
          $(MATHOPT_TESTER_OBJ) $(HASH_PERF_TESTER_OBJ) \
          $(CONFIG_FILE_TESTER_OBJ) $(MUTEX_TESTER_OBJ) \
          $(THREAD_TESTER_OBJ) $(SHAREDQ_TESTER_OBJ) \
          $(TIMER_TESTER_OBJ) $(LOCKFREEQ_TESTER_OBJ) \
          $(THRPOOL_TESTER_OBJ) $(FILEBASE_TESTER_OBJ) \
          $(FIXEDSIZEQ_TESTER_OBJ) $(FIXEDSIZEQ_TESTER_OBJ) \
          $(MMFILE_TESTER_OBJ) $(SHAREDMEM_TESTER_OBJ)

install_lib:
	cp -pf $(TARGET_LIB) $(PROJECT_LIB_DIR)/.

install_hdr:
	cp -pf $(HEADERS) $(PROJECT_INCLUDE_DIR)/.

# -----------------------------------------------------------------------------

## Local Variables:
## mode:Makefile
## tab-width:4
## End:
