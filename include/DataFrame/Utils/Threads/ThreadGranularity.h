// Hossein Moein
// November 27, 2018
/*
Copyright (c) 2019-2026, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#pragma once

#include <DataFrame/Utils/Threads/ThreadPool.h>

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <thread>

// Lightweight CPU pause hint — reduces pipeline flushes in spin loops on
// hyperthreaded x86 and signals to the CPU that this is a spin-wait.
#if defined(_MSC_VER) || defined(__INTEL_COMPILER)
#  include <intrin.h>
#  define HMDF_CPU_PAUSE() _mm_pause()
#elif defined(__x86_64__) || defined(__i386__)
#  define HMDF_CPU_PAUSE() __builtin_ia32_pause()
#else
#  define HMDF_CPU_PAUSE() std::this_thread::yield()
#endif

// Software prefetch hints for read and read-for-write access.
// Pass the raw pointer of the element you want loaded into L1/L2 cache
// before it is needed.  On unsupported platforms the macro expands to a
// no-op cast so it compiles cleanly everywhere.
//
//   HMDF_PREFETCH_R(ptr)  — hint that *ptr will be read soon
//   HMDF_PREFETCH_W(ptr)  — hint that *ptr will be written soon
//                           (requests exclusive ownership of the cache line,
//                            avoiding an invalidation round-trip on write)
//
// Prefetch distance: issue the prefetch HMDF_PF_DIST iterations ahead of the
// current loop index.  16 elements covers two 64-byte cache lines for 8-byte
// types (double / int64), which matches typical L2 fill latency (~30 ns).
#if defined(_MSC_VER) || defined(__INTEL_COMPILER)
#  define HMDF_PREFETCH_R(ptr)  _mm_prefetch((const char *)(ptr), _MM_HINT_T0)
#  define HMDF_PREFETCH_W(ptr)  _mm_prefetch((const char *)(ptr), _MM_HINT_T0)
#elif defined(__GNUC__) || defined(__clang__)
#  define HMDF_PREFETCH_R(ptr)  __builtin_prefetch((ptr), 0, 3)
#  define HMDF_PREFETCH_W(ptr)  __builtin_prefetch((ptr), 1, 3)
#else
#  define HMDF_PREFETCH_R(ptr)  ((void)(ptr))
#  define HMDF_PREFETCH_W(ptr)  ((void)(ptr))
#endif

static constexpr std::size_t    HMDF_PF_DIST = 16;

// ----------------------------------------------------------------------------

namespace hmdf
{

struct  ThreadGranularity  {

    using size_type = ThreadPool::size_type;

    static inline void set_thread_level(size_type n)  {

        thr_pool_.add_thread(n - thr_pool_.capacity_threads());
    }
    static inline void set_optimum_thread_level()  {

        set_thread_level(std::thread::hardware_concurrency());
    }
    static inline size_type get_thread_level()  {

        return (thr_pool_.capacity_threads());
    }

    // By defaut, there are no threads
    //
    inline static ThreadPool    thr_pool_ { 0 };

protected:

    ThreadGranularity() = default;
};

// ----------------------------------------------------------------------------

struct  SpinLock  {

    SpinLock () = default;
    ~SpinLock() = default;

    inline void lock() noexcept {

        const std::thread::id   thr_id = std::this_thread::get_id();

        if (thr_id != owner_) [[likely]]  {
#ifdef __cpp_lib_atomic_flag_test
            while (true) {
                if (! lock_.test_and_set(std::memory_order_acquire)) break;
                // TTAS inner loop: spin on plain test() (no bus-lock) until
                // the lock is released, yielding to the CPU each iteration.
                while (lock_.test(std::memory_order_relaxed))
                    HMDF_CPU_PAUSE();
            }
#else
            while (lock_.test_and_set(std::memory_order_acquire))
                HMDF_CPU_PAUSE();
#endif // __cpp_lib_atomic_flag_test
            owner_ = thr_id;
        }

        count_ += 1;
    }
    [[nodiscard]] inline bool try_lock() noexcept {

        const std::thread::id   thr_id = std::this_thread::get_id();

        if (thr_id == owner_ ||
            ! lock_.test_and_set(std::memory_order_acquire))  {
            owner_ = thr_id;
            count_ += 1;
        }

        return (thr_id == owner_);
    }
    inline void unlock() noexcept {

        const std::thread::id   thr_id = std::this_thread::get_id();

        if (thr_id == owner_) [[likely]]  {
            count_ -= 1;

            assert(count_ >= 0);
            if (count_ == 0)  {
                owner_ = std::thread::id { };
                lock_.clear(std::memory_order_release);
            }
        }
        else  std::abort();
    }

    SpinLock (const SpinLock &) = delete;
    SpinLock &operator = (const SpinLock &) = delete;

private:

    std::atomic_flag    lock_ = ATOMIC_FLAG_INIT;
    std::thread::id     owner_ { };
    int                 count_ { 0 };
};

// ----------------------------------------------------------------------------

struct  SpinGuard  {

    inline explicit
    SpinGuard(SpinLock *l) noexcept : lock_(l)  { if (lock_)  lock_->lock(); }
    inline ~SpinGuard() noexcept  { if (lock_)  lock_->unlock(); }

    inline void release() noexcept  {

        if (lock_)  {
            lock_->unlock();
            lock_ = nullptr;
        }
    }

    SpinGuard () = delete;
    SpinGuard (const SpinGuard &) = delete;
    SpinGuard &operator = (const SpinGuard &) = delete;

private:

    SpinLock    *lock_;
};

// ----------------------------------------------------------------------------

// This is a lock-free and wait-free synchronization that allows consistent
// reads and writes from multiple threads.
// Single producer, multiple consumers.
//
template<typename T>
struct  SeqLock  {

    using value_type = T;
    using size_type = std::size_t;

    SeqLock() = default;
    inline explicit SeqLock (const value_type &value) : value_(value)  {   }

    // There can be only a single producer at a time
    //
    void store(const value_type &value) noexcept  {

        const size_type seq_0 = seq_.load (std::memory_order_relaxed);

        seq_.store (seq_0 + 1, std::memory_order_release);
        value_ = value;
        seq_.store (seq_0 + 2, std::memory_order_release);
    }

    // There can be multiple consumers concurrently
    //
    [[nodiscard]] value_type load() const noexcept  {

        while (true)  {
            const size_type seq_1 = seq_.load (std::memory_order_relaxed);

            if (! (seq_1 & 0x01)) [[likely]]  {
                std::atomic_thread_fence (std::memory_order_acquire);

                const value_type    copy = value_;

                std::atomic_thread_fence (std::memory_order_acquire);
                if (seq_1 == seq_.load (std::memory_order_relaxed)) [[likely]]
                    return (copy);
            }
            // A write is in progress (odd sequence); pause before retrying
            // to avoid burning memory bandwidth on the shared cache line.
            HMDF_CPU_PAUSE();
        }
    }

    SeqLock (const SeqLock &) = delete;
    SeqLock &operator = (const SeqLock &) = delete;
    SeqLock (SeqLock &&) = delete;
    SeqLock &operator = (SeqLock &&) = delete;

private:

    value_type          value_ { };
    std::atomic_size_t  seq_ { 0 };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
