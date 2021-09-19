// Hossein Moein
// November 27, 2018
/*
Copyright (c) 2019-2022, Hossein Moein
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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <thread>

#pragma once

#if defined(_WIN32) || defined(_WIN64)
#  ifdef _MSC_VER
#    ifdef LIBRARY_EXPORTS
#      define LIBRARY_API __declspec(dllexport)
#    else
#      define LIBRARY_API __declspec(dllimport)
#    endif // LIBRARY_EXPORTS
#  else
#    define LIBRARY_API
#  endif // _MSC_VER
#  ifdef min
#    undef min
#  endif // min
#  ifdef max
#    undef max
#  endif // max
#else
#  define LIBRARY_API
#endif // _WIN32 || _WIN64

// ----------------------------------------------------------------------------

namespace hmdf
{

struct LIBRARY_API ThreadGranularity {

    static inline void
    set_thread_level(unsigned int n)  { num_of_threads_ = n; }
    static inline unsigned int
    get_thread_level()  { return (num_of_threads_); }
    static inline unsigned int
    get_supported_thread()  { return (supported_threads_); }

    static inline unsigned int
    get_sensible_thread_level()  {

        return (supported_threads_ != 0
                    ? std::min(supported_threads_, num_of_threads_)
                    : num_of_threads_);
    }

protected:

    ThreadGranularity() = default;

private:

    inline static unsigned int          num_of_threads_ { 0 };
    inline static const unsigned int    supported_threads_ {
        std::thread::hardware_concurrency() };
};

// ----------------------------------------------------------------------------

struct  SpinLock  {

    SpinLock () = default;
    ~SpinLock() = default;

    inline void lock() noexcept {

        const std::thread::id   thr_id = std::this_thread::get_id();

        if (thr_id == owner_)
            count_ += 1;
        else  {
#ifdef __cpp_lib_atomic_flag_test
            while (true) {
                if (! lock_.test_and_set(std::memory_order_acquire)) break;
                while (lock_.test(std::memory_order_relaxed)) ;
            }
#else
            while (lock_.test_and_set(std::memory_order_acquire)) ;
#endif // __cpp_lib_atomic_flag_test
            owner_ = thr_id;
            count_ += 1;
        }
    }
    inline bool try_lock() noexcept {

        const std::thread::id   thr_id = std::this_thread::get_id();

        if (thr_id == owner_)  {
            count_ += 1;
            return (true);
        }
        else if (! lock_.test_and_set(std::memory_order_acquire))  {
            owner_ = thr_id;
            count_ += 1;
            return (true);
        }
        return (false);
    }
    inline void unlock() noexcept {

        const std::thread::id   thr_id = std::this_thread::get_id();

        if (thr_id == owner_)  {
            count_ -= 1;

            assert(count_ >= 0);
            if (count_ == 0)  {
                owner_ = std::thread::id { };
                lock_.clear(std::memory_order_release);
            }
        }
        else  assert(0);
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

    inline
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
