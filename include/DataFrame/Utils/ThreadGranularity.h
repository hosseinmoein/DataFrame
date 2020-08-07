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

#pragma once

#if defined(_WIN32) && defined(HMDF_SHARED)
#  ifdef LIBRARY_EXPORTS
#    define LIBRARY_API __declspec(dllexport)
#  else
#    define LIBRARY_API __declspec(dllimport)
#  endif // LIBRARY_EXPORTS
#  ifdef min
#    undef min
#  endif // min
#  ifdef max
#    undef max
#  endif // max
#else
#  define LIBRARY_API
#endif // _WIN32

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

    static unsigned int         num_of_threads_;
    static const unsigned int   supported_threads_;
};

// ----------------------------------------------------------------------------

struct  SpinLock  {

    SpinLock () = default;
    ~SpinLock() = default;

    inline void
    lock() noexcept { while (lock_.test_and_set(std::memory_order_acquire)) ; }
    inline bool try_lock() noexcept {

        return (! lock_.test_and_set(std::memory_order_acquire));
    }
    inline void unlock() noexcept { lock_.clear(std::memory_order_release); }

    SpinLock (const SpinLock &) = delete;
    SpinLock &operator = (const SpinLock &) = delete;

private:

    std::atomic_flag    lock_ = ATOMIC_FLAG_INIT;
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
