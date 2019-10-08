// Hossein Moein
// November 27, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <atomic>

#pragma once

// ----------------------------------------------------------------------------

namespace hmdf
{

struct  ThreadGranularity  {

    static inline void
    set_thread_level(unsigned int n)  { num_of_threads_ = n; }
    static inline unsigned int
    get_thread_level()  { return (num_of_threads_); }

protected:

    ThreadGranularity() = default;

private:

    static unsigned int num_of_threads_;
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

    SpinGuard (const SpinGuard &) = delete;
    SpinGuard &operator = (const SpinGuard &) = delete;

private:

    SpinLock    *lock_ { nullptr };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
