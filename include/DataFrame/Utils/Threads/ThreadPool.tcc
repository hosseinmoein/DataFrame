// Hossein Moein
// August 9, 2023
/*
Copyright (c) 2023-2028, Hossein Moein
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

#include <DataFrame/Utils/Threads/ThreadPool.h>

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <memory>
#include <stdexcept>
#include <type_traits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename F, typename ... As>
requires std::invocable<F, As ...>
Conditioner::Conditioner(F &&routine, As && ... args)
    : func_([&] () -> void { routine(std::forward<As>(args) ...); })  {   }

// ----------------------------------------------------------------------------

void Conditioner::execute()  { func_(); }

// ----------------------------------------------------------------------------

ThreadPool::ThreadPool(size_type thr_num,
                       Conditioner pre_conditioner,
                       Conditioner post_conditioner)
    : pre_conditioner_(pre_conditioner), post_conditioner_(post_conditioner)  {

    threads_.reserve(thr_num * 2);
    for (size_type i = 0; i < thr_num; ++i)  {
        local_queues_.push_back(LocalQueueType { });
        threads_.emplace_back(&ThreadPool::thread_routine_, this, i);
    }

    // Make sure all threads are running before we exit the constructor
    //
    while (capacity_threads() != thr_num)
        std::this_thread::yield();
}

// ----------------------------------------------------------------------------

ThreadPool::~ThreadPool()  {

    shutdown();
    for (auto &routine : threads_)
        if (routine.joinable())
            routine.join();
}

// ----------------------------------------------------------------------------

void
ThreadPool::set_timeout(bool timeout_flag, time_type timeout_time)  {

    timeout_flag_ = timeout_flag;
    timeout_time_ = timeout_time;
}

// ----------------------------------------------------------------------------

void
ThreadPool::queue_timed_outs_() noexcept  {

    const size_type timeys { capacity_threads() };

    for (size_type i = 0; i < timeys; ++i)  {
        const WorkUnit  work_unit { WORK_TYPE::_timeout_ };

        global_queue_.push(work_unit);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename F, typename ... As>
ThreadPool::dispatch_res_t<F, As ...>
ThreadPool::dispatch(bool immediately, F &&routine, As && ... args)  {

    if (is_shutdown() || (capacity_threads() == 0 && ! immediately))
        throw std::runtime_error("ThreadPool::dispatch(): "
                                 "Thread-pool has 0 thread capacity.");

    using task_return_t =
        std::invoke_result_t<std::decay_t<F>, std::decay_t<As> ...>;
    using future_t = dispatch_res_t<F, As ...>;

    auto            callable  {
        std::make_shared<std::packaged_task<task_return_t()>>
            (std::bind<task_return_t>(std::forward<F>(routine),
                                      std::forward<As>(args) ...))
    };
    future_t        return_fut { callable->get_future() };
    const WorkUnit  work_unit {
        WORK_TYPE::_client_service_, [callable]() -> void { (*callable)(); }
    };

    if (immediately && available_threads() == 0)
        add_thread(1);

    if (local_queue_)  {  // Is this one of the pool threads
        const guard_type    guard { state_ };

        local_queue_->push(work_unit);
    }
    else
        global_queue_.push(work_unit);

    if (timeout_flag_)
        queue_timed_outs_();
    return (return_fut);
}

// ----------------------------------------------------------------------------

template<typename F, typename I, typename ... As>
ThreadPool::loop_res_t<F, I, As ...>
ThreadPool::parallel_loop(I begin, I end, F &&routine, As && ... args)  {

    using task_return_t =
        std::invoke_result_t<std::decay_t<F>,
                             std::decay_t<I>,
                             std::decay_t<I>,
                             std::decay_t<As> ...>;
    using future_t = std::future<task_return_t>;

    size_type   n { 0 };

    if constexpr (std::is_integral<I>::value)
        n = end - begin;
    else
        n = std::distance(begin, end);

    const size_type         cap_thrs { capacity_threads() };
    const size_type         block_size { n / cap_thrs };
    std::vector<future_t>   ret;

    ret.reserve(cap_thrs + 1);
    for (size_type i = 0; i < n; i += block_size)  {
        const size_type block_end {
            ((i + block_size) > n) ? n : i + block_size
        };

        ret.push_back(dispatch(false,
                               routine,
                               begin + i,
                               begin + block_end,
                               std::forward<As>(args) ...));
    }

    return (ret);
}

// ----------------------------------------------------------------------------

template<std::random_access_iterator I, long TH>
void
ThreadPool::parallel_sort(const I begin, const I end)  {

    using value_type = typename std::iterator_traits<I>::value_type;

    auto    compare = std::less<value_type>{ };

    parallel_sort<I, decltype(compare), TH>(begin, end, std::move(compare));
}

// ----------------------------------------------------------------------------

template<std::random_access_iterator I, typename P, long TH>
void
ThreadPool::parallel_sort(const I begin, const I end, P compare)  {

    using value_type = typename std::iterator_traits<I>::value_type;
    using fut_type = std::future<void>;

    if (begin >= end) return;

    const size_type data_size = std::distance(begin, end);

    if (data_size > 0)  {
        auto                left_iter = begin;
        auto                right_iter = end - 1;
        bool                is_swapped_left = false;
        bool                is_swapped_right = false;
        const value_type    pivot = *begin;
        auto                fwd_iter = begin + 1;

        while (fwd_iter <= right_iter)  {
            if (compare(*fwd_iter, pivot))  {
                is_swapped_left = true;
                std::iter_swap(left_iter, fwd_iter);
                ++left_iter;
                ++fwd_iter;
            }
            else if (compare(pivot, *fwd_iter))  {
                is_swapped_right = true;
                std::iter_swap(right_iter, fwd_iter);
                --right_iter;
            }
            else ++fwd_iter;
        }

        const bool  do_left =
            is_swapped_left && std::distance(begin, left_iter) > 0;
        const bool  do_right =
            is_swapped_right && std::distance(right_iter, end) > 0;

        if (data_size >= TH)  {
            fut_type    left_fut;
            fut_type    right_fut;

            if (do_left)
                left_fut = dispatch(false,
                                    &ThreadPool::parallel_sort<I, P, TH>,
                                    this,
                                    begin,
                                    left_iter,
                                    compare);
            if (do_right)
                right_fut = dispatch(false,
                                     &ThreadPool::parallel_sort<I, P, TH>,
                                     this,
                                     right_iter + 1,
                                     end,
                                     compare);

            if (do_left)
                while (left_fut.wait_for(std::chrono::seconds(0)) ==
                           std::future_status::timeout)
                    run_task();
            if (do_right)
                while (right_fut.wait_for(std::chrono::seconds(0)) ==
                           std::future_status::timeout)
                    run_task();
        }
        else  {
            if (do_left)
                parallel_sort<I, P, TH>(begin, left_iter, compare);

            if (do_right)
                parallel_sort<I, P, TH>(right_iter + 1, end, compare);
        }
    }
}

// ----------------------------------------------------------------------------

bool
ThreadPool::add_thread(size_type thr_num)  {

    if (is_shutdown())
        throw std::runtime_error("ThreadPool::add_thread(): "
                                 "Thread pool is shutdown.");

    if (thr_num < 0)  {
        const size_type shutys { ::abs(thr_num) };

        if (shutys > capacity_threads())  {
            char    err[1024];

            ::snprintf(err, 1023,
                       "ThreadPool::add_thread(): Cannot subtract "
                       "'%ld' threads from the pool with capacity '%ld'",
                       shutys, capacity_threads());
            throw std::runtime_error(err);
        }

        for (size_type i = 0; i < shutys; ++i)  {
            const WorkUnit  work_unit { WORK_TYPE::_terminate_ };

            global_queue_.push(work_unit);
        }
    }
    else if (thr_num > 0)  {
        const guard_type    guard { state_ };
        const size_type     local_size { size_type(threads_.size()) };

        for (size_type i = 0; i < thr_num; ++i)  {
            local_queues_.push_back(LocalQueueType { });
            threads_.emplace_back(&ThreadPool::thread_routine_,
                                  this, local_size + i);
        }
    }

    std::this_thread::yield();  // Give +/- threads a chance
    return (true);
}

// ----------------------------------------------------------------------------

void
ThreadPool::attach(thread_type &&this_thr)  {

    if (is_shutdown())
        throw std::runtime_error("ThreadPool::attach(): "
                                 "Thread pool is shutdown.");

    size_type   local_size { 0 };

    {
        const guard_type    guard { state_ };

        local_size = size_type(threads_.size());
        local_queues_.push_back(LocalQueueType { });
        threads_.push_back(std::move(this_thr));
    }
    thread_routine_(local_size);
}

// ----------------------------------------------------------------------------

ThreadPool::size_type
ThreadPool::available_threads() const noexcept  {

    return (available_threads_.load(std::memory_order_relaxed));
}

// ----------------------------------------------------------------------------

ThreadPool::size_type
ThreadPool::capacity_threads() const noexcept  {

    return (capacity_threads_.load(std::memory_order_relaxed));
}

// ----------------------------------------------------------------------------

bool
ThreadPool::is_shutdown() const noexcept  {

    return (shutdown_flag_.load(std::memory_order_relaxed));
}

// ----------------------------------------------------------------------------

ThreadPool::size_type
ThreadPool::pending_tasks() const noexcept  {

    return (global_queue_.size());
}

// ----------------------------------------------------------------------------

bool
ThreadPool::shutdown() noexcept  {

    bool    expected { false };

    if (shutdown_flag_.compare_exchange_strong(expected, true,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed))  {
        const size_type capacity { capacity_threads() + 10 };

        for (size_type i = 0; i < capacity; ++i)  {
            const WorkUnit  work_unit { WORK_TYPE::_terminate_ };

            global_queue_.push(work_unit);
        }
    }

    return (true);
}

// ----------------------------------------------------------------------------

ThreadPool::WorkUnit
ThreadPool::get_one_local_task_() noexcept  {

    WorkUnit            work_unit;
    const guard_type    guard { state_ };

    if (local_queue_ && (! local_queue_->empty()))  {
        work_unit = local_queue_->front();
        local_queue_->pop();
    }
    else  {  // Try to steal tasks from other queues
        for (auto &q : local_queues_)
            if (! q.empty())  {
                work_unit = q.front();
                q.pop();
                break;
            }
    }
    return (work_unit);
}

// ----------------------------------------------------------------------------

bool
ThreadPool::run_task() noexcept  {

    WorkUnit    work_unit = get_one_local_task_();

    if (work_unit.work_type == WORK_TYPE::_undefined_)  {
        const auto  opt_ret = global_queue_.pop_front(false); // Don't wait

        if (opt_ret.has_value())
            work_unit = opt_ret.value();
    }
    if (work_unit.work_type == WORK_TYPE::_client_service_) {
        (work_unit.func)();  // Execute the callable
        return (true);
    }
    else if (work_unit.work_type != WORK_TYPE::_undefined_)
        global_queue_.push(work_unit);  // Put it back
    return (false);
}

// ----------------------------------------------------------------------------

bool
ThreadPool::thread_routine_(size_type local_q_idx) noexcept  {

    if (is_shutdown())
        return (false);

    pre_conditioner_.execute();

    time_type   last_busy_time { timeout_flag_ ? ::time(nullptr) : 0 };
    auto        iter = local_queues_.begin();

    std::advance(iter, local_q_idx);
    local_queue_ = &(*iter);
    ++capacity_threads_;
    while (true)  {
        ++available_threads_;

        WorkUnit    work_unit = get_one_local_task_();

        if (work_unit.work_type == WORK_TYPE::_undefined_)  {
            const auto  opt_ret = global_queue_.pop_front(true); // Wait

            if (opt_ret.has_value())
                work_unit = opt_ret.value();
        }

        --available_threads_;

        if (work_unit.work_type == WORK_TYPE::_terminate_)  {
            break;
        }
        else if (work_unit.work_type == WORK_TYPE::_timeout_)  {
            if (timeout_flag_ &&
                ((::time(nullptr) - last_busy_time) >= timeout_time_))
                break;
        }
        else if (work_unit.work_type == WORK_TYPE::_client_service_)  {
            if (timeout_flag_)
                last_busy_time = ::time(nullptr);
            (work_unit.func)();  // Execute the callable
        }
    }
    --capacity_threads_;
    local_queue_ = nullptr;
    post_conditioner_.execute();

    return (true);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
