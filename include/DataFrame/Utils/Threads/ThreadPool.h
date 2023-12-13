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

#pragma once

#include <DataFrame/Utils/Threads/SharedQueue.h>

#include <atomic>
#include <concepts>
#include <condition_variable>
#include <functional>
#include <future>
#include <iterator>
#include <list>
#include <mutex>
#include <ranges>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct  Conditioner  {

    template<typename F, typename ... As>
    requires std::invocable<F, As ...>
    explicit Conditioner(F &&routine, As && ... args);

    Conditioner() = default;
    Conditioner(const Conditioner &) = default;
    Conditioner(Conditioner &&) = default;
    ~Conditioner() = default;

    void execute();

private:

    using routine_type = std::function<void()>;

    routine_type    func_ { [] () -> void  { } };
};

// ----------------------------------------------------------------------------

class   ThreadPool  {

public:

    using size_type = long;
    using time_type = time_t;
    using thread_type = std::thread;

    inline static constexpr size_type   MUL_THR_THHOLD = 150'000L;

    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator = (const ThreadPool &) = delete;

    // Conditioner(s) are a handy interface, if threads need to be initialized
    // before doing anything. And/or they need a clean up before exiting.
    // For example, see Windows CoInitializeEx function in COM library
    //
    explicit
    ThreadPool(size_type thr_num = std::thread::hardware_concurrency(),
               Conditioner pre_conditioner = Conditioner { },
               Conditioner post_conditioner = Conditioner { });
    ~ThreadPool();

    void set_timeout(bool timeout_flag, time_type timeout_time = 30 * 60);

    template<typename F, typename ... As>
    requires std::invocable<F, As ...>
    using dispatch_res_t =
        std::future<std::invoke_result_t<std::decay_t<F>,
                                         std::decay_t<As> ...>>;

    template<typename F, typename I, typename ... As>
    requires std::invocable<F, I, I, As ...>
    using loop_res_t =
        std::vector<std::future<std::invoke_result_t<std::decay_t<F>,
                                                     std::decay_t<I>,
                                                     std::decay_t<I>,
                                                     std::decay_t<As> ...>>>;

    // The return type of dispatch is std::future of return type of routine
    //
    template<typename F, typename ... As>
    dispatch_res_t<F, As ...>
    dispatch(bool immediately, F &&routine, As && ... args);

    // It dispatches n / number_of_capacity_threads tasks where n is the
    // distance between begin and end.
    //
    template<typename F, typename I, typename ... As>
    loop_res_t<F, I, As ...>
    parallel_loop(I begin, I end, F &&routine, As && ... args);

    template<std::random_access_iterator I, long TH = MUL_THR_THHOLD>
    void parallel_sort(const I begin, const I end);
    template<std::random_access_iterator I, typename P,
             long TH = MUL_THR_THHOLD>
    void parallel_sort(const I begin, const I end, P compare);


    // It attaches the current thread to the pool so that it may be used for
    // executing submitted tasks. It blocks the calling thread until the pool
    // is shutdown or the thread is timed-out.
    // This is handy, if you already have thread(s), and want to repurpose them
    //
    void attach(thread_type &&this_thr);

    // If the pool is not shutdown and there is a pending task, run the one
    // task on the calling thread.
    // Return true, if a task was executed, otherwise false.
    //
    bool run_task() noexcept;

    bool add_thread(size_type thr_num);  // Could be positive or negative

    size_type available_threads() const noexcept;
    size_type capacity_threads() const noexcept;
    size_type pending_tasks() const noexcept; // How many tasks in the queue
    bool is_shutdown() const noexcept;

    bool shutdown() noexcept;

private:

    using routine_type = std::function<void()>;

    enum class WORK_TYPE : unsigned char {
        _undefined_ = 0,
        _client_service_ = 1,
        _terminate_ = 2,
        _timeout_ = 3,
    };

    struct  WorkUnit  {

        WorkUnit() = default;
        WorkUnit(const WorkUnit &) = default;
        WorkUnit(WorkUnit &&) = default;
        WorkUnit &operator=(const WorkUnit &) = default;
        WorkUnit &operator=(WorkUnit &&) = default;

        explicit WorkUnit(WORK_TYPE work_t) : work_type(work_t)  {   }
        WorkUnit(WORK_TYPE work_t, routine_type &&routine)
            : func(std::forward<routine_type>(routine)),
              work_type(work_t)  {   }

        routine_type    func {  };
        WORK_TYPE       work_type { WORK_TYPE::_undefined_ };
    };

    bool thread_routine_(size_type local_q_idx) noexcept;  // Engine routine
    void queue_timed_outs_() noexcept;
    WorkUnit get_one_local_task_() noexcept;

    using guard_type = std::lock_guard<std::mutex>;
    using GlobalQueueType = SharedQueue<WorkUnit>;
    using LocalQueueType = std::queue<WorkUnit>;

    using LocalQueueList = std::list<LocalQueueType>;
    using ThreadVector = std::vector<thread_type>;

    ThreadVector    threads_ { };
    LocalQueueList  local_queues_ { };
    GlobalQueueType global_queue_ { };

    inline static thread_local LocalQueueType *local_queue_ { nullptr };

    std::atomic<size_type>  available_threads_ { 0 };
    std::atomic<size_type>  capacity_threads_ { 0 };
    std::atomic_bool        shutdown_flag_ { false };
    time_type               timeout_time_ { 30 * 60 };
    mutable std::mutex      state_ { };
    bool                    timeout_flag_ { false };

    Conditioner pre_conditioner_ { };
    Conditioner post_conditioner_ { };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/Threads/ThreadPool.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
