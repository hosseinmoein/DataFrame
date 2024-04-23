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

#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <queue>
#include <chrono>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
class   SharedQueue  {

public:

    using value_type = T;
    using size_type = std::size_t;
    using optional_ret = std::optional<value_type>;

    SharedQueue() = default;
    SharedQueue(SharedQueue &&) = default;
    SharedQueue &operator = (SharedQueue &&) = default;
    SharedQueue(const SharedQueue &) = delete;
    SharedQueue &operator = (const SharedQueue &) = delete;

    inline void push(const value_type &element) noexcept;

    // NOTE: The following method returns the data by value.
    //       Therefore, it is not as efficient as front().
    //       Use it only if you have to.
    //
    inline optional_ret
    pop_front(bool wait_on_front = true) noexcept;

    bool empty() const noexcept;
    size_type size() const noexcept;

private:

    using QueueType = std::queue<value_type, std::deque<value_type>>;
    using AutoLockable = std::lock_guard<std::mutex>;

    mutable std::mutex              mutex_ { };
    mutable std::condition_variable cvx_ { };
    QueueType                       queue_ { };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/Threads/SharedQueue.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
