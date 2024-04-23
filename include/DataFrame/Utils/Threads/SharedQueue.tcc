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

#include <DataFrame/Utils/Threads/SharedQueue.h>

using namespace std::chrono_literals;

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
inline void
SharedQueue<T>::push(const value_type &element) noexcept  {

    const AutoLockable  lock { mutex_ };
    const bool          was_empty { queue_.empty() };

    queue_.push (element);
    if (was_empty)  cvx_.notify_all();
}

// ----------------------------------------------------------------------------

template<typename T>
inline typename SharedQueue<T>::optional_ret
SharedQueue<T>::pop_front(bool wait_on_front) noexcept  {

    optional_ret                    ret { };
    std::unique_lock<std::mutex>    ul { mutex_ };

    if (queue_.empty() && wait_on_front)
        cvx_.wait_for(ul, 2s);

    if (! queue_.empty())  {
        ret = queue_.front();
        queue_.pop();
    }
    return (ret);
}

// ----------------------------------------------------------------------------

template<typename T>
bool SharedQueue<T>::empty() const noexcept  {

    const AutoLockable  lock { mutex_ };

    return (queue_.empty());
}

// ----------------------------------------------------------------------------

template<typename T>
typename SharedQueue<T>::size_type
SharedQueue<T>::size() const noexcept  {

    const AutoLockable  lock { mutex_ };

    return (queue_.size());
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
