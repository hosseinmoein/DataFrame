// Hossein Moein
// October 16 2020
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

#pragma once

#include <algorithm>
#include <array>
#include <functional>
#include <iterator>
#include <vector>

#if defined(_WIN32) && defined(HMDF_SHARED)
#  ifdef LIBRARY_EXPORTS
#    define LIBRARY_API __declspec(dllexport)
#  else
#    define LIBRARY_API __declspec(dllimport)
#  endif // LIBRARY_EXPORTS
#else
#  define LIBRARY_API
#endif // _WIN32

// ----------------------------------------------------------------------------

namespace hmdf
{

// Fixed size priority queue. By default, it is a max heap
//
template <typename T, std::size_t N, typename Cmp = std::less<T>>
class LIBRARY_API   FixedSizePriorityQueue  {

    using container_type = std::array<T, N>;
    using iterator = typename container_type::iterator;
    using const_iterator = typename container_type::const_iterator;

public:

    using value_type = T;
    using compare_type = Cmp;
    using size_type = std::size_t;

    void push(value_type &&item)  {

        if (data_end_ != array_.end())  {
            *data_end_++ = std::move(item);
            std::push_heap(array_.begin(), data_end_, cmp_);
        }
        else  {
            std::sort_heap(array_.begin(), array_.end(), cmp_);
            if (cmp_(array_.front(), item))
                array_[0] = std::move(item);
            std::make_heap(array_.begin(), array_.end(), cmp_);
        }
    }

    inline const value_type &top() const noexcept  { return (array_.front()); }
    inline void pop()  {

        if (! empty())
            std::pop_heap(array_.begin(), data_end_--, cmp_);
    }

    inline size_type size() const noexcept {

        return (std::distance(array_.begin(),
                              static_cast<const_iterator>(data_end_)));
    }
    inline bool empty() const noexcept  { return (size() == 0); }

    inline void clear()  { data_end_ = array_.begin(); }

    inline std::vector<value_type> data() const  {

        std::vector<value_type> result;

        result.reserve(size());
        for (auto citer = array_.begin(); citer < data_end_; ++citer)
            result.push_back(*citer);
        std::sort_heap(result.begin(), result.end(), cmp_);
        return (result);
    }

private:

    container_type  array_ {  };
    iterator        data_end_ { array_.begin() };
    compare_type    cmp_ {  };
};

} // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
