// Hossein Moein
// September 22, 2017
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

#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/DataFrameTypes.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, typename I = unsigned long>
struct ClipVisitor  {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = size_type;

    inline void operator() (const index_type &, value_type &val)  {

        if (val > upper_)  {
            val = upper_;
            count_ += 1;
        }
        else if (val < lower_)  {
            val = lower_;
            count_ += 1;
        }
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        const auto  &dummy = *idx_begin;

        while (column_begin < column_end)
            (*this)(dummy, *column_begin++);
    }

    inline void pre ()  { count_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (count_); }

    ClipVisitor(const value_type &u, const value_type &l)
        : upper_(u), lower_(l)  {   }

private:

    result_type         count_ { 0 };
    const value_type    &upper_;
    const value_type    &lower_;
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct HampelFilterVisitor {

public:

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = size_type;

private:

    template<typename K, typename H, typename A>
    inline void
    hampel_(K idx_begin, K idx_end, H column_begin, H column_end, A &&aggr)  {

        aggr.pre();
        aggr(idx_begin, idx_end, column_begin, column_end);
        aggr.post();

        const size_type col_s = std::distance(column_begin, column_end);
        std::vector<T>  diff;

        diff.reserve(col_s);
        std::transform(aggr.get_result().begin(), aggr.get_result().end(),
                       column_begin,
                       std::back_inserter(diff),
                       [](const T &lhs, const T &rhs) -> T  {
                           return (std::fabs(lhs - rhs));
                       });

        // Calculate median absolute deviation
        aggr.pre();
        aggr(idx_begin, idx_end, diff.begin(), diff.end());
        aggr.post();

        const value_type    factor = num_of_std_ * unbiased_factor_;

        std::transform(aggr.get_result().begin(), aggr.get_result().end(),
                       aggr.get_result().begin(),
                       [factor](const T &v) -> T { return (factor * v); });

        for (size_type i = 0; i < col_s; ++i)
            if (diff[i] > aggr.get_result()[i])  {
                *(column_begin + i) =
                    std::numeric_limits<value_type>::quiet_NaN();
                count_ += 1;
            }
    }

public:

    template<typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        SimpleRollAdopter<MeanVisitor<T, I>, T, I>    med_v(
            MeanVisitor<T, I>(), window_size_);

        if (type_ == hampel_type::median)
            hampel_(idx_begin, idx_end, column_begin, column_end,
                    SimpleRollAdopter<MedianVisitor<T, I>, T, I>
                        (MedianVisitor<T, I>(), window_size_));
        else if (type_ == hampel_type::mean)
            hampel_(idx_begin, idx_end, column_begin, column_end,
                    SimpleRollAdopter<MeanVisitor<T, I>, T, I>
                        (MeanVisitor<T, I>(), window_size_));
    }

    inline void pre ()  { count_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (count_); }

    explicit HampelFilterVisitor(size_type widnow_size,
                                 hampel_type ht = hampel_type::median,
                                 value_type num_of_std = 3)
        : window_size_(widnow_size),
          type_(ht),
          num_of_std_(num_of_std)  {   }

private:

    static constexpr value_type unbiased_factor_ { value_type(1.4826) };
    const size_type             window_size_;
    const hampel_type           type_;
    const value_type            num_of_std_;
    result_type                 count_ { 0 };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
