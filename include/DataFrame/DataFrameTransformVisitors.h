// Hossein Moein
// September 22, 2017
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

#include <DataFrame/DataFrameStatsVisitors.h>

#include <algorithm>
#include <cmath>
#include <iterator>
#include <limits>
#include <type_traits>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define DEFINE_VISIT_BASIC_TYPES_4 \
    DEFINE_VISIT_BASIC_TYPES \
    using result_type = size_type;

#define DEFINE_PRE_POST_2 \
    inline void pre ()  { count_ = 0; } \
    inline void post ()  {  } \
    inline result_type get_result () const  { return (count_); }

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  ClipVisitor  {

    DEFINE_VISIT_BASIC_TYPES_4

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
    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end) {

        GET_COL_SIZE

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, this]
                    (auto begin, auto end) -> result_type  {
                        result_type count { 0 };

                        for (size_type i = begin; i < end; ++i)  {
                            value_type  &val = *(column_begin + i);

                            if (val > this->upper_)  {
                                val = this->upper_;
                                count += 1;
                            }
                            else if (val < this->lower_)  {
                                val = this->lower_;
                                count += 1;
                            }
                        }
                        return (count);
                    });

            for (auto &fut : futures)  count_ += fut.get();
        }
        else  {
            for (size_type i = 0; i < col_s; ++i)  {
                value_type  &val = *(column_begin + i);

                if (val > upper_)  {
                    val = upper_;
                    count_ += 1;
                }
                else if (val < lower_)  {
                    val = lower_;
                    count_ += 1;
                }
            }
        }
    }

    DEFINE_PRE_POST_2

    ClipVisitor(const value_type &u, const value_type &l)
        : upper_(u), lower_(l)  {   }

private:

    result_type         count_ { 0 };
    const value_type    &upper_;
    const value_type    &lower_;
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  AbsVisitor  {

    DEFINE_VISIT_BASIC_TYPES_4

    inline void operator() (const index_type &, value_type &val)  {

        if (val < T(0))  {
            if constexpr (std::is_floating_point<T>::value)
                val = std::fabs(val);
            else
                val = std::abs(val);
            count_ += 1;
        }
    }
    PASS_DATA_ONE_BY_ONE
    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end) {

        GET_COL_SIZE

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin]
                    (auto begin, auto end) -> result_type  {
                        result_type count { 0 };

                        for (size_type i = begin; i < end; ++i)  {
                            value_type  &val = *(column_begin + i);

                            if (val < T(0))  {
                                if constexpr (std::is_floating_point<T>::value)
                                    val = std::fabs(val);
                                else
                                    val = std::abs(val);
                                count += 1;
                            }
                        }
                        return (count);
                    });

            for (auto &fut : futures)  count_ += fut.get();
        }
        else  {
            for (size_type i = 0; i < col_s; ++i)  {
                value_type  &val = *(column_begin + i);

                if (val < T(0))  {
                    if constexpr (std::is_floating_point<T>::value)
                        val = std::fabs(val);
                    else
                        val = std::abs(val);
                    count_ += 1;
                }
            }
        }
    }

    DEFINE_PRE_POST_2

    AbsVisitor() = default;

private:

    result_type count_ { 0 };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  HampelFilterVisitor {

public:

    DEFINE_VISIT_BASIC_TYPES_4

private:

    template<typename K, typename H, typename AR>
    inline void
    hampel_(K idx_begin, K idx_end, H column_begin, H column_end, AR &&aggr)  {

        aggr.pre();
        aggr(idx_begin, idx_end, column_begin, column_end);
        aggr.post();

        GET_COL_SIZE

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        vec_t       diff(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&aggr = std::as_const(aggr.get_result()), &diff,
                     &column_begin]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)
                            diff[i] = aggr[i] - *(column_begin + i);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(aggr.get_result().begin(), aggr.get_result().end(),
                           column_begin,
                           diff.begin(),
                           [](const T &agr, const T &col) -> T  {
                               return (std::fabs(agr - col));
                           });
        }

        // Calculate median absolute deviation
        //
        aggr.pre();
        aggr(idx_begin, idx_end, diff.begin(), diff.end());
        aggr.post();

        const value_type    factor = num_of_std_ * unbiased_factor_;

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, &diff = std::as_const(diff), factor,
                     &aggr = std::as_const(aggr.get_result())]
                    (auto begin, auto end) -> result_type  {
                        result_type count { 0 };

                        for (size_type i = begin; i < end; ++i)  {
                            if (diff[i] > (aggr[i] * factor))  {
                                *(column_begin + i) =
                                    std::numeric_limits<T>::quiet_NaN();
                                count += 1;
                            }
                        }
                        return (count);
                    });

            for (auto &fut : futures)  count_ += fut.get();
        }
        else  {
            for (size_type i = 0; i < col_s; ++i)  {
                if (diff[i] > (aggr.get_result()[i] * factor))  {
                    *(column_begin + i) = std::numeric_limits<T>::quiet_NaN();
                    count_ += 1;
                }
            }
        }
    }

public:

    template<typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        if (type_ == hampel_type::median)
            hampel_(idx_begin, idx_end, column_begin, column_end,
                    SimpleRollAdopter<MedianVisitor<T, I>, T, I>
                        (MedianVisitor<T, I> { }, window_size_));
        else if (type_ == hampel_type::mean)
            hampel_(idx_begin, idx_end, column_begin, column_end,
                    SimpleRollAdopter<MeanVisitor<T, I>, T, I>
                        (MeanVisitor<T, I> { true }, window_size_));
    }

    DEFINE_PRE_POST_2

    explicit
    HampelFilterVisitor(size_type widnow_size,
                        hampel_type ht = hampel_type::median,
                        value_type num_of_std = 3)
        : window_size_(widnow_size),
          type_(ht),
          num_of_std_(num_of_std)  {   }

private:

    using vec_t = std::vector<T, typename allocator_declare<T, A>::type>;

    static constexpr value_type unbiased_factor_ { value_type(1.4826) };
    const size_type             window_size_;
    const hampel_type           type_;
    const value_type            num_of_std_;
    result_type                 count_ { 0 };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using hamf_v = HampelFilterVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// The exponential smoothing could be done multiple times, if there is a
// trend in the data
template<typename T, typename I = unsigned long>
struct  ExpoSmootherVisitor {

    DEFINE_VISIT_BASIC_TYPES_4

    template<typename K, typename H>
    inline void
    operator() (K, K, H column_begin, H column_end)  {

        count_ = std::distance(column_begin, column_end);

        for (size_type j = 0; j < repeat_count_; ++j)  {
            value_type  prev_v = *column_begin;

            // Y0 = X0
            // Yt = aXt + (1 - a)Yt-1
            for (size_type i = 1; i < count_; ++i) [[likely]]  {
                const value_type    curr_v = *(column_begin + i);

                *(column_begin + i) = prev_v + alfa_ * (curr_v - prev_v);
                prev_v = curr_v;
            }
        }
    }

    DEFINE_PRE_POST_2

    explicit
    ExpoSmootherVisitor(value_type data_smoothing_factor, size_type rc = 1)
        : alfa_(data_smoothing_factor),
          repeat_count_(rc)  {   }

private:

    const value_type    alfa_;
    const size_type     repeat_count_;
    result_type         count_ { 0 };
};

template<typename T, typename I = unsigned long>
using exs_v = ExpoSmootherVisitor<T, I>;

// ----------------------------------------------------------------------------

// Holt-Winters double exponential smoothing
template<typename T, typename I = unsigned long>
struct  HWExpoSmootherVisitor {

    DEFINE_VISIT_BASIC_TYPES_4

    template<typename K, typename H>
    inline void
    operator() (K /*idx_begin*/, K /*idx_end*/,
                H column_begin, H column_end)  {

        count_ = std::distance(column_begin, column_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (count_ <= 2)
            throw DataFrameError("HWExpoSmootherVisitor: count must be > 2");
#endif // HMDF_SANITY_EXCEPTIONS

        value_type  prev_v = *column_begin;
        value_type  tf = *(column_begin + 1) - prev_v;

        for (size_type i = 1; i < count_; ++i) [[likely]]  {
            const value_type    curr_v = *(column_begin + i);

            *(column_begin + i) =
                alfa_ * curr_v + (T(1) - alfa_) * (prev_v + tf);
            tf = beta_ * (curr_v - prev_v) + (T(1) - beta_) * tf;
            prev_v = curr_v;
        }
    }

    DEFINE_PRE_POST_2

    HWExpoSmootherVisitor(value_type data_smoothing_factor,
                          value_type trend_smoothing_factor)
        : alfa_(data_smoothing_factor), beta_(trend_smoothing_factor)  {   }

private:

    const value_type    alfa_;
    const value_type    beta_;
    result_type         count_ { 0 };
};

template<typename T, typename I = unsigned long>
using hwexp_v = HWExpoSmootherVisitor<T, I>;

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
