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
struct  EhlersHighPassFilterVisitor  {

private:

    static constexpr bool   is_md_ { is_std_vector_v<T> || is_std_array_v<T> };

    using data_t =
        typename std::conditional_t<! is_md_,
                                    lazy_type<T>,
                                    value_type_of<T>>::type;

public:

    DEFINE_VISIT_BASIC_TYPES_4

    template <typename K, typename H>
    inline void
    operator()(K idx_begin, K idx_end, H column_begin, H column_end) {

        GET_COL_SIZE

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s < 3)
            throw DataFrameError("EhlersHighPassFilterVisitor: "
                                 "column size must be > 2");

        if constexpr (is_md_)  {
            const size_type dim { column_begin->size() };

            for (size_type i { 1 }; i < col_s; ++i)
                if ((column_begin + i)->size() != dim)
                    throw DataFrameError(
                        "EhlersHighPassFilterVisitor: "
                        "Inconsistent data dimensions");
        }
#endif // HMDF_SANITY_EXCEPTIONS

        const data_t    ang { TAU / period_ };
        const data_t    ang_cos { std::cos(ang) };
        const data_t    alpha {
            ang_cos != data_t(0)
                ? (ang_cos + std::sin(ang) - data_t(1)) / ang_cos : data_t(0)
        };
        const data_t    t_plus { data_t(1) + alpha * data_t(0.5) };
        const data_t    t_minus { data_t(1) - alpha };
        value_type      prev_input { *column_begin };
        value_type      prev_filter { };

        if constexpr (is_md_)  {
            if constexpr (is_std_vector_v<T>)
                 prev_filter.resize(column_begin->size(), data_t(0));
            else
                prev_filter.fill(data_t(0));
        }
        else  prev_filter = 0;
        for (size_type i { 1 }; i < col_s; ++i)  {
            const value_type    &diff { *(column_begin + i) - prev_input };
            const value_type    filter {
                (diff * t_plus) + (prev_filter * t_minus)
            };

            prev_filter = filter;
            prev_input = *(column_begin + i);
            *(column_begin + i) -= filter;
        }
    }

    inline void pre()  {  }
    inline void post()  {  }
    inline result_type get_result() const  { return (0); }

    explicit
    EhlersHighPassFilterVisitor(data_t period = 20) : period_(period)  {  }

private:

    const data_t    period_;
};

template<typename T, typename I = unsigned long>
using ehpf_v = EhlersHighPassFilterVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  EhlersBandPassFilterVisitor  {

private:

    static constexpr bool   is_md_ { is_std_vector_v<T> || is_std_array_v<T> };

    using data_t =
        typename std::conditional_t<! is_md_,
                                    lazy_type<T>,
                                    value_type_of<T>>::type;

public:

    DEFINE_VISIT_BASIC_TYPES_4

    template <typename K, typename H>
    inline void
    operator()(K idx_begin, K idx_end, H column_begin, H column_end) {

        GET_COL_SIZE

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s < 10)
            throw DataFrameError("EhlersBandPassFilterVisitor: "
                                 "column size must be > 9");

        if constexpr (is_md_)  {
            const size_type dim { column_begin->size() };

            for (size_type i { 1 }; i < col_s; ++i)
                if ((column_begin + i)->size() != dim)
                    throw DataFrameError(
                        "EhlersBandPassFilterVisitor: "
                        "Inconsistent data dimensions");
        }
#endif // HMDF_SANITY_EXCEPTIONS

        const data_t            beta { std::cos(TAU / period_) };
        const data_t            gamma {
            data_t(1) / std::cos(TAU * bandw_ / period_)
        };
        const data_t            alpha {
            gamma - std::sqrt(gamma * gamma - data_t(1))
        };
        const data_t            f1 { data_t(0.5) * (data_t(1) - alpha) };
        const data_t            f2 { beta * (data_t(1) + alpha) };
        std::vector<value_type> filter(col_s);

        if constexpr (is_md_)  {
            if constexpr (is_std_vector_v<T>)  {
                const auto  dim { column_begin->size() };

                filter[0] = filter[1] = value_type(dim, 0);
            }
            else  {
                filter[0] = filter[1] = value_type();
            }
        }
        else  {
            filter[0] = filter[1] = 0;
        }
        filter[2] = (*(column_begin + 2) - *(column_begin)) * f1;
        for (size_type i { 3 }; i < col_s; ++i)
            filter[i] =
                ((*(column_begin + i) - *(column_begin + (i - 2))) * f1) +
                (filter[i - 1] * f2) - (filter[i - 2] * alpha);
        for (size_type i { 0 }; i < col_s; ++i)
            *(column_begin + i) -= filter[i];
    }

    inline void pre()  {  }
    inline void post()  {  }
    inline result_type get_result() const  { return (0); }

    explicit
    EhlersBandPassFilterVisitor(data_t period = 20, data_t bandwidth = 0.3)
        : period_(period), bandw_(bandwidth)  {  }

private:

    const data_t    period_;
    const data_t    bandw_;
};

template<typename T, typename I = unsigned long>
using ebpf_v = EhlersBandPassFilterVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  ClipVisitor  {

private:

    static constexpr bool   is_md_ { is_std_vector_v<T> || is_std_array_v<T> };

public:

    DEFINE_VISIT_BASIC_TYPES_4

    inline void operator()(const index_type &, value_type &val)  {

        if constexpr (! is_md_)  {
            if (val > upper_)  {
                val = upper_;
                count_ += 1;
            }
            else if (val < lower_)  {
                val = lower_;
                count_ += 1;
            }
        }
        else  {
            if (val == nan_)  return;

            const size_type dim { val.size() };

            for (size_type j { 0 }; j < dim; ++j)  {
                if (val[j] > upper_[j])  {
                    val[j] = upper_[j];
                    count_ += 1;
                }
                else if (val[j] < lower_[j])  {
                    val[j] = lower_[j];
                    count_ += 1;
                }
            }
        }
    }

    template <typename K, typename H>
    inline void
    operator()(K idx_begin, K idx_end, H column_begin, H column_end) {

        GET_COL_SIZE

#ifdef HMDF_SANITY_EXCEPTIONS
        if constexpr (is_md_)  {
            const size_type dim { column_begin->size() };

            for (size_type i { 1 }; i < col_s; ++i)
                if ((column_begin + i)->size() != dim)
                    throw DataFrameError(
                        "ClipVisitor: Inconsistent data dimensions");

            if (upper_.size() != dim || lower_.size() != dim)
                throw DataFrameError(
                    "ClipVisitor: Upper and lower limits must of the same "
                    "dimension as input data");
        }
#endif // HMDF_SANITY_EXCEPTIONS

        auto    lbd =
            [&column_begin, this](auto begin, auto end) -> result_type  {
                result_type count { 0 };
                size_type   dim { 1 };

                if constexpr (is_md_)  dim = column_begin->size();
                for (size_type i { begin }; i < end; ++i)  {
                    value_type  &val { *(column_begin + i) };

                    if constexpr (! is_md_)  {
                        if (val > upper_)  {
                            val = upper_;
                            count += 1;
                        }
                        else if (val < lower_)  {
                            val = lower_;
                            count += 1;
                        }
                    }
                    else  {
                        for (size_type j { 0 }; j < dim; ++j)  {
                            if (val[j] > upper_[j])  {
                                val[j] = upper_[j];
                                count += 1;
                            }
                            else if (val[j] < lower_[j])  {
                                val[j] = lower_[j];
                                count += 1;
                            }
                        }
                    }
                }
                return (count);
            };

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures {
                ThreadGranularity::thr_pool_.parallel_loop<value_type>(
                    size_type(0),
                    col_s,
                    std::move(lbd))
            };

            for (auto &fut : futures)  count_ += fut.get();
        }
        else  {
            count_ = lbd(size_type(0), col_s);
        }
    }

    DEFINE_PRE_POST_2

    ClipVisitor(const value_type &u, const value_type &l)
        : upper_(u), lower_(l)  {   }

private:

    result_type         count_ { 0 };
    const value_type    &upper_;
    const value_type    &lower_;
    const value_type    nan_ { get_nan<T>() };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  AbsVisitor  {

    DEFINE_VISIT_BASIC_TYPES_4

    inline void operator()(const index_type &, value_type &val)  {

        if (val < T(0))  {
            if constexpr (std::is_floating_point<T>::value)
                val = std::fabs(val);
            else
                val = std::abs(val);
            count_ += 1;
        }
    }

    // PASS_DATA_ONE_BY_ONE
    template <typename K, typename H>
    inline void
    operator()(K idx_begin, K idx_end, H column_begin, H column_end) {

        GET_COL_SIZE

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop<T>(
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

// The exponential smoothing could be done multiple times, if there is a
// trend in the data
template<typename T, typename I = unsigned long>
struct  ExpoSmootherVisitor {

    DEFINE_VISIT_BASIC_TYPES_4

    template<typename K, typename H>
    inline void
    operator()(K, K, H column_begin, H column_end)  {

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
struct  HWExpoSmootherVisitor  {

    DEFINE_VISIT_BASIC_TYPES_4

    template<typename K, typename H>
    inline void
    operator()(K /*idx_begin*/, K /*idx_end*/,
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
