// Hossein Moein

// January 08, 2020
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
#include <DataFrame/DataFrameTypes.h>
#include <DataFrame/Utils/MetaProg.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <future>
#include <iterator>
#include <limits>
#include <numeric>
#include <type_traits>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  ReturnVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

        if (col_s < 3)  return;

        // t1 = today,  tm1 = yesterday

        // Log return
        //
        std::function<value_type(value_type, value_type)>   func =
            [](value_type t1, value_type tm1) -> value_type  {
                return (std::log(t1 / tm1));
            };

        if (ret_p_ == return_policy::percentage)
            func = [](value_type t1, value_type tm1) -> value_type  {
                      return ((t1 - tm1) / tm1);
                   };
        else if (ret_p_ == return_policy::monetary)
            func = [](value_type t1, value_type tm1) -> value_type  {
                       return (t1 - tm1);
                   };
        else if (ret_p_ == return_policy::trinary)
            func = [](value_type t1, value_type tm1) -> value_type  {
                       const value_type diff = t1 - tm1;

                       return ((diff > 0) ? 1 : ((diff < 0) ? -1 : 0));
                   };

        result_type result(col_s);

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&result, &column_begin, &func]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] = func(*(column_begin + i),
                                             *(column_begin + (i - 1)));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::adjacent_difference (column_begin, column_end,
                                      result.begin(),
                                      func);
        }
        result[0] = std::numeric_limits<T>::quiet_NaN();
        result.swap(result_);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit ReturnVisitor (return_policy rp) : ret_p_(rp)  {   }

private:

    OBO_PORT_DECL

    result_type         result_ {  };
    const return_policy ret_p_;
};

// ----------------------------------------------------------------------------

template<typename S_RT,  // Short duration rolling adopter
         typename L_RT,  // Longer duration rolling adopter
         arithmetic T,
         typename I = unsigned long, std::size_t A = 0>
struct  DoubleCrossOver  {

private:

    S_RT    short_roller_;
    L_RT    long_roller_;

    template <forward_iterator K, forward_iterator H>
    inline std::size_t
    run_short_roller_(const K &idx_begin,
                      const K &idx_end,
                      const H &prices_begin,
                      const H &prices_end)  {

        short_roller_(idx_begin, idx_end, prices_begin, prices_end);

        const auto          &result = short_roller_.get_result();
        const std::size_t   col_s =
            std::min<std::size_t>(
                { size_t(std::distance(idx_begin, idx_end)),
                  size_t(std::distance(prices_begin, prices_end)),
                  result.size() });

        col_to_short_term_.resize(col_s);
        if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &prices_begin, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            this->col_to_short_term_[i] =
                                *(prices_begin + i) - result[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(prices_begin, prices_begin + col_s,
                           result.begin(),
                           col_to_short_term_.begin(),
                           [](auto p, auto r) -> value_type  {
                               return (p - r);
                           });
        }
        return (col_s);
    }
    template <typename K, typename H>
    inline std::size_t
    run_long_roller_(const K &idx_begin,
                     const K &idx_end,
                     const H &prices_begin,
                     const H &prices_end)  {

        long_roller_(idx_begin, idx_end, prices_begin, prices_end);

        const auto          &result = long_roller_.get_result();
        const std::size_t   col_s =
            std::min<std::size_t>(
                { size_t(std::distance(idx_begin, idx_end)),
                  size_t(std::distance(prices_begin, prices_end)),
                  result.size() });

        col_to_long_term_.resize(col_s);
        if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &prices_begin, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            this->col_to_long_term_[i] =
                                *(prices_begin + i) - result[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(prices_begin, prices_begin + col_s,
                           result.begin(),
                           col_to_long_term_.begin(),
                           [](auto p, auto r) -> value_type  {
                               return (p - r);
                           });
        }
        return (col_s);
    }

public:

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &prices_begin, const H &prices_end)  {

        size_type   re_count1 = 0;
        size_type   re_count2 = 0;

        if (thread_level_ > 2)  {
            std::future<size_type>  fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    &DoubleCrossOver::run_short_roller_<K, H>,
                        this,
                        std::cref(idx_begin),
                        std::cref(idx_end),
                        std::cref(prices_begin),
                        std::cref(prices_end));
            std::future<size_type>  fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    &DoubleCrossOver::run_long_roller_<K, H>,
                        this,
                        std::cref(idx_begin),
                        std::cref(idx_end),
                        std::cref(prices_begin),
                        std::cref(prices_end));

            re_count1 = fut1.get();
            re_count2 = fut2.get();
        }
        else  {
            re_count1 =
                run_short_roller_(idx_begin,
                                  idx_end,
                                  prices_begin,
                                  prices_end);
            re_count2 =
                run_long_roller_(idx_begin, idx_end, prices_begin, prices_end);
        }

        const size_type col_s = std::min<size_type>(re_count1, re_count2);

        short_term_to_long_term_.resize(col_s);
        if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            this->short_term_to_long_term_[i] =
                                this->short_roller_.get_result()[i] -
                                this->long_roller_.get_result()[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(short_roller_.get_result().begin(),
                           short_roller_.get_result().begin() + col_s,
                           long_roller_.get_result().begin(),
                           short_term_to_long_term_.begin(),
                           [](auto s, auto l) -> value_type  {
                               return (s - l);
                           });
        }
    }

    inline void pre ()  {

        short_roller_.pre();
        long_roller_.pre();
        col_to_short_term_.clear();
        col_to_long_term_.clear();
        short_term_to_long_term_.clear();
    }
    inline void post ()  {

        short_roller_.post();
        long_roller_.post();
    }
    const result_type &
    get_raw_to_short_term() const  { return (col_to_short_term_); }
    result_type &get_raw_to_short_term()  { return (col_to_short_term_); }
    const result_type &
    get_raw_to_long_term() const  { return (col_to_long_term_); }
    result_type &get_raw_to_long_term()  { return (col_to_long_term_); }
    const result_type &
    get_short_term_to_long_term() const  { return (short_term_to_long_term_); }
    result_type &
    get_short_term_to_long_term()  { return (short_term_to_long_term_); }

    inline DoubleCrossOver(S_RT &&sr, L_RT &&lr)
        : short_roller_(std::move(sr)),
          long_roller_(std::move(lr)),
          thread_level_(ThreadGranularity::get_thread_level())  {   }

private:

    result_type col_to_short_term_;
    result_type col_to_long_term_;
    result_type short_term_to_long_term_;

    const long  thread_level_;
};

template<typename S_RT,
         typename L_RT,
         typename T,
         typename I = unsigned long,
         std::size_t A = 0>
using dco_v = DoubleCrossOver<S_RT, L_RT, T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  BollingerBand {

private:

    const double                                    upper_band_multiplier_;
    const double                                    lower_band_multiplier_;
    SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   mean_roller_;
    SimpleRollAdopter<StdVisitor<T, I>, T, I, A>    std_roller_;

    template <forward_iterator K, forward_iterator H>
    inline void
    run_mean_roller_(const K &idx_begin,
                     const K &idx_end,
                     const H &prices_begin,
                     const H &prices_end)  {

        mean_roller_.pre();
        mean_roller_(idx_begin, idx_end, prices_begin, prices_end);
        mean_roller_.post();
    }

    template <forward_iterator K, forward_iterator H>
    inline void
    run_std_roller_(const K &idx_begin,
                    const K &idx_end,
                    const H &prices_begin,
                    const H &prices_end)  {

        std_roller_.pre();
        std_roller_(idx_begin, idx_end, prices_begin, prices_end);
        std_roller_.post();
    }

public:

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &prices_begin, const H &prices_end)  {

        const auto  thread_level =
            (std::distance(prices_begin, prices_end) <
                 ThreadPool::MUL_THR_THHOLD)
                     ? 0L : ThreadGranularity::get_thread_level();

        if (thread_level > 2)  {
            std::future<void>   fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    &BollingerBand::run_mean_roller_<K, H>,
                        this,
                        std::cref(idx_begin),
                        std::cref(idx_end),
                        std::cref(prices_begin),
                        std::cref(prices_end));
            std::future<void>   fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    &BollingerBand::run_std_roller_<K, H>,
                        this,
                        std::cref(idx_begin),
                        std::cref(idx_end),
                        std::cref(prices_begin),
                        std::cref(prices_end));

            fut1.get();
            fut2.get();
        }
        else  {
            run_mean_roller_(idx_begin, idx_end, prices_begin, prices_end);
            run_std_roller_(idx_begin, idx_end, prices_begin, prices_end);
        }

        const auto      &std_result = std_roller_.get_result();
        const auto      &mean_result = mean_roller_.get_result();
        const size_type col_s =
            std::min<size_type>(
                { size_t(std::distance(idx_begin, idx_end)),
                  size_t(std::distance(prices_begin, prices_end)),
                  std_result.size(),
                  mean_result.size() });

        if (thread_level > 2)  {
            upper_band_to_raw_.resize(col_s);
            raw_to_lower_band_.resize(col_s);

            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&prices_begin,
                     &mean_result = std::as_const(mean_result),
                     &std_result = std::as_const(std_result),
                     this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    p = *(prices_begin + i);
                            const value_type    mr = mean_result[i];
                            const value_type    sr = std_result[i];

                            this->upper_band_to_raw_[i] =
                                (mr + sr * this->upper_band_multiplier_) - p;
                            this->raw_to_lower_band_[i] =
                                p - (mr - sr * this->lower_band_multiplier_);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            upper_band_to_raw_.reserve(col_s);
            raw_to_lower_band_.reserve(col_s);
            for_each_list3(
                prices_begin, prices_end,
                mean_result.begin(), mean_result.end(),
                std_result.begin(), std_result.end(),
                [this](const auto &p, const auto &mr, const auto &sr) -> void {
                    this->upper_band_to_raw_.push_back(
                        (mr + sr * this->upper_band_multiplier_) - p);
                    this->raw_to_lower_band_.push_back(
                        p - (mr - sr * this->lower_band_multiplier_));
                });
        }
    }

    inline void pre ()  {

        upper_band_to_raw_.clear();
        raw_to_lower_band_.clear();
    }
    inline void post ()  {  }
    const result_type &
    get_upper_band_to_raw() const  { return (upper_band_to_raw_); }
    result_type &get_upper_band_to_raw()  { return (upper_band_to_raw_); }
    const result_type &
    get_raw_to_lower_band() const  { return (raw_to_lower_band_); }
    result_type &get_raw_to_lower_band()  { return (raw_to_lower_band_); }

    inline BollingerBand(double upper_band_multiplier,
                         double lower_band_multiplier,
                         size_type moving_mean_period,
                         bool biased = false)
        : upper_band_multiplier_(upper_band_multiplier),
          lower_band_multiplier_(lower_band_multiplier),
          mean_roller_(std::move(MeanVisitor<T, I>()), moving_mean_period),
          std_roller_(std::move(StdVisitor<T, I>(biased)),
                      moving_mean_period) {
    }

private:

    result_type upper_band_to_raw_;
    result_type raw_to_lower_band_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using bband_v = BollingerBand<T, I, A>;

// ----------------------------------------------------------------------------

// Moving Average Convergence/Divergence
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  MACDVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        macd_roller_t   short_roller(exponential_decay_spec::span,
                                     short_mean_period_);
        macd_roller_t   long_roller(exponential_decay_spec::span,
                                    long_mean_period_);
        const size_type col_s =
            std::min<size_type>(
                { size_t(std::distance(idx_begin, idx_end)),
                  size_t(std::distance(column_begin, column_end)) });
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        short_roller.pre();
        long_roller.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&short_roller,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        short_roller(idx_begin, idx_end,
                                     column_begin, column_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&long_roller,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        long_roller(idx_begin, idx_end,
                                    column_begin, column_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            short_roller(idx_begin, idx_end, column_begin, column_end);
            long_roller(idx_begin, idx_end, column_begin, column_end);
        }
        long_roller.post();
        short_roller.post();

        const auto  &short_result = short_roller.get_result();
        const auto  &long_result = long_roller.get_result();

        macd_line_.resize(col_s);
        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [this,
                     &short_result = std::as_const(short_result),
                     &long_result = std::as_const(long_result)]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            this->macd_line_[i] =
                                short_result[i] - long_result[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(short_result.begin(), short_result.begin() + col_s,
                           long_result.begin(),
                           macd_line_.begin(),
                           [](auto s, auto l) -> value_type  {
                               return (s - l);
                           });
        }

        signal_line_roller_.pre();
        signal_line_roller_(idx_begin, idx_end,
                            macd_line_.begin(), macd_line_.end());
        signal_line_roller_.post();

        const auto  &signal_line_result { signal_line_roller_.get_result() };

        macd_histogram_.resize(col_s);
        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [this,
                     &signal_line_result = std::as_const(signal_line_result)]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            this->macd_histogram_[i] =
                                this->macd_line_[i] - signal_line_result[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(macd_line_.begin(), macd_line_.begin() + col_s,
                           signal_line_result.begin(),
                           macd_histogram_.begin(),
                           [](const auto &ml, const auto &sl) -> value_type  {
                               return (ml - sl);
                           });
        }
    }

    inline void pre ()  {

        macd_line_.clear();
        macd_histogram_.clear();
    }
    inline void post ()  {  }
    const result_type &get_macd_line() const { return (macd_line_); }
    result_type &get_macd_line()  { return (macd_line_); }
    const result_type &
    get_signal_line() const { return (signal_line_roller_.get_result()); }
    result_type &
    get_signal_line()  { return (signal_line_roller_.get_result()); }
    const result_type &get_macd_histogram() const { return (macd_histogram_); }
    result_type &get_macd_histogram()  { return (macd_histogram_); }

    inline MACDVisitor(
        size_type short_mean_period,  // e.g. 12-day
        size_type long_mean_period,   // e.g. 26-day
        size_type signal_line_period) // e.g.  9-day
        : short_mean_period_(short_mean_period),
          long_mean_period_(long_mean_period),
          signal_line_roller_(exponential_decay_spec::span,
                              signal_line_period)  {   }

private:

    using macd_roller_t = ewm_v<T, I, A>;

    const size_type short_mean_period_;
    const size_type long_mean_period_;
    macd_roller_t   signal_line_roller_;
    result_type     macd_line_ { };       // short-mean EMA - long-mean EMA
    result_type     macd_histogram_ { };  // MACD Line - Signal Line
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using macd_v = MACDVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Volume Weighted Average Price
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  VWAPVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    struct  VWAP  {
        value_type  average_price { 0 };
        value_type  vwap { 0 };
        index_type  index_value { 0 };
        size_type   event_count { 0 };
        value_type  total_volume { 0 };
        value_type  high_price { 0 };
        value_type  low_price { std::numeric_limits<value_type>::max() };
        value_type  cumulative_vwap { 0 };
        size_type   cumulative_event_count { 0 };
        value_type  cumulative_total_volume { 0 };
        value_type  cumulative_high_price { 0 };
        value_type  comulative_low_price
            { std::numeric_limits<value_type>::max() };
    };

    using result_type =
        std::vector<VWAP, typename allocator_declare<VWAP, A>::type>;
    using distance_func =
        std::function<double(const index_type &, const index_type &)>;

    // Index value is assumed to represent time
    // The first value is assumed to be the price of a traded instrument.
    // The second value is assimed to be the size of a traded instrument.
    // The provided "dfunc" measures time elapsed between two index values
    //
    inline void
    operator() (const index_type &idx,
                const value_type &price,
                const value_type &size)  {

        // If reached the limit, stop
        //
        if (total_volume_limit_ == 0 ||
            cum_volume_accumulator_ < total_volume_limit_)  {
            if (started_ && dfunc_(last_time_, idx) >= interval_)  {
                post();

                reset_(idx);
                accumulate_(size, price);
            }
            else  {
                if (! started_)
                    reset_ (idx);
                accumulate_ (size, price);
            }
        }
    }
    PASS_DATA_ONE_BY_ONE_2

    inline void pre ()  {

        result_.clear();
        vw_price_accumulator_ = 0;
        price_accumulator_ = 0;
        volume_accumulator_ = 0;
        last_time_  = index_type();
        started_ = false;
        event_count_ = 0;
        high_price_ = 0;
        low_price_ = std::numeric_limits<double>::max();
        cum_high_price_ = 0;
        cum_low_price_ = std::numeric_limits<double>::max();
        cum_price_accumulator_ = 0;
        cum_volume_accumulator_ = 0;
        cum_event_count_ = 0;
    }
    inline void post ()  {

        if (event_count_ > 0)  {
            if (volume_accumulator_ > 0)  {
                result_.push_back (
                    { price_accumulator_ / event_count_,
                      vw_price_accumulator_ / volume_accumulator_,
                      last_time_,
                      event_count_,
                      volume_accumulator_,
                      high_price_,
                      low_price_,
                      cum_price_accumulator_ / cum_volume_accumulator_,
                      cum_event_count_,
                      cum_volume_accumulator_,
                      cum_high_price_,
                      cum_low_price_,
                    });
            }
            else  {
                result_.push_back (
                    { std::numeric_limits<T>::quiet_NaN(),
                      std::numeric_limits<T>::quiet_NaN(),
                      last_time_,
                      event_count_,
                    });
            }
        }
    }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

    explicit
    VWAPVisitor(double interval,
                double max_volume = 0,
                double total_volume_limit = 0,
                distance_func f =
                    [](const I &idx1, const I &idx2) -> double {
                        return (static_cast<double>(idx2 - idx1));
                    })
        : dfunc_(f),
          interval_(interval),
          max_volume_(max_volume),
          total_volume_limit_(total_volume_limit)  {   }

private:

    inline void accumulate_ (value_type the_size, value_type the_price)  {

        if (max_volume_ == 0 || the_size < max_volume_)  {
            vw_price_accumulator_ += the_price * the_size;
            price_accumulator_ += the_price;
            cum_price_accumulator_ += the_price * the_size;
            volume_accumulator_ += the_size;
            cum_volume_accumulator_ += the_size;
            event_count_ += 1;
            cum_event_count_ += 1;
            if (the_price > high_price_)
                high_price_ = the_price;
            if (the_price < low_price_)
                low_price_ = the_price;
            if (the_price > cum_high_price_)
                cum_high_price_ = the_price;
            if (the_price < cum_low_price_)
                cum_low_price_ = the_price;
        }
    }
    inline void reset_ (index_type index_value)  {

        vw_price_accumulator_ = 0;
        price_accumulator_ = 0;
        volume_accumulator_ = 0;
        last_time_ = index_value;
        started_ = true;
        event_count_ = 0;
        high_price_ = 0;
        low_price_ = std::numeric_limits<value_type>::max();
    }

    result_type     result_ { };
    value_type      vw_price_accumulator_ { 0 };
    value_type      price_accumulator_ { 0 };
    value_type      volume_accumulator_ { 0 };
    index_type      last_time_ { };
    bool            started_ { false };
    size_type       event_count_ { 0 };
    value_type      high_price_ { 0 };
    value_type      low_price_ { std::numeric_limits<value_type>::max() };
    value_type      cum_high_price_ { 0 };
    value_type      cum_low_price_ { std::numeric_limits<value_type>::max() };
    value_type      cum_price_accumulator_ { 0 };
    value_type      cum_volume_accumulator_ { 0 };
    size_type       cum_event_count_ { 0 };
    distance_func   dfunc_;
    const double    interval_;
    const double    max_volume_;
    const double    total_volume_limit_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using vwap_v = VWAPVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Volume Weighted Bid-Ask Spread
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  VWBASVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    struct  VWBAS  {
        value_type  spread { 0 };
        value_type  percent_spread { 0 };
        value_type  vwbas { 0 };
        value_type  percent_vwbas { 0 };
        index_type  index_value { 0 };
        size_type   event_count { 0 };
        value_type  total_ask_volume { 0 };
        value_type  total_bid_volume { 0 };
        value_type  high_ask_price { 0 };
        value_type  low_ask_price { std::numeric_limits<value_type>::max() };
        value_type  high_bid_price { 0 };
        value_type  low_bid_price { std::numeric_limits<value_type>::max() };
        value_type  cumulative_vwbas { 0 };
        size_type   cumulative_event_count { 0 };
        value_type  cumulative_total_ask_volume { 0 };
        value_type  cumulative_total_bid_volume { 0 };
        value_type  cumulative_high_ask_price { 0 };
        value_type  comulative_low_ask_price
            { std::numeric_limits<value_type>::max() };
        value_type  cumulative_high_bid_price { 0 };
        value_type  comulative_low_bid_price
            { std::numeric_limits<value_type>::max() };
    };

    using result_type =
        std::vector<VWBAS, typename allocator_declare<VWBAS, A>::type>;
    using distance_func =
        std::function<double(const index_type &, const index_type &)>;

    // Index value is assumed to represent time
    // The first value is assumed to be the bid price of a traded instrument.
    // The second value is assumed to be the ask price of a traded instrument.
    // The third value is assimed to be the bid size of a traded instrument.
    // The fourth value is assimed to be the ask size of a traded instrument.
    // The provided "dfunc" measures time elapsed between two index values
    //
    inline void
    operator() (const index_type &idx,
                const value_type &bid_price,
                const value_type &ask_price,
                const value_type &bid_size,
                const value_type &ask_size)  {

        if (started_ && dfunc_(last_time_, idx) >= interval_)  {
            post();

            reset_(idx);
            accumulate_(bid_size, ask_size, bid_price, ask_price);
        }
        else  {
            if (! started_)
                reset_ (idx);
            accumulate_(bid_size, ask_size, bid_price, ask_price);
        }
    }
    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (K idx_begin, K,
                H bid_price_begin, H bid_price_end,
                H ask_price_begin, H ask_price_end,
                H bid_size_begin, H bid_size_end,
                H ask_size_begin, H ask_size_end)  {

        while (bid_price_begin < bid_price_end &&
               ask_price_begin < ask_price_end &&
               bid_size_begin < bid_size_end &&
               ask_size_begin < ask_size_end)
            (*this)(*idx_begin, *bid_price_begin++, *ask_price_begin++,
                    *bid_size_begin++, *ask_size_begin++);
    }

    inline void pre ()  {

        result_.clear();
        vw_bid_price_accumulator_ = 0;
        vw_ask_price_accumulator_ = 0;
        bid_price_accumulator_ = 0;
        ask_price_accumulator_ = 0;
        bid_volume_accumulator_ = 0;
        ask_volume_accumulator_ = 0;
        last_time_ = index_type();
        started_ = false;
        event_count_ = 0;
        high_bid_price_ = 0;
        high_ask_price_ = 0;
        low_bid_price_ = std::numeric_limits<value_type>::max();
        low_ask_price_ = std::numeric_limits<value_type>::max();
        cum_high_bid_price_ = 0;
        cum_high_ask_price_ = 0;
        cum_low_bid_price_ = std::numeric_limits<value_type>::max();
        cum_low_ask_price_ = std::numeric_limits<value_type>::max();
        cum_bid_price_accumulator_ = 0;
        cum_ask_price_accumulator_ = 0;
        cum_bid_volume_accumulator_ = 0;
        cum_ask_volume_accumulator_ = 0;
        cum_event_count_ = 0;
    }
    inline void post ()  {

        if (event_count_ > 0)  {
            if (bid_volume_accumulator_ > 0 && ask_volume_accumulator_ > 0)  {
                const value_type    vwa {
                    vw_ask_price_accumulator_ / ask_volume_accumulator_ };
                const value_type    vwb {
                    vw_bid_price_accumulator_ / bid_volume_accumulator_ };
                const value_type    vwbas { vwa - vwb };
                const value_type    per_vwbas { (vwbas / vwb) * 100.0 };
                const value_type    spread {
                    (ask_price_accumulator_ / event_count_) -
                    (bid_price_accumulator_ / event_count_) };
                const value_type    per_spread {
                    (spread / (bid_price_accumulator_ / event_count_)) *
                    100.0 };

                result_.push_back (
                    { spread,
                      per_spread,
                      vwbas,
                      per_vwbas,
                      last_time_,
                      event_count_,
                      ask_volume_accumulator_,
                      bid_volume_accumulator_,
                      high_ask_price_,
                      low_ask_price_,
                      high_bid_price_,
                      low_bid_price_,

                      // Accumulative VWBAS
                      (cum_ask_price_accumulator_ /
                       cum_ask_volume_accumulator_) -
                      (cum_bid_price_accumulator_ /
                       cum_bid_volume_accumulator_),

                      cum_event_count_,
                      cum_ask_volume_accumulator_,
                      cum_bid_volume_accumulator_,
                      cum_high_ask_price_,
                      cum_low_ask_price_,
                      cum_high_bid_price_,
                      cum_low_bid_price_,
                    });
            }
            else  {
                result_.push_back (
                    { std::numeric_limits<T>::quiet_NaN(),
                      std::numeric_limits<T>::quiet_NaN(),
                      std::numeric_limits<T>::quiet_NaN(),
                      std::numeric_limits<T>::quiet_NaN(),
                      last_time_,
                      event_count_,
                    });
            }
        }
    }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

    explicit
    VWBASVisitor(double interval,
                 double max_volume = 0,
                 distance_func f =
                     [](const I &idx1, const I &idx2) -> double {
                         return (static_cast<double>(idx2 - idx1));
                     })
        : dfunc_(f), interval_(interval), max_volume_(max_volume)  {   }

private:

    inline void accumulate_ (value_type bid_size, value_type ask_size,
                             value_type bid_price, value_type ask_price)  {

        if (max_volume_ == 0 ||
            (bid_size < max_volume_ && ask_size < max_volume_))  {
            vw_bid_price_accumulator_ += bid_price * bid_size;
            bid_price_accumulator_ += bid_price;
            cum_bid_price_accumulator_ += bid_price * bid_size;
            bid_volume_accumulator_ += bid_size;
            cum_bid_volume_accumulator_ += bid_size;
            if (bid_price > high_bid_price_)
                high_bid_price_ = bid_price;
            if (bid_price < low_bid_price_)
                low_bid_price_ = bid_price;
            if (bid_price > cum_high_bid_price_)
                cum_high_bid_price_ = bid_price;
            if (bid_price < cum_low_bid_price_)
                cum_low_bid_price_ = bid_price;

            vw_ask_price_accumulator_ += ask_price * ask_size;
            ask_price_accumulator_ += ask_price;
            cum_ask_price_accumulator_ += ask_price * ask_size;
            ask_volume_accumulator_ += ask_size;
            cum_ask_volume_accumulator_ += ask_size;
            if (ask_price > high_ask_price_)
                high_ask_price_ = ask_price;
            if (ask_price < low_ask_price_)
                low_ask_price_ = ask_price;
            if (ask_price > cum_high_ask_price_)
                cum_high_ask_price_ = ask_price;
            if (ask_price < cum_low_ask_price_)
                cum_low_ask_price_ = ask_price;

            event_count_ += 1;
            cum_event_count_ += 1;
        }
    }
    inline void reset_ (index_type index_value)  {

        vw_bid_price_accumulator_ = 0;
        vw_ask_price_accumulator_ = 0;
        bid_price_accumulator_ = 0;
        ask_price_accumulator_ = 0;
        bid_volume_accumulator_ = 0;
        ask_volume_accumulator_ = 0;
        last_time_ = index_value;
        started_ = true;
        event_count_ = 0;
        high_bid_price_ = 0;
        high_ask_price_ = 0;
        low_bid_price_ = std::numeric_limits<value_type>::max();
        low_ask_price_ = std::numeric_limits<value_type>::max();
    }

    result_type     result_ { };
    value_type      vw_bid_price_accumulator_ { 0 };
    value_type      vw_ask_price_accumulator_ { 0 };
    value_type      bid_price_accumulator_ { 0 };
    value_type      ask_price_accumulator_ { 0 };
    value_type      bid_volume_accumulator_ { 0 };
    value_type      ask_volume_accumulator_ { 0 };
    index_type      last_time_ { };
    bool            started_ { false };
    size_type       event_count_ { 0 };
    value_type      high_bid_price_ { 0 };
    value_type      high_ask_price_ { 0 };
    value_type      low_bid_price_ { std::numeric_limits<value_type>::max() };
    value_type      low_ask_price_ { std::numeric_limits<value_type>::max() };
    value_type      cum_high_bid_price_ { 0 };
    value_type      cum_high_ask_price_ { 0 };
    value_type      cum_low_bid_price_
        { std::numeric_limits<value_type>::max() };
    value_type      cum_low_ask_price_
        { std::numeric_limits<value_type>::max() };
    value_type      cum_bid_price_accumulator_ { 0 };
    value_type      cum_ask_price_accumulator_ { 0 };
    value_type      cum_bid_volume_accumulator_ { 0 };
    value_type      cum_ask_volume_accumulator_ { 0 };
    size_type       cum_event_count_ { 0 };
    distance_func   dfunc_;
    const double    interval_;
    const double    max_volume_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using vwbas_v = VWBASVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// This is meaningfull, only if the return series is close to normal
// distribution
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  SharpeRatioVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &asset_ret_begin, const H &asset_ret_end,
                const H &benchmark_ret_begin, const H &benchmark_ret_end)  {

        const size_type col_s = std::distance(asset_ret_begin, asset_ret_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        const size_type b_s =
            std::distance(benchmark_ret_begin, benchmark_ret_end);

        if (col_s != b_s || col_s <= 3)
            throw DataFrameError("SharpeRatioVisitor: column size must be > 3 "
                                 "and two column sizes must be equal");
#endif // HMDF_SANITY_EXCEPTIONS

        value_type  cum_ret { 0.0 };
        auto        a_citer { asset_ret_begin };

        if (! ds_only_)  {  // Sharpe Ratio
            StdVisitor<T, I>    std_vis { biased_ };
            const index_type    index_val { }; // Ignored

            std_vis.pre();
            for (auto b_citer { benchmark_ret_begin };
                 b_citer != benchmark_ret_end;
                 ++a_citer, ++b_citer) [[likely]]  {
                const value_type    val { *a_citer - *b_citer };

                std_vis (index_val, val);
                cum_ret += val;
            }
            std_vis.post();
            result_ = (cum_ret / T(col_s)) / std_vis.get_result();
        }
        else  {  // Sortino Ratio
            value_type  ret_prod { 0 };

            for (auto b_citer { benchmark_ret_begin };
                 b_citer != benchmark_ret_end;
                 ++a_citer, ++b_citer) [[likely]]  {
                const value_type    val { *a_citer - *b_citer };
                const value_type    min_val { val - min_rt_ };

                if (min_val < 0)
                    ret_prod += min_val * min_val;
                cum_ret += val;
            }
            result_ = (cum_ret / T(col_s)) / std::sqrt(ret_prod / T(col_s));
        }
    }

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    explicit
    SharpeRatioVisitor(bool biased = false,
                       bool downside_only = false,
                       value_type min_return = 0)
        : min_rt_(min_return), biased_(biased), ds_only_(downside_only)  {  }

private:

    result_type         result_ { 0 };
    const value_type    min_rt_;
    const bool          biased_;
    const bool          ds_only_;
};

template<typename T, typename I = unsigned long>
using sharper_v = SharpeRatioVisitor<T, I>;

// ----------------------------------------------------------------------------

// Relative Strength Index (RSI) is a technical indicator used in the
// analysis of financial markets. It is intended to chart the current and
// historical strength or weakness of a stock or market based on the closing
// prices of a recent trading period. The indicator should not be confused
// with relative strength.
// The RSI is classified as a momentum oscillator, measuring the velocity and
// magnitude of price movements.
// The RSI is most typically used on a 14-day (a parameter in this visitor)
// time frame, measured on a scale from 0 to 100.
// Traditional interpretation and usage of the RSI are that values of 70 or
// above indicate that a security is becoming overbought or overvalued and may
// be primed for a trend reversal or corrective pullback in price. An RSI
// reading of 30 or below indicates an oversold or undervalued condition.
//
// The input (column) to this visitor is assumed to be instrument prices.
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  RSIVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &prices_begin, const H &prices_end)  {

        const size_type col_s = std::distance(prices_begin, prices_end);

        // This data doesn't make sense
        //
        if (avg_period_ >= T(col_s - 3))  return;

        ReturnVisitor<T, I, A>  return_v { rp_ };

        return_v.pre();
        return_v (idx_begin, idx_end, prices_begin, prices_end);
        return_v.post();

        value_type          avg_up { 0 };
        value_type          avg_down { 0 };
        const value_type    avg_period_1 { avg_period_ - T(1) };

        for (size_type i { 1 }; i < size_type(avg_period_); ++i) [[likely]]  {
            const value_type    value = return_v.get_result()[i];

            if (value > 0)
                avg_up = (avg_up * avg_period_1 + value) / avg_period_;
            else if (value < 0)
                avg_down = (avg_down * avg_period_1 - value) / avg_period_;
        }

        result_type result(col_s, std::numeric_limits<T>::quiet_NaN());

        constexpr value_type    h { 100 };

        result[size_type(avg_period_ - 1)] =
            h - (h / (T(1) + avg_up / avg_down));
        for (size_type i { size_type(avg_period_) }; i < col_s; ++i)  {
            const value_type    value { return_v.get_result()[i] };

            if (value > 0)
                avg_up = (avg_up * avg_period_1 + value) / avg_period_;
            else if (value < 0)
                avg_down = (avg_down * avg_period_1 - value) / avg_period_;
            result[i] = h - (h / (T(1) + avg_up / avg_down));
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit RSIVisitor(return_policy rp, size_type avg_period = 14)
        : rp_(rp), avg_period_(T(avg_period))  {   }

private:

    const return_policy rp_;
    const value_type    avg_period_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using rsi_v = RSIVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// RSX is a "noise free" version of RSI, with no added lag.
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  RSXVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (avg_period_ >= col_s)
            throw DataFrameError("RSXVisitor: period must be < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        value_type  vc { 0 }, v1c { 0 };
        value_type  v4 { 0 }, v8 { 0 }, v10 { 0 }, v14 { 0 }, v18 { 0 },
                    v20 { 0 };
        value_type  f0 { 0 }, f8 { 0 }, f10 { 0 }, f18 { 0 }, f20 { 0 },
                    f28 { 0 }, f30 { 0 }, f38 { 0 },  f40 { 0 }, f48 { 0 },
                    f50 { 0 }, f58 { 0 }, f60 { 0 }, f68 { 0 }, f70 { 0 },
                    f78 { 0 }, f80 { 0 }, f88 { 0 }, f90 { 0 };

        constexpr value_type    zero { 0 };
        constexpr value_type    epsilon { std::numeric_limits<T>::epsilon() };
        constexpr value_type    half { 0.5 };
        constexpr value_type    one_half { 1.5 };
        constexpr value_type    one { 1 };
        constexpr value_type    two { 2 };
        constexpr value_type    three { 3 };
        constexpr value_type    five { 5 };
        constexpr value_type    fifty { 50 };
        constexpr value_type    hundred { 100 };

        result_type result(col_s, std::numeric_limits<T>::quiet_NaN());

        result[avg_period_ - 1] = 0;
        for (size_type i { size_type(avg_period_) };
             i < col_s; ++i) [[likely]]  {
            if (f90 == zero)  {
                f90 = one;
                f0 = zero;
                if (avg_period_ - one >= five)
                    f88 = avg_period_ - one;
                else
                    f88 = five;
                f8 = hundred * *(column_begin + i);
                f18 = three / (avg_period_ + two);
                f20 = one - f18;
            }
            else  {
                if (f88 <= f90)
                    f90 = f88 + one;
                else
                    f90 = f90 + one;
                f10 = f8;
                f8 = hundred * *(column_begin + i);
                v8 = f8 - f10;
                f28 = f20 * f28 + f18 * v8;
                f30 = f18 * f28 + f20 * f30;
                vc = one_half * f28 - half * f30;
                f38 = f20 * f38 + f18 * vc;
                f40 = f18 * f38 + f20 * f40;
                v10 = one_half * f38 - half * f40;
                f48 = f20 * f48 + f18 * v10;
                f50 = f18 * f48 + f20 * f50;
                v14 = one_half * f48 - half * f50;
                f58 = f20 * f58 + f18 * std::fabs(v8);
                f60 = f18 * f58 + f20 * f60;
                v18 = one_half * f58 - half * f60;
                f68 = f20 * f68 + f18 * v18;
                f70 = f18 * f68 + f20 * f70;
                v1c = one_half * f68 - half * f70;
                f78 = f20 * f78 + f18 * v1c;
                f80 = f18 * f78 + f20 * f80;
                v20 = one_half * f78 - half * f80;

                if (f88 >= f90 && f8 != f10)
                    f0 = one;
                if (f88 == f90 && f0 == zero)
                    f90 = zero;
            }

            if (f88 < f90 && v20 > epsilon)  {
                v4 = (v14 / v20 + one) * fifty;
                if (v4 > hundred)
                    v4 = hundred;
                if (v4 < zero)
                    v4 = zero;
            }
            else
                v4 = fifty;

            result[i] = v4;
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit RSXVisitor(size_type avg_period = 14)
        : avg_period_(T(avg_period))  {   }

private:

    OBO_PORT_DECL

    const value_type    avg_period_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using rsx_v = RSXVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Relative Volatility Index (RVI). Instead of adding up price changes
// like RSI based on price direction, the RVI adds up standard deviations
// based on price direction.
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  RVIVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const H &high_begin, const H &high_end,
                const H &low_begin, const H &low_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(high_begin, high_end)) ||
            col_s != size_type(std::distance(low_begin, low_end)) ||
            roll_period_ >= col_s)
            throw DataFrameError("RVIVisitor: all columns must be of the same "
                                 "size and roll period must be < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        rvi_(idx_begin, idx_end, close_begin, close_end, result_, col_s);

        result_type loc_result;

        rvi_(idx_begin, idx_end, high_begin, high_end, loc_result, col_s);
        for (size_type i = 0; i < col_s; ++i) [[likely]]
            result_[i] += loc_result[i];
        rvi_(idx_begin, idx_end, low_begin, low_end, loc_result, col_s);
        for (size_type i = 0; i < col_s; ++i) [[likely]]
            result_[i] = (result_[i] + loc_result[i]) / T(3);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    RVIVisitor(size_type roll_period = 14) : roll_period_(roll_period)  {   }

private:

    template <forward_iterator K, forward_iterator H>
    inline void
    rvi_(const K &idx_begin, const K &idx_end,
         const H &ts_begin, const H &ts_end,
         result_type &result, size_type col_s)  {

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        stdev_.pre();
        ret_v_.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [this,
                     &idx_begin, &idx_end,
                     &ts_begin, &ts_end]() -> void  {
                        this->stdev_(idx_begin, idx_end, ts_begin, ts_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [this,
                     &idx_begin, &idx_end,
                     &ts_begin, &ts_end]() -> void  {
                        this->ret_v_(idx_begin, idx_end, ts_begin, ts_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            stdev_ (idx_begin, idx_end, ts_begin, ts_end);
            ret_v_ (idx_begin, idx_end, ts_begin, ts_end);
        }
        stdev_.post();
        ret_v_.post();

        neg_.resize(col_s);
        std::copy(ret_v_.get_result().begin(), ret_v_.get_result().end(),
                  neg_.begin());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type          &pos =
                                this->ret_v_.get_result()[i];
                            value_type          &neg = this->neg_[i];
                            const value_type    stdev =
                                this->stdev_.get_result()[i];

                            if (pos < 0)  pos = 0;
                            if (neg > 0)  neg = 0;
                            else if (neg < 0)  neg = 1;

                            pos *= stdev;
                            neg *= stdev;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for_each_list3(ret_v_.get_result().begin(),
                           ret_v_.get_result().end(),
                           neg_.begin(), neg_.end(),
                           stdev_.get_result().begin(),
                           stdev_.get_result().end(),
                           []
                           (auto &pos, auto &neg, const auto &stdev) -> void  {
                               if (pos < 0)  pos = 0;
                               if (neg > 0)  neg = 0;
                               else if (neg < 0)  neg = 1;

                               pos *= stdev;
                               neg *= stdev;
                           });
        }

        ewm_pos_.pre();
        ewm_pos_ (idx_begin, idx_end,
                  ret_v_.get_result().begin(), ret_v_.get_result().end());
        ewm_pos_.post();

        ewm_neg_.pre();
        ewm_neg_ (idx_begin, idx_end, neg_.begin(), neg_.end());
        ewm_neg_.post();

        if (thread_level > 2)  {
            result.resize(col_s);
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    pos =
                                this->ewm_pos_.get_result()[i];
                            const value_type    neg =
                                this->ewm_neg_.get_result()[i];

                            result[i] = (T(100) * pos) / (pos + neg);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            result.clear();
            result.reserve(col_s);
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]
                result.push_back(
                    (T(100) * ewm_pos_.get_result()[i]) /
                    (ewm_pos_.get_result()[i] + ewm_neg_.get_result()[i]));
            }
    }

    const size_type roll_period_;
    result_type     result_ { };
    result_type     neg_ {  };

    SimpleRollAdopter<StdVisitor<T, I>, T, I, A>    stdev_
        { StdVisitor<T, I>(), roll_period_  };
    ReturnVisitor<T, I, A>                          ret_v_
        { return_policy::trinary };
    // Adjusting for finite series in both cases
    //
    ewm_v<T, I, A>                                  ewm_pos_
        { exponential_decay_spec::span, T(roll_period_), true };
    ewm_v<T, I, A>                                  ewm_neg_
        { exponential_decay_spec::span, T(roll_period_), true };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using rvi_v = RVIVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  HurstExponentVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    using RangeVec =
        std::vector<size_type, typename allocator_declare<size_type, A>::type>;

private:

    struct  range_data  {

        size_type   id { 0 };
        size_type   begin { 0 };
        size_type   end { 0 };
        value_type  mean { 0 };
        value_type  st_dev { 0 };
        value_type  rescaled_range { 0 };
    };

    using RangeDataVec =
        std::vector<range_data,
                    typename allocator_declare<range_data, A>::type>;

public:

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        RangeDataVec        buckets;
        MeanVisitor<T, I>   mv;
        StdVisitor<T, I>    sv;

        // Calculate each range basic stats
        //
        buckets.reserve(std::accumulate(ranges_.begin(), ranges_.end(), 0));
        for (auto range : ranges_) [[likely]]  {
            const size_type ch_size { col_s / range };

            for (size_type i { 0 }; i < range; ++i) [[likely]]  {
                range_data  rd;

                rd.id = range;
                rd.begin = ch_size * i;
                rd.end = ch_size * i + ch_size;

                mv.pre();
                sv.pre();
                mv(idx_begin, idx_end,
                   column_begin + rd.begin, column_begin + rd.end);
                sv(idx_begin, idx_end,
                   column_begin + rd.begin, column_begin + rd.end);
                mv.post();
                sv.post();

                rd.mean = mv.get_result();
                rd.st_dev = sv.get_result();
                buckets.push_back(rd);
            }
        }

        // Calculate the so-called rescaled range
        //
        for (auto &bucket : buckets) [[likely]]  {
            value_type  total { 0 };  // Cumulative sum (CumSum)
            value_type  max_dev { std::numeric_limits<T>::min() };
            value_type  min_dev { std::numeric_limits<T>::max() };

            for (size_type i { bucket.begin }; i < bucket.end; ++i)  {
                total += *(column_begin + i) - bucket.mean;
                if (total > max_dev)  max_dev = total;
                if (total < min_dev)  min_dev = total;
            }
            bucket.rescaled_range = (max_dev - min_dev) / bucket.st_dev;
        }

        // Caluculate Hurst exponent
        //
        size_type   prev_id { 0 };
        size_type   prev_size { 0 };
        value_type  count { 0 };
        value_type  total_rescaled_range  { 0 };
        std::vector<
            value_type,
            typename allocator_declare<value_type, A>::type> log_rescaled_mean;
        std::vector<
            value_type,
            typename allocator_declare<value_type, A>::type> log_size;

        log_rescaled_mean.reserve(ranges_.size());
        log_size.reserve(ranges_.size());
        for (const auto &bucket : buckets) [[likely]]  {
            if (bucket.id != prev_id && count > 0)  {
                log_size.push_back(std::log(prev_size));
                log_rescaled_mean.push_back(
                    std::log(total_rescaled_range / count));
                total_rescaled_range = 0;
                count = 0;
            }
            total_rescaled_range += bucket.rescaled_range;
            count += 1;
            prev_size = bucket.end - bucket.begin;
            prev_id = bucket.id;
        }
        if (count > 0)  {
            log_size.push_back(std::log(prev_size));
            log_rescaled_mean.push_back(
                std::log(total_rescaled_range / count));
        }

        PolyFitVisitor<T, I, A> pfv { 1 };  // First degree

        pfv.pre();
        pfv (idx_begin, idx_end,
             log_size.begin(), log_size.end(),
             log_rescaled_mean.begin(), log_rescaled_mean.end());
        pfv.post();

        exponent_ = pfv.get_slope();
    }

    inline void pre ()  { exponent_ = -1; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (exponent_); }

    explicit
    HurstExponentVisitor(RangeVec &&ranges) : ranges_ (std::move(ranges))  {  }

private:

    const RangeVec  ranges_;
    result_type     exponent_ { -1 };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using hexpo_v = HurstExponentVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  MassIndexVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &high_begin, const H &high_end,
                const H &low_begin, const H &low_end)  {

        const size_type col_s = std::distance(high_begin, high_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            fast_ >= slow_)
            throw DataFrameError("MassIndexVisitor: column sizes must equal "
                                 "and fast < slow");
#endif // HMDF_SANITY_EXCEPTIONS

        nzr_v<T, I, A>  non_z_range;

        non_z_range.pre();
        non_z_range(idx_begin, idx_end,
                    high_begin, high_end, low_begin, low_end);
        non_z_range.post();

        result_type result { std::move(non_z_range.get_result()) };
        erm_t       fast_roller (exponential_decay_spec::span, fast_);

        fast_roller.pre();
        fast_roller(idx_begin, idx_end, result.begin(), result.end());
        fast_roller.post();

        // Backfill the result with simple averges
        //
        value_type  sum { 0 };

        for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
            auto    &fr_value = fast_roller.get_result()[i];

            if (is_nan__(fr_value)) [[unlikely]]  {
                sum += result[i];
                fr_value = sum / T(i + 1);
            }
            else [[likely]]  break;
        }
        result = std::move(fast_roller.get_result());
        fast_roller.pre();
        fast_roller(idx_begin, idx_end, result.begin(), result.end());
        fast_roller.post();

        // Backfill the result with simple averges
        //
        sum = 0;
        for (size_type i { 0 }; i < col_s; ++i)  {
            auto    &fr_value = fast_roller.get_result()[i];

            if (is_nan__(fr_value))  {
                sum += result[i];
                fr_value = sum / T(i + 1);
            }
            else [[likely]]  break;
        }

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&fast_roller = std::as_const(fast_roller.get_result()),
                     &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] /= fast_roller[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(fast_roller.get_result().begin(),
                           fast_roller.get_result().begin() + col_s,
                           result.begin(),
                           result.begin(),
                           [](const auto &fr, const auto &r) -> value_type  {
                               return (r / fr);
                           });
        }

        srs_t   slow_roller { std::move(SumVisitor<T, I>()), slow_ };

        slow_roller.pre();
        slow_roller(idx_begin, idx_end, result.begin(), result.end());
        slow_roller.post();
        result = std::move(slow_roller.get_result());

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    MassIndexVisitor(size_type fast_period = 9, size_type slow_period = 25)
        : slow_(slow_period), fast_(fast_period)  {  }

private:

    using erm_t = ewm_v<T, I, A>;
    using srs_t = SimpleRollAdopter<SumVisitor<T, I>, T, I, A>;

    result_type     result_ {  };
    const size_type slow_;
    const size_type fast_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using mass_idx_v = MassIndexVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  HullRollingMeanVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ <= 1)  return;

        using wma_t = SimpleRollAdopter<WeightedMeanVisitor<T, I>, T, I, A>;

        GET_COL_SIZE

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        wma_t   wma_half { WeightedMeanVisitor<T, I>(), roll_count_ / 2 };
        wma_t   wma_full { WeightedMeanVisitor<T, I>(), roll_count_ };

        wma_half.pre();
        wma_full.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&wma_half,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        wma_half(idx_begin, idx_end, column_begin, column_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&wma_full,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        wma_full(idx_begin, idx_end, column_begin, column_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            wma_half (idx_begin, idx_end, column_begin, column_end);
            wma_full (idx_begin, idx_end, column_begin, column_end);
        }
        wma_full.post();
        wma_half.post();

        result_type result { std::move(wma_half.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s - 1,
                    [&wma_full = std::as_const(wma_full.get_result()),
                     &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  &r = result[i];

                            r = T(2) * r - wma_full[i];
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(wma_full.get_result().begin(),
                           wma_full.get_result().begin() + (col_s - 1),
                           result.begin(),
                           result.begin(),
                           [](const auto &wf, const auto &r) -> value_type  {
                               return (T(2) * r - wf);
                           });
        }

        wma_t   wma_sqrt { WeightedMeanVisitor<T, I>(),
                           size_type(std::sqrt(roll_count_)) };

        wma_sqrt.pre();
        wma_sqrt (idx_begin, idx_end, result.begin(), result.end());
        wma_sqrt.post();

        result = std::move(wma_sqrt.get_result());
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    HullRollingMeanVisitor(size_type r_count = 10) : roll_count_(r_count)  {  }

private:

    OBO_PORT_DECL

    const size_type roll_count_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using hull_mean_v = HullRollingMeanVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  RollingMidValueVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end)  {

        const size_type col_s = std::distance(high_begin, high_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            roll_count_ == 0)
            throw DataFrameError("RollingMidValueVisitor: column sizes must "
                                 "be equal and roll count > 0");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result;

        result.reserve(col_s);
        for (size_type i { 0 }; i < roll_count_ - 1 && i < col_s; ++i)
            result.push_back(std::numeric_limits<T>::quiet_NaN());

        value_type              min_v { *low_begin };
        value_type              max_v { *high_begin };
        constexpr value_type    p5 { 0.5 };

        for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
            const size_type limit { i + roll_count_ };

            if (limit <= col_s)  {
                for (size_type j { i }; j < limit; ++j)  {
                    const value_type    low { *(low_begin + j) };
                    const value_type    high { *(high_begin + j) };

                    if (low < min_v)  min_v = low;
                    if (high > max_v)  max_v = high;
                }
                result.push_back((min_v + max_v) * p5);
                if (limit < col_s)  {
                    min_v = *(low_begin + limit);
                    max_v = *(high_begin + limit);
                }
            }
            else  break;
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    RollingMidValueVisitor(size_type r_count) : roll_count_(r_count)  {   }

private:

    const size_t    roll_count_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using mid_val_v = RollingMidValueVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  DrawdownVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        CumMaxVisitor<T, I, A>  cm_v;

        cm_v.pre();
        cm_v (idx_begin, idx_end, column_begin, column_end);
        cm_v.post();

        const auto              &cm_result { cm_v.get_result() };
        constexpr value_type    one { 1 };

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            drawdown_.resize(col_s);
            pct_drawdown_.resize(col_s);
            log_drawdown_.resize(col_s);

            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin,
                     &cm_result = std::as_const(cm_result), this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type   col_val { *(column_begin + i) };
                            const value_type   cm_val { cm_result[i] };

                            this->drawdown_[i] = cm_val - col_val;
                            this->pct_drawdown_[i] = one - col_val / cm_val;
                            this->log_drawdown_[i] =
                                std::log(cm_val / col_val);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            drawdown_.reserve(col_s);
            pct_drawdown_.reserve(col_s);
            log_drawdown_.reserve(col_s);
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                const value_type    col_val { *(column_begin + i) };
                const value_type    cm_val { cm_result[i] };

                drawdown_.push_back(cm_val - col_val);
                pct_drawdown_.push_back(one - col_val / cm_val);
                log_drawdown_.push_back(std::log(cm_val / col_val));
            }
        }
    }

    inline void pre ()  {

        drawdown_.clear();
        pct_drawdown_.clear();
        log_drawdown_.clear();
    }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (drawdown_); }
    inline result_type &get_result ()  { return (drawdown_); }
    inline const result_type &
    get_log_drawdown () const  { return (log_drawdown_); }
    inline result_type &get_log_drawdown ()  { return (log_drawdown_); }
    inline const result_type &
    get_pct_drawdown () const  { return (pct_drawdown_); }
    inline result_type &get_pct_drawdown ()  { return (pct_drawdown_); }

private:

    result_type drawdown_ { };
    result_type pct_drawdown_ { };
    result_type log_drawdown_ { };
};

// ----------------------------------------------------------------------------

// Also called Stochastic Oscillator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  WilliamPrcRVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_count_ == 0)
            throw DataFrameError("WilliamPrcRVisitor: column sizes must be "
                                 "equal and roll count > 0");
#endif // HMDF_SANITY_EXCEPTIONS

        SimpleRollAdopter<MinVisitor<T, I>, T, I, A>   min_v {
            MinVisitor<T, I>(), roll_count_ };
        SimpleRollAdopter<MaxVisitor<T, I>, T, I, A>   max_v {
            MaxVisitor<T, I>(), roll_count_ };

        min_v.pre();
        max_v.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&min_v,
                     &idx_begin, &idx_end,
                     &low_begin, &low_end]() -> void  {
                        min_v(idx_begin, idx_end, low_begin, low_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&max_v,
                     &idx_begin, &idx_end,
                     &high_begin, &high_end]() -> void  {
                        max_v(idx_begin, idx_end, high_begin, high_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            min_v (idx_begin, idx_end, low_begin, low_end);
            max_v (idx_begin, idx_end, high_begin, high_end);
        }
        min_v.post();
        max_v.post();

        result_type result { std::move(max_v.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&min_v = std::as_const(min_v.get_result()),
                     &result, &close_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    low = min_v[i];
                            value_type          &res = result[i];

                            res = T(100) *
                                  ((*(close_begin + i) - low) /
                                   (res - low) - T(1));
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for_each_list3(min_v.get_result().begin(),
                           min_v.get_result().end(),
                           result.begin(), result.end(),
                           close_begin, close_end,
                           [](const auto &low,
                              auto &res,
                              const auto &close) -> void  {
                               res = T(100) *
                                     ((close - low) / (res - low) - T(1));
                           });
        }

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    WilliamPrcRVisitor(size_type r_count = 14) : roll_count_(r_count)  {   }

private:

    const size_type roll_count_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using willp_v = WilliamPrcRVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Psychological Line (PSL) is an oscillator-type indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  PSLVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator()(const K &idx_begin, const K &idx_end,
               const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            result_.resize(col_s);
            result_[0] = 0;

            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&close_begin, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    ct = *(close_begin + i);
                            const value_type    ct_1 =
                                *(close_begin + (i - 1));

                            this->result_[i] = ((ct - ct_1) > 0) ? 1 : 0;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            result_.reserve(col_s);
            result_.push_back(0);
            for (size_type i { 1 }; i < col_s; ++i) [[likely]]
                result_.push_back(
                    (*(close_begin + i) - *(close_begin + (i - 1)) > 0)
                    ? 1 : 0);
        }
        calculate_(idx_begin, idx_end);
    }

    template <forward_iterator K, forward_iterator H>
    inline void
    operator()(const K &idx_begin, const K &idx_end,
               const H &close_begin, const H &close_end,
               const H &open_begin, const H &open_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(open_begin, open_end)))
            throw DataFrameError("PSLVisitor: column sizes must be equal");
#endif // HMDF_SANITY_EXCEPTIONS

        result_.resize(col_s);
        if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&close_begin, &open_begin, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    c = *(close_begin + i);
                            const value_type    o = *(open_begin + i);

                            this->result_[i] = ((c - o) > 0) ? 1 : 0;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(close_begin, close_begin + col_s,
                           open_begin,
                           result_.begin(),
                           [](const auto &c, const auto &o) -> value_type  {
                               return (((c - o) > 0) ? 1 : 0);
                           });
        }
        calculate_(idx_begin, idx_end);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    PSLVisitor(size_type r_count = 14)
        : roll_count_(r_count),
          thread_level_(ThreadGranularity::get_thread_level())  {   }

private:

    template <forward_iterator K>
    inline void calculate_(const K &idx_begin, const K &idx_end)  {

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>   sum_r {
            SumVisitor<T, I>(), roll_count_ };

        sum_r.pre();
        sum_r(idx_begin, idx_end, result_.begin(), result_.end());
        sum_r.post();

        const size_type col_s { result_.size() };

        if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [this, &sum_r = std::as_const(sum_r.get_result())]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            this->result_[i] =
                               sum_r[i] * T(100) / T(this->roll_count_);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(sum_r.get_result().begin(),
                           sum_r.get_result().begin() + col_s,
                           result_.begin(),
                           [this](const auto &s) -> value_type  {
                               return (s * T(100) / T(this->roll_count_));
                           });
        }
    }

    const size_type roll_count_;
    const long      thread_level_;
    result_type     result_ { };
};

// ----------------------------------------------------------------------------

// Commodity Channel Index (CCI)
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  CCIVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_count_ == 0)
            throw DataFrameError("CCIVisitor: All columns must be of equal "
                                 "size and roll count > 0");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result;

        if (thread_level > 2)  {
            result.resize(col_s);

            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result,
                     &low_begin = std::as_const(low_begin),
                     &high_begin = std::as_const(high_begin),
                     &close_begin = std::as_const(close_begin)]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] = (*(low_begin + i) +
                                         *(high_begin + i) +
                                         *(close_begin + i)) / T(3);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            result.reserve(col_s);
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]
                result.push_back((*(low_begin + i) +
                                  *(high_begin + i) +
                                  *(close_begin + i)) / T(3));
        }

        SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   avg_v {
            MeanVisitor<T, I>(), roll_count_ };

        avg_v.pre();
        avg_v (idx_begin, idx_end, result.begin(), result.end());
        avg_v.post();

        SimpleRollAdopter<MADVisitor<T, I>, T, I, A>    mad_v {
            MADVisitor<T, I>(mad_type::mean_abs_dev_around_mean),
            roll_count_ };

        mad_v.pre();
        mad_v (idx_begin, idx_end, result.begin(), result.end());
        mad_v.post();

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result,
                     &avg_v = std::as_const(avg_v.get_result()),
                     &mad_v = std::as_const(mad_v.get_result()),
                     this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  &r = result[i];

                            r = (r - avg_v[i]) /
                                (this->lambert_const_ * mad_v[i]);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i)
                result[i] =
                    (result[i] - avg_v.get_result()[i]) /
                    (lambert_const_ * mad_v.get_result()[i]);
        }

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    CCIVisitor(size_type r_count = 14,
               value_type lambert_const = T(0.015))
        : roll_count_(r_count), lambert_const_(lambert_const)  {   }

private:

    const size_type     roll_count_;
    const value_type    lambert_const_;
    result_type         result_ { };
};

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  GarmanKlassVolVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(open_begin, open_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_count_ >= (col_s - 1))
            throw DataFrameError("GarmanKlassVolVisitor: All columns must be "
                                 "of equal size and roll count < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        // 2 * log(2) - 1
        //
        constexpr value_type    cf { T(2) * T(0.6931471805599453) - T(1) };
        result_type             result(
            col_s, std::numeric_limits<T>::quiet_NaN());

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    roll_count_,
                    col_s,
                    [this, &result,
                     &low_begin, &high_begin, &open_begin, &close_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  sum { 0 };
                            size_type   cnt { 0 };

                            for (size_type j { i - this->roll_count_ };
                                 j < i; ++j) {
                                const value_type    hl_rt =
                                    std::log(*(high_begin + j) /
                                             *(low_begin + j));
                                const value_type    co_rt =
                                    std::log(*(close_begin + j) /
                                             *(open_begin + j));

                                sum += T(0.5) * hl_rt * hl_rt -
                                       cf * co_rt * co_rt;
                                cnt += 1;
                            }
                            result[i] = std::sqrt((sum / T(cnt)) *
                                                  this->trading_periods_);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { roll_count_ }; i < col_s; ++i) [[likely]]  {
                value_type  sum { 0 };
                size_type   cnt { 0 };

                for (size_type j { i - roll_count_ }; j < i; ++j) {
                    const value_type    hl_rt =
                        std::log(*(high_begin + j) / *(low_begin + j));
                    const value_type    co_rt =
                        std::log(*(close_begin + j) / *(open_begin + j));

                    sum += T(0.5) * hl_rt * hl_rt - cf * co_rt * co_rt;
                    cnt += 1;
                }
                result[i] = std::sqrt((sum / T(cnt)) * trading_periods_);
            }
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    GarmanKlassVolVisitor(size_type roll_count = 30,
                          size_type trading_periods = 252)
        : roll_count_(roll_count), trading_periods_(trading_periods)  {  }

private:

    result_type     result_ {  };
    const size_type roll_count_;
    const size_type trading_periods_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using gk_vol_v = GarmanKlassVolVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  YangZhangVolVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (roll_count_ == 0 ||
            col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(open_begin, open_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_count_ >= (col_s - 1))
            throw DataFrameError("YangZhangVolVisitor: All columns must be of "
                                 "equal size and roll count > 0");
#endif // HMDF_SANITY_EXCEPTIONS

        const value_type    k {
            T(0.34) / (T(1) + T(roll_count_ + 1) / T(roll_count_ - 1)) };
        const value_type    one_k { T(1) - k };
        const value_type    norm { T(1) / T(roll_count_ - 1) };
        result_type         result(col_s, std::numeric_limits<T>::quiet_NaN());

        if (col_s >= (ThreadPool::MUL_THR_THHOLD / 2) &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    roll_count_,
                    col_s,
                    [this, &result,
                     k, one_k, norm,
                     &low_begin, &high_begin, &open_begin, &close_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  c_vol_sum { 0 };
                            value_type  o_vol_sum { 0 };
                            value_type  rs_vol_sum { 0 };

                            for (size_type j { i - this->roll_count_ };
                                 j < i; ++j) [[likely]]  {
                                const value_type    open { *(open_begin + j) };
                                const value_type    close {
                                    *(close_begin + j) };
                                const value_type    ho_rt {
                                    std::log(*(high_begin + j) / open) };
                                const value_type    lo_rt {
                                    std::log(*(low_begin + j) / open) };
                                const value_type    co_rt {
                                    std::log(close/ open) };
                                const value_type    oc_rt {
                                    j > 0
                                        ? std::log(open /
                                                   *(close_begin + (j - 1)))
                                        : std::numeric_limits<T>::quiet_NaN()};
                                const value_type    cc_rt {
                                    j > 0
                                        ? std::log(close /
                                                   *(close_begin + (j - 1)))
                                        : std::numeric_limits<T>::quiet_NaN()};
                                // Rogers-Satchell volatility
                                const value_type    rs_vol {
                                    ho_rt * (ho_rt - co_rt) +
                                    lo_rt * (lo_rt - co_rt) };

                                c_vol_sum += cc_rt * cc_rt * norm;
                                o_vol_sum += oc_rt * oc_rt * norm;
                                rs_vol_sum += rs_vol * norm;
                            }
                            result[i] =
                                std::sqrt(o_vol_sum +
                                          k * c_vol_sum +
                                          one_k * rs_vol_sum) *
                                std::sqrt(this->trading_periods_);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { roll_count_ }; i < col_s; ++i) [[likely]]  {
                value_type  c_vol_sum { 0 };
                value_type  o_vol_sum { 0 };
                value_type  rs_vol_sum { 0 };

                for (size_type j { i - roll_count_ }; j < i; ++j) [[likely]]  {
                    const value_type    open { *(open_begin + j) };
                    const value_type    close { *(close_begin + j) };
                    const value_type    ho_rt {
                        std::log(*(high_begin + j) / open) };
                    const value_type    lo_rt {
                        std::log(*(low_begin + j) / open) };
                    const value_type    co_rt { std::log(close/ open) };
                    const value_type    oc_rt {
                        j > 0
                            ? std::log(open / *(close_begin + (j - 1)))
                            : std::numeric_limits<T>::quiet_NaN() };
                    const value_type    cc_rt {
                        j > 0
                            ? std::log(close / *(close_begin + (j - 1)))
                            : std::numeric_limits<T>::quiet_NaN() };
                    // Rogers-Satchell volatility
                    const value_type    rs_vol {
                        ho_rt * (ho_rt - co_rt) + lo_rt * (lo_rt - co_rt) };

                    c_vol_sum += cc_rt * cc_rt * norm;
                    o_vol_sum += oc_rt * oc_rt * norm;
                    rs_vol_sum += rs_vol * norm;
                }
                result[i] =
                    std::sqrt(o_vol_sum + k * c_vol_sum + one_k * rs_vol_sum) *
                    std::sqrt(trading_periods_);
            }
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    YangZhangVolVisitor(size_type roll_count = 30,
                        size_type trading_periods = 252)
        : roll_count_(roll_count), trading_periods_(trading_periods)  {  }

private:

    result_type     result_ {  };
    const size_type roll_count_;
    const size_type trading_periods_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using yz_vol_v = YangZhangVolVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Kaufman's Adaptive Moving Average
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  KamaVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ < 2)  return;

        GET_COL_SIZE2

        result_type change_diff (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i { roll_count_ }; i < col_s; ++i)
            change_diff[i] =
                std::fabs(*(column_begin + (i - roll_count_)) -
                          *(column_begin + i));

        result_type peer_diff (col_s, std::numeric_limits<T>::quiet_NaN());

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&peer_diff, &column_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            peer_diff[i] =
                                std::fabs(*(column_begin + (i - 1)) -
                                          *(column_begin + i));
                    });

        }
        else  {
            for (size_type i { 1 }; i < col_s; ++i) [[likely]]
                peer_diff[i] =
                    std::fabs(*(column_begin + (i - 1)) - *(column_begin + i));
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    vol {
            SumVisitor<T, I>(), roll_count_ };

        vol.pre();
        vol(idx_begin, idx_end, peer_diff.begin(), peer_diff.end());
        vol.post();

        result_type result(col_s, std::numeric_limits<T>::quiet_NaN());

        result[roll_count_ - 1] = 0;
        for (size_type i { roll_count_ }; i < col_s; ++i) [[likely]]  {
            const value_type    exp_ratio {
                change_diff[i] / vol.get_result()[i] };
            value_type          smoothing_const {
                exp_ratio * (fast_sc_ - slow_sc_) + slow_sc_ };

            smoothing_const *= smoothing_const;
            result[i] = smoothing_const * *(column_begin + i) +
                        (T(1) - smoothing_const) * result[i - 1];
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    KamaVisitor(size_type roll_count = 10,
                size_type fast_smoothing_const = 2,
                size_type slow_smoothing_const = 30)
        : roll_count_(roll_count),
          fast_sc_(T(2) / T(fast_smoothing_const + 1)),
          slow_sc_(T(2) / T(slow_smoothing_const + 1))  {   }

private:

    OBO_PORT_DECL

    const size_type     roll_count_;
    const value_type    fast_sc_;
    const value_type    slow_sc_;
    result_type         result_ { };
};

// ----------------------------------------------------------------------------

// Fisher Transform Indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  FisherTransVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end)  {

        const size_type col_s = std::distance(low_begin, low_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_count_ > (col_s - 1))
            throw DataFrameError("FisherTransVisitor: All columns must be of "
                                 "equal size and roll count < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type mid_hl(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&mid_hl, &low_begin, &high_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            mid_hl[i] = (*(low_begin + i) +
                                         *(high_begin + i)) * T(0.5);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i)
                mid_hl[i] = (*(low_begin + i) + *(high_begin + i)) * T(0.5);
        }

        SimpleRollAdopter<MaxVisitor<T, I>, T, I, A>   max_v {
            MaxVisitor<T, I>(), roll_count_ };
        SimpleRollAdopter<MinVisitor<T, I>, T, I, A>   min_v {
            MinVisitor<T, I>(), roll_count_ };

        max_v.pre();
        max_v (idx_begin, idx_end, mid_hl.begin(), mid_hl.end());
        max_v.post();
        min_v.pre();
        min_v (idx_begin, idx_end, mid_hl.begin(), mid_hl.end());
        min_v.post();

        result_type result(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&min_v = std::as_const(min_v.get_result()),
                     &max_v = std::as_const(max_v.get_result()),
                     &result, &mid_hl = std::as_const(mid_hl)]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    low { min_v[i] };
                            const value_type    diff { max_v[i] - low };

                            result[i] =
                                ((mid_hl[i] - low) /
                                 (diff >= T(0.001) ? diff : T(0.001))) -
                                T(0.5);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                const value_type    low { min_v.get_result()[i] };
                const value_type    diff { max_v.get_result()[i] - low };

                result[i] = ((mid_hl[i] - low) /
                             (diff >= T(0.001) ? diff : T(0.001))) - T(0.5);
            }
        }

        size_type   i = 0;
        value_type  val = 0;

        // This is done for effciency, so we do not use a third vector
        //
        std::swap(result, mid_hl);
        for ( ; i < roll_count_ - 1; ++i)
            result[i] = std::numeric_limits<T>::quiet_NaN();
        result[i++] = 0;
        for ( ; i < col_s; ++i)  {
            val = T(0.66) * mid_hl[i] + T(0.67) * val;
            if (val < T(-0.99))  val = -0.999;
            else if (val > T(0.99))  val = 0.999;
            result[i] =
                T(0.5) *
                (std::log((T(1) + val) / (T(1) - val)) + result[i - 1]);
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    FisherTransVisitor(size_type roll_count = 9) : roll_count_(roll_count) {  }

private:

    result_type     result_ {  };
    const size_type roll_count_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ftrans_v = FisherTransVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Percentage Price Oscillator (PPO)
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  PercentPriceOSCIVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (fast_ >= slow_)
            throw DataFrameError(
                "PercentPriceOSCIVisitor: fast must be < slow");
#endif // HMDF_SANITY_EXCEPTIONS

        srs_t   fast_roller { std::move(MeanVisitor<T, I>()), fast_ };

        fast_roller.pre();
        fast_roller(idx_begin, idx_end, close_begin, close_end);
        fast_roller.post();

        srs_t   slow_roller { std::move(MeanVisitor<T, I>()), slow_ };

        slow_roller.pre();
        slow_roller(idx_begin, idx_end, close_begin, close_end);
        slow_roller.post();

        result_type result { std::move(slow_roller.get_result()) };

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result,
                     &fast_roller = std::as_const(fast_roller.get_result())]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  &r { result[i] };

                            r = (T(100) * (fast_roller[i] - r)) / r;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i)  {
                value_type  &r { result[i] };

                r = (T(100) * (fast_roller.get_result()[i] - r)) / r;
            }
        }

        erm_t   signal_roller (exponential_decay_spec::span, signal_);

        signal_roller.pre();
        signal_roller(idx_begin + slow_, idx_end,
                      result.begin() + slow_, result.end());
        signal_roller.post();

        histogram_.reserve(col_s);
        for (size_type i { 0 }; i < slow_; ++i) [[likely]]
            histogram_.push_back(std::numeric_limits<T>::quiet_NaN());

        const size_type new_col_s {
            std::min(col_s, signal_roller.get_result().size()) };

        std::transform(result.begin() + slow_, result.begin() + new_col_s,
                       signal_roller.get_result().begin() + slow_,
                       std::back_inserter(histogram_),
                       [](const auto &r, const auto &sr) -> value_type  {
                           return (r - sr);
                       });
        result_.swap(result);
    }

    inline void pre ()  { result_.clear(); histogram_.clear(); }
    inline void post ()  {  }
    DEFINE_RESULT

    explicit
    PercentPriceOSCIVisitor(size_type fast_period = 12,
                            size_type slow_period = 26,
                            size_type signal_line = 9)
        : slow_(slow_period), fast_(fast_period), signal_(signal_line)  {  }

private:

    using erm_t = ewm_v<T, I, A>;
    using srs_t = SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>;

    result_type     result_ {  };
    result_type     histogram_ {  };
    const size_type slow_;
    const size_type fast_;
    const size_type signal_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using pp_osc_v = PercentPriceOSCIVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  SlopeVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if ((as_angle_ && (! in_degrees_)) || periods_ < 2 ||
            periods_ >= col_s)
            throw DataFrameError("SlopeVisitor: as_angle must be in degrees "
                                 "and periods >= 2 and < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        DiffVisitor<T, I, A>    diff (periods_, false);

        diff.pre();
        diff (idx_begin, idx_end, column_begin, column_end);
        diff.post();

        result_type             result { std::move(diff.get_result()) };
        constexpr value_type    pi_180 { T(180) / T(M_PI) };

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  &r = result[i];

                            r /= T(this->periods_);
                            if (this->as_angle_)  {
                                r = std::atan(r);
                                if (this->in_degrees_)
                                    r *= pi_180;
                            }
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                value_type  &r = result[i];

                r /= T(periods_);
                if (as_angle_)  {
                    r = std::atan(r);
                    if (in_degrees_)
                        r *= pi_180;
                }
            }
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    SlopeVisitor(size_type periods = 1,
                 bool as_angle = false,
                 bool in_degrees = false)
        : periods_(periods), as_angle_(as_angle), in_degrees_(in_degrees)  {  }

private:

    OBO_PORT_DECL

    const size_type periods_;
    const bool      as_angle_;
    const bool      in_degrees_;
    result_type     result_ { };
};

// ----------------------------------------------------------------------------

// Ultimate Oscillator indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  UltimateOSCIVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("UltimateOSCIVisitor: All columns must be of "
                                 "equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type max_high(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type min_low(col_s, std::numeric_limits<T>::quiet_NaN());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&max_high, &min_low,
                     &close_begin, &high_begin, &low_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    close =
                                *(close_begin + (i - 1));

                            max_high[i] = std::max(*(high_begin + i), close);
                            min_low[i] = std::min(*(low_begin + i), close);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 1 }; i < col_s; ++i)  {
                const value_type    close = *(close_begin + (i - 1));

                max_high[i] = std::max(*(high_begin + i), close);
                min_low[i] = std::min(*(low_begin + i), close);
            }
        }

        result_type buying_pressure(col_s);
        result_type true_range(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&max_high, &min_low, &close_begin,
                     &buying_pressure, &true_range]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    ml = min_low[i];

                            buying_pressure[i] = *(close_begin + i) - ml;
                            true_range[i] = max_high[i] - ml;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i)  {
                const value_type    ml = min_low[i];

                buying_pressure[i] = *(close_begin + i) - ml;
                true_range[i] = max_high[i] - ml;
            }
        }

        ssr_t   fast_bp_sum { SumVisitor<T, I> { true }, fast_ };
        ssr_t   fast_tr_sum { SumVisitor<T, I> { true }, fast_ };

        fast_bp_sum.pre();
        fast_tr_sum.pre();
        fast_bp_sum (idx_begin, idx_end,
                     buying_pressure.begin(), buying_pressure.end());
        fast_tr_sum (idx_begin, idx_end, true_range.begin(), true_range.end());
        fast_bp_sum.post();
        fast_tr_sum.post();

        ssr_t   med_bp_sum { SumVisitor<T, I> { true }, medium_ };
        ssr_t   med_tr_sum { SumVisitor<T, I> { true }, medium_ };

        med_bp_sum.pre();
        med_tr_sum.pre();
        med_bp_sum (idx_begin, idx_end,
                    buying_pressure.begin(), buying_pressure.end());
        med_tr_sum (idx_begin, idx_end, true_range.begin(), true_range.end());
        med_bp_sum.post();
        med_tr_sum.post();

        ssr_t   slow_bp_sum { SumVisitor<T, I> { true }, slow_ };
        ssr_t   slow_tr_sum { SumVisitor<T, I> { true }, slow_ };

        slow_bp_sum.pre();
        slow_tr_sum.pre();
        slow_bp_sum (idx_begin, idx_end,
                     buying_pressure.begin(), buying_pressure.end());
        slow_tr_sum (idx_begin, idx_end, true_range.begin(), true_range.end());
        slow_bp_sum.post();
        slow_tr_sum.post();

        const value_type    total_weights { slow_w_ + fast_w_ + medium_w_ };
        result_type         result(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [this, &result, total_weights,
                     &fast_bp_sum = std::as_const(fast_bp_sum.get_result()),
                     &fast_tr_sum = std::as_const(fast_tr_sum.get_result()),
                     &med_bp_sum = std::as_const(med_bp_sum.get_result()),
                     &med_tr_sum = std::as_const(med_tr_sum.get_result()),
                     &slow_bp_sum = std::as_const(slow_bp_sum.get_result()),
                     &slow_tr_sum = std::as_const(slow_tr_sum.get_result())]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    weights {
                                (this->fast_w_ *
                                 (fast_bp_sum[i] / fast_tr_sum[i]) +
                                 this->medium_w_ *
                                 (med_bp_sum[i] / med_tr_sum[i]) +
                                 this->slow_w_ *
                                 (slow_bp_sum[i] / slow_tr_sum[i])) *
                                T(100) };

                            result[i] = weights / total_weights;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                const value_type    weights {
                    (fast_w_ *
                     (fast_bp_sum.get_result()[i] /
                      fast_tr_sum.get_result()[i]) +
                     medium_w_ *
                     (med_bp_sum.get_result()[i] /
                      med_tr_sum.get_result()[i]) +
                     slow_w_ *
                     (slow_bp_sum.get_result()[i] /
                      slow_tr_sum.get_result()[i])) *
                    T(100) };

                result[i] = weights / total_weights;
            }
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    UltimateOSCIVisitor(size_type slow_roll = 28,
                        size_type fast_roll = 7,
                        size_type medium_roll = 14,
                        value_type slow_weight = T(1.0),
                        value_type fast_weight = T(4.0),
                        value_type medium_weight = T(2.0))
        : slow_(slow_roll),
          fast_(fast_roll),
          medium_(medium_roll),
          slow_w_(slow_weight),
          fast_w_(fast_weight),
          medium_w_(medium_weight)  {   }

private:

    using ssr_t = SimpleRollAdopter<SumVisitor<T, I>, T, I, A>;

    const size_type     slow_;
    const size_type     fast_;
    const size_type     medium_;
    const value_type    slow_w_;
    const value_type    fast_w_;
    const value_type    medium_w_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using u_osc_v = UltimateOSCIVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  UlcerIndexVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (periods_ < 2)  return;

        GET_COL_SIZE2

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        SimpleRollAdopter<MaxVisitor<T, I>, T, I, A>    high {
            MaxVisitor<T, I> { true }, periods_ };

        high.pre();
        high (idx_begin, idx_end, column_begin, column_end);
        high.post();

        result_type result { std::move(high.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &column_begin]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type          &r { result[i] };
                            const value_type    val {
                                (T(100) * (*(column_begin + i) - r)) / r };

                            r = val * val;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_begin + col_s,
                           result.begin(),
                           result.begin(),
                           [](const auto &c, const auto &r) -> value_type  {
                               const value_type    val {
                                   (T(100) * (c - r)) / r };

                               return (val * val);
                           });
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    sum {
            SumVisitor<T, I> { true }, periods_ };
        SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   avg {
            MeanVisitor<T, I> { true }, periods_ };

        if (use_sum_)  {
            sum.pre();
            sum (idx_begin, idx_end, result.begin(), result.end());
            sum.post();
        }
        else  {
            avg.pre();
            avg (idx_begin, idx_end, result.begin(), result.end());
            avg.post();
        }

        const result_type   &vec {
            use_sum_ ? sum.get_result() : avg.get_result() };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &vec = std::as_const(vec), this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] = std::sqrt(vec[i] / T(this->periods_));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(vec.begin(), vec.begin() + col_s,
                           result.begin(),
                           [this](const auto &v) -> value_type  {
                               return (std::sqrt(v / T(this->periods_)));
                           });
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    UlcerIndexVisitor(size_type periods = 14, bool use_sum = true)
        : periods_(periods), use_sum_(use_sum)  {   }

private:

    OBO_PORT_DECL

    const size_type periods_;
    const bool      use_sum_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using u_idx_v = UlcerIndexVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Trade To Market trend indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  TTMTrendVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        std::vector<bool, typename allocator_declare<bool, A>::type>;

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (bar_periods_ == 0 ||
            bar_periods_ >= (col_s - 1) ||
            col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("TTMTrendVisitor: All columns must be of "
                                 "equal sizes and bar period < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        avg_vec_t   trend_avg(col_s, std::numeric_limits<T>::quiet_NaN());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    bar_periods_,
                    col_s,
                    [&trend_avg, &high_begin, &low_begin]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            trend_avg[i] =
                                (*(high_begin + i) + *(low_begin + i)) * 0.5;
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(high_begin + bar_periods_, high_begin + col_s,
                           low_begin + bar_periods_,
                           trend_avg.begin() + bar_periods_,
                           [](const auto &h, const auto &l) -> value_type  {
                               return ((h + l) * T(0.5));
                           });
        }

        for (size_type i { 1 }; i <= bar_periods_; ++i) [[likely]]
            for (size_type j { i }; j < col_s; ++j) [[likely]]
                trend_avg[j] +=
                    (*(high_begin + (j - i)) + *(low_begin + (j - i))) / T(2);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&trend_avg, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            trend_avg[i] /= T(this->bar_periods_ + 1);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(trend_avg.begin(), trend_avg.end(),
                           trend_avg.begin(),
                           std::bind(std::divides<T>(), std::placeholders::_1,
                                     T(bar_periods_ + 1)));
        }

        result_.resize(col_s);
        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [this, &close_begin, &trend_avg = std::as_const(trend_avg)]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            this->result_[i] =
                                *(close_begin + i)  > trend_avg[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(close_begin, close_begin + col_s,
                           trend_avg.begin(),
                           result_.begin(),
                           [](const auto &c, const auto &ta) -> bool  {
                               return (c > ta);
                           });
        }
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    TTMTrendVisitor(size_type bar_periods = 5) : bar_periods_(bar_periods) {  }

private:

    using avg_vec_t = std::vector<T, typename allocator_declare<T, A>::type>;

    const size_type bar_periods_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ttmt_v = TTMTrendVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Parabolic Stop And Reverse (PSAR)
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  ParabolicSARVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        std::vector<bool, typename allocator_declare<bool, A>::type>;

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            col_s <= 2)
            throw DataFrameError("ParabolicSARVisitor: All columns must be of "
                                 "equal sizes and column size > 2");
#endif // HMDF_SANITY_EXCEPTIONS

        bool        bullish { true };
        value_type  high_point { *high_begin };
        value_type  low_point { *low_begin };
        vec_t       sar { close_begin, close_end };
        value_type  current_af { af_ };
        vec_t       long_vec(col_s, std::numeric_limits<T>::quiet_NaN());
        vec_t       short_vec { long_vec };
        vec_t       accel_fact { long_vec };
        result_type reversal (col_s, false);

        accel_fact[0] = accel_fact[1] = current_af;
        for (size_type i { 2 }; i < col_s; ++i) [[likely]]  {
            bool                reverse { false };
            const value_type    low { *(low_begin + i) };
            const value_type    high { *(high_begin + i) };

            accel_fact[i] = current_af;
            if (bullish)  {
                sar[i] = sar[i - 1] + current_af * (high_point - sar[i - 1]);
                if (low < sar[i])  {
                    bullish = false;
                    reverse = true;
                    current_af = af_;
                    sar[i] = high_point;
                    low_point = low;
                }
            }
            else  {
                sar[i] = sar[i - 1] + current_af * (low_point - sar[i - 1]);
                if (high > sar[i])  {
                    bullish = true;
                    reverse = true;
                    current_af = af_;
                    sar[i] = low_point;
                    high_point = high;
                }
            }

            reversal[i] = reverse;

            if (! reverse)  {
                if (bullish)  {
                    if (high > high_point)  {
                        high_point = high;
                        current_af = std::min(current_af + af_, max_af_);
                    }
                    if (*(low_begin + (i - 1)) < sar[i])
                        sar[i] = *(low_begin + (i - 1));
                    if (*(low_begin + (i - 2)) < sar[i])
                        sar[i] = *(low_begin + (i - 2));
                }
                else  {
                    if (low < low_point)  {
                        low_point = low;
                        current_af = std::min(current_af + af_, max_af_);
                    }
                    if (*(high_begin + (i - 1)) > sar[i])
                        sar[i] = *(high_begin + (i - 1));
                    if (*(high_begin + (i - 2)) > sar[i])
                        sar[i] = *(high_begin + (i - 2));
                }
            }

            if (bullish)
                long_vec[i] = sar[i];
            else
                short_vec[i] = sar[i];
        }

        result_.swap(reversal);
        long_.swap(long_vec);
        short_.swap(short_vec);
        accel_fact_.swap(accel_fact);
    }

    inline void pre ()  {

        result_.clear();
        long_.clear();
        short_.clear();
        accel_fact_.clear();
    }
    inline void post ()  {  }
    DEFINE_RESULT

    inline
    const std::vector<value_type,
                      typename allocator_declare<value_type, A>::type> &
    get_longs () const  { return (long_); }
    inline
    std::vector<value_type,
                typename allocator_declare<value_type, A>::type> &
    get_longs ()  { return (long_); }

    inline
    const std::vector<value_type,
                      typename allocator_declare<value_type, A>::type> &
    get_shorts () const  { return (short_); }
    inline
    std::vector<value_type,
                typename allocator_declare<value_type, A>::type> &
    get_shorts ()  { return (short_); }

    inline
    const std::vector<value_type,
                      typename allocator_declare<value_type, A>::type> &
    get_acceleration_factors () const  { return (accel_fact_); }
    inline
    std::vector<value_type,
                typename allocator_declare<value_type, A>::type> &
    get_acceleration_factors ()  { return (accel_fact_); }

    explicit
    ParabolicSARVisitor(value_type acceleration_factor = T(0.02),
                        value_type max_acceleration_factor = T(0.2))
        : af_(acceleration_factor),
          max_af_(max_acceleration_factor)  {  }

private:

    using vec_t =
        std::vector<value_type,
                    typename allocator_declare<value_type, A>::type>;

    const value_type    af_;
    const value_type    max_af_;
    result_type         result_ { };
    vec_t               long_ { };
    vec_t               short_ { };
    vec_t               accel_fact_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using psar_v = ParabolicSARVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Even Better Sine Wave (EBSW) indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  EBSineWaveVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (hp_period_ <= 38 || bar_period_ >= col_s ||
            bar_period_ == 0 || hp_period_ >= col_s)
            throw DataFrameError("EBSineWaveVisitor: 38 < high pass < column "
                                 "size and 0 < bar period < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type         result(col_s, std::numeric_limits<T>::quiet_NaN());
        value_type          last_close { *close_begin };
        value_type          last_high_pass { 0 };
        value_type          filter_hist[2] = { 0, 0 };
        const value_type    sin_hp { std::sin(T(360) / T(hp_period_)) };
        const value_type    cos_hp { std::cos(T(360) / T(hp_period_)) };
        const value_type    cos_bar {
            std::cos(std::sqrt(T(2)) * T(180) / T(bar_period_)) };
        const value_type    alpha1 { (T(1) - sin_hp) / cos_hp };
        const value_type    alpha1_effect { T(0.5) * (T(1) + alpha1) };

        // Smooth with a Super Smoother Filter from equation 3-3
        //
        const value_type    alpha2 {
            std::exp(-std::sqrt(T(2)) * T(M_PI) / T(bar_period_)) };
        const value_type    c2 { T(2) * alpha2 * cos_bar };
        const value_type    c3 { T(-1) * alpha2 * alpha2 };
        const value_type    c1 { T(1) - c2 - c3 };

        for (size_type i { 1 }; i < col_s; ++i) [[likely]]  {
            const value_type    this_close { *(close_begin + i) };

            // High pass filter cyclic components whose periods are shorter
            // than duration input
            //
            const value_type    high_pass {
                alpha1_effect * (this_close - last_close) +
                alpha1 * last_high_pass };
            const value_type    filter {
                c1 * (high_pass + last_high_pass) / T(2) +
                c2 * filter_hist[1] + c3 * filter_hist[0] };

            // 3 bar average of wave amplitude and power
            //
            const value_type    wave {
                (filter + filter_hist[1] + filter_hist[0]) / T(3) };
            const value_type    power {
                (filter * filter +
                 filter_hist[1] * filter_hist[1] +
                 filter_hist[0] * filter_hist[0]) / T(3) };

            // Normalize the average wave to square root of the average power
            //
            result[i] = wave / std::sqrt(power);

            filter_hist[0] = filter_hist[1];
            filter_hist[1] = filter;
            last_high_pass = high_pass;
            last_close = this_close;
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    EBSineWaveVisitor(size_type high_pass_period = 40,
                      size_type bar_period = 10)
        : hp_period_(high_pass_period), bar_period_(bar_period)  {  }

private:

    OBO_PORT_DECL

    const size_type hp_period_;
    const size_type bar_period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ebsw_v = EBSineWaveVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Ehler's Super Smoother Filter (SSF) indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  EhlerSuperSmootherVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if ((poles_ != 2 && poles_ != 3) ||
            bar_period_ == 0 || bar_period_ >= col_s)
            throw DataFrameError("EhlerSuperSmootherVisitor: poles must be "
                                 "either 2 or 3 and 0 < bar period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result(column_begin, column_end);

        if (poles_ == 2)  {
            const value_type   x {
                T(M_PI) * std::sqrt(T(2)) / T(bar_period_) };
            const value_type   a0 { std::exp(-x) };
            const value_type   a1 { -a0 * a0 };
            const value_type   c0 { T(2) * a0 * std::cos(x) };
            const value_type   c1 { T(1) - a1 - c0 };

            for (size_type i { poles_ }; i < col_s; ++i)
                result[i] =
                    c1 * *(column_begin + i) +
                    c0 * result[i - 1] +
                    a1 * result[i - 2];
        }
        else if (poles_ == 3)  {
            const value_type   x { T(M_PI) / T(bar_period_) };
            const value_type   a0 { std::exp(-x) };
            const value_type   b0 {
                T(2) * a0 * std::cos(std::sqrt(T(3)) * x) };
            const value_type   a1 { a0 * a0 };
            const value_type   a2 { a1 * a1 };
            const value_type   c2 { -a1 * (T(1) + b0) };
            const value_type   c0 { a1 + b0 };
            const value_type   c1 { T(1) - c0 - c2 - a2 };

            for (size_type i { poles_ }; i < col_s; ++i) [[likely]]
                result[i] =
                    c1 * *(column_begin + i) +
                    c0 * result[i - 1] +
                    c2 * result[i - 2] +
                    a2 * result[i - 3];
        }

        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    EhlerSuperSmootherVisitor(size_type poles = 2, size_type bar_period = 10)
        : poles_(poles), bar_period_(bar_period)  {  }

private:

    OBO_PORT_DECL

    const size_type poles_;
    const size_type bar_period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ess_v = EhlerSuperSmootherVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Variable Index Dynamic Average (VIDYA) indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  VarIdxDynAvgVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (roll_period_ <= 1 && roll_period_ >= col_s)
            throw DataFrameError("VarIdxDynAvgVisitor: 1 < roll period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        DiffVisitor<T, I, A>    diff { 1, false };

        diff.pre();
        diff (idx_begin, idx_end, column_begin, column_end);
        diff.post();

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        result_type positive { std::move(diff.get_result()) };

        positive[0] = 0;

        result_type negative = positive;

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&positive, &negative]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            if (positive[i] < 0)  positive[i] = 0;
                            if (negative[i] > 0)  negative[i] = 0;
                            else  negative[i] = std::fabs(negative[i]);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                if (positive[i] < 0)  positive[i] = 0;
                if (negative[i] > 0)  negative[i] = 0;
                else  negative[i] = std::fabs(negative[i]);
            }
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    sum {
            SumVisitor<T, I>(), roll_period_ };

        sum.pre();
        sum (idx_begin, idx_end, positive.begin(), positive.end());
        sum.post();
        positive = std::move(sum.get_result());
        sum.pre();
        sum (idx_begin, idx_end, negative.begin(), negative.end());
        sum.post();
        negative = std::move(sum.get_result());

        result_type result(col_s, std::numeric_limits<T>::quiet_NaN());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    roll_period_,
                    col_s,
                    [&positive, &negative, &result]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    p = positive[i];
                            const value_type    n = negative[i];

                            result[i] = std::fabs((p - n) / (p + n));
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(positive.begin() + roll_period_,
                           positive.begin() + col_s,
                           negative.begin() + roll_period_,
                           result.begin() + roll_period_,
                           [](const auto &p, const auto &n) -> value_type  {
                               return (std::fabs((p - n) / (p + n)));
                           });
        }

        const value_type    alpha { T(2) / (T(roll_period_) + T(1)) };

        result[roll_period_ - 1] = 0;
        for (size_type i { roll_period_ }; i < col_s; ++i) [[likely]]  {
            auto    res = result.begin() + i;

            *res = alpha * *res * *(column_begin + i) +
                   *std::prev(res) * (T(1) - alpha * *res);
        }

        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    VarIdxDynAvgVisitor(size_type roll_period = 14)
        : roll_period_(roll_period)  {  }

private:

    OBO_PORT_DECL

    const size_type roll_period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using vidya_v = VarIdxDynAvgVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Pivot Points, Supports and Resistances indicators
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  PivotPointSRVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s <= 1 ||
            col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("PivotPointSRVisitor: All columns must be of "
                                 "equal sizes and column size > 1");
#endif // HMDF_SANITY_EXCEPTIONS

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        constexpr value_type two = 2;

        result_type pivot_point(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type resist_1(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type resist_2(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type resist_3(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type support_1(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type support_2(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type support_3(col_s, std::numeric_limits<T>::quiet_NaN());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&close_begin, &low_begin, &high_begin,
                     &pivot_point,
                     &resist_1, &resist_2, &resist_3,
                     &support_1, &support_2, &support_3]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    low { *(low_begin + i) };
                            const value_type    high { *(high_begin + i) };
                            const value_type    pp {
                                (low + high + *(close_begin + i)) / T(3) };

                            pivot_point[i] = pp;
                            resist_1[i] = two * pp - low;
                            support_1[i] = two * pp - high;
                            resist_2[i] = pp + high - low;
                            support_2[i] = pp - high + low;
                            resist_3[i] = high + two * (pp - low);
                            support_3[i] = low - two * (high - pp);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                const value_type    low { *(low_begin + i) };
                const value_type    high { *(high_begin + i) };
                const value_type    pp {
                    (low + high + *(close_begin + i)) / T(3) };

                pivot_point[i] = pp;
                resist_1[i] = two * pp - low;
                support_1[i] = two * pp - high;
                resist_2[i] = pp + high - low;
                support_2[i] = pp - high + low;
                resist_3[i] = high + two * (pp - low);
                support_3[i] = low - two * (high - pp);
            }
        }

        result_.swap(pivot_point);
        resist_1_.swap(resist_1);
        resist_2_.swap(resist_2);
        resist_3_.swap(resist_3);
        support_1_.swap(support_1);
        support_2_.swap(support_2);
        support_3_.swap(support_3);
    }

    inline void pre ()  {

        result_.clear();
        resist_1_.clear();
        resist_2_.clear();
        resist_3_.clear();
        support_1_.clear();
        support_2_.clear();
        support_3_.clear();
    }
    inline void post ()  {  }

    DEFINE_RESULT

    inline const result_type &get_resist_1 () const  { return (resist_1_); }
    inline result_type &get_resist_1 ()  { return (resist_1_); }

    inline const result_type &get_resist_2 () const  { return (resist_2_); }
    inline result_type &get_resist_2 ()  { return (resist_2_); }

    inline const result_type &get_resist_3 () const  { return (resist_3_); }
    inline result_type &get_resist_3 ()  { return (resist_3_); }

    inline const result_type &get_support_1 () const  { return (support_1_); }
    inline result_type &get_support_1 ()  { return (support_1_); }

    inline const result_type &get_support_2 () const  { return (support_2_); }
    inline result_type &get_support_2 ()  { return (support_2_); }

    inline const result_type &get_support_3 () const  { return (support_3_); }
    inline result_type &get_support_3 ()  { return (support_3_); }

    PivotPointSRVisitor() = default;

private:

    result_type result_ { };  // Pivot point
    result_type resist_1_ { };
    result_type resist_2_ { };
    result_type resist_3_ { };
    result_type support_1_ { };
    result_type support_2_ { };
    result_type support_3_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ppsr_v = PivotPointSRVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Average Directional Movement Index (ADX)
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  AvgDirMovIdxVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s <= 3 ||
            col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("AvgDirMovIdxVisitor: All columns must be of "
                                 "equal size and column size > 3");
#endif // HMDF_SANITY_EXCEPTIONS

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        result_type pos_di(col_s);
        result_type neg_di(col_s);
        result_type true_range(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&close_begin, &low_begin, &high_begin,
                     &pos_di, &neg_di, &true_range]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    nxt_h {
                                *(high_begin + (i + 1)) };
                            const value_type    nxt_l {
                                *(low_begin + (i + 1)) };
                            const value_type    pos_move {
                                nxt_h - *(high_begin + i) };
                            const value_type    neg_move {
                                *(low_begin + i) - nxt_l };

                            pos_di[i] =
                                (pos_move > neg_move && pos_move > 0)
                                    ? pos_move : 0;
                            neg_di[i] =
                                (neg_move > pos_move && neg_move > 0)
                                    ? neg_move : 0;

                            const value_type    close { *(close_begin + i) };

                            true_range[i] =
                                std::max(nxt_h, close) -
                                std::min(nxt_l, close);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s - 1; ++i) [[likely]]  {
                const value_type    nxt_h { *(high_begin + (i + 1)) };
                const value_type    nxt_l { *(low_begin + (i + 1)) };
                const value_type    pos_move { nxt_h - *(high_begin + i) };
                const value_type    neg_move { *(low_begin + i) - nxt_l };

                pos_di[i] =
                    (pos_move > neg_move && pos_move > 0) ? pos_move : 0;
                neg_di[i] =
                    (neg_move > pos_move && neg_move > 0) ? neg_move : 0;

                const value_type    close { *(close_begin + i) };

                true_range[i] =
                    std::max(nxt_h, close) - std::min(nxt_l, close);
            }
        }

        ewm_v<double, I, A> ewm(exponential_decay_spec::span,
                                dir_smoother_, true);

        ewm.pre();
        ewm (idx_begin, idx_end, true_range.begin(), true_range.end());
        ewm.post();
        true_range.swap(ewm.get_result());

        ewm.pre();
        ewm (idx_begin, idx_end, pos_di.begin(), pos_di.end());
        ewm.post();
        pos_di.swap(ewm.get_result());

        ewm.pre();
        ewm (idx_begin, idx_end, neg_di.begin(), neg_di.end());
        ewm.post();
        neg_di.swap(ewm.get_result());

        result_type dx(col_s);
        value_type  prev_val { 0 };

        for (size_type i { 0 }; i < col_s - 1; ++i) [[likely]]  {
            const value_type    pos_val { pos_di[i] / true_range[i] };
            const value_type    neg_val { neg_di[i] / true_range[i] };
            const value_type    val {
                std::fabs(pos_val - neg_val) / (pos_val + neg_val) };

            if (! is_nan__(val))  prev_val = val;
            dx[i] = prev_val;
        }

        ewm_v<double, I, A> ewm2(exponential_decay_spec::span,
                                 adx_smoother_, true);

        ewm2.pre();
        ewm2 (idx_begin, idx_end, dx.begin(), dx.end());
        ewm2.post();
        result_.swap(ewm2.get_result());
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    AvgDirMovIdxVisitor(value_type dir_smoother, value_type adx_smoother)
        : dir_smoother_(dir_smoother), adx_smoother_(adx_smoother)   {  }

private:

    const value_type    dir_smoother_;
    const value_type    adx_smoother_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using adx_v = AvgDirMovIdxVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Holt-Winter Channel (HWC) indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  HoltWinterChannelVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s <= 3)
            throw DataFrameError(
                "HoltWinterChannelVisitor: column size must be > 3");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type upper (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type lower (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type pct_diff (col_s, std::numeric_limits<T>::quiet_NaN());
        value_type  last_a { 0 };
        value_type  last_v { 0 };
        value_type  last_var { 0 };
        value_type  last_f { *close_begin };
        value_type  last_price { last_f };
        value_type  last_result { last_f };

        for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
            const value_type    cls { *(close_begin + i) };
            const value_type    f { (T(1) - na_) *
                                    (last_f + last_v + T(0.5) *
                                    last_a) + na_ * cls };
            const value_type    v {
                (T(1) - nb_) * (last_v + last_a) + nb_ * (f - last_f) };
            const value_type    a {
                (T(1) - nc_) * last_a + nc_ * (v - last_v) };
            const value_type    var {
                (T(1) - nd_) * last_var +
                nd_ * (last_price - last_result) *
                (last_price - last_result) };
            const value_type    stddev { std::sqrt(last_var) };

            last_result = result[i] = f + v + T(0.5) * a;
            upper[i] = result[i] + stddev;
            lower[i] = result[i] - stddev;

            const value_type    diff { upper[i] - lower[i] };

            if (diff > 0)
                pct_diff[i] = (cls - lower[i]) / diff;

            last_price = cls;
            last_a = a;
            last_f = f;
            last_v = v;
            last_var = var;
        }

        result_.swap(result);
        upper_band_.swap(upper);
        lower_band_.swap(lower);
        pct_diff_.swap(pct_diff);
    }

    inline void pre ()  {

        result_.clear();
        upper_band_.clear();
        lower_band_.clear();
        pct_diff_.clear();
    }
    inline void post ()  { }

    DEFINE_RESULT
    const result_type &get_upper_band() const  { return (upper_band_); }
    result_type &get_upper_band()  { return (upper_band_); }
    const result_type &get_lower_band() const  { return (lower_band_); }
    result_type &get_lower_band()  { return (lower_band_); }
    const result_type &get_pct_diff() const  { return (pct_diff_); }
    result_type &get_pct_diff()  { return (pct_diff_); }

    explicit
    HoltWinterChannelVisitor(value_type na = T(0.2),
                             value_type nb = T(0.1),
                             value_type nc = T(0.1),
                             value_type nd = T(0.1))
        : na_(na), nb_(nb), nc_(nc), nd_(nd)  {   }

private:

    const value_type    na_;
    const value_type    nb_;
    const value_type    nc_;
    const value_type    nd_;
    result_type         result_ { };
    result_type         upper_band_ { };
    result_type         lower_band_ { };
    result_type         pct_diff_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using hwc_v = HoltWinterChannelVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  HeikinAshiCndlVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(open_begin, open_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("HeikinAshiCndlVisitor: All columns must be "
                                 "of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type open (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type high (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type low (col_s, std::numeric_limits<T>::quiet_NaN());

        result[0] =
            T(0.25) * (*open_begin + *high_begin + *low_begin + *close_begin);
        high[0] = *high_begin;
        low[0] = *low_begin;
        open[0] = T(0.5) * (*open_begin + *close_begin);
        for (size_type i { 1 }; i < col_s; ++i) [[likely]]  {
            const value_type    hval { *(high_begin + i) };
            const value_type    lval { *(low_begin + i) };
            auto                oval = open.begin() + i;
            auto                res = result.begin() + i;

            *res = T(0.25) *
                   (*(open_begin + i) + hval + lval + *(close_begin + i));
            *oval = T(0.5) * (*std::prev(oval) + *std::prev(res));
            high[i] = std::max({ *oval, hval, *res });
            low[i] = std::min({ *oval, lval, *res });
        }

        result_.swap(result);
        open_.swap(open);
        high_.swap(high);
        low_.swap(low);
    }

    inline void pre ()  {

        result_.clear();
        open_.clear();
        high_.clear();
        low_.clear();
    }
    inline void post ()  { }

    DEFINE_RESULT
    const result_type &get_open() const  { return (open_); }
    result_type &get_open()  { return (open_); }
    const result_type &get_high() const  { return (high_); }
    result_type &get_high()  { return (high_); }
    const result_type &get_low() const  { return (low_); }
    result_type &get_low()  { return (low_); }

private:

    result_type result_ {  };  // Close
    result_type open_ {  };
    result_type high_ {  };
    result_type low_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ha_cdl_v = HeikinAshiCndlVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Also called Stochastic Oscillator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  CenterOfGravityVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {


        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (roll_count_ == 0 || col_s <= roll_count_)
            throw DataFrameError(
                "CenterOfGravityVisitor: 0 < roll count < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    roll_count_ - 1,
                    col_s,
                    [this, &column_begin, &result]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            size_type   count { 0 };
                            value_type  dot_prd { 0 };

                            for (size_type j { (i + 1) - this->roll_count_ };
                                 j <= i;
                                 ++j, ++count)
                                dot_prd += *(column_begin + j) *
                                           T(this->roll_count_ - count);
                            result[i] = dot_prd;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { roll_count_ - 1 }; i < col_s; ++i) [[likely]]  {
                size_type   count { 0 };
                value_type  dot_prd { 0 };

                for (size_type j { (i + 1) - roll_count_ }; j <= i;
                     ++j, ++count)
                    dot_prd += *(column_begin + j) * T(roll_count_ - count);
                result[i] = dot_prd;
            }
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    sum_v
            { SumVisitor<T, I>(), roll_count_ };

        sum_v.pre();
        sum_v (idx_begin, idx_end, column_begin, column_end);
        sum_v.post();

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&sum_v = std::as_const(sum_v.get_result()), &result]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] /= -sum_v[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(sum_v.get_result().begin(),
                           sum_v.get_result().begin() + col_s,
                           result.begin(),
                           result.begin(),
                           [](const auto &s, const auto &r) -> value_type  {
                               return (r / -s);
                           });
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    CenterOfGravityVisitor(size_type r_count = 10) : roll_count_(r_count) {  }

private:

    OBO_PORT_DECL

    const size_type roll_count_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using cog_v = CenterOfGravityVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Arnaud Legoux Moving Average
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  ArnaudLegouxMAVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (roll_count_ <= 1 || col_s <= roll_count_)
            throw DataFrameError("ArnaudLegouxMAVisitor:  1 < roll count < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    roll_count_,
                    col_s,
                    [this, &column_begin, &result]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  win_sum { 0 };

                            for (size_type j { 0 }; j < this->roll_count_; ++j)
                                win_sum +=
                                    this->wtd_[j] * *(column_begin + (i - j));

                            result[i] = win_sum / this->cum_sum_;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else {
            for (size_type i { roll_count_ }; i < col_s; ++i) [[likely]]  {
                value_type  win_sum { 0 };

                for (size_type j { 0 }; j < roll_count_; ++j) [[likely]]
                    win_sum += wtd_[j] * *(column_begin + (i - j));

                result[i] = win_sum / cum_sum_;
            }
        }

        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    ArnaudLegouxMAVisitor(size_type r_count = 10,
                          value_type sigma = 6.0,
                          value_type dist_offset = 0.85)
        : roll_count_(r_count),
          m_(dist_offset * T(r_count - 1)),
          s_(T(r_count) / sigma),
          wtd_(r_count)  {

        const value_type    denom = T(2) * s_ * s_;

        for (size_type i { 0 }; i < roll_count_; ++i)  {
            wtd_[i] = std::exp(-((T(i) - m_) * (T(i) - m_)) / denom);
            cum_sum_ += wtd_[i];
        }
    }

private:

    OBO_PORT_DECL

    const size_type     roll_count_;
    const value_type    m_;
    const value_type    s_;
    value_type          cum_sum_ { 0 };
    result_type         wtd_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using alma_v = ArnaudLegouxMAVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  RateOfChangeVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (period_ == 0)
            throw DataFrameError("RateOfChangeVisitor: period must be > 0");
#endif // HMDF_SANITY_EXCEPTIONS

        DiffVisitor<T, I, A>    diff (period_, false);

        diff.pre();
        diff (idx_begin, idx_end, column_begin, column_end);
        diff.post();

        result_type result { std::move(diff.get_result()) };

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    period_,
                    col_s,
                    [this, &column_begin, &result]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] /= *(column_begin + (i - this->period_));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { period_ }; i < col_s; ++i) [[likely]]
                result[i] /= *(column_begin + (i - period_));
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    RateOfChangeVisitor(size_type period) : period_(period)  {   }

private:

    OBO_PORT_DECL

    const size_type period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using roc_v = RateOfChangeVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Accumulation/Distribution (AD) indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  AccumDistVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H, forward_iterator V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(open_begin, open_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            col_s != size_type(std::distance(volume_begin, volume_end)))
            throw DataFrameError("AccumDistVisitor: All columns must be of "
                                 "equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result,
                     &low_begin, &high_begin, &open_begin, &close_begin,
                     &volume_begin]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    co
                                { *(close_begin + i) - *(open_begin + i) };
                            const value_type    hl
                                { *(high_begin + i) - *(low_begin + i) };

                            result[i] = co * T(*(volume_begin + i)) / hl;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                const value_type    co
                    { *(close_begin + i) - *(open_begin + i) };
                const value_type    hl
                    { *(high_begin + i) - *(low_begin + i) };

                result[i] = co * T(*(volume_begin + i)) / hl;
            }
        }

        CumSumVisitor<T, I, A>  cumsum;

        cumsum.pre();
        cumsum (idx_begin, idx_end, result.begin(), result.end());
        cumsum.post();
        result = std::move(cumsum.get_result());

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

private:

    result_type result_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ad_v = AccumDistVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Chaikin Money Flow (CMF) indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  ChaikinMoneyFlowVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H, forward_iterator V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(open_begin, open_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            col_s != size_type(std::distance(volume_begin, volume_end)) ||
            period_ == 0 || period_ >= col_s)
            throw DataFrameError("ChaikinMoneyFlowVisitor: All columns must "
                                 "be of equal size and 0 < period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
            const value_type    co { *(close_begin + i) - *(open_begin + i) };
            const value_type    hl { *(high_begin + i) - *(low_begin + i) };

            result[i] = co * T(*(volume_begin + i)) / hl;
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    sum
            { SumVisitor<T, I>(), period_ };

        sum.pre();
        sum (idx_begin, idx_end, result.begin(), result.end());
        sum.post();
        result = std::move(sum.get_result());

        sum.pre();
        sum (idx_begin, idx_end, volume_begin, volume_end);
        sum.post();
        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &sum = std::as_const(sum.get_result())]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] /= sum[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(result.begin(), result.end(),
                           sum.get_result().begin(),
                           result.begin(),
                           std::divides<T>());
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    ChaikinMoneyFlowVisitor(size_type period = 21) : period_(period)  {   }

private:

    const size_type period_;
    result_type     result_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using cmf_v = ChaikinMoneyFlowVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Vertical Horizontal Filter (VHF) indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  VertHorizFilterVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (period_ == 0 || period_ >= col_s)
            throw DataFrameError("VertHorizFilterVisitor: 0 < period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        SimpleRollAdopter<MaxVisitor<T, I>, T, I, A>    mx
            { MaxVisitor<T, I>(), period_ };
        SimpleRollAdopter<MinVisitor<T, I>, T, I, A>    mn
            { MinVisitor<T, I>(), period_ };
        DiffVisitor<T, I, A>                            diff { 1, false };

        mx.pre();
        mn.pre();
        diff.pre();
        if (thread_level > 3)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&mx,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        mx (idx_begin, idx_end, column_begin, column_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&mn,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        mn (idx_begin, idx_end, column_begin, column_end);
                    });
            auto    fut3 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&diff,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        diff (idx_begin, idx_end, column_begin, column_end);
                    });

            fut1.get();
            fut2.get();
            fut3.get();
        }
        else  {
            mx (idx_begin, idx_end, column_begin, column_end);
            mn (idx_begin, idx_end, column_begin, column_end);
            diff (idx_begin, idx_end, column_begin, column_end);
        }
        mx.post();
        mn.post();
        diff.post();

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&diff = diff.get_result()]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            diff[i] = std::fabs(diff[i]);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]
                diff.get_result()[i] = std::fabs(diff.get_result()[i]);
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    diff_sum
            { SumVisitor<T, I>(), period_ };

        diff_sum.pre();
        diff_sum (idx_begin, idx_end,
                  diff.get_result().begin(), diff.get_result().end());
        diff_sum.post();

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    period_,
                    col_s,
                    [&result,
                     &mx = std::as_const(mx.get_result()),
                     &mn = std::as_const(mn.get_result()),
                     &diff_sum = std::as_const(diff_sum.get_result())]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] = std::fabs(mx[i] - mn[i]) / diff_sum[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { period_ }; i < col_s; ++i) [[likely]]
                result[i] =
                    std::fabs(mx.get_result()[i] - mn.get_result()[i]) /
                    diff_sum.get_result()[i];
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    VertHorizFilterVisitor(size_type period = 28) : period_(period)  {   }

private:

    OBO_PORT_DECL

    const size_type period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using vhf_v = VertHorizFilterVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// On Balance Volume (OBV) indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  OnBalanceVolumeVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H, forward_iterator V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(volume_begin, volume_end)))
            throw DataFrameError("OnBalanceVolumeVisitor: All columns must "
                                 "be of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        ReturnVisitor<T, I, A>  ret { return_policy::trinary };

        ret.pre();
        ret (idx_begin, idx_end, close_begin, close_end);
        ret.post();

        result_type result { std::move(ret.get_result()) };

        result[0] = T(1);
        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &volume_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] *= T(*(volume_begin + i));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(volume_begin, volume_end,
                           result.begin(),
                           result.begin(),
                           [](const auto &v, const auto &r) -> value_type  {
                               return (r * T(v));
                           });
        }

        CumSumVisitor<T, I, A>  cumsum;

        cumsum.pre();
        cumsum (idx_begin, idx_end, result.begin(), result.end());
        cumsum.post();
        result_.swap(cumsum.get_result());
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

private:

    result_type result_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using obv_v = OnBalanceVolumeVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  TrueRangeVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &/*low_end*/,
                const H &high_begin, const H &/*high_end*/,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        result_type     result (col_s);

        result[0] = *high_begin - *low_begin;
        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&low_begin, &high_begin, &close_begin, &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    high { *(high_begin + i) };
                            const value_type    low { *(low_begin + i) };
                            const value_type    prev_c
                                { *(close_begin + (i - 1)) };

                            result[i] = std::max({ std::fabs(high - low),
                                                   std::fabs(high - prev_c),
                                                   std::fabs(prev_c - low) });
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 1 }; i < col_s; ++i) [[likely]]  {
                const value_type    high { *(high_begin + i) };
                const value_type    low { *(low_begin + i) };
                const value_type    prev_c { *(close_begin + (i - 1)) };

                result[i] = std::max({ std::fabs(high - low),
                                       std::fabs(high - prev_c),
                                       std::fabs(prev_c - low) });
            }
        }

        if (rolling_avg_)  {
            ewm_v<T, I, A>  avg (exponential_decay_spec::span,
                                 rolling_period_,
                                 true);

            avg.pre();
            avg (idx_begin, idx_end, result.begin(), result.end());
            avg.post();
            result = std::move(avg.get_result());

            if (normalize_)  {
                if (thread_level > 2)  {
                    auto    futures =
                        ThreadGranularity::thr_pool_.parallel_loop(
                            size_type(0),
                            col_s,
                            [&close_begin, &result]
                            (auto begin, auto end) mutable -> void  {
                                for (size_type i = begin; i < end; ++i) {
                                    result[i] *= T(100) / *(close_begin + i);
                                }
                            });

                    for (auto &fut : futures)  fut.get();
                }
                else  {
                    std::transform(
                        close_begin, close_end,
                        result.begin(),
                        result.begin(),
                        [](const auto &c, const auto &r) -> value_type {
                            return (r * (T(100) / c));
                        });
                    }
            }
        }

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    TrueRangeVisitor(bool rolling_avg = true,
                     size_type rolling_period = 14,
                     bool normalize = false)
        : rolling_period_(rolling_period),
          rolling_avg_(rolling_avg),
          normalize_(normalize)  {  }

private:

    result_type     result_ {  };
    const size_type rolling_period_;
    const bool      rolling_avg_;
    const bool      normalize_;
};

// ----------------------------------------------------------------------------

// Decay indicator
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  DecayVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (period_ == 0 || period_ >= col_s)
            throw DataFrameError("DecayVisitor: 0 < period < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type         result (col_s);
        const value_type    decay {
            expo_ ? std::exp(-T(period_)) : (T(1) / T(period_)) };

        result[0] = *column_begin;
        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&result, &column_begin, decay]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] =
                                std::max({ *(column_begin + i),
                                           *(column_begin + (i - 1)) - decay,
                                           T(0) });
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 1 }; i < col_s; ++i) [[likely]]
                result[i] = std::max({ *(column_begin + i),
                                       *(column_begin + (i - 1)) - decay,
                                       T(0) });
        }

        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    DecayVisitor(size_type period = 5, bool exponential = false)
        : period_(period), expo_(exponential)  {   }

private:

    OBO_PORT_DECL

    const size_type period_;
    const bool      expo_;
    result_type     result_ { };
};

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  HodgesTompkinsVolVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (roll_count_ == 0 || roll_count_ >= col_s)
            throw DataFrameError("HodgesTompkinsVolVisitor: 0 < roll count < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        ReturnVisitor<T, I, A>  ret { return_policy::log };

        ret.pre();
        ret (idx_begin, idx_end, column_begin, column_end);
        ret.post();

        SimpleRollAdopter<StdVisitor<T, I>, T, I, A>    stdev
            { StdVisitor<T, I> { false, true }, roll_count_ };

        stdev.pre();
        stdev (idx_begin, idx_end,
               ret.get_result().begin(), ret.get_result().end());
        stdev.post();

        result_type         result { std::move(stdev.get_result()) };
        const value_type    annual { std::sqrt(trading_periods_) };
        const value_type    n { T(col_s - roll_count_ + 1) };
        const value_type    adj_factor {
            T(1) / (T(1) -
                    (T(roll_count_) / n) +
                    ((T(roll_count_ * roll_count_) -
                      T(1)) / (T(3) * n * n))) };

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result,annual, adj_factor]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] *= annual * adj_factor;
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(result.begin(), result.end(), result.begin(),
                           std::bind(std::multiplies<T>(),
                                     std::placeholders::_1,
                                     annual * adj_factor));
        }

        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    HodgesTompkinsVolVisitor(size_type roll_count = 30,
                             size_type trading_periods = 252)
        : roll_count_(roll_count), trading_periods_(trading_periods)  {  }

private:

    OBO_PORT_DECL

    result_type     result_ {  };
    const size_type roll_count_;
    const size_type trading_periods_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ht_vol_v = HodgesTompkinsVolVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  ParkinsonVolVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end)  {

        const size_type col_s = std::distance(low_begin, low_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_count_ == 0 || roll_count_ >= col_s)
            throw DataFrameError("ParkinsonVolVisitor: All columns must be of "
                                 "equal sizes and roll count < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        const auto          thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        result_type         result (col_s);
        const value_type    factor { T(1) / (T(4) * std::log(T(2))) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &low_begin, &high_begin, factor]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    val
                                { std::log(*(high_begin + i) /
                                           *(low_begin + i)) };

                            result[i] = factor * val * val;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(high_begin, high_begin + col_s,
                           low_begin,
                           result.begin(),
                           [factor]
                           (const auto &h, const auto &l) -> value_type  {
                               const value_type    val { std::log(h / l) };

                               return (factor * val * val);
                           });
        }

        SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   avg
            { MeanVisitor<T, I>(), roll_count_ } ;

        avg.pre();
        avg (idx_begin, idx_end, result.begin(), result.end());
        avg.post();
        result = std::move(avg.get_result());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    roll_count_ - 1,
                    col_s,
                    [&result, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  &res = result[i];

                            res = std::sqrt(res * T(this->trading_periods_));
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { roll_count_ - 1 }; i < col_s; ++i) [[likely]]  {
                auto    &res = result[i];

                res = std::sqrt(res * T(trading_periods_));
            }
        }

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    ParkinsonVolVisitor(size_type roll_count = 30,
                        size_type trading_periods = 252)
        : roll_count_(roll_count), trading_periods_(trading_periods)  {  }

private:

    result_type     result_ {  };
    const size_type roll_count_;
    const size_type trading_periods_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using p_vol_v = ParkinsonVolVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  CoppockCurveVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        roc_v<T, I, A>  roc_l (roc_long_);
        roc_v<T, I, A>  roc_s (roc_short_);

        roc_l.pre();
        roc_s.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&roc_l,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        roc_l(idx_begin, idx_end, column_begin, column_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&roc_s,
                     &idx_begin, &idx_end,
                     &column_begin, &column_end]() -> void  {
                        roc_s(idx_begin, idx_end, column_begin, column_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            roc_l (idx_begin, idx_end, column_begin, column_end);
            roc_s (idx_begin, idx_end, column_begin, column_end);
        }
        roc_l.post();
        roc_s.post();

        result_type result { std::move(roc_l.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&roc_s = std::as_const(roc_s.get_result()), &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] += roc_s[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(roc_s.get_result().begin(),
                           roc_s.get_result().begin() + col_s,
                           result.begin(),
                           result.begin(),
                           [](const auto &roc, const auto &r) -> value_type  {
                               return (r + roc);
                           });
        }

        using wma_t = SimpleRollAdopter<WeightedMeanVisitor<T, I>, T, I, A>;

        wma_t   wma { WeightedMeanVisitor<T, I>(), wma_period_ };

        wma.pre();
        wma (idx_begin, idx_end, result.begin(), result.end());
        wma.post();

        result_.swap(wma.get_result());
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    CoppockCurveVisitor(size_type roc_long = 14,
                        size_type roc_short = 11,
                        size_type wma_period = 10)
        : roc_long_(roc_long),
          roc_short_(roc_short),
          wma_period_(wma_period)  {   }

private:

    OBO_PORT_DECL

    const size_type roc_long_;
    const size_type roc_short_;
    const size_type wma_period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using coppc_v = CoppockCurveVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  BalanceOfPowerVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(open_begin, open_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("BalanceOfPowerVisitor: All columns must be "
                                 "of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        nzr_v<T, I, A>  non_z_range;

        non_z_range.pre();
        non_z_range(idx_begin, idx_end,
                    close_begin, close_end, open_begin, open_end);
        non_z_range.post();

        result_type result { std::move(non_z_range.get_result()) };

        non_z_range.pre();
        non_z_range(idx_begin, idx_end,
                    high_begin, high_end, low_begin, low_end);
        non_z_range.post();

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&non_z_range = std::as_const(non_z_range.get_result()),
                     &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] /= non_z_range[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(non_z_range.get_result().begin(),
                           non_z_range.get_result().begin() + col_s,
                           result.begin(),
                           result.begin(),
                           [](const auto &nzr, const auto &r) -> value_type  {
                               return (r / nzr);
                           });
        }

        if (rolling_avg_)  {
            SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   avg
                { MeanVisitor<T, I>(), rolling_period_ } ;

            avg.pre();
            avg (idx_begin, idx_end, result.begin(), result.end());
            avg.post();
            result_ = std::move(avg.get_result());

        }
        else
            result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    BalanceOfPowerVisitor(bool rolling_avg = false,
                          size_type rolling_period = 14)
        : rolling_avg_(rolling_avg), rolling_period_(rolling_period)  {  }

private:

    result_type     result_ {  };
    const bool      rolling_avg_;
    const size_type rolling_period_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using bop_v = BalanceOfPowerVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  ChandeKrollStopVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("ChandeKrollStopVisitor: All columns must "
                                 "be of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        TrueRangeVisitor<T, I, A>                       atr
            { true, p_period_ };
        SimpleRollAdopter<MaxVisitor<T, I>, T, I, A>    max1
            { MaxVisitor<T, I> { true }, p_period_ };

        atr.pre();
        max1.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&atr,
                     &idx_begin, &idx_end,
                     &low_begin, &low_end,
                     &high_begin, &high_end,
                     &close_begin, &close_end]() -> void  {
                         atr(idx_begin, idx_end,
                             low_begin, low_end,
                             high_begin, high_end,
                             close_begin, close_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&max1,
                     &idx_begin, &idx_end,
                     &high_begin, &high_end]() -> void  {
                         max1 (idx_begin, idx_end, high_begin, high_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            atr(idx_begin, idx_end,
                low_begin, low_end,
                high_begin, high_end,
                close_begin, close_end);
            max1 (idx_begin, idx_end, high_begin, high_end);
        }
        atr.post();
        max1.post();

        result_type long_stop { std::move(max1.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&atr = std::as_const(atr.get_result()),
                     &long_stop, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            long_stop[i] -= this->multiplier_ * atr[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i)
                long_stop[i] -= multiplier_ * atr.get_result()[i];
        }

        SimpleRollAdopter<MaxVisitor<T, I>, T, I, A>    max2
            { MaxVisitor<T, I> { true }, q_period_ };
        SimpleRollAdopter<MinVisitor<T, I>, T, I, A>    min1
            { MinVisitor<T, I> { true }, p_period_ };

        max2.pre();
        min1.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&max2,
                     &idx_begin, &idx_end, &long_stop]() -> void  {
                         max2 (idx_begin, idx_end,
                               long_stop.begin(), long_stop.end());
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&min1,
                     &idx_begin, &idx_end,
                     &low_begin, &low_end]() -> void  {
                         min1 (idx_begin, idx_end, low_begin, low_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            max2 (idx_begin, idx_end, long_stop.begin(), long_stop.end());
            min1 (idx_begin, idx_end, low_begin, low_end);
        }
        max2.post();
        min1.post();
        long_stop = std::move(max2.get_result());

        result_type short_stop { std::move(min1.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&atr = std::as_const(atr.get_result()),
                     &short_stop, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            short_stop[i] += this->multiplier_ * atr[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]
                short_stop[i] += multiplier_ * atr.get_result()[i];
        }

        SimpleRollAdopter<MinVisitor<T, I>, T, I, A>    min2
            { MinVisitor<T, I> { true }, q_period_ };

        min2.pre();
        min2 (idx_begin, idx_end, short_stop.begin(), short_stop.end());
        min2.post();
        short_stop = std::move(min2.get_result());

        long_stop_ = std::move(long_stop);
        short_stop_ = std::move(short_stop);
    }

    inline void pre ()  {

        long_stop_.clear();
        short_stop_.clear();
    }
    inline void post ()  {   }

    const result_type &get_result() const  { return (long_stop_); }
    result_type &get_result()  { return (long_stop_); }
    const result_type &get_long_stop() const  { return (long_stop_); }
    result_type &get_long_stop()  { return (long_stop_); }
    const result_type &get_short_stop() const  { return (short_stop_); }
    result_type &get_short_stop()  { return (short_stop_); }

    explicit
    ChandeKrollStopVisitor(size_type p_period = 10,
                           size_type q_period = 20,
                           value_type multiplier = 3)
        : p_period_(p_period),
          q_period_(q_period),
          multiplier_(multiplier)  {  }

private:

    result_type         long_stop_ {  };
    result_type         short_stop_ {  };
    const size_type     p_period_;
    const size_type     q_period_;
    const value_type    multiplier_; // X
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using cksp_v = ChandeKrollStopVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  VortexVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_period_ >= (col_s - 1))
            throw DataFrameError("VortexVisitor: All columns must be of "
                                 "equal sizes and roll period < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        TrueRangeVisitor<T, I, A>   tr { false };

        tr.pre();
        tr(idx_begin, idx_end,
           low_begin, low_end,
           high_begin, high_end,
           close_begin, close_end);
        tr.post();

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    tr_sum
            { SumVisitor<T, I>(), roll_period_ };

        tr_sum.pre();
        tr_sum (idx_begin, idx_end,
                tr.get_result().begin(), tr.get_result().end());
        tr_sum.post();

        result_type plus_indicator(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type minus_indicator(col_s,
                                    std::numeric_limits<T>::quiet_NaN());
        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&high_begin, &low_begin,
                     &plus_indicator, &minus_indicator]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const auto  high = high_begin + i;
                            const auto  low = low_begin + i;

                            plus_indicator[i] =
                                std::fabs(*high - *std::prev(low));
                            minus_indicator[i] =
                                std::fabs(*low - *std::prev(high));
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 1 }; i < col_s; ++i) [[likely]]  {
                const auto  high = high_begin + i;
                const auto  low = low_begin + i;

                plus_indicator[i] = std::fabs(*high - *std::prev(low));
                minus_indicator[i] = std::fabs(*low - *std::prev(high));
            }
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    vtx_sum
            { SumVisitor<T, I>(), roll_period_ };

        vtx_sum.pre();
        vtx_sum (idx_begin, idx_end,
                 plus_indicator.begin(), plus_indicator.end());
        vtx_sum.post();
        plus_indicator = std::move(vtx_sum.get_result());

        vtx_sum.pre();
        vtx_sum (idx_begin, idx_end,
                 minus_indicator.begin(), minus_indicator.end());
        vtx_sum.post();
        minus_indicator = std::move(vtx_sum.get_result());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&tr_sum = std::as_const(tr_sum.get_result()),
                     &plus_indicator, &minus_indicator]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    val { tr_sum[i] };

                            plus_indicator[i] /= val;
                            minus_indicator[i] /= val;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                const value_type    val { tr_sum.get_result()[i] };

                plus_indicator[i] /= val;
                minus_indicator[i] /= val;
            }
        }

        plus_indicator_ = std::move(plus_indicator);
        minus_indicator_ = std::move(minus_indicator);
    }

    inline void pre ()  {

        plus_indicator_.clear();
        minus_indicator_.clear();
    }
    inline void post ()  {   }

    const result_type &get_result() const  { return (plus_indicator_); }
    result_type &get_result()  { return (plus_indicator_); }
    const result_type &get_plus_indicator() const  { return(plus_indicator_); }
    result_type &get_plus_indicator()  { return (plus_indicator_); }
    const result_type &
    get_minus_indicator() const  { return (minus_indicator_); }
    result_type &get_minus_indicator()  { return (minus_indicator_); }

    explicit
    VortexVisitor(size_type roll_period = 14) : roll_period_(roll_period)  {  }

private:

    result_type     plus_indicator_ {  };
    result_type     minus_indicator_ {  };
    const size_type roll_period_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using vtx_v = VortexVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  KeltnerChannelsVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_period_ >= (col_s - 1))
            throw DataFrameError("KeltnerChannelsVisitor: All columns must "
                                 "be of equal sizes and roll period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        TrueRangeVisitor<T, I, A>   tr { false };
        ewm_v<T, I, A>              basis(
            exponential_decay_spec::span, roll_period_, true);

        tr.pre();
        basis.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&tr,
                     &idx_begin, &idx_end,
                     &low_begin, &low_end,
                     &high_begin, &high_end,
                     &close_begin, &close_end]() -> void  {
                        tr (idx_begin, idx_end,
                            low_begin, low_end,
                            high_begin, high_end,
                            close_begin, close_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&basis,
                     &idx_begin, &idx_end,
                     &close_begin, &close_end]() -> void  {
                        basis (idx_begin, idx_end, close_begin, close_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            tr (idx_begin, idx_end,
                low_begin, low_end,
                high_begin, high_end,
                close_begin, close_end);
            basis (idx_begin, idx_end, close_begin, close_end);
        }
        tr.post();
        basis.post();

        ewm_v<T, I, A>  band(exponential_decay_spec::span, roll_period_, true);

        band.pre();
        band (idx_begin, idx_end,
              tr.get_result().begin(), tr.get_result().end());
        band.post();

        // I am doing this in two loops to reuse the vector spaces
        //
        result_type lower_band { std::move(tr.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&basis = std::as_const(basis.get_result()),
                     &band = std::as_const(band.get_result()),
                     &lower_band, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            lower_band[i] = basis[i] - this->b_mult_ * band[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]
                lower_band[i] =
                    basis.get_result()[i] - b_mult_ * band.get_result()[i];
        }

        result_type upper_band { std::move(basis.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&band = std::as_const(band.get_result()),
                     &upper_band, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            upper_band[i] += this->b_mult_ * band[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(band.get_result().begin(),
                           band.get_result().begin() + col_s,
                           upper_band.begin(),
                           upper_band.begin(),
                           [this]
                           (const auto &b, const auto &ub) -> value_type  {
                               return (ub + this->b_mult_ * b);
                           });
        }

        upper_band_ = std::move(upper_band);
        lower_band_ = std::move(lower_band);
    }

    inline void pre ()  {

        upper_band_.clear();
        lower_band_.clear();
    }
    inline void post ()  {  }
    const result_type &get_result() const  { return (upper_band_); }
    result_type &get_result()  { return (upper_band_); }
    const result_type &get_upper_band() const  { return (upper_band_); }
    result_type &get_upper_band()  { return (upper_band_); }
    const result_type &get_lower_band() const  { return (lower_band_); }
    result_type &get_lower_band()  { return (lower_band_); }

    explicit
    KeltnerChannelsVisitor(size_type roll_period = 20,
                           value_type band_multiplier = 2.0)
        : roll_period_(roll_period), b_mult_(band_multiplier)  {   }

private:

    result_type         upper_band_ { };
    result_type         lower_band_ { };
    const size_type     roll_period_;
    const value_type    b_mult_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using kch_v = KeltnerChannelsVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  TrixVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

#ifdef HMDF_SANITY_EXCEPTIONS
        if (std::distance(column_begin, column_end) < 3)
            throw DataFrameError("TrixVisitor: column size must be > 3");
#endif // HMDF_SANITY_EXCEPTIONS

        ewm_v<T, I, A>  ewm13(exponential_decay_spec::span,
                              roll_period_,
                              true);

        ewm13.pre();
        ewm13 (idx_begin, idx_end, column_begin, column_end);
        ewm13.post();

        ewm_v<T, I, A>  ewm2(exponential_decay_spec::span, roll_period_, true);

        ewm2.pre();
        ewm2 (idx_begin, idx_end,
              ewm13.get_result().begin(), ewm13.get_result().end());
        ewm2.post();

        ewm13.pre();
        ewm13 (idx_begin, idx_end,
               ewm2.get_result().begin(), ewm2.get_result().end());
        ewm13.post();

        ReturnVisitor<T, I, A>  ret { return_policy::percentage };

        ret.get_result().swap(ewm2.get_result());
        ret.pre();
        ret (idx_begin, idx_end,
             ewm13.get_result().begin(), ewm13.get_result().end());
        ret.post();

        result_type result { std::move(ret.get_result()) };

        if (avg_signal_)  {
            SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   avg
                { MeanVisitor<T, I> { true }, sroll_period_ } ;

            avg.get_result().swap(ewm13.get_result());
            avg.pre();
            avg (idx_begin, idx_end, result.begin(), result.end());
            avg.post();
            result = std::move(avg.get_result());
        }

        result_ = std::move(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    TrixVisitor (size_type roll_period = 14,
                 bool avg_signal = false,
                 size_type signal_roll_period = 7)
        : roll_period_(roll_period),
          sroll_period_(signal_roll_period),
          avg_signal_(avg_signal)  {   }

private:

    OBO_PORT_DECL

    result_type     result_ {  };
    const size_type roll_period_;
    const size_type sroll_period_;
    const bool      avg_signal_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using trix_v = TrixVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  PrettyGoodOsciVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_period_ >= (col_s - 1))
            throw DataFrameError("PrettyGoodOsciVisitor: All columns must be "
                                 "of equal sizes and roll period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   savg
            { MeanVisitor<T, I>(), roll_period_ } ;
        TrueRangeVisitor<T, I, A>                       atr
            { true, roll_period_ };

        savg.pre();
        atr.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&savg,
                     &idx_begin, &idx_end,
                     &close_begin, &close_end]() -> void  {
                        savg (idx_begin, idx_end, close_begin, close_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&atr,
                     &idx_begin, &idx_end,
                     &high_begin, &high_end,
                     &low_begin, &low_end,
                     &close_begin, &close_end]() -> void  {
                        atr(idx_begin, idx_end,
                            low_begin, low_end,
                            high_begin, high_end,
                            close_begin, close_end);
                    });

            fut1.get();
            fut2.get();
        }
        else  {
            savg (idx_begin, idx_end, close_begin, close_end);
            atr(idx_begin, idx_end,
                low_begin, low_end,
                high_begin, high_end,
                close_begin, close_end);
        }
        savg.post();
        atr.post();

        result_type result { std::move(savg.get_result()) };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &close_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  &r = result[i];

                            r = *(close_begin + i) - r;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(close_begin, close_begin + col_s,
                           result.begin(),
                           result.begin(),
                           [](const auto &c, const auto &r) -> value_type  {
                               return (c - r);
                           });
        }

        ewm_v<T, I, A>  ewm(exponential_decay_spec::span, roll_period_, true);

        ewm.pre();
        ewm (idx_begin, idx_end,
             atr.get_result().begin(), atr.get_result().end());
        ewm.post();

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&ewm = std::as_const(ewm.get_result()),
                     &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] /= ewm[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(ewm.get_result().begin(),
                           ewm.get_result().begin() + col_s,
                           result.begin(),
                           result.begin(),
                           [](const auto &ew, const auto &r) -> value_type  {
                               return (r / ew);
                           });
        }
        result_ = std::move(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    PrettyGoodOsciVisitor (size_type roll_period = 14)
        : roll_period_(roll_period)  {   }

private:

    result_type     result_ {  };
    const size_type roll_period_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using pgo_v = PrettyGoodOsciVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  T3MovingMeanVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        erm_t   e1(exponential_decay_spec::span, rolling_period_);

        e1.pre();
        e1(idx_begin, idx_end, column_begin, column_end);
        e1.post();

        erm_t   e2(exponential_decay_spec::span, rolling_period_);

        e2.pre();
        e2(idx_begin, idx_end, e1.get_result().begin(), e1.get_result().end());
        e2.post();

        erm_t   e3(exponential_decay_spec::span, rolling_period_);

        e3.pre();
        e3(idx_begin, idx_end, e2.get_result().begin(), e2.get_result().end());
        e3.post();

        erm_t   e4(exponential_decay_spec::span, rolling_period_);

        e4.pre();
        e4(idx_begin, idx_end, e3.get_result().begin(), e3.get_result().end());
        e4.post();

        erm_t   e5(exponential_decay_spec::span, rolling_period_);

        e5.pre();
        e5(idx_begin, idx_end, e4.get_result().begin(), e4.get_result().end());
        e5.post();

        erm_t   e6(exponential_decay_spec::span, rolling_period_);

        e6.pre();
        e6(idx_begin, idx_end, e5.get_result().begin(), e5.get_result().end());
        e6.post();

        GET_COL_SIZE2

        const double    c1 { -v_factor_ * v_factor_ * v_factor_ };
        const double    c2 { 3.0 * v_factor_ * v_factor_ +
                             3.0 * v_factor_ * v_factor_ * v_factor_ };
        const double    c3 { -6.0 * v_factor_ * v_factor_ -
                             3.0 * v_factor_ -
                             3.0 * v_factor_ * v_factor_ * v_factor_ };
        const double    c4 { v_factor_ * v_factor_ * v_factor_ +
                             3.0 * v_factor_ * v_factor_ +
                             3.0 * v_factor_ +
                             1.0 };
        result_type     result { std::move(e6.get_result()) };

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&e5 = std::as_const(e5.get_result()),
                     &e4 = std::as_const(e4.get_result()),
                     &e3 = std::as_const(e3.get_result()),
                     &result, c1, c2, c3, c4]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            auto    &res = result[i];

                            res = c1 * res +
                                  c2 * e5[i] +
                                  c3 * e4[i] +
                                  c4 * e3[i];
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                auto    &res = result[i];

                res = c1 * res +
                      c2 * e5.get_result()[i] +
                      c3 * e4.get_result()[i] +
                      c4 * e3.get_result()[i];
            }
        }

        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    T3MovingMeanVisitor(size_type rolling_period = 10,
                        double volum_factor = 0.7)
        : rolling_period_(rolling_period), v_factor_(volum_factor)  {  }

private:

    using erm_t = ewm_v<T, I, A>;

    OBO_PORT_DECL

    result_type     result_ {  };
    const size_type rolling_period_;
    const double    v_factor_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using t3_v = T3MovingMeanVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// This is meaningfull, only if the return series is close to normal
// distribution
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  TreynorRatioVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &asset_ret_begin, const H &asset_ret_end,
                const H &benchmark_ret_begin, const H &benchmark_ret_end)  {

        const size_type col_s = std::distance(asset_ret_begin, asset_ret_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        const size_type b_s =
            std::distance(benchmark_ret_begin, benchmark_ret_end);

        if (col_s != b_s || col_s <= 3)
            throw DataFrameError("TreynorRatioVisitor: All columns must be of "
                                 "equal sizes and column size > 3");
#endif // HMDF_SANITY_EXCEPTIONS

        value_type          cum_return { 0.0 };
        BetaVisitor<T, I>   beta_vis { biased_ };

        beta_vis.pre();
        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&beta_vis,
                     &idx_begin, &idx_end,
                     &asset_ret_begin, &asset_ret_end,
                     &benchmark_ret_begin, &benchmark_ret_end]() -> void  {
                        beta_vis(idx_begin, idx_end,
                                 asset_ret_begin, asset_ret_end,
                                 benchmark_ret_begin, benchmark_ret_end);
                    });
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&asset_ret_begin, &benchmark_ret_begin]
                    (auto begin, auto end) mutable -> value_type  {
                        value_type  cum_return { 0.0 };

                        for (size_type i = begin; i < end; ++i) [[likely]]
                            cum_return += *(asset_ret_begin + i) -
                                          *(benchmark_ret_begin + i);
                        return (cum_return);
                    });

            for (auto &fut : futures)  cum_return += fut.get();
            fut1.get();
        }
        else  {
            const index_type    &index_val { *idx_begin }; // Ignored
            auto                a_citer { asset_ret_begin };

            for (auto b_citer { benchmark_ret_begin };
                 b_citer != benchmark_ret_end; ++a_citer, ++b_citer)  {
                beta_vis (index_val, *a_citer, *b_citer);
                cum_return += *a_citer - *b_citer;
            }
        }
        beta_vis.post();
        result_ = (cum_return / T(col_s)) / beta_vis.get_result();
    }

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    explicit
    TreynorRatioVisitor(bool biased = false) : biased_ (biased)  {  }

private:

    const bool  biased_;
    result_type result_ { 0 };
};

template<typename T, typename I = unsigned long>
using treynorr_v = TreynorRatioVisitor<T, I>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  InertiaVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const H &high_begin, const H &high_end,
                const H &low_begin, const H &low_end)  {

#ifdef HMDF_SANITY_EXCEPTIONS
        const size_type col_s = std::distance(close_begin, close_end);

        if (col_s != size_type(std::distance(high_begin, high_end)) ||
            col_s != size_type(std::distance(low_begin, low_end)) ||
            roll_period_ >= col_s ||
            roll_period_ <= rvi_rp_)
            throw DataFrameError("InertiaVisitor: All columns must be of "
                                 "equal sizes and roll period < column size "
                                 "and roll period > RVI roll period");
#endif // HMDF_SANITY_EXCEPTIONS

        rvi_v<T, I> rvi { rvi_rp_ };

        rvi.pre();
        rvi(idx_begin, idx_end,
            close_begin, close_end,
            high_begin, high_end,
            low_begin, low_end);
        rvi.post();

        linregmm_v<T, I>    linreg { roll_period_ };

        linreg.pre();
        linreg(idx_begin, idx_end,
               rvi.get_result().begin(), rvi.get_result().end());
        linreg.post();

        result_ = std::move(linreg.get_result());
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    InertiaVisitor(size_type roll_period = 20, size_type rvi_roll_period = 14)
        : roll_period_(roll_period), rvi_rp_(rvi_roll_period)  {   }

private:

    const size_type roll_period_;
    const size_type rvi_rp_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using iner_v = InertiaVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  RelativeVigorIndexVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(open_begin, open_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_period_ >= (col_s - 1) ||
            swma_period_ >= roll_period_)
            throw DataFrameError("RelativeVigorIndexVisitor: All columns must "
                                 "be of equal sizes and roll period < column "
                                 "size and roll period < SWMA period");
#endif // HMDF_SANITY_EXCEPTIONS

        nzr_v<T, I, A>  non_z_range;

        non_z_range.pre();
        non_z_range(idx_begin, idx_end,
                    high_begin, high_end, low_begin, low_end);
        non_z_range.post();

        result_type high_low_range = std::move(non_z_range.get_result());

        non_z_range.pre();
        non_z_range(idx_begin, idx_end,
                    close_begin, close_end, open_begin, open_end);
        non_z_range.post();

        result_type         close_open_range =
            std::move(non_z_range.get_result());
        symtmm_v<T, I, A>   symm { swma_period_ };

        symm.pre();
        symm(idx_begin, idx_end,
             close_open_range.begin(), close_open_range.end());
        symm.post();
        close_open_range.swap(symm.get_result());  // numerator

        symm.pre();
        symm(idx_begin, idx_end, high_low_range.begin(), high_low_range.end());
        symm.post();
        high_low_range.swap(symm.get_result());  // denominator

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    sum
            { SumVisitor<T, I> { true }, roll_period_ };

        sum.pre();
        sum(idx_begin, idx_end,
             close_open_range.begin(), close_open_range.end());
        sum.post();
        close_open_range.swap(sum.get_result());  // numerator

        sum.pre();
        sum(idx_begin, idx_end, high_low_range.begin(), high_low_range.end());
        sum.post();
        high_low_range.swap(sum.get_result());  // denominator

        result_type result(col_s);  // rvgi

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&close_open_range = std::as_const(close_open_range),
                     &high_low_range = std::as_const(high_low_range),
                     &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] =
                                close_open_range[i] / high_low_range[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(close_open_range.begin(),
                           close_open_range.begin() + col_s,
                           high_low_range.begin(),
                           result.begin(),
                           [](const auto &co, const auto &hl) -> value_type  {
                               return (co / hl);
                           });
        }

        symm.pre();
        symm(idx_begin, idx_end, result.begin(), result.end());
        symm.post();

        result_.swap(result);
        signal_.swap(symm.get_result());
    }

    inline void pre ()  { result_.clear(); signal_.clear(); }
    inline void post ()  {  }

    const result_type &get_result() const  { return (result_); }
    result_type &get_result()  { return (result_); }
    const result_type &get_signal() const  { return (signal_); }
    result_type &get_signal()  { return (signal_); }

    explicit
    RelativeVigorIndexVisitor(size_type roll_period = 14,
                              size_type swma_period = 4)
        : roll_period_(roll_period), swma_period_(swma_period)  {  }

private:

    result_type     result_ {  };
    result_type     signal_ {  };
    const size_type roll_period_;
    const size_type swma_period_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using rvgi_v = RelativeVigorIndexVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  ElderRayIndexVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const H &high_begin, const H &high_end,
                const H &low_begin, const H &low_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_period_ >= (col_s - 1))
            throw DataFrameError("ElderRayIndexVisitor: All columns must be "
                                 "of equal sizes and roll period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        ewm_v<T, I, A>  ewm(exponential_decay_spec::span, roll_period_, true);

        ewm.pre();
        ewm (idx_begin, idx_end, close_begin, close_end);
        ewm.post();

        result_type bulls = ewm.get_result();
        result_type bears = std::move(ewm.get_result());

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&bulls, &bears, &low_begin, &high_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  &bull = bulls[i];
                            value_type  &bear = bears[i];

                            bull = *(high_begin + i) - bull;
                            bear = *(low_begin + i) - bear;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                value_type  &bull = bulls[i];
                value_type  &bear = bears[i];

                bull = *(high_begin + i) - bull;
                bear = *(low_begin + i) - bear;
            }
        }

        result_.swap(bulls);
        bear_vec_.swap(bears);
    }

    inline void pre ()  { result_.clear(); bear_vec_.clear(); }
    inline void post ()  {  }

    const result_type &get_result() const  { return (result_); }
    result_type &get_result()  { return (result_); }
    const result_type &get_bears() const  { return (bear_vec_); }
    result_type &get_bears()  { return (bear_vec_); }

    explicit
    ElderRayIndexVisitor (size_type roll_period = 13)
        : roll_period_(roll_period)  {   }

private:

    result_type     result_ {  };    // Bull vector
    result_type     bear_vec_ {  };  // Bear vector
    const size_type roll_period_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using eri_v = ElderRayIndexVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  ChopIndexVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const H &high_begin, const H &high_end,
                const H &low_begin, const H &low_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(high_begin, high_end)) ||
            col_s != size_type(std::distance(low_begin, low_end)) ||
            roll_period_ >= col_s)
            throw DataFrameError("ChopIndexVisitor: All columns must be of "
                                 "equal size and roll period < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        SimpleRollAdopter<MaxVisitor<T, I>, T, I, A>    maxv
            { MaxVisitor<T, I>(), roll_period_ };
        SimpleRollAdopter<MinVisitor<T, I>, T, I, A>    minv
            { MinVisitor<T, I>(), roll_period_ };
        TrueRangeVisitor<T, I, A>                       atr
            { true, atr_period_ };

        atr.pre();
        maxv.pre();
        minv.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&maxv,
                     &idx_begin, &idx_end,
                     &high_begin, &high_end]() -> void  {
                        maxv (idx_begin, idx_end, high_begin, high_end);
                    });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&minv,
                     &idx_begin, &idx_end,
                     &low_begin, &low_end]() -> void  {
                        minv (idx_begin, idx_end, low_begin, low_end);
                    });
            auto    fut3 =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    [&atr,
                     &idx_begin, &idx_end,
                     &high_begin, &high_end,
                     &low_begin, &low_end,
                     &close_begin, &close_end]() -> void  {
                        atr (idx_begin, idx_end,
                             low_begin, low_end,
                             high_begin, high_end,
                             close_begin, close_end);
                    });

            fut1.get();
            fut2.get();
            fut3.get();
        }
        else  {
            maxv (idx_begin, idx_end, high_begin, high_end);
            minv (idx_begin, idx_end, low_begin, low_end);
            atr (idx_begin, idx_end,
                 low_begin, low_end,
                 high_begin, high_end,
                 close_begin, close_end);
        }
        maxv.post();
        minv.post();
        atr.post();

        result_type diff = std::move(maxv.get_result());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&minv = std::as_const(minv.get_result()), &diff]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            diff[i] -= minv[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(minv.get_result().begin(),
                           minv.get_result().begin() + col_s,
                           diff.begin(),
                           diff.begin(),
                           [](const auto &m, const auto &d) -> value_type  {
                               return (d - m);
                           });
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>    atr_sum
            { SumVisitor<T, I>(), roll_period_ };

        atr_sum.get_result().swap(minv.get_result());  // Reuse the space
        atr_sum.pre();
        atr_sum (idx_begin, idx_end,
                 atr.get_result().begin(), atr.get_result().end());
        atr_sum.post();

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&atr_sum = std::as_const(atr_sum.get_result()), &diff,
                     rp = this->roll_period_]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            value_type  &d = diff[i];

                            d = T(100) *
                                (std::log10(atr_sum[i]) - std::log10(d)) /
                                std::log10(T(rp));
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(atr_sum.get_result().begin(),
                           atr_sum.get_result().begin() + col_s,
                           diff.begin(),
                           diff.begin(),
                           [rp = this->roll_period_]
                           (const auto &as, const auto &d) -> value_type  {
                               return (T(100) *
                                       (std::log10(as) - std::log10(d)) /
                                       std::log10(T(rp)));
                           });
        }
        result_.swap(diff);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    ChopIndexVisitor(size_type roll_period = 14, size_type atr_period = 1)
        : roll_period_(roll_period), atr_period_(atr_period)  {   }

private:

    result_type     result_ { };
    const size_type roll_period_;
    const size_type atr_period_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using chop_v = ChopIndexVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  DetrendPriceOsciVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s <= 3 || col_s <= roll_period_)
            throw DataFrameError("DetrendPriceOsciVisitor: column size must "
                                 "be > 3 and roll period < column size");
#endif // HMDF_SANITY_EXCEPTIONS

        SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   savg
            { MeanVisitor<T, I>(), roll_period_ } ;

        savg.pre();
        savg (idx_begin, idx_end, column_begin, column_end);
        savg.post();

        const size_type shift { roll_period_ / 2 + 1 };
        result_type     result(col_s, std::numeric_limits<T>::quiet_NaN());

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    shift,
                    col_s,
                    [&savg = std::as_const(savg.get_result()),
                     &result, &column_begin, shift]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] = *(column_begin + i) - savg[i - shift];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin + shift, column_begin + col_s,
                           savg.get_result().begin(), // Offset by (i - shift)
                           result.begin() + shift,
                           [] (const auto &c, const auto &s) -> value_type  {
                               return (c - s);
                           });
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    DetrendPriceOsciVisitor(size_type roll_period = 20)
        : roll_period_(roll_period)  {  }

private:

    OBO_PORT_DECL

    result_type     result_ {  };
    const size_type roll_period_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using dpo_v = DetrendPriceOsciVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  AccelerationBandsVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const H &high_begin, const H &high_end,
                const H &low_begin, const H &low_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            roll_period_ == 0 || roll_period_ >= col_s)
            throw DataFrameError("AccelerationBandsVisitor: All columns must "
                                 "be of equal sizes and roll period < column "
                                 "size roll period > 0");
#endif // HMDF_SANITY_EXCEPTIONS

        NonZeroRangeVisitor<T, I, A>    nzr;

        nzr.pre();
        nzr(idx_begin, idx_end, high_begin, high_end, low_begin, low_end);
        nzr.post();

        result_type lower_band(col_s);
        result_type upper_band(col_s);

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&nzr = std::as_const(nzr.get_result()),
                     &lower_band, &upper_band,
                     &low_begin, &high_begin, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    low = *(low_begin + i);
                            const value_type    high = *(high_begin + i);
                            const value_type    hl_ratio =
                                (nzr[i] / (high + low)) * this->mlp_;

                            lower_band[i] = low * (T(1) - hl_ratio);
                            upper_band[i] = high * (T(1) + hl_ratio);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                const value_type    low = *(low_begin + i);
                const value_type    high = *(high_begin + i);
                const value_type    hl_ratio =
                    (nzr.get_result()[i] / (high + low)) * mlp_;

                lower_band[i] = low * (T(1) - hl_ratio);
                upper_band[i] = high * (T(1) + hl_ratio);
            }
        }

        SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   savg
            { MeanVisitor<T, I>(), roll_period_ } ;

        savg.get_result().swap(nzr.get_result());  // Reuse the space
        savg.pre();
        savg (idx_begin, idx_end, lower_band.begin(), lower_band.end());
        savg.post();
        lower_band_ = std::move(savg.get_result());

        savg.get_result().swap(lower_band);  // Reuse the space
        savg.pre();
        savg (idx_begin, idx_end, close_begin, close_end);
        savg.post();
        result_ = std::move(savg.get_result());

        savg.pre();
        savg (idx_begin, idx_end, upper_band.begin(), upper_band.end());
        savg.post();
        upper_band_ = std::move(savg.get_result());
    }

    inline void pre ()  {

        result_.clear();
        lower_band_.clear();
        upper_band_.clear();
    }
    inline void post ()  {  }
    const result_type &get_result() const  { return (result_); }
    result_type &get_result()  { return (result_); }
    const result_type &get_lower_band() const  { return (lower_band_); }
    result_type &get_lower_band()  { return (lower_band_); }
    const result_type &get_upper_band() const  { return (upper_band_); }
    result_type &get_upper_band()  { return (upper_band_); }

    explicit
    AccelerationBandsVisitor(size_type roll_period = 20,
                             value_type multiplier = 4)
        : roll_period_(roll_period), mlp_(multiplier)  {   }

private:

    const size_type roll_period_;
    const size_type mlp_;
    result_type     result_ { };  // Mid-band
    result_type     lower_band_ { };
    result_type     upper_band_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using aband_v = AccelerationBandsVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  PriceDistanceVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(open_begin, open_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("PriceDistanceVisitor: All columns must be "
                                 "of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        nzr_v<T, I, A>  nzr;

        nzr.pre();
        nzr(idx_begin, idx_end, high_begin, high_end, low_begin, low_end);
        nzr.post();

        result_type result(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&nzr = std::as_const(nzr.get_result()), &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] = T(2) * nzr[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(nzr.get_result().begin(),
                           nzr.get_result().begin() + col_s,
                           result.begin(),
                           [](const auto &n) -> value_type  {
                               return (T(2) * n);
                           });
        }

        nzr.pre();
        nzr(idx_begin, idx_end,
            // FIXME: "close_begin - 1" is not good
            open_begin, open_end, close_begin - 1, close_end);
        nzr.post();
        result[0] = std::numeric_limits<T>::quiet_NaN();
        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&nzr = std::as_const(nzr.get_result()), &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] += abs__(nzr[i]);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(nzr.get_result().begin() + 1,
                           nzr.get_result().begin() + col_s,
                           result.begin() + 1,
                           result.begin() + 1,
                           [](const auto &n, const auto &r) -> value_type  {
                               return (r + abs__(n));
                           });
        }

        nzr.pre();
        nzr(idx_begin, idx_end, close_begin, close_end, open_begin, open_end);
        nzr.post();
        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&nzr = std::as_const(nzr.get_result()), &result]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] -= abs__(nzr[i]);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(nzr.get_result().begin(),
                           nzr.get_result().begin() + col_s,
                           result.begin(),
                           result.begin(),
                           [](const auto &n, const auto &r) -> value_type  {
                               return (r - abs__(n));
                           });
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    PriceDistanceVisitor() = default;

private:

    result_type result_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using pdist_v = PriceDistanceVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  EldersThermometerVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    using bool_vec =
        std::vector<bool, typename allocator_declare<bool, A>::type>;

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end)  {

        const size_type col_s = std::distance(low_begin, low_end);
        const auto      thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(high_begin, high_end)))
            throw DataFrameError("EldersThermometerVisitor: All columns must "
                                 "be of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result(col_s, std::numeric_limits<T>::quiet_NaN());

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&result, &low_begin, &high_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    low =
                                abs__(*(low_begin + (i - 1)) -
                                      *(low_begin + i));
                            const value_type    high =
                                abs__(*(high_begin + i) -
                                      *(high_begin + (i - 1)));

                            result[i] = std::max(high, low);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 1 }; i < col_s; ++i) [[likely]]  {
                const value_type    low =
                    abs__(*(low_begin + (i - 1)) - *(low_begin + i));
                const value_type    high =
                    abs__(*(high_begin + i) - *(high_begin + (i - 1)));

                result[i] = std::max(high, low);
            }
        }

        ewm_v<T, I, A>  ewm(exponential_decay_spec::span, roll_period_, true);

        ewm.pre();
        ewm (idx_begin, idx_end, result.begin(), result.end());
        ewm.post();

        bool_vec    t_long(col_s);
        bool_vec    t_short(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&ewm = std::as_const(ewm.get_result()),
                     &result = std::as_const(result),
                     &t_long, &t_short, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type   thermo = result[i];
                            const value_type   thermo_ma = ewm[i];

                            t_long[i] = thermo < (thermo_ma * this->buy_f_);
                            t_short[i] = thermo > (thermo_ma * this->sell_f_);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]  {
                const value_type    thermo = result[i];
                const value_type    thermo_ma = ewm.get_result()[i];

                t_long[i] = thermo < (thermo_ma * buy_f_);
                t_short[i] = thermo > (thermo_ma * sell_f_);
            }
        }

        result_.swap(result);
        result_ma_.swap(ewm.get_result());
        buy_signal_.swap(t_long);
        sell_signal_.swap(t_short);
    }

    inline void pre ()  {

        result_.clear();
        result_ma_.clear();
        buy_signal_.clear();
        sell_signal_.clear();
    }
    inline void post ()  {  }
    const result_type &get_result() const  { return (result_); }
    result_type &get_result()  { return (result_); }
    const result_type &get_result_ma() const  { return (result_ma_); }
    result_type &get_result_ma()  { return (result_ma_); }
    const bool_vec &get_buy_signal() const  { return (buy_signal_); }
    bool_vec &get_buy_signal()  { return (buy_signal_); }
    const bool_vec &get_sell_signal() const  { return (sell_signal_); }
    bool_vec &get_sell_signal()  { return (sell_signal_); }

    explicit
    EldersThermometerVisitor(size_type roll_period = 20,
                             value_type buy_factor = 2,
                             value_type sell_factor = 0.5)
        : roll_period_(roll_period),
          buy_f_(buy_factor),
          sell_f_(sell_factor)  {   }


private:

    const size_type     roll_period_;
    const value_type    buy_f_;
    const value_type    sell_f_;
    result_type         result_ {  };
    result_type         result_ma_ {  };
    bool_vec            buy_signal_ {  };
    bool_vec            sell_signal_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ether_v = EldersThermometerVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  EldersForceIndexVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H, forward_iterator V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(volume_begin, volume_end)) ||
            roll_period_ >= col_s)
            throw DataFrameError("EldersForceIndexVisitor: All columns must "
                                 "be of equal sizes and roll period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        DiffVisitor<T, I, A>    diff { 1, false };

        diff.pre();
        diff (idx_begin, idx_end, close_begin, close_end);
        diff.post();

        result_type result = std::move(diff.get_result());;

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &volume_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] *= *(volume_begin + i);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i) [[likely]]
                result[i] *= *(volume_begin + i);
        }

        ewm_v<T, I, A>  ewm(exponential_decay_spec::span, roll_period_, true);

        ewm.pre();
        ewm (idx_begin, idx_end, result.begin(), result.end());
        ewm.post();

        result_ = std::move(ewm.get_result());
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    EldersForceIndexVisitor(size_type roll_period = 13)
        : roll_period_(roll_period)  {   }

private:

    result_type     result_ {  };
    const size_type roll_period_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using efi_v = EldersForceIndexVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  EaseOfMovementVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H, forward_iterator V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(low_begin, low_end)) ||
            col_s != size_type(std::distance(high_begin, high_end)) ||
            col_s != size_type(std::distance(volume_begin, volume_end)) ||
            roll_period_ >= col_s)
            throw DataFrameError("EaseOfMovementVisitor: All columns must be "
                                 "of equal sizes and roll period < "
                                 "column size");
#endif // HMDF_SANITY_EXCEPTIONS

        result_type result(col_s, std::numeric_limits<T>::quiet_NaN());

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&result, &low_begin, &high_begin, &volume_begin, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const value_type    low = *(low_begin + i);
                            const value_type    high = *(high_begin + i);
                            const value_type    distance =
                                ((high + low) / T(2)) -
                                ((*(high_begin + (i - 1)) +
                                  *(low_begin + (i - 1))) / T(2));
                            const value_type    box_ratio =
                                (*(volume_begin + i) / this->vol_div_) /
                                (high - low);

                            result[i] = distance / box_ratio;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i { 1 }; i < col_s; ++i)  {
                const value_type    low = *(low_begin + i);
                const value_type    high = *(high_begin + i);
                const value_type    distance =
                    ((high + low) / T(2)) -
                    ((*(high_begin + (i - 1)) +
                      *(low_begin + (i - 1))) / T(2));
                const value_type    box_ratio =
                    (*(volume_begin + i) / vol_div_) / (high - low);

                result[i] = distance / box_ratio;
            }
        }

        SimpleRollAdopter<MeanVisitor<T, I>, T, I, A>   savg
            { MeanVisitor<T, I>(), roll_period_ } ;

        savg.pre();
        savg (idx_begin, idx_end, result.begin(), result.end());
        savg.post();

        result_ = std::move(savg.get_result());
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    EaseOfMovementVisitor(size_type roll_period = 14,
                          value_type vol_divisor = 100000000)
        : roll_period_(roll_period), vol_div_(vol_divisor)  {   }

private:

    result_type         result_ {  };
    const size_type     roll_period_;
    const value_type    vol_div_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using eom_v = EaseOfMovementVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  PriceVolumeTrendVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H, forward_iterator V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(volume_begin, volume_end)))
            throw DataFrameError("PriceVolumeTrendVisitor: All columns must "
                                 "be of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        ReturnVisitor<T, I, A>  ret { return_policy::percentage };

        ret.pre();
        ret (idx_begin, idx_end, close_begin, close_end);
        ret.post();

        result_type result { std::move(ret.get_result()) };

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&result, &volume_begin]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            result[i] *= *(volume_begin + i);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(result.begin(), result.end(),
                           volume_begin,
                           result.begin(),
                           std::multiplies<T>{ });
        }

        CumSumVisitor<T, I, A>  cumsum { true };

        cumsum.pre();
        cumsum (idx_begin, idx_end, result.begin(), result.end());
        cumsum.post();
        result_.swap(cumsum.get_result());
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

private:

    result_type result_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using pvt_v = PriceVolumeTrendVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Quantitative Qualitative Estimation (QQE)
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  QuantQualEstimationVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &price_begin, const H &price_end)  {

        RSIVisitor<T, I, A> rsi { return_policy::monetary, avg_period_ };

        rsi.pre();
        rsi(idx_begin, idx_end, price_begin, price_end);
        rsi.post();

        ewm_v<double, I, A> rsi_ewm(exponential_decay_spec::span,
                                    smooth_period_, true);

        rsi_ewm.pre();
        rsi_ewm(idx_begin, idx_end,
            rsi.get_result().begin(), rsi.get_result().end());
        rsi_ewm.post();

        DiffVisitor<T, I, A>    diff { 1, false, false, true };  // abs value

        rsi_ewm_ = std::move(rsi_ewm.get_result());
        diff.pre();
        diff(idx_begin, idx_end, rsi_ewm_.begin(), rsi_ewm_.end());
        diff.post();

        ewm_v<double, I, A> ewm2(exponential_decay_spec::span,
                                 wilders_period_, true);

        ewm2.pre();
        ewm2(idx_begin, idx_end,
             diff.get_result().begin(), diff.get_result().end());
        ewm2.post();

        ewm_v<double, I, A> ewm3(exponential_decay_spec::span,
                                 wilders_period_, true);

        ewm3.get_result() = std::move(diff.get_result()); // Reuse the space
        ewm3.pre();
        ewm3(idx_begin, idx_end,
             ewm2.get_result().begin(), ewm2.get_result().end());
        ewm3.post();

        std::transform(ewm3.get_result().begin(), ewm3.get_result().end(),
                       ewm3.get_result().begin(),
                       std::bind(std::multiplies<T>(), std::placeholders::_1,
                                 width_factor_));

        const size_type col_s = std::distance(price_begin, price_end);

        result_type upperband(col_s);
        result_type lowerband(col_s);

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&ewm3 = std::as_const(ewm3.get_result()),
                     &upperband, &lowerband, this]
                    (auto begin, auto end) mutable -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const auto  rsi = this->rsi_ewm_[i];
                            const auto  dar = ewm3[i];

                            upperband[i] = rsi + dar;
                            lowerband[i] = rsi - dar;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i = 0; i < col_s; ++i)  {
                const auto  local_rsi = rsi_ewm_[i];
                const auto  dar = ewm3.get_result()[i];

                upperband[i] = local_rsi + dar;
                lowerband[i] = local_rsi - dar;
            }
        }

        result_type         long_line (col_s, 0);
        result_type         short_line (col_s, 0);
        std::vector<bool>   going_up (col_s, true);
        result_type         qqe (col_s, rsi_ewm_[0]);

        for (size_type i = 2; i < col_s; ++i)  {
            const auto  c_rsi = rsi_ewm_[i];          // Current RSI
            const auto  p_rsi = rsi_ewm_[i - 1];      // Prior RSI
            const auto  c_long = long_line[i - 1];    // Current long line
            const auto  p_long = long_line[i - 2];    // Prior long line
            const auto  c_short = short_line[i - 1];  // Current short line
            const auto  p_short = short_line[i - 2];  // Prior short line

            if (p_rsi > c_long && c_rsi > c_long)
                long_line[i] = std::max(c_long, lowerband[i]);
            else
                long_line[i] = lowerband[i];
            if (p_rsi < c_short && c_rsi < c_short)
                short_line[i] = std::min(c_short, upperband[i]);
            else
                short_line[i] = upperband[i];

            // Trend & QQE Calculation
            // Long: Current rsi_ewm value crosses the prior short line value
            // Short: Current rsi_ewm crosses the prior long line value
            //
            if ((c_rsi > c_short && p_rsi < p_short) ||
                (c_rsi <= c_short && p_rsi >= p_short))  {
                going_up[i] = true;
                qqe[i] = long_line[i];
            }
            else if ((c_rsi > c_long && p_rsi < p_long) ||
                     (c_rsi <= c_long && p_rsi >= p_long))  {
                going_up[i] = false;
                qqe[i] = short_line[i];
            }
            else  {
                going_up[i] = going_up[i - 1];
                qqe[i] = going_up[i] ? long_line[i] : short_line[i];
            }
        }

        result_ = std::move(qqe);
        long_line_ = std::move(long_line);
        short_line_ = std::move(short_line);
    }

    inline void pre ()  {

        result_.clear();
        rsi_ewm_.clear();
        long_line_.clear();
        short_line_.clear();
    }
    inline void post ()  {  }
    const result_type &get_result() const  { return (result_); }
    result_type &get_result()  { return (result_); }
    const result_type &get_rsi_ma() const  { return (rsi_ewm_); }
    result_type &get_rsi_ma()  { return (rsi_ewm_); }
    const result_type &get_long_line() const  { return (long_line_); }
    result_type &get_long_line()  { return (long_line_); }
    const result_type &get_short_line() const  { return (short_line_); }
    result_type &get_short_line()  { return (short_line_); }

    explicit
    QuantQualEstimationVisitor(size_type avg_period = 14,
                               size_type smooth_period = 5,
                               value_type width_factor = 4.236)
        : avg_period_(avg_period),
          smooth_period_(smooth_period),
          width_factor_(width_factor)  {  }

private:

    result_type         result_ { };  // qqe
    result_type         rsi_ewm_ { };
    result_type         long_line_ { };
    result_type         short_line_ { };
    const size_type     avg_period_;
    const size_type     smooth_period_;
    const size_type     wilders_period_ { 2 * avg_period_ - 1 };
    const value_type    width_factor_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using qqe_v = QuantQualEstimationVisitor<T, I, A>;

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
