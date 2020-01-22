// Hossein Moein
// January 08, 2020
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
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
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
#include <functional>
#include <iterator>
#include <limits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename S_RT,  // Short duration rolling adopter
         typename L_RT,  // Longer duration rolling adopter
         typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DoubleCrossOver {

private:

    S_RT    short_roller_;
    L_RT    long_roller_;

    template <typename K, typename H>
    inline std::size_t run_short_roller_(const K &idx, const H &column)  {

        short_roller_(idx, column);

        const auto          &result = short_roller_.get_result();
        const std::size_t   col_s =
            std::min<std::size_t>({ idx.size(), column.size(), result.size() });

        col_to_short_term_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            col_to_short_term_.push_back(column[i] - result[i]);
        return (col_s);
    }
    template <typename K, typename H>
    inline std::size_t run_long_roller_(const K &idx, const H &column)  {

        long_roller_(idx, column);

        const auto          &result = long_roller_.get_result();
        const std::size_t   col_s =
            std::min<std::size_t>({ idx.size(), column.size(), result.size() });

        col_to_long_term_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            col_to_long_term_.push_back(column[i] - result[i]);
        return (col_s);
    }

public:

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    inline DoubleCrossOver(S_RT &&sr, L_RT &&lr)
        : short_roller_(std::move(sr)), long_roller_(std::move(lr))  {   }

    template <typename K, typename H>
    inline void
    operator() (const K &idx, const H &column)  {

        const size_type thread_level =
            ThreadGranularity::get_sensible_thread_level();
        size_type       re_count1 = 0;
        size_type       re_count2 = 0;

        if (thread_level >= 2)  {
            std::future<size_type>  fut1 =
                std::async(std::launch::async,
                           &DoubleCrossOver::run_short_roller_<K, H>,
                           this,
                           std::cref(idx),
                           std::cref(column));
            std::future<size_type>  fut2 =
                std::async(std::launch::async,
                           &DoubleCrossOver::run_long_roller_<K, H>,
                           this,
                           std::cref(idx),
                           std::cref(column));

            re_count1 = fut1.get();
            re_count2 = fut2.get();
        }
        else  {
            re_count1 = run_short_roller_(idx, column);
            re_count2 = run_long_roller_(idx, column);
        }

        const size_type col_s = std::min<size_type>(re_count1, re_count2);

        short_term_to_long_term_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            short_term_to_long_term_.push_back(
                short_roller_.get_result()[i] - long_roller_.get_result()[i]);
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
    const result_type &
    get_raw_to_long_term() const  { return (col_to_long_term_); }
    const result_type &
    get_short_term_to_long_term() const  { return (short_term_to_long_term_); }

private:

    result_type col_to_short_term_;
    result_type col_to_long_term_;
    result_type short_term_to_long_term_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct BollingerBand {

private:

    const double                                upper_band_multiplier_;
    const double                                lower_band_multiplier_;
    SimpleRollAdopter<MeanVisitor<T, I>, T, I>  mean_roller_;
    SimpleRollAdopter<StdVisitor<T, I>, T, I>   std_roller_;

    template <typename K, typename H>
    inline void run_mean_roller_(const K &idx, const H &column)  {

        mean_roller_.pre();
        mean_roller_(idx, column);
        mean_roller_.post();
    }

    template <typename K, typename H>
    inline void run_std_roller_(const K &idx, const H &column)  {

        std_roller_.pre();
        std_roller_(idx, column);
        std_roller_.post();
    }

public:

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    inline BollingerBand(double upper_band_multiplier,
                         double lower_band_multiplier,
                         size_type moving_mean_period,
                         bool biased = false)
        : upper_band_multiplier_(upper_band_multiplier),
          lower_band_multiplier_(lower_band_multiplier),
          mean_roller_(std::move(MeanVisitor<T, I>()), moving_mean_period),
          std_roller_(std::move(StdVisitor<T, I>(biased)), moving_mean_period) {
    }

    template <typename K, typename H>
    inline void
    operator() (const K &idx, const H &column)  {

        const size_type thread_level =
            ThreadGranularity::get_sensible_thread_level();

        if (thread_level >= 2)  {
            std::future<void>   fut1 =
                std::async(std::launch::async,
                           &BollingerBand::run_mean_roller_<K, H>,
                           this,
                           std::cref(idx),
                           std::cref(column));
            std::future<void>   fut2 =
                std::async(std::launch::async,
                           &BollingerBand::run_std_roller_<K, H>,
                           this,
                           std::cref(idx),
                           std::cref(column));

            fut1.get();
            fut2.get();
		}
		else  {
            run_mean_roller_(idx, column);
            run_std_roller_(idx, column);
		}

        const auto      &std_result = std_roller_.get_result();
        const auto      &mean_result = mean_roller_.get_result();
        const size_type col_s =
            std::min<size_type>(
                { idx.size(),
                  column.size(),
                  std_result.size(),
                  mean_result.size()
                });

        upper_band_to_raw_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            upper_band_to_raw_.push_back(
                (mean_result[i] + std_result[i] * upper_band_multiplier_) -
                column[i]);

        raw_to_lower_band_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            raw_to_lower_band_.push_back(
                column[i] -
                (mean_result[i] - std_result[i] * lower_band_multiplier_));
    }

    inline void pre ()  {

        upper_band_to_raw_.clear();
        raw_to_lower_band_.clear();
    }
    inline void post ()  {  }

    const result_type &
    get_upper_band_to_raw() const  { return (upper_band_to_raw_); }
    const result_type &
    get_raw_to_lower_band() const  { return (raw_to_lower_band_); }

private:

    result_type upper_band_to_raw_;
    result_type raw_to_lower_band_;
};

// ----------------------------------------------------------------------------

// Moving Average Convergence/Divergence
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MACDVisitor {

private:

    using macd_roller_t = ExponentialRollAdopter<MeanVisitor<T, I>, T, I>;

    const std::size_t   short_mean_period_;
    const std::size_t   long_mean_period_;
    macd_roller_t       signal_line_roller_;

public:

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    inline MACDVisitor(size_type short_mean_period,  // e.g. 12-day
                       size_type long_mean_period,   // e.g. 26-day
                       size_type signal_line_period, // e.g.  9-day
                       exponential_decay_spec ed_spec =
                           exponential_decay_spec::span,
                       double expo_decay_value = 0.2)
        : short_mean_period_(short_mean_period),
          long_mean_period_(long_mean_period),
          signal_line_roller_(std::move(MeanVisitor<T, I>()),
                              signal_line_period,
                              ed_spec, expo_decay_value)  {
    }

    template <typename K, typename H>
    inline void
    operator() (const K &idx, const H &column)  {

        macd_roller_t   short_roller(std::move(MeanVisitor<T, I>()),
                                     short_mean_period_,
                                     exponential_decay_spec::span, 0.2);

        short_roller.pre();
        short_roller(idx, column);
        short_roller.post();

        macd_roller_t   long_roller(std::move(MeanVisitor<T, I>()),
                                    long_mean_period_,
                                    exponential_decay_spec::span, 0.2);

        long_roller.pre();
        long_roller(idx, column);
        long_roller.post();

        const auto      &short_result = short_roller.get_result();
        const auto      &long_result = long_roller.get_result();
        const size_type col_s =
            std::min<size_type>(
                { idx.size(),
                  column.size(),
                  short_result.size(),
                  long_result.size()
                });

        macd_line_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            macd_line_.push_back(short_result[i] - long_result[i]);

        signal_line_roller_.pre();
        signal_line_roller_(idx, macd_line_);
        signal_line_roller_.post();

        const auto  &signal_line_result = signal_line_roller_.get_result();

        macd_histogram_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            macd_histogram_.push_back(macd_line_[i] - signal_line_result[i]);
    }

    inline void pre ()  {

        macd_line_.clear();
        macd_histogram_.clear();
    }
    inline void post ()  {  }

    const result_type &get_macd_line() const { return (macd_line_); }
    const result_type &
    get_signal_line() const { return (signal_line_roller_.get_result()); }
    const result_type &get_macd_histogram() const { return (macd_histogram_); }

private:

    result_type macd_line_;       // short-mean EMA - long-mean EMA
    result_type macd_histogram_;  // MACD Line - Signal Line
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
