// Hossein Moein
// January 08, 2020
// Copyright (C) 2020-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

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

        short_roller_(idx, column);
        long_roller_(idx, column);

        const auto  &result1 = short_roller_.get_result();
        size_type   col_s =
            std::min<size_type>({ idx.size(), column.size(), result1.size() });

        col_to_short_term_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            col_to_short_term_.push_back(column[i] - result1[i]);

        const auto  &result2 = long_roller_.get_result();

        col_s =
            std::min<size_type>({ idx.size(), column.size(), result2.size() });
        col_to_long_term_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            col_to_long_term_.push_back(column[i] - result2[i]);

        col_s =
            std::min<size_type>({ idx.size(), result1.size(), result2.size() });
        short_term_to_long_term_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            short_term_to_long_term_.push_back(result1[i] - result2[i]);
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

        mean_roller_.pre();
        mean_roller_(idx, column);
        mean_roller_.post();

        std_roller_.pre();
        std_roller_(idx, column);
        std_roller_.post();

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
                       size_type signal_line_period) // e.g.  9-day
        : short_mean_period_(short_mean_period),
          long_mean_period_(long_mean_period),
          signal_line_roller_(std::move(MeanVisitor<T, I>()),
                              signal_line_period)  {
    }

    template <typename K, typename H>
    inline void
    operator() (const K &idx, const H &column)  {

        macd_roller_t   short_roller(std::move(MeanVisitor<T, I>()),
                                     short_mean_period_);

        short_roller.pre();
        short_roller(idx, column);
        short_roller.post();

        macd_roller_t   long_roller(std::move(MeanVisitor<T, I>()),
                                    long_mean_period_);

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
