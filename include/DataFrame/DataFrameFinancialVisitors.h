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
    StdVisitor<T, I>                            std_visitor_;

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
          std_visitor_(biased)  {   }

    template <typename K, typename H>
    inline void
    operator() (const K &idx, const H &column)  {

        const size_type col_s = std::min(idx.size(), column.size());

        mean_roller_.pre();
        mean_roller_(idx, column);
        mean_roller_.post();

        std_visitor_.pre();
        for (size_type i = 0; i < col_s; ++i)
            std_visitor_(idx[i], column[i]);
        std_visitor_.post();

        const value_type    std_result = std_visitor_.get_result();
        const value_type    upper = std_result * upper_band_multiplier_;
        const value_type    lower = std_result * lower_band_multiplier_;
        const auto          &mean_result = mean_roller_.get_result();

        upper_band_to_raw_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            upper_band_to_raw_.push_back((mean_result[i] + upper) - column[i]); 
        raw_to_lower_band_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            raw_to_lower_band_.push_back(column[i] - (mean_result[i] - lower)); 
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
