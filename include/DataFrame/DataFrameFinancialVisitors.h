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

#include <algorithm>
#include <functional>
#include <iterator>
#include <limits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct ReturnVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    explicit ReturnVisitor (return_policy rp) : ret_p_(rp)  {   }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &prices_begin,
                const H &prices_end)  {

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(prices_begin, prices_end);
        const size_type col_len = std::min(idx_size, col_size);

        if (col_len < 3)  return;

        // Log return
        std::function<value_type(value_type, value_type)>   func =
            [](value_type lhs, value_type rhs) -> value_type  {
                return (::log(lhs / rhs));
            };

        if (ret_p_ == return_policy::percentage)
            func = [](value_type lhs, value_type rhs) -> value_type  {
                      return ((lhs - rhs) / rhs);
                   };
        else if (ret_p_ == return_policy::monetary)
            func = [](value_type lhs, value_type rhs) -> value_type  {
                       return (lhs - rhs);
                   };

        result_type tmp_result;

        tmp_result.reserve(col_len);
        std::adjacent_difference (prices_begin, prices_end,
                                  std::back_inserter (tmp_result),
                                  func);
        tmp_result.erase (tmp_result.begin ());
        tmp_result.swap(result_);
    }

    inline void pre ()  { result_.clear(); }
    inline void post ()  {   }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

private:

    result_type         result_ {  };
    const return_policy ret_p_;
};

// ----------------------------------------------------------------------------

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
    inline std::size_t
    run_short_roller_(const K &idx_begin,
                      const K &idx_end,
                      const H &prices_begin,
                      const H &prices_end)  {

        short_roller_(idx_begin, idx_end, prices_begin, prices_end);

        const auto          &result = short_roller_.get_result();
        const size_type     idx_size = std::distance(idx_begin, idx_end);
        const size_type     col_size = std::distance(prices_begin, prices_end);
        const std::size_t   col_s =
            std::min<std::size_t>({ idx_size, col_size, result.size() });

        col_to_short_term_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            col_to_short_term_.push_back(*(prices_begin + i) - result[i]);
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
        const size_type     idx_size = std::distance(idx_begin, idx_end);
        const size_type     col_size = std::distance(prices_begin, prices_end);
        const std::size_t   col_s =
            std::min<std::size_t>({ idx_size, col_size, result.size() });

        col_to_long_term_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            col_to_long_term_.push_back(*(prices_begin + i) - result[i]);
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
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &prices_begin,
                const H &prices_end)  {

        const size_type thread_level =
            ThreadGranularity::get_sensible_thread_level();
        size_type       re_count1 = 0;
        size_type       re_count2 = 0;

        if (thread_level >= 2)  {
            std::future<size_type>  fut1 =
                std::async(std::launch::async,
                           &DoubleCrossOver::run_short_roller_<K, H>,
                           this,
                           std::cref(idx_begin),
                           std::cref(idx_end),
                           std::cref(prices_begin),
                           std::cref(prices_end));
            std::future<size_type>  fut2 =
                std::async(std::launch::async,
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
                run_short_roller_(idx_begin, idx_end, prices_begin, prices_end);
            re_count2 =
                run_long_roller_(idx_begin, idx_end, prices_begin, prices_end);
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
    inline void
    run_mean_roller_(const K &idx_begin,
                     const K &idx_end,
                     const H &prices_begin,
                     const H &prices_end)  {

        mean_roller_.pre();
        mean_roller_(idx_begin, idx_end, prices_begin, prices_end);
        mean_roller_.post();
    }

    template <typename K, typename H>
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
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &prices_begin,
                const H &prices_end)  {

        const size_type thread_level =
            ThreadGranularity::get_sensible_thread_level();

        if (thread_level >= 2)  {
            std::future<void>   fut1 =
                std::async(std::launch::async,
                           &BollingerBand::run_mean_roller_<K, H>,
                           this,
                           std::cref(idx_begin),
                           std::cref(idx_end),
                           std::cref(prices_begin),
                           std::cref(prices_end));
            std::future<void>   fut2 =
                std::async(std::launch::async,
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

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(prices_begin, prices_end);
        const auto      &std_result = std_roller_.get_result();
        const auto      &mean_result = mean_roller_.get_result();
        const size_type col_s =
            std::min<size_type>(
                { idx_size, col_size, std_result.size(), mean_result.size() });

        upper_band_to_raw_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            upper_band_to_raw_.push_back(
                (mean_result[i] + std_result[i] * upper_band_multiplier_) -
                *(prices_begin + i));

        raw_to_lower_band_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            raw_to_lower_band_.push_back(
                *(prices_begin + i) -
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

    inline MACDVisitor(
        size_type short_mean_period,  // e.g. 12-day
        size_type long_mean_period,   // e.g. 26-day
        size_type signal_line_period, // e.g.  9-day
        exponential_decay_spec ed_spec = exponential_decay_spec::span,
        double expo_decay_value = 0.2)
        : short_mean_period_(short_mean_period),
          long_mean_period_(long_mean_period),
          signal_line_roller_(std::move(MeanVisitor<T, I>()),
                              signal_line_period,
                              ed_spec, expo_decay_value)  {
    }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        macd_roller_t   short_roller(std::move(MeanVisitor<T, I>()),
                                     short_mean_period_,
                                     exponential_decay_spec::span, 0.2);

        short_roller.pre();
        short_roller(idx_begin, idx_end, column_begin, column_end);
        short_roller.post();

        macd_roller_t   long_roller(std::move(MeanVisitor<T, I>()),
                                    long_mean_period_,
                                    exponential_decay_spec::span, 0.2);

        long_roller.pre();
        long_roller(idx_begin, idx_end, column_begin, column_end);
        long_roller.post();

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);
        const auto      &short_result = short_roller.get_result();
        const auto      &long_result = long_roller.get_result();
        const size_type col_s =
            std::min<size_type>(
                {idx_size, col_size, short_result.size(), long_result.size()});

        macd_line_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            macd_line_.push_back(short_result[i] - long_result[i]);

        signal_line_roller_.pre();
        signal_line_roller_(idx_begin, idx_end,
                            macd_line_.begin(), macd_line_.end());
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

// ----------------------------------------------------------------------------

// Volume Weighted Average Price
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct  VWAPVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;

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

    using result_type = std::vector<VWAP>;
    using distance_func =
        std::function<double(const index_type &, const index_type &)>;

    // Index value is assumed to represent time
    // The first value is assumed to be the price of a traded instrument.
    // The second value is assimed to be the size of a traded instrument.
    // The provided "dfunc" measures time elapsed between two index values
    inline void operator() (const index_type &idx,
                            const value_type &price,
                            const value_type &size)  {

        // If reached the limit, stop
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
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end,
                H price_begin, H price_end,
                H size_begin, H size_end)  {

        while (price_begin < price_end && size_begin < size_end)
            (*this)(*idx_begin, *price_begin++, *size_begin++);
    }

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
                    { std::numeric_limits<value_type>::quiet_NaN(),
                      std::numeric_limits<value_type>::quiet_NaN(),
                      last_time_,
                      event_count_,
                    });
            }
        }
    }
    inline const result_type &get_result () const  { return (result_); }

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

// ----------------------------------------------------------------------------

// Volume Weighted Bid-Ask Spread
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct  VWBASVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;

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

    using result_type = std::vector<VWBAS>;
    using distance_func =
        std::function<double(const index_type &, const index_type &)>;

    // Index value is assumed to represent time
    // The first value is assumed to be the bid price of a traded instrument.
    // The second value is assumed to be the ask price of a traded instrument.
    // The third value is assimed to be the bid size of a traded instrument.
    // The fourth value is assimed to be the ask size of a traded instrument.
    // The provided "dfunc" measures time elapsed between two index values
    inline void operator() (const index_type &idx,
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
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end,
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
                const value_type    vwa =
                    vw_ask_price_accumulator_ / ask_volume_accumulator_;
                const value_type    vwb =
                    vw_bid_price_accumulator_ / bid_volume_accumulator_;
                const value_type    vwbas = vwa - vwb;
                const value_type    per_vwbas = (vwbas / vwb) * 100.0;
                const value_type    spread =
                    (ask_price_accumulator_ / event_count_) -
                    (bid_price_accumulator_ / event_count_);
                const value_type    per_spread =
                    (spread / (bid_price_accumulator_ / event_count_)) * 100.0;

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
                    { std::numeric_limits<value_type>::quiet_NaN(),
                      std::numeric_limits<value_type>::quiet_NaN(),
                      std::numeric_limits<value_type>::quiet_NaN(),
                      std::numeric_limits<value_type>::quiet_NaN(),
                      last_time_,
                      event_count_,
                    });
            }
        }
    }
    inline const result_type &get_result () const  { return (result_); }

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

// ----------------------------------------------------------------------------

// This is meaningfull, only if the return series is close to normal
// distribution
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SharpeRatioVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    explicit
    SharpeRatioVisitor(bool biased = false) : biased_ (biased) {  }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &asset_ret_begin,
                const H &asset_ret_end,
                const H &benchmark_ret_begin,
                const H &benchmark_ret_end)  {

        const size_type vec_s = std::distance(asset_ret_begin, asset_ret_end);
        const size_type b_s =
            std::distance(benchmark_ret_begin, benchmark_ret_end);

        if (vec_s != b_s || vec_s < 3)  {
            char    err[512];

            sprintf (err,
#ifdef _WIN32
                     "SharpeRatioVisitor: Size of asset = %zu and "
                     "benchmark = %zu time-series are not feasible.",
#else
                     "SharpeRatioVisitor: Size of asset = %lu and "
                     "benchmark = %lu time-series are not feasible.",
#endif // _WIN32
                     vec_s, b_s);
            throw NotFeasible (err);
        }

        value_type                          cum_return { 0.0 };
        StdVisitor<value_type, index_type>  std_vis(biased_);
        auto                                a_citer = asset_ret_begin;
        const index_type                    &index_val = *idx_begin; // Ignored

        std_vis.pre();
        for (auto b_citer = benchmark_ret_begin;
             b_citer != benchmark_ret_end; ++a_citer, ++b_citer)  {
            std_vis (index_val, *a_citer - *b_citer);
            cum_return += *a_citer - *b_citer;
        }
        std_vis.post();
        result_ = (cum_return / value_type(vec_s)) / std_vis.get_result();
    }

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

private:

    const bool  biased_;
    result_type result_ { 0 };
};

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
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct RSIVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &prices_begin,
                const H &prices_end)  {

        const size_type col_s = std::distance(prices_begin, prices_end);

        // This data doesn't make sense
        if (avg_period_ >= col_s - 3)  return;

        ReturnVisitor<value_type>   return_v (rp_);

        return_v.pre();
        return_v (idx_begin, idx_end, prices_begin, prices_end);
        return_v.post();

        value_type          avg_up = 0;
        value_type          avg_down = 0;
        const value_type    avg_period_1 = avg_period_ - value_type(1);

        for (size_type i = 0; i < avg_period_; ++i)  {
            const value_type    value = return_v.get_result()[i];

            if (value > 0)
                avg_up = (avg_up * avg_period_1 + value) / avg_period_;
            else if (value < 0)
                avg_down = (avg_down * avg_period_1 - value) / avg_period_;
        }

        result_.reserve(col_s - static_cast<size_type>(avg_period_));

        constexpr value_type    hundred = value_type(100);
        constexpr value_type    one = value_type(1);

        result_.push_back(hundred - (hundred / (one + avg_up / avg_down)));

        const size_type ret_s = return_v.get_result().size();

        for (size_type i = size_type(avg_period_); i < ret_s; ++i)  {
            const value_type    value = return_v.get_result()[i];

            if (value > 0)
                avg_up = (avg_up * avg_period_1 + value) / avg_period_;
            else if (value < 0)
                avg_down = (avg_down * avg_period_1 - value) / avg_period_;
            result_.push_back(hundred - (hundred / (one + avg_up / avg_down)));
        }
    }

    explicit RSIVisitor(return_policy rp, size_type avg_period = 14)
        : rp_(rp), avg_period_(value_type(avg_period))  {   }

    inline void pre ()  { result_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

private:

    const return_policy rp_;
    const value_type    avg_period_;
    result_type         result_ { };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
