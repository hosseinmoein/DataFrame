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
#include <numeric>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct ReturnVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        if (col_s < 3)  return;

        // Log return
        std::function<value_type(value_type, value_type)>   func =
            [](value_type lhs, value_type rhs) -> value_type  {
                return (std::log(lhs / rhs));
            };

        if (ret_p_ == return_policy::percentage)
            func = [](value_type lhs, value_type rhs) -> value_type  {
                      return ((lhs - rhs) / rhs);
                   };
        else if (ret_p_ == return_policy::monetary)
            func = [](value_type lhs, value_type rhs) -> value_type  {
                       return (lhs - rhs);
                   };
        else if (ret_p_ == return_policy::trinary)
            func = [](value_type lhs, value_type rhs) -> value_type  {
                       const value_type diff = lhs - rhs;

                       return ((diff > 0) ? 1 : ((diff < 0) ? -1 : 0));
                   };

        result_type tmp_result;

        tmp_result.reserve(col_s);
        std::adjacent_difference (column_begin, column_end,
                                  std::back_inserter (tmp_result),
                                  func);
        tmp_result[0] = std::numeric_limits<T>::quiet_NaN();
        tmp_result.swap(result_);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit ReturnVisitor (return_policy rp) : ret_p_(rp)  {   }

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
struct  DoubleCrossOver {

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

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &prices_begin, const H &prices_end)  {

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
    result_type &get_raw_to_short_term()  { return (col_to_short_term_); }
    const result_type &
    get_raw_to_long_term() const  { return (col_to_long_term_); }
    result_type &get_raw_to_long_term()  { return (col_to_long_term_); }
    const result_type &
    get_short_term_to_long_term() const  { return (short_term_to_long_term_); }
    result_type &
    get_short_term_to_long_term()  { return (short_term_to_long_term_); }

    inline DoubleCrossOver(S_RT &&sr, L_RT &&lr)
        : short_roller_(std::move(sr)), long_roller_(std::move(lr))  {   }

private:

    result_type col_to_short_term_;
    result_type col_to_long_term_;
    result_type short_term_to_long_term_;
};

template<typename S_RT, typename L_RT, typename T, typename I = unsigned long>
using dco_v = DoubleCrossOver<S_RT, L_RT, T, I>;

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

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &prices_begin, const H &prices_end)  {

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

// ----------------------------------------------------------------------------

// Moving Average Convergence/Divergence
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MACDVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        macd_roller_t   short_roller(exponential_decay_spec::span,
                                     short_mean_period_);

        short_roller.pre();
        short_roller(idx_begin, idx_end, column_begin, column_end);
        short_roller.post();

        macd_roller_t   long_roller(exponential_decay_spec::span,
                                    long_mean_period_);

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
                              signal_line_period)  {
    }

private:


    using macd_roller_t = ewm_v<T, I>;

    const size_type short_mean_period_;
    const size_type long_mean_period_;
    macd_roller_t   signal_line_roller_;
    result_type     macd_line_ { };       // short-mean EMA - long-mean EMA
    result_type     macd_histogram_ { };  // MACD Line - Signal Line
};

template<typename T, typename I = unsigned long>
using macd_v = MACDVisitor<T, I>;

// ----------------------------------------------------------------------------

// Volume Weighted Average Price
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct  VWAPVisitor {

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

    using result_type = std::vector<VWAP>;
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

template<typename T, typename I = unsigned long>
using vwap_v = VWAPVisitor<T, I>;

// ----------------------------------------------------------------------------

// Volume Weighted Bid-Ask Spread
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct  VWBASVisitor {

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

    using result_type = std::vector<VWBAS>;
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

template<typename T, typename I = unsigned long>
using vwbas_v = VWBASVisitor<T, I>;

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

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &asset_ret_begin, const H &asset_ret_end,
                const H &benchmark_ret_begin, const H &benchmark_ret_end)  {

        const size_type vec_s = std::distance(asset_ret_begin, asset_ret_end);
        const size_type b_s =
            std::distance(benchmark_ret_begin, benchmark_ret_end);

        if (vec_s != b_s || vec_s < 3)  {
            char    err[512];

            sprintf (err,
#ifdef _MSC_VER
                     "SharpeRatioVisitor: Size of asset = %zu and "
                     "benchmark = %zu time-series are not feasible.",
#else
                     "SharpeRatioVisitor: Size of asset = %lu and "
                     "benchmark = %lu time-series are not feasible.",
#endif // _MSC_VER
                     vec_s, b_s);
            throw NotFeasible (err);
        }

        value_type          cum_return { 0.0 };
        StdVisitor<T, I>    std_vis(biased_);
        auto                a_citer = asset_ret_begin;
        const index_type    &index_val = *idx_begin; // Ignored

        std_vis.pre();
        for (auto b_citer = benchmark_ret_begin;
             b_citer != benchmark_ret_end; ++a_citer, ++b_citer)  {
            std_vis (index_val, *a_citer - *b_citer);
            cum_return += *a_citer - *b_citer;
        }
        std_vis.post();
        result_ = (cum_return / T(vec_s)) / std_vis.get_result();
    }

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    explicit
    SharpeRatioVisitor(bool biased = false) : biased_ (biased) {  }

private:

    const bool  biased_;
    result_type result_ { 0 };
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
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct RSIVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &prices_begin, const H &prices_end)  {

        const size_type col_s = std::distance(prices_begin, prices_end);

        // This data doesn't make sense
        //
        if (avg_period_ >= T(col_s - 3))  return;

        ReturnVisitor<T, I> return_v (rp_);

        return_v.pre();
        return_v (idx_begin, idx_end, prices_begin, prices_end);
        return_v.post();

        static constexpr value_type one { 1 };
        value_type                  avg_up = 0;
        value_type                  avg_down = 0;
        const value_type            avg_period_1 = avg_period_ - one;

        for (size_type i = 1; i < avg_period_; ++i)  {
            const value_type    value = return_v.get_result()[i];

            if (value > 0)
                avg_up = (avg_up * avg_period_1 + value) / avg_period_;
            else if (value < 0)
                avg_down = (avg_down * avg_period_1 - value) / avg_period_;
        }

        result_type result;

        result.reserve(col_s - static_cast<size_type>(avg_period_));

        static constexpr value_type h { 100 };

        result.push_back(h - (h / (one + avg_up / avg_down)));

        const size_type ret_s = return_v.get_result().size();

        for (size_type i = size_type(avg_period_); i < ret_s; ++i)  {
            const value_type    value = return_v.get_result()[i];

            if (value > 0)
                avg_up = (avg_up * avg_period_1 + value) / avg_period_;
            else if (value < 0)
                avg_down = (avg_down * avg_period_1 - value) / avg_period_;
            result.push_back(h - (h / (one + avg_up / avg_down)));
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

template<typename T, typename I = unsigned long>
using rsi_v = RSIVisitor<T, I>;

// ----------------------------------------------------------------------------

// RSX is a "noise free" version of RSI, with no added lag.
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct RSXVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        assert(avg_period_ < col_s);

        value_type  vc { 0 }, v1c { 0 };
        value_type  v4 { 0 }, v8 { 0 }, v10 { 0 }, v14 { 0 }, v18 { 0 },
                    v20 { 0 };
        value_type  f0 { 0 }, f8 { 0 }, f10 { 0 }, f18 { 0 }, f20 { 0 },
                    f28 { 0 }, f30 { 0 }, f38 { 0 },  f40 { 0 }, f48 { 0 },
                    f50 { 0 }, f58 { 0 }, f60 { 0 }, f68 { 0 }, f70 { 0 },
                    f78 { 0 }, f80 { 0 }, f88 { 0 }, f90 { 0 };

        constexpr value_type    zero { 0 };
        constexpr value_type    epsilon { 0.0000000001 };
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
        for (size_type i = avg_period_; i < col_s; ++i)  {
            if (f90 == zero)  {
                f90 = one;
                f0 = zero;
                if (avg_period_ - 1 >= 5)
                    f88 = T(avg_period_) - one;
                else
                    f88 = five;
                f8 = hundred * *(column_begin + i);
                f18 = three / (T(avg_period_) + two);
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

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit RSXVisitor(size_type avg_period = 14)
        : avg_period_(T(avg_period))  {   }

private:

    const value_type    avg_period_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long>
using rsx_v = RSXVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct HurstExponentVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    using RangeVec = std::vector<size_type>;

private:

    struct  range_data  {

        size_type   id { 0 };
        size_type   begin { 0 };
        size_type   end { 0 };
        value_type  mean { 0 };
        value_type  st_dev { 0 };
        value_type  rescaled_range { 0 };
    };

public:

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE
        std::vector<range_data> buckets;
        MeanVisitor<T, I>       mv;
        StdVisitor<T, I>        sv;

        // Calculate each range basic stats
        //
        buckets.reserve(std::accumulate(ranges_.begin(), ranges_.end(), 0));
        for (auto range : ranges_)  {
            const size_type ch_size = col_s / range;

            for (size_type i = 0; i < range; ++i)  {
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
        for (auto &iter : buckets)  {
            value_type  total { 0 };  // Cumulative sum (CumSum)
            value_type  max_dev { std::numeric_limits<T>::min() };
            value_type  min_dev { std::numeric_limits<T>::max() };

            for (size_type i = iter.begin; i < iter.end; ++i)  {
                total += *(column_begin + i) - iter.mean;
                if (total > max_dev)  max_dev = total;
                if (total < min_dev)  min_dev = total;
            }
            iter.rescaled_range = (max_dev - min_dev) / iter.st_dev;
        }

        // Caluculate Hurst exponent
        //
        size_type               prev_id { 0 };
        size_type               prev_size { 0 };
        value_type              count { 0 };
        value_type              total_rescaled_range  { 0 };
        std::vector<value_type> log_rescaled_mean;
        std::vector<value_type> log_size;

        log_rescaled_mean.reserve(ranges_.size());
        log_size.reserve(ranges_.size());
        for (auto citer : buckets)  {
            if (citer.id != prev_id && count > 0)  {
                log_size.push_back(std::log(prev_size));
                log_rescaled_mean.push_back(
                    std::log(total_rescaled_range / count));
                total_rescaled_range = 0;
                count = 0;
            }
            total_rescaled_range += citer.rescaled_range;
            count += 1;
            prev_size = citer.end - citer.begin;
            prev_id = citer.id;
        }
        if (count > 0)  {
            log_size.push_back(std::log(prev_size));
            log_rescaled_mean.push_back(
                std::log(total_rescaled_range / count));
        }

        PolyFitVisitor<T, I>    pfv (1);  // First degree

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
    HurstExponentVisitor(RangeVec &&ranges) : ranges_ (std::move(ranges)) {  }

private:

    const RangeVec  ranges_;
    result_type     exponent_ { -1 };
};

template<typename T, typename I = unsigned long>
using hexpo_v = HurstExponentVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MassIndexVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &high_begin, const H &high_end,
                const H &low_begin, const H &low_end)  {

        const size_type col_s = std::distance(high_begin, high_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert(fast_ < slow_);

        bool        there_is_zero = false;
        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    v = *(high_begin + i) - *(low_begin + i);

            result.push_back(v);
            if (v == 0)  there_is_zero = true;
        }
        if (there_is_zero)
            std::for_each(result.begin(), result.end(),
                          [](value_type &v)  {
                              v += std::numeric_limits<value_type>::epsilon();
                          });

        erm_t   fast_roller(exponential_decay_spec::span, fast_);

        fast_roller.pre();
        fast_roller(idx_begin, idx_end, result.begin(), result.end());
        fast_roller.post();

        // Backfill the result with simple averges
        //
        value_type  sum = 0;

        for (size_type i = 0; i < col_s; ++i)  {
            if (is_nan__(fast_roller.get_result()[i]))  {
                sum += result[i];
                fast_roller.get_result()[i] = sum / T(i + 1);
            }
            else  break;
        }
        result = std::move(fast_roller.get_result());
        fast_roller.pre();
        fast_roller(idx_begin, idx_end, result.begin(), result.end());
        fast_roller.post();

        // Backfill the result with simple averges
        //
        sum = 0;
        for (size_type i = 0; i < col_s; ++i)  {
            if (is_nan__(fast_roller.get_result()[i]))  {
                sum += result[i];
                fast_roller.get_result()[i] = sum / T(i + 1);
            }
            else  break;
        }

        for (size_type i = 0; i < col_s; ++i)
            result[i] = result[i] / fast_roller.get_result()[i];

        srs_t   slow_roller(std::move(SumVisitor<T, I>()), slow_);

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

    using erm_t = ewm_v<T, I>;
    using srs_t = SimpleRollAdopter<SumVisitor<T, I>, T, I>;

    result_type     result_ {  };
    const size_type slow_;
    const size_type fast_;
};

template<typename T, typename I = unsigned long>
using mass_idx_v = MassIndexVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  HullRollingMeanVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ == 0)  return;

        using wma_t = SimpleRollAdopter<WeightedMeanVisitor<T, I>, T, I>;

        GET_COL_SIZE

        wma_t   wma_half (WeightedMeanVisitor<T, I>(), roll_count_ / 2);

        wma_half.pre();
        wma_half (idx_begin, idx_end, column_begin, column_end);
        wma_half.post();

        wma_t   wma_full (WeightedMeanVisitor<T, I>(), roll_count_);

        wma_full.pre();
        wma_full (idx_begin, idx_end, column_begin, column_end);
        wma_full.post();

        static constexpr value_type two { 2 };
        result_type      result = std::move(wma_half.get_result());

        for (size_type i = 0; i < col_s - 1 && i < col_s; ++i)
            result[i] = two * result[i] - wma_full.get_result()[i];

        wma_t   wma_sqrt (WeightedMeanVisitor<T, I>(),
                          size_type(std::sqrt(roll_count_)));

        wma_sqrt.pre();
        wma_sqrt (idx_begin, idx_end, result.begin(), result.end());
        wma_sqrt.post();

        result = std::move(wma_sqrt.get_result());
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    HullRollingMeanVisitor(size_type r_count = 10) : roll_count_(r_count) {   }

private:

    const size_type roll_count_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using hull_mean_v = HullRollingMeanVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  RollingMidValueVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end)  {

        if (roll_count_ == 0)  return;

        const size_type col_s = std::distance(high_begin, high_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));

        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < roll_count_ - 1 && i < col_s; ++i)
            result.push_back(std::numeric_limits<T>::quiet_NaN());

        value_type                  min_v = *low_begin;
        value_type                  max_v = *high_begin;
        static constexpr value_type p5 { 0.5 };

        for (size_type i = 0; i < col_s; ++i)  {
            const size_type limit = i + roll_count_;

            if (limit <= col_s)  {
                for (size_type j = i; j < limit; ++j)  {
                    if (*(low_begin + j) < min_v)
                        min_v = *(low_begin + j);
                    if (*(high_begin + j) > max_v)
                        max_v = *(high_begin + j);
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
    RollingMidValueVisitor(size_type r_count) : roll_count_(r_count) {   }

private:

    const size_t    roll_count_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using mid_val_v = RollingMidValueVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  DrawdownVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        CumMaxVisitor<T, I> cm_v;

        cm_v.pre();
        cm_v (idx_begin, idx_end, column_begin, column_end);
        cm_v.post();

        const auto                  &cm_result = cm_v.get_result();
        static constexpr value_type one { 1 };

        drawdown_.reserve(col_s);
        pct_drawdown_.reserve(col_s);
        log_drawdown_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            drawdown_.push_back(cm_result[i] - *(column_begin + i));
            pct_drawdown_.push_back(one - *(column_begin + i) / cm_result[i]);
            log_drawdown_.push_back(
                std::log(cm_result[i] / *(column_begin + i)));
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
template<typename T, typename I = unsigned long>
struct  WilliamPrcRVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        if (roll_count_ == 0)  return;

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));

        SimpleRollAdopter<MinVisitor<T, I>, T, I>   min_v (MinVisitor<T, I>(),
                                                           roll_count_);

        min_v.pre();
        min_v (idx_begin, idx_end, low_begin, low_end);
        min_v.post();

        SimpleRollAdopter<MaxVisitor<T, I>, T, I>   max_v (MaxVisitor<T, I>(),
                                                           roll_count_);

        max_v.pre();
        max_v (idx_begin, idx_end, high_begin, high_end);
        max_v.post();

        result_type result = std::move(max_v.get_result());

        static constexpr value_type h { 100 };
        static constexpr value_type one { 1 };

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    low = min_v.get_result()[i];

            result[i] =
                h * ((*(close_begin + i) - low) / (result[i] - low) - one);
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    WilliamPrcRVisitor(size_type r_count = 14) : roll_count_(r_count) {   }

private:

    const size_type roll_count_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using willp_v = WilliamPrcRVisitor<T, I>;

// ----------------------------------------------------------------------------

// Psychological Line (PSL) is an oscillator-type indicator
//
template<typename T, typename I = unsigned long>
struct  PSLVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator()(const K &idx_begin, const K &idx_end,
               const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        result_.reserve(col_s);
        result_.push_back(0);
        for (size_type i = 1; i < col_s; ++i)
            result_.push_back(
                (*(close_begin + i) - *(close_begin + (i - 1)) > 0) ? 1 : 0);
        calculate_(idx_begin, idx_end);
    }

    template <typename K, typename H>
    inline void
    operator()(const K &idx_begin, const K &idx_end,
               const H &close_begin, const H &close_end,
               const H &open_begin, const H &open_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(open_begin, open_end))));

        result_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            result_.push_back(
                (*(close_begin + i) - *(open_begin + i) > 0) ? 1 : 0);
        calculate_(idx_begin, idx_end);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    PSLVisitor(size_type r_count = 14) : roll_count_(r_count) {   }

private:

    template <typename K>
    inline void calculate_(const K &idx_begin, const K &idx_end)  {

        SimpleRollAdopter<SumVisitor<T, I>, T, I>   sum_r (SumVisitor<T, I>(),
                                                           roll_count_);

        sum_r.pre();
        sum_r(idx_begin, idx_end, result_.begin(), result_.end());
        sum_r.post();

        const size_type col_s = result_.size();

        for (size_type i = 0; i < col_s; ++i)
            result_[i] = sum_r.get_result()[i] * T(100) / T(roll_count_);
    }

    const size_type roll_count_;
    result_type     result_ { };
};

// ----------------------------------------------------------------------------

// Commodity Channel Index (CCI)
//
template<typename T, typename I = unsigned long>
struct  CCIVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        if (roll_count_ == 0)  return;

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));

        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            result.push_back(
                (*(low_begin + i) + *(high_begin + i) + *(close_begin + i)) /
                T(3));

        SimpleRollAdopter<MeanVisitor<T, I>, T, I>  avg_v (
            MeanVisitor<T, I>(), roll_count_);

        avg_v.pre();
        avg_v (idx_begin, idx_end, result.begin(), result.end());
        avg_v.post();

        SimpleRollAdopter<MADVisitor<T, I>, T, I>   mad_v (
            MADVisitor<T, I>(mad_type::mean_abs_dev_around_mean), roll_count_);

        mad_v.pre();
        mad_v (idx_begin, idx_end, result.begin(), result.end());
        mad_v.post();

        for (size_type i = 0; i < col_s; ++i)
            result[i] =
                (result[i] - avg_v.get_result()[i]) /
                (lambert_const_ * mad_v.get_result()[i]);

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

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct GarmanKlassVolVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(open_begin, open_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));
        assert((roll_count_ < (col_s - 1)));

        // 2 * log(2) - 1
        constexpr value_type    cf = T(2) * T(0.69314718056) - T(1);
        constexpr value_type    hlf = 0.5;
        result_type             result;

        result.reserve(col_s);
        for (size_type i = 0; i < roll_count_ - 1; ++i)
            result.push_back(std::numeric_limits<T>::quiet_NaN());
        for (size_type i = 0; i < col_s; ++i)  {
            if (i + roll_count_ <= col_s)  {
                value_type  sum { 0 };
                size_type   cnt { 0 };

                for (size_type j = i; j < (i + roll_count_); ++j)  {
                    const value_type    hl_rt =
                        std::log(*(high_begin + j) / *(low_begin + j));
                    const value_type    co_rt =
                        std::log(*(close_begin + j) / *(open_begin + j));

                    sum += hlf * hl_rt * hl_rt - cf * co_rt * co_rt;
                    cnt += 1;
                }
                result.push_back(std::sqrt((sum / T(cnt)) * trading_periods_));
            }
            else  break;
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

template<typename T, typename I = unsigned long>
using gk_vol_v = GarmanKlassVolVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct YangZhangVolVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert(roll_count_ > 1);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(open_begin, open_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));
        assert((roll_count_ < (col_s - 1)));

        const value_type    k =
            T(0.34) / (T(1) + T(roll_count_ + 1) / T(roll_count_ - 1));
        const value_type    one_k = T(1) - k;
        const value_type    norm = T(1) / T(roll_count_ - 1);
        result_type         result;

        result.reserve(col_s);
        for (size_type i = 0; i < roll_count_ - 1; ++i)
            result.push_back(std::numeric_limits<T>::quiet_NaN());

        for (size_type i = 0; i < col_s; ++i)  {
            if (i + roll_count_ <= col_s)  {
                value_type  c_vol_sum { 0 };
                value_type  o_vol_sum { 0 };
                value_type  rs_vol_sum { 0 };

                for (size_type j = i; j < (i + roll_count_); ++j)  {
                    const value_type    open = *(open_begin + j);
                    const value_type    close = *(close_begin + j);
                    const value_type    ho_rt =
                        std::log(*(high_begin + j) / open);
                    const value_type    lo_rt =
                        std::log(*(low_begin + j) / open);
                    const value_type    co_rt = std::log(close/ open);
                    const value_type    oc_rt = j > 0
                        ? std::log(open / *(close_begin + (j - 1)))
                        : std::numeric_limits<T>::quiet_NaN();
                    const value_type    cc_rt = j > 0
                        ? std::log(close / *(close_begin + (j - 1)))
                        : std::numeric_limits<T>::quiet_NaN();
                    // Rogers-Satchell volatility
                    const value_type    rs_vol =
                        ho_rt * (ho_rt - co_rt) + lo_rt * (lo_rt - co_rt);

                    c_vol_sum += cc_rt * cc_rt * norm;
                    o_vol_sum += oc_rt * oc_rt * norm;
                    rs_vol_sum += rs_vol * norm;
                }
                result.push_back(
                    std::sqrt(o_vol_sum + k * c_vol_sum + one_k * rs_vol_sum) *
                    std::sqrt(trading_periods_));
            }
            else  break;
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

template<typename T, typename I = unsigned long>
using yz_vol_v = YangZhangVolVisitor<T, I>;

// ----------------------------------------------------------------------------

// Kaufman's Adaptive Moving Average
//
template<typename T, typename I = unsigned long>
struct  KamaVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ < 2)  return;

        GET_COL_SIZE

        result_type change_diff (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = roll_count_; i < col_s; ++i)
            change_diff[i] =
                std::fabs(*(column_begin + (i - roll_count_)) -
                          *(column_begin + i));

        result_type peer_diff (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = 1; i < col_s; ++i)
            peer_diff[i] =
                std::fabs(*(column_begin + (i - 1)) - *(column_begin + i));

        SimpleRollAdopter<SumVisitor<T, I>, T, I>   vol (
            SumVisitor<T, I>(), roll_count_);

        vol.pre();
        vol(idx_begin, idx_end, peer_diff.begin(), peer_diff.end());
        vol.post();

        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < roll_count_ - 1; ++i)
            result.push_back(std::numeric_limits<T>::quiet_NaN());
        result.push_back(0);
        for (size_type i = roll_count_; i < col_s; ++i)  {
            const value_type    exp_ratio =
                change_diff[i] / vol.get_result()[i];
            value_type          smoothing_const =
                exp_ratio * (fast_sc_ - slow_sc_) + slow_sc_;

            smoothing_const *= smoothing_const;
            result.push_back(smoothing_const * *(column_begin + i) +
                             (T(1) - smoothing_const) * result[i - 1]);
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    KamaVisitor(size_type roll_count = 10,
                size_type fast_smoothing_const = 2,
                size_type slow_smoothing_const = 30)
        : roll_count_(roll_count),
          fast_sc_(T(2) / T(fast_smoothing_const + 1)),
          slow_sc_(T(2) / T(slow_smoothing_const + 1)) {   }

private:

    const size_type     roll_count_;
    const value_type    fast_sc_;
    const value_type    slow_sc_;
    result_type         result_ { };
};

// ----------------------------------------------------------------------------

// Fisher Transform Indicator
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct FisherTransVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end)  {

        const size_type col_s = std::distance(low_begin, low_end);

        assert((col_s == size_type(std::distance(high_begin, high_end))));
        assert((roll_count_ < (col_s - 1)));

        std::vector<value_type> mid_hl;

        mid_hl.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            mid_hl.push_back((*(low_begin + i) + *(high_begin + i)) * T(0.5));

        SimpleRollAdopter<MaxVisitor<T, I>, T, I>   max_v (MaxVisitor<T, I>(),
                                                           roll_count_);
        SimpleRollAdopter<MinVisitor<T, I>, T, I>   min_v (MinVisitor<T, I>(),
                                                           roll_count_);

        max_v.pre();
        max_v (idx_begin, idx_end, mid_hl.begin(), mid_hl.end());
        max_v.post();
        min_v.pre();
        min_v (idx_begin, idx_end, mid_hl.begin(), mid_hl.end());
        min_v.post();

        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    v =
                max_v.get_result()[i] - min_v.get_result()[i];

            result.push_back(((mid_hl[i] - min_v.get_result()[i]) /
                              (v >= T(0.001) ? v : T(0.001))) - T(0.5));
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

template<typename T, typename I = unsigned long>
using ftrans_v = FisherTransVisitor<T, I>;

// ----------------------------------------------------------------------------

// Percentage Price Oscillator (PPO)
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct PercentPriceOSCIVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert(fast_ < slow_);

        srs_t   fast_roller(std::move(MeanVisitor<T, I>()), fast_);

        fast_roller.pre();
        fast_roller(idx_begin, idx_end, close_begin, close_end);
        fast_roller.post();

        srs_t   slow_roller (std::move(MeanVisitor<T, I>()), slow_);

        slow_roller.pre();
        slow_roller(idx_begin, idx_end, close_begin, close_end);
        slow_roller.post();

        result_type result = std::move(slow_roller.get_result());

        for (size_type i = 0; i < col_s; ++i)
            result[i] =
                (T(100) * (fast_roller.get_result()[i] - result[i])) /
                result[i];

        erm_t   signal_roller (exponential_decay_spec::span, signal_);

        signal_roller.pre();
        signal_roller(idx_begin + slow_, idx_end,
                      result.begin() + slow_, result.end());
        signal_roller.post();

        histogram_.reserve(col_s);
        for (size_type i = 0; i < slow_; ++i)
            histogram_.push_back(std::numeric_limits<T>::quiet_NaN());

        const size_type new_col_s =
            std::min(col_s, signal_roller.get_result().size());

        for (size_type i = slow_; i < new_col_s; ++i)
            histogram_.push_back(result[i] - signal_roller.get_result()[i]);

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

    using erm_t = ewm_v<T, I>;
    using srs_t = SimpleRollAdopter<MeanVisitor<T, I>, T, I>;

    result_type     result_ {  };
    result_type     histogram_ {  };
    const size_type slow_;
    const size_type fast_;
    const size_type signal_;
};

template<typename T, typename I = unsigned long>
using pp_osc_v = PercentPriceOSCIVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  SlopeVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (periods_ < 2)  return;

        assert(! ((! as_angle_) && in_degrees_));

        GET_COL_SIZE

        DiffVisitor<T, I>   diff (periods_, false);

        diff.pre();
        diff (idx_begin, idx_end, column_begin, column_end);
        diff.post();

        result_type             result = std::move(diff.get_result());
        constexpr value_type    pi_180 = T(180) / T(M_PI);

        for (size_type i = 0; i < col_s; ++i)  {
            result[i] /= T(periods_);
            if (as_angle_)  {
                result[i] = std::atan(result[i]);
                if (in_degrees_)
                    result[i] *= pi_180;
            }
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    SlopeVisitor(size_type periods = 1,
                 bool as_angle = false,
                 bool in_degrees = false)
        : periods_(periods), as_angle_(as_angle), in_degrees_(in_degrees)  {  }

private:

    const size_type periods_;
    const bool      as_angle_;
    const bool      in_degrees_;
    result_type     result_ { };
};

// ----------------------------------------------------------------------------

// Ultimate Oscillator indicator
//
template<typename T, typename I = unsigned long>
struct  UltimateOSCIVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));

        std::vector<value_type> max_high;

        max_high.reserve(col_s);
        max_high.push_back(std::numeric_limits<T>::quiet_NaN());
        for (size_type i = 1; i < col_s; ++i)
            max_high.push_back(std::max(*(high_begin + i),
                                        *(close_begin + (i - 1))));

        std::vector<value_type> min_low;

        min_low.reserve(col_s);
        min_low.push_back(std::numeric_limits<T>::quiet_NaN());
        for (size_type i = 1; i < col_s; ++i)
            min_low.push_back(std::min(*(low_begin + i),
                                       *(close_begin + (i - 1))));

        std::vector<value_type> buying_pressure;

        buying_pressure.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            buying_pressure.push_back(*(close_begin + i) - min_low[i]);

        std::vector<value_type> true_range;

        true_range.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            true_range.push_back(max_high[i] - min_low[i]);

        ssr_t   fast_bp_sum (SumVisitor<T, I>(), fast_);
        ssr_t   fast_tr_sum (SumVisitor<T, I>(), fast_);

        fast_bp_sum.pre();
        fast_tr_sum.pre();
        fast_bp_sum (idx_begin, idx_end,
                     buying_pressure.begin(), buying_pressure.end());
        fast_tr_sum (idx_begin, idx_end, true_range.begin(), true_range.end());
        fast_bp_sum.post();
        fast_tr_sum.post();

        ssr_t   med_bp_sum (SumVisitor<T, I>(), medium_);
        ssr_t   med_tr_sum (SumVisitor<T, I>(), medium_);

        med_bp_sum.pre();
        med_tr_sum.pre();
        med_bp_sum (idx_begin, idx_end,
                    buying_pressure.begin(), buying_pressure.end());
        med_tr_sum (idx_begin, idx_end, true_range.begin(), true_range.end());
        med_bp_sum.post();
        med_tr_sum.post();

        ssr_t   slow_bp_sum (SumVisitor<T, I>(), slow_);
        ssr_t   slow_tr_sum (SumVisitor<T, I>(), slow_);

        slow_bp_sum.pre();
        slow_tr_sum.pre();
        slow_bp_sum (idx_begin, idx_end,
                     buying_pressure.begin(), buying_pressure.end());
        slow_tr_sum (idx_begin, idx_end, true_range.begin(), true_range.end());
        slow_bp_sum.post();
        slow_tr_sum.post();

        const value_type    total_weights = slow_w_ + fast_w_ + medium_w_;
        result_type         result;

        result.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    weights =
                (fast_w_ *
                 (fast_bp_sum.get_result()[i] / fast_tr_sum.get_result()[i]) +
                 medium_w_ *
                 (med_bp_sum.get_result()[i] / med_tr_sum.get_result()[i]) +
                 slow_w_ *
                 (slow_bp_sum.get_result()[i] / slow_tr_sum.get_result()[i])) *
                T(100);

            result.push_back(weights / total_weights);
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

    using ssr_t = SimpleRollAdopter<SumVisitor<T, I>, T, I>;

    const size_type     slow_;
    const size_type     fast_;
    const size_type     medium_;
    const value_type    slow_w_;
    const value_type    fast_w_;
    const value_type    medium_w_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long>
using u_osc_v = UltimateOSCIVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  UlcerIndexVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (periods_ < 2)  return;

        GET_COL_SIZE

        SimpleRollAdopter<MaxVisitor<T, I>, T, I>   high (MaxVisitor<T, I>(),
                                                          periods_);

        high.pre();
        high (idx_begin, idx_end, column_begin, column_end);
        high.post();

        result_type result = std::move(high.get_result());

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    val =
                (T(100) * (*(column_begin + i) - result[i])) / result[i];

            result[i] = val * val;
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I>   sum (SumVisitor<T, I>(),
                                                         periods_);
        SimpleRollAdopter<MeanVisitor<T, I>, T, I>  avg (MeanVisitor<T, I>(),
                                                         periods_);

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

        const result_type   &vec =
            use_sum_ ? sum.get_result() : avg.get_result();

        for (size_type i = 0; i < col_s; ++i)
            result[i] = std::sqrt(vec[i] / T(periods_));

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    UlcerIndexVisitor(size_type periods = 14, bool use_sum = true)
        : periods_(periods), use_sum_(use_sum)  {   }

private:

    const size_type periods_;
    const bool      use_sum_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using u_idx_v = UlcerIndexVisitor<T, I>;

// ----------------------------------------------------------------------------

// Trade To Market trend indicator
//
template<typename T, typename I = unsigned long>
struct  TTMTrendVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type = std::vector<bool>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        assert(bar_periods_ > 0);

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));
        assert(((bar_periods_ + 1) < col_s));

        std::vector<T>  trend_avg;

        trend_avg.reserve(col_s);
        for (size_type i = 0; i < bar_periods_; ++i)
            trend_avg.push_back(std::numeric_limits<T>::quiet_NaN());
        for (size_type i = bar_periods_; i < col_s; ++i)
            trend_avg.push_back((*(high_begin + i) + *(low_begin + i)) / T(2));
        for (size_type i = 1; i <= bar_periods_; ++i)
            for (size_type j = i; j < col_s; ++j)
                trend_avg[j] +=
                    (*(high_begin + (j - i)) + *(low_begin + (j - i))) / T(2);
        std::transform(trend_avg.begin(), trend_avg.end(), trend_avg.begin(),
                       std::bind(std::divides<T>(), std::placeholders::_1,
                                 T(bar_periods_ + 1)));

        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)
            result.push_back(*(close_begin + i) > trend_avg[i]);

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    TTMTrendVisitor(size_type bar_periods = 5) : bar_periods_(bar_periods) {  }

private:

    const size_type bar_periods_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using ttmt_v = TTMTrendVisitor<T, I>;

// ----------------------------------------------------------------------------

// Parabolic Stop And Reverse (PSAR)
//
template<typename T, typename I = unsigned long>
struct  ParabolicSARVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type = std::vector<bool>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));
        assert(col_s > 2);

        bool                    bullish { true };
        value_type              high_point { *high_begin };
        value_type              low_point { *low_begin };
        std::vector<value_type> sar { close_begin, close_end };
        value_type              current_af { af_ };
        std::vector<value_type> long_vec(
            col_s, std::numeric_limits<T>::quiet_NaN());
        std::vector<value_type> short_vec { long_vec };
        std::vector<value_type> accel_fact { long_vec };
        std::vector<bool>       reversal(col_s, false);

        accel_fact[0] = accel_fact[1] = current_af;
        for (size_type i = 2; i < col_s; ++i)  {
            bool    reverse { false };

            accel_fact[i] = current_af;
            if (bullish)  {
                sar[i] = sar[i - 1] + current_af * (high_point - sar[i - 1]);
                if (*(low_begin + i) < sar[i])  {
                    bullish = false;
                    reverse = true;
                    current_af = af_;
                    sar[i] = high_point;
                    low_point = *(low_begin + i);
                }
            }
            else  {
                sar[i] = sar[i - 1] + current_af * (low_point - sar[i - 1]);
                if (*(high_begin + i) > sar[i])  {
                    bullish = true;
                    reverse = true;
                    current_af = af_;
                    sar[i] = low_point;
                    high_point = *(high_begin + i);
                }
            }

            reversal[i] = reverse;

            if (! reverse)  {
                if (bullish)  {
                    if (*(high_begin + i) > high_point)  {
                        high_point = *(high_begin + i);
                        current_af = std::min(current_af + af_, max_af_);
                    }
                    if (*(low_begin + (i - 1)) < sar[i])
                        sar[i] = *(low_begin + (i - 1));
                    if (*(low_begin + (i - 2)) < sar[i])
                        sar[i] = *(low_begin + (i - 2));
                }
                else  {
                    if (*(low_begin + i) < low_point)  {
                        low_point = *(low_begin + i);
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

    inline const std::vector<value_type> &
    get_longs () const  { return (long_); }
    inline std::vector<value_type> &get_longs ()  { return (long_); }

    inline const std::vector<value_type> &
    get_shorts () const  { return (short_); }
    inline std::vector<value_type> &get_shorts ()  { return (short_); }

    inline const std::vector<value_type> &
    get_acceleration_factors () const  { return (accel_fact_); }
    inline std::vector<value_type> &
    get_acceleration_factors ()  { return (accel_fact_); }

    explicit
    ParabolicSARVisitor(value_type acceleration_factor = T(0.02),
                        value_type max_acceleration_factor = T(0.2))
        : af_(acceleration_factor),
          max_af_(max_acceleration_factor) {  }

private:

    const value_type        af_;
    const value_type        max_af_;
    result_type             result_ { };
    std::vector<value_type> long_ { };
    std::vector<value_type> short_ { };
    std::vector<value_type> accel_fact_ { };
};

template<typename T, typename I = unsigned long>
using psar_v = ParabolicSARVisitor<T, I>;

// ----------------------------------------------------------------------------

// Even Better Sine Wave (EBSW) indicator
//
template<typename T, typename I = unsigned long>
struct  EBSineWaveVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert(hp_period_ > 38 && bar_period_ < col_s);
        assert(bar_period_ > 0 && hp_period_ < col_s);

        std::vector<T>      result(col_s, std::numeric_limits<T>::quiet_NaN());
        value_type          last_close = *close_begin;
        value_type          last_high_pass { 0 };
        value_type          filter_hist[2] = { 0, 0 };
        const value_type    sin_hp = std::sin(T(360) / T(hp_period_));
        const value_type    cos_hp = std::cos(T(360) / T(hp_period_));
        const value_type    cos_bar =
            std::cos(std::sqrt(T(2)) * T(180) / T(bar_period_));
        const value_type    alpha1 = (T(1) - sin_hp) / cos_hp;
        const value_type    alpha1_effect = T(0.5) * (T(1) + alpha1);

        // Smooth with a Super Smoother Filter from equation 3-3
        //
        const value_type    alpha2 =
            std::exp(-std::sqrt(T(2)) * T(M_PI) / T(bar_period_));
        const value_type    c2 = T(2) * alpha2 * cos_bar;
        const value_type    c3 = T(-1) * alpha2 * alpha2;
        const value_type    c1 = T(1) - c2 - c3;

        for (size_type i = 1; i < col_s; ++i)  {
            const value_type    this_close = *(close_begin + i);

            // High pass filter cyclic components whose periods are shorter
            // than duration input
            //
            const value_type    high_pass =
                alpha1_effect * (this_close - last_close) +
                alpha1 * last_high_pass;
            const value_type    filter =
                c1 * (high_pass + last_high_pass) / T(2) +
                c2 * filter_hist[1] + c3 * filter_hist[0];

            // 3 bar average of wave amplitude and power
            //
            const value_type    wave =
                (filter + filter_hist[1] + filter_hist[0]) / T(3);
            const value_type    power =
                (filter * filter +
                 filter_hist[1] * filter_hist[1] +
                 filter_hist[0] * filter_hist[0]) / T(3);

            // Normalize the average wave to square root of the average power
            //
            result[i] = wave / std::sqrt(power);

            filter_hist[0] = filter_hist[1];  filter_hist[1] = filter;
            last_high_pass = high_pass;
            last_close = this_close;
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    EBSineWaveVisitor(size_type high_pass_period = 40,
                      size_type bar_period = 10)
        : hp_period_(high_pass_period), bar_period_(bar_period) {  }

private:

    const size_type hp_period_;
    const size_type bar_period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using ebsw_v = EBSineWaveVisitor<T, I>;

// ----------------------------------------------------------------------------

// Ehler's Super Smoother Filter (SSF) indicator
//
template<typename T, typename I = unsigned long>
struct  EhlerSuperSmootherVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        assert(poles_ == 2 || poles_ == 3);
        assert(bar_period_ > 0 && bar_period_ < col_s);

        result_type result(column_begin, column_end);

        if (poles_ == 2)  {
            const value_type    x = T(M_PI) * std::sqrt(T(2)) / T(bar_period_);
            const value_type    a0 = std::exp(-x);
            const value_type    a1 = -a0 * a0;
            const value_type    c0 = T(2) * a0 * std::cos(x);
            const value_type    c1 = T(1) - a1 - c0;

            for (size_type i = poles_; i < col_s; ++i)
                result[i] =
                    c1 * *(column_begin + i) +
                    c0 * result[i - 1] +
                    a1 * result[i - 2];
        }
        else if (poles_ == 3)  {
            const value_type    x = T(M_PI) / T(bar_period_);
            const value_type    a0 = std::exp(-x);
            const value_type    b0 = T(2) * a0 * std::cos(std::sqrt(T(3)) * x);
            const value_type    a1 = a0 * a0;
            const value_type    a2 = a1 * a1;
            const value_type    c2 = -a1 * (T(1) + b0);
            const value_type    c0 = a1 + b0;
            const value_type    c1 = T(1) - c0 - c2 - a2;

            for (size_type i = poles_; i < col_s; ++i)
                result[i] =
                    c1 * *(column_begin + i) +
                    c0 * result[i - 1] +
                    c2 * result[i - 2] +
                    a2 * result[i - 3];
        }

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    EhlerSuperSmootherVisitor(size_type poles = 2, size_type bar_period = 10)
        : poles_(poles), bar_period_(bar_period)  {  }

private:

    const size_type poles_;
    const size_type bar_period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using ess_v = EhlerSuperSmootherVisitor<T, I>;

// ----------------------------------------------------------------------------

// Variable Index Dynamic Average (VIDYA) indicator
//
template<typename T, typename I = unsigned long>
struct  VarIdxDynAvgVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        assert(roll_period_ > 1 && roll_period_ < col_s);

        DiffVisitor<T, I>   diff(1, false);

        diff.pre();
        diff (idx_begin, idx_end, column_begin, column_end);
        diff.post();

        result_type positive = std::move(diff.get_result());

        positive[0] = 0;

        result_type negative = positive;

        for (size_type i = 0; i < col_s; ++i)  {
            if (positive[i] < 0)  positive[i] = 0;
            if (negative[i] > 0)
                negative[i] = 0;
            else
                negative[i] = std::fabs(negative[i]);
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I>   sum (SumVisitor<T, I>(),
                                                         roll_period_);

        sum.pre();
        sum (idx_begin, idx_end, positive.begin(), positive.end());
        sum.post();
        positive = std::move(sum.get_result());
        sum.pre();
        sum (idx_begin, idx_end, negative.begin(), negative.end());
        sum.post();
        negative = std::move(sum.get_result());

        result_type result(col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = roll_period_; i < col_s; ++i)
            result[i] =
                std::fabs((positive[i] - negative[i]) /
                          (positive[i] + negative[i]));

        const value_type    alpha = T(2) / (T(roll_period_) + T(1));

        result[roll_period_ - 1] = 0;
        for (size_type i = roll_period_; i < col_s; ++i)
            result[i] =
                alpha * result[i] * *(column_begin + i) +
                result[i - 1] * (T(1) - alpha * result[i]);

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    VarIdxDynAvgVisitor(size_type roll_period = 14)
        : roll_period_(roll_period)  {  }

private:

    const size_type roll_period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using vidya_v = VarIdxDynAvgVisitor<T, I>;

// ----------------------------------------------------------------------------

// Pivot Points, Supports and Resistances indicators
//
template<typename T, typename I = unsigned long>
struct  PivotPointSRVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert(col_s > 1);
        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));

        constexpr value_type two = 2;

        result_type pivot_point(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type resist_1(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type resist_2(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type resist_3(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type support_1(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type support_2(col_s, std::numeric_limits<T>::quiet_NaN());
        result_type support_3(col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    low = *(low_begin + i);
            const value_type    high = *(high_begin + i);
            const value_type    pp = (low + high + *(close_begin + i)) / T(3);

            pivot_point[i] = pp;
            resist_1[i] = two * pp - low;
            support_1[i] = two * pp - high;
            resist_2[i] = pp + high - low;
            support_2[i] = pp - high + low;
            resist_3[i] = high + two * (pp - low);
            support_3[i] = low - two * (high - pp);
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

template<typename T, typename I = unsigned long>
using ppsr_v = PivotPointSRVisitor<T, I>;

// ----------------------------------------------------------------------------

// Average Directional Movement Index (ADX)
//
template<typename T, typename I = unsigned long>
struct  AvgDirMovIdxVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert(col_s > 3);
        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));

        result_type pos_di(col_s);
        result_type neg_di(col_s);
        result_type true_range(col_s);
        result_type result(col_s);

        for (size_type i = 0; i < col_s - 1; ++i)  {
            const value_type    nxt_h = *(high_begin + (i + 1));
            const value_type    nxt_l = *(low_begin + (i + 1));
            const value_type    pos_move = nxt_h - *(high_begin + i);
            const value_type    neg_move = *(low_begin + i) - nxt_l;

            pos_di[i] = (pos_move > neg_move && pos_move > 0) ? pos_move : 0;
            neg_di[i] = (neg_move > pos_move && neg_move > 0) ? neg_move : 0;

            const value_type    close = *(close_begin + i);

            true_range[i] = std::max(nxt_h, close) - std::min(nxt_l, close);
        }

        ewm_v<double>   ewm(exponential_decay_spec::span, dir_smoother_, true);

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
        value_type  prev_val = 0;

        for (size_type i = 0; i < col_s - 1; ++i)  {
            const value_type    pos_val = pos_di[i] / true_range[i];
            const value_type    neg_val = neg_di[i] / true_range[i];
            const value_type    val =
                std::fabs(pos_val - neg_val) / (pos_val + neg_val);

            if (! is_nan__(val))  prev_val = val;
            dx[i] = prev_val;
        }

        ewm_v<double>   ewm2(exponential_decay_spec::span, adx_smoother_, true);

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

template<typename T, typename I = unsigned long>
using adx_v = AvgDirMovIdxVisitor<T, I>;

// ----------------------------------------------------------------------------

// Holt-Winter Channel (HWC) indicator
//
template<typename T, typename I = unsigned long>
struct  HoltWinterChannelVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert(col_s > 3);

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type upper (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type lower (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type pct_diff (col_s, std::numeric_limits<T>::quiet_NaN());
        value_type  last_a = 0;
        value_type  last_v = 0;
        value_type  last_var = 0;
        value_type  last_f = *close_begin;
        value_type  last_price = last_f;
        value_type  last_result = last_f;

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    val = *(close_begin + i);
            const value_type    f =
                (T(1) - na_) * (last_f + last_v + T(0.5) * last_a) + na_ * val;
            const value_type    v =
                (T(1) - nb_) * (last_v + last_a) + nb_ * (f - last_f);
            const value_type    a =
                (T(1) - nc_) * last_a + nc_ * (v - last_v);
            const value_type    var =
                (T(1) - nd_) * last_var +
                nd_ * (last_price - last_result) * (last_price - last_result);
            const value_type    stddev = std::sqrt(last_var);

            last_result = result[i] = f + v + T(0.5) * a;
            upper[i] = result[i] + stddev;
            lower[i] = result[i] - stddev;

            const value_type    diff = upper[i] - lower[i];

            if (diff > 0)
                pct_diff[i] = (val - lower[i]) / diff;

            last_price = val;
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

template<typename T, typename I = unsigned long>
using hwc_v = HoltWinterChannelVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct  HeikinAshiCndlVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(open_begin, open_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type open (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type high (col_s, std::numeric_limits<T>::quiet_NaN());
        result_type low (col_s, std::numeric_limits<T>::quiet_NaN());

        result[0] =
            T(0.25) * (*open_begin + *high_begin + *low_begin + *close_begin);
        high[0] = *high_begin;
        low[0] = *low_begin;
        open[0] = T(0.5) * (*open_begin + *close_begin);
        for (size_type i = 1; i < col_s; ++i)  {
            const value_type    hval = *(high_begin + i);
            const value_type    lval = *(low_begin + i);

            result[i] =
                T(0.25) *
                (*(open_begin + i) + hval + lval + *(close_begin + i));
            open[i] = T(0.5) * (open[i - 1] + result[i - 1]);
            high[i] = std::max({ open[i], hval, result[i] });
            low[i] = std::min({ open[i], lval, result[i] });
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

template<typename T, typename I = unsigned long>
using ha_cdl_v = HeikinAshiCndlVisitor<T, I>;

// ----------------------------------------------------------------------------

// Also called Stochastic Oscillator
//
template<typename T, typename I = unsigned long>
struct  CenterOfGravityVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ == 0)  return;

        GET_COL_SIZE
        assert (col_s > roll_count_);

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = roll_count_ - 1; i < col_s; ++i)  {
            size_type   count { 0 };
            value_type  dot_prd { 0 };

            for (size_type j = (i + 1) - roll_count_; j <= i; ++j, ++count)
                dot_prd += *(column_begin + j) * (roll_count_ - count);
            result[i] = dot_prd;
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I>   sum_v
            { SumVisitor<T, I>(), roll_count_ };

        sum_v.pre();
        sum_v (idx_begin, idx_end, column_begin, column_end);
        sum_v.post();

        for (size_type i = 0; i < col_s; ++i)
            result[i] /= -sum_v.get_result()[i];
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    CenterOfGravityVisitor(size_type r_count = 10) : roll_count_(r_count) {   }

private:

    const size_type roll_count_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using cog_v = CenterOfGravityVisitor<T, I>;

// ----------------------------------------------------------------------------

// Arnaud Legoux Moving Average
//
template<typename T, typename I = unsigned long>
struct  ArnaudLegouxMAVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ == 0)  return;

        GET_COL_SIZE
        assert (roll_count_ > 1);
        assert (col_s > roll_count_);

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = roll_count_; i < col_s; ++i)  {
            value_type  win_sum { 0 };

            for (size_type j = 0; j < roll_count_; ++j)
                win_sum += wtd_[j] * *(column_begin + (i - j));

            result[i] = win_sum / cum_sum_;
        }

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    ArnaudLegouxMAVisitor(size_type r_count = 10,
                          value_type sigma = 6.0,
                          value_type dist_offset = 0.85)
        : roll_count_(r_count),
          m_(dist_offset * T(r_count - 1)),
          s_(T(r_count) / sigma),
          wtd_(r_count)  {

        for (size_type i = 0; i < roll_count_; ++i)  {
            wtd_[i] =
                std::exp(T(-1) *
                         ((T(i) - m_) * (T(i) - m_)) /
                         ((T(2) * s_ * s_)));
            cum_sum_ += wtd_[i];
        }
    }

private:

    const size_type         roll_count_;
    const value_type        m_;
    const value_type        s_;
    value_type              cum_sum_ { 0 };
    std::vector<value_type> wtd_;
    result_type             result_ { };
};

template<typename T, typename I = unsigned long>
using alma_v = ArnaudLegouxMAVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  RateOfChangeVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE
        assert (period_ > 0);

        DiffVisitor<T, I>   diff(period_, false);

        diff.pre();
        diff (idx_begin, idx_end, column_begin, column_end);
        diff.post();

        result_type result = std::move(diff.get_result());

        for (size_type i = period_; i < col_s; ++i)
            result[i] /= *(column_begin + (i - period_));
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    RateOfChangeVisitor(size_type period) : period_(period)  {   }

private:

    const size_type period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using roc_v = RateOfChangeVisitor<T, I>;

// ----------------------------------------------------------------------------

// Accumulation/Distribution (AD) indicator
//
template<typename T, typename I = unsigned long>
struct  AccumDistVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H, typename V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(open_begin, open_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));
        assert((col_s == size_type(std::distance(volume_begin, volume_end))));

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    co = *(close_begin + i) - *(open_begin + i);
            const value_type    hl = *(high_begin + i) - *(low_begin + i);

            result[i] = co * T(*(volume_begin + i)) / hl;
        }

        CumSumVisitor<T, I> cumsum;

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

template<typename T, typename I = unsigned long>
using ad_v = AccumDistVisitor<T, I>;

// ----------------------------------------------------------------------------

// Chaikin Money Flow (CMF) indicator
//
template<typename T, typename I = unsigned long>
struct  ChaikinMoneyFlowVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H, typename V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &open_begin, const H &open_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(low_begin, low_end))));
        assert((col_s == size_type(std::distance(open_begin, open_end))));
        assert((col_s == size_type(std::distance(high_begin, high_end))));
        assert((col_s == size_type(std::distance(volume_begin, volume_end))));
        assert (period_ > 0 && period_ < col_s);

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    co = *(close_begin + i) - *(open_begin + i);
            const value_type    hl = *(high_begin + i) - *(low_begin + i);

            result[i] = co * T(*(volume_begin + i)) / hl;
        }

        SimpleRollAdopter<SumVisitor<T, I>, T, I>   sum
            { SumVisitor<T, I>(), period_ };

        sum.pre();
        sum (idx_begin, idx_end, result.begin(), result.end());
        sum.post();
        result = std::move(sum.get_result());

        sum.pre();
        sum (idx_begin, idx_end, volume_begin, volume_end);
        sum.post();
        std::transform(result.begin(), result.end(),
                       sum.get_result().begin(),
                       result.begin(),
                       std::divides<T>());

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

template<typename T, typename I = unsigned long>
using cmf_v = ChaikinMoneyFlowVisitor<T, I>;

// ----------------------------------------------------------------------------

// Vertical Horizontal Filter (VHF) indicator
//
template<typename T, typename I = unsigned long>
struct  VertHorizFilterVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE
        assert (period_ > 0 && period_ < col_s);

        SimpleRollAdopter<MaxVisitor<T, I>, T, I>   mx
            { MaxVisitor<T, I>(), period_ };

        mx.pre();
        mx (idx_begin, idx_end, column_begin, column_end);
        mx.post();

        SimpleRollAdopter<MinVisitor<T, I>, T, I>   mn
            { MinVisitor<T, I>(), period_ };

        mn.pre();
        mn (idx_begin, idx_end, column_begin, column_end);
        mn.post();

        DiffVisitor<T, I>   diff(1, false);

        diff.pre();
        diff (idx_begin, idx_end, column_begin, column_end);
        diff.post();
        for (size_type i = 0; i < col_s; ++i)
            diff.get_result()[i] = std::fabs(diff.get_result()[i]);

        SimpleRollAdopter<SumVisitor<T, I>, T, I>   diff_sum
            { SumVisitor<T, I>(), period_ };

        diff_sum.pre();
        diff_sum (idx_begin, idx_end,
                  diff.get_result().begin(), diff.get_result().end());
        diff_sum.post();

        result_type result (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = period_; i < col_s; ++i)
            result[i] =
                std::fabs(mx.get_result()[i] - mn.get_result()[i]) /
                diff_sum.get_result()[i];

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    VertHorizFilterVisitor(size_type period = 28) : period_(period)  {   }

private:

    const size_type period_;
    result_type     result_ { };
};

template<typename T, typename I = unsigned long>
using vhf_v = VertHorizFilterVisitor<T, I>;

// ----------------------------------------------------------------------------

// On Balance Volume (OBV) indicator
//
template<typename T, typename I = unsigned long>
struct  OnBalanceVolumeVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H, typename V>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &close_begin, const H &close_end,
                const V &volume_begin, const V &volume_end)  {

        const size_type col_s = std::distance(close_begin, close_end);

        assert((col_s == size_type(std::distance(volume_begin, volume_end))));

        ReturnVisitor<T, I> ret (return_policy::trinary);

        ret.pre();
        ret (idx_begin, idx_end, close_begin, close_end);
        ret.post();

        result_type result = std::move(ret.get_result());

        result[0] = T(1);
        for (size_type i = 0; i < col_s; ++i)
            result[i] *= T(*(volume_begin + i));

        CumSumVisitor<T, I> cumsum;

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

template<typename T, typename I = unsigned long>
using obv_v = OnBalanceVolumeVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  TrueRangeVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end,
                const H &close_begin, const H &close_end)  {

        const size_type col_s = std::distance(close_begin, close_end);
        result_type     result (col_s, std::numeric_limits<T>::quiet_NaN());

        for (size_type i = 1; i < col_s; ++i)  {
            const value_type    high = *(high_begin + i);
            const value_type    low = *(low_begin + i);
            const value_type    prev_c = *(close_begin + (i - 1));

            result[i] = std::max({ std::fabs(high - low),
                                   std::fabs(high - prev_c),
                                   std::fabs(prev_c - low) });
        }

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

private:

    result_type result_ {  };
};

// ----------------------------------------------------------------------------

// Decay indicator
//
template<typename T, typename I = unsigned long>
struct  DecayVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE
        assert (period_ > 0 && period_ < col_s);

        result_type         result (col_s);
        const value_type    decay =
            expo_ ? std::exp(-T(period_)) : (T(1) / T(period_));

        result[0] = *column_begin;
        for (size_type i = 1; i < col_s; ++i)
            result[i] =
                std::max({ *(column_begin + i),
                           *(column_begin + (i - 1)) - decay,
                           T(0) });

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    DecayVisitor(size_type period = 5, bool exponential = false)
        : period_(period), expo_(exponential)  {   }

private:

    const size_type period_;
    const bool      expo_;
    result_type     result_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct HodgesTompkinsVolVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE
        assert (roll_count_ > 0 && roll_count_ < col_s);

        ReturnVisitor<T, I> ret (return_policy::log);

        ret.pre();
        ret (idx_begin, idx_end, column_begin, column_end);
        ret.post();

        SimpleRollAdopter<StdVisitor<T, I>, T, I>   stdev
            { StdVisitor<T, I>(), roll_count_ };

        stdev.pre();
        stdev (idx_begin, idx_end,
               ret.get_result().begin(), ret.get_result().end());
        stdev.post();

        result_type         result = std::move(stdev.get_result());
        const value_type    annual = std::sqrt(trading_periods_);
        const value_type    n = T(col_s - roll_count_ + 1);
        const value_type    adj_factor =
            T(1) / (T(1) -
                    (T(roll_count_) / n) +
                    ((T(roll_count_ * roll_count_) - T(1)) / (T(3) * n * n)));

        std::transform(result.begin(), result.end(), result.begin(),
                       std::bind(std::multiplies<T>(), std::placeholders::_1,
                                 annual * adj_factor));

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    HodgesTompkinsVolVisitor(size_type roll_count = 30,
                             size_type trading_periods = 252)
        : roll_count_(roll_count), trading_periods_(trading_periods)  {  }

private:

    result_type     result_ {  };
    const size_type roll_count_;
    const size_type trading_periods_;
};

template<typename T, typename I = unsigned long>
using ht_vol_v = HodgesTompkinsVolVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct ParkinsonVolVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &low_begin, const H &low_end,
                const H &high_begin, const H &high_end)  {

        const size_type col_s = std::distance(low_begin, low_end);

        assert((col_s == size_type(std::distance(high_begin, high_end))));
        assert (roll_count_ > 0 && roll_count_ < col_s);

        result_type         result (col_s);
        const value_type    factor = T(1) / (T(4) * std::log(T(2)));

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    val =
                std::log(*(high_begin + i) / *(low_begin + i));

            result[i] = factor * val * val;
        }

        SimpleRollAdopter<MeanVisitor<T, I>, T, I>  avg
            { MeanVisitor<T, I>(), roll_count_ } ;

        avg.pre();
        avg (idx_begin, idx_end, result.begin(), result.end());
        avg.post();
        result = std::move(avg.get_result());

        for (size_type i = roll_count_ - 1; i < col_s; ++i)
            result[i] = std::sqrt(result[i] * T(trading_periods_));

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

template<typename T, typename I = unsigned long>
using p_vol_v = ParkinsonVolVisitor<T, I>;

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
