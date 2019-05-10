// Hossein Moein
// September 22, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#ifndef HMDF_DATAFRAMEVISITORS_HPP
#define HMDF_DATAFRAMEVISITORS_HPP

#include "DataFrame_lib_exports.h"

#include "DataFrame.h"
#include <cstddef>
#include <algorithm>
#include <limits>
#include <type_traits>
#include <array>
#include <cmath>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MeanVisitor {

private:

    T           mean_ { T(0) };
    std::size_t cnt_ { 0 };

public:

    using value_type = T;

    inline void operator() (const TS_T &, const T &val)  {

        mean_ += val;
        cnt_ +=1;
    }
    inline void pre ()  { mean_ = T(0); cnt_ = 0; }
    inline void post ()  {  }
    inline std::size_t get_count () const  { return (cnt_); }
    inline T get_sum () const  { return (mean_); }
    inline T get_value () const  { return (mean_ / T(cnt_)); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SumVisitor {

private:

    T   sum_ { T(0) };

public:

    using value_type = T;

    inline void operator() (const TS_T &, const T &val)  {

        sum_ += val;
    }
    inline void pre ()  { sum_ = T(0); }
    inline void post ()  {  }
    inline T get_value () const  { return (sum_); }
};

// ----------------------------------------------------------------------------

template<typename T, typename TS_T = unsigned long>
struct MaxVisitor {

private:

    T       max_ { };
    TS_T    index_ { };
    bool    is_first { true };

public:

    using value_type = T;

    inline void operator() (const TS_T &idx, const T &val)  {

        if (val > max_ || is_first) {
            max_ = val;
            index_ = idx;
            is_first = false;
        }
    }
    inline void pre ()  { is_first = true; }
    inline void post ()  {  }
    inline T get_value () const  { return (max_); }
    inline TS_T get_index () const  { return (index_); }
};

// ----------------------------------------------------------------------------

template<typename T, typename TS_T = unsigned long>
struct MinVisitor {

private:

    T       min_ { };
    TS_T    index_ { };
    bool    is_first { true };

public:

    using value_type = T;

    inline void operator() (const TS_T &idx, const T &val)  {

        if (val < min_ || is_first) {
            min_ = val;
            index_ = idx;
            is_first = false;
        }
    }
    inline void pre ()  { is_first = true; }
    inline void post ()  {  }
    inline T get_value () const  { return (min_); }
    inline TS_T get_index () const  { return (index_); }
};

// ----------------------------------------------------------------------------

// This visitor takes, at most (when sequance is already sorted), O(N*M)
// time, where N is the number of largest values and M is the total number
// of all values. The assumption is that N should be relatively small, so the
// complexity is not bad.
// I could have used a priority queue, but that requires a std::vector
// instaed of std::array. I think the advantage of using std::array is bigger
// than O(MlogM) vs. O(N*M) for majority of usage.
//
template<std::size_t N, typename T, typename TS_T = unsigned long>
struct  NLargestVisitor {

public:

    struct  DataItem  {
        T       value { };
        TS_T    index { };
    };

    using value_type = DataItem;
    using result_type = std::array<value_type, N>;

private:

    result_type items_ { };
    std::size_t counter_ { 0 };
    int         min_index_ { -1 };

public:

    inline void operator() (const TS_T &idx, const T &val)  {

        if (counter_ < N)  {
            items_[counter_] = { val, idx };
            if (min_index_ < 0 || val < items_[min_index_].value)
                min_index_ = static_cast<int>(counter_);
        }
        else if (val > items_[min_index_].value)  {
            items_[min_index_] = { val, idx };
            min_index_ = 0;
            for (int i = 1; i < N; ++i)
                if (items_[i].value < items_[min_index_].value)
                    min_index_ = i;
        }

        counter_ += 1;
    }
    inline void pre ()  { counter_ = 0; min_index_ = -1; }
    inline void post ()  {  }
    inline const result_type &get_values () const  { return (items_); }

    inline void sort_by_index()  {

        std::sort(items_.begin(), items_.end(),
                  [](const value_type &lhs, const value_type &rhs) -> bool  {
                      return (lhs.index < rhs.index);
                  });
    }
    inline void sort_by_value()  {

        std::sort(items_.begin(), items_.end(),
                  [](const value_type &lhs, const value_type &rhs) -> bool  {
                      return (lhs.value < rhs.value);
                  });
    }
};

// ----------------------------------------------------------------------------

// This visitor takes, at most (when the sequance is already sorted), O(N*M)
// time, where N is the number of largest values and M is the total number
// of all values. The assumption is that N should be relatively small, so the
// complexity is not bad.
// I could have used a priority queue, but that requires a std::vector
// instaed of std::array. I think the advantage of using std::array is bigger
// than O(MlogM) vs. O(N*M) for majority of usage.
//
template<std::size_t N, typename T, typename TS_T = unsigned long>
struct  NSmallestVisitor {

public:

    struct  DataItem  {
        T       value { };
        TS_T    index { };
    };

    using value_type = DataItem;
    using result_type = std::array<value_type, N>;

private:

    result_type items_ { };
    std::size_t counter_ { 0 };
    int         max_index_ { -1 };

public:

    inline void operator() (const TS_T &idx, const T &val)  {

        if (counter_ < N)  {
            items_[counter_] = { val, idx };
            if (max_index_ < 0 || val > items_[max_index_].value)
                max_index_ = static_cast<int>(counter_);
        }
        else if (val < items_[max_index_].value)  {
            items_[max_index_] = { val, idx };
            max_index_ = 0;
            for (int i = 1; i < N; ++i)
                if (items_[i].value > items_[max_index_].value)
                    max_index_ = i;
        }

        counter_ += 1;
    }
    inline void pre ()  { counter_ = 0; max_index_ = -1; }
    inline void post ()  {  }
    inline const result_type &get_values () const  { return (items_); }

    inline void sort_by_index()  {

        std::sort(items_.begin(), items_.end(),
                  [](const value_type &lhs, const value_type &rhs) -> bool  {
                      return (lhs.index < rhs.index);
                  });
    }
    inline void sort_by_value()  {

        std::sort(items_.begin(), items_.end(),
                  [](const value_type &lhs, const value_type &rhs) -> bool  {
                      return (lhs.value < rhs.value);
                  });
    }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CovVisitor {

private:

    T           total1_ { T(0) };
    T           total2_ { T(0) };
    T           dot_prod_ { T(0) };
    T           dot_prod1_ { T(0) };
    T           dot_prod2_ { T(0) };
    std::size_t cnt_ { 0 };
    const bool  b_;

public:

    using value_type = T;

    explicit CovVisitor (bool bias = true) : b_ (bias) {  }
    inline void operator() (const TS_T &, const T &val1, const T &val2)  {

        total1_ += val1;
        total2_ += val2;
        dot_prod_ += (val1 * val2);
        dot_prod1_ += (val1 * val1);
        dot_prod2_ += (val2 * val2);
        cnt_ += 1;
    }
    inline void pre ()  {

        total1_ = total2_ = dot_prod_ = dot_prod1_ = dot_prod2_ = T(0);
        cnt_ = 0;
    }
    inline void post ()  {  }
    inline T get_value () const  {

        return ((dot_prod_ - (total1_ * total2_) / T(cnt_)) / (T(cnt_) - T(1)));
    }

    inline T get_var1 () const  {

        return((dot_prod1_ - (total1_ * total1_) / T(cnt_)) / (T(cnt_) - T(1)));
    }
    inline T get_var2 () const  {

        return((dot_prod2_ - (total2_ * total2_) / T(cnt_)) / (T(cnt_) - T(1)));
    }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct VarVisitor  {

private:

    CovVisitor<T, TS_T> cov_;

public:

    using value_type = T;

    explicit VarVisitor (std::size_t bias = 1) : cov_ (bias)  {   }
    inline void operator() (const TS_T &idx, const T &val)  {

        cov_ (idx, val, val);
    }
    inline void pre ()  { cov_.pre(); }
    inline void post ()  {  }
    inline T get_value () const  { return (cov_.get_value()); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StdVisitor   {

private:

    VarVisitor<T, TS_T> var_;

public:

    using value_type = T;

    explicit StdVisitor (std::size_t bias = 1) : var_ (bias)  {   }
    inline void operator() (const TS_T &idx, const T &val)  {

        var_ (idx, val);
    }
    inline void pre ()  { var_.pre(); }
    inline void post ()  {  }
    inline T get_value () const  { return (::sqrt(var_.get_value())); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CorrVisitor  {

private:

    CovVisitor<T, TS_T> cov_;

public:

    using value_type = T;

    explicit CorrVisitor (std::size_t bias = 1) : cov_ (bias)  {   }
    inline void operator() (const TS_T &idx, const T &val1, const T &val2)  {

        cov_ (idx, val1, val2);
    }
    inline void pre ()  { cov_.pre(); }
    inline void post ()  {  }
    inline T get_value () const  {

        return (cov_.get_value() /
                (::sqrt(cov_.get_var1())* ::sqrt(cov_.get_var2())));
    }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DotProdVisitor  {

private:

    T dot_prod_ { T(0) };

public:

    using value_type = T;

    inline void operator() (const TS_T &idx, const T &val1, const T &val2)  {

        dot_prod_ += (val1 * val2);
    }
    inline void pre ()  { dot_prod_ = T(0); }
    inline void pro ()  {  }
    inline T get_value () const  { return (dot_prod_); }
};

// ----------------------------------------------------------------------------

// One-pass stats calculation.
//
template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StatsVisitor  {

private:

    std::size_t n_ { 0 };
    T           m1_ { T(0) };
    T           m2_ { T(0) };
    T           m3_ { T(0) };
    T           m4_ { T(0) };

public:

    using value_type = T;

    inline void operator() (const TS_T &idx, const T &val)  {

        T           delta, delta_n, delta_n2, term1;
        std::size_t n1 = n_;

        n_ += 1;
        delta = val - m1_;
        delta_n = delta / T(n_);
        delta_n2 = delta_n * delta_n;
        term1 = delta * delta_n * T(n1);
        m1_ += delta_n;
        m4_ += term1 * delta_n2 * T(n_ * n_ - 3 * n_ + 3) +
               6.0 * delta_n2 * m2_ -
               4.0 * delta_n * m3_;
        m3_ += term1 * delta_n * T(n_ - 2) - 3.0 * delta_n * m2_;
        m2_ += term1;
    }
    inline void pre ()  {

        n_ = 0;
        m1_ = m2_ = m3_ = m4_ = T(0);
    }
    inline void post ()  {  }

    inline std::size_t get_count () const { return (n_); }
    inline T get_mean () const  { return (m1_); }
    inline T get_variance () const  { return (m2_ / (T(n_) - 1.0)); }
    inline T get_std () const  { return (::sqrt(get_variance())); }
    inline T get_skew () const  {

        return (::sqrt(n_) * m3_ / ::pow(m2_, 1.5));
    }
    inline T get_kurtosis () const  {

        return (T(n_) * m4_ / (m2_ * m2_) - 3.0);
    }
};

// ----------------------------------------------------------------------------

// One pass simple linear regression
//
template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SLRegressionVisitor  {

private:

    std::size_t             n_ { 0 };

    // Sum of the product of the difference between x and its mean and
    // the difference between y and its mean.
    T                       s_xy_ { T(0) };
    StatsVisitor<T, TS_T>   x_stats_;
    StatsVisitor<T, TS_T>   y_stats_;

public:

    using value_type = T;

    inline void operator() (const TS_T &idx, const T &x, const T &y)  {

        s_xy_ += (x_stats_.get_mean() - x) *
                 (y_stats_.get_mean() - y) *
                 T(n_) / T(n_ + 1);

        x_stats_(idx, x);
        y_stats_(idx, y);
        n_ += 1;
    }
    inline void pre ()  {

        n_ = 0;
        s_xy_ = T(0);
        x_stats_.pre();
        y_stats_.pre();
    }
    inline void post ()  {  }

    inline std::size_t get_count () const { return (n_); }
    inline T get_slope () const  {

        // Sum of the squares of the difference between each x and
        // the mean x value.
        const T s_xx = x_stats_.get_variance() * T(n_ - 1);

        return (s_xy_ / s_xx);
    }
    inline T get_intercept () const  {

        return (y_stats_.get_mean() - get_slope() * x_stats_.get_mean());
    }
    inline T get_corr () const  {

        const T t = x_stats_.get_std() * y_stats_.get_std();

        return (s_xy_ / (T(n_ - 1) * t));
    }
};

// ----------------------------------------------------------------------------

struct GroupbySum
    : HeteroVector::visitor_base<int, double, long, std::string>  {

private:

    int             int_sum { 0 };
    unsigned int    uint_sum { 0 };
    double          double_sum { 0.0 };
    long            long_sum { 0 };
    unsigned long   ulong_sum { 0 };
    std::string     str_sum { };

public:

    template<typename T>
    void
    operator() (const unsigned long &ts, const char *name, const T &datum)  {

        return;
    }

    void reset ()  {

        int_sum = 0;
        uint_sum = 0;
        double_sum = 0.0;
        long_sum = 0;
        ulong_sum = 0;
        str_sum.clear();
    }

    template<typename T> void get_value (T &) const  { return; }
};

// -------------------------------------

template<>
void GroupbySum::
operator() (const unsigned long &ts,
            const char *name,
            const int &datum)  { int_sum += datum; }
template<>
void GroupbySum::
operator() (const unsigned long &ts,
            const char *name,
            const unsigned int &datum)  { uint_sum += datum; }
template<>
void GroupbySum::
operator() (const unsigned long &ts,
            const char *name,
            const double &datum)  { double_sum += datum; }
template<>
void GroupbySum::
operator() (const unsigned long &ts,
            const char *name,
            const long &datum)  { long_sum += datum; }
template<>
void GroupbySum::
operator() (const unsigned long &ts,
            const char *name,
            const unsigned long &datum)  { ulong_sum += datum; }
template<>
void GroupbySum::
operator() (const unsigned long &ts,
            const char *name,
            const std::string &datum)  {

    if (str_sum.empty())
        str_sum += datum;
    else  {
        str_sum += '|';
        str_sum += datum;
    }
}

// -------------------------------------

template<>
void GroupbySum::get_value<int> (int &v) const  { v = int_sum; }
template<>
void GroupbySum::
get_value<unsigned int> (unsigned int &v) const  { v = uint_sum; }
template<>
void GroupbySum::get_value<double> (double &v) const  { v = double_sum; }
template<>
void GroupbySum::get_value<long> (long &v) const  { v = long_sum; }
template<>
void GroupbySum::
get_value<unsigned long>(unsigned long &v) const { v = ulong_sum; }
template<>
void GroupbySum::
get_value<std::string> (std::string &v) const  { v = str_sum; }

} // namespace hmdf

#endif //HMDF_DATAFRAMEVISITORS_HPP

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
