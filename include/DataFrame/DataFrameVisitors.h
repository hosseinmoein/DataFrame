// Hossein Moein
// September 22, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <DataFrame/DataFrame.h>
#include <cstddef>
#include <algorithm>
#include <limits>
#include <numeric>
#include <type_traits>
#include <array>

// ----------------------------------------------------------------------------

namespace hmdf
{

enum class return_policy : unsigned char  {
    log = 1,
    percentage = 2,
    monetary = 3,
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MeanVisitor {

private:

    T           mean_ { T(0) };
    std::size_t cnt_ { 0 };

public:

    using value_type = T;

    inline void operator() (const I &, const T &val)  {

        if (is_nan__(val))  return;

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
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SumVisitor {

private:

    T   sum_ { T(0) };

public:

    using value_type = T;

    inline void operator() (const I &, const T &val)  {

        if (is_nan__(val))  return;

        sum_ += val;
    }
    inline void pre ()  { sum_ = T(0); }
    inline void post ()  {  }
    inline T get_value () const  { return (sum_); }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MaxVisitor {

private:

    T       max_ { };
    I       index_ { };
    bool    is_first { true };

public:

    using value_type = T;

    inline void operator() (const I &idx, const T &val)  {

        if (is_nan__(val))  return;

        if (val > max_ || is_first) {
            max_ = val;
            index_ = idx;
            is_first = false;
        }
    }
    inline void pre ()  { is_first = true; }
    inline void post ()  {  }
    inline T get_value () const  { return (max_); }
    inline I get_index () const  { return (index_); }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MinVisitor {

private:

    T       min_ { };
    I       index_ { };
    bool    is_first { true };

public:

    using value_type = T;

    inline void operator() (const I &idx, const T &val)  {

        if (is_nan__(val))  return;

        if (val < min_ || is_first) {
            min_ = val;
            index_ = idx;
            is_first = false;
        }
    }
    inline void pre ()  { is_first = true; }
    inline void post ()  {  }
    inline T get_value () const  { return (min_); }
    inline I get_index () const  { return (index_); }
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
template<std::size_t N, typename T, typename I = unsigned long>
struct  NLargestVisitor {

public:

    struct  DataItem  {
        T   value { };
        I   index { };
    };

    using value_type = DataItem;
    using result_type = std::array<value_type, N>;

private:

    result_type items_ { };
    std::size_t counter_ { 0 };
    int         min_index_ { -1 };

public:

    inline void operator() (const I &idx, const T &val)  {

        if (is_nan__(val))  return;

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
    inline const result_type &get_value () const  { return (items_); }

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
template<std::size_t N, typename T, typename I = unsigned long>
struct  NSmallestVisitor {

public:

    struct  DataItem  {
        T   value { };
        I   index { };
    };

    using value_type = DataItem;
    using result_type = std::array<value_type, N>;

private:

    result_type items_ { };
    std::size_t counter_ { 0 };
    int         max_index_ { -1 };

public:

    inline void operator() (const I &idx, const T &val)  {

        if (is_nan__(val))  return;

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
    inline const result_type &get_value () const  { return (items_); }

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
         typename I = unsigned long,
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
    inline void operator() (const I &, const T &val1, const T &val2)  {

        if (is_nan__(val1) || is_nan__(val2))  return;

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

        const T b = T(1) ? b_ : T(0);

        return ((dot_prod_ - (total1_ * total2_) / T(cnt_)) / (T(cnt_) - b));
    }

    inline T get_var1 () const  {

        const T b = T(1) ? b_ : T(0);

        return((dot_prod1_ - (total1_ * total1_) / T(cnt_)) / (T(cnt_) - b));
    }
    inline T get_var2 () const  {

        const T b = T(1) ? b_ : T(0);

        return((dot_prod2_ - (total2_ * total2_) / T(cnt_)) / (T(cnt_) - b));
    }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct VarVisitor  {

private:

    CovVisitor<T, I>    cov_;

public:

    using value_type = T;

    explicit VarVisitor (bool bias = true) : cov_ (bias)  {   }
    inline void operator() (const I &idx, const T &val)  {

        cov_ (idx, val, val);
    }
    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline T get_value () const  { return (cov_.get_value()); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct BetaVisitor  {

private:

    CovVisitor<T, I>    cov_;

public:

    using value_type = T;

    explicit BetaVisitor (bool bias = true) : cov_ (bias)  {   }
    inline void
    operator() (const I &idx, const T &val1, const T &benchmark)  {

        cov_ (idx, val1, benchmark);
    }
    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline T get_value () const  {

        return (cov_.get_var2() != 0.0
                    ? cov_.get_value() / cov_.get_var2()
                    : std::numeric_limits<T>::quiet_NaN());
    }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StdVisitor   {

private:

    VarVisitor<T, I>    var_;

public:

    using value_type = T;

    explicit StdVisitor (bool bias = true) : var_ (bias)  {   }
    inline void operator() (const I &idx, const T &val)  {

        var_ (idx, val);
    }
    inline void pre ()  { var_.pre(); }
    inline void post ()  { var_.post(); }
    inline T get_value () const  { return (::sqrt(var_.get_value())); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct TrackingErrorVisitor {

private:

    StdVisitor<T, I>    std_;

public:

    using value_type = T;

    explicit TrackingErrorVisitor (bool bias = true) : std_ (bias) {  }
    inline void operator() (const I & idx, const T &val1, const T &val2)  {

        std_ (idx, val1 - val2);
    }
    inline void pre ()  { std_.pre(); }
    inline void post ()  { std_.post();  }
    inline T get_value () const  { return (std_.get_value()); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CorrVisitor  {

private:

    CovVisitor<T, I>    cov_;

public:

    using value_type = T;

    explicit CorrVisitor (bool bias = true) : cov_ (bias)  {   }
    inline void operator() (const I &idx, const T &val1, const T &val2)  {

        cov_ (idx, val1, val2);
    }
    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline T get_value () const  {

        return (cov_.get_value() /
                (::sqrt(cov_.get_var1()) * ::sqrt(cov_.get_var2())));
    }
};

// ----------------------------------------------------------------------------

//
// Single Action Visitors
//

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct AutoCorrVisitor  {

private:

    std::vector<T>  result_ {  };

    using CorrResult = std::pair<std::size_t, T>;

    inline CorrResult
    get_auto_corr_(std::size_t col_len,
                   std::size_t lag,
                   const std::vector<T> &column) const  {

        CorrVisitor<T, I>   corr {  };

        corr.pre();
        for (std::size_t i = 0; i < col_len - lag; ++i)
            corr (I(), column[i], column[i + lag]);

        return (CorrResult(lag, corr.get_value()));
    }

public:

    using value_type = T;

    AutoCorrVisitor () = default;
    inline void
    operator() (const std::vector<I> &idx, const std::vector<T> &column)  {

        const std::size_t   col_len = column.size();

        if (col_len <= 4)  return;

        std::vector<T>                        tmp_result(col_len - 4);
        std::size_t                           lag = 1;
        std::vector<std::future<CorrResult>>  futures(
            ThreadGranularity::get_thread_level());
        std::size_t                           thread_count = 0;

        tmp_result[0] = 1.0;
        while (lag < col_len - 4)  {
            if (thread_count >= ThreadGranularity::get_thread_level())  {
                const auto  result = get_auto_corr_(col_len, lag, column);

                tmp_result[result.first] = result.second;
            }
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &AutoCorrVisitor::get_auto_corr_,
                               this,
                               col_len,
                               lag,
                               std::cref(column));
                thread_count += 1;
            }
            lag += 1;
        }

        for (std::size_t i = 0; i < thread_count; ++i)  {
            const auto  &result = futures[i].get();

            tmp_result[result.first] = result.second;
        }
        tmp_result.swap(result_);
    }
    inline void pre ()  { result_.clear(); }
    inline void post ()  {  }
    inline const std::vector<T> &get_value () const  { return (result_); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct ReturnVisitor  {

private:

    std::vector<T>      result_ {  };
    const return_policy ret_p_;

public:

    using value_type = T;

    inline ReturnVisitor (return_policy rp) : ret_p_(rp)  {   }
    inline void
    operator() (const std::vector<I> &idx, const std::vector<T> &column)  {

        const std::size_t   col_len = column.size();

        if (col_len < 3)  return;

        std::vector<T>  tmp_result;

        tmp_result.reserve(col_len);

        if (ret_p_ == return_policy::log)  {
            auto    func =
                [](T lhs, T rhs) -> T  { return (::log(lhs / rhs)); };

            std::adjacent_difference (column.begin(), column.end(),
                                      std::back_inserter (tmp_result),
                                      func);
        }
        else if (ret_p_ == return_policy::percentage)  {
            auto    func =
            [](T lhs, T rhs) -> T  { return ((lhs - rhs) / rhs); };

            std::adjacent_difference (column.begin(), column.end(),
                                      std::back_inserter (tmp_result),
                                      func);
        }
        else if (ret_p_ == return_policy::monetary)  {
            auto    func =
            [](T lhs, T rhs) -> T  { return (lhs - rhs); };

            std::adjacent_difference (column.begin(), column.end(),
                                      std::back_inserter (tmp_result),
                                      func);
        }
        tmp_result.erase (tmp_result.begin ());
        tmp_result.swap(result_);
    }
    inline void pre ()  { result_.clear(); }
    inline void post ()  {   }
    inline const std::vector<T> &get_value () const  { return (result_); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct KthValueVisitor  {

private:

    T                   result_ {  };
    const std::size_t   kth_element_;

    inline T
    find_kth_element_ (typename std::vector<T>::const_iterator begin,
                       typename std::vector<T>::const_iterator end,
                       size_t k) const  {

        const std::size_t   vec_size = static_cast<size_t>(end - begin);

        if (k > vec_size || k <= 0)  {
            char    err[512];

            sprintf (err,
#ifdef _WIN32
                     "find_kth_element_(): vector length = %zu and k = %zu.",
#else
                     "find_kth_element_(): vector length = %lu and k = %lu.",
#endif // _WIN32
                     vec_size, k);
            throw NotFeasible (err);
        }

        std::vector<T>  tmp_vec (vec_size - 1);
        T               kth_value = *(begin + (vec_size / 2));
        std::size_t     less_count = 0;
        std::size_t     great_count = vec_size - 2;

        for (auto citer = begin; citer < end; ++citer)  {
            if (is_nan__(*citer))  continue;

            if (*citer < kth_value)
                tmp_vec [less_count++] = *citer;
            else if (*citer > kth_value)
                tmp_vec [great_count--] = *citer;
        }

        if (less_count > k - 1)
            return (find_kth_element_ (tmp_vec.begin (),
                                       tmp_vec.begin () + less_count,
                                       k));
        else if (less_count < k - 1)
            return (find_kth_element_ (tmp_vec.begin () + less_count,
                                       tmp_vec.end (),
                                       k - less_count - 1));
        else
            return (kth_value);
    }

public:

    using value_type = T;

    inline KthValueVisitor (std::size_t ke) : kth_element_(ke)  {   }
    inline void
    operator() (const std::vector<I> &idx, const std::vector<T> &column)  {

        result_ =
            find_kth_element_ (column.begin(), column.end(), kth_element_);
    }
    inline void pre ()  { result_ = T(); }
    inline void post ()  {   }
    inline const T get_value () const  { return (result_); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MedianVisitor  {

private:

    T   result_ {  };

public:

    using value_type = T;

    MedianVisitor () = default;
    inline void
    operator() (const std::vector<I> &idx, const std::vector<T> &column)  {

        const std::size_t       vec_size = column.size();
        KthValueVisitor<T, I>   kv_visitor (vec_size >> 1);


        kv_visitor.pre();
        kv_visitor(idx, column);
        result_ = kv_visitor.get_value();
        if (! (vec_size & 0x0001))  { // even
            KthValueVisitor<T, I>   kv_visitor2 ((vec_size >> 1) + 1);

            kv_visitor2.pre();
            kv_visitor2(idx, column);
            result_ = (result_ + kv_visitor2.get_value()) / T(2);
        }
    }
    inline void pre ()  { result_ = T(); }
    inline void post ()  {   }
    inline const T get_value () const  { return (result_); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DotProdVisitor  {

private:

    T dot_prod_ { T(0) };

public:

    using value_type = T;

    inline void operator() (const I &idx, const T &val1, const T &val2)  {

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
         typename I = unsigned long,
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

    inline void operator() (const I &idx, const T &val)  {

        if (is_nan__(val))  return;

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
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SLRegressionVisitor  {

private:

    std::size_t             n_ { 0 };

    // Sum of the product of the difference between x and its mean and
    // the difference between y and its mean.
    T                   s_xy_ { T(0) };
    StatsVisitor<T, I>  x_stats_;
    StatsVisitor<T, I>  y_stats_;

public:

    using value_type = T;

    inline void operator() (const I &idx, const T &x, const T &y)  {

        if (is_nan__(x) || is_nan__(y))  return;

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

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
