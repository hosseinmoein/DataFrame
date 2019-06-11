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

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &, const value_type &val)  {

        if (is_nan__(val))  return;

        mean_ += val;
        cnt_ +=1;
    }
    inline void pre ()  { mean_ = 0; cnt_ = 0; }
    inline void post ()  {  }
    inline count_type get_count () const  { return (cnt_); }
    inline value_type get_sum () const  { return (mean_); }
    inline result_type get_result () const  {

        return (mean_ / value_type(cnt_));
    }

private:

    value_type  mean_ { 0 };
    count_type  cnt_ { 0 };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SumVisitor {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &, const value_type &val)  {

        if (is_nan__(val))  return;

        sum_ += val;
    }
    inline void pre ()  { sum_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (sum_); }

private:

    value_type  sum_ { 0 };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MaxVisitor {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (is_nan__(val))  return;

        if (val > max_ || is_first) {
            max_ = val;
            index_ = idx;
            is_first = false;
        }
    }
    inline void pre ()  { is_first = true; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (max_); }
    inline index_type get_index () const  { return (index_); }

private:

    value_type  max_ { };
    index_type  index_ { };
    bool        is_first { true };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MinVisitor {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (is_nan__(val))  return;

        if (val < min_ || is_first) {
            min_ = val;
            index_ = idx;
            is_first = false;
        }
    }
    inline void pre ()  { is_first = true; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (min_); }
    inline index_type get_index () const  { return (index_); }

private:

    value_type  min_ { };
    index_type  index_ { };
    bool        is_first { true };
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

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;

    struct  DataItem  {
        value_type  value { };
        index_type  index { };
    };

    using result_type = std::array<DataItem, N>;

    inline void operator() (const index_type &idx, const value_type &val)  {

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
    inline const result_type &get_result () const  { return (items_); }

    inline void sort_by_index()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.index < rhs.index);
                  });
    }
    inline void sort_by_value()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.value < rhs.value);
                  });
    }

private:

    result_type items_ { };
    count_type  counter_ { 0 };
    int         min_index_ { -1 };
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

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;

    struct  DataItem  {
        value_type  value { };
        index_type  index { };
    };

    using result_type = std::array<DataItem, N>;

    inline void operator() (const index_type &idx, const value_type &val)  {

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
    inline const result_type &get_result () const  { return (items_); }

    inline void sort_by_index()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.index < rhs.index);
                  });
    }
    inline void sort_by_value()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.value < rhs.value);
                  });
    }

private:

    result_type items_ { };
    count_type  counter_ { 0 };
    int         max_index_ { -1 };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CovVisitor {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    explicit CovVisitor (bool bias = true) : b_ (bias) {  }
    inline void operator() (const index_type &,
                            const value_type &val1,
                            const value_type &val2)  {

        if (is_nan__(val1) || is_nan__(val2))  return;

        total1_ += val1;
        total2_ += val2;
        dot_prod_ += (val1 * val2);
        dot_prod1_ += (val1 * val1);
        dot_prod2_ += (val2 * val2);
        cnt_ += 1;
    }
    inline void pre ()  {

        total1_ = total2_ = dot_prod_ = dot_prod1_ = dot_prod2_ = 0;
        cnt_ = 0;
    }
    inline void post ()  {  }
    inline result_type get_result () const  {

        const value_type    b = 1 ? b_ : 0;

        return ((dot_prod_ - (total1_ * total2_) / value_type(cnt_)) /
                (value_type(cnt_) - b));
    }

    inline value_type get_var1 () const  {

        const value_type    b = 1 ? b_ : 0;

        return ((dot_prod1_ - (total1_ * total1_) / value_type(cnt_)) /
                (value_type(cnt_) - b));
    }
    inline value_type get_var2 () const  {

        const value_type    b = 1 ? b_ : 0;

        return ((dot_prod2_ - (total2_ * total2_) / value_type(cnt_)) /
                (value_type(cnt_) - b));
    }

private:

    value_type  total1_ { 0 };
    value_type  total2_ { 0 };
    value_type  dot_prod_ { 0 };
    value_type  dot_prod1_ { 0 };
    value_type  dot_prod2_ { 0 };
    count_type  cnt_ { 0 };
    const bool  b_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct VarVisitor  {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    explicit VarVisitor (bool bias = true) : cov_ (bias)  {   }
    inline void operator() (const index_type &idx, const value_type &val)  {

        cov_ (idx, val, val);
    }
    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline result_type get_result () const  { return (cov_.get_result()); }

private:

    CovVisitor<value_type, index_type>  cov_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct BetaVisitor  {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    explicit BetaVisitor (bool bias = true) : cov_ (bias)  {   }
    inline void operator() (const index_type &idx,
                            const value_type &val1,
                            const value_type &benchmark)  {

        cov_ (idx, val1, benchmark);
    }
    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline result_type get_result () const  {

        return (cov_.get_var2() != 0.0
                    ? cov_.get_result() / cov_.get_var2()
                    : std::numeric_limits<value_type>::quiet_NaN());
    }

private:

    CovVisitor<value_type, index_type>  cov_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StdVisitor   {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    explicit StdVisitor (bool bias = true) : var_ (bias)  {   }
    inline void operator() (const index_type &idx, const value_type &val)  {

        var_ (idx, val);
    }
    inline void pre ()  { var_.pre(); }
    inline void post ()  { var_.post(); }
    inline result_type get_result () const  {

        return (::sqrt(var_.get_result()));
    }

private:

    VarVisitor<value_type, index_type>  var_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct TrackingErrorVisitor {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    explicit TrackingErrorVisitor (bool bias = true) : std_ (bias) {  }
    inline void operator() (const index_type &idx,
                            const value_type &val1,
                            const value_type &val2)  {

        std_ (idx, val1 - val2);
    }
    inline void pre ()  { std_.pre(); }
    inline void post ()  { std_.post();  }
    inline result_type get_result () const  { return (std_.get_result()); }

private:

    StdVisitor<value_type, index_type>  std_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CorrVisitor  {

public:

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    explicit CorrVisitor (bool bias = true) : cov_ (bias)  {   }
    inline void operator() (const index_type &idx,
                            const value_type &val1,
                            const value_type &val2)  {

        cov_ (idx, val1, val2);
    }
    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline result_type get_result () const  {

        return (cov_.get_result() /
                (::sqrt(cov_.get_var1()) * ::sqrt(cov_.get_var2())));
    }

private:

    CovVisitor<value_type, index_type>  cov_;
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

public:

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = std::vector<value_type>;

    AutoCorrVisitor () = default;
    inline void
    operator() (const std::vector<index_type> &idx,
                const std::vector<value_type> &column)  {

        const count_type    col_len = column.size();

        if (col_len <= 4)  return;

        std::vector<value_type>               tmp_result(col_len - 4);
        count_type                            lag = 1;
        std::vector<std::future<CorrResult>>  futures(
            ThreadGranularity::get_thread_level());
        count_type                            thread_count = 0;

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

        for (count_type i = 0; i < thread_count; ++i)  {
            const auto  &result = futures[i].get();

            tmp_result[result.first] = result.second;
        }
        tmp_result.swap(result_);
    }
    inline void pre ()  { result_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }

private:

    result_type result_ {  };

    using CorrResult = std::pair<count_type, value_type>;

    inline CorrResult
    get_auto_corr_(count_type col_len,
                   count_type lag,
                   const std::vector<value_type> &column) const  {

        CorrVisitor<value_type, index_type> corr {  };

        corr.pre();
        for (count_type i = 0; i < col_len - lag; ++i)
            corr (I(), column[i], column[i + lag]);

        return (CorrResult(lag, corr.get_result()));
    }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct ReturnVisitor  {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = std::vector<value_type>;

    inline ReturnVisitor (return_policy rp) : ret_p_(rp)  {   }
    inline void operator() (const std::vector<index_type> &idx,
                            const std::vector<value_type> &column)  {

        const count_type    col_len = column.size();

        if (col_len < 3)  return;

        std::vector<value_type> tmp_result;

        tmp_result.reserve(col_len);

        if (ret_p_ == return_policy::log)  {
            auto    func =
                [](value_type lhs, value_type rhs) -> value_type  {
                    return (::log(lhs / rhs));
                };

            std::adjacent_difference (column.begin(), column.end(),
                                      std::back_inserter (tmp_result),
                                      func);
        }
        else if (ret_p_ == return_policy::percentage)  {
            auto    func =
            [](value_type lhs, value_type rhs) -> value_type  {
                return ((lhs - rhs) / rhs);
            };

            std::adjacent_difference (column.begin(), column.end(),
                                      std::back_inserter (tmp_result),
                                      func);
        }
        else if (ret_p_ == return_policy::monetary)  {
            auto    func =
            [](value_type lhs, value_type rhs) -> value_type  {
                return (lhs - rhs);
            };

            std::adjacent_difference (column.begin(), column.end(),
                                      std::back_inserter (tmp_result),
                                      func);
        }
        tmp_result.erase (tmp_result.begin ());
        tmp_result.swap(result_);
    }
    inline void pre ()  { result_.clear(); }
    inline void post ()  {   }
    inline const result_type &get_result () const  { return (result_); }

private:

    result_type         result_ {  };
    const return_policy ret_p_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct KthValueVisitor  {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    inline KthValueVisitor (count_type ke) : kth_element_(ke)  {   }
    inline void
    operator() (const std::vector<index_type> &idx,
                const std::vector<value_type> &column)  {

        result_ =
            find_kth_element_ (column.begin(), column.end(), kth_element_);
    }
    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

private:

    result_type         result_ {  };
    const count_type    kth_element_;

    inline value_type
    find_kth_element_ (typename std::vector<value_type>::const_iterator begin,
                       typename std::vector<value_type>::const_iterator end,
                       count_type k) const  {

        const count_type    vec_size = static_cast<count_type>(end - begin);

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

        std::vector<value_type> tmp_vec (vec_size - 1);
        value_type              kth_value = *(begin + (vec_size / 2));
        count_type              less_count = 0;
        count_type              great_count = vec_size - 2;

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
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MedianVisitor  {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    MedianVisitor () = default;
    inline void
    operator() (const std::vector<index_type> &idx,
                const std::vector<value_type> &column)  {

        const count_type                        vec_size = column.size();
        KthValueVisitor<value_type, index_type> kv_visitor (vec_size >> 1);


        kv_visitor.pre();
        kv_visitor(idx, column);
        result_ = kv_visitor.get_result();
        if (! (vec_size & 0x0001))  { // even
            KthValueVisitor<value_type, I>   kv_visitor2 ((vec_size >> 1) + 1);

            kv_visitor2.pre();
            kv_visitor2(idx, column);
            result_ = (result_ + kv_visitor2.get_result()) / value_type(2);
        }
    }
    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

private:

    result_type result_ {  };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DotProdVisitor  {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &idx,
                            const value_type &val1,
                            const value_type &val2)  {

        dot_prod_ += (val1 * val2);
    }
    inline void pre ()  { dot_prod_ = value_type(0); }
    inline void pro ()  {  }
    inline result_type get_result () const  { return (dot_prod_); }

private:

    result_type dot_prod_ { 0 };
};

// ----------------------------------------------------------------------------

// One-pass stats calculation.
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StatsVisitor  {

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (is_nan__(val))  return;

        value_type  delta, delta_n, delta_n2, term1;
        count_type  n1 = n_;

        n_ += 1;
        delta = val - m1_;
        delta_n = delta / value_type(n_);
        delta_n2 = delta_n * delta_n;
        term1 = delta * delta_n * value_type(n1);
        m1_ += delta_n;
        m4_ += term1 * delta_n2 * value_type(n_ * n_ - 3 * n_ + 3) +
               6.0 * delta_n2 * m2_ -
               4.0 * delta_n * m3_;
        m3_ += term1 * delta_n * value_type(n_ - 2) - 3.0 * delta_n * m2_;
        m2_ += term1;
    }
    inline void pre ()  {

        n_ = 0;
        m1_ = m2_ = m3_ = m4_ = value_type(0);
    }
    inline void post ()  {  }

    inline count_type get_count () const { return (n_); }
    inline result_type get_mean () const  { return (m1_); }
    inline result_type
    get_variance () const  { return (m2_ / (value_type(n_) - 1.0)); }
    inline result_type get_std () const  { return (::sqrt(get_variance())); }
    inline result_type get_skew () const  {

        return (::sqrt(n_) * m3_ / ::pow(m2_, 1.5));
    }
    inline result_type get_kurtosis () const  {

        return (value_type(n_) * m4_ / (m2_ * m2_) - 3.0);
    }

private:

    count_type  n_ { 0 };
    value_type  m1_ { 0 };
    value_type  m2_ { 0 };
    value_type  m3_ { 0 };
    value_type  m4_ { 0 };
};

// ----------------------------------------------------------------------------

// One pass simple linear regression
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SLRegressionVisitor  {

public:

    using value_type = T;
    using index_type = I;
    using count_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &idx,
                            const value_type &x,
                            const value_type &y)  {

        if (is_nan__(x) || is_nan__(y))  return;

        s_xy_ += (x_stats_.get_mean() - x) *
                 (y_stats_.get_mean() - y) *
                 value_type(n_) / value_type(n_ + 1);

        x_stats_(idx, x);
        y_stats_(idx, y);
        n_ += 1;
    }
    inline void pre ()  {

        n_ = 0;
        s_xy_ = 0;
        x_stats_.pre();
        y_stats_.pre();
    }
    inline void post ()  {  }

    inline count_type get_count () const { return (n_); }
    inline result_type get_slope () const  {

        // Sum of the squares of the difference between each x and
        // the mean x value.
        const value_type    s_xx =
            x_stats_.get_variance() * value_type(n_ - 1);

        return (s_xy_ / s_xx);
    }
    inline result_type get_intercept () const  {

        return (y_stats_.get_mean() - get_slope() * x_stats_.get_mean());
    }
    inline result_type get_corr () const  {

        const value_type    t = x_stats_.get_std() * y_stats_.get_std();

        return (s_xy_ / (value_type(n_ - 1) * t));
    }

private:

    count_type                              n_ { 0 };

    // Sum of the product of the difference between x and its mean and
    // the difference between y and its mean.
    value_type                              s_xy_ { 0 };
    StatsVisitor<value_type, index_type>    x_stats_;
    StatsVisitor<value_type, index_type>    y_stats_;
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
