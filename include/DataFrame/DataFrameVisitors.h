// Hossein Moein
// September 22, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <DataFrame/DataFrameTypes.h>

#include <algorithm>
#include <cstddef>
#include <numeric>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MeanVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        mean_ += val;
        cnt_ +=1;
    }
    inline void pre ()  { mean_ = 0; cnt_ = 0; }
    inline void post ()  {  }
    inline size_type get_count () const  { return (cnt_); }
    inline value_type get_sum () const  { return (mean_); }
    inline result_type get_result () const  {

        return (mean_ / value_type(cnt_));
    }

    explicit MeanVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  mean_ { 0 };
    size_type   cnt_ { 0 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SumVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        sum_ += val;
    }
    inline void pre ()  { sum_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (sum_); }

    explicit SumVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  sum_ { 0 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct ProdVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        prod_ *= val;
    }
    inline void pre ()  { prod_ = 1; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (prod_); }

    explicit ProdVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  prod_ { 1 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MaxVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (is_nan__(val))  {
            if (skip_nan_)  return;
            else  {
                max_ = std::numeric_limits<value_type>::quiet_NaN();
                is_first = false;
            }
        }

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

    explicit MaxVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  max_ { };
    index_type  index_ { };
    bool        is_first { true };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MinVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (is_nan__(val))  {
            if (skip_nan_)  return;
            else  {
                min_ = std::numeric_limits<value_type>::quiet_NaN();
                is_first = false;
            }
        }

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

    explicit MinVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  min_ { };
    index_type  index_ { };
    bool        is_first { true };
    const bool  skip_nan_ { };
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
    using size_type = std::size_t;

    struct  DataItem  {
        value_type  value { };
        index_type  index { };
    };

    using result_type = std::array<DataItem, N>;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

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

    explicit NLargestVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    result_type items_ { };
    size_type   counter_ { 0 };
    int         min_index_ { -1 };
    const bool  skip_nan_ { };
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
    using size_type = std::size_t;

    struct  DataItem  {
        value_type  value { };
        index_type  index { };
    };

    using result_type = std::array<DataItem, N>;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

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

    explicit NSmallestVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    result_type items_ { };
    size_type   counter_ { 0 };
    int         max_index_ { -1 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CovVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = T;

    explicit CovVisitor (bool bias = true, bool skipnan = true)
        : b_ (bias), skip_nan_(skipnan) {  }
    inline void operator() (const index_type &,
                            const value_type &val1,
                            const value_type &val2)  {

        if (skip_nan_ && (is_nan__(val1) || is_nan__(val2)))  return;

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
    size_type   cnt_ { 0 };
    const bool  b_;
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct VarVisitor  {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
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
    using size_type = std::size_t;
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
    using size_type = std::size_t;
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
    using size_type = std::size_t;
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
    using size_type = std::size_t;
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

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DotProdVisitor  {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &,
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

template<typename F, typename T, typename I = unsigned long>
struct SimpleRollAdopter {

private:

    using functor_type = F;
    using f_result_type = typename functor_type::result_type;

    functor_type                functor_ { };
    const size_t                roll_count_ { 0 };
    std::vector<f_result_type>  result_ { };

public:

    using value_type = T;
    using index_type = I;
    using result_type = std::vector<f_result_type>;

    inline SimpleRollAdopter(F &&functor, size_t r_count)
        : functor_(std::move(functor)), roll_count_(r_count)  {   }

    inline void
    operator() (const std::vector<index_type> &idx,
                const std::vector<value_type> &column)  {

        if (roll_count_ == 0)  return;

        const size_t    col_s = column.size();

        result_.reserve(col_s);

        for (size_t i = 0; i < roll_count_ - 1 && i < col_s; ++i)
            result_.push_back(std::numeric_limits<f_result_type>::quiet_NaN());
        for (size_t i = 0; i < col_s; ++i)  {
            size_t  r = 0;

            functor_.pre();
            for (size_t j = i; r < roll_count_ && j < col_s; ++j, ++r)
                functor_(idx[j], column[j]);
            if (r == roll_count_)
                result_.push_back(functor_.get_result());
            functor_.post();
        }

        return;
    }

    inline void pre ()  { functor_.pre(); result_.clear(); }
    inline void post ()  { functor_.post(); }
    inline const result_type &get_result () const  { return (result_); }
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
    using size_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        value_type  delta, delta_n, delta_n2, term1;
        size_type   n1 = n_;

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

    inline size_type get_count () const { return (n_); }
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

    explicit StatsVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    size_type   n_ { 0 };
    value_type  m1_ { 0 };
    value_type  m2_ { 0 };
    value_type  m3_ { 0 };
    value_type  m4_ { 0 };
    const bool  skip_nan_ { };
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
    using size_type = std::size_t;
    using result_type = T;

    inline void operator() (const index_type &idx,
                            const value_type &x,
                            const value_type &y)  {

        if (skip_nan_ && (is_nan__(x) || is_nan__(y)))  return;

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

    inline size_type get_count () const { return (n_); }
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

    explicit SLRegressionVisitor(bool skipnan = true)
        : x_stats_(skipnan), y_stats_(skipnan), skip_nan_(skipnan)  {   }

private:

    size_type                               n_ { 0 };

    // Sum of the product of the difference between x and its mean and
    // the difference between y and its mean.
    value_type                              s_xy_ { 0 };
    StatsVisitor<value_type, index_type>    x_stats_ {  };
    StatsVisitor<value_type, index_type>    y_stats_ {  };
    const bool                              skip_nan_ { };
};

// ----------------------------------------------------------------------------

//
// Single Action Visitors
//

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CumSumVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    inline void
    operator() (const std::vector<index_type> &,
                const std::vector<value_type> &column)  {

        value_type  running_sum = 0;

        sum_.reserve(column.size());
        for (const auto citer : column)  {
            if (! skip_nan_ || ! is_nan__(citer))  {
                running_sum += citer;
                sum_.push_back(running_sum);
            }
            else
                sum_.push_back(citer);
        }
    }
    inline void pre ()  { sum_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (sum_); }

    explicit CumSumVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    std::vector<value_type> sum_ {  };
    const bool              skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CumProdVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    inline void
    operator() (const std::vector<index_type> &,
                const std::vector<value_type> &column)  {

        value_type  running_prod = 1;

        prod_.reserve(column.size());
        for (const auto citer : column)  {
            if (! skip_nan_ || ! is_nan__(citer))  {
                running_prod *= citer;
                prod_.push_back(running_prod);
            }
            else
                prod_.push_back(citer);
        }
    }
    inline void pre ()  { prod_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (prod_); }

    explicit CumProdVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    std::vector<value_type> prod_ {  };
    const bool              skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct CumMaxVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    inline void
    operator() (const std::vector<index_type> &,
                const std::vector<value_type> &column)  {

        if (column.empty())  return;

        value_type  running_max = column[0];

        max_.reserve(column.size());
        for (const auto citer : column)  {
            if (! skip_nan_ || ! is_nan__(citer))  {
                if (citer > running_max)
                    running_max = citer;
                max_.push_back(running_max);
            }
            else
                max_.push_back(citer);
        }
    }
    inline void pre ()  { max_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (max_); }

    explicit CumMaxVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    std::vector<value_type> max_ {  };
    const bool              skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct CumMinVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    inline void
    operator() (const std::vector<index_type> &,
                const std::vector<value_type> &column)  {

        if (column.empty())  return;

        value_type  running_min = column[0];

        min_.reserve(column.size());
        for (const auto citer : column)  {
            if (! skip_nan_ || ! is_nan__(citer))  {
                if (citer < running_min)
                    running_min = citer;
                min_.push_back(running_min);
            }
            else
                min_.push_back(citer);
        }
    }
    inline void pre ()  { min_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (min_); }

    explicit CumMinVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    std::vector<value_type> min_ {  };
    const bool              skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct AutoCorrVisitor  {

public:

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    AutoCorrVisitor () = default;
    inline void
    operator() (const std::vector<index_type> &,
                const std::vector<value_type> &column)  {

        const size_type col_len = column.size();

        if (col_len <= 4)  return;

        std::vector<value_type>               tmp_result(col_len - 4);
        size_type                             lag = 1;
        std::vector<std::future<CorrResult>>  futures(
            ThreadGranularity::get_thread_level());
        size_type                             thread_count = 0;

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

        for (size_type i = 0; i < thread_count; ++i)  {
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

    using CorrResult = std::pair<size_type, value_type>;

    inline CorrResult
    get_auto_corr_(size_type col_len,
                   size_type lag,
                   const std::vector<value_type> &column) const  {

        CorrVisitor<value_type, index_type> corr {  };

        corr.pre();
        for (size_type i = 0; i < col_len - lag; ++i)
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
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    explicit ReturnVisitor (return_policy rp) : ret_p_(rp)  {   }
    inline void operator() (const std::vector<index_type> &,
                            const std::vector<value_type> &column)  {

        const size_type col_len = column.size();

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
    using size_type = std::size_t;
    using result_type = T;

    explicit KthValueVisitor (size_type ke, bool skipnan = true)
        : kth_element_(ke), skip_nan_(skipnan)  {   }
    inline void
    operator() (const std::vector<index_type> &,
                const std::vector<value_type> &column)  {

        result_ =
            find_kth_element_ (column.begin(), column.end(), kth_element_);
    }
    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

private:

    result_type     result_ {  };
    const size_type kth_element_;
    const bool      skip_nan_ { };

    inline value_type
    find_kth_element_ (typename std::vector<value_type>::const_iterator begin,
                       typename std::vector<value_type>::const_iterator end,
                       size_type k) const  {

        const size_type vec_size = static_cast<size_type>(end - begin);

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
        size_type               less_count = 0;
        size_type               great_count = vec_size - 2;

        for (auto citer = begin; citer < end; ++citer)  {
            if (skip_nan_ && is_nan__(*citer))  continue;

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
    using size_type = std::size_t;
    using result_type = T;

    MedianVisitor () = default;
    inline void
    operator() (const std::vector<index_type> &idx,
                const std::vector<value_type> &column)  {

        const size_type                         vec_size = column.size();
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

// Mode of a vector is a value that appears most often in the vector.
// This visitor extracts the top N repeated values in the column with the
// associated indices.
// The T type must be hashable
// Because of the information this has to return, it is not a cheap operation
//
template<std::size_t N, typename T, typename I = unsigned long>
struct  ModeVisitor {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;

    struct  DataItem  {
        // Value of the column item
        value_type              value { };
        // List of indices where value occurred
        std::vector<index_type> indices { };
        // Number of times value occurred
        inline size_type repeat_count() const  { return (indices.size()); }
        // List of column indices where value occurred
        std::vector<size_type>  value_indices_in_col {  };

        DataItem() = default;
        inline DataItem(value_type v) : value(v)  {
            indices.reserve(4);
            value_indices_in_col.reserve(4);
        }
    };

    using result_type = std::array<DataItem, N>;

    inline void
    operator() (const std::vector<index_type> &idx,
                const std::vector<value_type> &column)  {

        DataItem                                    nan_item(
            std::numeric_limits<value_type>::quiet_NaN());
        const size_type                             col_size = column.size();
        std::unordered_map<value_type, DataItem>    val_map (col_size);

        for (size_type i = 0; i < col_size; ++i)  {
            if (is_nan__(column[i]))  {
                nan_item.indices.push_back(idx[i]);
                nan_item.value_indices_in_col.push_back(i);
            }
            else  {
                auto    ret =
                    val_map.emplace(
                        std::pair<value_type, DataItem>(
                            column[i], DataItem(column[i])));

                ret.first->second.indices.push_back(idx[i]);
                ret.first->second.value_indices_in_col.push_back(i);
            }
        }

        std::vector<DataItem> val_vec;

        val_vec.reserve(val_map.size() + 1);
        val_vec.push_back(nan_item);
        std::for_each(val_map.begin(),
                      val_map.end(),
                      [&val_vec](const auto &map_pair) -> void {
                          val_vec.push_back(map_pair.second);
                      });
        std::sort(val_vec.begin(), val_vec.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.repeat_count() > rhs.repeat_count()); // dec
                  });
        for (size_type i = 0; i < N && i < val_vec.size(); ++i)
            items_[i] = val_vec[i];
    }
    inline void pre ()  { result_type x; items_.swap (x); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (items_); }

    inline void sort_by_repeat_count()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.repeat_count() < rhs.repeat_count());
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
};


// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DiffVisitor  {

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::vector<value_type>;

    explicit DiffVisitor(long periods = 1, bool skipnan = true)
        : periods_(periods), skip_nan_(skipnan) {  }

    inline void operator() (const std::vector<index_type> &,
                            const std::vector<value_type> &column)  {

        const size_type col_len = column.size();

        result_.reserve(col_len);
        if (periods_ >= 0)  {
            if (! skip_nan_)  {
                for (long i = 0;
                     i < periods_ && static_cast<size_type>(i) < col_len; ++i)
                    result_.push_back(
                        std::numeric_limits<value_type>::quiet_NaN());
            }
            for (auto i = column.begin() + periods_, j = column.begin();
                 i < column.end(); ++i, ++j) {
                if (skip_nan_ && (is_nan__(*i) || is_nan__(*j)))  continue;
                result_.push_back(*i - *j);
            }
        }
        else {
            for (auto i = column.rbegin() + std::abs(periods_),
                      j = column.rbegin();
                 i < column.rend(); ++i, ++j) {
                if (skip_nan_ && (is_nan__(*i) || is_nan__(*j)))  continue;
                result_.insert(result_.begin(), (*i - *j));
            }

            if (skip_nan_ )  return;
            for (size_type i = 0;
                 i < static_cast<size_type>(std::abs(periods_)) && i < col_len;
                 ++i)
                result_.push_back(
                    std::numeric_limits<value_type>::quiet_NaN());
        }
    }

    inline void pre ()  { result_.clear(); }
    inline void post ()  {   }
    inline const result_type &get_result () const  { return (result_); }

private:

    result_type result_ { };
    const long  periods_ { };
    const bool  skip_nan_ { };
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
