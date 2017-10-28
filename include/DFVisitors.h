// Hossein Moein
// September 22, 2017

#pragma once

#include <DataFrame.h>
#include <limits>
#include <type_traits>
#include <cmath>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, typename TS_T = unsigned long>
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
    inline void reset ()  { mean_ = T(0); cnt_ = 0; }
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
    inline void reset ()  { sum_ = T(0); }
    inline T get_value () const  { return (sum_); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MaxVisitor {

private:

    T   max_ { std::numeric_limits<T>::min() };

public:

    using value_type = T;

    inline void operator() (const TS_T &, const T &val)  {

        if (val > max_)  max_ = val;
    }
    inline void reset ()  { max_ = std::numeric_limits<T>::min(); }
    inline T get_value () const  { return (max_); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MinVisitor {

private:

    T   min_ { std::numeric_limits<T>::max() };

public:

    using value_type = T;

    inline void operator() (const TS_T &, const T &val)  {

        if (val < min_)  min_ = val;
    }
    inline void reset ()  { min_ = std::numeric_limits<T>::max(); }
    inline T get_value () const  { return (min_); }
};

// ----------------------------------------------------------------------------

template<typename T,
         typename TS_T = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CovVisitor {

private:

    T                   total1_ { T(0) };
    T                   total2_ { T(0) };
    T                   dot_prod_ { T(0) };
    T                   dot_prod1_ { T(0) };
    T                   dot_prod2_ { T(0) };
    std::size_t         cnt_ { 0 };
    const unsigned char b_;

public:

    using value_type = T;

    explicit CovVisitor (std::size_t bias = 1) : b_ (bias) {  }
    inline void operator() (const TS_T &, const T &val1, const T &val2)  {

        total1_ += val1;
        total2_ += val2;
        dot_prod_ += (val1 * val2);
        dot_prod1_ += (val1 * val1);
        dot_prod2_ += (val2 * val2);
        cnt_ += 1;
    }
    inline void reset ()  {

        total1_ = total2_ = dot_prod_ = dot_prod1_ = dot_prod2_ = T(0);
        cnt_ = 0;
    }
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
    inline void reset ()  { cov_.reset(); }
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
    inline void reset ()  { var_.reset(); }
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
    inline void reset ()  { cov_.reset(); }
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
    inline void reset ()  { dot_prod_ = T(0); }
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
    inline void reset ()  {

        n_ = 0;
        m1_ = m2_ = m3_ = m4_ = T(0);
    }

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
    inline void reset ()  {

        n_ = 0;
        s_xy_ = T(0);
        x_stats_.reset();
        y_stats_.reset();
    }

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
