// Hossein Moein
// July 16, 2019
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

#include <DataFrame/Vectors/HeteroVector.h>

#include <cmath>
#include <complex>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <type_traits>

// ----------------------------------------------------------------------------

namespace hmdf
{

#if defined(_WIN32) || defined(_WIN64)
#  ifdef min
#    undef min
#  endif // min
#  ifdef max
#    undef max
#  endif // max
#endif // _WIN32 || _WIN64

// ----------------------------------------------------------------------------

// Generic DataFrame error
//
struct DataFrameError : public std::runtime_error  {

    DataFrameError (const char *desc) : std::runtime_error (desc)  {   }
};

// Column does not exist error
//
struct ColNotFound : public DataFrameError  {

    ColNotFound (const char *desc) : DataFrameError (desc)  {   }
};

// Something in DataFrame/operation is not proper error. For example, trying
// to load a column with more data than there is in index
//
struct InconsistentData : public DataFrameError  {

    InconsistentData (const char *desc) : DataFrameError (desc)  {   }
};

// Bad data range specification error
//
struct BadRange : public DataFrameError  {

    BadRange (const char *desc) : DataFrameError (desc)  {   }
};

// The operation is not feasible error. For example, trying to do interpolation
// on non-arithmetic types
//
struct NotFeasible : public DataFrameError  {

    NotFeasible (const char *desc) : DataFrameError (desc)  {   }
};

// The functionality is not implemented error
//
struct NotImplemented : public DataFrameError  {

    NotImplemented (const char *desc) : DataFrameError (desc)  {   }
};

// ----------------------------------------------------------------------------

inline constexpr const char *DF_INDEX_COL_NAME = "INDEX";

// ----------------------------------------------------------------------------

enum class nan_policy : bool  {
    pad_with_nans = true,
    dont_pad_with_nans = false,
};

// ----------------------------------------------------------------------------

enum class sort_state : bool  {
    sorted = true,
    not_sorted = false,
};

// ----------------------------------------------------------------------------

enum class sort_spec : unsigned char  {
    ascen = 1,
    desce = 2,
};

// ----------------------------------------------------------------------------

enum class join_policy : unsigned char  {
    inner_join = 1,
    left_join = 2,
    right_join = 3,
    left_right_join = 4,  // This is merge
};

// ----------------------------------------------------------------------------

enum class concat_policy : unsigned char  {
    common_columns = 1,
    all_columns = 2,
    lhs_and_common_columns = 3,
};

// ----------------------------------------------------------------------------

// This policy is relative to a tabular data structure
//
enum class shift_policy : unsigned char  {
    // Shift/rotate the content of all columns down, keep index unchanged
    //
    down = 1,
    // Shift/rotate the content of all columns up, keep index unchanged
    //
    up = 2,
    // Shift/rotate the columns to the left
    //
    left = 3,
    // Shift/rotate the columns to the right
    //
    right = 4,
};

// ----------------------------------------------------------------------------

enum class fill_policy : unsigned char  {

    // Fill all missing values with the given substitute
    //
    value = 1,

    // Fill the missing values with the previous value
    //
    fill_forward = 2,

    // Fill the missing values with the next value
    //
    fill_backward = 3,

    //           X - X1
    // Y = Y1 + --------- * (Y2 - Y1)
    //           X2 - X1
    //
    linear_interpolate = 4,  // Using the index as X coordinate
    linear_extrapolate = 5,  // Using the index as X coordinate

    // Fill missing values with mid-point of surrounding values
    //
    mid_point = 6,
};

// ----------------------------------------------------------------------------

enum class quantile_policy : unsigned char  {
    lower_value = 1,   // Take the higher index
    higher_value = 2,  // Take the lower index
    mid_point = 3,     // Average the two quantiles
    linear = 4,        // Linearly combine the two quantiles
};

// ----------------------------------------------------------------------------

enum class drop_policy : unsigned char  {
    all = 1,       // Remove row if all columns are nan
    any = 2,       // Remove row if any column is nan
    threshold = 3, // Remove row if threshold number of columns are nan
};

// ----------------------------------------------------------------------------

enum class mean_type : unsigned char  {

    arithmetic = 1, // sum(value) / count
    weighted = 2,   // sum(weighted value) / ((count * (count + 1)) / 2)
    geometric = 3,  // exp(log(sum(value)) / count)
    harmonic = 4,   // count / sum(1 / value)
    quadratic = 5,  // sqrt(sum(value * value) / count)
};

// ----------------------------------------------------------------------------

enum class hampel_type : unsigned char  {
    mean = 1,   // Use mean absolute deviation
    median = 2, // Use median absolute deviation
};

// ----------------------------------------------------------------------------

enum class remove_dup_spec : unsigned char  {
    keep_first = 1,  // Keep the first duplicated row
    keep_last = 2,   // Keep the last duplicated row
    keep_none = 3,   // Discard all duplicated rows
};

// ----------------------------------------------------------------------------

enum class exponential_decay_spec : unsigned char  {
    center_of_gravity = 1, // decay = 1 / (1 + value), for value >= 0
    span = 2,              // decay = 2 / (1 + value), for value >= 1
    halflife = 3,          // decay = 1 − exp(log(0.5) / value), for value > 0
    fixed = 4,             // decay = value, for 0 < value <= 1
};

// ----------------------------------------------------------------------------

// It defines different ways of calculating averages around averages,
// in other words different types of Mean Absolute Deviation.
//
enum class mad_type : unsigned char  {
    mean_abs_dev_around_mean = 1,
    mean_abs_dev_around_median = 2,
    median_abs_dev_around_mean = 3,
    median_abs_dev_around_median = 4,
};

// ----------------------------------------------------------------------------

enum class io_format : unsigned char  {
    csv = 1,   // DataFrame specific csv format
    csv2 = 2,  // Regular csv format (similar to Pandas)
    json = 3,
    hdf5 = 4,
    binary = 5,
};

// ----------------------------------------------------------------------------

enum class time_frequency : unsigned char  {
    annual = 1,
    monthly = 2,
    weekly = 3,
    daily = 4,
    hourly = 5,
    minutely = 6,
    secondly = 7,
    millisecondly = 8,
    // microsecondly = 9,
    // nanosecondly = 10,
};

// ----------------------------------------------------------------------------

enum class return_policy : unsigned char  {
    log = 1,         // log(t1 / t0)
    percentage = 2,  // (t1 - t0) / t0
    monetary = 3,    // t1 - t0
    trinary = 4,     // 1 if t1 - t0 > 0, -1 if t1 - t0 < 0, else 0
};

// ----------------------------------------------------------------------------

// Specification for calling get_[data|view]_by_rand()
// Number of rows means the n parameter is an positive integer specifying
// the number of rows to select
// Fraction of rows means the n parameter is a positive real number [0:1]
// specifying a fraction of rows to select
//
enum class random_policy : unsigned char  {
    num_rows_with_seed = 1,  // Number of rows with specifying a seed
    num_rows_no_seed = 2,    // Number of rows with no seed specification
    frac_rows_with_seed = 3, // Fraction of rows with specifying a seed
    frac_rows_no_seed = 4,   // Fraction of rows with no seed specification
};

// ----------------------------------------------------------------------------

enum class pattern_spec : unsigned char  {
    monotonic_increasing = 1,           // i >= j
    strictly_monotonic_increasing = 2,  // i > j
    monotonic_decreasing = 3,           // i <= j
    strictly_monotonic_decreasing = 4,  // i < j
    normally_distributed = 6,  // 68%-95%-99.7% 3-sigma rule to approximate
    standard_normally_distributed = 7,  // Same as normal + 0/1.0 check
    lognormally_distributed = 8, // Uses the above normal test on the log(x)
};

// ----------------------------------------------------------------------------

// Specification for RankVisitor
// This specifies how to assign ranks to members of a column
//
enum class rank_policy : unsigned char  {
    average = 1,  // Average of ranks, if an item is repeated
    first = 2,    // First rank, if an item is repeated
    last = 3,     // Last rank, if an item is repeated
    actual = 4,   // The actual rank of each item based on stable sort
};

// ----------------------------------------------------------------------------

// Different types of Sigmoid functions. For now, there is no integration
//
enum class sigmoid_type : unsigned char  {
    logistic = 1,        // f(x) = 1 / (1 + exp(-x))
    algebraic = 2,       // f(x) = 1 / sqrt(1 + pow(x, 2.0))
    // f(x) = [(exp(x) - exp(-x)) / (exp(x) + exp(-x))] = tanh(x)
    hyperbolic_tan = 3,
    arc_tan = 4,         // f(x) = atan(x)
    error_function = 5,  // f(x) = 2/sqrt(pi) * ∫exp(-pow(t, 2) dt
    gudermannian = 6,    // f(x) = atan(sinh(x))
    // f(x) = 0 if x <= 0, 1 if x >= 1 else 3pow(x, 2) - 2pow(x, 3)
    smoothstep = 7,
};

// ----------------------------------------------------------------------------

// The additive decomposition is the most appropriate if the magnitude of the
// seasonal fluctuations, or the variation around the trend-cycle, does not
// vary with the level of the time series. When the variation in the seasonal
// pattern, or the variation around the trend-cycle, appears to be
// proportional to the level of the time series, then a multiplicative
// decomposition is more appropriate. Multiplicative decompositions are common
// with economic time series.

// An alternative to using a multiplicative decomposition is to first
// transform the data until the variation in the series appears to be stable
// over time, then use an additive decomposition. When a log transformation
// has been used, this is equivalent to using a multiplicative decomposition
// because:
//    Y[t] = T * S * R is equivalent to log(Y[t]) = log(T) + logt(S) + log(R)
//
enum class decompose_type : unsigned char  {
    additive = 1,        // Y(t) = Trend + Seasonal + Residual
    multiplicative = 2,  // Y(t) = Trend * Seasonal * Residual
};

// ----------------------------------------------------------------------------

enum class box_cox_type : unsigned char  {
    // y(λ) = (y^λ - 1) / λ,  if λ != 0
    // y(λ) = log(y),         if λ == 0
    //
    original = 1,
    // y(λ) = (y^λ - 1) / (λ * GM^(λ - 1)),  if λ != 0
    // y(λ) = GM * log(y),                   if λ == 0
    //
    geometric_mean = 2,
    // y(λ) = sign(y) * (((|y| + 1)^λ - 1) / λ),  if λ != 0
    // y(λ) = sign(y) * log(|y| + 1),             if λ == 0
    //
    modulus = 3,
    // y(λ) = (e^λy - 1) / λ,  if λ != 0
    // y(λ) = y,               if λ == 0
    //
    exponential = 4,
};

// ----------------------------------------------------------------------------

enum class bucket_type : unsigned char  {
    // Bucketize by distance between two index values (i.g. X2 - X1 = N)
    //
    by_distance = 1,
    // Bucketize by counting of index values (e.g. every N index items)
    //
    by_count = 2,
};

// ----------------------------------------------------------------------------

template<typename T>
struct  RandGenParams  {
    T   min_value { std::numeric_limits<T>::min() };
    T   max_value { std::numeric_limits<T>::max() };

    unsigned int    seed { static_cast<unsigned int>(-1) };

    // The p distribution parameter (probability of generating true)
    //
    double      prob_true { 0.5 };
    // The t or k distribution parameter (number of trials)
    //
    std::size_t t_dist { 1 };
    // The μ distribution parameter (the mean of the distribution)
    //
    double      mean { 1.0 };
    // the σ distribution parameter (standard deviation)
    //
    double      std { 0 };
    // The λ distribution parameter (the rate parameter)
    //
    double      lambda { 1.0 };
    // The α distribution parameter (shape, location)
    //
    double      alpha { 1.0 };
    // The β distribution parameter (scale)
    //
    double      beta { 1.0 };
    // The m distribution parameter (log-scale)
    //
    double      m { 0 };
    // The s distribution parameter (shape)
    //
    double      s { 1.0 };
    // The n distribution parameter (degrees of freedom)
    //
    double      n { 1.0 };
    // degrees of freedom for fisher_f_distribution
    //
    double      n2 { 1.0 };
};

// ----------------------------------------------------------------------------

// It represents a range with begin and end within a continuous memory space
//
template<typename T>
struct Index2D  {
    T   begin {};
    T   end {};
};

// ----------------------------------------------------------------------------

template<typename V>
using GroupBySpec = std::tuple<const char *, const char *, V>;

// ----------------------------------------------------------------------------

template<typename, typename>
struct template_switch {  };

template<typename A, template<typename> typename C, typename B>
struct template_switch<A, C<B>> { using type = C<A>; };

// ----------------------------------------------------------------------------

template<typename T, typename U>
struct type_declare;

template<typename U>
struct type_declare<HeteroVector, U>  { using type = std::vector<U>; };

template<typename U>
struct type_declare<HeteroView, U>  { using type = VectorView<U>; };

template<typename U>
struct type_declare<HeteroPtrView, U>  { using type = VectorPtrView<U>; };

// ----------------------------------------------------------------------------

template<typename T> struct is_complex  {
    inline static const bool    value = false;
};
template<typename T> struct is_complex<std::complex<T>>  {
    inline static const bool    value = true;
};

// ----------------------------------------------------------------------------

// H stands for a heterogeneous vector
//
template<typename I, typename H>
class DataFrame;

template<typename I>
using StdDataFrame = DataFrame<I, HeteroVector>;

template<typename I>
using DataFrameView = DataFrame<I, HeteroView>;

template<typename I>
using DataFramePtrView = DataFrame<I, HeteroPtrView>;

// ----------------------------------------------------------------------------

// Evertyhting is in bytes. The numbers are estimates, since memory allocated
// is really unknown to the objects such as vectors.
// If type has dynamically allocated memory, it is not counted here
//
struct  MemUsage  {

    size_t  column_used_memory { 0 };
    size_t  column_capacity_memory { 0 };
    size_t  column_type_size { 0 };
    size_t  index_used_memory { 0 };
    size_t  index_capacity_memory { 0 };
    size_t  index_type_size { 0 };

    template<typename S>
    friend S &operator << (S &stream, const MemUsage &mu)  {

        stream << "Column Used Memory: " << mu.column_used_memory << '\n'
               << "Column Capacity Memory: "
               << mu.column_capacity_memory << '\n'
               << "Column Type Size: " << mu.column_type_size << '\n'
               << "Index Used Memory: " << mu.index_used_memory << '\n'
               << "Index Capacity Memory: " << mu.index_capacity_memory << '\n'
               << "Index Type Size: " << mu.index_type_size << '\n';
        return (stream);
    }
};

} // namespace hmdf

// ----------------------------------------------------------------------------


// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
