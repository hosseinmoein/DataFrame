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
#include <limits>
#include <stdexcept>

// ----------------------------------------------------------------------------

namespace hmdf
{

#if defined(WIN32) || defined(_WIN32)
#  ifdef min
#    undef min
#  endif // min
#  ifdef max
#    undef max
#  endif // max
#endif // WIN32 || _WIN32

// ----------------------------------------------------------------------------

struct DataFrameError : public std::runtime_error  {

    DataFrameError (const char *desc) : std::runtime_error (desc)  {   }
};
struct ColNotFound : public DataFrameError  {

    ColNotFound (const char *desc) : DataFrameError (desc)  {   }
};
struct InconsistentData : public DataFrameError  {

    InconsistentData (const char *desc) : DataFrameError (desc)  {   }
};
struct BadRange : public DataFrameError  {

    BadRange (const char *desc) : DataFrameError (desc)  {   }
};
struct NotFeasible : public DataFrameError  {

    NotFeasible (const char *desc) : DataFrameError (desc)  {   }
};
struct NotImplemented : public DataFrameError  {

    NotImplemented (const char *desc) : DataFrameError (desc)  {   }
};

// ----------------------------------------------------------------------------

inline constexpr const char *const  DF_INDEX_COL_NAME = "INDEX";

// ----------------------------------------------------------------------------

enum class nan_policy : bool  {
    pad_with_nans = true,
    dont_pad_with_nans = false
};

// ----------------------------------------------------------------------------

enum class sort_state : bool  {
    sorted = true,
    not_sorted = false
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
    left_right_join = 4  // This is merge
};

// ----------------------------------------------------------------------------

enum class concat_policy : unsigned char  {
    common_columns = 1,
    all_columns = 2,
    lhs_and_common_columns = 3,
};

// ----------------------------------------------------------------------------
// This policy is relative to a tabular data structure
// There is no right or left shift (like Pandas), because columns in DataFrame
// have no ordering. They can only be accessed by name
enum class shift_policy : unsigned char  {
    down = 1,  // Shift/rotate the content of all columns down,
               // keep index unchanged
    up = 2,    // Shift/rotate the content of all columns up,
               // keep index unchanged
};

// ----------------------------------------------------------------------------

enum class fill_policy : unsigned char  {
    value = 1,
    fill_forward = 2,
    fill_backward = 3,

    //           X - X1
    // Y = Y1 + ------- * (Y2 - Y1)
    //          X2 - X1
    linear_interpolate = 4,  // Using the index as X coordinate
    linear_extrapolate = 5,  // Using the index as X coordinate
    mid_point = 6,           // Mid-point of x and y
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
    all = 1,  // Remove row if all columns are nan
    any = 2,  // Remove row if any column is nan
    threshold = 3  // Remove row if threshold number of columns are nan
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
    csv = 1,
    json = 2,
    hdf5 = 3,
    binary = 4,
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
    // nanosecondly = 10
};

// ----------------------------------------------------------------------------

enum class return_policy : unsigned char  {
    log = 1,
    percentage = 2,
    monetary = 3,
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

template<typename T>
struct  RandGenParams  {
    T   min_value { std::numeric_limits<T>::min() };
    T   max_value { std::numeric_limits<T>::max() };

    unsigned int    seed { static_cast<unsigned int>(-1) };

    // The p distribution parameter (probability of generating true)
    double      prob_true { 0.5 };
    // The t or k distribution parameter (number of trials)
    std::size_t t_dist { 1 };
    // The μ distribution parameter (the mean of the distribution)
    double      mean { 1.0 };
    // the σ distribution parameter (standard deviation)
    double      std { 0 };
    // The λ distribution parameter (the rate parameter)
    double      lambda { 1.0 };
    // The α distribution parameter (shape, location)
    double      alpha { 1.0 };
    // The β distribution parameter (scale)
    double      beta { 1.0 };
    // The m distribution parameter (log-scale)
    double      m { 0 };
    // The s distribution parameter (shape)
    double      s { 1.0 };
    // The n distribution parameter (degrees of freedom)
    double      n { 1.0 };
    // degrees of freedom for fisher_f_distribution
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

template<typename T, typename U>
struct type_declare;

template<typename U>
struct type_declare<HeteroVector, U>  { using type = std::vector<U>; };

template<typename U>
struct type_declare<HeteroView, U>  { using type = VectorView<U>; };

template<typename U>
struct type_declare<HeteroPtrView, U>  { using type = VectorPtrView<U>; };

// ----------------------------------------------------------------------------

// H stands for a heterogeneous vector
template<typename I, typename H>
class DataFrame;

template<typename I>
using StdDataFrame = DataFrame<I, HeteroVector>;

template<typename I>
using DataFrameView = DataFrame<I, HeteroView>;

template<typename I>
using DataFramePtrView = DataFrame<I, HeteroPtrView>;

// ----------------------------------------------------------------------------

// These are templated, so they work for all types

template<typename T>
inline bool is_nan__(const T &)  { return(false); }

template<>
inline bool is_nan__<double>(const double &val)  { return(std::isnan(val)); }

template<>
inline bool is_nan__<float>(const float &val)  { return(std::isnan(val)); }

template<>
inline bool
is_nan__<long double>(const long double &val)  { return(std::isnan(val)); }

} // namespace hmdf

// ----------------------------------------------------------------------------

// Evertyhting is in bytes. The numbers are estimates, since memory allocated
// is really unknown to the objects such as vectors.
// If type has dynamically allocated memory, it is not counted here

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

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
