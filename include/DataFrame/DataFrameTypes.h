// Hossein Moein
// July 16, 2019
// Copyright (C) 2019-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

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

enum class join_policy : unsigned char  {
    inner_join = 1,
    left_join = 2,
    right_join = 3,
    left_right_join = 4  // This is merge
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
    linear_extrapolate = 5   // Using the index as X coordinate
};

// ----------------------------------------------------------------------------

enum class drop_policy : unsigned char  {
    all = 1,  // Remove row if all columns are nan
    any = 2,  // Remove row if any column is nan
    threshold = 3  // Remove row if threshold number of columns are nan
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

template<typename T>
struct  RandGenParams  {
    T   min_value { std::numeric_limits<T>::min() };
    T   max_value { std::numeric_limits<T>::max() };

    unsigned int    seed { (unsigned int) -1 };

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

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
