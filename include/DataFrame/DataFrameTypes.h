// Hossein Moein
// July 16, 2019
/*
Copyright (c) 2019-2026, Hossein Moein
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
#include <DataFrame/Utils/Concepts.h>
#include <DataFrame/Utils/FixedSizeString.h>
#include <DataFrame/Utils/DateTime.h>

#include <complex>
#include <limits>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

// ----------------------------------------------------------------------------

// Generic DataFrame error
//
struct  DataFrameError : public std::runtime_error  {

    explicit
    DataFrameError (const char *desc) : std::runtime_error (desc)  {   }
};

// Column does not exist error
//
struct  ColNotFound : public DataFrameError  {

    explicit
    ColNotFound (const char *desc) : DataFrameError (desc)  {   }
};

// Something in DataFrame/operation is not proper error. For example, trying
// to load a column with more data than there is in index
//
struct  InconsistentData : public DataFrameError  {

    explicit
    InconsistentData (const char *desc) : DataFrameError (desc)  {   }
};

// Bad data range specification error
//
struct  BadRange : public DataFrameError  {

    explicit
    BadRange (const char *desc) : DataFrameError (desc)  {   }
};

// The operation is not feasible error. For example, trying to do interpolation
// on non-arithmetic types
//
struct  NotFeasible : public DataFrameError  {

    explicit
    NotFeasible (const char *desc) : DataFrameError (desc)  {   }
};

// An I/O operation could not be done. For example, trying to open a file
// that doesn't exist.
//
struct  BadIO : public DataFrameError  {

    explicit
    BadIO (const char *desc) : DataFrameError (desc)  {   }
};

// The functionality is not implemented error
//
struct  NotImplemented : public DataFrameError  {

    explicit
    NotImplemented (const char *desc) : DataFrameError (desc)  {   }
};

// ----------------------------------------------------------------------------

inline constexpr const char *DF_INDEX_COL_NAME = "INDEX";

// ----------------------------------------------------------------------------

enum class  nan_policy : bool  {

    pad_with_nans = true,
    dont_pad_with_nans = false,
};

// ----------------------------------------------------------------------------

enum class  sort_state : bool  {

    sorted = true,
    not_sorted = false,
};

// ----------------------------------------------------------------------------

enum class  sort_spec : unsigned char  {

    ascen = 1,
    desce = 2,
    abs_ascen = 3,
    abs_desce = 4,
};

// ----------------------------------------------------------------------------

enum class  join_policy : unsigned char  {

    inner_join = 1,
    left_join = 2,
    right_join = 3,
    left_right_join = 4,  // This is merge
};

// ----------------------------------------------------------------------------

enum class  concat_policy : unsigned char  {

    common_columns = 1,
    all_columns = 2,
    lhs_and_common_columns = 3,
};

// ----------------------------------------------------------------------------

// This policy is relative to a tabular data structure
//
enum class  shift_policy : unsigned char  {

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

enum class  fill_policy : unsigned char  {

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

    // Fill missing values with mid-point of surrounding values
    //
    mid_point = 5,

    // O(n2) algorithm for each missing value.
    // It uses the index as X coordinate
    // This is very much a garbage in, garbage out algorithm.
    // The index and column data must be in the same scale and be correlated.
    //
    lagrange_interpolate = 6,
};

// ----------------------------------------------------------------------------

enum class  roll_policy : unsigned char  {

    // Roll one block of data at a time
    // The length of result will be "input length / rolling period"
    //
    blocks = 1,

    // Roll one data point at a time
    // The length of result will be "input length - rolling period"
    //
    continuous = 2,
};

// ----------------------------------------------------------------------------

enum class  quantile_policy : unsigned char  {

    lower_value = 1,   // Take the higher index
    higher_value = 2,  // Take the lower index
    mid_point = 3,     // Average the two quantiles
    linear = 4,        // Linearly combine the two quantiles
};

// ----------------------------------------------------------------------------

enum class  drop_policy : unsigned char  {

    all = 1,       // Remove row if all columns are nan
    any = 2,       // Remove row if any column is nan
    threshold = 3, // Remove row if threshold number of columns are nan
};

// ----------------------------------------------------------------------------

enum class  mean_type : unsigned char  {

    arithmetic = 1, // sum(value) / count
    weighted = 2,   // sum(weighted value) / ((count * (count + 1)) / 2)
    geometric = 3,  // exp(log(sum(value)) / count)
    harmonic = 4,   // count / sum(1 / value)
    quadratic = 5,  // sqrt(sum(value * value) / count)
};

// ----------------------------------------------------------------------------

enum class  correlation_type : unsigned char  {

    pearson = 1,      // ρ =  COV(X, Y) / (σ(X) * σ(Y))

    // Non-parametric (rank-based) correlations
    //
    spearman = 2,     // ρ = 1 - ((6 * SUM(di^2)) / (n * (n^2 - 1)))
    kendall_tau = 3,  // ρ = (C - D) / ((n * (n - 1)) / 2)
};

// ----------------------------------------------------------------------------

enum class  hampel_type : unsigned char  {

    mean = 1,   // Use mean absolute deviation
    median = 2, // Use median absolute deviation
};

// ----------------------------------------------------------------------------

enum class  remove_dup_spec : unsigned char  {

    keep_first = 1,  // Keep the first duplicated row
    keep_last = 2,   // Keep the last duplicated row
    keep_none = 3,   // Discard all duplicated rows
};

// ----------------------------------------------------------------------------

enum class  exponential_decay_spec : unsigned char  {

    center_of_gravity = 1, // decay = 1 / (1 + value), for value >= 0
    span = 2,              // decay = 2 / (1 + value), for value >= 1
    halflife = 3,          // decay = 1 − exp(log(0.5) / value), for value > 0
    fixed = 4,             // decay = value, for 0 < value <= 1
};

// ----------------------------------------------------------------------------

// It defines different ways of calculating averages around averages,
// in other words different types of Mean Absolute Deviation.
//
enum class  mad_type : unsigned char  {

    mean_abs_dev_around_mean = 1,
    mean_abs_dev_around_median = 2,
    median_abs_dev_around_mean = 3,
    median_abs_dev_around_median = 4,
};

// ----------------------------------------------------------------------------

enum class  io_format : unsigned char  {

    csv = 1,   // DataFrame specific csv format
    csv2 = 2,  // Regular csv format (similar to Pandas)
    json = 3,
    hdf5 = 4,
    binary = 5,
};

// ----------------------------------------------------------------------------

enum class  time_frequency : unsigned char  {

    not_valid = 0,
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

enum class  return_policy : unsigned char  {

    log = 1,         // log(t1 / t0)
    percentage = 2,  // (t1 - t0) / t0
    monetary = 3,    // t1 - t0
    trinary = 4,     // 1 if t1 - t0 > 0, else -1 if t1 - t0 < 0, else 0
};

// ----------------------------------------------------------------------------

// Specification for calling get_[data|view]_by_rand()
// Number of rows means the n parameter is a positive integer specifying
// the number of rows to select
// Fraction of rows means the n parameter is a positive real number [0:1]
// specifying a fraction of rows to select
//
enum class  random_policy : unsigned char  {

    num_rows_with_seed = 1,  // Number of rows with specifying a seed
    num_rows_no_seed = 2,    // Number of rows with no seed specification
    frac_rows_with_seed = 3, // Fraction of rows with specifying a seed
    frac_rows_no_seed = 4,   // Fraction of rows with no seed specification
};

// ----------------------------------------------------------------------------

enum class  pattern_spec : unsigned char  {

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
enum class  rank_policy : unsigned char  {

    average = 1,  // Average of ranks, if an item is repeated
    first = 2,    // First rank, if an item is repeated
    last = 3,     // Last rank, if an item is repeated
    actual = 4,   // The actual rank of each item based on stable sort
};

// ----------------------------------------------------------------------------

// Different types of Sigmoid functions. For now, there is no integration
//
enum class  sigmoid_type : unsigned char  {

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

enum class  rectify_type : unsigned char  {

    ReLU = 1,           // f(x) = x if x > 0 else 0
    param_ReLU = 2,     // f(x) = x if x > 0 else a * x,          a <= 1
    GeLU = 3,           // f(x) = x * SND(x),       SND = Standard Normal Dist
    SiLU = 4,           // f(x) = x * Sigmoid(x)
    softplus = 5,       // f(x) = log(1 + e^(a * x)) / a,         a != 0
    elu = 6,            // f(x) = x if x > 0 else a * (e^x - 1),  a >= 0
    mish = 7,           // f(x) = x * tanh(softplus(x))
    metallic_mean = 8,  // f(x) = (x + sqrt(x^2 + 4)) / 2
};

// ----------------------------------------------------------------------------

enum class  linreg_moving_mean_type : unsigned char  {

    linreg = 1,
    forecast = 2,
    slope = 3,
    intercept = 4,
    theta = 5,   // arc-tan of slope
    degree = 6,  // degree of slope
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
enum class  decompose_type : unsigned char  {

    additive = 1,        // Y(t) = Trend + Seasonal + Residual
    multiplicative = 2,  // Y(t) = Trend * Seasonal * Residual
};

// ----------------------------------------------------------------------------

enum class  box_cox_type : unsigned char  {

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

enum class  bucket_type : unsigned char  {

    // Bucketize by distance between two index values (i.g. X2 - X1 = N)
    //
    by_distance = 1,
    // Bucketize by counting of index values (e.g. every N index items)
    //
    by_count = 2,
};

// ----------------------------------------------------------------------------

enum class  impurity_type : unsigned char  {

    // Measure how often a randomly chosen element from the set would be
    // incorrectly labeled
    //
    gini_index = 1,
    // Average level of "information", "surprise", or "uncertainty" inherent
    // to the variable's possible outcomes.
    //
    info_entropy = 2,
};

// ----------------------------------------------------------------------------

enum class  prob_dist_type : unsigned char  {

    // No negative values
    // yi = xi / sum(X)
    //
    arithmetic = 1,

    // All values must be >= 1
    // yi = loge(xi) / sum(loge(X))
    //
    log = 2,

    // yi = e^xi / sum(e^X)
    //
    softmax = 3,

    // yi = 2^xi / sum(2^X)
    //
    pow2 = 4,

    // yi = 10^xi / sum(10^X)
    //
    pow10 = 5,
};

// ----------------------------------------------------------------------------

enum class  normalization_type : unsigned char  {

    none = 0,
    simple = 1,           // V / sum(xi)
    euclidean = 2,        // V / sqrt(sum(xi^2))
    maxi = 3,             // V / max(xi)
    z_score = 4,          // (V - μ(V)) / σ(V)
    min_max = 5,          // (V - min(V)) / (max(V) - min(V))
    decimal_scaling = 6,  // V / 10^max(xi) order
    log_transform = 7,    // ln(xi)
    root_transform = 8,   // sqrt(xi)
};

// ----------------------------------------------------------------------------

enum class  loss_function_type : unsigned char  {

    // L = sum(P(x) * log(P(x) / Q(x)))  --  P = Actual, Q = Model
    //
    kullback_leibler = 1,

    // L = sum(|Yi - YHati|) / N  --  Y = Actual, YHat = Model
    //
    mean_abs_error = 2,

    // L = sum((Yi - YHati)^2) / N  --  Y = Actual, YHat = Model
    //
    mean_sqr_error = 3,

    // L = sum(log(1 + Yi) - log(1 + YHati)^2) / N
    //     Y = Actual, YHat = Model
    //
    mean_sqr_log_error = 4,

    // L = -sum(Yi * log(P(Yi))) / N
    //     Y = Actual, P(Yi) = Model probability prediction
    //
    cross_entropy = 5,

    // L = sum(-(Yi * log(P(Yi))) + (1 - Yi) * log(1 - P(Yi))) / N
    //     Y = Actual binary (0/1), P(Yi) = Model probability prediction
    //
    binary_cross_entropy = 6,

    // L = max(sum((1 - Y) * YHat) - sum(Yi * YHati) + 1, 0)
    //     Y = Actual, YHat = Model
    //
    categorical_hinge = 7,

    // L = (Y . YHat) / (||Y|| * ||YHat||)
    //     Y = Actual, YHat = Model
    //
    cosine_similarity = 8,

    // L = sum(log(cosh(YHati - Yi))) / N
    //     Y = Actual, YHat = Model
    //
    log_cosh = 9,
};

// ----------------------------------------------------------------------------

enum class  vector_sim_type : unsigned char  {

    euclidean_dist = 1,     // Euclidean distance
    manhattan_dist = 2,     // Manhattan distance
    dot_product = 3,
    simple_similarity = 4,  // Two binary vectors must be of equal length
    cosine_similarity = 5,
    jaccard_similarity = 6, // Relatively expensive to calculate

    // Hamming distance is number of unequal members
    // Two vectors must be of equal length
    //
    hamming_dist = 7,
};

// ----------------------------------------------------------------------------

enum class  mean_shift_kernel : unsigned char  {

    uniform = 1,
    triangular = 2,
    parabolic = 3,
    biweight = 4,
    triweight = 5,
    tricube = 6,
    gaussian = 7,
    cosin = 8,
    logistic = 9,
    sigmoid = 10,
    silverman = 11,
};

// ----------------------------------------------------------------------------

enum class  stationary_method : unsigned char  {

    differencing = 1,  // Xt = Yt - Yt-1
    log_trans = 2,     // Xt = ln(Yt)
    sqrt_trans = 3,    // Xt = sqrt(Yt)
    boxcox_trans = 4,  // See BoxCoxVisitor
    decomposition = 5, // See DecomposeVisitor
    smoothing = 6,     // See ExponentiallyWeightedMeanVisitor
};

struct  StationaryParams  {

    // Only considered in case of BoxCox transformation method
    //
    box_cox_type            bc_type { box_cox_type::original };
    double                  lambda { 0 };
    bool                    is_all_positive { true };

    // Only considered in case of Decomposition method
    //
    std::size_t             season_period { 0 };
    double                  dcom_fraction { 0 };
    double                  dcom_delta { 0 };
    decompose_type          dcom_type { decompose_type::additive };

    // Only considered in case of Smoothing method
    //
    exponential_decay_spec  decay_spec { exponential_decay_spec::span };
    double                  decay_alpha { 0 };
    bool                    finite_adjust { true };
};

// ----------------------------------------------------------------------------

enum class  stationary_test : unsigned char  {

    // Kwiatkowski-Phillips-Schmidt–Shin (KPSS)
    // In econometrics, Kwiatkowski–Phillips–Schmidt–Shin (KPSS) tests are
    // used for testing a null hypothesis that an observable time series is
    // stationary around a deterministic trend (i.e. trend-stationary) against
    // the alternative of a unit root.
    // Contrary to most unit root tests, the presence of a unit root is not
    // the null hypothesis but the alternative. Additionally, in the KPSS test,
    // the absence of a unit root is not a proof of stationarity but, by
    // design, of trend-stationarity. This is an important distinction since it
    // is possible for a time series to be non-stationary, have no unit root
    // yet be trend-stationary.
    //
    // In a KPSS test, a higher test statistic value (meaning a larger
    // calculated KPSS statistic) indicates a greater likelihood that the time
    // series is not stationary around a deterministic trend, while a lower
    // value suggests stationarity; essentially, you want a low KPSS test value
    // to conclude stationarity.
    //
    kpss = 1,

    // Augmented Dickey-Fuller (ADF)
    // In statistics, an augmented Dickey–Fuller test (ADF) tests the null
    // hypothesis that a unit root is present in a time series sample. The
    // alternative hypothesis depends on which version of the test is used,
    // but is usually stationarity or trend-stationarity. It is an augmented
    // version of the Dickey–Fuller test for a larger and more complicated set
    // of time series models.
    // The augmented Dickey–Fuller (ADF) statistic, used in the test, is a
    // negative number. The more negative it is, the stronger the rejection of
    // the hypothesis that there is a unit root at some level of confidence.
    //
    // To interpret an ADF test statistic, compare its value to the critical
    // value at a chosen significance level (usually 0.05): if the test
    // statistic is less than the critical value, you reject the null
    // hypothesis and conclude that the time series is stationary; if it's
    // greater than the critical value, you fail to reject the null hypothesis,
    // indicating non-stationarity; a more negative ADF statistic signifies
    // stronger evidence against the null hypothesis (i.e., more likely
    // stationary).
    //
    adf = 2,
};

struct  StationaryTestParams  {

    // Only considered for KPSS test
    //
    double  critical_values[4] { 0.347, 0.463, 0.574, 0.739 };

    // Only considered for ADF test
    //
    std::size_t adf_lag { 1 };
    bool        adf_with_trend { false };
};

// ----------------------------------------------------------------------------

// These are enumerations for data types that can be read/written from/to files
//
enum class  file_dtypes : std::size_t  {

    FLOAT = 0,            // float
    DOUBLE = 1,           // double
    LONG_DOUBLE = 2,      // long double
    SHORT = 3,            // short int
    USHORT = 4,           // unsigned short int
    INT = 5,              // int
    UINT = 6,             // unsigned int
    LONG = 7,             // long int
    ULONG = 8,            // unsigned long int
    LONG_LONG = 9,        // long long int
    ULONG_LONG = 10,      // unsigned long long int
    CHAR = 11,            // char
    UCHAR = 12,           // unsigned char
    BOOL = 13,            // bool

    STRING = 14,          // std::string
    VSTR32 = 15,          // FixedSizeString<31> == String32
    VSTR64 = 16,          // FixedSizeString<63> == String64
    VSTR128 = 17,         // FixedSizeString<127> == String128
    VSTR512 = 18,         // FixedSizeString<511> == String512
    VSTR1K = 19,          // FixedSizeString<1023> == String1K
    VSTR2K = 20,          // FixedSizeString<2047> == String2K

    DATETIME = 21,        // DateTime
    DATETIME_AME = 22,    // DateTime
    DATETIME_EUR = 23,    // DateTime
    DATETIME_ISO = 24,    // DateTime

    STR_DBL_PAIR = 25,    // std::pair<std::string, double>
    STR_STR_PAIR = 26,    // std::pair<std::string, std::string>
    DBL_DBL_PAIR = 27,    // std::pair<double, double>

    DBL_VEC = 28,         // std::vector<double>
    STR_VEC = 29,         // std::vector<std::string>
    DBL_SET = 30,         // std::set<double>
    STR_SET = 31,         // std::set<std::string>
    STR_DBL_MAP = 32,     // std::map<std::string, double>
    STR_DBL_UNOMAP = 33,  // std::unordered_map<std::string, double>
};

// ----------------------------------------------------------------------------

// Different methods of detecting anomalous data points. Anomalous data don’t
// mean NaN or missing datapoints. The best description is unusual datapoints.
//
enum class  detect_method : unsigned char  {

    fft = 1,     // Fast Fourier Transform (FFT)
    iqr = 2,     // Inter-Quartile Range (IQR)
    lof = 3,     // Local Outlier Factor (LOF)
    hampel = 4,  // Hampel Filter
    zscore = 5,  // Z-Score method
};

// -------------------------------------

// Parameters to member function detect_and_fill()
//
template<typename T>
struct  DetectAndChangeParams  {

    using value_type = T;
    using distance_func = std::function<double(const T &x, const T &y)>;

    // Parameter specific to Z-Score, FFT and LOF
    //
    value_type          threshold { 0 };

    // Parameter specific to FFT and LOF
    //
    normalization_type  norm_type { normalization_type::none };

    // Parameter specific to FFT
    //
    std::size_t         freq_num { 0 };

    // Parameters specific to IQR
    //
    value_type          high_fence { 1.5 };
    value_type          low_fence { 1.5 };

    // Parameter specific to LOF
    //
    std::size_t         k { 0 };
    distance_func       dist_fun =
        [](const value_type &x, const value_type &y) -> double  {
            return (std::fabs(x - y));
        };

    // Parameters specific to Hampel filter
    //
    std::size_t         window_size { 0 };
    hampel_type         htype { hampel_type::median };
    value_type          num_stdev { 3.0 };
};

// ----------------------------------------------------------------------------

// A structure to describe a column of data used in a read operation to
// decipher data in a file in csv2 format.
//
struct  ReadSchema  {

    using ColNameType = String64;

    ColNameType col_name { DF_INDEX_COL_NAME };
    file_dtypes col_type { file_dtypes::ULONG };
    std::size_t num_rows { 0 };

    // 0-based index of columns starting at index column at 0.
    // Regular columns start at 1.
    //
    int         col_idx { -1 };
};

// Parameters to read() function of DataFrame
//
struct  ReadParams  {

    using SchemaVec = std::vector<ReadSchema>;

    // If true, it only reads the data columns and skips the index column
    //
    bool        columns_only { false };

    // Start reading data from this row number.
    // It only applies to csv2 and binary formats
    //
    std::size_t starting_row { 0 };

    // Only read this many rows of data.
    // It only applies to csv2 and binary formats
    //
    std::size_t num_rows { std::numeric_limits<std::size_t>::max() };

    // These are only considered in csv2 format. They are ignored in all
    // other formats.
    //
    // If schema is nonempty, it indicates that the caller wants to read a csv
    // file that was not generated by C++ DataFrame. The schema must
    // contain the relevant entries for each column of data. The first entry
    // in schema must be the index column. The schema must have an entry for
    // each column in the file, in the order they appear in the file. All
    // entries in ReadSchema struct must be set, again in the order they
    // appear in the file. This also allows the user to skip column(s) in the
    // file they don't want to read into a DataFrame.
    // The skip_first_line is checked after it is determined that schema is
    // nonempty. skip_first_line means the first line of the file is a header
    // that was not generated by C++ DataFrame and must be skipped.
    //
    bool        skip_first_line { true };
    SchemaVec   schema { };

    // This only applies to csv and csv2 formats. It specifies the delimiting
    // (separating) character.
    //
    char        delim { ',' };
};

// ----------------------------------------------------------------------------

// Parameters to write() function of DataFrame
//
template<DT_ALLOWABLE_FORMATS DT_F = DT_FORMAT>
struct  WriteParams  {

    // Floating-point values precision when written to a file
    //
    std::streamsize precision { 12 };

    // If true, it only reads the data columns and skips the index column
    //
    bool            columns_only { false };

    // Max number of rows to write
    //
    long            max_recs { std::numeric_limits<long>::max() };

    // This specifies with what format DateTime columns are written into a csv
    // file. This only applies to csv2 format.
    // The only permitted formats are DT_PRECISE, ISO_DT_TM, AMR_DT_TM,
    // EUR_DT_TM, ISO_DT, AMR_DT, and EUR_DT.
    // See Dateitme docs (DT_FORMAT) for more info.
    // The default format is seconds since Epoch.nanoseconds.
    //
    DT_F            dt_format { DT_FORMAT::DT_PRECISE };

    // This only applies to csv and csv2 formats. It specifies the delimiting
    // (separating) character.
    //
    char            delim { ',' };
};

// ----------------------------------------------------------------------------

struct  PCAParams  {

    normalization_type  norm_type { normalization_type::z_score };

    // If populated (set above zero), number of top eigen values to keep.
    //
    long                num_comp_to_keep { 0 };

    // If populated (num_comp_is 0), percentage of eigen values to keep.
    // 0.9 means 90%.
    //
    double              pct_comp_to_keep { 0.9 };
};

// ----------------------------------------------------------------------------

struct  ICAParams  {

    using seed_t = std::random_device::result_type;

    // First center the data matrix
    //
    bool                center { true };

    // Max number of iterations before giving up
    //
    std::size_t         num_iter { 200 };

    // Epsilon value to call it done
    //
    double              epsilon { 1e-5 };

    // Nonlinearity applied to data
    //
    sigmoid_type        nonlinearity { sigmoid_type::hyperbolic_tan };

    // Seed for random number generator
    //
    seed_t              seed { seed_t(-1) };
};

// ----------------------------------------------------------------------------

// Canonical correlation analysis result
//
template<typename T>
struct  CanonCorrResult  {

    // These values represent the strength of the linear relationship between
    // each pair of canonical variates, ranging from -1 to 1, with higher
    // absolute values signifying a stronger association.
    //
    std::vector<T>  coeffs { };     // Canonical correlation coefficients

    // The Redundancy Index is a measure that indicates how much variance in
    // one set of variables is explained by the linear combination of the other
    // set of variables. This was proposed by Stewart and Love (1968).
    //
    T               x_red_idx { };  // Redundancy index for X
    T               y_red_idx { };  // Redundancy index for Y
};

// ----------------------------------------------------------------------------

// Parameter to the SeasonalPeriodVisitor constructor
//
template<typename T>
struct  SeasonalityParams  {

    // Remove trend
    bool        detrend { true };
    // Remove serial correlation by differencing
    bool        de_serial_corr { false };

    // Parameters to generate trend using LOWESS
    // The two parameters below must be adjusted for different datasets
    // carefully sometimes by trail and error. The defaults are suitable for
    // financial market data
    //
    std::size_t num_loops { 3 };  // Number of loops
    // The fraction of the data used when estimating each y-value.
    T           frac { 0.08 };
    // Distance within which to use linear-interpolation instead of regression
    T           delta { 0.0001 };

    // Assume the time series is per 1 unit of time
    std::size_t sampling_rate { 1 };
};

// ----------------------------------------------------------------------------

template<typename T>
struct  RandGenParams  {

    T   min_value { std::numeric_limits<T>::min() };
    T   max_value { std::numeric_limits<T>::max() };

    unsigned int    seed { static_cast<unsigned int>(-1) };

    // The p distribution parameter (probability of generating true)
    //
    double          prob_true { 0.5 };
    // The t or k distribution parameter (number of trials)
    //
    std::size_t     t_dist { 1 };
    // The μ distribution parameter (the mean of the distribution)
    //
    double          mean { 0 };
    // the σ distribution parameter (standard deviation)
    //
    double          std { 1 };
    // The λ distribution parameter (the rate parameter)
    //
    double          lambda { 1.0 };
    // The α distribution parameter (shape, location)
    //
    double          alpha { 1.0 };
    // The β distribution parameter (scale)
    //
    double          beta { 1.0 };
    // The m distribution parameter (log-scale)
    //
    double          m { 0 };
    // The s distribution parameter (shape)
    //
    double          s { 1.0 };
    // The n distribution parameter (degrees of freedom)
    //
    double          n { 1.0 };
    // Degrees of freedom for fisher_f_distribution
    //
    double          n2 { 1.0 };
};

// ----------------------------------------------------------------------------

// It represents a range with begin and end within a continuous memory space
//
template<typename T>
struct  Index2D  {

    T   begin {};
    T   end {};
};

// ----------------------------------------------------------------------------

template<typename T>
using KNNPair = std::pair<std::vector<T>, std::size_t>;

template<typename T>
using KNNResult = std::vector<KNNPair<T>>;

template<typename T>
using KNNDistFunc =
    std::function<T(const std::vector<T> &X, const std::vector<T> &y)>;

// ----------------------------------------------------------------------------

template<typename V>
using GroupBySpec = std::tuple<const char *, const char *, V>;

// ----------------------------------------------------------------------------

template<typename, typename>
struct  template_switch  {   };

template<typename A, template<typename> typename C, typename B>
struct  template_switch<A, C<B>>  { using type = C<A>; };

// ----------------------------------------------------------------------------

template<class T, typename U, std::size_t A>
struct  type_declare;

template<typename U, std::size_t A>
struct  type_declare<HeteroVector<A>, U, A>  {
    using type = std::vector<U, typename allocator_declare<U, A>::type>;
};

template<typename U, std::size_t A>
struct  type_declare<HeteroView<A>, U, A>  { using type = VectorView<U, A>; };

template<typename U, std::size_t A>
struct  type_declare<HeteroConstView<A>, U, A>  {
    using type = VectorConstView<U, A>;
};

template<typename U, std::size_t A>
struct  type_declare<HeteroPtrView<A>, U, A>  {
    using type = VectorPtrView<U, A>;
};

template<typename U, std::size_t A>
struct  type_declare<HeteroConstPtrView<A>, U, A>  {
    using type = VectorConstPtrView<U, A>;
};

// ----------------------------------------------------------------------------

template<typename T>
struct  is_complex  {
    inline static constexpr bool    value = false;
};
template<typename T>
struct  is_complex<std::complex<T>>  {
    inline static constexpr bool    value = true;
};

// ----------------------------------------------------------------------------

// Since std::is_arithmetic cannot be overloaded, we need a way to determine
// if a user-defined type supports arithmetic
//
template<typename ...>
struct  make_void  { using type = void; };

template<typename ... Ts>
using void_t = typename make_void<Ts ...>::type;

template<typename T, typename = void>
struct  supports_arithmetic : std::false_type  {  };

template<typename T>
struct  supports_arithmetic<
    T,
    void_t<decltype(std::declval<T>() + std::declval<T>()),
           decltype(std::declval<T>() - std::declval<T>()),
           decltype(std::declval<T>() * std::declval<T>()),
           decltype(std::declval<T>() / std::declval<T>())>>
    : std::true_type  {  };

// ----------------------------------------------------------------------------

// I stands for Index type
// H stands for Heterogeneous vector
// A stands for memory Alignment
//
template<typename I, typename H>
class   DataFrame;

template<typename I>
using StdDataFrame = DataFrame<I, HeteroVector<0>>;

// Convenient typedefs to facilitate SIMD operations
//
template<typename I>
using StdDataFrame64 = DataFrame<I, HeteroVector<64>>;

template<typename I>
using StdDataFrame128 = DataFrame<I, HeteroVector<128>>;

template<typename I>
using StdDataFrame256 = DataFrame<I, HeteroVector<256>>;

template<typename I>
using StdDataFrame512 = DataFrame<I, HeteroVector<512>>;

template<typename I>
using StdDataFrame1024 = DataFrame<I, HeteroVector<1024>>;

// ----------------------------------------------------------------------------

inline static const std::vector<std::string>   describe_index_col  {
    "COUNT", "MISSING", "MEAN", "STD", "MIN", "MAX", "25%", "50%", "75%"
};

// ----------------------------------------------------------------------------

// Everything is in bytes. The numbers are estimates, since memory allocated
// is really unknown to the objects such as vectors.
// If type has dynamically allocated memory, it is not counted here
//
struct  MemUsage  {

    std::size_t column_used_memory { 0 };
    std::size_t column_capacity_memory { 0 };
    std::size_t column_type_size { 0 };
    std::size_t index_used_memory { 0 };
    std::size_t index_capacity_memory { 0 };
    std::size_t index_type_size { 0 };

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

struct  StringStats  {

    double  avg_size { 0 };        // Average of sizes
    double  std_size { 0 };        // Standard deviation of sizes
    double  avg_alphabets { 0 };   // Average number of a - z, A - Z
    double  avg_caps { 0 };        // Average number of A - Z
    double  avg_digits { 0 };      // Average number of 0 - 9
    double  avg_spaces { 0 };      // Average number of ' '
    double  avg_arithmetic { 0 };  // Average number of + - / *
    double  avg_line_feed { 0 };   // Average number of \n

    // Punctuations. Anything that's not an alphabet, digit, space, line feed,
    // or arithmetic operators.
    //
    double  avg_puncts { 0 };
};

// ----------------------------------------------------------------------------

// Helper macro for building DataFrame
// e.g.:
// struct MyDfSchema  {
//     DECL_COL(Col0, std::string);
//     DECL_COL(Col1, int32_t);
//     DECL_COL(Col2, int64_t);
//     DECL_COL(Col3, double);
// };
//
// then usage:
// df.load_column<MyDfSchema::Col0::type>(MyDfSchema::Col0::name,
//                                        std::move(col_vec));

#define DECL_COL(COLUMN_NAME, C_TYPE) \
    struct  COLUMN_NAME  { \
\
        constexpr static const char *name = {#COLUMN_NAME}; \
\
        using type = C_TYPE; \
\
        const char *col_name() const  { return (name); } \
    }; \

template <typename T>
struct  CommonColumn  {

    explicit CommonColumn(std::string_view col_name) : name_ (col_name)  {  }

    using type = T;

    [[nodiscard]] const char *col_name() const  { return (name_.c_str()); }

    std::string name_;
};

} // namespace hmdf

// ----------------------------------------------------------------------------


// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
