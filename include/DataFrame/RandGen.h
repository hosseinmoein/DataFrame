// Hossein Moein
// July 22, 2019
// Copyright (C) 2019-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <DataFrame/DataFrameTypes.h>

#include <random>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

// It generates n uniform interger distribution random numbers.
//                  1
// P(i|a,b) = --------------
//              b − a + 1
//
// It retruns the vector of results
// Optional parameters to set:
//     max_value, min_value, seed
//
template<typename T>
std::vector<T>
gen_uniform_int_distribution(
    std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// It generates n uniform real distribution random numbers.
//                1
// P(i|a,b) = ----------
//              b − a 
//
// It retruns the vector of results
// Optional parameters to set:
//     max_value, min_value, seed
//
template<typename T>
std::vector<T>
gen_uniform_real_distribution(
    std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// It generates random boolean values, according to the discrete probability
// function. The probability of true is:
//          ⎧     p if b == true
// P(b|p) = ⎨
//          ⎩ 1 - p if b == fasle 
//
// It retruns the vector of results
// Optional parameters to set:
//     prob_true, seed
//
std::vector<bool>
gen_bernoulli_distribution(
    std::size_t n, const RandGenParams<bool> &params = { });

// ----------------------------------------------------------------------------

// Produces random non-negative integer values i, distributed according to
// discrete probability function:
//            (t)    i          t-i
// P(i|t,p) = (i) . p . (1 - p)
//
// It retruns the vector of results
// Optional parameters to set:
//     t_dist, prob_true, seed
//
template<typename T>
std::vector<T>
gen_binomial_distribution(
    std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// Produces random non-negative integer values i, distributed according to
// discrete probability function:
//            (k+i-1)    k         i
// P(i|k,p) = (i    ) . p . (1 - p)
//
// It retruns the vector of results
// Optional parameters to set:
//     t_dist, prob_true, seed
//
template<typename T>
std::vector<T>
gen_negative_binomial_distribution(
    std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// Produces random non-negative integer values i, distributed according to
// discrete probability function:
//                     i
// P(i|p) = p . (1 - p)
//
// It retruns the vector of results
// Optional parameters to set:
//     prob_true, seed
//
template<typename T>
std::vector<T>
gen_geometric_distribution(
    std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// Produces random non-negative integer values i, distributed according to
// discrete probability function:
//             -u    i      
//            e     u
// P(i|u) = ------------
//               i!
//
// It retruns the vector of results
// Optional parameters to set:
//     mean, seed
//
template<typename T>
std::vector<T>
gen_poisson_distribution(std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// Produces random non-negative floating-point values x,
// distributed according to probability density function:
// P(x|λ) = λe-λx
//
// It retruns the vector of results
// Optional parameters to set:
//     lambda, seed
//
template<typename T>
std::vector<T>
gen_exponential_distribution(
    std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// Produces random non-negative floating-point values x,
// distributed according to probability density function:
// p(x∣α,β)=e−x/ββα⋅Γ(α)⋅xα−1
//
// It retruns the vector of results
// Optional parameters to set:
//     alpha, beta, seed
//
template<typename T>
std::vector<T>
gen_gamma_distribution(std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// Produces random non-negative floating-point values x,
// distributed according to probability density function:
// see https://en.cppreference.com/w/cpp/numeric/random/weibull_distribution
//
// It retruns the vector of results
// Optional parameters to set:
//     alpha (shape), beta (scale), seed
//
template<typename T>
std::vector<T>
gen_weibull_distribution(std::size_t n, const RandGenParams<T> &params = { });

// ----------------------------------------------------------------------------

// Produces random numbers according to the extreme value distribution
// (it is also known as Gumbel Type I, log-Weibull, Fisher-Tippett Type I):
// see https://en.cppreference.com/w/cpp/numeric/random/extreme_value_distribution
//
// It retruns the vector of results
// Optional parameters to set:
//     alpha (location), beta (scale), seed
//
template<typename T>
std::vector<T>
gen_extreme_value_distribution(
    std::size_t n, const RandGenParams<T> &params = { });

} // namespace hmdf

// ----------------------------------------------------------------------------

#  ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#    include <DataFrame/Internals/RandGen.tcc>
#  endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
