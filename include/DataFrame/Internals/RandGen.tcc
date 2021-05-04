// Hossein Moein
// July 22, 2019
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

#include <DataFrame/RandGen.h>

#include <cmath>
#include <functional>
#include <random>
#include <type_traits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, typename D>
static inline std::vector<T>
_gen_rand_(std::size_t n, const RandGenParams<T> &params, D &&dist)  {

    std::random_device  rd;
    std::mt19937_64     gen(rd());

    if (params.seed != static_cast<unsigned int>(-1))  gen.seed(params.seed);

    std::vector<T>  result(n);

    for (auto iter = result.begin(); iter < result.end(); ++iter)
        *iter = dist(gen);
    return (result);
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_uniform_int_dist(std::size_t n, const RandGenParams<T> &params)  {

    static_assert(std::is_same<char, T>::value ||
                  std::is_same<unsigned char, T>::value ||
                  std::is_same<short int, T>::value ||
                  std::is_same<unsigned short int, T>::value ||
                  std::is_same<int, T>::value ||
                  std::is_same<unsigned int, T>::value ||
                  std::is_same<long int, T>::value ||
                  std::is_same<unsigned long int, T>::value ||
                  std::is_same<long long int, T>::value ||
                  std::is_same<unsigned long long int, T>::value,
                  "gen_uniform_int_dist() requires whole number type");

    using D = std::uniform_int_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::uniform_int_distribution<T>(params.min_value,
                                                 params.max_value)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_uniform_real_dist(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_uniform_real_dist() requires real number type");

    using D = std::uniform_real_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::uniform_real_distribution<T>(params.min_value,
                                                  params.max_value)));
}

// ----------------------------------------------------------------------------

std::vector<bool>
gen_bernoulli_dist(std::size_t n, const RandGenParams<bool> &params) {

    using D = std::bernoulli_distribution;

    return (_gen_rand_<bool, D>(
                n, params,
                std::bernoulli_distribution(params.prob_true)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_binomial_dist(std::size_t n, const RandGenParams<T> &params)  {

    static_assert(std::is_same<char, T>::value ||
                  std::is_same<unsigned char, T>::value ||
                  std::is_same<short int, T>::value ||
                  std::is_same<unsigned short int, T>::value ||
                  std::is_same<int, T>::value ||
                  std::is_same<unsigned int, T>::value ||
                  std::is_same<long int, T>::value ||
                  std::is_same<unsigned long int, T>::value ||
                  std::is_same<long long int, T>::value ||
                  std::is_same<unsigned long long int, T>::value,
                  "gen_binomial_dist() requires whole number type");

    using D = std::binomial_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::binomial_distribution<T>(static_cast<T>(params.t_dist),
                                              params.prob_true)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_negative_binomial_dist(std::size_t n, const RandGenParams<T> &params)  {

    static_assert(std::is_same<char, T>::value ||
                  std::is_same<unsigned char, T>::value ||
                  std::is_same<short int, T>::value ||
                  std::is_same<unsigned short int, T>::value ||
                  std::is_same<int, T>::value ||
                  std::is_same<unsigned int, T>::value ||
                  std::is_same<long int, T>::value ||
                  std::is_same<unsigned long int, T>::value ||
                  std::is_same<long long int, T>::value ||
                  std::is_same<unsigned long long int, T>::value,
                  "gen_negative_binomial_dist() requires whole number type");

    using D = std::negative_binomial_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::negative_binomial_distribution<T>
                    (static_cast<T>(params.t_dist), params.prob_true)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_geometric_dist(std::size_t n, const RandGenParams<T> &params)  {

    static_assert(std::is_same<char, T>::value ||
                  std::is_same<unsigned char, T>::value ||
                  std::is_same<short int, T>::value ||
                  std::is_same<unsigned short int, T>::value ||
                  std::is_same<int, T>::value ||
                  std::is_same<unsigned int, T>::value ||
                  std::is_same<long int, T>::value ||
                  std::is_same<unsigned long int, T>::value ||
                  std::is_same<long long int, T>::value ||
                  std::is_same<unsigned long long int, T>::value,
                  "gen_geometric_dist() requires whole number type");

    using D = std::geometric_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::geometric_distribution<T>(params.prob_true)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_poisson_dist(std::size_t n, const RandGenParams<T> &params)  {

    static_assert(std::is_same<char, T>::value ||
                  std::is_same<unsigned char, T>::value ||
                  std::is_same<short int, T>::value ||
                  std::is_same<unsigned short int, T>::value ||
                  std::is_same<int, T>::value ||
                  std::is_same<unsigned int, T>::value ||
                  std::is_same<long int, T>::value ||
                  std::is_same<unsigned long int, T>::value ||
                  std::is_same<long long int, T>::value ||
                  std::is_same<unsigned long long int, T>::value,
                  "gen_poisson_dist() requires whole number type");

    using D = std::poisson_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::poisson_distribution<T>(params.mean)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_exponential_dist(std::size_t n, const RandGenParams<T> &params)  {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_exponential_dist() requires real number type");

    using D = std::exponential_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::exponential_distribution<T>(params.lambda)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_gamma_dist(std::size_t n, const RandGenParams<T> &params)  {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_gamma_dist() requires real number type");

    using D = std::gamma_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::gamma_distribution<T>(params.alpha, params.beta)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_weibull_dist(std::size_t n, const RandGenParams<T> &params)  {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_weibull_dist() requires real number type");

    using D = std::weibull_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::weibull_distribution<T>(params.alpha, params.beta)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_extreme_value_dist(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_extreme_value_dist() requires real number type");

    using D = std::extreme_value_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::extreme_value_distribution<T>(params.alpha, params.beta)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_normal_dist(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_normal_dist() requires real number type");

    using D = std::normal_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::normal_distribution<T>(params.mean, params.std)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_lognormal_dist(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_lognormal_dist() requires real number type");

    using D = std::lognormal_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::lognormal_distribution<T>(params.m, params.s)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_chi_squared_dist(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_chi_squared_dist() requires real number type");

    using D = std::chi_squared_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::chi_squared_distribution<T>(params.n)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_cauchy_dist(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_cauchy_dist() requires real number type");

    using D = std::cauchy_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::cauchy_distribution<T>(params.alpha, params.beta)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_fisher_f_dist(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_fisher_f_dist() requires real number type");

    using D = std::fisher_f_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::fisher_f_distribution<T>(params.n, params.n2)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_student_t_dist(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_student_t_dist() requires real number type");

    using D = std::student_t_distribution<T>;

    return (_gen_rand_<T, D>(
                n, params,
                std::student_t_distribution<T>(params.n)));
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_log_space_nums(std::size_t n, T first, T last, T base)  {

    const T         step = (last - first) / (T(n) - T(1));
    T               current = first;
    std::vector<T>  result (n);

    for (auto iter = result.begin(); iter < result.end(); ++iter)  {
        const T val = ::pow(base, current);

        current += step;
        *iter = val;
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_even_space_nums(std::size_t n, T first, T last)  {

    const T         step = (last - first) / T(n);
    std::vector<T>  result;

    result.reserve(n + 1); // Make it efficient, if user wants to add last
    result.push_back(first);
    for (std::size_t i = 1; i < n; ++i)
        result.push_back(result[i - 1] + step);

    return (result);
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_triangular_nums(T last, T first)  {

    std::vector<T>  result;

    // With arbitrary first and last values, it is hard to do result.reserve();
    //
    for (T i = first, val = i * (i + T(1)) / T(2);
         val <= last;
         ++i, val = i * (i + T(1)) / T(2))
        result.push_back(val);

    result.shrink_to_fit();
    return (result);
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_sym_triangle(std::size_t n, const T &start_val, bool normalize)  {

    std::vector<T>      result;
    const bool          is_even = ! (n & std::size_t(0x01)); 
    const std::size_t   max_loop { n / 2 + (is_even ? 0 : 1) };
    T                   sum { 0 };

    result.reserve(n);
    for (std::size_t i = 0; i < max_loop; ++i)  {
        const T val = start_val + i;

        result.push_back(val);
        sum += val;
    }
    for (long i = max_loop - (is_even ? 1 : 2); i >= 0; --i)  {
        const T &val = result[i];

        result.push_back(val);
        sum += val;
    }
    if (normalize)
        std::transform(result.begin(), result.end(), result.begin(),
                       std::bind(std::divides<T>(), std::placeholders::_1,
                                 sum));

    return (result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
