// Hossein Moein
// July 22, 2019
// Copyright (C) 2019-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/RandGen.h>

#include <random>
#include <type_traits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, typename D>
static inline std::vector<T>
_gen_rand_(std::size_t n, const RandGenParams<T> &params, D &&dist)  {

    std::random_device  rd;
    std::mt19937        gen(rd());

    if (params.seed != (unsigned int)-1)  gen.seed(params.seed);

    std::vector<T>  result(n);

    for (std::size_t i = 0; i < n; ++i)
        result[i] = dist(gen);
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
                std::binomial_distribution<T>(params.t_dist,
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
                std::negative_binomial_distribution<T>(params.t_dist,
                                                       params.prob_true)));
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
