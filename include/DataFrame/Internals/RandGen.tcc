// Hossein Moein
// July 22, 2019
// Copyright (C) 2019-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/RandGen.h>

#include <type_traits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
std::vector<T>
gen_uniform_int_distribution(std::size_t n, const RandGenParams<T> &params)  {

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
                  "gen_uniform_int_distribution() requires whole number type");

    std::random_device  rd;
    std::mt19937        gen(rd());

    if (params.seed != std::size_t(-1))  gen.seed(params.seed);

    std::uniform_int_distribution<T>    dis(params.min_value,
                                            params.max_value);
    std::vector<T>                      result(n);

    for (std::size_t i = 0; i < n; ++i)
        result[i] = dis(gen);
    return (result);
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_uniform_real_distribution(std::size_t n, const RandGenParams<T> &params) {

    static_assert(std::is_same<float, T>::value ||
                  std::is_same<double, T>::value ||
                  std::is_same<long double, T>::value,
                  "gen_uniform_real_distribution() requires real number type");

    std::random_device  rd;
    std::mt19937        gen(rd());

    if (params.seed != std::size_t(-1))  gen.seed(params.seed);

    std::uniform_real_distribution<T>   dis(params.min_value,
                                            params.max_value);
    std::vector<T>                      result(n);

    for (std::size_t i = 0; i < n; ++i)
        result[i] = dis(gen);
    return (result);
}

// ----------------------------------------------------------------------------

std::vector<bool>
gen_bernoulli_distribution(std::size_t n, const RandGenParams<bool> &params) {

    std::random_device  rd;
    std::mt19937        gen(rd());

    if (params.seed != std::size_t(-1))  gen.seed(params.seed);

    std::bernoulli_distribution dis(params.prob_true);
    std::vector<bool>           result(n);

    for (std::size_t i = 0; i < n; ++i)
        result[i] = dis(gen);
    return (result);
}

// ----------------------------------------------------------------------------

template<typename T>
std::vector<T>
gen_binomial_distribution(std::size_t n, const RandGenParams<T> &params)  {

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
                  "gen_binomial_distribution() requires whole number type");

    std::random_device  rd;
    std::mt19937        gen(rd());

    if (params.seed != std::size_t(-1))  gen.seed(params.seed);

    std::binomial_distribution<T>   dis(params.t_dist, params.prob_true);
    std::vector<T>                  result(n);

    for (std::size_t i = 0; i < n; ++i)
        result[i] = dis(gen);
    return (result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
