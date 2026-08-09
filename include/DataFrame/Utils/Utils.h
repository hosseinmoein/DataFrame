// Hossein Moein
// December 30 2020
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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename ... Ts, typename F, std::size_t ... Is>
inline void
in_tuple_helper(const std::tuple<Ts ...> &tu,
                F func,
                std::index_sequence<Is ...>)  {

    (func(std::get<Is>(tu)), ...);
}

// ----------------------------------------------------------------------------

template<typename ... Ts, typename F, std::size_t ... Is>
inline void
in_tuple_helper(std::tuple<Ts ...> &tu, F func, std::index_sequence<Is ...>)  {

    (func(std::get<Is>(tu)), ...);
}

// ----------------------------------------------------------------------------

template<typename ... Ts, typename F>
inline void
for_each_in_tuple(const std::tuple<Ts...> &tu, F func)  {

    in_tuple_helper(tu, func, std::make_index_sequence<sizeof...(Ts)>());
}

// ----------------------------------------------------------------------------

template<typename ... Ts, typename F>
inline void
for_each_in_tuple(std::tuple<Ts...> &tu, F func)  {

    in_tuple_helper(tu, func, std::make_index_sequence<sizeof...(Ts)>());
}

// ----------------------------------------------------------------------------

template <typename Tuple, typename T>
struct tuple_contain  {   };

template <typename First, typename... Rest, typename T>
struct tuple_contain<std::tuple<First, Rest ...>, T>
    : tuple_contain<std::tuple<Rest...>, T>  {   };

template <typename... Rest, typename T>
struct tuple_contain<std::tuple<T, Rest...>, T> : std::true_type  {   };

template <typename T>
struct tuple_contain<std::tuple<>, T> : std::false_type  {   };

template <class Out, class In>
struct tuple_filter;

template <class ... Out, class InFirst, class ... InRest>
struct tuple_filter<std::tuple<Out...>, std::tuple<InFirst, InRest...>>  {

    using type =
        typename std::conditional<
            tuple_contain<std::tuple<Out...>, InFirst>::value,
            typename tuple_filter<std::tuple<Out...>,
                                  std::tuple<InRest...>>::type,
            typename tuple_filter<std::tuple<Out..., InFirst>,
                                  std::tuple<InRest...>>::type
        >::type;
};

template <class Out>
struct tuple_filter<Out, std::tuple<>>  { using type = Out; };

template <class Tuple>
using make_tuple_types_unique =
    typename tuple_filter<std::tuple<>, Tuple>::type;

template <typename Out, typename Tuple>
struct append_inner_type;

template <class... Out, class InFirst, class... InRest>
struct append_inner_type<std::tuple<Out...>, std::tuple<InFirst, InRest...>> {

    using type =
        typename append_inner_type<std::tuple<typename InFirst::type, Out...>,
                                   std::tuple<InRest...>>::type;
};

template <typename Out>
struct append_inner_type<Out, std::tuple<>>  { using type = Out; };

template <typename... Args>
using projection_list = make_tuple_types_unique<
    typename append_inner_type<std::tuple<>, std::tuple<Args...>>::type>;

template <typename... Args>
using projection_type_list = make_tuple_types_unique<std::tuple<Args...>>;

// ----------------------------------------------------------------------------

template<typename T>
inline bool
is_nan__(const T &)  { return (false); }

template<>
inline bool
is_nan__<double>(const double &val)  { return(std::isnan(val)); }

template<>
inline bool
is_nan__<float>(const float &val)  { return(std::isnan(val)); }

template<>
inline bool
is_nan__<long double>(const long double &val)  { return(std::isnan(val)); }

// ----------------------------------------------------------------------------

template<typename T>
inline T
get_nan()  { return (T()); }

template<>
inline double
get_nan<double>()  { return (std::numeric_limits<double>::quiet_NaN()); }

template<>
inline long double
get_nan<long double>()  {

    return (std::numeric_limits<long double>::quiet_NaN());
}

template<>
inline float
get_nan<float>()  { return (std::numeric_limits<float>::quiet_NaN()); }

template<>
inline DateTime
get_nan<DateTime>()  { return (DateTime { DateTime::DateType(19700101) }); }

// ----------------------------------------------------------------------------

template<typename T>
inline constexpr bool
is_nan(const T &val)  {

    if constexpr (std::numeric_limits<T>::has_quiet_NaN)
        return (is_nan__(val));
    return (get_nan<T>() == val);
}

// ----------------------------------------------------------------------------

template<typename T>
inline auto
abs__(const T &val)  { return (val); }

template<>
inline auto
abs__<char>(const char &val)  { return (char(std::abs(int(val)))); }

template<>
inline auto
abs__<short>(const short &val)  { return (short(std::abs(int(val)))); }

template<>
inline auto
abs__<int>(const int &val)  { return (std::abs(val)); }

template<>
inline auto
abs__<long>(const long &val)  { return (std::labs(val)); }

template<>
inline auto
abs__<long long>(const long long &val)  { return (std::llabs(val)); }

template<>
inline auto
// abs__<float>(const float &val)  { return (std::fabsf(val)); }
abs__<float>(const float &val)  { return (std::fabs(val)); }

template<>
inline auto
abs__<double>(const double &val)  { return (std::fabs(val)); }

template<>
inline auto
// abs__<long double>(const long double &val)  { return (std::fabsl(val)); }
abs__<long double>(const long double &val)  { return (std::fabs(val)); }

// ----------------------------------------------------------------------------

// This should work for both std::map and std::unordered_map
//
namespace mappish_detail {
  template<typename T, typename U = void>
  struct  is_mappish_impl : std::false_type {  };

  template<typename T>
  struct  is_mappish_impl<
      T,
      std::void_t<typename T::key_type,
                  typename T::mapped_type,
                  decltype(std::declval<T &>()
                           [std::declval<const typename T::key_type &>()])>>
      : std::true_type {  };
}  // mappish_detail

template<typename T>
struct  is_mappish : mappish_detail::is_mappish_impl<T>::type {  };

// ----------------------------------------------------------------------------

template<typename V>
[[nodiscard]] inline bool
is_monotonic_increasing(const V &column)  {

    return (std::is_sorted(column.begin(), column.end()));
}

// ----------------------------------------------------------------------------

// std::sort requires a strict weak ordering comparison, which means the
// comparison function must return false for equal elements (using <). If you
// use <=, sorting algorithms cannot properly handle equivalent elements,
// leading to broken internal logic, infinite loops, or memory corruption.
//
template<typename V>
[[nodiscard]] inline bool
is_strictly_monotonic_increasing(const V &column)  {

    using value_t = typename V::value_type;

    return (std::adjacent_find(column.begin(), column.end(), 
                               [](const value_t &prev, const value_t &next) {
                                   return (next <= prev);
                               }) == column.end());
}

// ----------------------------------------------------------------------------

template<typename V>
[[nodiscard]] inline bool
is_monotonic_decreasing(const V &column)  {

    using value_t = typename V::value_type;

    return (std::is_sorted(column.begin(),
                           column.end(),
                           std::greater<value_t>()));
}

// ----------------------------------------------------------------------------

// std::sort requires a strict weak ordering comparison, which means the
// comparison function must return false for equal elements (using <). If you
// use <=, sorting algorithms cannot properly handle equivalent elements,
// leading to broken internal logic, infinite loops, or memory corruption.
//
template<typename V>
[[nodiscard]] inline bool
is_strictly_monotonic_decreasing(const V &column)  {

    using value_t = typename V::value_type;

    return (std::adjacent_find(column.begin(), column.end(), 
                               [](const value_t &prev, const value_t &next) {
                                   return (next >= prev);
                               }) == column.end());
}

// ----------------------------------------------------------------------------

template<typename V>
inline void
shift_right(V &vec, std::size_t n)  {

    using value_type = typename V::value_type;

    const std::size_t vec_s { vec.size() };
    const std::size_t fill_n { std::min(n, vec_s) };

    std::shift_right(vec.begin(), vec.end(),
                     static_cast<typename V::difference_type>(n));
    std::fill_n(vec.begin(), fill_n, get_nan<value_type>());
}

// ----------------------------------------------------------------------------

template<typename V>
inline void
shift_left(V &vec, std::size_t n)  {

    using value_type = typename V::value_type;

    const std::size_t vec_s { vec.size() };
    const std::size_t fill_n { std::min(n, vec_s) };

    std::shift_left(vec.begin(), vec.end(),
                    static_cast<typename V::difference_type>(n));
    std::fill_n(vec.end() - fill_n, fill_n, get_nan<value_type>());
}

// ----------------------------------------------------------------------------

template<typename STR, std::size_t SIZ = 64 * 1024>
struct  IOStreamOpti  {

    IOStreamOpti (STR &stream, const char *file_name, bool binary = false)
        : stream_(stream),
          tie_(std::cin.tie(nullptr)),
          sync_(std::ios_base::sync_with_stdio(false))  {

        stream_.rdbuf()->pubsetbuf(buffer_, SIZ);
        if (file_name && ! stream_.is_open())  {
            if (! binary)
                stream_.open(file_name);
            else
                stream_.open(file_name, std::ios::binary);

#ifdef HMDF_SANITY_EXCEPTIONS
            if (stream.fail()) [[unlikely]]  {
                String1K    err;

                err.printf("IOStreamOpti: ERROR: Unable to open file '%s'",
                           file_name);
                throw DataFrameError(err.c_str());
            }
#endif // HMDF_SANITY_EXCEPTIONS
        }
    }

    ~IOStreamOpti ()  {

        stream_.close();
        std::ios_base::sync_with_stdio(sync_);
        std::cin.tie(tie_);
    }

    IOStreamOpti () = delete;
    IOStreamOpti (const IOStreamOpti &) = delete;
    IOStreamOpti &operator = (const IOStreamOpti &) = delete;

private:

    char            buffer_[SIZ];
    STR             &stream_;
    std::ostream    *tie_;
    const bool      sync_;
};

// ----------------------------------------------------------------------------

template<std::size_t SIZ = 64 * 1024>
struct  IOFileOpti  {

    std::FILE   *file;

    explicit
    IOFileOpti(const char *file_name, bool binary = false)
        : file(std::fopen(file_name, binary ? "rb" : "r"))  {

#ifdef HMDF_SANITY_EXCEPTIONS
        if (! file) [[unlikely]]  {
            String1K    err;

            err.printf("IOFileOpti: ERROR: Unable to open file '%s'",
                       file_name);
            throw DataFrameError(err.c_str());
        }
#endif // HMDF_SANITY_EXCEPTIONS
        std::setvbuf(file, buffer_, _IOFBF, SIZ);
    }

    ~IOFileOpti ()  { if (file)  std::fclose(file); }

    IOFileOpti () = delete;
    IOFileOpti (const IOFileOpti &) = delete;
    IOFileOpti &operator = (const IOFileOpti &) = delete;

private:

    char    buffer_[SIZ];
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
