// Hossein Moein
// December 30 2020
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

#include <cmath>
#include <iterator>
#include <limits>
#include <tuple>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename ... Ts, typename F, std::size_t ... Is>
inline void
in_tuple_helper(const std::tuple<Ts ...> &tu,
                F func,
                std::index_sequence<Is ...>)  {

    using expander = int[];

    (void) expander { 0, (func(std::get<Is>(tu)), 0) ... };
}

// ----------------------------------------------------------------------------

template<typename ... Ts, typename F, std::size_t ... Is>
inline void
in_tuple_helper(std::tuple<Ts ...> &tu, F func, std::index_sequence<Is ...>)  {

    using expander = int[];

    (void) expander { 0, (func(std::get<Is>(tu)), 0) ... };
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

template<typename F, typename H, typename ... T>
inline void unpacker(F func, H &head, T& ... tail);

template<typename F, typename H>
inline void head_exec(F func, H &head)  { func(head); }

template<typename F>
inline void unpacker(F func)  {   }

template<typename F, typename H, typename ... T>
inline void unpacker(F func, H &head, T& ... tail)  {

    head_exec(func, head);
    unpacker(func, tail ...);
}

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

// ----------------------------------------------------------------------------

template<typename T>
inline constexpr bool
is_nan(const T &val)  {

    if (std::numeric_limits<T>::has_quiet_NaN)
        return (is_nan__(val));
    return (get_nan<T>() == val);
}

// ----------------------------------------------------------------------------

template<typename V>
inline bool
is_monotonic_increasing(const V &column)  {

    const std::size_t   col_s = column.size();

    for (std::size_t i = 1; i < col_s; ++i)
        if (column[i] < column[i - 1])
            return (false);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename V>
inline bool
is_strictly_monotonic_increasing(const V &column)  {

    const std::size_t   col_s = column.size();

    for (std::size_t i = 1; i < col_s; ++i)
        if (column[i] <= column[i - 1])
            return (false);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename V>
inline bool
is_monotonic_decreasing(const V &column)  {

    const std::size_t   col_s = column.size();

    for (std::size_t i = 1; i < col_s; ++i)
        if (column[i] > column[i - 1])
            return (false);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename V>
inline bool
is_strictly_monotonic_decreasing(const V &column)  {

    const std::size_t   col_s = column.size();

    for (std::size_t i = 1; i < col_s; ++i)
        if (column[i] >= column[i - 1])
            return (false);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename V>
inline void
shift_right(V &vec, std::size_t n)  {

    using value_type = typename V::value_type;

    const auto  vec_rend = vec.rend();

    for (auto riter = vec.rbegin(); riter != vec_rend; ++riter)  {
        if (std::size_t(std::distance(riter, vec_rend)) > n)
            *riter = std::move(*(riter + n));
        else
            *riter = std::move(get_nan<value_type>());
    }
}

// ----------------------------------------------------------------------------

template<typename V>
inline void
shift_left(V &vec, std::size_t n)  {

    using value_type = typename V::value_type;

    const auto  vec_end = vec.end();

    for (auto iter = vec.begin(); iter != vec_end; ++iter)  {
        if (std::size_t(std::distance(iter, vec_end)) > n)
            *iter = std::move(*(iter + n));
        else
            *iter = std::move(get_nan<value_type>());
    }
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
