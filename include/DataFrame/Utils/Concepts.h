// Hossein Moein
// June 05 2023
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


#include <DataFrame/Utils/FixedSizeString.h>

#include <concepts>
#include <functional>
#include <iterator>
#include <sstream>
#include <string>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
concept arithmetic = requires (const std::remove_reference_t<T> &a,
                               const std::remove_reference_t<T> &b)  {
    { a + b };
    { a - b };
    { a * b };
    { a / b };
};

// ----------------------------------------------------------------------------

template<typename T>
concept addable = requires (const std::remove_reference_t<T> &a,
                            const std::remove_reference_t<T> &b)  {
    { a + b };
    { a - b };
};

// ----------------------------------------------------------------------------

template<typename T>
concept comparable = requires (const std::remove_reference_t<T> &a,
                               const std::remove_reference_t<T> &b)  {
    { a > b } -> std::convertible_to<bool>;
    { a < b } -> std::convertible_to<bool>;
    { a >= b } -> std::convertible_to<bool>;
    { a <= b } -> std::convertible_to<bool>;
    { a == b } -> std::convertible_to<bool>;
    { a != b } -> std::convertible_to<bool>;
};

// ----------------------------------------------------------------------------

template<typename T>
concept container = requires(T t)  {
    typename T::value_type;
    typename T::size_type;
    typename T::iterator;
    typename T::const_iterator;
    t.size();
    t.begin();
    t.end();
    t.cbegin();
    t.cend();
};

// ----------------------------------------------------------------------------

template<class T>
concept forward_iterator =
    std::input_iterator<T> &&
    std::constructible_from<T> &&
    std::is_reference_v<std::iter_reference_t<T>> &&
    std::same_as<std::remove_cvref_t<std::iter_reference_t<T>>,
                 typename std::indirectly_readable_traits<T>::value_type> &&
    requires(T it)  {
        { ++it } -> std::same_as<T &>;
        { it++ } -> std::convertible_to<const T &>;
        { *it++ } -> std::same_as<std::iter_reference_t<T>>;
    };

// ----------------------------------------------------------------------------

template<typename T>
concept bidirectional_iterator =
    forward_iterator<T> &&
    requires(T it)  {
        { --it } -> std::same_as<T &>;
        { it-- } -> std::convertible_to<const T &>;
        { *it-- } -> std::same_as<std::iter_reference_t<T>>;
    };

// ----------------------------------------------------------------------------

template<typename T>
concept hashable = requires (const std::remove_reference_t<T> &a)  {
    { std::hash<T>{}(a) } -> std::convertible_to<std::size_t>;
};

template<typename T>
concept hashable_equal = hashable<T> && std::equality_comparable<T>;

template<typename T>
concept hashable_stringable = requires(const std::remove_reference_t<T> &a,
                                       std::stringstream &ss)  {
    requires hashable<T>;
    { ss << a };
};

// ----------------------------------------------------------------------------

template<typename T>
concept has_result = requires (const std::remove_reference_t<T> &a)  {
    { a.get_result() };
    typename T::result_type;
};

// ----------------------------------------------------------------------------

template<typename F, typename U, typename V>
concept replace_callable = requires (F &&f, const U &u, V &v)  {
    std::invoke(std::forward<F>(f), u, v);
};

// ----------------------------------------------------------------------------

// This is to be used in for_each_list algorithm
// IT1 is for Iterator 1
// IT2 is for Iterator 2
//
template<typename F, typename IT1, typename IT2>
concept iter_invokale2 =
    requires (F &&f,
              std::remove_reference_t<IT1> &it1,
              std::remove_reference_t<IT2> &it2)  {
    std::invoke(std::forward<F>(f), it1, it2);
};

template<typename F, typename IT1, typename IT2, typename IT3>
concept iter_invokale3 =
    requires (F &&f,
              std::remove_reference_t<IT1> &it1,
              std::remove_reference_t<IT2> &it2,
              std::remove_reference_t<IT3> &it3)  {
    std::invoke(std::forward<F>(f), it1, it2, it3);
};

template<typename F, typename IT1, typename IT2, typename IT3, typename IT4>
concept iter_invokale4 =
    requires (F &&f,
              std::remove_reference_t<IT1> &it1,
              std::remove_reference_t<IT2> &it2,
              std::remove_reference_t<IT3> &it3,
              std::remove_reference_t<IT4> &it4)  {
    std::invoke(std::forward<F>(f), it1, it2, it3, it4);
};

template<typename F,
         typename IT1, typename IT2, typename IT3, typename IT4, typename IT5>
concept iter_invokale5 =
    requires (F &&f,
              std::remove_reference_t<IT1> &it1,
              std::remove_reference_t<IT2> &it2,
              std::remove_reference_t<IT3> &it3,
              std::remove_reference_t<IT4> &it4,
              std::remove_reference_t<IT5> &it5)  {
    std::invoke(std::forward<F>(f), it1, it2, it3, it4, it5);
};

// ----------------------------------------------------------------------------

template<typename T>
concept StringOnly =
    std::is_same_v<T, std::string> ||
    std::is_same_v<T, VirtualString> ||
    std::is_same_v<T, char *> ||
    std::is_same_v<T, const char *>;

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
