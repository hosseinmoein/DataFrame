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


#include <concepts>
#include <functional>
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
concept hashable = requires (const std::remove_reference_t<T> &a)  {
    { std::hash<T>{}(a) } -> std::convertible_to<std::size_t>;
};

template<typename T>
concept hashable_equal = hashable<T> && std::equality_comparable<T>;

template<typename T>
concept hashable_stringable = requires(const std::remove_reference_t<T> &a,
									   std::stringstream &ss)  {
    hashable<T>;
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
