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


#include <algorithm>
#include <iterator>
#include <tuple>
#include <type_traits>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

// ADL (Argument-Dependent Lookup) get
// G is for Gettable
//
template<std::size_t I, typename G>
constexpr auto
adl_get(G &&tup) noexcept -> decltype(auto)  {

    return (std::get<I>(std::forward<G>(tup)));
}

template<typename T, typename G>
constexpr auto
adl_get(G &&tup) noexcept -> decltype(auto)  {

    return (std::get<T>(std::forward<G>(tup)));
}

// ----------------------------------------------------------------------------

template <std::size_t I>
using index_constant = std::integral_constant<std::size_t, I>;

// ----------------------------------------------------------------------------

// Compute the size of a type list
// LI is for List
// Ts is for Types
//
template<typename LI>
struct  list_size;

template<template<typename ...> typename LI, typename ... Ts>
struct  list_size<LI<Ts ...>> : index_constant<sizeof ...(Ts)> {  };

template<template<typename...> typename LI, typename ... Ts>
struct  list_size<LI<Ts ...> &> : index_constant<sizeof ...(Ts)> {  };

template<template<typename ...> typename LI, typename ... Ts>
struct  list_size<LI<Ts ...> &&> : index_constant<sizeof ...(Ts)> {  };

template<template<typename ...> typename LI, typename ... Ts>
struct  list_size<const LI<Ts ...> &> : index_constant<sizeof ...(Ts)> {  };

template<template<typename ...> typename LI, typename ... Ts>
struct  list_size<const LI<Ts ...> &&> : index_constant<sizeof ...(Ts)> {  };

template<template<typename ...> typename LI, typename ... Ts>
struct  list_size<volatile LI<Ts ...> &> : index_constant<sizeof ...(Ts)> {  };

template<template<typename ...> typename LI, typename ... Ts>
struct  list_size<volatile LI<Ts ...> &&> : index_constant<sizeof ...(Ts)> {  };

template<template<typename ...> typename LI, typename ... Ts>
struct  list_size<const volatile LI<Ts ...> &>
    : index_constant<sizeof ...(Ts)> {  };

template<template<typename ...> typename LI, typename ... Ts>
struct  list_size<const volatile LI<Ts ...> &&>
    : index_constant<sizeof ...(Ts)> {  };

template<typename LI>
constexpr inline std::size_t    list_size_v = list_size<LI>::value;

// ----------------------------------------------------------------------------

// Creates a type list of the specified type
// LI is for List
// Ts is for Types
//
template<template <typename ...> typename LI, typename ... Ts>
using make_list = std::type_identity<LI<Ts ...>>;

template<template <typename ...> typename LI, typename... Ts>
using make_list_t = typename make_list<LI, Ts ...>::type;

// ----------------------------------------------------------------------------

// Determine if type T is present in pack.
//
template<typename T, typename ... Ts>
struct  is_type_in_ts :
    std::bool_constant<(std::is_same_v<T, Ts> || ...)>  {   };

template<typename T>
struct  is_type_in_ts<T> : std::false_type  {   };

template<typename T, typename ... Ts>
constexpr inline bool   is_type_in_ts_v = is_type_in_ts<T, Ts ...>::value;

// ----------------------------------------------------------------------------

// Return first type in a pack. Empty pack is considered `void`.
//
template<typename ... Ts>
struct  peel_first : std::type_identity<void>  {   };

// F is for First
//
template<typename F, typename ... Ts>
struct  peel_first<F, Ts ...> : std::type_identity<F>  {   };

template<typename ... Ts>
using peel_first_t = typename peel_first<Ts ...>::type;

// Return last type in a pack. Empty pack is considered `void`.
//
template<typename ... Ts>
struct  peel_last : std::type_identity<void>  {   };

template<typename F, typename ... Ts>
struct  peel_last<F, Ts ...>
    : std::type_identity<typename peel_last<Ts ...>::type>  {   };

// L is for Last
//
template<typename L>
struct  peel_last<L> : std::type_identity<L>  {   };

template<typename ... Ts>
using peel_last_t = typename peel_last<Ts ...>::type;

// ----------------------------------------------------------------------------

// Determine if a type can be iterated over.
//
template<typename T, typename = void>
struct  is_iterable : std::false_type  {   };

template<typename T>
struct  is_iterable<T, std::void_t<decltype(std::begin(std::declval<T &>())),
                                   decltype(std::end(std::declval<T &>()))>>
    : std::true_type  {   };

template<typename T>
constexpr inline bool   is_iterable_v = is_iterable<T>::value;

// ----------------------------------------------------------------------------

// Retrieve type at index `I` within type list `LI` -- think tuple or pair.
// I is for Index
// CU is for Current index
// LI is for List
//
template<std::size_t I, typename LI, std::size_t CU = 0>
struct  at_index;

template<std::size_t I,
         template <typename ...> typename LI,
         typename T,
         typename ... Ts,
         std::size_t CU>
struct  at_index<I, LI<T, Ts ...>, CU>
    : std::conditional_t<I == CU,
                         std::type_identity<T>,
                         at_index<I, LI<Ts ...>, CU + 1>>  {
    static_assert(I - CU <= sizeof ...(Ts), "Index out of bounds");
};

template<std::size_t I, typename LI>
using at_index_t = typename at_index<I, LI>::type;

// ----------------------------------------------------------------------------

// P is for Predicate
// T_F is for transforming Function
//
template<typename IN_IT, typename OUT_IT, typename P, typename T_F>
constexpr void
transform_if(IN_IT begin,
             const IN_IT end,
             OUT_IT output_itr,
             P &&pred,
             T_F &&func) noexcept(noexcept(func(*begin)))  {

    for ( ; begin != end; ++begin)  {
        if (pred (*begin))  {
            *output_itr = func (*begin);
            ++output_itr;
        }
    }
}

// ----------------------------------------------------------------------------

// Determine if all elements of a tuple satisfy a unary predicate
// P is for Predicate
// Is is for Indices
// TU is for Tuple
//
template<typename TU, typename P, std::size_t ... Is>
[[nodiscard]] constexpr bool
_tuple_all_of_impl_(const TU &tup, P &&pred, std::index_sequence<Is ...>)  {

    static_assert(
        std::conjunction_v<std::is_invocable_r<
            bool,
            P,
            decltype(adl_get<Is>(tup))> ...>,
        "Predicate must be invocable returning a bool with every "
        "type in the tuple");

    return (pred(adl_get<Is>(tup)) && ...);
}

template<typename TU, typename P>
[[nodiscard]] constexpr bool
tuple_all_of(const TU &tup, P &&pred)  {

    constexpr auto  seq {std::make_index_sequence<list_size_v<TU>> { }};

    return (_tuple_all_of_impl_(tup, std::forward<P>(pred), seq));
}

// ----------------------------------------------------------------------------

// Split the tuple into pair of tuples each containing every other element.
// For example, think of a tuple that contains a series of begin and end
// iterators. You want to divide that into two tuples of begins and ends.
// TU is for Tuple
// Is is for Indices
//
template<typename TU, std::size_t ... Is>
[[nodiscard]] constexpr
std::pair<std::tuple<at_index_t<Is * 2, TU> ...>,
          std::tuple<at_index_t<Is * 2 + 1, TU> ...>>
_every_other_split_impl_(const TU &tup, std::index_sequence<Is ...>) noexcept  {

    return {{adl_get<Is * 2>(tup) ...}, {adl_get<Is * 2 + 1>(tup) ...}};
}

template <typename TU>
[[nodiscard]] constexpr auto
every_other_split(const TU &tup) noexcept  {

    constexpr auto  seq { std::make_index_sequence<list_size_v<TU> / 2> { } };

    return (_every_other_split_impl_(tup, seq));
}

// ----------------------------------------------------------------------------

// For_each_list is equivalent of running std::for_each sequentially on pairs
// of begin and end iterators.
// TR is for Tuple Iterator
// Is is for Indices
// F is for Function
//
template<typename F, typename TR, std::size_t ... Is>
constexpr void
_for_each_list_impl_(F &&func,
                     TR &begins,
                     TR &ends,
                     std::index_sequence<Is ...>) noexcept {

    (std::for_each(std::get<Is>(begins),
                   std::get<Is>(ends),
                   std::forward<F>(func)),
     ...);
}

// ITs is for Iterators
//
template <typename F, typename ... ITs>
constexpr void
for_each_list(F &&func, ITs ... its) noexcept  {

    static_assert(sizeof ...(ITs) % 2 == 0,
                  "Expected even number of iterators");

    // Even indices in pack (begins)
    // Odd indices in pack (ends)
    //
    static_assert(
        std::is_same_v<decltype(every_other_split(
                                    std::tuple<ITs & ...> {its ...}).first),
                       decltype(every_other_split(
                                    std::tuple<ITs & ...> {its ...}).second)>,
            "Begin-End pairs must be the same type of iterator");

    auto    [begins, ends] {every_other_split(std::tuple<ITs & ...>(its ...))};

    _for_each_list_impl_(std::forward<F>(func),
                         begins,
                         ends,
                         std::make_index_sequence<sizeof ...(ITs) / 2> {});
}

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct  overload : Ts ...  { using Ts::operator() ...; };

/*
Now you can do somthing like this
int main()  {

    overload    f = {
        [](int i)   { std::cout << "int thingy\n"; },
        [](float f)   { std::cout << "float thingy\n"; },
    };

    f(2);     // Prints int thingy
    f(2.0f);  // Prints float thingy

    std::vairant<int, float>    v = 2.0f;

    std::visit(f, v);  // Prints float thingy
}
*/

// --------------------------------------------

/*
// Functional programming with Lambdas -- decorator
//
auto twice = [] (auto &&f)  {

    return ([=] { f(); f();  ));
}

auto print_hihi = twice([] { std::cout << "hi" ; });
print_hihi;
*/

// --------------------------------------------

/*
// Immediately executed lambda
//
const Foo   foo = [&]  {
    if (has_database)
        return (get_foo_from_database());
    else
        return (get_foo());
 }();  // () immediately executes the lambda

std::vector<Foo>    foos;

foos.emplace_back([&]  {
    if (has_database)
        return (get_foo_from_database());
    else
        return (get_foo());
 }());

// or equivalently

foos.emplace_back(std::invoke([&]  {
    if (has_database)
        return (get_foo_from_database());
    else
        return (get_foo());
 }));
*/

// --------------------------------------------

/*
// If you want to run a peice if code only once in your program
//
struct  X  {
    X()  {
        static auto _ = [] {
            std::cout << "Called once!\n";
            return (0);
        }();
    }
};
*/

// --------------------------------------------

/*
template<auto = [] {}>
struct  X { };

X   x1;
X   x2;
// x1 and x2 are two different types
*/

// --------------------------------------------

/*
// Recursive lambda to calculate factorial
//
auto f = [](auto &&self, int i)  {
    if (i == 0)
        return (1);
    return (i * self(self, i - 1));
}

std::cout << f(f, 5) << std::endl;
*/

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
