// Hossein Moein
// December 27, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DataFrame.h"

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename TS, typename HETERO>
template<typename ... types>
void DataFrame<TS, HETERO>::self_shift(size_type periods, shift_policy sp)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call self_shift()");

    if (periods > 0)  {
        vertical_shift_functor_<types ...> functor(periods, sp);

        for (auto &iter : data_)
            iter.change(functor);
    }
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
shift(size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call shift()");

    StdDataFrame<TS>    slug = *this;

    slug.template self_shift<types ...>(periods, sp);
    return (slug);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename V>
void DataFrame<TS, HETERO>::shift_right_(V &vec, size_type n)  {

    using value_type =
        typename std::remove_reference<decltype(vec)>::type::value_type;

    const auto  vec_rend = vec.rend();

    for (auto riter = vec.rbegin(); riter != vec_rend; ++riter)
        if (riter + n < vec_rend)
            *riter = std::move(*(riter + n));
        else
            *riter = std::move(_get_nan<value_type>());

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename V>
void DataFrame<TS, HETERO>::shift_left_(V &vec, size_type n)  {

    using value_type =
        typename std::remove_reference<decltype(vec)>::type::value_type;

    const auto  vec_end = vec.end();

    for (auto iter = vec.begin(); iter != vec_end; ++iter)
        if (iter + n < vec_end)
            *iter = std::move(*(iter + n));
        else
            *iter = std::move(_get_nan<value_type>());

    return;
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
