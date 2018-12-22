// Hossein Moein
// December 22, 2017
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
                  "Only a StdDataFrame can call shift()");

    throw NotImplemented("Shift is not implemented yet");
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
