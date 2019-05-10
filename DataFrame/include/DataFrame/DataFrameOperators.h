// Hossein Moein
// February 1, 2019
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#ifndef HMDF_DATAFRAMEOPERATORS_HPP
#define HMDF_DATAFRAMEOPERATORS_HPP

#include "dllexports/DataFrame_lib_exports.h"

#include "DataFrame.h"
// #include <execution>

// ----------------------------------------------------------------------------

namespace hmdf
{
#if defined(WIN32) || defined (_WIN32)
#undef min
#undef max
#endif // defined(WIN32) || defined (_WIN32)

// Both lhs and rhs must be already sorted by index, otherwise the result
// is nonsensical.
//
template<typename DF, template<typename> class OPT, typename ... types>
DF binary_operation (const DF &lhs, const DF &rhs)  {

    typename DF::TSVec          result_idx;
    const typename DF::TSVec    &lhs_ts_vec = lhs.get_index();
    const typename DF::TSVec    &rhs_ts_vec = rhs.get_index();

    result_idx.reserve(std::min(lhs_ts_vec.size(), rhs_ts_vec.size()));
    std::set_intersection(// std::execution::parallel_policy,
                          lhs_ts_vec.begin(), lhs_ts_vec.end(),
                          rhs_ts_vec.begin(), rhs_ts_vec.end(),
                          std::back_inserter(result_idx));

    DF  result;

    result.load_index(std::move(result_idx));

    const typename DF::TSVec    &new_idx = result.get_index();

    for (const auto &lhs_citer : lhs.data_tb_)  {
        const auto  rhs_citer = rhs.data_tb_.find(lhs_citer.first.c_str());

        if (rhs_citer == rhs.data_tb_.end())  continue;

        typename DF::template operator_functor_
            <typename DF::TimeStamp, OPT, types ...>  functor (
                lhs_ts_vec,
                rhs_ts_vec,
                new_idx,
                rhs,
                lhs_citer.first.c_str(),
                result);

        lhs.data_[lhs_citer.second].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

//
// These arithmetic operations operate on the same-name and same_type columns
// in lhs and rhs, if the entry has the same index value.
// They return a new DataFrame
//
// NOTE: Both lhs and rhs must be already sorted by index, otherwise the
//       result is nonsensical.
//

template<typename DF, typename ... types>
inline DF df_plus (const DF &lhs, const DF &rhs)  {

    return (binary_operation<DF, std::plus, types ...>(lhs, rhs));
}

// ----------------------------------------------------------------------------

template<typename DF, typename ... types>
inline DF df_minus (const DF &lhs, const DF &rhs)  {

    return (binary_operation<DF, std::minus, types ...>(lhs, rhs));
}

// ----------------------------------------------------------------------------

template<typename DF, typename ... types>
inline DF df_multiplies (const DF &lhs, const DF &rhs)  {

    return (binary_operation<DF, std::multiplies, types ...>(lhs, rhs));
}

// ----------------------------------------------------------------------------

template<typename DF, typename ... types>
inline DF df_divides (const DF &lhs, const DF &rhs)  {

    return (binary_operation<DF, std::divides, types ...>(lhs, rhs));
}

} // namespace hmdf

#endif //HMDF_DATAFRAMEOPERATORS_HPP

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
