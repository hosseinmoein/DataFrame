// Hossein Moein
// February 1, 2019
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

// #include <execution>

// ----------------------------------------------------------------------------

namespace hmdf
{

#ifdef _MSC_VER
#  ifdef min
#    undef min
#  endif // min
#  ifdef max
#    undef max
#  endif // max
#endif // _MSC_VER

// Both lhs and rhs must be already sorted by index, otherwise the result
// is nonsensical.
//
template<typename DF, template<typename> class OPT, typename ... Ts>
DF binary_operation (const DF &lhs, const DF &rhs)  {

    typename DF::IndexVecType       result_idx;

    const typename DF::IndexVecType &lhs_ts_vec = lhs.get_index();
    const typename DF::IndexVecType &rhs_ts_vec = rhs.get_index();

    result_idx.reserve(std::min(lhs_ts_vec.size(), rhs_ts_vec.size()));
    std::set_intersection(// std::execution::parallel_policy,
                          lhs_ts_vec.begin(), lhs_ts_vec.end(),
                          rhs_ts_vec.begin(), rhs_ts_vec.end(),
                          std::back_inserter(result_idx));

    DF  result;

    result.load_index(std::move(result_idx));

    const typename DF::IndexVecType &new_idx = result.get_index();

    for (const auto &lhs_citer : lhs.column_list_)  {
        const auto  rhs_citer = rhs.column_tb_.find(lhs_citer.first.c_str());

        if (rhs_citer == rhs.column_tb_.end())  continue;

        typename DF::template operator_functor_
            <typename DF::IndexType, OPT, Ts ...>   functor (
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

template<typename DF, typename ... Ts>
inline DF df_plus (const DF &lhs, const DF &rhs)  {

    return (binary_operation<DF, std::plus, Ts ...>(lhs, rhs));
}

// ----------------------------------------------------------------------------

template<typename DF, typename ... Ts>
inline DF df_minus (const DF &lhs, const DF &rhs)  {

    return (binary_operation<DF, std::minus, Ts ...>(lhs, rhs));
}

// ----------------------------------------------------------------------------

template<typename DF, typename ... Ts>
inline DF df_multiplies (const DF &lhs, const DF &rhs)  {

    return (binary_operation<DF, std::multiplies, Ts ...>(lhs, rhs));
}

// ----------------------------------------------------------------------------

template<typename DF, typename ... Ts>
inline DF df_divides (const DF &lhs, const DF &rhs)  {

    return (binary_operation<DF, std::divides, Ts ...>(lhs, rhs));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
