// Hossein Moein
// September 25, 2017
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

#include <DataFrame/DataFrame.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
bool DataFrame<I, H>::is_equal (const DataFrame &rhs) const  {

    if (column_list_.size() != rhs.column_list_.size())
        return (false);
    if (indices_ != rhs.indices_)
        return (false);

    for (const auto &iter : column_list_)  {
        auto    rhs_citer = rhs.column_tb_.find(iter.first);

        if (rhs_citer == rhs.column_tb_.end())  return (false);

        equal_functor_<Ts ...>   functor (iter.first.c_str(), *this);
        const SpinGuard          guard(lock_);

        rhs.data_[rhs_citer->second].change(functor);
        if (! functor.result)
            return (false);
    }

    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrame<I, H> &DataFrame<I, H>::
modify_by_idx (DataFrame &rhs, sort_state already_sorted)  {

    if (already_sorted == sort_state::not_sorted)  {
        rhs.sort<IndexType, Ts ...>(DF_INDEX_COL_NAME, sort_spec::ascen);
        sort<IndexType, Ts ...>(DF_INDEX_COL_NAME, sort_spec::ascen);
    }

    const size_type lhs_s { indices_.size() };
    const size_type rhs_s { rhs.indices_.size() };

    for (size_type lhs_i = 0, rhs_i = 0;
         lhs_i < lhs_s && rhs_i < rhs_s; ++rhs_i)  {
        while (indices_[lhs_i] < rhs.indices_[rhs_i] && lhs_i < lhs_s)
            lhs_i += 1;

        if (indices_[lhs_i] == rhs.indices_[rhs_i])  {
            for (auto &iter : column_list_)  {
                mod_by_idx_functor_<Ts ...>  functor (iter.first.c_str(),
                                                      rhs,
                                                      lhs_i,
                                                      rhs_i);
                const SpinGuard              guard(lock_);

                data_[iter.second].change(functor);
            }

            lhs_i += 1;
        }
        else if (indices_[lhs_i] < rhs.indices_[rhs_i])  break;
    }

    return (*this);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:

