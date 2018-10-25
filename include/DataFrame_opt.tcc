// Hossein Moein
// September 25, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DataFrame.h"

// ----------------------------------------------------------------------------

namespace hmdf
{

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ... types>
bool DataFrame<TS, HETERO>::is_equal (const DataFrame &rhs) const  {

    if (data_tb_.size() != rhs.data_tb_.size())
        return (false);
    if (timestamps_ != rhs.timestamps_)
        return (false);

    for (const auto &iter : data_tb_)  {
        equal_functor_<types ...>   functor (iter.first.c_str(), *this);

        rhs.data_[iter.second].change(functor);
        if (! functor.result)
            return (false);
    }

    return (true);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ... types>
DataFrame<TS, HETERO> &DataFrame<TS, HETERO>::
modify_by_idx (DataFrame &rhs, sort_state already_sorted)  {

    if (already_sorted == sort_state::not_sorted)  {
        rhs.sort<TimeStamp, types ...>();
        sort<TimeStamp, types ...>();
    }

    const size_type lhs_s { timestamps_.size() };
    const size_type rhs_s { rhs.timestamps_.size() };
    size_type       lhs_i { 0 };

    for (size_type rhs_i = 0; rhs_i < rhs_s; ++rhs_i)  {
        if (lhs_i >= lhs_s)
            break;
        while (timestamps_[lhs_i] < rhs.timestamps_[rhs_i] && lhs_i < lhs_s)
            lhs_i += 1;

        if (timestamps_[lhs_i] == rhs.timestamps_[rhs_i])  {
            for (auto &iter : data_tb_)  {
                mod_by_idx_functor_<types ...>  functor (iter.first.c_str(),
                                                         rhs,
                                                         lhs_i,
                                                         rhs_i);

                data_[iter.second].change(functor);
            }

            lhs_i += 1;
        }
        else if (timestamps_[lhs_i] < rhs.timestamps_[rhs_i])  break;
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
