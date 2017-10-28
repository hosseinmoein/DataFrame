// Hossein Moein
// September 25, 2017
// To the extent possible under law, the author(s) have dedicated all
// copyright and related and neighboring rights to this software to
// the public domain worldwide. This software is distributed without
// any warranty.

#include <DataFrame.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... types>
bool DataFrame<TS, DS>::is_equal (const DataFrame &rhs) const  {

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

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... types>
DataFrame<TS, DS> &DataFrame<TS, DS>::
modify_by_idx (DataFrame &rhs, bool already_sorted)  {

    if (! already_sorted)  {
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
