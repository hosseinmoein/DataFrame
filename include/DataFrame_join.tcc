// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DataFrame.h"
#include <tuple>

// ----------------------------------------------------------------------------

namespace hmdf
{

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename RHS_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
join_by_index (const RHS_T &rhs, join_policy mp) const  {

    static_assert(std::is_base_of<StdDataFrame<TS>, RHS_T>::value or
                      std::is_base_of<DataFrameView<TS>, RHS_T>::value,
                  "The rhs argument to join_by_index() can only be "
                  "StdDataFrame<TS> or DataFrameView<TS>");

    switch(mp)  {
        case join_policy::inner_join:
            return (index_inner_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case join_policy::left_join:
            return (index_left_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case join_policy::right_join:
            return (index_right_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case join_policy::left_right_join:
        default:
            return (index_left_right_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
    }
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename RHS_T, typename T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
join_by_column (const RHS_T &rhs,
                 const char *col_name,
                 join_policy mp) const {

    static_assert(std::is_base_of<StdDataFrame<TS>, RHS_T>::value or
                      std::is_base_of<DataFrameView<TS>, RHS_T>::value,
                  "The rhs argument to join_by_column() can only be "
                  "StdDataFrame<TS> or DataFrameView<TS>");

    switch(mp)  {
        case join_policy::inner_join:
            return (column_inner_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (col_name, *this, rhs));
            break;
        case join_policy::left_join:
            return (column_left_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (col_name, *this, rhs));
            break;
        case join_policy::right_join:
            return (column_right_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (col_name, *this, rhs));
            break;
        case join_policy::left_right_join:
        default:
            return (column_left_right_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (col_name, *this, rhs));
            break;
    }
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
index_inner_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    std::vector<std::tuple<size_type, size_type>>   joined_index_idx;

    joined_index_idx.reserve(std::min(lhs_end, rhs_end));
    while (lhs_current != lhs_end && rhs_current != rhs_end) {
        if (lhs.indices_[lhs_current] < rhs.indices_[rhs_current])
            lhs_current += 1;
        else  {
            if (lhs.indices_[lhs_current] == rhs.indices_[rhs_current])
                joined_index_idx.emplace_back(lhs_current++, rhs_current);
            rhs_current += 1;
        }
    }

    StdDataFrame<TS>    result;
    std::vector<TS>     result_index;

    // Load the index
    result_index.reserve(joined_index_idx.size());
    for (const auto &citer : joined_index_idx)
        result_index.push_back(lhs.indices_[std::get<0>(citer)]);
    result.load_index(std::move(result_index));

    // Load the common and lhs columns
    for (auto &iter : lhs.data_tb_)  {
        auto    rhs_citer = rhs.data_tb_.find(iter.first);

        // Common column between two frames
        if (rhs_citer != rhs.data_tb_.end())  {
            index_join_functor_common_<types ...> functor (iter.first.c_str(),
                                                           rhs,
                                                           joined_index_idx,
                                                           result);

            lhs.data_[iter.second].change(functor);

        }
        else  {  // lhs only column
            // 0 = Left
            index_join_functor_oneside_<0, types ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            lhs.data_[iter.second].change(functor);
        }
    }

    // Load the rhs columns
    for (auto &iter : rhs.data_tb_)  {
        auto    lhs_citer = lhs.data_tb_.find(iter.first);

        if (lhs_citer == lhs.data_tb_.end())  {  // rhs only column
            // 1 = Right
            index_join_functor_oneside_<1, types ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            rhs.data_[iter.second].change(functor);
        }
    }

    return(result);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
index_left_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    StdDataFrame<TS>    result;

    return(result);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
index_right_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    StdDataFrame<TS>    result;

    return(result);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
index_left_right_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    StdDataFrame<TS>    result;

    return(result);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename LHS_T, typename RHS_T, typename COL_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
column_inner_join_(const char *col_name, const LHS_T &lhs, const RHS_T &rhs)  {

    StdDataFrame<TS>    result;

    return(result);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename LHS_T, typename RHS_T, typename COL_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
column_left_join_(const char *col_name, const LHS_T &lhs, const RHS_T &rhs)  {

    StdDataFrame<TS>    result;

    return(result);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename LHS_T, typename RHS_T, typename COL_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
column_right_join_(const char *col_name, const LHS_T &lhs, const RHS_T &rhs)  {

    StdDataFrame<TS>    result;

    return(result);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename LHS_T, typename RHS_T, typename COL_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
column_left_right_join_(const char *col_name,
                        const LHS_T &lhs,
                        const RHS_T &rhs)  {

    StdDataFrame<TS>    result;

    return(result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
