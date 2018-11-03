// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DataFrame.h"

// ----------------------------------------------------------------------------

namespace hmdf
{

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename RHS_T, typename ... types>
StdDataFrame<TS> DataFrame<TS, HETERO>::
merge_by_index (const RHS_T &rhs, merge_policy mp) const  {

    static_assert(std::is_base_of<StdDataFrame<TS>, RHS_T>::value or
                      std::is_base_of<DataFrameView<TS>, RHS_T>::value,
                  "The rhs argument to merge_by_index() can only be "
                  "StdDataFrame<TS> or DataFrameView<TS>");

    switch(mp)  {
        case merge_policy::inner_join:
            return (index_inner_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case merge_policy::left_join:
            return (index_left_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case merge_policy::right_join:
            return (index_right_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case merge_policy::left_right_join:
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
merge_by_column (const RHS_T &rhs,
                 const char *col_name,
                 merge_policy mp) const {

    static_assert(std::is_base_of<StdDataFrame<TS>, RHS_T>::value or
                      std::is_base_of<DataFrameView<TS>, RHS_T>::value,
                  "The rhs argument to merge_by_column() can only be "
                  "StdDataFrame<TS> or DataFrameView<TS>");

    switch(mp)  {
        case merge_policy::inner_join:
            return (column_inner_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (col_name, *this, rhs));
            break;
        case merge_policy::left_join:
            return (column_left_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (col_name, *this, rhs));
            break;
        case merge_policy::right_join:
            return (column_right_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (col_name, *this, rhs));
            break;
        case merge_policy::left_right_join:
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

    StdDataFrame<TS>    result;

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
