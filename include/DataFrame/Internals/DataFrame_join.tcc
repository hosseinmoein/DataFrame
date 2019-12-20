// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>

#include <cstdio>
#include <tuple>

// ----------------------------------------------------------------------------

namespace hmdf
{
#if defined(WIN32) || defined (_WIN32)
#undef min
#undef max
#endif // defined(WIN32) || defined (_WIN32)

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
join_by_index (const RHS_T &rhs, join_policy mp) const  {

    static_assert(std::is_base_of<StdDataFrame<IndexType>, RHS_T>::value ||
                      std::is_base_of<DataFrameView<IndexType>, RHS_T>::value,
                  "The rhs argument to join_by_index() can only be "
                  "StdDataFrame<IndexType> or DataFrameView<IndexType>");

    switch(mp)  {
        case join_policy::inner_join:
            return (index_inner_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
        case join_policy::left_join:
            return (index_left_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
        case join_policy::right_join:
            return (index_right_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
        case join_policy::left_right_join:
        default:
            return (index_left_right_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename T, typename ... types>
StdDataFrame<unsigned int> DataFrame<I, H>::
join_by_column (const RHS_T &rhs, const char *name, join_policy mp) const  {

    static_assert(std::is_base_of<StdDataFrame<IndexType>, RHS_T>::value ||
                      std::is_base_of<DataFrameView<IndexType>, RHS_T>::value,
                  "The rhs argument to join_by_index() can only be "
                  "StdDataFrame<IndexType> or DataFrameView<IndexType>");

    const auto  lhs_iter = column_tb_.find (name);
    const auto  rhs_iter = rhs.column_tb_.find (name);

    if (lhs_iter == column_tb_.end() || rhs_iter == rhs.column_tb_.end()) {
        char buffer [512];

        sprintf (buffer, "DataFrame::join_by_column(): ERROR: "
                         "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    const DataVec   &lhs_hv = data_[lhs_iter->second];
    const DataVec   &rhs_hv = rhs.data_[rhs_iter->second];
    SpinGuard       guard(lock_);

    const std::vector<T>    &lhs_vec = lhs_hv.template get_vector<T>();
    const std::vector<T>    &rhs_vec = rhs_hv.template get_vector<T>();
    guard.release();

    const size_type lhs_vec_s = lhs_vec.size();
    const size_type rhs_vec_s = rhs_vec.size();

    std::vector<JoinSortingPair<T>> col_vec_lhs;
    std::vector<JoinSortingPair<T>> col_vec_rhs;

    col_vec_lhs.reserve(lhs_vec_s);
    for (size_type i = 0; i < lhs_vec_s; ++i)
        col_vec_lhs.push_back(std::make_pair(&(lhs_vec[i]), i));
    col_vec_rhs.reserve(rhs_vec_s);
    for (size_type i = 0; i < rhs_vec_s; ++i)
        col_vec_rhs.push_back(std::make_pair(&(rhs_vec[i]), i));

    auto    cf = [] (const JoinSortingPair<T> &l,
                     const JoinSortingPair<T> &r) -> bool  {
                     return (*(l.first) < *(r.first));
                 };

    std::sort(col_vec_lhs.begin(), col_vec_lhs.end(), cf);
    std::sort(col_vec_rhs.begin(), col_vec_rhs.end(), cf);

    switch(mp)  {
        case join_policy::inner_join:
            return (column_inner_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::left_join:
            return (column_left_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::right_join:
            return (column_right_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::left_right_join:
        default:
            return (column_left_right_join_
                        <decltype(*this), decltype(rhs), T, types ...>
                            (*this, rhs, name, col_vec_lhs, col_vec_rhs));
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_join_helper_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const IndexIdxVector &joined_index_idx)  {

    StdDataFrame<IndexType>    result;
    std::vector<IndexType>     result_index;

    // Load the index
    result_index.reserve(joined_index_idx.size());
    for (const auto &citer : joined_index_idx)  {
        const size_type left_i = std::get<0>(citer);

        result_index.push_back(
            left_i != std::numeric_limits<size_type>::max()
                ? lhs.indices_[left_i] : rhs.indices_[std::get<1>(citer)]);
    }
    result.load_index(std::move(result_index));

    // Load the common and lhs columns
    for (auto &iter : lhs.column_tb_)  {
        auto    rhs_citer = rhs.column_tb_.find(iter.first);

        // Common column between two frames
        if (rhs_citer != rhs.column_tb_.end())  {
            index_join_functor_common_<decltype(result), types ...> functor(
                iter.first.c_str(),
                rhs,
                joined_index_idx,
                result);

            lhs.data_[iter.second].change(functor);

        }
        else  {  // lhs only column
            // 0 = Left
            index_join_functor_oneside_<0,
                                        decltype(result),
                                        types ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            lhs.data_[iter.second].change(functor);
        }
    }

    // Load the rhs columns
    for (auto &iter : rhs.column_tb_)  {
        auto    lhs_citer = lhs.column_tb_.find(iter.first);

        if (lhs_citer == lhs.column_tb_.end())  {  // rhs only column
            // 1 = Right
            index_join_functor_oneside_<1,
                                        decltype(result),
                                        types ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            rhs.data_[iter.second].change(functor);
        }
    }

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... types>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_join_helper_(const LHS_T &lhs,
                    const RHS_T &rhs,
                    const char *col_name,
                    const IndexIdxVector &joined_index_idx)  {

    using left_idx_t = typename std::remove_reference<LHS_T>::type::IndexType;
    using right_idx_t = typename std::remove_reference<RHS_T>::type::IndexType;

    const size_type             jii_s = joined_index_idx.size();
    StdDataFrame<unsigned int>  result;

    // Load the new result index
    result.load_index(
        std::move(StdDataFrame<unsigned int>::gen_sequence_index(0, jii_s, 1)));

    // Load the lhs and rhs indices into two columns in the result
    // Also load the unified named column
    std::vector<left_idx_t>     lhs_index;
    std::vector<right_idx_t>    rhs_index;
    std::vector<T>              named_col_vec;
    const std::vector<T>        &lhs_named_col_vec =
        lhs.template get_column<T>(col_name);
    const std::vector<T>        &rhs_named_col_vec =
        rhs.template get_column<T>(col_name);

    lhs_index.reserve(jii_s);
    rhs_index.reserve(jii_s);
    named_col_vec.reserve(jii_s);
    for (const auto &citer : joined_index_idx)  {
        const size_type left_i = std::get<0>(citer);
        const size_type right_i = std::get<1>(citer);

        if (left_i != std::numeric_limits<size_type>::max())  {
            lhs_index.push_back(lhs.indices_[left_i]);
            named_col_vec.push_back(lhs_named_col_vec[left_i]);
        }
        else  {
            named_col_vec.push_back(rhs_named_col_vec[right_i]);
            lhs_index.push_back(DataFrame::_get_nan<left_idx_t>());
        }
        if (right_i != std::numeric_limits<size_type>::max())
            rhs_index.push_back(rhs.indices_[right_i]);
        else
            rhs_index.push_back(DataFrame::_get_nan<right_idx_t>());
    }

    char    buffer[64];

    ::sprintf(buffer, "lhs.%s", DF_INDEX_COL_NAME);
    result.load_column(buffer, std::move(lhs_index));
    ::sprintf(buffer, "rhs.%s", DF_INDEX_COL_NAME);
    result.load_column(buffer, std::move(rhs_index));
    result.load_column(col_name, std::move(named_col_vec));

    // Load the common and lhs columns
    for (auto &iter : lhs.column_tb_)  {
        auto    rhs_citer = rhs.column_tb_.find(iter.first);

        if (iter.first == col_name)  continue;

        // Common column between two frames
        if (rhs_citer != rhs.column_tb_.end())  {
            index_join_functor_common_<decltype(result), types ...> functor(
                iter.first.c_str(),
                rhs,
                joined_index_idx,
                result);

            lhs.data_[iter.second].change(functor);

        }
        else  {  // lhs only column
            // 0 = Left
            index_join_functor_oneside_<0,
                                        decltype(result),
                                        types ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            lhs.data_[iter.second].change(functor);
        }
    }

    // Load the rhs columns
    for (auto &iter : rhs.column_tb_)  {
        auto    lhs_citer = lhs.column_tb_.find(iter.first);

        if (iter.first == col_name)  continue;

        if (lhs_citer == lhs.column_tb_.end())  {  // rhs only column
            // 1 = Right
            index_join_functor_oneside_<1,
                                        decltype(result),
                                        types ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            rhs.data_[iter.second].change(functor);
        }
    }

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_inner_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

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

    return (index_join_helper_<LHS_T, RHS_T, types ...>
                (lhs, rhs, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... types>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_inner_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                   const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = col_vec_lhs.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = col_vec_rhs.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(std::min(lhs_end, rhs_end));
    while (lhs_current != lhs_end && rhs_current != rhs_end) {
        if (*(col_vec_lhs[lhs_current].first) <
                *(col_vec_rhs[rhs_current].first))
            lhs_current += 1;
        else  {
            if (*(col_vec_lhs[lhs_current].first) ==
                    *(col_vec_rhs[rhs_current].first))  {
                joined_index_idx.emplace_back(col_vec_lhs[lhs_current].second,
                                              col_vec_rhs[rhs_current].second);
                lhs_current += 1;
            }
            rhs_current += 1;
        }
    }

    return (column_join_helper_<LHS_T, RHS_T, T, types ...>
                (lhs, rhs, col_name, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_left_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(lhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (lhs_current >= lhs_end)  break;
        if (rhs_current >= rhs_end)  {
            joined_index_idx.emplace_back(
                lhs_current++,
                std::numeric_limits<size_type>::max());
            continue;
        }

        if (lhs.indices_[lhs_current] < rhs.indices_[rhs_current])
            joined_index_idx.emplace_back(
                lhs_current++,
                std::numeric_limits<size_type>::max());
        else  {
            if (lhs.indices_[lhs_current] == rhs.indices_[rhs_current])
                joined_index_idx.emplace_back(lhs_current++, rhs_current);
            rhs_current += 1;
        }
    }

    return (index_join_helper_<LHS_T, RHS_T, types ...>
                (lhs, rhs, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... types>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_left_join_(const LHS_T &lhs,
                  const RHS_T &rhs,
                  const char *col_name,
                  const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                  const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(lhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (lhs_current >= lhs_end)  break;
        if (rhs_current >= rhs_end)  {
            joined_index_idx.emplace_back(
                col_vec_lhs[lhs_current++].second,
                std::numeric_limits<size_type>::max());
            continue;
        }

        if (*(col_vec_lhs[lhs_current].first) <
                *(col_vec_rhs[rhs_current].first))
            joined_index_idx.emplace_back(
                col_vec_lhs[lhs_current++].second,
                std::numeric_limits<size_type>::max());
        else  {
            if (*(col_vec_lhs[lhs_current].first) ==
                    *(col_vec_rhs[rhs_current].first))
                joined_index_idx.emplace_back(col_vec_lhs[lhs_current++].second,
                                              col_vec_rhs[rhs_current].second);
            rhs_current += 1;
        }
    }

    return (column_join_helper_<LHS_T, RHS_T, T, types ...>
                (lhs, rhs, col_name, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_right_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(rhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (rhs_current >= rhs_end)  break;
        if (lhs_current >= lhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                rhs_current++);
            continue;
        }

        if (lhs.indices_[lhs_current] < rhs.indices_[rhs_current])
            lhs_current += 1;
        else  {
            if (lhs.indices_[lhs_current] == rhs.indices_[rhs_current])
                joined_index_idx.emplace_back(lhs_current++, rhs_current);
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    rhs_current);
            rhs_current += 1;
        }
    }

    return (index_join_helper_<LHS_T, RHS_T, types ...>
                (lhs, rhs, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... types>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_right_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                   const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(rhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (rhs_current >= rhs_end)  break;
        if (lhs_current >= lhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                col_vec_rhs[rhs_current].second);
            rhs_current += 1;
            continue;
        }

        if (*(col_vec_lhs[lhs_current].first) <
                *(col_vec_rhs[rhs_current].first))
            lhs_current += 1;
        else  {
            if (*(col_vec_lhs[lhs_current].first) ==
                    *(col_vec_rhs[rhs_current].first))  {
                joined_index_idx.emplace_back(
                    col_vec_lhs[lhs_current].second,
                    col_vec_rhs[rhs_current].second);
                lhs_current += 1;
            }
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    col_vec_rhs[rhs_current].second);
            rhs_current += 1;
        }
    }

    return (column_join_helper_<LHS_T, RHS_T, T, types ...>
                (lhs, rhs, col_name, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_left_right_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(std::max(lhs_end, rhs_end));
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (lhs_current >= lhs_end && rhs_current < rhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                rhs_current++);
            continue;
        }
        if (rhs_current >= rhs_end && lhs_current < lhs_end)  {
            joined_index_idx.emplace_back(
                lhs_current++,
                std::numeric_limits<size_type>::max());
            continue;
        }

        if (lhs.indices_[lhs_current] < rhs.indices_[rhs_current])  {
            joined_index_idx.emplace_back(
                lhs_current++,
                std::numeric_limits<size_type>::max());
        }
        else  {
            if (lhs.indices_[lhs_current] == rhs.indices_[rhs_current])
                joined_index_idx.emplace_back(lhs_current++, rhs_current);
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    rhs_current);
            rhs_current += 1;
        }
    }

    return (index_join_helper_<LHS_T, RHS_T, types ...>
                (lhs, rhs, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... types>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_left_right_join_(const LHS_T &lhs,
                        const RHS_T &rhs,
                        const char *col_name,
                        const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                        const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(std::max(lhs_end, rhs_end));
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (lhs_current >= lhs_end && rhs_current < rhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                col_vec_rhs[rhs_current].second);
            rhs_current += 1;
            continue;
        }
        if (rhs_current >= rhs_end && lhs_current < lhs_end)  {
            joined_index_idx.emplace_back(
                col_vec_lhs[lhs_current].second,
                std::numeric_limits<size_type>::max());
            lhs_current += 1;
            continue;
        }

        if (*(col_vec_lhs[lhs_current].first) <
                *(col_vec_rhs[rhs_current].first))  {
            joined_index_idx.emplace_back(
                col_vec_lhs[lhs_current].second,
                std::numeric_limits<size_type>::max());
            lhs_current += 1;
        }
        else  {
            if (*(col_vec_lhs[lhs_current].first) ==
                    *(col_vec_rhs[rhs_current].first))  {
                joined_index_idx.emplace_back(col_vec_lhs[lhs_current].second,
                                              col_vec_rhs[rhs_current].second);
                lhs_current += 1;
            }
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    col_vec_rhs[rhs_current].second);
            rhs_current += 1;
        }
    }

    return (column_join_helper_<LHS_T, RHS_T, T, types ...>
                (lhs, rhs, col_name, joined_index_idx));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
