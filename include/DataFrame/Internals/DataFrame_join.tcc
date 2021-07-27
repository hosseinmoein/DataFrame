// Hossein Moein
// September 12, 2017
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

#include <cstdio>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
StdDataFrame<I> DataFrame<I, H>::
join_by_index (const RHS_T &rhs, join_policy mp) const  {

    static_assert(std::is_base_of<StdDataFrame<I>, RHS_T>::value ||
                      std::is_base_of<DataFrameView<I>, RHS_T>::value ||
                      std::is_base_of<DataFramePtrView<I>, RHS_T>::value,
                  "The rhs argument to join_by_index() can only be "
                  "StdDataFrame<IndexType> or DataFrame[Ptr]View<IndexType>");

    const auto                              &lhs_idx = get_index();
    const auto                              &rhs_idx = rhs.get_index();
    const size_type                         lhs_idx_s = lhs_idx.size();
    const size_type                         rhs_idx_s = rhs_idx.size();
    std::vector<JoinSortingPair<IndexType>> idx_vec_lhs;
    std::vector<JoinSortingPair<IndexType>> idx_vec_rhs;

    idx_vec_lhs.reserve(lhs_idx_s);
    for (size_type i = 0; i < lhs_idx_s; ++i)
        idx_vec_lhs.push_back(std::make_pair(&(lhs_idx[i]), i));
    idx_vec_rhs.reserve(rhs_idx_s);
    for (size_type i = 0; i < rhs_idx_s; ++i)
        idx_vec_rhs.push_back(std::make_pair(&(rhs_idx[i]), i));

    auto    cf = [] (const JoinSortingPair<IndexType> &l,
                     const JoinSortingPair<IndexType> &r) -> bool  {
                     return (*(l.first) < *(r.first));
                 };

    std::sort(idx_vec_lhs.begin(), idx_vec_lhs.end(), cf);
    std::sort(idx_vec_rhs.begin(), idx_vec_rhs.end(), cf);

    switch(mp)  {
        case join_policy::inner_join:
            return (index_inner_join_
                        <decltype(*this), RHS_T, Ts ...>
                    (*this, rhs, idx_vec_lhs, idx_vec_rhs));
        case join_policy::left_join:
            return (index_left_join_
                        <decltype(*this), RHS_T, Ts ...>
                    (*this, rhs, idx_vec_lhs, idx_vec_rhs));
        case join_policy::right_join:
            return (index_right_join_
                        <decltype(*this), RHS_T, Ts ...>
                    (*this, rhs, idx_vec_lhs, idx_vec_rhs));
        case join_policy::left_right_join:
        default:
            return (index_left_right_join_
                        <decltype(*this), RHS_T, Ts ...>
                    (*this, rhs, idx_vec_lhs, idx_vec_rhs));
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename T, typename ... Ts>
StdDataFrame<unsigned int> DataFrame<I, H>::
join_by_column (const RHS_T &rhs, const char *name, join_policy mp) const  {

    static_assert(std::is_base_of<StdDataFrame<I>, RHS_T>::value ||
                      std::is_base_of<DataFrameView<I>, RHS_T>::value ||
                      std::is_base_of<DataFramePtrView<I>, RHS_T>::value,
                  "The rhs argument to join_by_column() can only be "
                  "StdDataFrame<IndexType> or DataFrame[Ptr]View<IndexType>");

    const auto      &lhs_vec = get_column<T>(name);
    const auto      &rhs_vec = rhs.template get_column<T>(name);
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
                        <decltype(*this), RHS_T, T, Ts ...>
                            (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::left_join:
            return (column_left_join_
                        <decltype(*this), RHS_T, T, Ts ...>
                            (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::right_join:
            return (column_right_join_
                        <decltype(*this), RHS_T, T, Ts ...>
                            (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::left_right_join:
        default:
            return (column_left_right_join_
                        <decltype(*this), RHS_T, T, Ts ...>
                            (*this, rhs, name, col_vec_lhs, col_vec_rhs));
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename IDX_T, typename ... Ts>
void DataFrame<I, H>::
join_helper_common_(const LHS_T &lhs,
                    const RHS_T &rhs,
                    const IndexIdxVector &joined_index_idx,
                    StdDataFrame<IDX_T> &result,
                    const char *skip_col_name)  {

    // Load the common and lhs columns
    for (auto &iter : lhs.column_list_)  {
        auto    rhs_citer = rhs.column_tb_.find(iter.first);

        if (skip_col_name && iter.first == skip_col_name)  continue;

        // Common column between two frames
        if (rhs_citer != rhs.column_tb_.end())  {
            index_join_functor_common_<decltype(result), Ts ...> functor(
                iter.first.c_str(),
                rhs,
                joined_index_idx,
                result);

            lhs.data_[iter.second].change(functor);
        }
        else  {  // lhs only column
            // 0 = Left
            index_join_functor_oneside_<0, decltype(result), Ts ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            lhs.data_[iter.second].change(functor);
        }
    }

    // Load the rhs columns
    for (auto &iter : rhs.column_list_)  {
        auto    lhs_citer = lhs.column_tb_.find(iter.first);

        if (skip_col_name && iter.first == skip_col_name)  continue;

        if (lhs_citer == lhs.column_tb_.end())  {  // rhs only column
            // 1 = Right
            index_join_functor_oneside_<1, decltype(result), Ts ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            rhs.data_[iter.second].change(functor);
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
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

    join_helper_common_<LHS_T, RHS_T, IndexType, Ts ...>
        (lhs, rhs, joined_index_idx, result);
    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
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
        StdDataFrame<unsigned int>::gen_sequence_index(
               0, static_cast<unsigned int>(jii_s), 1));

    // Load the lhs and rhs indices into two columns in the result
    // Also load the unified named column
    std::vector<left_idx_t>     lhs_index;
    std::vector<right_idx_t>    rhs_index;
    std::vector<T>              named_col_vec;
    const ColumnVecType<T>      &lhs_named_col_vec =
        lhs.template get_column<T>(col_name);
    const ColumnVecType<T>      &rhs_named_col_vec =
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
            lhs_index.push_back(get_nan<left_idx_t>());
        }
        if (right_i != std::numeric_limits<size_type>::max())
            rhs_index.push_back(rhs.indices_[right_i]);
        else
            rhs_index.push_back(get_nan<right_idx_t>());
    }

    char    buffer[64];

    ::sprintf(buffer, "lhs.%s", DF_INDEX_COL_NAME);
    result.load_column(buffer, std::move(lhs_index));
    ::sprintf(buffer, "rhs.%s", DF_INDEX_COL_NAME);
    result.load_column(buffer, std::move(rhs_index));
    result.load_column(col_name, std::move(named_col_vec));

    join_helper_common_<LHS_T, RHS_T, unsigned int, Ts ...>
        (lhs, rhs, joined_index_idx, result, col_name);
    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::IndexIdxVector
DataFrame<I, H>::get_inner_index_idx_vector_(
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
                    *(col_vec_rhs[rhs_current].first))
                joined_index_idx.emplace_back(
                    col_vec_lhs[lhs_current++].second,
                    col_vec_rhs[rhs_current].second);
            rhs_current += 1;
        }
    }
    return (joined_index_idx);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
StdDataFrame<I> DataFrame<I, H>::
index_inner_join_(const LHS_T &lhs, const RHS_T &rhs,
                  const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
                  const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs) {

    return (index_join_helper_<LHS_T, RHS_T, Ts ...>
                (lhs, rhs,
                 get_inner_index_idx_vector_(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_inner_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                   const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    return (column_join_helper_<LHS_T, RHS_T, T, Ts ...>
                (lhs, rhs, col_name,
                 get_inner_index_idx_vector_(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::IndexIdxVector
DataFrame<I, H>::get_left_index_idx_vector_(
    const std::vector<JoinSortingPair<T>> &col_vec_lhs,
    const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = col_vec_lhs.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = col_vec_rhs.size();

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
    return (joined_index_idx);
}
// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
StdDataFrame<I> DataFrame<I, H>::
index_left_join_(const LHS_T &lhs, const RHS_T &rhs,
                 const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
                 const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs) {

    return (index_join_helper_<LHS_T, RHS_T, Ts ...>
                (lhs, rhs,
                 get_left_index_idx_vector_(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_left_join_(const LHS_T &lhs,
                  const RHS_T &rhs,
                  const char *col_name,
                  const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                  const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    return (column_join_helper_<LHS_T, RHS_T, T, Ts ...>
                (lhs, rhs, col_name,
                 get_left_index_idx_vector_(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::IndexIdxVector
DataFrame<I, H>::get_right_index_idx_vector_(
    const std::vector<JoinSortingPair<T>> &col_vec_lhs,
    const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = col_vec_lhs.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = col_vec_rhs.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(rhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (rhs_current >= rhs_end)  break;
        if (lhs_current >= lhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                col_vec_rhs[rhs_current++].second);
            continue;
        }

        if (*(col_vec_lhs[lhs_current].first) <
                *(col_vec_rhs[rhs_current].first))
            lhs_current += 1;
        else  {
            if (*(col_vec_lhs[lhs_current].first) ==
                    *(col_vec_rhs[rhs_current].first))
                joined_index_idx.emplace_back(
                    col_vec_lhs[lhs_current++].second,
                    col_vec_rhs[rhs_current].second);
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    col_vec_rhs[rhs_current].second);
            rhs_current += 1;
        }
    }
    return (joined_index_idx);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
StdDataFrame<I> DataFrame<I, H>::
index_right_join_(const LHS_T &lhs, const RHS_T &rhs,
                  const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
                  const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs) {

    return (index_join_helper_<LHS_T, RHS_T, Ts ...>
                (lhs, rhs,
                 get_right_index_idx_vector_(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_right_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                   const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    return (column_join_helper_<LHS_T, RHS_T, T, Ts ...>
                (lhs, rhs, col_name,
                 get_right_index_idx_vector_(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::IndexIdxVector
DataFrame<I, H>::get_left_right_index_idx_vector_(
    const std::vector<JoinSortingPair<T>> &col_vec_lhs,
    const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = col_vec_lhs.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = col_vec_rhs.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(std::max(lhs_end, rhs_end));
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (lhs_current >= lhs_end && rhs_current < rhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                col_vec_rhs[rhs_current++].second);
            continue;
        }
        if (rhs_current >= rhs_end && lhs_current < lhs_end)  {
            joined_index_idx.emplace_back(
                col_vec_lhs[lhs_current++].second,
                std::numeric_limits<size_type>::max());
            continue;
        }

        if (*(col_vec_lhs[lhs_current].first) <
                *(col_vec_rhs[rhs_current].first))  {
            joined_index_idx.emplace_back(
                col_vec_lhs[lhs_current++].second,
                std::numeric_limits<size_type>::max());
        }
        else  {
            if (*(col_vec_lhs[lhs_current].first) ==
                    *(col_vec_rhs[rhs_current].first))
                joined_index_idx.emplace_back(col_vec_lhs[lhs_current++].second,
                                              col_vec_rhs[rhs_current].second);
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    col_vec_rhs[rhs_current].second);
            rhs_current += 1;
        }
    }
    return (joined_index_idx);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
StdDataFrame<I> DataFrame<I, H>::
index_left_right_join_(
    const LHS_T &lhs,
    const RHS_T &rhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs) {

    return (index_join_helper_<LHS_T, RHS_T, Ts ...>
                (lhs, rhs,
                 get_left_right_index_idx_vector_(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
StdDataFrame<unsigned int> DataFrame<I, H>::
column_left_right_join_(const LHS_T &lhs,
                        const RHS_T &rhs,
                        const char *col_name,
                        const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                        const std::vector<JoinSortingPair<T>> &col_vec_rhs)  {

    return (column_join_helper_<LHS_T, RHS_T, T, Ts ...>
                (lhs, rhs, col_name,
                 get_left_right_index_idx_vector_(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
void DataFrame<I, H>::
concat_helper_(LHS_T &lhs, const RHS_T &rhs, bool add_new_columns)  {

    const size_type orig_index_s = lhs.get_index().size();

    lhs.get_index().insert(lhs.get_index().end(),
                           rhs.get_index().begin(), rhs.get_index().end());

    // Load common columns
    for (auto &lhs_iter : lhs.column_list_)  {
        auto    rhs_citer = rhs.column_tb_.find(lhs_iter.first);

        if (rhs_citer != rhs.column_tb_.end())  {
            concat_functor_<LHS_T, Ts ...>  functor(lhs_iter.first.c_str(),
                                                    lhs,
                                                    false,
                                                    orig_index_s);

            rhs.data_[rhs_citer->second].change(functor);
        }
    }

    // Load columns from rhs that do not exist in lhs
    if (add_new_columns)  {
        for (auto &rhs_citer : rhs.column_list_)  {
            auto    lhs_iter = lhs.column_tb_.find(rhs_citer.first);

            if (lhs_iter == lhs.column_tb_.end())  {
                concat_functor_<LHS_T, Ts ...>  functor(rhs_citer.first.c_str(),
                                                        lhs,
                                                        true,
                                                        orig_index_s);

                rhs.data_[rhs_citer.second].change(functor);
            }
        }
    }
}


// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
void
DataFrame<I, H>::self_concat(const RHS_T &rhs, bool add_new_columns)  {

    static_assert((std::is_base_of<StdDataFrame<I>, RHS_T>::value ||
                   std::is_base_of<DataFrameView<I>, RHS_T>::value ||
                   std::is_base_of<DataFramePtrView<I>, RHS_T>::value) &&
                  ! std::is_base_of<StdDataFrame<I>, decltype(*this)>::value,
                  "The rhs argument to self_concat() can only be "
                  "StdDataFrame<IndexType> or DataFrame[Ptr]View<IndexType>. "
                  "Self must be StdDataFrame<IndexType>");

    concat_helper_<decltype(*this), RHS_T, Ts ...>(*this, rhs, add_new_columns);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
StdDataFrame<I>
DataFrame<I, H>::concat(const RHS_T &rhs, concat_policy cp) const  {

    static_assert((std::is_base_of<StdDataFrame<I>, RHS_T>::value ||
                   std::is_base_of<DataFrameView<I>, RHS_T>::value ||
                   std::is_base_of<DataFramePtrView<I>, RHS_T>::value) &&
                  ! std::is_base_of<StdDataFrame<I>, decltype(*this)>::value,
                  "The rhs argument to concat() can only be "
                  "StdDataFrame<IndexType> or DataFrame[Ptr]View<IndexType>. "
                  "Self must be StdDataFrame<IndexType>");

    StdDataFrame<I> result;

    if (cp == concat_policy::all_columns ||
        cp == concat_policy::lhs_and_common_columns)  {
        result = *this;
        concat_helper_<decltype(result), RHS_T, Ts ...>(
            result, rhs, cp == concat_policy::all_columns);
    }
    else if (cp == concat_policy::common_columns)  {
        result.load_index(this->get_index().begin(), this->get_index().end());
        for (auto &lhs_citer : column_list_)  {
            auto    rhs_citer = rhs.column_tb_.find(lhs_citer.first);

            if (rhs_citer != rhs.column_tb_.end())  {
                load_all_functor_<Ts ...>   functor(lhs_citer.first.c_str(),
                                                    result);

                data_[lhs_citer.second].change(functor);
            }
        }
        concat_helper_<decltype(result), RHS_T, Ts ...>(result, rhs, false);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
DataFramePtrView<I>
DataFrame<I, H>::concat_view(const RHS_T &rhs, concat_policy cp) const  {

    static_assert(! std::is_base_of<StdDataFrame<I>, RHS_T>::value ||
                  ! std::is_base_of<StdDataFrame<I>, decltype(*this)>::value,
                  "Currently, arguments to concat_view() can only be "
                  "StdDataFrame<IndexType>.");

    DataFramePtrView<I> result;

    using idxvec_t = typename DataFramePtrView<I>::IndexVecType;

    const size_type idx_s = get_index().size();
    const size_type rhs_idx_s = rhs.get_index().size();
    idxvec_t        result_idx;

    result_idx.reserve(idx_s + rhs_idx_s);
    for (size_type i = 0; i < idx_s; ++i)
        result_idx.push_back(const_cast<IndexType *>(&(get_index()[i])));
    for (size_type i = 0; i < rhs_idx_s; ++i)
        result_idx.push_back(const_cast<IndexType *>(&(rhs.get_index()[i])));
    result.indices_ = std::move(result_idx);

    if (cp == concat_policy::all_columns)  {
        for (auto &lhs_citer : column_list_)  {
            concat_load_view_functor_<DataFramePtrView<I>, Ts ...> functor(
                lhs_citer.first.c_str(), result);

            data_[lhs_citer.second].change(functor);
        }
        for (auto &rhs_citer : rhs.column_list_)  {
            concat_load_view_functor_<DataFramePtrView<I>, Ts ...> functor(
                rhs_citer.first.c_str(), result);

            rhs.data_[rhs_citer.second].change(functor);
        }
    }
    else if (cp == concat_policy::lhs_and_common_columns)  {
        for (auto &lhs_citer : column_list_)  {
            concat_load_view_functor_<DataFramePtrView<I>, Ts ...> functor(
                lhs_citer.first.c_str(), result);

            data_[lhs_citer.second].change(functor);

            auto    rhs_citer = rhs.column_tb_.find(lhs_citer.first);

            if (rhs_citer != rhs.column_tb_.end())
                rhs.data_[rhs_citer->second].change(functor);
        }
    }
    else if (cp == concat_policy::common_columns)  {
        for (auto &lhs_citer : column_list_)  {
            concat_load_view_functor_<DataFramePtrView<I>, Ts ...> functor(
                lhs_citer.first.c_str(), result);
            auto                                                   rhs_citer =
                rhs.column_tb_.find(lhs_citer.first);

            if (rhs_citer != rhs.column_tb_.end())  {
                data_[lhs_citer.second].change(functor);
                rhs.data_[rhs_citer->second].change(functor);
            }
        }

    }

    return (result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
