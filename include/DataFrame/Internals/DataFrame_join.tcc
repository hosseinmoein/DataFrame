// Hossein Moein
// September 12, 2017
/*
Copyright (c) 2019-2026, Hossein Moein
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

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
join_by_index (const RHS_T &rhs, join_policy mp) const  {

    static_assert(comparable<I>, "Index type must have comparison operators");

    using pair_vec_t = StlVecType<JoinSortingPair<IndexType>>;
    using pair_vec_iter =
        typename StlVecType<JoinSortingPair<IndexType>>::iterator;

    const auto      &lhs_idx = get_index();
    const auto      &rhs_idx = rhs.get_index();
    const size_type lhs_idx_s = lhs_idx.size();
    const size_type rhs_idx_s = rhs_idx.size();
    pair_vec_t      idx_vec_lhs;
    pair_vec_t      idx_vec_rhs;

    idx_vec_lhs.reserve(lhs_idx_s);
    for (size_type i = 0; i < lhs_idx_s; ++i) [[likely]]
        idx_vec_lhs.push_back(std::make_pair(&(lhs_idx[i]), i));
    idx_vec_rhs.reserve(rhs_idx_s);
    for (size_type i = 0; i < rhs_idx_s; ++i) [[likely]]
        idx_vec_rhs.push_back(std::make_pair(&(rhs_idx[i]), i));

    auto        cf = [] (const JoinSortingPair<IndexType> &l,
                         const JoinSortingPair<IndexType> &r) -> bool  {
                         return (*(l.first) < *(r.first));
                     };
    const auto  thread_level =
        (lhs_idx_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (thread_level > 3)  {
        std::future<void>   futures[2];

        futures[0] = thr_pool_.dispatch(
            false,
            &ThreadPool::parallel_sort<pair_vec_iter, decltype(cf)>,
                &thr_pool_,
                idx_vec_lhs.begin(),
                idx_vec_lhs.end(),
                std::move(cf));
        futures[1] = thr_pool_.dispatch(
            false,
            &ThreadPool::parallel_sort<pair_vec_iter, decltype(cf)>,
                &thr_pool_,
                idx_vec_rhs.begin(),
                idx_vec_rhs.end(),
                std::move(cf));
        futures[0].get();
        futures[1].get();
    }
    else  {
        std::ranges::sort(idx_vec_lhs, cf);
        std::ranges::sort(idx_vec_rhs, cf);
    }

    switch(mp)  {
        case join_policy::inner_join:
            return (index_inner_join_<DataFrame, RHS_T, Ts ...>
                        (*this, rhs, idx_vec_lhs, idx_vec_rhs));
        case join_policy::left_join:
            return (index_left_join_<DataFrame, RHS_T, Ts ...>
                        (*this, rhs, idx_vec_lhs, idx_vec_rhs));
        case join_policy::right_join:
            return (index_right_join_<DataFrame, RHS_T, Ts ...>
                        (*this, rhs, idx_vec_lhs, idx_vec_rhs));
        case join_policy::left_right_join:
        default:
            return (index_left_right_join_<DataFrame, RHS_T, Ts ...>
                        (*this, rhs, idx_vec_lhs, idx_vec_rhs));
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, comparable T, typename ... Ts>
DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::
join_by_column (const RHS_T &rhs, const char *name, join_policy mp) const  {

    using pair_vec_t = StlVecType<JoinSortingPair<T>>;
    using pair_vec_iter = typename StlVecType<JoinSortingPair<T>>::iterator;

    const auto      &lhs_vec = get_column<T>(name);
    const auto      &rhs_vec = rhs.template get_column<T>(name);
    const size_type lhs_vec_s = lhs_vec.size();
    const size_type rhs_vec_s = rhs_vec.size();
    pair_vec_t      col_vec_lhs;
    pair_vec_t      col_vec_rhs;

    col_vec_lhs.reserve(lhs_vec_s);
    for (size_type i = 0; i < lhs_vec_s; ++i) [[likely]]
        col_vec_lhs.push_back(std::make_pair(&(lhs_vec[i]), i));
    col_vec_rhs.reserve(rhs_vec_s);
    for (size_type i = 0; i < rhs_vec_s; ++i) [[likely]]
        col_vec_rhs.push_back(std::make_pair(&(rhs_vec[i]), i));

    auto        cf = [] (const JoinSortingPair<T> &l,
                         const JoinSortingPair<T> &r) -> bool  {
                         return (*(l.first) < *(r.first));
                     };
    const auto  thread_level =
        (lhs_vec_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (thread_level > 3)  {
        std::future<void>   futures[2];

        futures[0] = thr_pool_.dispatch(
            false,
            &ThreadPool::parallel_sort<pair_vec_iter, decltype(cf)>,
                &thr_pool_,
                col_vec_lhs.begin(),
                col_vec_lhs.end(),
                std::move(cf));
        futures[1] = thr_pool_.dispatch(
            false,
            &ThreadPool::parallel_sort<pair_vec_iter, decltype(cf)>,
                &thr_pool_,
                col_vec_rhs.begin(),
                col_vec_rhs.end(),
                std::move(cf));
        futures[0].get();
        futures[1].get();
    }
    else  {
        std::ranges::sort(col_vec_lhs, cf);
        std::ranges::sort(col_vec_rhs, cf);
    }

    switch(mp)  {
        case join_policy::inner_join:
            return (column_inner_join_<DataFrame, RHS_T, T, Ts ...>
                        (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::left_join:
            return (column_left_join_<DataFrame, RHS_T, T, Ts ...>
                        (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::right_join:
            return (column_right_join_<DataFrame, RHS_T, T, Ts ...>
                        (*this, rhs, name, col_vec_lhs, col_vec_rhs));
        case join_policy::left_right_join:
        default:
            return (column_left_right_join_<DataFrame, RHS_T, T, Ts ...>
                        (*this, rhs, name, col_vec_lhs, col_vec_rhs));
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
index_join_helper_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const IndexIdxVector &joined_index_idx)  {

    using result_t = DataFrame<IndexType, HeteroVector<align_value>>;

    result_t                result;
    const size_type         len = joined_index_idx.size();
    StlVecType<IndexType>   result_index(len);
    auto                    lbd =
        [&joined_index_idx = std::as_const(joined_index_idx),
         &result_index,
         &lhs = std::as_const(lhs),
         &rhs = std::as_const(rhs)]
        (const auto begin, const auto end) -> void  {
            for (size_type i = begin; i < end; ++i) [[likely]]  {
                const size_type left_i = std::get<0>(joined_index_idx[i]);

                result_index[i] =
                    left_i != std::numeric_limits<size_type>::max()
                        ? lhs.indices_[left_i]
                        : rhs.indices_[std::get<1>(joined_index_idx[i])];
            }
        };

    // Load the index
    if (len >= ThreadPool::MUL_THR_THHOLD && get_thread_level() > 2)  {
        auto    futures =
            thr_pool_.parallel_loop(size_type(0), len, std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else
        lbd(0, len);
    result.load_index(std::move(result_index));

    join_helper_common_<LHS_T, RHS_T, IndexType, Ts ...>
        (lhs, rhs, joined_index_idx, result);
    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::
column_join_helper_(const LHS_T &lhs,
                    const RHS_T &rhs,
                    const char *col_name,
                    const IndexIdxVector &joined_index_idx)  {

    using left_idx_t = typename std::remove_reference<LHS_T>::type::IndexType;
    using right_idx_t = typename std::remove_reference<RHS_T>::type::IndexType;
    using result_t = DataFrame<unsigned long, HeteroVector<align_value>>;

    const size_type len = joined_index_idx.size();
    result_t        result;

    // Load the new result index
    result.load_index(
        result_t::gen_sequence_index(0, static_cast<unsigned long>(len), 1));

    // Load the lhs and rhs indices into two columns in the result
    // Also load the unified named column
    StlVecType<left_idx_t>  lhs_index(len);
    StlVecType<right_idx_t> rhs_index(len);
    StlVecType<T>           named_col_vec(len);
    const auto              &lhs_named_col_vec =
        lhs.template get_column<T>(col_name);
    const auto              &rhs_named_col_vec =
        rhs.template get_column<T>(col_name);
    auto                    lbd =
        [&joined_index_idx = std::as_const(joined_index_idx),
         &lhs_index, &rhs_index, &named_col_vec,
         &lhs = std::as_const(lhs),
         &rhs = std::as_const(rhs),
         &lhs_named_col_vec = std::as_const(lhs_named_col_vec),
         &rhs_named_col_vec = std::as_const(rhs_named_col_vec)]
        (const auto begin, const auto end) -> void  {
            for (size_type i = begin; i < end; ++i) [[likely]]  {
                const size_type left_i = std::get<0>(joined_index_idx[i]);
                const size_type right_i = std::get<1>(joined_index_idx[i]);

                if (left_i != std::numeric_limits<size_type>::max())
                [[likely]]  {
                    lhs_index[i] = lhs.indices_[left_i];
                    named_col_vec[i] = lhs_named_col_vec[left_i];
                }
                else  {
                    named_col_vec[i] = rhs_named_col_vec[right_i];
                    lhs_index[i] = get_nan<left_idx_t>();
                }
                if (right_i != std::numeric_limits<size_type>::max())
                [[likely]]
                    rhs_index[i] = rhs.indices_[right_i];
                else
                    rhs_index[i] = get_nan<right_idx_t>();
            }
        };

    if (len >= ThreadPool::MUL_THR_THHOLD && get_thread_level() > 2)  {
        auto    futures =
            thr_pool_.parallel_loop(size_type(0), len, std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else
        lbd(0, len);

    {
        char            buffer[64];
        const SpinGuard guard(lock_);

        ::snprintf(buffer, sizeof(buffer) - 1, "lhs.%s", DF_INDEX_COL_NAME);
        result.template load_column<left_idx_t>(buffer,
                                                std::move(lhs_index),
                                                nan_policy::pad_with_nans,
                                                false);
        ::snprintf(buffer, sizeof(buffer) - 1, "rhs.%s", DF_INDEX_COL_NAME);
        result.template load_column<right_idx_t>(buffer,
                                                 std::move(rhs_index),
                                                 nan_policy::pad_with_nans,
                                                 false);
        result.template load_column<T>(col_name,
                                       std::move(named_col_vec),
                                       nan_policy::pad_with_nans,
                                       false);
    }

    join_helper_common_<LHS_T, RHS_T, unsigned long, Ts ...>
        (lhs, rhs, joined_index_idx, result, col_name);
    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::IndexIdxVector
DataFrame<I, H>::get_inner_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = col_vec_lhs.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = col_vec_rhs.size();
    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(std::min(lhs_end, rhs_end));
    while (lhs_current != lhs_end && rhs_current != rhs_end) [[likely]] {
        if (*(col_vec_lhs[lhs_current].first) <
            *(col_vec_rhs[rhs_current].first))  {
            lhs_current += 1;
        }
        else  {
            if (*(col_vec_lhs[lhs_current].first) ==
                *(col_vec_rhs[rhs_current].first))  {
                const auto  &prev_value = *(col_vec_lhs[lhs_current].first);

                while (rhs_current < rhs_end &&
                       *(col_vec_lhs[lhs_current].first) ==
                       *(col_vec_rhs[rhs_current].first))  {
                    joined_index_idx.emplace_back(
                        col_vec_lhs[lhs_current].second,
                        col_vec_rhs[rhs_current++].second);
                }
                lhs_current += 1;
                while (lhs_current < lhs_end &&
                       *(col_vec_lhs[lhs_current].first) == prev_value)  {
                    joined_index_idx.emplace_back(
                        col_vec_lhs[lhs_current++].second,
                        col_vec_rhs[rhs_current - 1].second);
                }
            }
            else  rhs_current += 1;
        }
    }
    return (joined_index_idx);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
index_inner_join_(const LHS_T &lhs,
                  const RHS_T &rhs,
                  const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
                  const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs) {

    return (index_join_helper_<LHS_T, RHS_T, Ts ...>
        (lhs, rhs,
         get_inner_index_idx_vector_<IndexType>(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::
column_inner_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                   const StlVecType<JoinSortingPair<T>> &col_vec_rhs)  {

    return (column_join_helper_<LHS_T, RHS_T, T, Ts ...>
                (lhs, rhs, col_name,
                 get_inner_index_idx_vector_<T>(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::IndexIdxVector
DataFrame<I, H>::get_left_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = col_vec_lhs.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = col_vec_rhs.size();
    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(lhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) [[likely]] {
        if (lhs_current >= lhs_end) [[unlikely]]  break;
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
                *(col_vec_rhs[rhs_current].first))  {
                const auto  &prev_value = *(col_vec_lhs[lhs_current].first);

                while (rhs_current < rhs_end &&
                       *(col_vec_lhs[lhs_current].first) ==
                       *(col_vec_rhs[rhs_current].first))  {
                    joined_index_idx.emplace_back(
                        col_vec_lhs[lhs_current].second,
                        col_vec_rhs[rhs_current++].second);
                }
                lhs_current += 1;
                while (lhs_current < lhs_end &&
                       *(col_vec_lhs[lhs_current].first) == prev_value)  {
                    joined_index_idx.emplace_back(
                        col_vec_lhs[lhs_current++].second,
                        col_vec_rhs[rhs_current - 1].second);
                }
            }
            else  rhs_current += 1;
        }
    }
    return (joined_index_idx);
}
// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
index_left_join_(const LHS_T &lhs, const RHS_T &rhs,
                 const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
                 const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs) {

    return (index_join_helper_<LHS_T, RHS_T, Ts ...>
                (lhs, rhs,
                 get_left_index_idx_vector_<IndexType>(col_vec_lhs,
                                                       col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::
column_left_join_(const LHS_T &lhs,
                  const RHS_T &rhs,
                  const char *col_name,
                  const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                  const StlVecType<JoinSortingPair<T>> &col_vec_rhs)  {

    return (column_join_helper_<LHS_T, RHS_T, T, Ts ...>
                (lhs, rhs, col_name,
                 get_left_index_idx_vector_<T>(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::IndexIdxVector
DataFrame<I, H>::get_right_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = col_vec_lhs.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = col_vec_rhs.size();
    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(rhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) [[likely]] {
        if (rhs_current >= rhs_end)  break;
        if (lhs_current >= lhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                col_vec_rhs[rhs_current++].second);
            continue;
        }

        if (*(col_vec_lhs[lhs_current].first) <
            *(col_vec_rhs[rhs_current].first))  {
            lhs_current += 1;
        }
        else  {
            if (*(col_vec_rhs[rhs_current].first) ==
                *(col_vec_lhs[lhs_current].first))  {
                const auto  &prev_value = *(col_vec_rhs[rhs_current].first);

                while (lhs_current < lhs_end &&
                       *(col_vec_rhs[rhs_current].first) ==
                       *(col_vec_lhs[lhs_current].first))  {
                    joined_index_idx.emplace_back(
                        col_vec_lhs[lhs_current++].second,
                        col_vec_rhs[rhs_current].second);
                }
                rhs_current += 1;
                while (rhs_current < rhs_end &&
                       *(col_vec_rhs[rhs_current].first) == prev_value)  {
                    joined_index_idx.emplace_back(
                        col_vec_lhs[lhs_current - 1].second,
                        col_vec_rhs[rhs_current++].second);
                }
            }
            else  {
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    col_vec_rhs[rhs_current++].second);
            }
        }
    }
    return (joined_index_idx);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
index_right_join_(const LHS_T &lhs, const RHS_T &rhs,
                  const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
                  const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs) {

    return (index_join_helper_<LHS_T, RHS_T, Ts ...>
                (lhs, rhs,
                 get_right_index_idx_vector_<IndexType>(col_vec_lhs,
                                                        col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::
column_right_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                   const StlVecType<JoinSortingPair<T>> &col_vec_rhs)  {

    return (column_join_helper_<LHS_T, RHS_T, T, Ts ...>
                (lhs, rhs, col_name,
                 get_right_index_idx_vector_<T>(col_vec_lhs, col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::IndexIdxVector
DataFrame<I, H>::get_left_right_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = col_vec_lhs.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = col_vec_rhs.size();
    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(std::max(lhs_end, rhs_end));
    while (lhs_current != lhs_end || rhs_current != rhs_end) [[likely]] {
        if (lhs_current >= lhs_end && rhs_current < rhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                col_vec_rhs[rhs_current++].second);
        }
        else if (rhs_current >= rhs_end && lhs_current < lhs_end)  {
            joined_index_idx.emplace_back(
                col_vec_lhs[lhs_current++].second,
                std::numeric_limits<size_type>::max());
            continue;
        }
        else if (*(col_vec_lhs[lhs_current].first) <
                 *(col_vec_rhs[rhs_current].first))  {
            joined_index_idx.emplace_back(
                col_vec_lhs[lhs_current++].second,
                std::numeric_limits<size_type>::max());
        }
        else  {
            if (*(col_vec_lhs[lhs_current].first) ==
                *(col_vec_rhs[rhs_current].first))  {
                const auto  &prev_value = *(col_vec_lhs[lhs_current].first);

                while (rhs_current < rhs_end &&
                       *(col_vec_lhs[lhs_current].first) ==
                       *(col_vec_rhs[rhs_current].first))  {
                    joined_index_idx.emplace_back(
                        col_vec_lhs[lhs_current].second,
                        col_vec_rhs[rhs_current++].second);
                }
                lhs_current += 1;
                while (lhs_current < lhs_end &&
                       *(col_vec_lhs[lhs_current].first) == prev_value)  {
                    joined_index_idx.emplace_back(
                        col_vec_lhs[lhs_current++].second,
                        col_vec_rhs[rhs_current - 1].second);
                }
            }
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    col_vec_rhs[rhs_current++].second);
        }
    }
    return (joined_index_idx);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
index_left_right_join_(
    const LHS_T &lhs,
    const RHS_T &rhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs) {

    return (index_join_helper_<LHS_T, RHS_T, Ts ...>
                (lhs, rhs,
                 get_left_right_index_idx_vector_<IndexType>(col_vec_lhs,
                                                             col_vec_rhs)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::
column_left_right_join_(const LHS_T &lhs,
                        const RHS_T &rhs,
                        const char *col_name,
                        const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                        const StlVecType<JoinSortingPair<T>> &col_vec_rhs)  {

    return (column_join_helper_<LHS_T, RHS_T, T, Ts ...>
                (lhs, rhs, col_name,
                 get_left_right_index_idx_vector_<T>(col_vec_lhs,
                                                     col_vec_rhs)));
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
    //
    for (const auto &lhs_iter : lhs.column_list_) [[likely]]  {
        const auto  rhs_citer = rhs.column_tb_.find(lhs_iter.first);

        if (rhs_citer != rhs.column_tb_.end()) [[likely]]  {
            concat_functor_<LHS_T, Ts ...>  functor(lhs_iter.first.c_str(),
                                                    lhs,
                                                    false,
                                                    orig_index_s);

            rhs.data_[rhs_citer->second].change(functor);
        }
    }

    // Load columns from rhs that do not exist in lhs
    //
    if (add_new_columns)  {
        for (const auto &[rhs_name, rhs_idx] : rhs.column_list_) [[likely]]  {
            if (! lhs.column_tb_.contains(rhs_name))  {
                concat_functor_<LHS_T, Ts ...>  functor(rhs_name.c_str(),
                                                        lhs,
                                                        true,
                                                        orig_index_s);

                rhs.data_[rhs_idx].change(functor);
            }
        }
    }
}


// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
void
DataFrame<I, H>::self_concat(const RHS_T &rhs, bool add_new_columns)  {

    static_assert(
        (std::is_base_of<
             DataFrame<I,
                       HeteroVector<std::size_t(H::align_value)>>,
                       RHS_T>::value ||
         std::is_base_of<View, RHS_T>::value ||
         std::is_base_of<PtrView, RHS_T>::value) &&
        ! std::is_base_of<DataFrame<I,
                                    HeteroVector<std::size_t(H::align_value)>>,
                          decltype(*this)>::value,
        "The rhs argument to self_concat() can only be "
        "StdDataFrame<IndexType> or DataFrame[Ptr]View<IndexType>. "
        "Self must be StdDataFrame<IndexType>");

    const SpinGuard guard(lock_);

    concat_helper_<decltype(*this), RHS_T, Ts ...>
        (*this, rhs, add_new_columns);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::concat(const RHS_T &rhs, concat_policy cp) const  {

    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    res_t           result;
    const SpinGuard guard(lock_);

    if (cp == concat_policy::all_columns ||
        cp == concat_policy::lhs_and_common_columns)  {

        if constexpr (std::is_same<res_t, DataFrame>::value)
            result = *this;
        else
            result.template assign<DataFrame, Ts ...>(*this);

        concat_helper_<res_t, RHS_T, Ts ...>(
            result,
            rhs,
            cp == concat_policy::all_columns);
    }
    else if (cp == concat_policy::common_columns)  {
        result.load_index(this->get_index().begin(), this->get_index().end());

        for (const auto &[lhs_name, lhs_idx] : column_list_)  {
            if (rhs.column_tb_.contains(lhs_name))  {
                load_all_functor_<res_t, Ts ...>    functor(
                    lhs_name.c_str(),
                    result);

                data_[lhs_idx].change(functor);
            }
        }
        concat_helper_<res_t, RHS_T, Ts ...>(result, rhs, false);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
typename DataFrame<I, H>::PtrView
DataFrame<I, H>::concat_view(RHS_T &rhs, concat_policy cp)  {

    PtrView result;

    using idxvec_t = typename PtrView::IndexVecType;

    const size_type idx_s = get_index().size();
    const size_type rhs_idx_s = rhs.get_index().size();
    idxvec_t        result_idx;

    result_idx.reserve(idx_s + rhs_idx_s);
    for (size_type i = 0; i < idx_s; ++i) [[likely]]
        result_idx.push_back(&(get_index()[i]));
    for (size_type i = 0; i < rhs_idx_s; ++i) [[likely]]
        result_idx.push_back(&(rhs.get_index()[i]));
    result.indices_ = std::move(result_idx);

    if (cp == concat_policy::all_columns)  {
        for (const auto &[lhs_name, lhs_idx] : column_list_)  {
            concat_load_view_functor_<PtrView, Ts ...>  functor(
                lhs_name.c_str(), result);

            data_[lhs_idx].change(functor);
        }
        for (const auto &[rhs_name, rhs_idx] : rhs.column_list_)  {
            concat_load_view_functor_<PtrView, Ts ...>  functor(
                rhs_name.c_str(), result);

            rhs.data_[rhs_idx].change(functor);
        }
    }
    else if (cp == concat_policy::lhs_and_common_columns)  {
        for (const auto &[lhs_name, lhs_idx] : column_list_)  {
            concat_load_view_functor_<PtrView, Ts ...>  functor(
                lhs_name.c_str(), result);

            data_[lhs_idx].change(functor);

            const auto  rhs_citer = rhs.column_tb_.find(lhs_name);

            if (rhs_citer != rhs.column_tb_.end())
                rhs.data_[rhs_citer->second].change(functor);
        }
    }
    else if (cp == concat_policy::common_columns)  {
        for (const auto &[lhs_name, lhs_idx] : column_list_)  {
            const auto  rhs_citer = rhs.column_tb_.find(lhs_name);

            if (rhs_citer != rhs.column_tb_.end())  {
                concat_load_view_functor_<PtrView, Ts ...>  functor(
                    lhs_name.c_str(), result);

                data_[lhs_idx].change(functor);
                rhs.data_[rhs_citer->second].change(functor);
            }
        }
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
typename DataFrame<I, H>::ConstPtrView
DataFrame<I, H>::concat_view(RHS_T &rhs, concat_policy cp) const  {

    ConstPtrView    result;

    using idxvec_t = typename ConstPtrView::IndexVecType;

    const size_type idx_s = get_index().size();
    const size_type rhs_idx_s = rhs.get_index().size();
    idxvec_t        result_idx;

    result_idx.reserve(idx_s + rhs_idx_s);
    for (size_type i = 0; i < idx_s; ++i) [[likely]]
        result_idx.push_back(&(get_index()[i]));
    for (size_type i = 0; i < rhs_idx_s; ++i) [[likely]]
        result_idx.push_back(&(rhs.get_index()[i]));
    result.indices_ = std::move(result_idx);

    if (cp == concat_policy::all_columns)  {
        for (const auto &[lhs_name, lhs_idx] : column_list_)  {
            concat_load_view_functor_<ConstPtrView, Ts ...> functor(
                lhs_name.c_str(), result);

            data_[lhs_idx].change(functor);
        }
        for (const auto &[rhs_name, rhs_idx] : rhs.column_list_)  {
            concat_load_view_functor_<ConstPtrView, Ts ...> functor(
                rhs_name.c_str(), result);

            rhs.data_[rhs_idx].change(functor);
        }
    }
    else if (cp == concat_policy::lhs_and_common_columns)  {
        for (const auto &[lhs_name, lhs_idx] : column_list_)  {
            concat_load_view_functor_<ConstPtrView, Ts ...> functor(
                lhs_name.c_str(), result);

            data_[lhs_idx].change(functor);

            const auto  rhs_citer = rhs.column_tb_.find(lhs_name);

            if (rhs_citer != rhs.column_tb_.end())
                rhs.data_[rhs_citer->second].change(functor);
        }
    }
    else if (cp == concat_policy::common_columns)  {
        for (const auto &[lhs_name, lhs_idx] : column_list_)  {
            const auto  rhs_citer = rhs.column_tb_.find(lhs_name);

            if (rhs_citer != rhs.column_tb_.end())  {
                concat_load_view_functor_<ConstPtrView, Ts ...> functor(
                    lhs_name.c_str(), result);

                data_[lhs_idx].change(functor);
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
