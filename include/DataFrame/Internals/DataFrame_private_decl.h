// Hossein Moein
// April 21, 2021
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

#pragma once

#include <ranges>

// ----------------------------------------------------------------------------

// This file was factored out so DataFrame.h doesn't become a huge file.
// This was meant to be included inside the private section of DataFrame class.
// This file, by itself, is not useable/compile-able.

// ----------------------------------------------------------------------------

template<typename DF, template<typename> class OPT, typename ... Ts>
friend DF
binary_operation(const DF &lhs, const DF &rhs);

template<typename DF, typename ST,
         template<typename> class OPT, typename ... Ts>
friend DF
sc_binary_operation(const DF &lhs, const ST &rhs);

// ----------------------------------------------------------------------------

// Maps row number -> number of missing column(s)
//
using DropRowMap = DFMap<size_type, size_type>;
using IndexIdxVector = StlVecType<std::tuple<size_type, size_type>>;

template<typename T>
using JoinSortingPair = std::pair<const T *, size_type>;

// ----------------------------------------------------------------------------

void read_json_(std::istream &file, bool columns_only);
void read_csv_(std::istream &file, bool columns_only);
void read_csv2_(std::istream &file,
                bool columns_only,
                size_type starting_row,
                size_type num_rows);

template<typename T, typename ITR>
void
setup_view_column_(const char *name, Index2D<ITR> range);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_join_helper_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const IndexIdxVector &joined_index_idx);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
column_join_helper_(const LHS_T &lhs,
                    const RHS_T &rhs,
                    const char *col_name,
                    const IndexIdxVector &joined_index_idx);

template<typename T>
static IndexIdxVector
get_inner_index_idx_vector_(const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                            const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_inner_join_(const LHS_T &lhs, const RHS_T &rhs,
                  const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
                  const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
column_inner_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                   const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename T>
static IndexIdxVector
get_left_index_idx_vector_(const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                           const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_left_join_(const LHS_T &lhs, const RHS_T &rhs,
                 const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
                 const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
column_left_join_(const LHS_T &lhs,
                  const RHS_T &rhs,
                  const char *col_name,
                  const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                  const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename T>
static IndexIdxVector
get_right_index_idx_vector_(const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                            const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_right_join_(const LHS_T &lhs, const RHS_T &rhs,
                  const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
                  const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
column_right_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                   const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static void
concat_helper_(LHS_T &lhs, const RHS_T &rhs, bool add_new_columns);

template<typename T>
static IndexIdxVector
get_left_right_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_left_right_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
column_left_right_join_(const LHS_T &lhs,
                        const RHS_T &rhs,
                        const char *col_name,
                        const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                        const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

// ----------------------------------------------------------------------------

template<typename T1, typename T2>
size_type
load_pair_(std::pair<T1, T2> &col_name_data, bool do_lock = true)  {

    return (load_column<typename decltype(col_name_data.second)::value_type>(
                col_name_data.first, // column name
                std::forward<T2>(col_name_data.second),
                nan_policy::pad_with_nans,
                do_lock));
}

// ----------------------------------------------------------------------------

template<typename T>
size_type
append_row_(std::pair<const char *, T> &row_name_data)  {

    return (append_column<T>(row_name_data.first, // column name
                             std::forward<T>(row_name_data.second),
                             nan_policy::dont_pad_with_nans));
}

// ----------------------------------------------------------------------------

template<typename T>
static void
drop_missing_rows_(T &vec,
                   const DropRowMap &missing_row_map,
                   drop_policy policy,
                   size_type threshold,
                   size_type col_num)  {

    size_type   erase_count = 0;
    auto        dropper =
        [&vec, &erase_count](const auto &idx) -> void  {
            vec.erase(vec.begin() + (idx - erase_count++));
        };

    if (policy == drop_policy::all)  {
        for (const auto &[idx, count] : missing_row_map)
            if (count == col_num)  dropper(idx);
    }
    else if (policy == drop_policy::any)  {
        for (const auto &[idx, count] : missing_row_map)
            if (count > 0)  dropper(idx);
    }
    else if (policy == drop_policy::threshold)  {
        for (const auto &[idx, count] : missing_row_map)
            if (count > threshold)  dropper(idx);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename T>
static void
fill_missing_value_(ColumnVecType<T> &vec,
                    const T &value,
                    int limit,
                    size_type col_num)  {

    const size_type vec_size = vec.size();
    int             count = 0;

    if (limit < 0)
        vec.reserve(col_num);
    for (size_type i = 0; i < col_num; ++i)  {
        if (limit >= 0 && count >= limit)  break;
        if (i >= vec_size)  {
            vec.push_back(value);
            count += 1;
        }
        else if (is_nan<T>(vec[i]))  {
            vec[i] = value;
            count += 1;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename T>
static void
fill_missing_ffill_(ColumnVecType<T> &vec, int limit, size_type col_num)  {

    const size_type vec_size = vec.size();

    if (vec_size == 0)  return;

    int count = 0;
    T   last_value = vec[0];

    for (size_type i = 1; i < col_num; ++i)  {
        if (limit >= 0 && count >= limit)  break;
        if (i >= vec_size)  {
            if (! is_nan(last_value))  {
                vec.reserve(col_num);
                vec.push_back(last_value);
                count += 1;
            }
            else  break;
        }
        else  {
            if (! is_nan<T>(vec[i]))
                last_value = vec[i];
            else if (! is_nan<T>(last_value))  {
                vec[i] = last_value;
                count += 1;
            }
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename T,
         typename std::enable_if<
             supports_arithmetic<T>::value &&
             supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_midpoint_(ColumnVecType<T> &vec, int limit, size_type)  {

    const size_type vec_size = vec.size();

    if (vec_size < 3) [[unlikely]]  return;

    int count = 0;
    T   last_value = vec[0];

    for (size_type i = 1; i < vec_size - 1; ++i)  {
        if (limit >= 0 && count >= limit)  break;

        if (! is_nan<T>(vec[i]))
            last_value = vec[i];
        else if (! is_nan<T>(last_value))  {
            for (size_type j = i + 1; j < vec_size; ++j)  {
                if (! is_nan<T>(vec[j]))  {
                    vec[i] = (last_value + vec[j]) / T(2);
                    last_value = vec[i];
                    count += 1;
                    break;
                }
            }
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename T,
         typename std::enable_if<
             ! supports_arithmetic<T>::value ||
             ! supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_midpoint_(ColumnVecType<T> &, int, size_type)  {

    throw NotFeasible("fill_missing_midpoint_(): ERROR: Mid-point filling is "
                      "not feasible on non-arithmetic types");
}

// ----------------------------------------------------------------------------

template<typename T>
static void
fill_missing_bfill_(ColumnVecType<T> &vec, int limit)  {

    const long  vec_size = static_cast<long>(vec.size());

    if (vec_size == 0)  return;

    int count = 0;
    T   last_value = vec[vec_size - 1];

    for (long i = vec_size - 1; i >= 0; --i)  {
        if (limit >= 0 && count >= limit)  break;
        if (! is_nan<T>(vec[i]))  last_value = vec[i];
        if (is_nan<T>(vec[i]) && ! is_nan<T>(last_value))  {
            vec[i] = last_value;
            count += 1;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename T,
         typename std::enable_if<
             supports_arithmetic<T>::value &&
             supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_linter_(ColumnVecType<T> &vec,
                     const IndexVecType &index,
                     int limit)  {

    const long  vec_size = static_cast<long>(vec.size());

    if (vec_size < 3)  return;

    int             count = 0;
    const T         *y1 = &(vec[0]);
    const T         *y2 = &(vec[2]);
    const IndexType *x = &(index[1]);
    const IndexType *x1 = &(index[0]);
    const IndexType *x2 = &(index[2]);

    for (long i = 1; i < vec_size - 1; ++i)  {
        if (limit >= 0 && count >= limit)  break;
        if (is_nan<T>(vec[i]))  {
            if (is_nan<T>(*y2))  {
                bool    found = false;

                for (long j = i + 1; j < vec_size; ++j)  {
                    if (! is_nan(vec[j]))  {
                        y2 = &(vec[j]);
                        x2 = &(index[j]);
                        found = true;
                        break;
                    }
                }
                if (! found)  break;
            }
            if (is_nan<T>(*y1))  {
                for (long j = i - 1; j >= 0; --j)  {
                    if (! is_nan(vec[j]))  {
                        y1 = &(vec[j]);
                        x1 = &(index[j]);
                        break;
                    }
                }
            }
            vec[i] =
                *y1 +
                (static_cast<T>(*x - *x1) / static_cast<T>(*x2 - *x1)) *
                (*y2 - *y1);
            count += 1;
        }
        if (i < (vec_size - 2))  {
            y1 = &(vec[i]);
            y2 = &(vec[i + 2]);
            x = &(index[i + 1]);
            x1 = &(index[i]);
            x2 = &(index[i + 2]);
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename T,
         typename std::enable_if<
             ! supports_arithmetic<T>::value ||
             ! supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_linter_(ColumnVecType<T> &, const IndexVecType &, int)  {

    throw NotFeasible("fill_missing_linter_(): ERROR: Interpolation is "
                      "not feasible on non-arithmetic types");
}

// ----------------------------------------------------------------------------

template<typename ... Ts>
void remove_data_by_sel_common_(const StlVecType<size_type> &col_indices)  {

    const auto  thread_level =
        (indices_.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();
    SpinGuard   guard (lock_);

    if (thread_level > 2)  {
        auto    lbd =
            [&col_indices = std::as_const(col_indices), this]
            (const auto &begin, const auto &end) -> void  {
                const sel_remove_functor_<Ts ...>   functor (col_indices);

                for (auto citer = begin; citer < end; ++citer)
                    this->data_[citer->second].change(functor);
            };
        auto    lbd_idx =
            [&col_indices = std::as_const(col_indices), this] () -> void  {
                const size_type col_indices_s = col_indices.size();
                size_type       del_count = 0;

                for (size_type i = 0; i < col_indices_s; ++i)  {
                    if constexpr (
                       std::is_base_of<HeteroVector<align_value>, H>::value)
                        this->indices_.erase(this->indices_.begin() +
                                             (col_indices[i] - del_count++));
                    else 
                        this->indices_.erase(col_indices[i] - del_count++);
                }
            };
            auto    future_idx = thr_pool_.dispatch(false, lbd_idx);
            auto    futures =
                thr_pool_.parallel_loop(column_list_.begin(),
                                        column_list_.end(),
                                        std::move(lbd));

            future_idx.get();
            for (auto &fut : futures)  fut.get();
    }
    else  {
        const sel_remove_functor_<Ts ...>   functor (col_indices);

        for (const auto &citer : column_list_)
            data_[citer.second].change(functor);
        guard.release();

        const size_type col_indices_s = col_indices.size();
        size_type       del_count = 0;

        for (size_type i = 0; i < col_indices_s; ++i)  {
            if constexpr (std::is_base_of<HeteroVector<align_value>, H>::value)
                indices_.erase(indices_.begin() +
                               (col_indices[i] - del_count++));
            else 
                indices_.erase(col_indices[i] - del_count++);
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename LHS_T, typename RHS_T, typename IDX_T, typename ... Ts>
static void
join_helper_common_(
    const LHS_T &lhs,
    const RHS_T &rhs,
    const IndexIdxVector &joined_index_idx,
    DataFrame<IDX_T, HeteroVector<std::size_t(H::align_value)>> &result,
    const char *skip_col_name = nullptr)  {

    using res_t = decltype(result);

    std::vector<std::future<void>>  futures;
    const auto                      thread_level =
        (lhs.indices_.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();


    if (thread_level > 2)
        futures.reserve(lhs.column_list_.size() + rhs.column_list_.size());

    const SpinGuard guard(lock_);

    // NOTE: I had to do this in two separate loops. Otherwise, it would
    //       occasionally crash in multithreaded mode under MacOS.
    //
    for (const auto &[name, idx] : lhs.column_list_) [[likely]]  {
        if (skip_col_name && name == skip_col_name)  continue;

        if (rhs.column_tb_.find(name) != rhs.column_tb_.end())  {
            create_join_common_col_functor_<res_t, Ts ...>   create_f(
                name.c_str(), result);

            lhs.data_[idx].change(create_f);
        }
        else  {
            create_col_functor_<res_t, Ts ...>  create_f(name.c_str(), result);

            lhs.data_[idx].change(create_f);
        }
    }

    // Load the common and lhs columns
    //
    for (const auto &[name, idx] : lhs.column_list_) [[likely]]  {
        if (skip_col_name && name == skip_col_name)  continue;

        // Common column between two frames
        //
        if (rhs.column_tb_.find(name) != rhs.column_tb_.end())  {
            auto    jcomm_lbd =
                [&name = std::as_const(name),
                 idx,
                 &lhs = std::as_const(lhs),
                 &rhs = std::as_const(rhs),
                 &joined_index_idx = std::as_const(joined_index_idx),
                 &result] () -> void  {
                    index_join_functor_common_<res_t, RHS_T, Ts ...>    functor(
                        name.c_str(),
                        rhs,
                        joined_index_idx,
                        result);

                    lhs.data_[idx].change(functor);
                };

            if (thread_level > 2)
                futures.emplace_back(thr_pool_.dispatch(false, jcomm_lbd));
            else
                jcomm_lbd();
        }
        else  {  // lhs only column
            auto    jlhs_lbd =
                [&name = std::as_const(name),
                 idx,
                 &lhs = std::as_const(lhs),
                 &joined_index_idx = std::as_const(joined_index_idx),
                 &result] () -> void  {
                    // 0 = Left
                    index_join_functor_oneside_<0, res_t, Ts ...>   functor (
                        name.c_str(),
                        joined_index_idx,
                        result);

                    lhs.data_[idx].change(functor);
                };

            if (thread_level > 2)
                futures.emplace_back(thr_pool_.dispatch(false, jlhs_lbd));
            else
                jlhs_lbd();
        }
    }

    // Load the rhs columns
    //
    for (const auto &[name, idx] : rhs.column_list_) [[likely]]  {
        const auto  lhs_citer = lhs.column_tb_.find(name);

        if (skip_col_name && name == skip_col_name)  continue;

        if (lhs_citer == lhs.column_tb_.end())  {  // rhs only column
            create_col_functor_<res_t, Ts ...>  create_f(name.c_str(), result);

            rhs.data_[idx].change(create_f);
        }

        if (lhs_citer == lhs.column_tb_.end())  {  // rhs only column
            auto    jrhs_lbd =
                [&name = std::as_const(name),
                 idx,
                 &rhs = std::as_const(rhs),
                 &joined_index_idx = std::as_const(joined_index_idx),
                 &result] () -> void  {
                    // 1 = Right
                    index_join_functor_oneside_<1, res_t, Ts ...>   functor (
                        name.c_str(),
                        joined_index_idx,
                        result);

                    rhs.data_[idx].change(functor);
                };

            if (thread_level > 2)
                futures.emplace_back(thr_pool_.dispatch(false, jrhs_lbd));
            else
                jrhs_lbd();
        }
    }

    for (auto &fut : futures)  fut.get();
}

// ----------------------------------------------------------------------------

template<typename MAP, typename ... Ts>
static
DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
remove_dups_common_(const DataFrame &s_df,
                    remove_dup_spec rds,
                    const MAP &row_table,
                    const IndexVecType &index)  {

    using count_vec = StlVecType<size_type>;
    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    count_vec   rows_to_del;

    rows_to_del.reserve(512);
    if (rds == remove_dup_spec::keep_first)  {
        for (const auto &[val_tuple, idx_vec] : row_table)  {
            if (idx_vec.size() > 1)  {
                for (size_type i = 1; i < idx_vec.size(); ++i)
                    rows_to_del.push_back(idx_vec[i]);
            }
        }
    }
    else if (rds == remove_dup_spec::keep_last)  {
        for (const auto &[val_tuple, idx_vec] : row_table)  {
            if (idx_vec.size() > 1)  {
                for (size_type i = 0; i < idx_vec.size() - 1; ++i)
                    rows_to_del.push_back(idx_vec[i]);
            }
        }
    }
    else  {  // remove_dup_spec::keep_none
        for (const auto &[val_tuple, idx_vec] : row_table)  {
            if (idx_vec.size() > 1)  {
                for (size_type i = 0; i < idx_vec.size(); ++i)
                    rows_to_del.push_back(idx_vec[i]);
            }
        }
    }

    res_t                        new_df;
    typename res_t::IndexVecType new_index (index.size() - rows_to_del.size());
    const SpinGuard              guard(lock_);

    // Load the index
    //
    _remove_copy_if_(index.begin(), index.end(), new_index.begin(),
                     [&rows_to_del = std::as_const(rows_to_del)]
                     (std::size_t n) -> bool  {
                         return (std::find(rows_to_del.begin(),
                                           rows_to_del.end(),
                                           n) != rows_to_del.end());
                     });
    new_df.load_index(std::move(new_index));

    // Create the columns, so loading can proceed in parallel
    //
    for (const auto &citer : s_df.column_list_)  {
        create_col_functor_<res_t, Ts ...>  functor(
            citer.first.c_str(), new_df);

        s_df.data_[citer.second].change(functor);
    }

    const auto  thread_level =
        (new_df.get_index().size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();

    if (thread_level > 2)  {
        auto    lbd =
            [&rows_to_del = std::as_const(rows_to_del),
             &s_df = std::as_const(s_df),
             &new_df]
            (const auto &begin, const auto &end) -> void  {
                for (auto citer = begin; citer < end; ++citer)  {
                    copy_remove_functor_<res_t, Ts ...> functor(
                        citer->first.c_str(),
                        rows_to_del,
                        new_df);

                    s_df.data_[citer->second].change(functor);
                }
            };
        auto    futures =
            thr_pool_.parallel_loop(s_df.column_list_.begin(),
                                    s_df.column_list_.end(),
                                    std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        for (const auto &citer : s_df.column_list_)  {
            copy_remove_functor_<res_t, Ts ...> functor(citer.first.c_str(),
                                                        rows_to_del,
                                                        new_df);

            s_df.data_[citer.second].change(functor);
        }
    }
    return (new_df);
}

// ----------------------------------------------------------------------------

template<typename ... Ts>
DataFrame<I, HeteroVector<align_value>>
data_by_sel_common_(const StlVecType<size_type> &col_indices,
                    size_type idx_s) const  {

    using res_t = DataFrame<I, HeteroVector<align_value>>;
    using idx_vec_t = res_t::IndexVecType;

    res_t       ret_df;
    idx_vec_t   new_index;

    new_index.reserve(col_indices.size());
    for (const auto &citer: col_indices) [[likely]]
        new_index.push_back(indices_[citer]);
    ret_df.load_index(std::move(new_index));

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        create_col_functor_<res_t, Ts ...>  functor(name.c_str(), ret_df);

        data_[idx].change(functor);
    }

    const auto  thread_level =
        (idx_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (thread_level > 2)  {
        auto    lbd =
            [&col_indices = std::as_const(col_indices), idx_s, &ret_df, this]
            (const auto &begin, const auto &end) -> void  {
                for (auto citer = begin; citer < end; ++citer)  {
                    sel_load_functor_<res_t, size_type, Ts ...> functor(
                        citer->first.c_str(),
                        col_indices,
                        idx_s,
                        ret_df);

                    this->data_[citer->second].change(functor);
                }
            };

        auto    futuers =
            thr_pool_.parallel_loop(column_list_.begin(),
                                    column_list_.end(),
                                    std::move(lbd));

        for (auto &fut : futuers)  fut.get();
    }
    else  {
        for (const auto &[name, idx] : column_list_) [[likely]]  {
            sel_load_functor_<res_t, size_type, Ts ...> functor(name.c_str(),
                                                                col_indices,
                                                                 idx_s,
                                                                 ret_df);

            data_[idx].change(functor);
        }
    }

    return (ret_df);
}

// ----------------------------------------------------------------------------

template<typename ... Ts>
PtrView
view_by_sel_common_(const StlVecType<size_type> &col_indices,
                    size_type idx_s)  {

    using TheView = PtrView;

    TheView                         ret_dfv;
    typename TheView::IndexVecType  new_index;

    new_index.reserve(col_indices.size());
    for (const auto &citer : col_indices) [[likely]]
        new_index.push_back(&(indices_[citer]));
    ret_dfv.indices_ = std::move(new_index);

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        sel_load_view_functor_<size_type, TheView, Ts ...>   functor (
            name.c_str(),
            col_indices,
            idx_s,
            ret_dfv);

        data_[idx].change(functor);
    }

    return (ret_dfv);
}

// ----------------------------------------------------------------------------

template<typename ... Ts>
ConstPtrView
view_by_sel_common_(const StlVecType<size_type> &col_indices,
                    size_type idx_s) const  {

    using TheView = ConstPtrView;

    TheView                         ret_dfv;
    typename TheView::IndexVecType  new_index;

    new_index.reserve(col_indices.size());
    for (const auto &citer: col_indices) [[likely]]
        new_index.push_back(&(indices_[citer]));
    ret_dfv.indices_ = std::move(new_index);

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        sel_load_view_functor_<size_type, TheView, Ts ...>   functor (
            name.c_str(),
            col_indices,
            idx_s,
            ret_dfv);

        data_[idx].change(functor);
    }

    return (ret_dfv);
}

// ----------------------------------------------------------------------------

template<typename V, typename T>
inline static void
replace_vector_vals_(V &data_vec,
                     const StlVecType<T> &old_values,
                     const StlVecType<T> &new_values,
                     std::size_t &count,
                     int limit)  {

    const std::size_t   vcnt = old_values.size();

#ifdef HMDF_SANITY_EXCEPTIONS
    if (vcnt != new_values.size())
        throw DataFrameError("replace_vector_vals_(): "
                             "vector sizes don't match");
#endif // HMDF_SANITY_EXCEPTIONS

    const std::size_t   vec_s = data_vec.size();

    for (std::size_t i = 0; i < vcnt; ++i) [[likely]]  {
        for (std::size_t j = 0; j < vec_s; ++j) [[likely]]  {
            if (limit >= 0 && count >= static_cast<std::size_t>(limit))
                return;
            if (old_values[i] == data_vec[j])  {
                data_vec[j] = new_values[i];
                count += 1;
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
inline static void
col_vector_push_back_func_(V &vec,
                           std::istream &file,
                           T (*converter)(const char *, char **, int),
                           io_format file_type = io_format::csv)  {

    std::string value;
    char        c = 0;

    value.reserve(1024);
    while (file.get(c)) [[likely]] {
        value.clear();
        if (file_type == io_format::csv && c == '\n')  break;
        else if (file_type == io_format::json && c == ']')  break;
        file.unget();
        _get_token_from_file_(file, ',', value,
                              file_type == io_format::json ? ']' : '\0');
        vec.push_back(static_cast<T>(converter(value.c_str(), nullptr, 0)));
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
inline static void
col_vector_push_back_cont_func_(V &vec,
                                std::istream &file,
                                T (*converter)(const char *))  {

    std::string value;
    char        c = 0;

    value.reserve(2048);
    while (file.get(c)) [[likely]] {
        if (c == '\n')  break;
        file.unget();
        value.clear();
        _get_token_from_file_(file, ',', value, '\0');
        vec.push_back(converter(value.c_str()));
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename V, typename Dummy = void>
struct  ColVectorPushBack_  {

    inline void
    operator ()(V &vec,
                std::istream &file,
                T (*converter)(const char *, char **),
                io_format file_type = io_format::csv) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            value.clear();
            _get_token_from_file_(file, ',', value,
                                  file_type == io_format::json ? ']' : '\0');
            vec.push_back(static_cast<T>(converter(value.c_str(), nullptr)));
        }
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  ColVectorPushBack_<const char *, StlVecType<std::string>, Dummy>  {

    inline void
    operator ()(StlVecType<std::string> &vec,
                std::istream &file,
                const char * (*)(const char *, char **),
                io_format file_type = io_format::csv) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            value.clear();
            _get_token_from_file_(file, ',', value,
                                  file_type == io_format::json ? ']' : '\0');
            vec.push_back(value);
        }
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  ColVectorPushBack_<DateTime, StlVecType<DateTime>, Dummy>  {

    inline void
    operator ()(StlVecType<DateTime> &vec,
                std::istream &file,
                DateTime (*)(const char *, char **),
                io_format file_type = io_format::csv) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            value.clear();
            _get_token_from_file_(file, ',', value,
                                  file_type == io_format::json ? ']' : '\0');

            time_t      t;
            int         n;
            DateTime    dt;

#ifdef _MSC_VER
            ::sscanf(value.c_str(), "%lld.%d", &t, &n);
#else
            ::sscanf(value.c_str(), "%ld.%d", &t, &n);
#endif // _MSC_VER
            dt.set_time(t, n);
            vec.emplace_back(std::move(dt));
        }
    }
};

// ----------------------------------------------------------------------------

inline static void
json_str_col_vector_push_back_(StlVecType<std::string> &vec,
                               std::istream &file)  {

    char    value[1024];
    char    c = 0;

    while (file.get(c))
        if (c != ' ' && c != '\n' && c != '\t')  {
            file.unget();
            break;
        }

    while (file.get(c)) [[likely]] {
        if (c == ']')  break;
        file.unget();

        std::size_t count = 0;

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '"')
            throw DataFrameError(
                "json_str_col_vector_push_back_(): ERROR: Expected '\"' (0)");

        while (file.get(c))
            if (c == '"')
                break;
            else
                value[count++] = c;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (1)");

        value[count] = 0;
        vec.push_back(value);

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c == ']')  break;
        else if (c != ',')
            throw DataFrameError(
                "json_str_col_vector_push_back_(): ERROR: Expected ',' (2)");
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename Dummy = void>
struct  IdxParserFunctor_  {

    void operator()(StlVecType<T> &, std::istream &, io_format)  {   }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<float, Dummy>  {

    inline void operator()(StlVecType<float> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        const ColVectorPushBack_<float, StlVecType<float>>  slug;

        slug(vec, file, &::strtof, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<double, Dummy>  {

    inline void operator()(StlVecType<double> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        const ColVectorPushBack_<double, StlVecType<double>>  slug;

        slug(vec, file, &::strtod, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long double, Dummy>  {

    inline void operator()(StlVecType<long double> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        const ColVectorPushBack_<long double, StlVecType<long double>>  slug;

        slug(vec, file, &::strtold, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<int, Dummy>  {

    inline void operator()(StlVecType<int> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long, Dummy>  {

    inline void operator()(StlVecType<long> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long long, Dummy>  {

    inline void operator()(StlVecType<long long> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtoll, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned int, Dummy>  {

    inline void operator()(StlVecType<unsigned int> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtoul, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned long, Dummy>  {

    inline void operator()(StlVecType<unsigned long> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtoul, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned long long, Dummy>  {

    inline void operator()(StlVecType<unsigned long long> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtoull, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<std::string, Dummy>  {

    inline void operator()(StlVecType<std::string> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        auto                   converter =
            [](const char *s, char **)-> const char * { return s; };
        const ColVectorPushBack_<const char *, StlVecType<std::string>>  slug;

        slug(vec, file, converter, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<DateTime, Dummy>  {

    inline void operator()(StlVecType<DateTime> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        auto                    converter =
            [](const char *, char **)-> DateTime  { return DateTime(); };
        const ColVectorPushBack_<DateTime, StlVecType<DateTime>>  slug;

        slug(vec, file, converter, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<bool, Dummy>  {

    inline void operator()(StlVecType<bool> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename T, typename Dummy = void>
struct  GenerateTSIndex_  {

    inline void
    operator ()(StlVecType<T> &index_vec,
                DateTime &start_di,
                const DateTime &end_di,
                time_frequency t_freq,
                long increment) const  {

        switch(t_freq)  {
        case time_frequency::annual:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(static_cast<T>(start_di.date()));
                    start_di.add_years(increment);
                }
            }
            break;
        case time_frequency::monthly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(static_cast<T>(start_di.date()));
                    start_di.add_months(increment);
                }
            }
            break;
        case time_frequency::weekly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(static_cast<T>(start_di.date()));
                    start_di.add_days(increment * 7);
                }
            }
            break;
        case time_frequency::daily:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(static_cast<T>(start_di.date()));
                    start_di.add_days(increment);
                }
            }
            break;
        case time_frequency::hourly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(static_cast<T>(start_di.time()));
                    start_di.add_seconds(increment * 60 * 60);
                }
            }
            break;
        case time_frequency::minutely:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(static_cast<T>(start_di.time()));
                    start_di.add_seconds(increment * 60);
                }
            }
            break;
        case time_frequency::secondly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(static_cast<T>(start_di.time()));
                    start_di.add_seconds(increment);
                }
            }
            break;
        case time_frequency::millisecondly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(static_cast<T>(start_di.long_time()));
                    start_di.add_nanoseconds(increment * 1000000);
                }
            }
            break;
        default:
            break;
        }
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  GenerateTSIndex_<DateTime, Dummy>  {

    inline void
    operator ()(StlVecType<DateTime> &index_vec,
                DateTime &start_di,
                const DateTime &end_di,
                time_frequency t_freq,
                long increment) const  {

        switch(t_freq)  {
        case time_frequency::annual:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(start_di);
                    start_di.add_years(increment);
                }
            }
            break;
        case time_frequency::monthly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(start_di);
                    start_di.add_months(increment);
                }
            }
            break;
        case time_frequency::weekly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(start_di);
                    start_di.add_days(increment * 7);
                }
            }
            break;
        case time_frequency::daily:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(start_di);
                    start_di.add_days(increment);
                }
            }
            break;
        case time_frequency::hourly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(start_di);
                    start_di.add_seconds(increment * 60 * 60);
                }
            }
            break;
        case time_frequency::minutely:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(start_di);
                    start_di.add_seconds(increment * 60);
                }
            }
            break;
        case time_frequency::secondly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(start_di);
                    start_di.add_seconds(increment);
                }
            }
            break;
        case time_frequency::millisecondly:
            {
                while (start_di < end_di)  {
                    index_vec.push_back(start_di);
                    start_di.add_nanoseconds(increment * 1000000);
                }
            }
            break;
        default:
            break;
        }
    }
};

// ----------------------------------------------------------------------------

template<typename T>
inline static void
get_mem_numbers_(const VectorView<T, align_value> &,
                 size_t &used_mem,
                 size_t &capacity_mem) {

    used_mem = sizeof(T *) * 2;
    capacity_mem = sizeof(T *) * 2;
}

// ----------------------------------------------------------------------------

template<typename T>
inline static void
get_mem_numbers_(const VectorPtrView<T, align_value> &container,
                 size_t &used_mem,
                 size_t &capacity_mem) {

    used_mem = container.size() * sizeof(T *);
    capacity_mem = container.capacity() * sizeof(T *);
}

// ----------------------------------------------------------------------------

template<typename T>
inline static void
get_mem_numbers_(const StlVecType<T> &container,
                 size_t &used_mem,
                 size_t &capacity_mem) {

    used_mem = container.size() * sizeof(T);
    capacity_mem = container.capacity() * sizeof(T);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
