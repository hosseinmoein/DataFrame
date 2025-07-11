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

#include <cstdio>
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

template<typename S>
void read_json_(S &file, bool columns_only);

template<typename S>
void read_binary_(S &file,
                  bool columns_only,
                  size_type starting_row,
                  size_type num_rows);

template<typename S>
void read_csv_(S &file, bool columns_only, char delim);

template<typename S>
void read_csv2_(S &stream,
                bool columns_only,
                size_type starting_row,
                size_type num_rows,
                bool skip_first_line,
                const std::vector<ReadSchema> &schema,
                char delim);

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

template<typename T, typename ITR>
void
setup_view_column_(const char *name, Index2D<ITR> range)  {

    static_assert(
        std::is_base_of<HeteroView<align_value>, DataVec>::value ||
        std::is_base_of<HeteroConstView<align_value>, DataVec>::value ||
        std::is_base_of<HeteroPtrView<align_value>, DataVec>::value ||
        std::is_base_of<HeteroConstPtrView<align_value>, DataVec>::value,
        "Only a DataFrameView or DataFramePtrView can "
        "call setup_view_column_()");

    DataVec dv;

    dv.set_begin_end_special(&*(range.begin), &*(range.end - 1));

    const SpinGuard guard(lock_);

    data_.emplace_back (dv);
    column_tb_.emplace (name, data_.size() - 1);
    column_list_.emplace_back (name, data_.size() - 1);

    return;
}

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

template<typename ITR>
inline size_type
load_column_(const char *name,
             Index2D<const ITR &> range,
             nan_policy padding,
             bool do_lock)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call load_column()");

    using value_t = decltype(ITR::begin);

    const auto          iter = column_tb_.find (name);
    StlVecType<value_t> *vec_ptr = nullptr;

    {
        const SpinGuard guard (do_lock ? lock_ : nullptr);

        if (iter == column_tb_.end()) [[likely]]
            vec_ptr = &(create_column<value_t>(name, false));
        else  {
            DataVec &hv = data_[iter->second];

            vec_ptr = &(hv.template get_vector<value_t>());
        }
    }

    vec_ptr->clear();
    vec_ptr->insert (vec_ptr->end(), range.begin, range.end);

    size_type       ret_cnt = std::distance(range.begin, range.end);
    const size_type idx_s = indices_.size();
    const size_type s = vec_ptr->size();

    if (padding == nan_policy::pad_with_nans && s < idx_s)  {
        for (size_type i = 0; i < idx_s - s; ++i)  {
            vec_ptr->push_back (std::move(get_nan<value_t>()));
            ret_cnt += 1;
        }
    }

    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename T>
inline size_type
load_column_(const char *name,
             StlVecType<T> &&column,
             nan_policy padding,
             bool do_lock)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call load_column()");

    const size_type idx_s = indices_.size();
    const size_type data_s = column.size();

    using value_t = typename StlVecType<T>::value_type;

    size_type   ret_cnt = data_s;

    if (padding == nan_policy::pad_with_nans && data_s < idx_s)  {
        for (size_type i = 0; i < idx_s - data_s; ++i)  {
            column.push_back (std::move(get_nan<value_t>()));
            ret_cnt += 1;
        }
    }

    const auto          iter = column_tb_.find (name);
    StlVecType<value_t> *vec_ptr = nullptr;
    const SpinGuard     guard (do_lock ? lock_ : nullptr);

    if (iter == column_tb_.end()) [[likely]]
        vec_ptr = &(create_column<value_t>(name, false));
    else  {
        DataVec &hv = data_[iter->second];

        vec_ptr = &(hv.template get_vector<value_t>());
    }

    *vec_ptr = std::move(column);
    return (ret_cnt);
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
             supports_arithmetic<T>::value &&
             supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_lagrange_(ColumnVecType<T> &vec,
                       const IndexVecType &index,
                       int limit)  {

    const size_type vec_size = vec.size();

    if (vec_size < 3)  return;

    int count { 0 };

    for (size_type k = 0; k < vec_size; ++k)  {
        if (limit >= 0 && count >= limit) [[unlikely]]  break;

        if (is_nan<T>(vec[k])) [[unlikely]]  {
            const auto  x = index[k];
            T           y { 0 };

            for (size_type i = 0; i < vec_size; ++i)  {
                T   product { static_cast<T>(index[i]) };

                for (size_type j = 0; j < vec_size; ++j)
                    if (i != j && index[i] != index[j]) [[likely]]
                        product *= static_cast<T>(x - index[j]) /
                                   static_cast<T>(index[i] - index[j]);

                y += product;
            }

            vec[k] = y;
            count += 1;
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
                    index_join_functor_common_<res_t, RHS_T, Ts ...>   functor(
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

    using count_set = DFUnorderedSet<size_type>;
    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    const size_type idx_s = index.size();
    count_set       rows_to_del;

    rows_to_del.reserve(idx_s / 100);
    if (rds == remove_dup_spec::keep_first)  {
        for (const auto &[val_tuple, idx_vec] : row_table)  {
            if (idx_vec.size() > 1)
                rows_to_del.insert(idx_vec.begin() + 1, idx_vec.end());
        }
    }
    else if (rds == remove_dup_spec::keep_last)  {
        for (const auto &[val_tuple, idx_vec] : row_table)  {
            if (idx_vec.size() > 1)
                rows_to_del.insert(idx_vec.begin(), idx_vec.end() - 1);
        }
    }
    else  {  // remove_dup_spec::keep_none
        for (const auto &[val_tuple, idx_vec] : row_table)
            if (idx_vec.size() > 1)
                rows_to_del.insert(idx_vec.begin(), idx_vec.end());
    }

    res_t                        new_df;
    typename res_t::IndexVecType new_index;
    const SpinGuard              guard(lock_);

    // Load the index
    //
    new_index.reserve(idx_s - rows_to_del.size());
    for (size_type i = 0; i < idx_s; ++i)  {
        if (! rows_to_del.contains(i))
            new_index.push_back(index[i]);
    }

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

template<typename T, typename V, typename R, typename ... Ts>
void top_n_common_(const char *col_name, V &&visitor, R &result) const  {

    using res_t = R;

    const ColumnVecType<T>  *vec { nullptr };

    if (! ::strcmp(col_name, DF_INDEX_COL_NAME))
        vec = (const ColumnVecType<T> *) &(get_index());
    else
        vec = (const ColumnVecType<T> *) &(get_column<T>(col_name));

    visitor.pre();
    visitor(indices_.begin(), indices_.end(), vec->begin(), vec->end());
    visitor.post();
    visitor.sort_by_index_idx();

    typename res_t::IndexVecType    new_index;
    StlVecType<size_type>           idxs;

    new_index.reserve(visitor.get_result().size());
    idxs.reserve(visitor.get_result().size());
    for (const auto &res : visitor.get_result())  {
        if constexpr (std::is_same_v<res_t,
                                     DataFrame<I, HeteroVector<align_value>>>)
            new_index.push_back(indices_[res.index_idx]);
        else  // Views
            new_index.push_back(
                &(const_cast<DataFrame *>(this)->indices_[res.index_idx]));
        idxs.push_back(res.index_idx);
    }
    result.indices_ = std::move(new_index);

    const SpinGuard guard(lock_);

    if constexpr (std::is_same_v<res_t,
                                 DataFrame<I, HeteroVector<align_value>>>)  {
        for (const auto &[name, idx] : column_list_) [[likely]]  {
            sel_load_functor_<res_t, size_type, Ts ...> functor(
                name.c_str(), idxs, 0, result);

            data_[idx].change(functor);
        }
    }
    else  {  // Views
        for (const auto &[name, idx] : column_list_) [[likely]]  {
            sel_load_view_functor_<size_type, res_t, Ts ...>    functor(
                name.c_str(), idxs, 0, result);

            data_[idx].change(functor);
        }
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename C, typename R, typename ... Ts>
void
above_quantile_common_(const char *col_name,
                       double quantile,
                       C &&comp_func,
                       R &result) const  {

    using res_t = R;

    const ColumnVecType<T>  *vec { nullptr };

    if (! ::strcmp(col_name, DF_INDEX_COL_NAME))
        vec = (const ColumnVecType<T> *) &(get_index());
    else
        vec = (const ColumnVecType<T> *) &(get_column<T>(col_name));

    QuantileVisitor<T, I>   quant { quantile };

    quant.pre();
    quant(indices_.begin(), indices_.end(), vec->begin(), vec->end());
    quant.post();

    typename res_t::IndexVecType    new_index;
    StlVecType<size_type>           idxs;

    new_index.reserve(vec->size() / 2);
    idxs.reserve(vec->size() / 2);
    for (size_type i { 0 }; i < vec->size(); ++i)  {
        if (comp_func((*vec)[i], quant.get_result()))  {
            if constexpr (std::is_same_v<
                              res_t,
                              DataFrame<I, HeteroVector<align_value>>>)
                new_index.push_back(indices_[i]);
            else  // Views
                new_index.push_back(
                    &(const_cast<DataFrame *>(this)->indices_[i]));
            idxs.push_back(i);
        }
    }
    result.indices_ = std::move(new_index);

    const SpinGuard guard(lock_);

    if constexpr (std::is_same_v<res_t,
                                  DataFrame<I, HeteroVector<align_value>>>)  {
        for (const auto &[name, idx] : column_list_) [[likely]]  {
            sel_load_functor_<res_t, size_type, Ts ...> functor(
                name.c_str(), idxs, 0, result);

            data_[idx].change(functor);
        }
    }
    else  {  // Views
        for (const auto &[name, idx] : column_list_) [[likely]]  {
            sel_load_view_functor_<size_type, res_t, Ts ...>    functor(
                name.c_str(), idxs, 0, result);

            data_[idx].change(functor);
        }
    }
}

// ----------------------------------------------------------------------------

template<typename V, typename T>
inline static void
replace_vector_vals_(V &data_vec,
                     const StlVecType<T> &old_values,
                     const StlVecType<T> &new_values,
                     std::size_t &count,
                     long limit)  {

#ifdef HMDF_SANITY_EXCEPTIONS
    if (old_values.size() != new_values.size())
        throw DataFrameError("replace_vector_vals_(): "
                             "old/new vector sizes don't match");
#endif // HMDF_SANITY_EXCEPTIONS

    using map_t = DFUnorderedMap<T, T>;

    const auto  v_zip = std::ranges::views::zip(old_values, new_values);
    const map_t v_map (v_zip.begin(), v_zip.end());
    const auto  thread_level =
        (data_vec.size() < ThreadPool::MUL_THR_THHOLD || limit >= 0)
            ? 0L : get_thread_level();
    auto        lbd =
        [&v_map = std::as_const(v_map)]
        (auto begin, auto end) -> std::size_t  {
            std::size_t count { 0 };

            for (auto iter = begin; iter < end; ++iter)  {
                const auto  map_iter = v_map.find(*iter);

                if (map_iter != v_map.end())  {
                    *iter = map_iter->second;
                    count += 1;
                }
            }
            return (count);
        };

    if (thread_level > 2)  {
        auto    futuers = thr_pool_.parallel_loop(data_vec.begin(),
                                                  data_vec.end(),
                                                  std::move(lbd));

        for (auto &fut : futuers)  count += fut.get();
    }
    else  {
        if (limit < 0) [[likely]]  {
            count = lbd(data_vec.begin(), data_vec.end());
        }
        else  {
            for (auto &data : data_vec)  {
                if (count >= static_cast<std::size_t>(limit))  return;

                const auto  map_iter = v_map.find(data);

                if (map_iter != v_map.end())  {
                    data = map_iter->second;
                    count += 1;
                }
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename V>
inline static void
col_vector_push_back_func_(
    typename V::value_type(*converter)(const char *, int),
    std::istream &file,
    V &vec,
    io_format file_type,
    char delim)  {

    std::string value;
    char        c = 0;

    while (file.get(c)) [[likely]] {
        value.clear();
        if (file_type == io_format::csv && c == '\n')  break;
        else if (file_type == io_format::json && c == ']')  break;
        file.unget();
        _get_token_from_file_(file, delim, value,
                              file_type == io_format::json ? ']' : '\0');
        vec.push_back(converter(value.c_str(), int(value.size())));
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
inline static void
col_vector_push_back_func_(V &vec,
                           std::istream &file,
                           T (*converter)(const char *, char **, int),
                           io_format file_type,
                           char delim)  {

    std::string value;
    char        c = 0;

    value.reserve(1024);
    while (file.get(c)) [[likely]] {
        value.clear();
        if (file_type == io_format::csv && c == '\n')  break;
        else if (file_type == io_format::json && c == ']')  break;
        file.unget();
        _get_token_from_file_(file, delim, value,
                              file_type == io_format::json ? ']' : '\0');
        vec.push_back(static_cast<T>(converter(value.c_str(), nullptr, 0)));
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
inline static void
col_vector_push_back_cont_func_(V &vec,
                                std::istream &file,
                                T (*converter)(const char *),
                                char delim)  {

    std::string value;
    char        c = 0;

    value.reserve(2048);
    while (file.get(c)) [[likely]] {
        if (c == '\n') [[unlikely]] break;
        file.unget();
        value.clear();
        _get_token_from_file_(file, delim, value, '\0');
        vec.push_back(converter(value.c_str()));
    }
}

// ----------------------------------------------------------------------------

template<typename NEW_COL_T, typename COL_T, typename ... Ts>
inline void
explode_helper_(DataFrame &result,
                size_type total_cnt,
                const char *col_name,
                const StlVecType<size_type> &idx_mask,
                const COL_T &col) const {

    ColumnVecType<NEW_COL_T>    new_col;

    new_col.reserve(total_cnt);
    for (size_type i { 0 }; i < col.size(); ++i)
        for (const auto &val : col[i])
            new_col.push_back(val);

    SpinGuard   guard (lock_);

    result.template load_column<NEW_COL_T>(col_name,
                                           std::move(new_col),
                                           nan_policy::dont_pad_with_nans,
                                           false);
    for (const auto &[name, idx] : column_list_) [[likely]]
        if (name != col_name)  {
            explode_functor_<Ts ...>   functor(name.c_str(), result, idx_mask);

            data_[idx].change(functor);
        }

    return;
}

// ----------------------------------------------------------------------------

template<typename T, typename V, typename Dummy = void>
struct  ColVectorPushBack_  {

    inline void
    operator ()(V &vec,
                std::istream &file,
                T (*converter)(const char *, char **),
                io_format file_type,
                char delim) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            value.clear();
            _get_token_from_file_(file, delim, value,
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
                io_format file_type,
                char delim) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            value.clear();
            _get_token_from_file_(file, delim, value,
                                  file_type == io_format::json ? ']' : '\0');

            if (file_type == io_format::json)  { // Get rid of "'s
                value.pop_back();
                value.erase(value.begin());
            }

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
                io_format file_type,
                char delim) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            value.clear();
            _get_token_from_file_(file, delim, value,
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

template<typename STR>
inline static void
json_str_col_vector_push_back_(StlVecType<STR> &vec,
                               std::istream &file,
                               char delim)  {

    char    value[2048];
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

        while (file.get(c))  {
            if (c == '"')  break;
            else  value[count++] = c;
        }
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (1)");

        value[count] = 0;
        vec.push_back(value);

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c == ']')  break;
        else if (c != delim)
            throw DataFrameError(
                "json_str_col_vector_push_back_(): ERROR: Expected ',' (2)");
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename Dummy = void>
struct  IdxParserFunctor_  {

    void
    operator()(StlVecType<T> &, std::istream &, io_format, char delim)  {   }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<float, Dummy>  {

    inline void operator()(StlVecType<float> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        const ColVectorPushBack_<float, StlVecType<float>>  slug;

        slug(vec, file, &::strtof, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<double, Dummy>  {

    inline void operator()(StlVecType<double> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        const ColVectorPushBack_<double, StlVecType<double>>  slug;

        slug(vec, file, &::strtod, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long double, Dummy>  {

    inline void operator()(StlVecType<long double> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        const ColVectorPushBack_<long double, StlVecType<long double>>  slug;

        slug(vec, file, &::strtold, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<int, Dummy>  {

    inline void operator()(StlVecType<int> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        col_vector_push_back_func_(&_atoi_<int>, file, vec, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long, Dummy>  {

    inline void operator()(StlVecType<long> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        col_vector_push_back_func_(&_atoi_<long>, file, vec, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long long, Dummy>  {

    inline void operator()(StlVecType<long long> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        col_vector_push_back_func_(&_atoi_<long long>, file, vec,
                                   file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned int, Dummy>  {

    inline void operator()(StlVecType<unsigned int> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        col_vector_push_back_func_(&_atoi_<unsigned int>, file, vec,
                                   file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned long, Dummy>  {

    inline void operator()(StlVecType<unsigned long> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        col_vector_push_back_func_(&_atoi_<unsigned long>,
                                   file, vec, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned long long, Dummy>  {

    inline void operator()(StlVecType<unsigned long long> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        col_vector_push_back_func_(&_atoi_<unsigned long long>,
                                   file, vec, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<std::string, Dummy>  {

    inline void operator()(StlVecType<std::string> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        auto                   converter =
            [](const char *s, char **)-> const char * { return s; };
        const ColVectorPushBack_<const char *, StlVecType<std::string>>  slug;

        slug(vec, file, converter, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<DateTime, Dummy>  {

    inline void operator()(StlVecType<DateTime> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        auto                    converter =
            [](const char *, char **)-> DateTime  { return DateTime(); };
        const ColVectorPushBack_<DateTime, StlVecType<DateTime>>  slug;

        slug(vec, file, converter, file_type, delim);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<bool, Dummy>  {

    inline void operator()(StlVecType<bool> &vec,
                           std::istream &file,
                           io_format file_type,
                           char delim) const  {

        col_vector_push_back_func_(vec, file, &::strtol, file_type, delim);
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

template<typename T>
inline Matrix<T, matrix_orient::column_major>
get_scaled_data_matrix_(std::vector<const char *> &&col_names,
                        normalization_type norm_type) const  {

    const size_type                         col_num = col_names.size();
    size_type                               min_col_s { indices_.size() };
    std::vector<const ColumnVecType<T> *>   columns(col_num, nullptr);
    SpinGuard                               guard (lock_);

    for (size_type i { 0 }; i < col_num; ++i)  {
        columns[i] = &get_column<T>(col_names[i], false);
        if (columns[i]->size() < min_col_s)
            min_col_s = columns[i]->size();
    }
    guard.release();

    Matrix<T, matrix_orient::column_major>  data_mat {
        long(min_col_s), long(col_num) };
    auto                                    lbd =
        [norm_type, &data_mat, &columns = std::as_const(columns), this]
        (auto begin, auto end) -> void  {
            if (norm_type > normalization_type::none)  {
                for (size_type c { begin }; c < end; ++c)  {
                    NormalizeVisitor<T, I>  norm_v { norm_type };

                    norm_v.pre();
                    norm_v(indices_.begin(), indices_.end(),
                           columns[c]->begin(), columns[c]->end());
                    norm_v.post();
                    data_mat.set_column(norm_v.get_result().begin(), c);
                }
            }
            else  {
                for (size_type c { begin }; c < end; ++c)
                    data_mat.set_column(columns[c]->begin(), c);
            }
        };
    const auto                              thread_level =
        (min_col_s >= ThreadPool::MUL_THR_THHOLD || col_num >= 20 )
            ? get_thread_level() : 0L;

    if (thread_level > 2)  {
        auto    futuers =
            thr_pool_.parallel_loop(size_type(0), col_num, std::move(lbd));

        for (auto &fut : futuers)  fut.get();
    }
    else  lbd(size_type(0), col_num);

    return (data_mat);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
