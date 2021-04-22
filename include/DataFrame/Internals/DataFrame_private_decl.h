// Hossein Moein
// April 21, 2021
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

#pragma once

// ----------------------------------------------------------------------------

// This file was factored out so DataFrame.h doesn't become a huge file.
// This was meant to be included inside the private section of DataFrame class.
// This file, by itself, is not useable/compile-able.

// ----------------------------------------------------------------------------

template<typename DF, template<typename> class OPT, typename ... Ts>
friend DF
binary_operation(const DF &lhs, const DF &rhs);

template<typename T1, typename T2>
size_type
load_pair_(std::pair<T1, T2> &col_name_data);

void read_json_(std::istream &file, bool columns_only);
void read_csv_(std::istream &file, bool columns_only);
void read_csv2_(std::istream &file, bool columns_only);

template<typename CF, typename ... Ts>
static void
sort_common_(DataFrame<I, H> &df, CF &&comp_func);

template<typename T>
static void
fill_missing_value_(ColumnVecType<T> &vec,
                    const T &value,
                    int limit,
                    size_type col_num);

template<typename T>
static void
fill_missing_ffill_(ColumnVecType<T> &vec, int limit, size_type col_num);

template<typename T,
         typename std::enable_if<
             std::is_arithmetic<T>::value &&
             std::is_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_midpoint_(ColumnVecType<T> &vec, int limit, size_type col_num);

template<typename T,
         typename std::enable_if<
             ! std::is_arithmetic<T>::value ||
             ! std::is_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_midpoint_(ColumnVecType<T> &vec, int limit, size_type col_num);

template<typename T>
static void
fill_missing_bfill_(ColumnVecType<T> &vec, int limit);

template<typename T,
         typename std::enable_if<
             std::is_arithmetic<T>::value &&
             std::is_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_linter_(ColumnVecType<T> &vec,
                     const IndexVecType &index,
                     int limit);

template<typename T,
         typename std::enable_if<
             ! std::is_arithmetic<T>::value ||
             ! std::is_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_linter_(ColumnVecType<T> &, const IndexVecType &, int);

// Maps row number -> number of missing column(s)
using DropRowMap = std::map<size_type, size_type>;

template<typename T>
static void
drop_missing_rows_(T &vec,
                   const DropRowMap missing_row_map,
                   drop_policy policy,
                   size_type threshold,
                   size_type col_num);

template<typename T, typename ITR>
void
setup_view_column_(const char *name, Index2D<ITR> range);

using IndexIdxVector = std::vector<std::tuple<size_type, size_type>>;
template<typename T>
using JoinSortingPair = std::pair<const T *, size_type>;

template<typename LHS_T, typename RHS_T, typename IDX_T, typename ... Ts>
static void
join_helper_common_(const LHS_T &lhs,
                    const RHS_T &rhs,
                    const IndexIdxVector &joined_index_idx,
                    StdDataFrame<IDX_T> &result,
                    const char *skip_col_name = nullptr);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static StdDataFrame<IndexType>
index_join_helper_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const IndexIdxVector &joined_index_idx);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static StdDataFrame<unsigned int>
column_join_helper_(const LHS_T &lhs,
                    const RHS_T &rhs,
                    const char *col_name,
                    const IndexIdxVector &joined_index_idx);

template<typename T>
static IndexIdxVector
get_inner_index_idx_vector_(
    const std::vector<JoinSortingPair<T>> &col_vec_lhs,
    const std::vector<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static StdDataFrame<IndexType>
index_inner_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static StdDataFrame<unsigned int>
column_inner_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                   const std::vector<JoinSortingPair<T>> &col_vec_rhs);

template<typename T>
static IndexIdxVector
get_left_index_idx_vector_(
    const std::vector<JoinSortingPair<T>> &col_vec_lhs,
    const std::vector<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static StdDataFrame<IndexType>
index_left_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static StdDataFrame<unsigned int>
column_left_join_(const LHS_T &lhs,
                  const RHS_T &rhs,
                  const char *col_name,
                  const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                  const std::vector<JoinSortingPair<T>> &col_vec_rhs);

template<typename T>
static IndexIdxVector
get_right_index_idx_vector_(
    const std::vector<JoinSortingPair<T>> &col_vec_lhs,
    const std::vector<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static StdDataFrame<IndexType>
index_right_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static StdDataFrame<unsigned int>
column_right_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                   const std::vector<JoinSortingPair<T>> &col_vec_rhs);

template<typename MAP, typename ... Ts>
static StdDataFrame<IndexType>
remove_dups_common_(const DataFrame &s_df,
                    remove_dup_spec rds,
                    const MAP &row_table,
                    const IndexVecType &index);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static void
concat_helper_(LHS_T &lhs, const RHS_T &rhs, bool add_new_columns);

template<typename T>
static IndexIdxVector
get_left_right_index_idx_vector_(
    const std::vector<JoinSortingPair<T>> &col_vec_lhs,
    const std::vector<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static StdDataFrame<IndexType>
index_left_right_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
    const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static StdDataFrame<unsigned int>
column_left_right_join_(
    const LHS_T &lhs,
    const RHS_T &rhs,
    const char *col_name,
    const std::vector<JoinSortingPair<T>> &col_vec_lhs,
    const std::vector<JoinSortingPair<T>> &col_vec_rhs);

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
