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
template<typename T>
typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::create_column (const char *name, bool do_lock)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call create_column()");

    if (! ::strcmp(name, DF_INDEX_COL_NAME)) [[unlikely]]
        throw DataFrameError ("DataFrame::create_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");
    if (column_tb_.find(name) != column_tb_.end()) [[unlikely]]
       return (get_column<T>(name));

    const SpinGuard guard(do_lock ? lock_ : nullptr);

    if (column_list_.empty()) [[unlikely]]  {
        column_list_.reserve(32);
        data_.reserve(32);
    }
    data_.emplace_back (DataVec());
    column_tb_.emplace (name, data_.size() - 1);
    column_list_.emplace_back (name, data_.size() - 1);

    DataVec &hv = data_.back();
    auto    &data_vec = hv.template get_vector<T>();

    return (data_vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::remove_column (const char *name)  {

    ColumnVecType<T>    &vec = get_column<T>(name);

    // Free the memory space
    //
    vec = ColumnVecType<T>{ };

    // I do not erase the column from the data_ vector, because it will mess up
    // indices in the hash table column_tb_
    /* data_.erase (data_.begin() + iter->second); */
    column_tb_.erase (name);
    for (size_type i = 0; i < column_list_.size(); ++i)  {
        if (column_list_[i].first == name)  {
            column_list_.erase(column_list_.begin() + i);
            break;
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::remove_column(size_type index)  {

    return (remove_column<T>(column_list_[index].first.c_str()));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::clear()  {

    {
        const SpinGuard guard(lock_);

        data_.clear();
    }
    indices_.clear();
    column_tb_.clear();
    column_list_.clear();
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::swap(DataFrame &other)  {

    {
        const SpinGuard guard(lock_);

        data_.swap(other.data_);
    }
    indices_.swap(other.indices_);
    column_tb_.swap(other.column_tb_);
    column_list_.swap(other.column_list_);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::rename_column (const char *from, const char *to)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call rename_column()");

    if (! ::strcmp(from, DF_INDEX_COL_NAME) ||
        ! ::strcmp(to, DF_INDEX_COL_NAME))
        throw DataFrameError ("DataFrame::rename_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

    const auto  from_iter = column_tb_.find (from);

    if (from_iter == column_tb_.end())  {
        char buffer [512];

        snprintf (buffer, sizeof(buffer) - 1,
                  "DataFrame::rename_column(): ERROR: Cannot find column '%s'",
                  from);
        throw ColNotFound (buffer);
    }
    if (column_tb_.find (to) != column_tb_.end())  {
        char buffer [512];

        snprintf (buffer, sizeof(buffer) - 1,
                  "DataFrame::rename_column(): "
                  "ERROR: Column '%s' already exists",
                  to);
        throw DataFrameError (buffer);
    }

    column_tb_.emplace (to, from_iter->second);
    column_list_.emplace_back (to, from_iter->second);
    column_tb_.erase (from);
    for (size_type i = 0; i < column_list_.size(); ++i)  {
        if (column_list_[i].first == from)  {
            column_list_.erase(column_list_.begin() + i);
            break;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename FROM_T, typename TO_T>
void DataFrame<I, H>::
retype_column (const char *name,
               std::function<TO_T (const FROM_T &)> convert_func)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call retype_column()");

    if (! ::strcmp(name, DF_INDEX_COL_NAME))
        throw DataFrameError ("DataFrame::retype_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

    const ColumnVecType<FROM_T> &old_vec = get_column<FROM_T>(name);
    StlVecType<TO_T>            new_vec;

    new_vec.reserve(old_vec.size());
    for (const auto &citer : old_vec)
        new_vec.push_back(std::move(convert_func(citer)));
    remove_column<FROM_T>(name);
    load_column<TO_T>(name, std::move(new_vec));
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::load_data (IndexVecType &&indices, Ts&& ... args)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call load_data()");

    size_type   cnt = load_index(std::forward<IndexVecType>(indices));
    auto        args_tuple = std::tuple<Ts ...>(std::forward<Ts>(args) ...);
    // const size_type tuple_size =
    //     std::tuple_size<decltype(args_tuple)>::value;
    auto        fc = [this, &cnt](auto &pa) mutable -> void {
                         cnt += this->load_pair_(pa, false);
                     };

    const SpinGuard guard(lock_);

    for_each_in_tuple (args_tuple, fc);

    return (cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ITR>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::load_index(const ITR &begin, const ITR &end)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call load_index()");

    indices_.clear();
    indices_.insert (indices_.end(), begin, end);
    return (indices_.size());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::load_index(IndexVecType &&idx)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call load_index()");

    indices_ = idx;
    return (indices_.size());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename DataFrame<I, H>::template StlVecType<I>
DataFrame<I, H>::
gen_datetime_index(const char *start_datetime,
                   const char *end_datetime,
                   time_frequency t_freq,
                   long increment,
                   DT_TIME_ZONE tz)  {

    DateTime                start_di(start_datetime,
                                     DT_DATE_STYLE::AME_STYLE, tz);
    const DateTime          end_di(end_datetime, DT_DATE_STYLE::AME_STYLE, tz);
    const double            diff = end_di.diff_seconds(start_di);
    StlVecType<IndexType>   index_vec;

    switch(t_freq)  {
    case time_frequency::annual:
        index_vec.reserve(
          static_cast<size_type>(diff / (365 * 24 * 60 * 60) / increment + 1));
        break;
    case time_frequency::monthly:
        index_vec.reserve(
          static_cast<size_type>(diff / (30 * 24 * 60 * 60) / increment + 1));
        break;
    case time_frequency::weekly:
        index_vec.reserve(
            static_cast<size_type>(diff / (7 * 24 * 60 * 60) / increment + 1));
        break;
    case time_frequency::daily:
        index_vec.reserve(
            static_cast<size_type>(diff / (24 * 60 * 60) / increment + 1));
        break;
    case time_frequency::hourly:
        index_vec.reserve(
            static_cast<size_type>(diff / (60 * 60) / increment + 1));
        break;
    case time_frequency::minutely:
        index_vec.reserve(
            static_cast<size_type>((diff / 60) / increment + 1));
        break;
    case time_frequency::secondly:
        index_vec.reserve(static_cast<size_type>((diff / increment) + 1));
        break;
    case time_frequency::millisecondly:
        index_vec.reserve(
            static_cast<size_type>((diff / increment) * 999 + 100));
        break;
    default:
        throw NotFeasible ("ERROR: gen_datetime_index()");
    }

    const GenerateTSIndex_<IndexType>   slug;

    slug(index_vec, start_di, end_di, t_freq, increment);

    return (index_vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename DataFrame<I, H>::template StlVecType<I>
DataFrame<I, H>::
gen_sequence_index (const IndexType &start_value,
                    const IndexType &end_value,
                    long increment)  {

    StlVecType<IndexType>   index_vec;
    IndexType               sv = start_value;

    while (sv < end_value)  {
        index_vec.push_back(sv);
        sv = sv + increment;
    }
    return (index_vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ITR>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::append_index(Index2D<const ITR &> range)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call append_index()");

    const size_type s = std::distance(range.begin, range.end);

    indices_.insert(indices_.end(), range.begin, range.end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::append_index(const IndexType &val)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call append_index()");

    indices_.push_back (val);
    return (1);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ITR>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_column (const char *name,
             Index2D<const ITR &> range,
             nan_policy padding,
             bool do_lock)  {

    const size_type s = std::distance(range.begin, range.end);
    const size_type idx_s = indices_.size();

    if (s > idx_s) [[unlikely]]  {
        char buffer [512];

        snprintf (buffer, sizeof(buffer) - 1,
                  "DataFrame::load_column(): ERROR: "
#ifdef _MSC_VER
                  "data size of %zu is larger than index size of %zu",
#else
                  "data size of %lu is larger than index size of %lu",
#endif // _MSC_VER
                 s, idx_s);
        throw InconsistentData (buffer);
    }

    return (load_column_<ITR>(name, range, padding, do_lock));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<has_result V>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_result_as_column(V &visitor,
                      const char *new_col_name,
                      nan_policy padding)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call load_result_as_column()");

    const size_type idx_s = indices_.size();
    auto            &new_col = visitor.get_result();
    const size_type data_s = new_col.size();

    if (data_s > idx_s) [[unlikely]]  {
        char buffer [512];

        snprintf (buffer, sizeof(buffer) - 1,
                  "DataFrame::load_result_as_column(): ERROR: "
#ifdef _MSC_VER
                  "data size of %zu is larger than index size of %zu",
#else
                  "data size of %lu is larger than index size of %lu",
#endif // _MSC_VER
                 data_s, idx_s);
        throw InconsistentData (buffer);
    }

    using new_type = typename V::result_type::value_type;

    size_type   ret_cnt = data_s;

    if (padding == nan_policy::pad_with_nans && data_s < idx_s)  {
        for (size_type i = 0; i < idx_s - data_s; ++i)  {
            new_col.push_back (std::move(get_nan<new_type>()));
            ret_cnt += 1;
        }
    }

    const auto              iter = column_tb_.find (new_col_name);
    StlVecType<new_type>    *vec_ptr = nullptr;

    {
        const SpinGuard guard(lock_);

        if (iter == column_tb_.end())
            vec_ptr = &(create_column<new_type>(new_col_name, false));
        else  {
            DataVec &hv = data_[iter->second];

            vec_ptr = &(hv.template get_vector<new_type>());
        }
    }

    *vec_ptr = std::move(new_col);
    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, has_result V>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_result_as_column(const char *col_name,
                      V &&visitor,
                      const char *new_col_name,
                      nan_policy padding)  {

    V   vis = std::move(visitor);

    single_act_visit<T, V>(col_name, vis);
    return (load_result_as_column<V>(vis, new_col_name, padding));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, has_result V>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_result_as_column(const char *col_name1,
                      const char *col_name2,
                      V &&visitor,
                      const char *new_col_name,
                      nan_policy padding)  {

    V   vis = std::move(visitor);

    single_act_visit<T1, T2, V>(col_name1, col_name2, vis);
    return (load_result_as_column<V>(vis, new_col_name, padding));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, has_result V>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_result_as_column(const char *col_name1,
                      const char *col_name2,
                      const char *col_name3,
                      V &&visitor,
                      const char *new_col_name,
                      nan_policy padding)  {

    V   vis = std::move(visitor);

    single_act_visit<T1, T2, T3, V>(col_name1, col_name2, col_name3, vis);
    return (load_result_as_column<V>(vis, new_col_name, padding));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, has_result V>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_result_as_column(const char *col_name1,
                      const char *col_name2,
                      const char *col_name3,
                      const char *col_name4,
                      V &&visitor,
                      const char *new_col_name,
                      nan_policy padding)  {

    V   vis = std::move(visitor);

    single_act_visit<T1, T2, T3, T4, V>(col_name1,
                                        col_name2,
                                        col_name3,
                                        col_name4,
                                        vis);
    return (load_result_as_column<V>(vis, new_col_name, padding));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_stringable T, typename IT>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_indicators(const char *cat_col_name, const char *numeric_cols_prefix)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call load_indicators()");

    using map_t = DFUnorderedMap<T, StlVecType<IT> *>;

    const SpinGuard guard(lock_);
    const auto      &cat_col = get_column<T>(cat_col_name, false);
    const auto      col_s = cat_col.size();
    map_t           val_map;
    size_type       ret_cnt = 0;

    val_map.reserve(col_s / 2);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        const auto  val = cat_col[i];
        auto        in_ret = val_map.emplace(std::make_pair(val, nullptr));

        if (in_ret.second)  {
            ColNameType new_name;

            if (numeric_cols_prefix)
                new_name = numeric_cols_prefix;
            new_name += _to_string_(val).c_str();

            auto    *new_col = &(create_column<IT>(new_name.c_str(), false));

            new_col->resize(col_s, IT(0));
            in_ret.first->second = new_col;
            ret_cnt += col_s;
        }
        in_ret.first->second->at(i) = IT(1);
    }
    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename CT>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
from_indicators(const StlVecType<const char *> &ind_col_names,
                const char *cat_col_name,
                const char *numeric_cols_prefix)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call from_indicators()");

    const size_type                     ind_col_s = ind_col_names.size();
    StlVecType<const StlVecType<T> *>   ind_cols(ind_col_s, nullptr);
    SpinGuard                           guard (lock_);

    for (size_type i = 0; i < ind_col_s; ++i) [[likely]]
        ind_cols[i] = &(get_column<T>(ind_col_names[i], false));

    const size_type col_s = ind_cols[0]->size();
    auto            &new_col = create_column<CT>(cat_col_name, false);
    const size_type pre_offset =
        numeric_cols_prefix == nullptr ? 0 : strlen(numeric_cols_prefix);

    guard.release();
    new_col.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        for (size_type j = 0; j < ind_col_s; ++j) [[likely]]  {
            if (ind_cols[j]->at(i))  {
                new_col.push_back(
                    _string_to_<CT>(ind_col_names[j] + pre_offset));
                break;
            }
        }
    }

    return (col_s);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_column (const char *name,
             StlVecType<T> &&column,
             nan_policy padding,
             bool do_lock)  {

    const size_type idx_s = indices_.size();
    const size_type data_s = column.size();

    if (data_s > idx_s) [[unlikely]]  {
        char buffer [512];

        snprintf (buffer, sizeof(buffer) - 1,
                  "DataFrame::load_column(): ERROR: "
#ifdef _MSC_VER
                  "data size of %zu is larger than index size of %zu",
#else
                  "data size of %lu is larger than index size of %lu",
#endif // _MSC_VER
                 data_s, idx_s);
        throw InconsistentData (buffer);
    }

    return (load_column_<T>(name,
                            std::forward<StlVecType<T>>(column),
                            padding,
                            do_lock));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_align_column(
    const char *name,
    StlVecType<T> &&column,
    size_type interval,
    bool start_from_beginning,
    const T &null_value,
    std::function<std::size_t(
                        const DataFrame::IndexType &,
                        const DataFrame::IndexType &)> diff_func)  {

    const size_type idx_s = indices_.size();
    const size_type data_s = column.size();

    using value_t = typename StlVecType<T>::value_type;

    StlVecType<value_t> new_col(idx_s, null_value);
    size_type           idx_idx { 0 };

    if (start_from_beginning)  {
        new_col[0] = std::move(column[0]);
        idx_idx = 1;
    }

    size_type   idx_ref_idx { 0 };
    size_type   data_idx { idx_idx };

    for ( ; data_idx < data_s && idx_idx < idx_s; ++idx_idx)  {
        const size_type idx_diff =
            diff_func(indices_[idx_ref_idx], indices_[idx_idx]);

        if (idx_diff < interval)  continue;
        new_col[idx_idx + (idx_diff > interval ? -1 : 0)] =
            std::move(column[data_idx]);
        idx_ref_idx = idx_idx + (idx_diff > interval ? -1 : 0);
        data_idx += 1;
    }

    return (load_column<value_t>(name, std::move(new_col)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_column (const char *name,
             const StlVecType<T> &data,
             nan_policy padding,
             bool do_lock)  {

    using value_t = typename StlVecType<T>::value_type;

    return(load_column<value_t>(name,
                                { data.begin(), data.end() },
                                padding,
                                do_lock));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename NT, typename ET>
DataFrame<I, H>::size_type DataFrame<I, H>::
load_column(const char *new_col_name,
            const char *existing_col_name,
            std::function<NT(const IndexType &, const ET &)> &&func,
            nan_policy padding,
            bool do_lock)  {

    const auto          &e_col = get_column<ET>(existing_col_name);
    ColumnVecType<NT>   n_col;

    n_col.reserve(padding == nan_policy::pad_with_nans
                      ? indices_.size() : e_col.size());
    for (const auto &[idx, val] : std::ranges::views::zip(indices_, e_col))
        n_col.push_back(func(idx, val));
    return (load_column<NT>(new_col_name, std::move(n_col), padding, do_lock));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ITR>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
append_column (const char *name,
               Index2D<const ITR &> range,
               nan_policy padding)  {

    using value_t = decltype(ITR::begin);

    ColumnVecType<value_t> &vec = get_column<value_t>(name);
    size_type              s =
        std::distance(range.begin, range.end) + vec.size();
    const size_type        idx_s = indices_.size();

    if (s > idx_s) [[unlikely]]  {
        char buffer [512];

        snprintf(buffer, sizeof(buffer) - 1,
                 "DataFrame::append_column(): ERROR: "
#ifdef _MSC_VER
                 "data size of %zu is larger than index size of %zu",
#else
                 "data size of %lu is larger than index size of %lu",
#endif // _MSC_VER
                 s, idx_s);
        throw InconsistentData (buffer);
    }

    vec.insert (vec.end (), range.begin, range.end);

    size_type   ret_cnt = s;

    s = vec.size();
    if (padding == nan_policy::pad_with_nans && s < idx_s)  {
        for (size_type i = 0; i < idx_s - s; ++i)  {
            vec.push_back (std::move(get_nan<value_t>()));
            ret_cnt += 1;
        }
    }

    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
append_column (const char *name, const T &val, nan_policy padding)  {

    ColumnVecType<T>    &vec = get_column<T>(name);
    size_type           s = 1;
    const size_type     idx_s = indices_.size();

    if (s > idx_s) [[unlikely]]  {
        char buffer [512];

        snprintf(buffer, sizeof(buffer) - 1,
                 "DataFrame::append_column(): ERROR: "
#ifdef _MSC_VER
                 "data size of %zu is larger than index size of %zu",
#else
                 "data size of %lu is larger than index size of %lu",
#endif // _MSC_VER
                 s, idx_s);
        throw InconsistentData (buffer);
    }

    vec.push_back (val);

    size_type   ret_cnt = s;

    s = vec.size();
    if (padding == nan_policy::pad_with_nans && s < idx_s)  {
        for (size_type i = 0; i < idx_s - s; ++i)  {
            vec.push_back (std::move(get_nan<T>()));
            ret_cnt += 1;
        }
    }

    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::append_row (IndexType *idx_val, Ts&& ... args)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call append_row()");

    if (idx_val)
        indices_.push_back(*idx_val);

    size_type   cnt = 1;
    auto        args_tuple = std::tuple<Ts ...>(std::forward<Ts>(args) ...);
    auto        fc = [this, &cnt](auto &pa) mutable -> void {
                         cnt += this->append_row_(pa);
                     };

    for_each_in_tuple (args_tuple, fc);

    return (cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::
truncate(IndexType &&before, IndexType &&after)  {

    const auto  begin = indices_.begin();
    const auto  end = indices_.end();
    const auto  l_citer = std::lower_bound(begin, end, before);
    const auto  u_citer = std::upper_bound(begin, end, after);
    size_type   l_index { 0 };
    size_type   u_index { indices_.size() };

    if (l_citer > begin)
        l_index = std::distance(begin, l_citer);
    if (u_citer != end)
        u_index = std::distance(begin, u_citer);

    {
        const auto      thread_level =
            (indices_.size() < ThreadPool::MUL_THR_THHOLD)
                ? 0L : get_thread_level();
        auto            lbd =
            [this, l_index, u_index] (auto begin, auto end) -> void  {
                truncate_functor_<Ts ...>   functor (l_index, u_index);

                for (auto citer = begin; citer < end; ++citer)
                    this->data_[citer->second].change(functor);
            };
        const SpinGuard guard(lock_);

        if (thread_level > 2)  {
            auto    futures = thr_pool_.parallel_loop(column_list_.begin(),
                                                      column_list_.end(),
                                                      std::move(lbd));

            for (auto &fut : futures)  fut.get();
        }
        else
            lbd(column_list_.begin(), column_list_.end());
    }

    indices_.erase(begin + u_index, end);
    indices_.erase(begin, begin + l_index);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename OLD_T1, typename OLD_T2, typename NEW_T, typename F>
void DataFrame<I, H>::
consolidate(const char *old_col_name1,
            const char *old_col_name2,
            const char *new_col_name,
            F &functor,
            bool delete_old_cols)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call consolidate()");

    SpinGuard                   guard (lock_);
    const ColumnVecType<OLD_T1> &vec1 =
        get_column<OLD_T1>(old_col_name1, false);
    const ColumnVecType<OLD_T2> &vec2 =
        get_column<OLD_T2>(old_col_name2, false);

    load_column<NEW_T>(new_col_name,
                       std::move(functor(indices_.begin(), indices_.end(),
                                         vec1.begin(), vec1.end(),
                                         vec2.begin(), vec2.end())),
                       nan_policy::dont_pad_with_nans,
                       false);
    guard.release();
    if (delete_old_cols)  {
        remove_column<OLD_T1>(old_col_name1);
        remove_column<OLD_T2>(old_col_name2);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename OLD_T1, typename OLD_T2, typename OLD_T3,
         typename NEW_T, typename F>
void DataFrame<I, H>::
consolidate(const char *old_col_name1,
            const char *old_col_name2,
            const char *old_col_name3,
            const char *new_col_name,
            F &functor,
            bool delete_old_cols)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call consolidate()");

    SpinGuard                   guard (lock_);
    const ColumnVecType<OLD_T1> &vec1 =
        get_column<OLD_T1>(old_col_name1, false);
    const ColumnVecType<OLD_T2> &vec2 =
        get_column<OLD_T2>(old_col_name2, false);
    const ColumnVecType<OLD_T3> &vec3 =
        get_column<OLD_T3>(old_col_name3, false);

    load_column<NEW_T>(new_col_name,
                       std::move(functor(indices_.begin(), indices_.end(),
                                         vec1.begin(), vec1.end(),
                                         vec2.begin(), vec2.end(),
                                         vec3.begin(), vec3.end())),
                       nan_policy::dont_pad_with_nans,
                       false);
    guard.release();
    if (delete_old_cols)  {
        remove_column<OLD_T1>(old_col_name1);
        remove_column<OLD_T2>(old_col_name2);
        remove_column<OLD_T3>(old_col_name3);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename OLD_T1, typename OLD_T2, typename OLD_T3, typename OLD_T4,
         typename NEW_T, typename F>
void DataFrame<I, H>::
consolidate(const char *old_col_name1,
            const char *old_col_name2,
            const char *old_col_name3,
            const char *old_col_name4,
            const char *new_col_name,
            F &functor,
            bool delete_old_cols)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call consolidate()");

    SpinGuard                   guard (lock_);
    const ColumnVecType<OLD_T1> &vec1 =
        get_column<OLD_T1>(old_col_name1, false);
    const ColumnVecType<OLD_T2> &vec2 =
        get_column<OLD_T2>(old_col_name2, false);
    const ColumnVecType<OLD_T3> &vec3 =
        get_column<OLD_T3>(old_col_name3, false);
    const ColumnVecType<OLD_T4> &vec4 =
        get_column<OLD_T4>(old_col_name4, false);

    load_column<NEW_T>(new_col_name,
                       std::move(functor(indices_.begin(), indices_.end(),
                                         vec1.begin(), vec1.end(),
                                         vec2.begin(), vec2.end(),
                                         vec3.begin(), vec3.end(),
                                         vec4.begin(), vec4.end())),
                       nan_policy::dont_pad_with_nans,
                       false);
    guard.release();
    if (delete_old_cols)  {
        remove_column<OLD_T1>(old_col_name1);
        remove_column<OLD_T2>(old_col_name2);
        remove_column<OLD_T3>(old_col_name3);
        remove_column<OLD_T4>(old_col_name4);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename OLD_T1, typename OLD_T2, typename OLD_T3,
         typename OLD_T4, typename OLD_T5,
         typename NEW_T, typename F>
void DataFrame<I, H>::
consolidate(const char *old_col_name1,
            const char *old_col_name2,
            const char *old_col_name3,
            const char *old_col_name4,
            const char *old_col_name5,
            const char *new_col_name,
            F &functor,
            bool delete_old_cols)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call consolidate()");

    SpinGuard                   guard (lock_);
    const ColumnVecType<OLD_T1> &vec1 =
        get_column<OLD_T1>(old_col_name1, false);
    const ColumnVecType<OLD_T2> &vec2 =
        get_column<OLD_T2>(old_col_name2, false);
    const ColumnVecType<OLD_T3> &vec3 =
        get_column<OLD_T3>(old_col_name3, false);
    const ColumnVecType<OLD_T4> &vec4 =
        get_column<OLD_T4>(old_col_name4, false);
    const ColumnVecType<OLD_T5> &vec5 =
        get_column<OLD_T5>(old_col_name5, false);

    load_column<NEW_T>(new_col_name,
                       std::move(functor(indices_.begin(), indices_.end(),
                                         vec1.begin(), vec1.end(),
                                         vec2.begin(), vec2.end(),
                                         vec3.begin(), vec3.end(),
                                         vec4.begin(), vec4.end(),
                                         vec5.begin(), vec5.end())),
                       nan_policy::dont_pad_with_nans,
                       false);
    guard.release();
    if (delete_old_cols)  {
        remove_column<OLD_T1>(old_col_name1);
        remove_column<OLD_T2>(old_col_name2);
        remove_column<OLD_T3>(old_col_name3);
        remove_column<OLD_T4>(old_col_name4);
        remove_column<OLD_T5>(old_col_name5);
    }
    return;
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
