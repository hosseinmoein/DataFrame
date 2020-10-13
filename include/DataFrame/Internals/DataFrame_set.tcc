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

#include <cstring>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H, typename CLT>
template<typename T>
std::vector<T> &DataFrame<I, H, CLT>::create_column (const char *label)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call create_column()");

    if (! ::strcmp(label, DF_INDEX_COL_NAME))
        throw DataFrameError ("DataFrame::create_column(): ERROR: "
                              "Data column label cannot be 'INDEX'");

    data_.emplace_back (DataVec());
    column_tb_.emplace (label, data_.size() - 1);

    DataVec         &hv = data_.back();
    const SpinGuard guard(lock_);

    return (hv.template get_vector<T>());
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
void DataFrame<I, H, CLT>::remove_column (const char *label)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call remove_column()");

    if (! ::strcmp(label, DF_INDEX_COL_NAME))
        throw DataFrameError ("DataFrame::remove_column(): ERROR: "
                              "Data column label cannot be 'INDEX'");

    const auto  iter = column_tb_.find (label);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::remove_column(): ERROR: Cannot find column '%s'",
                 label);
        throw ColNotFound (buffer);
    }

    // I do not erase the column from the data_ vector, because it will mess up
    // indices in the hash table column_tb_
    /* data_.erase (data_.begin() + iter->second); */
    column_tb_.erase (iter);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
void DataFrame<I, H, CLT>::rename_column (const char *from, const char *to)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call rename_column()");

    if (! ::strcmp(from, DF_INDEX_COL_NAME) ||
        ! ::strcmp(to, DF_INDEX_COL_NAME))
        throw DataFrameError ("DataFrame::rename_column(): ERROR: "
                              "Data column label cannot be 'INDEX'");

    const auto  iter = column_tb_.find (from);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::rename_column(): ERROR: Cannot find column '%s'",
                 from);
        throw ColNotFound (buffer);
    }

    column_tb_.emplace (to, iter->second);
    column_tb_.erase (iter);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename FROM_T, typename TO_T>
void DataFrame<I, H, CLT>::
retype_column (const char *label,
               std::function<TO_T (const FROM_T &)> convert_func)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call retype_column()");

    if (! ::strcmp(label, DF_INDEX_COL_NAME))
        throw DataFrameError ("DataFrame::retype_column(): ERROR: "
                              "Data column label cannot be 'INDEX'");

    const ColumnVecType<FROM_T> &old_vec = get_column<FROM_T>(label);
    std::vector<TO_T>           new_vec;

    new_vec.reserve(old_vec.size());
    for (const auto &citer : old_vec)
        new_vec.push_back(std::move(convert_func(citer)));
    remove_column(label);
    load_column<TO_T>(label, std::move(new_vec));
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename ... Ts>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::load_data (IndexVecType &&indices, Ts&& ... args)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call load_data()");

    size_type       cnt = load_index(std::move(indices));
    auto            args_tuple = std::tuple<Ts ...>(args ...);
    // const size_type tuple_size =
    //     std::tuple_size<decltype(args_tuple)>::value;
    auto            fc =
        [this, &cnt](auto &pa) mutable -> void {
            cnt += this->_load_pair(pa);
        };

    for_each_in_tuple_ (args_tuple, fc);

    return (cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename ITR>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::load_index(const ITR &begin, const ITR &end)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call load_index()");

    indices_.clear();
    indices_.insert (indices_.end(), begin, end);
    return (indices_.size());
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::load_index(IndexVecType &&idx)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call load_index()");

    indices_ = idx;
    return (indices_.size());
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
std::vector<I> DataFrame<I, H, CLT>::
gen_datetime_index(const char *start_datetime,
                   const char *end_datetime,
                   time_frequency t_freq,
                   long increment,
                   DT_TIME_ZONE tz)  {

    DateTime                start_di(start_datetime,
                                     DT_DATE_STYLE::AME_STYLE, tz);
    const DateTime          end_di(end_datetime, DT_DATE_STYLE::AME_STYLE, tz);
    const double            diff = end_di.diff_seconds(start_di);
    std::vector<IndexType>  index_vec;

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

    while (start_di < end_di)
        _generate_ts_index_<IndexType>(index_vec, start_di, t_freq, increment);

    return (index_vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
std::vector<I> DataFrame<I, H, CLT>::
gen_sequence_index(const IndexType &start_value,
                   const IndexType &end_value,
                   long increment)  {

    std::vector<IndexType>  index_vec;
    IndexType               sv = start_value;

    while (sv < end_value)  {
        index_vec.push_back(sv);
        sv = sv + increment;
    }
    return (index_vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename ITR>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::append_index(Index2D<const ITR &> range)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call append_index()");

    const size_type s = std::distance(range.begin, range.end);

    indices_.insert(indices_.end(), range.begin, range.end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::append_index(const IndexType &val)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call append_index()");

    indices_.push_back (val);
    return (1);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T, typename ITR>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::
load_column (const char *label,
             Index2D<const ITR &>
             range, nan_policy padding)  {

    size_type       s = std::distance(range.begin, range.end);
    const size_type idx_s = indices_.size();

    if (s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::load_column(): ERROR: "
#ifdef _WIN32
                         "data size of %zu is larger than index size of %zu",
#else
                         "data size of %lu is larger than index size of %lu",
#endif // _WIN32
                 s, idx_s);
        throw InconsistentData (buffer);
    }

    const auto      iter = column_tb_.find (label);
    std::vector<T>  *vec_ptr = nullptr;

    if (iter == column_tb_.end())
        vec_ptr = &(create_column<T>(label));
    else  {
        DataVec         &hv = data_[iter->second];
        const SpinGuard guard(lock_);

        vec_ptr = &(hv.template get_vector<T>());
    }

    vec_ptr->clear();
    vec_ptr->insert (vec_ptr->end(), range.begin, range.end);

    size_type   ret_cnt = s;

    s = vec_ptr->size();
    if (padding == nan_policy::pad_with_nans && s < idx_s)  {
        for (size_type i = 0; i < idx_s - s; ++i)  {
            vec_ptr->push_back (std::move(_get_nan<T>()));
            ret_cnt += 1;
        }
    }

    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T, typename ITR>
void DataFrame<I, H, CLT>::
setup_view_column_ (const char *label, Index2D<ITR> range)  {

    static_assert(std::is_base_of<HeteroView, DataVec>::value ||
                      std::is_base_of<HeteroPtrView, DataVec>::value,
                  "Only a DataFrameView or DataFramePtrView can "
                  "call setup_view_column_()");

    DataVec dv;

    dv.set_begin_end_special(&*(range.begin), &*(range.end - 1));
    data_.emplace_back (dv);
    column_tb_.emplace (label, data_.size() - 1);

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::
load_column (const char *label, std::vector<T> &&data, nan_policy padding)  {

    const size_type idx_s = indices_.size();
    const size_type data_s = data.size();

    if (data_s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::load_column(): ERROR: "
#ifdef _WIN32
                         "data size of %zu is larger than index size of %zu",
#else
                         "data size of %lu is larger than index size of %lu",
#endif // _WIN32
                 data_s, idx_s);
        throw InconsistentData (buffer);
    }

    size_type   ret_cnt = data_s;

    if (padding == nan_policy::pad_with_nans && data_s < idx_s)  {
        for (size_type i = 0; i < idx_s - data_s; ++i)  {
            data.push_back (std::move(_get_nan<T>()));
            ret_cnt += 1;
        }
    }

    const auto      iter = column_tb_.find (label);
    std::vector<T>  *vec_ptr = nullptr;

    if (iter == column_tb_.end())
        vec_ptr = &(create_column<T>(label));
    else  {
        DataVec         &hv = data_[iter->second];
        const SpinGuard guard(lock_);

        vec_ptr = &(hv.template get_vector<T>());
    }

    *vec_ptr = std::move(data);
    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::
load_align_column(
    const char *label,
    std::vector<T> &&data,
    size_type interval,
    bool start_from_beginning,
    const T &null_value,
    std::function<DataFrame::size_type(
                        const DataFrame::IndexType &,
                        const DataFrame::IndexType &)> diff_func)  {

    const size_type idx_s = indices_.size();
    const size_type data_s = data.size();

    if (data_s > idx_s || data_s == 0)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::load_align_column(): ERROR: "
#ifdef _WIN32
                         "data size of %zu is larger than index size of %zu",
#else
                         "data size of %lu is larger than index size of %lu",
#endif // _WIN32
                 data_s, idx_s);
        throw InconsistentData (buffer);
    }

    std::vector<T>  new_col(idx_s, null_value);
    size_type       idx_idx { 0 };

    if (start_from_beginning)  {
        new_col[0] = std::move(data[0]);
        idx_idx = 1;
    }

    size_type   idx_ref_idx { 0 };
    size_type   data_idx { idx_idx };

    for ( ; data_idx < data_s && idx_idx < idx_s; ++idx_idx)  {
        const size_type idx_diff =
            diff_func(indices_[idx_ref_idx], indices_[idx_idx]);

        if (idx_diff < interval)  continue;
        new_col[idx_idx + (idx_diff > interval ? -1 : 0)] =
            std::move(data[data_idx]);
        idx_ref_idx = idx_idx + (idx_diff > interval ? -1 : 0);
        data_idx += 1;
    }

    return (load_column<T>(label, std::move(new_col)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::
load_column (const char *label,
             const std::vector<T> &data,
             nan_policy padding)  {

    return (load_column<T>(label, { data.begin(), data.end() }, padding));
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T1, typename T2>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::_load_pair(std::pair<T1, T2> &col_label_data)  {

    return (load_column<typename decltype(col_label_data.second)::value_type>(
                col_label_data.first, // column label
                std::move(col_label_data.second),
                nan_policy::pad_with_nans));
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T, typename ITR>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::
append_column (const char *label,
               Index2D<const ITR &> range,
               nan_policy padding)  {

    std::vector<T>  &vec = get_column<T>(label);
    size_type       s = std::distance(range.begin, range.end) + vec.size ();
    const size_type idx_s = indices_.size();

    if (s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
#ifdef _WIN32
                         "data size of %zu is larger than index size of %zu",
#else
                         "data size of %lu is larger than index size of %lu",
#endif // _WIN32
                 s, idx_s);
        throw InconsistentData (buffer);
    }

    vec.insert (vec.end (), range.begin, range.end);

    size_type   ret_cnt = s;

    s = vec.size();
    if (padding == nan_policy::pad_with_nans && s < idx_s)  {
        for (size_type i = 0; i < idx_s - s; ++i)  {
            vec.push_back (std::move(_get_nan<T>()));
            ret_cnt += 1;
        }
    }

    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T>
typename DataFrame<I, H, CLT>::size_type
DataFrame<I, H, CLT>::
append_column (const char *label, const T &val, nan_policy padding)  {

    std::vector<T>  &vec = get_column<T>(label);
    size_type       s = 1;
    const size_type idx_s = indices_.size();

    if (s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
#ifdef _WIN32
                         "data size of %zu is larger than index size of %zu",
#else
                         "data size of %lu is larger than index size of %lu",
#endif // _WIN32
                 s, idx_s);
        throw InconsistentData (buffer);
    }

    vec.reserve (idx_s);
    vec.push_back (val);

    size_type   ret_cnt = s;

    s = vec.size();
    if (padding == nan_policy::pad_with_nans && s < idx_s)  {
        for (size_type i = 0; i < idx_s - s; ++i)  {
            vec.push_back (std::move(_get_nan<T>()));
            ret_cnt += 1;
        }
    }

    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename ... Ts>
void DataFrame<I, H, CLT>::remove_data_by_idx (Index2D<IndexType> range)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call remove_data_by_idx()");

    const auto  &lower =
        std::lower_bound (indices_.begin(), indices_.end(), range.begin);
    const auto  &upper =
        std::upper_bound (indices_.begin(), indices_.end(), range.end);

    if (lower != indices_.end())  {
        const size_type b_dist = std::distance(indices_.begin(), lower);
        const size_type e_dist = std::distance(indices_.begin(),
                                               upper < indices_.end()
                                                   ? upper
                                                   : indices_.end());
        make_consistent<Ts ...>();
        indices_.erase(lower, upper);

        remove_functor_<Ts ...> functor (b_dist, e_dist);

        for (auto &iter : column_tb_)
            data_[iter.second].change(functor);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename ... Ts>
void DataFrame<I, H, CLT>::remove_data_by_loc (Index2D<long> range)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call remove_data_by_loc()");

    if (range.begin < 0)
        range.begin = static_cast<long>(indices_.size()) + range.begin;
    if (range.end < 0)
        range.end = static_cast<long>(indices_.size()) + range.end;

    if (range.end <= static_cast<long>(indices_.size()) &&
        range.begin <= range.end && range.begin >= 0)  {
        make_consistent<Ts ...>();
        indices_.erase(indices_.begin() + range.begin,
                       indices_.begin() + range.end);

        remove_functor_<Ts ...> functor (
            static_cast<size_type>(range.begin),
            static_cast<size_type>(range.end));

        for (auto &iter : column_tb_)
            data_[iter.second].change(functor);

        return;
    }

    char buffer [512];

    sprintf (buffer,
             "DataFrame::remove_data_by_loc(): ERROR: "
             "Bad begin, end range: %ld, %ld",
             range.begin, range.end);
    throw BadRange (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T, typename F, typename ... Ts>
void DataFrame<I, H, CLT>::
remove_data_by_sel (const char *label, F &sel_functor)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call remove_data_by_loc()");

    const ColumnVecType<T>  &vec = get_column<T>(label);
    const size_type         idx_s = indices_.size();
    const size_type         col_s = vec.size();
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i], vec[i]))
            col_indices.push_back(i);

    for (auto col_citer : column_tb_)  {
        sel_remove_functor_<Ts ...> functor (col_indices);

        data_[col_citer.second].change(functor);
    }

    const size_type col_indices_s = col_indices.size();
    size_type       del_count = 0;

    for (size_type i = 0; i < col_indices_s; ++i)
        indices_.erase(indices_.begin() + (col_indices[i] - del_count++));

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T1, typename T2, typename F, typename ... Ts>
void DataFrame<I, H, CLT>::
remove_data_by_sel (const char *label1, const char *label2, F &sel_functor)  {

    const ColumnVecType<T1> &vec1 = get_column<T1>(label1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(label2);
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s = std::max(col_s1, col_s2);
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : _get_nan<T1>(),
                         i < col_s2 ? vec2[i] : _get_nan<T2>()))
            col_indices.push_back(i);

    for (auto col_citer : column_tb_)  {
        sel_remove_functor_<Ts ...> functor (col_indices);

        data_[col_citer.second].change(functor);
    }

    const size_type col_indices_s = col_indices.size();
    size_type       del_count = 0;

    for (size_type i = 0; i < col_indices_s; ++i)
        indices_.erase(indices_.begin() + (col_indices[i] - del_count++));

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T1, typename T2, typename T3, typename F, typename ... Ts>
void DataFrame<I, H, CLT>::
remove_data_by_sel (const char *label1,
                    const char *label2,
                    const char *label3,
                    F &sel_functor)  {

    const ColumnVecType<T1> &vec1 = get_column<T1>(label1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(label2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(label3);
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s3 = vec3.size();
    const size_type         col_s = std::max(std::max(col_s1, col_s2), col_s3);
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : _get_nan<T1>(),
                         i < col_s2 ? vec2[i] : _get_nan<T2>(),
                         i < col_s3 ? vec3[i] : _get_nan<T3>()))
            col_indices.push_back(i);

    for (auto col_citer : column_tb_)  {
        sel_remove_functor_<Ts ...> functor (col_indices);

        data_[col_citer.second].change(functor);
    }

    const size_type col_indices_s = col_indices.size();
    size_type       del_count = 0;

    for (size_type i = 0; i < col_indices_s; ++i)
        indices_.erase(indices_.begin() + (col_indices[i] - del_count++));

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename MAP, typename ... Ts>
StdDataFrame<I> DataFrame<I, H, CLT>::
remove_dups_common_(const DataFrame &s_df,
                    remove_dup_spec rds,
                    const MAP &row_table,
                    const IndexVecType &index)  {

    using count_vec = std::vector<size_type>;

    count_vec   rows_to_del;

    rows_to_del.reserve(8);
    for (const auto &citer : row_table)  {
        if (citer.second.size() > 1)  {
            if (rds == remove_dup_spec::keep_first)  {
                for (size_type i = 1; i < citer.second.size(); ++i)
                    rows_to_del.push_back(citer.second[i]);
            }
            else if (rds == remove_dup_spec::keep_last)  {
                for (size_type i = 0; i < citer.second.size() - 1; ++i)
                    rows_to_del.push_back(citer.second[i]);
            }
            else  {  // remove_dup_spec::keep_none
                for (size_type i = 0; i < citer.second.size(); ++i)
                    rows_to_del.push_back(citer.second[i]);
            }
        }
    }

    StdDataFrame<I> new_df;
    IndexVecType    new_index (index.size() - rows_to_del.size());

    _remove_copy_if_(index.begin(), index.end(), new_index.begin(),
                     [&rows_to_del] (std::size_t n) -> bool  {
                         return (std::find(rows_to_del.begin(),
                                           rows_to_del.end(),
                                           n) != rows_to_del.end());
                     });
    new_df.load_index(std::move(new_index));

    for (auto citer : s_df.column_tb_)  {
        copy_remove_functor_<Ts ...>    functor (citer.first.c_str(),
                                                 rows_to_del,
                                                 new_df);

        s_df.data_[citer.second].change(functor);
    }
    return (new_df);
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T1, typename T2, typename ... Ts>
DataFrame<I, H, CLT> DataFrame<I, H, CLT>::
remove_duplicates (const char *label1,
                   const char *label2,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &, const IndexType &>;
    using count_vec = std::vector<size_type>;
    using data_map = std::unordered_map<data_tuple, count_vec>;

    const ColumnVecType<T1> &vec1 = get_column<T1>(label1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(label2);
    const auto              &index = get_index();
    const size_type         col_s =
        std::min<size_type>({ vec1.size(), vec2.size(), index.size() });
    data_map                row_table;
    count_vec               dummy_vec;
    const IndexType         dummy_idx { };

    for (size_type i = 0; i < col_s; ++i)  {
        auto    insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<data_map, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T1, typename T2, typename T3, typename ... Ts>
DataFrame<I, H, CLT> DataFrame<I, H, CLT>::
remove_duplicates (const char *label1,
                   const char *label2,
                   const char *label3,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &, const T3 &,
                                  const IndexType &>;
    using count_vec = std::vector<size_type>;
    using data_map = std::unordered_map<data_tuple, count_vec>;

    const ColumnVecType<T1> &vec1 = get_column<T1>(label1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(label2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(label3);
    const auto              &index = get_index();
    const size_type         col_s =
        std::min<size_type>(
            { vec1.size(), vec2.size(), vec3.size(), index.size() });
    data_map                row_table;
    count_vec               dummy_vec;
    const IndexType         dummy_idx { };

    for (size_type i = 0; i < col_s; ++i)  {
        auto    insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i], vec3[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<data_map, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T1, typename T2, typename T3, typename T4, typename ... Ts>
DataFrame<I, H, CLT> DataFrame<I, H, CLT>::
remove_duplicates (const char *label1,
                   const char *label2,
                   const char *label3,
                   const char *label4,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &,
                                  const T3 &, const T4 &,
                                  const IndexType &>;
    using count_vec = std::vector<size_type>;
    using data_map = std::unordered_map<data_tuple, count_vec>;

    const ColumnVecType<T1> &vec1 = get_column<T1>(label1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(label2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(label3);
    const ColumnVecType<T4> &vec4 = get_column<T4>(label4);
    const auto              &index = get_index();
    const size_type         col_s =
        std::min<size_type>(
            { vec1.size(), vec2.size(), vec3.size(), vec4.size(),
              index.size() });
    data_map                row_table;
    count_vec               dummy_vec;
    const IndexType         dummy_idx { };

    for (size_type i = 0; i < col_s; ++i)  {
        auto    insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i], vec3[i], vec4[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<data_map, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename ... Ts>
DataFrame<I, H, CLT> DataFrame<I, H, CLT>::
remove_duplicates (const char *label1,
                   const char *label2,
                   const char *label3,
                   const char *label4,
                   const char *label5,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &,
                                  const T3 &, const T4 &, const T5 &,
                                  const IndexType &>;
    using count_vec = std::vector<size_type>;
    using data_map = std::unordered_map<data_tuple, count_vec>;

    const ColumnVecType<T1> &vec1 = get_column<T1>(label1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(label2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(label3);
    const ColumnVecType<T4> &vec4 = get_column<T4>(label4);
    const ColumnVecType<T5> &vec5 = get_column<T5>(label5);
    const auto              &index = get_index();
    const size_type         col_s =
        std::min<size_type>(
            { vec1.size(), vec2.size(), vec3.size(), vec4.size(), vec5.size(),
              index.size() });
    data_map                row_table;
    count_vec               dummy_vec;
    const IndexType         dummy_idx { };

    for (size_type i = 0; i < col_s; ++i)  {
        auto    insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i], vec3[i],
                                      vec4[i], vec5[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<data_map, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename T1, typename T2, typename T3, typename T4,
         typename T5, typename T6,
         typename ... Ts>
DataFrame<I, H, CLT> DataFrame<I, H, CLT>::
remove_duplicates (const char *label1,
                   const char *label2,
                   const char *label3,
                   const char *label4,
                   const char *label5,
                   const char *label6,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &,
                                  const T3 &, const T4 &,
                                  const T5 &, const T6 &,
                                  const IndexType &>;
    using count_vec = std::vector<size_type>;
    using data_map = std::unordered_map<data_tuple, count_vec>;

    const ColumnVecType<T1> &vec1 = get_column<T1>(label1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(label2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(label3);
    const ColumnVecType<T4> &vec4 = get_column<T4>(label4);
    const ColumnVecType<T5> &vec5 = get_column<T5>(label5);
    const ColumnVecType<T6> &vec6 = get_column<T6>(label6);
    const auto              &index = get_index();
    const size_type         col_s =
        std::min<size_type>(
            { vec1.size(), vec2.size(), vec3.size(), vec4.size(),
              vec5.size(), vec6.size(),
              index.size() });
    data_map                row_table;
    count_vec               dummy_vec;
    const IndexType         dummy_idx { };

    for (size_type i = 0; i < col_s; ++i)  {
        auto    insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i], vec3[i],
                                      vec4[i], vec5[i], vec6[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<data_map, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename OLD_T1, typename OLD_T2, typename NEW_T, typename F>
void DataFrame<I, H, CLT>::
consolidate(const char *old_col_label1,
            const char *old_col_label2,
            const char *new_col_label,
            F &functor,
            bool delete_old_cols)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call consolidate()");

    const ColumnVecType<OLD_T1> &vec1 = get_column<OLD_T1>(old_col_label1);
    const ColumnVecType<OLD_T2> &vec2 = get_column<OLD_T2>(old_col_label2);

    load_column<NEW_T>(new_col_label,
                       std::move(functor(indices_.begin(), indices_.end(),
                                         vec1.begin(), vec1.end(),
                                         vec2.begin(), vec2.end())),
                       nan_policy::dont_pad_with_nans);
    if (delete_old_cols)  {
        remove_column(old_col_label1);
        remove_column(old_col_label2);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H, typename CLT>
template<typename OLD_T1, typename OLD_T2, typename OLD_T3,
         typename NEW_T, typename F>
void DataFrame<I, H, CLT>::
consolidate(const char *old_col_label1,
            const char *old_col_label2,
            const char *old_col_label3,
            const char *new_col_label,
            F &functor,
            bool delete_old_cols)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call consolidate()");

    const ColumnVecType<OLD_T1> &vec1 = get_column<OLD_T1>(old_col_label1);
    const ColumnVecType<OLD_T2> &vec2 = get_column<OLD_T2>(old_col_label2);
    const ColumnVecType<OLD_T3> &vec3 = get_column<OLD_T3>(old_col_label3);

    load_column<NEW_T>(new_col_label,
                       std::move(functor(indices_.begin(), indices_.end(),
                                         vec1.begin(), vec1.end(),
                                         vec2.begin(), vec2.end(),
                                         vec3.begin(), vec3.end())),
                       nan_policy::dont_pad_with_nans);
    if (delete_old_cols)  {
        remove_column(old_col_label1);
        remove_column(old_col_label2);
        remove_column(old_col_label3);
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
