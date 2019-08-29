// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>

#include <cstring>

// ----------------------------------------------------------------------------

namespace hmdf
{

#if defined(WIN32) || defined(_WIN32)
#  ifdef min
#    undef min
#  endif // min
#  ifdef max
#    undef max
#  endif // max
#endif // WIN32 || _WIN32

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
std::vector<T> &DataFrame<I, H>::create_column (const char *name)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call create_column()");

    if (! ::strcmp(name, "INDEX"))
        throw DataFrameError ("DataFrame::create_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

    data_.emplace_back (DataVec());
    column_tb_.emplace (name, data_.size() - 1);

    DataVec         &hv = data_.back();
    std::vector<T>  &vec = hv.template get_vector<T>();

    // vec.resize(indices_.size(), _get_nan<T>());
    return (vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
void DataFrame<I, H>::remove_column (const char *name)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call remove_column()");

    if (! ::strcmp(name, "INDEX"))
        throw DataFrameError ("DataFrame::remove_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

    const auto  iter = column_tb_.find (name);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::remove_column(): ERROR: Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    // I do not erase the column from the data_ vector, because it will mess up
    // indices in the hash table column_tb_
    /* data_.erase (data_.begin() + iter->second); */
    column_tb_.erase (iter);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
void DataFrame<I, H>::rename_column (const char *from, const char *to)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call rename_column()");

    if (! ::strcmp(from, "INDEX") || ! ::strcmp(to, "INDEX"))
        throw DataFrameError ("DataFrame::rename_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

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

template<typename I, typename  H>
template<typename ... Ts>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::load_data (IndexVecType &&indices, Ts&& ... args)  {

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

template<typename I, typename  H>
template<typename ITR>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::load_index(const ITR &begin, const ITR &end)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call load_index()");

    const size_type s = std::distance(begin, end);

    indices_.clear ();
    indices_.reserve (s);
    indices_.insert (indices_.begin (), begin, end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::load_index(IndexVecType &&idx)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call load_index()");

    indices_ = idx;
    return (indices_.size());
}

// ----------------------------------------------------------------------------

template<typename T>
inline static void
_generate_ts_index_(std::vector<T> &index_vec,
                    DateTime &start_di,
                    time_frequency t_freq,
                    long increment)  {

    switch(t_freq)  {
    case time_frequency::annual:
        index_vec.push_back(static_cast<T>(start_di.date()));
        start_di.add_years(increment);
        break;
    case time_frequency::monthly:
        index_vec.push_back(static_cast<T>(start_di.date()));
        start_di.add_months(increment);
        break;
    case time_frequency::weekly:
        index_vec.push_back(static_cast<T>(start_di.date()));
        start_di.add_days(increment * 7);
        break;
    case time_frequency::daily:
        index_vec.push_back(static_cast<T>(start_di.date()));
        start_di.add_days(increment);
        break;
    case time_frequency::hourly:
        index_vec.push_back(static_cast<T>(start_di.time()));
        start_di.add_seconds(increment * 60 * 60);
        break;
    case time_frequency::minutely:
        index_vec.push_back(static_cast<T>(start_di.time()));
        start_di.add_seconds(increment * 60);
        break;
    case time_frequency::secondly:
        index_vec.push_back(static_cast<T>(start_di.time()));
        start_di.add_seconds(increment);
        break;
    case time_frequency::millisecondly:
        index_vec.push_back(static_cast<T>(start_di.long_time()));
        start_di.add_nanoseconds(increment * 1000000);
        break;
    default:
        break;
    }
}

// ------------------------------------

template<>
inline void
_generate_ts_index_<DateTime>(std::vector<DateTime> &index_vec,
                              DateTime &start_di,
                              time_frequency t_freq,
                              long increment)  {

    index_vec.push_back(start_di);
    switch(t_freq)  {
    case time_frequency::annual:
        start_di.add_years(increment);
        break;
    case time_frequency::monthly:
        start_di.add_months(increment);
        break;
    case time_frequency::weekly:
        start_di.add_days(increment * 7);
        break;
    case time_frequency::daily:
        start_di.add_days(increment);
        break;
    case time_frequency::hourly:
        start_di.add_seconds(increment * 60 * 60);
        break;
    case time_frequency::minutely:
        start_di.add_seconds(increment * 60);
        break;
    case time_frequency::secondly:
        start_di.add_seconds(increment);
        break;
    case time_frequency::millisecondly:
        start_di.add_nanoseconds(increment * 1000000);
        break;
    default:
        break;
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
std::vector<I> DataFrame<I, H>::
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

template<typename I, typename  H>
std::vector<I> DataFrame<I, H>::
gen_sequence_index (const IndexType &start_value,
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

template<typename I, typename  H>
template<typename ITR>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::append_index(Index2D<const ITR &> range)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call append_index()");

    const size_type s = std::distance(range.begin, range.end);

    indices_.reserve (indices_.size() + s);
    indices_.insert (indices_.end (), range.begin, range.end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::append_index(const IndexType &val)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call append_index()");

    indices_.push_back (val);
    return (1);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename ITR>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_column (const char *name,
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

    const auto      iter = column_tb_.find (name);
    std::vector<T>  *vec_ptr = nullptr;

    if (iter == column_tb_.end())
        vec_ptr = &(create_column<T>(name));
    else  {
        DataVec &hv = data_[iter->second];

        vec_ptr = &(hv.template get_vector<T>());
    }

    vec_ptr->clear();
    vec_ptr->reserve(idx_s);
    vec_ptr->insert (vec_ptr->begin (), range.begin, range.end);

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

template<typename I, typename  H>
template<typename T, typename ITR>
void DataFrame<I, H>::
setup_view_column_ (const char *name, Index2D<ITR> range)  {

    static_assert(std::is_base_of<HeteroView, DataVec>::value ||
                      std::is_base_of<HeteroPtrView, DataVec>::value,
                  "Only a DataFrameView or DataFramePtrView can "
                  "call setup_view_column_()");

    data_.emplace_back (DataVec(&*(range.begin), &*(range.end)));
    column_tb_.emplace (name, data_.size() - 1);

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_column (const char *name, std::vector<T> &&data, nan_policy padding)  {

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

    const auto      iter = column_tb_.find (name);
    std::vector<T>  *vec_ptr = nullptr;

    if (iter == column_tb_.end())
        vec_ptr = &(create_column<T>(name));
    else  {
        DataVec &hv = data_[iter->second];

        vec_ptr = &(hv.template get_vector<T>());
    }

    *vec_ptr = std::move(data);
    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
load_column (const char *name,
             const std::vector<T> &data,
             nan_policy padding)  {

    return (load_column<T>(name, { data.begin(), data.end() }, padding));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::_load_pair(std::pair<T1, T2> &col_name_data)  {

    return (load_column<typename decltype(col_name_data.second)::value_type>(
                col_name_data.first, // column name
                std::move(col_name_data.second),
                nan_policy::pad_with_nans));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename ITR>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
append_column (const char *name,
               Index2D<const ITR &> range,
               nan_policy padding)  {

    const auto  iter = column_tb_.find (name);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
                         "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    DataVec         &hv = data_[iter->second];
    std::vector<T>  &vec = hv.template get_vector<T>();

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

    vec.reserve (idx_s);
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

template<typename I, typename  H>
template<typename T>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::
append_column (const char *name, const T &val, nan_policy padding)  {

    const auto  iter = column_tb_.find (name);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
                         "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    DataVec         &hv = data_[iter->second];
    std::vector<T>  &vec = hv.template get_vector<T>();

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

template<typename I, typename  H>
template<typename ... Ts>
void DataFrame<I, H>::remove_data_by_idx (Index2D<IndexType> range)  {

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

template<typename I, typename  H>
template<typename ... Ts>
void DataFrame<I, H>::remove_data_by_loc (Index2D<long> range)  {

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

template<typename I, typename  H>
template<typename T, typename F, typename ... Ts>
void DataFrame<I, H>::remove_data_by_sel (const char *name, F &sel_functor)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call remove_data_by_loc()");

    const auto  citer = column_tb_.find (name);

    if (citer == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::remove_data_by_sel(1): ERROR: "
                 "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv = data_[citer->second];
    const std::vector<T>    &vec = hv.template get_vector<T>();
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
        indices_.erase(indices_.begin() + col_indices[i] - del_count++);

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename F, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_sel (const char *name1, const char *name2, F &sel_functor)  {

    const auto  citer1 = column_tb_.find (name1);

    if (citer1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::remove_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }

    const auto  citer2 = column_tb_.find (name2);

    if (citer2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::remove_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[citer1->second];
    const DataVec           &hv2 = data_[citer2->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
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
        indices_.erase(indices_.begin() + col_indices[i] - del_count++);

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename F, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_sel (const char *name1,
                    const char *name2,
                    const char *name3,
                    F &sel_functor)  {

    const auto  citer1 = column_tb_.find (name1);

    if (citer1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }

    const auto  citer2 = column_tb_.find (name2);

    if (citer2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }

    const auto  citer3 = column_tb_.find (name3);

    if (citer3 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[citer1->second];
    const DataVec           &hv2 = data_[citer2->second];
    const DataVec           &hv3 = data_[citer3->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const std::vector<T3>   &vec3 = hv3.template get_vector<T3>();
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
        indices_.erase(indices_.begin() + col_indices[i] - del_count++);

    return;
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
