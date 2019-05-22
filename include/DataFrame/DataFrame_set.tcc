// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>
#include <DataFrame/DateTime.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename TS, typename  HETERO>
template<typename T>
std::vector<T> &DataFrame<TS, HETERO>::create_column (const char *name)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call create_column()");

    if (! ::strcmp(name, "INDEX"))
        throw DataFrameError ("DataFrame::create_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

    data_.emplace_back (DataVec());
    data_tb_.emplace (name, data_.size() - 1);

    DataVec         &hv = data_.back();
    std::vector<T>  &vec = hv.template get_vector<T>();

    // vec.resize(indices_.size(), _get_nan<T>());
    return (vec);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
void DataFrame<TS, HETERO>::remove_column (const char *name)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call remove_column()");

    if (! ::strcmp(name, "INDEX"))
        throw DataFrameError ("DataFrame::remove_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

    const auto  iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::remove_column(): ERROR: Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    // I do not erase the column from the data_ vector, because it will mess up
    // indices in the hash table data_tb_
    /* data_.erase (data_.begin() + iter->second); */
    data_tb_.erase (iter);
    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
void DataFrame<TS, HETERO>::rename_column (const char *from, const char *to)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call rename_column()");

    if (! ::strcmp(from, "INDEX") || ! ::strcmp(to, "INDEX"))
        throw DataFrameError ("DataFrame::rename_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

    const auto  iter = data_tb_.find (from);

    if (iter == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::rename_column(): ERROR: Cannot find column '%s'",
                 from);
        throw ColNotFound (buffer);
    }

    data_tb_.emplace (to, iter->second);
    data_tb_.erase (iter);
    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ... Ts>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::load_data (TSVec &&indices, Ts&& ... args)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
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

template<typename TS, typename  HETERO>
template<typename ITR>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::load_index(const ITR &begin, const ITR &end)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call load_index()");

    const size_type s = std::distance(begin, end);

    indices_.clear ();
    indices_.reserve (s);
    indices_.insert (indices_.begin (), begin, end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::load_index(TSVec &&idx)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
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
        start_di.add_years(increment);
        index_vec.push_back(static_cast<T>(start_di.date()));
        break;
    case time_frequency::monthly:
        start_di.add_months(increment);
        index_vec.push_back(static_cast<T>(start_di.date()));
        break;
    case time_frequency::weekly:
        start_di.add_days(increment * 7);
        index_vec.push_back(static_cast<T>(start_di.date()));
        break;
    case time_frequency::daily:
        start_di.add_days(increment);
        index_vec.push_back(static_cast<T>(start_di.date()));
        break;
    case time_frequency::hourly:
        start_di.add_seconds(increment * 60 * 60);
        index_vec.push_back(static_cast<T>(start_di.time()));
        break;
    case time_frequency::minutely:
        start_di.add_seconds(increment * 60);
        index_vec.push_back(static_cast<T>(start_di.time()));
        break;
    case time_frequency::secondly:
        start_di.add_seconds(increment);
        index_vec.push_back(static_cast<T>(start_di.time()));
        break;
    case time_frequency::millisecondly:
        start_di.add_nanoseconds(increment * 1000000);
        index_vec.push_back(static_cast<T>(start_di.long_time()));
        break;
    default:
        break;
    }
}

// ------------------------------------

template<>
inline void
_generate_ts_index_(std::vector<DateTime> &index_vec,
                    DateTime &start_di,
                    time_frequency t_freq,
                    long increment)  {

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
    index_vec.push_back(start_di);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
std::vector<TS> &&
gen_datetime_index(const char *start_datetime,
                   const char *end_datetime,
                   time_frequency t_freq,
                   long increment)  {

    DateTime        start_di(start_datetime, DT_DATE_STYLE::AME_STYLE);
    const DateTime  end_di(end_datetime, DT_DATE_STYLE::AME_STYLE);
    const double    diff = end_di.diff_seconds(start_di);
    std::vector<TS> index_vec;

    switch(t_freq)  {
        case time_frequency::annual:
            index_vec.reserve(diff / (365 * 24 * 60 * 60) + 1);
            break;
        case time_frequency::monthly:
            index_vec.reserve(diff / (30 * 24 * 60 * 60) + 1);
            break;
        case time_frequency::weekly:
            index_vec.reserve(diff / (7 * 24 * 60 * 60) + 1);
            break;
        case time_frequency::daily:
            index_vec.reserve(diff / (24 * 60 * 60) + 1);
            break;
        case time_frequency::hourly:
            index_vec.reserve(diff / (60 * 60) + 1);
            break;
        case time_frequency::minutely:
            index_vec.reserve(diff / 60 + 1);
            break;
        case time_frequency::secondly:
            index_vec.reserve(diff + 1);
            break;
        case time_frequency::millisecondly:
            index_vec.reserve(diff * 999);
            break;
        default:
            throw NotFeasible ("ERROR: gen_datetime_index()");
    }

    while (start_di < end_di)
        _generate_ts_index_<TS>(index_vec, start_di, t_freq, increment);

    return (std::move(index_vec));
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ITR>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::append_index(Index2D<const ITR &> range)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call append_index()");

    const size_type s = std::distance(range.begin, range.end);

    indices_.reserve (indices_.size() + s);
    indices_.insert (indices_.end (), range.begin, range.end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::append_index(const TimeStamp &val)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call append_index()");

    indices_.push_back (val);
    return (1);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T, typename ITR>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::
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

    const auto      iter = data_tb_.find (name);
    std::vector<T>  *vec_ptr = nullptr;

    if (iter == data_tb_.end())
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

template<typename TS, typename  HETERO>
template<typename T, typename ITR>
void DataFrame<TS, HETERO>::
setup_view_column_ (const char *name, Index2D<ITR> range)  {

    static_assert(std::is_base_of<HeteroView, HETERO>::value,
                  "Only a DataFrameView can call setup_view_column_()");

    data_.emplace_back (DataVec(&*(range.begin), &*(range.end)));
    data_tb_.emplace (name, data_.size() - 1);

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::
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

    const auto      iter = data_tb_.find (name);
    std::vector<T>  *vec_ptr = nullptr;

    if (iter == data_tb_.end())
        vec_ptr = &(create_column<T>(name));
    else  {
        DataVec &hv = data_[iter->second];

        vec_ptr = &(hv.template get_vector<T>());
    }

    *vec_ptr = std::move(data);
    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::
load_column (const char *name,
             const std::vector<T> &data,
             nan_policy padding)  {

    return (load_column<T>(name, { data.begin(), data.end() }, padding));
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T1, typename T2>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::_load_pair(std::pair<T1, T2> &col_name_data)  {

    return (load_column<typename decltype(col_name_data.second)::value_type>(
                col_name_data.first, // column name
                std::move(col_name_data.second),
                nan_policy::pad_with_nans));
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T, typename ITR>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::
append_column (const char *name,
               Index2D<const ITR &> range,
               nan_policy padding)  {

    const auto  iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
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

template<typename TS, typename  HETERO>
template<typename T>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::
append_column (const char *name, const T &val, nan_policy padding)  {

    const auto  iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
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

template<typename TS, typename  HETERO>
template<typename ... types>
void
DataFrame<TS, HETERO>::remove_data_by_idx (Index2D<TS> range)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
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
        make_consistent<types ...>();
        indices_.erase(lower, upper);

        remove_functor_<types ...>  functor (b_dist, e_dist);

        for (auto &iter : data_tb_)
            data_[iter.second].change(functor);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ... types>
void
DataFrame<TS, HETERO>::remove_data_by_loc (Index2D<int> range)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call remove_data_by_loc()");

    if (range.begin < 0)
        range.begin = static_cast<int>(indices_.size()) + range.begin;
    if (range.end < 0)
        range.end = static_cast<int>(indices_.size()) + range.end;

    if (range.end <= static_cast<int>(indices_.size()) &&
        range.begin <= range.end && range.begin >= 0)  {
        make_consistent<types ...>();
        indices_.erase(indices_.begin() + range.begin,
                          indices_.begin() + range.end);

        remove_functor_<types ...>  functor (
            static_cast<size_type>(range.begin),
            static_cast<size_type>(range.end));

        for (auto &iter : data_tb_)
            data_[iter.second].change(functor);

        return;
    }

    char buffer [512];

    sprintf (buffer,
             "DataFrame::remove_data_by_loc(): ERROR: "
             "Bad begin, end range: %d, %d",
             range.begin, range.end);
    throw BadRange (buffer);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
