// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DataFrame.h"
#include <type_traits>
#include <limits>

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

    // vec.resize(timestamps_.size(), _get_nan<T>());
    return (vec);
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
        [this, &cnt](auto &pa) mutable -> void { cnt += this->_load_pair(pa); };

    for_each_in_tuple_ (args_tuple, fc);

    return (cnt);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ITR>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::load_index(const ITR &begin, const ITR &end)  {

    const size_type s = std::distance(begin, end);

    timestamps_.clear ();
    timestamps_.reserve (s);
    timestamps_.insert (timestamps_.begin (), begin, end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::load_index(TSVec &&idx)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call load_index()");

    timestamps_ = idx;
    return (timestamps_.size());
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ITR>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::append_index(Index2D<const ITR &> range)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call append_index()");

    const size_type s = std::distance(range.begin, range.end);

    timestamps_.reserve (timestamps_.size() + s);
    timestamps_.insert (timestamps_.end (), range.begin, range.end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::append_index(const TimeStamp &val)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call append_index()");

    timestamps_.push_back (val);
    return (1);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T, typename ITR>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::
load_column (const char *name, Index2D<const ITR &> range, nan_policy padding) {

    size_type       s = std::distance(range.begin, range.end);
    const size_type idx_s = timestamps_.size();

    if (s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::load_column(): ERROR: "
                         "data size of %lu is larger than index size of %lu",
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

    data_.emplace_back (DataVec(&*(range.begin), &*(range.end - 1)));
    data_tb_.emplace (name, data_.size() - 1);

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T>
typename DataFrame<TS, HETERO>::size_type
DataFrame<TS, HETERO>::
load_column (const char *name, std::vector<T> &&data, nan_policy padding)  {

    const size_type idx_s = timestamps_.size();
    const size_type data_s = data.size();

    if (data_s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::load_column(): ERROR: "
                         "data size of %lu is larger than index size of %lu",
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
    const size_type idx_s = timestamps_.size();

    if (s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
                         "data size of %lu is larger than index size of %lu",
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
    const size_type idx_s = timestamps_.size();

    if (s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
                         "data size of %lu is larger than index size of %lu",
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
