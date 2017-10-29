// Hossein Moein
// September 12, 2017
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame.h>
#include <type_traits>
#include <limits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename TS, template<typename DT, class... types> class DS>
template<typename T>
DS<T> &DataFrame<TS, DS>::create_column (const char *name)  {

    if (! ::strcmp(name, "INDEX"))
        throw DataFrameError ("DataFrame::create_column(): ERROR: "
                              "Data column name cannot be 'INDEX'");

    data_.emplace_back (DataVec());
    data_tb_.emplace (name, data_.size() - 1);

    DataVec &hv = data_.back();
    DS<T>   &vec = hv.get_vec<T, DS<T>>();

    // vec.resize(timestamps_.size(), _get_nan<T>());
    return (vec);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::load_data (TSVec &&indices, Ts ... args)  {

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

template<typename TS, template<typename DT, class... types> class DS>
template<typename ITR>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::load_index(const ITR &begin, const ITR &end)  {

    const size_type s = std::distance(begin, end);

    timestamps_.clear ();
    timestamps_.reserve (s);
    timestamps_.insert (timestamps_.begin (), begin, end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::load_index(TSVec &&idx)  {

    timestamps_ = idx;
    return (timestamps_.size());
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ITR>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::append_index(const ITR &begin, const ITR &end)  {

    const size_type s = std::distance(begin, end);

    timestamps_.reserve (timestamps_.size() + s);
    timestamps_.insert (timestamps_.end (), begin, end);
    return (s);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::append_index(const TimeStamp &val)  {

    timestamps_.push_back (val);
    return (1);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T, typename ITR>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::load_column (const char *name,
                                const ITR &begin,
                                const ITR &end,
                                bool pad_with_nan)  {

    size_type s = std::distance(begin, end);
    const size_type idx_s = timestamps_.size();

    if (s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::load_column(): ERROR: "
                         "data size of %lu is larger than index size of %lu",
                 s, idx_s);
        throw InconsistentData (buffer);
    }

    const auto  iter = data_tb_.find (name);
    DS<T>       *vec_ptr = nullptr;

    if (iter == data_tb_.end())
        vec_ptr = &(create_column<T>(name));
    else  {
        DataVec &hv = data_[iter->second];

        vec_ptr = &(hv.get_vec<T, DS<T>>());
    }

    vec_ptr->clear();
    vec_ptr->reserve(idx_s);
    vec_ptr->insert (vec_ptr->begin (), begin, end);

    size_type   ret_cnt = s;

    s = vec_ptr->size();
    if (pad_with_nan && s < idx_s)  {
        for (size_type i = 0; i < idx_s - s; ++i)  {
            vec_ptr->push_back (std::move(_get_nan<T>()));
            ret_cnt += 1;
        }
    }

    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::
load_column (const char *name, DS<T> &&data, bool pad_with_nan)  {

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

    if (pad_with_nan && data_s < idx_s)  {
        for (size_type i = 0; i < idx_s - data_s; ++i)  {
            data.push_back (std::move(_get_nan<T>()));
            ret_cnt += 1;
        }
    }

    const auto  iter = data_tb_.find (name);
    DS<T>       *vec_ptr = nullptr;

    if (iter == data_tb_.end())
        vec_ptr = &(create_column<T>(name));
    else  {
        DataVec &hv = data_[iter->second];

        vec_ptr = &(hv.get_vec<T, DS<T>>());
    }

    *vec_ptr = std::move(data);
    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T1, typename T2>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::_load_pair(std::pair<T1, T2> &col_name_data)  {

    return (load_column<typename decltype(col_name_data.second)::value_type>(
                col_name_data.first, // column name
                std::move(col_name_data.second),
                true));
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T, typename ITR>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::append_column (const char *name,
                                  const ITR &begin,
                                  const ITR &end,
                                  bool pad_with_nan)  {

    const auto  iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
                         "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    DataVec &hv = data_[iter->second];
    DS<T>   &vec = hv.get_vec<T, DS<T>>();

    size_type       s = std::distance(begin, end) + vec.size ();
    const size_type idx_s = timestamps_.size();

    if (s > idx_s)  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
                         "data size of %lu is larger than index size of %lu",
                 s, idx_s);
        throw InconsistentData (buffer);
    }

    vec.reserve (idx_s);
    vec.insert (vec.end (), begin, end);

    size_type   ret_cnt = s;

    s = vec.size();
    if (pad_with_nan && s < idx_s)  {
        for (size_type i = 0; i < idx_s - s; ++i)  {
            vec.push_back (std::move(_get_nan<T>()));
            ret_cnt += 1;
        }
    }

    return (ret_cnt);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T>
typename DataFrame<TS, DS>::size_type
DataFrame<TS, DS>::
append_column (const char *name, const T &val, bool pad_with_nan)  {

    const auto  iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer, "DataFrame::append_column(): ERROR: "
                         "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    DataVec &hv = data_[iter->second];
    DS<T>   &vec = hv.get_vec<T, DS<T>>();

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
    if (pad_with_nan && s < idx_s)  {
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
