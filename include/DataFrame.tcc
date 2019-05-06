// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DataFrame.h"
#include "DateTime.h"
#include <limits>
#include <cmath>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename TS, typename HETERO>
template<typename T>
inline constexpr T DataFrame<TS, HETERO>::_get_nan()  {

    if (std::numeric_limits<T>::has_quiet_NaN)
        return (std::numeric_limits<T>::quiet_NaN());
    return (T());
}

// ----------------------------------------------------------------------------

template<typename T>
inline bool __is_nan__(const T &)  { return(false); }

template<>
inline bool __is_nan__<double>(const double &val)  { return(std::isnan(val)); }

template<>
inline bool __is_nan__<float>(const float &val)  { return(std::isnan(val)); }

template<>
inline bool
__is_nan__<long double>(const long double &val)  { return(std::isnan(val)); }

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T>
inline constexpr bool DataFrame<TS, HETERO>::_is_nan(const T &val)  {

    if (std::numeric_limits<T>::has_quiet_NaN)
        return (__is_nan__(val));
    return (_get_nan<T>() == val);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T>
void DataFrame<TS, HETERO>::
fill_missing_value_(std::vector<T> &vec,
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
        else if (_is_nan<T>(vec[i]))  {
            vec[i] = value;
            count += 1;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T>
void DataFrame<TS, HETERO>::
fill_missing_ffill_(std::vector<T> &vec, int limit, size_type col_num)  {

    const size_type vec_size = vec.size();

    if (vec_size == 0)  return; 

    int count = 0;
    T   last_value = vec[0];

    for (size_type i = 0; i < col_num; ++i)  {
        if (limit >= 0 && count >= limit)  break;
        if (i >= vec_size)  {
            if (! _is_nan(last_value))  {
                vec.reserve(col_num);
                vec.push_back(last_value);
                count += 1;
            }
            else  break;
        }
        else  {
            if (! _is_nan<T>(vec[i]))  last_value = vec[i];
            if (_is_nan<T>(vec[i]) && ! _is_nan<T>(last_value))  {
                vec[i] = last_value;
                count += 1;
            }
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T>
void DataFrame<TS, HETERO>::
fill_missing_bfill_(std::vector<T> &vec, int limit)  {

    const long  vec_size = static_cast<long>(vec.size());

    if (vec_size == 0)  return; 

    int count = 0;
    T   last_value = vec[vec_size - 1];

    for (long i = vec_size - 1; i >= 0; --i)  {
        if (limit >= 0 && count >= limit)  break;
        if (! _is_nan<T>(vec[i]))  last_value = vec[i];
        if (_is_nan<T>(vec[i]) && ! _is_nan<T>(last_value))  {
            vec[i] = last_value;
            count += 1;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T,
         typename std::enable_if<! std::is_arithmetic<T>::value ||
                                 ! std::is_arithmetic<TS>::value>::type*>
void DataFrame<TS, HETERO>::
fill_missing_linter_(std::vector<T> &, const TSVec &, int)  {

    throw NotFeasible("fill_missing_linter_(): ERROR: Interpolation is "
                      "not feasible on non-arithmetic types");
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T,
         typename std::enable_if<std::is_arithmetic<T>::value &&
                                 std::is_arithmetic<TS>::value>::type*>
void DataFrame<TS, HETERO>::
fill_missing_linter_(std::vector<T> &vec, const TSVec &index, int limit)  {

    const long  vec_size = static_cast<long>(vec.size());

    if (vec_size < 3)  return; 

    int         count = 0;
    T           *y1 = &(vec[0]);
    T           *y2 = &(vec[2]);
    const TS    *x = &(index[1]);
    const TS    *x1 = &(index[0]);
    const TS    *x2 = &(index[2]);

    for (long i = 1; i < vec_size - 1; ++i)  {
        if (limit >= 0 && count >= limit)  break;
        if (_is_nan<T>(vec[i]))  {
            if (_is_nan<T>(*y2))  {
                bool    found = false;

                for (long j = i + 1; j < vec_size; ++j)  {
                    if (! _is_nan(vec[j]))  {
                        y2 = &(vec[j]);
                        x2 = &(index[j]);
                        found = true;
                        break;
                    }
                }
                if (! found)  break;
            }
            if (_is_nan<T>(*y1))  {
                for (long j = i - 1; j >= 0; --j)  {
                    if (! _is_nan(vec[j]))  {
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
        y1 = &(vec[i]);
        y2 = &(vec[i + 2]);
        x = &(index[i + 1]);
        x1 = &(index[i]);
        x2 = &(index[i + 2]);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T, size_t N>
void DataFrame<TS, HETERO>::
fill_missing(const std::array<const char *, N> col_names,
             fill_policy fp,
             const std::array<T, N> values,
             int limit)  {

    std::vector<std::future<void>>  futures(get_thread_level());
    size_type                       thread_count = 0;

    for (size_type i = 0; i < N; ++i)  {
        const auto  citer = data_tb_.find (col_names[i]);

        if (citer == data_tb_.end())  {
            char buffer [512];

            sprintf(
                buffer,
                "DataFrame::fill_missing(): ERROR: Cannot find column '%s'",
                col_names[i]);
            throw ColNotFound(buffer);
        }

        DataVec         &hv = data_[citer->second];
        std::vector<T>  &vec = hv.template get_vector<T>();

        if (fp == fill_policy::value)  {
            if (thread_count >= get_thread_level())
                fill_missing_value_(vec, values[i], limit, indices_.size());
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_value_<T>,
                               std::ref(vec),
                               std::cref(values[i]),
                               limit,
                               indices_.size());
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::fill_forward)  {
            if (thread_count >= get_thread_level())
                fill_missing_ffill_(vec, limit, indices_.size());
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_ffill_<T>,
                               std::ref(vec),
                               limit,
                               indices_.size());
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::fill_backward)  {
            if (thread_count >= get_thread_level())
                fill_missing_bfill_(vec, limit);
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_bfill_<T>,
                               std::ref(vec),
                               limit);
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::linear_interpolate)  {
            if (thread_count >= get_thread_level())
                fill_missing_linter_(vec, indices_, limit);
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &DataFrame::fill_missing_linter_<T>,
                               std::ref(vec),
                               std::cref(indices_),
                               limit);
                thread_count += 1;
            }
        }
        else if (fp == fill_policy::linear_extrapolate)  {
            char buffer [512];

            sprintf (
                buffer,
                "DataFrame::fill_missing(): fill_policy %d is not implemented",
                static_cast<int>(fp));
            throw NotImplemented(buffer);
        }
    }

    for (size_type idx = 0; idx < thread_count; ++idx)
        futures[idx].get();
    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T>
void DataFrame<TS, HETERO>::
drop_missing_rows_(T &vec,
                   const DropRowMap missing_row_map,
                   drop_policy policy,
                   size_type threshold,
                   size_type col_num)  {

    size_type   erase_count = 0;

    for (auto &iter : missing_row_map)  {
        if (policy == drop_policy::all)  {
            if (iter.second == col_num)  {
                vec.erase(vec.begin() + iter.first - erase_count);
                erase_count += 1;
            }
        }
        else if (policy == drop_policy::any)  {
            if (iter.second > 0)  {
                vec.erase(vec.begin() + iter.first - erase_count);
                erase_count += 1;
            }
        }
        else if (policy == drop_policy::threshold)  {
            if (iter.second > threshold)  {
                vec.erase(vec.begin() + iter.first - erase_count);
                erase_count += 1;
            }
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename ... types>
void DataFrame<TS, HETERO>::
drop_missing(drop_policy policy, size_type threshold)  {       

    DropRowMap                      missing_row_map;
    std::vector<std::future<void>>  futures(get_thread_level());
    size_type                       thread_count = 0;
    const size_type                 data_size = data_.size();

    map_missing_rows_functor_<types ...>    functor (indices_.size(),
                                                     missing_row_map);

    for (size_type idx = 0; idx < data_size; ++idx)  {
        if (thread_count >= get_thread_level())
            data_[idx].change(functor);
        else  {
            auto    to_be_called =
                static_cast
                    <void(DataVec::*)(map_missing_rows_functor_<types ...> &&)>
                        (&DataVec::template
                             change<map_missing_rows_functor_<types ...>>);

            futures[thread_count] =
                std::async(std::launch::async,
                           to_be_called,
                           &(data_[idx]),
                           std::move(functor));
            thread_count += 1;
        }
    }
    for (size_type idx = 0; idx < thread_count; ++idx)
        futures[idx].get();
    thread_count = 0;

    drop_missing_rows_(indices_,
                       missing_row_map,
                       policy,
                       threshold,
                       data_size);

    drop_missing_rows_functor_<types ...>   functor2 (missing_row_map,
                                                      policy,
                                                      threshold,
                                                      data_.size());

    for (size_type idx = 0; idx < data_size; ++idx)  {
        if (thread_count >= get_thread_level())
            data_[idx].change(functor2);
        else  {
            auto    to_be_called =
                static_cast
                    <void(DataVec::*)(drop_missing_rows_functor_<types ...>&&)>
                        (&DataVec::template
                             change<drop_missing_rows_functor_<types ...>>);

            futures[thread_count] =
                std::async(std::launch::async,
                           to_be_called,
                           &(data_[idx]),
                           std::move(functor2));
            thread_count += 1;
        }
    }
    for (size_type idx = 0; idx < thread_count; ++idx)
        futures[idx].get();

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename ...types>
void DataFrame<TS, HETERO>::make_consistent ()  {

    const size_type                 idx_s = indices_.size();
    consistent_functor_<types ...>  functor (idx_s);

    for (auto &iter : data_)
        iter.change(functor);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T, typename ...types>
void DataFrame<TS, HETERO>::sort(const char *by_name)  {

    make_consistent<types ...>();

    if (by_name == nullptr)  {
        sort_functor_<TimeStamp, types ...> functor (indices_);

        for (auto &iter : data_)
            iter.change(functor);

        std::sort (indices_.begin(), indices_.end());
    }
    else  {
        const auto  iter = data_tb_.find (by_name);

        if (iter == data_tb_.end())  {
            char buffer [512];

            sprintf (buffer, "DataFrame::sort(): ERROR: "
                             "Cannot find column '%s'",
                     by_name);
            throw ColNotFound (buffer);
        }

        DataVec                     &hv = data_[iter->second];
        std::vector<T>              &idx_vec = hv.template get_vector<T>();
        sort_functor_<T, types ...> functor (idx_vec);

        for (size_type i = 0; i < data_.size(); ++i)
            if (i != iter->second)
                data_[i].change(functor);
        functor(indices_);

        std::sort (idx_vec.begin(), idx_vec.end());
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T, typename ...types>
std::future<void> DataFrame<TS, HETERO>::sort_async(const char *by_name)  {

    return (std::async(std::launch::async,
                       &DataFrame::sort<T, types ...>,
                       this, by_name));
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename F, typename T, typename ...types>
DataFrame<TS, HETERO>
DataFrame<TS, HETERO>:: groupby (F &&func,
                                 const char *gb_col_name,
                                 sort_state already_sorted) const  {

    DataFrame   tmp_df = *this;

    if (already_sorted == sort_state::not_sorted)  {
        if (gb_col_name == nullptr) { tmp_df.sort<T, types ...>(); }
        else { tmp_df.sort<T, types ...>(gb_col_name); }
    }

    DataFrame   df;

    for (const auto &iter : tmp_df.data_tb_)  {
        add_col_functor_<types ...> functor (iter.first.c_str(), df);

        tmp_df.data_[iter.second].change(functor);
    }

    const size_type vec_size = tmp_df.indices_.size();
    size_type       marker = 0;

    if (gb_col_name == nullptr)  { // Index
        for (size_type i = 0; i < vec_size; ++i)  {
            if (tmp_df.indices_[i] != tmp_df.indices_[marker])  {
                df.append_index(tmp_df.indices_[marker]);
                for (const auto &iter : tmp_df.data_tb_)  {
                    groupby_functor_<F, types...>   functor(
                                                iter.first.c_str(),
                                                marker,
                                                i,
                                                tmp_df.indices_[marker],
                                                func,
                                                df);

                    tmp_df.data_[iter.second].change(functor);
                    func.reset();
                }

                marker = i;
            }
        }
        if (marker < vec_size)  {
            df.append_index(tmp_df.indices_[vec_size - 1]);
            for (const auto &iter : tmp_df.data_tb_)  {
                groupby_functor_<F, types...>   functor(
                                            iter.first.c_str(),
                                            vec_size - 1,
                                            vec_size,
                                            tmp_df.indices_[vec_size - 1],
                                            func,
                                            df);

                tmp_df.data_[iter.second].change(functor);
            }
        }
    }
    else  { // Non-index column
        const std::vector<T>    &gb_vec = tmp_df.get_column<T>(gb_col_name);

        for (size_type i = 0; i < vec_size; ++i)  {
            if (gb_vec[i] != gb_vec[marker])  {
                groupby_functor_<F, TimeStamp>  ts_functor(
                                            "INDEX",
                                            marker,
                                            i,
                                            tmp_df.indices_[marker],
                                            func,
                                            df);

                ts_functor(tmp_df.indices_);
                df.append_column<T>(gb_col_name,
                                    gb_vec [marker],
                                    nan_policy::dont_pad_with_nans);
                func.reset();

                for (const auto &iter : tmp_df.data_tb_)  {
                    if (iter.first != gb_col_name)  {
                        groupby_functor_<F, types...>   functor(
                                                    iter.first.c_str(),
                                                    marker,
                                                    i,
                                                    tmp_df.indices_[marker],
                                                    func,
                                                    df);

                        tmp_df.data_[iter.second].change(functor);
                        func.reset();
                    }
                }

                marker = i;
            }
        }

        if (marker < vec_size)  {
            groupby_functor_<F, TimeStamp>  ts_functor(
                                        "INDEX",
                                        vec_size - 1,
                                        vec_size,
                                        tmp_df.indices_[vec_size - 1],
                                        func,
                                        df);

            ts_functor(tmp_df.indices_);
            df.append_column<T>(gb_col_name,
                                gb_vec [vec_size - 1],
                                nan_policy::dont_pad_with_nans);
            func.reset();

            for (const auto &iter : tmp_df.data_tb_)  {
                if (iter.first != gb_col_name)  {
                    groupby_functor_<F, types...>   functor(
                                            iter.first.c_str(),
                                            vec_size - 1,
                                            vec_size,
                                            tmp_df.indices_[vec_size - 1],
                                            func,
                                            df);

                    tmp_df.data_[iter.second].change(functor);
                }
            }
        }
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename F, typename T, typename ...types>
std::future<DataFrame<TS, HETERO>>
DataFrame<TS, HETERO>::groupby_async (F &&func,
                                      const char *gb_col_name,
                                      sort_state already_sorted) const  {

    return (std::async(std::launch::async,
                       &DataFrame::groupby<F, T, types ...>,
                           this,
                           std::move(func),
                           gb_col_name,
                           already_sorted));
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T>
StdDataFrame<T>
DataFrame<TS, HETERO>::value_counts (const char *col_name) const  {

    auto iter = data_tb_.find (col_name);

    if (iter == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::value_counts(): ERROR: Cannot find column '%s'",
                 col_name);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv = data_[iter->second];
    const std::vector<T>    &vec = hv.template get_vector<T>();
    auto                    hash_func =
        [](std::reference_wrapper<const T> v) -> std::size_t  {
            return(std::hash<T>{}(v.get()));
    };
    auto                    equal_func =
        [](std::reference_wrapper<const T> lhs,
           std::reference_wrapper<const T> rhs) -> bool  {
            return(lhs.get() == rhs.get());
    };

    std::unordered_map<
        typename std::reference_wrapper<const T>::type,
        size_type,
        decltype(hash_func),
        decltype(equal_func)>   values_map(vec.size(), hash_func, equal_func);
    size_type                   nan_count = 0;

    // take care of nans
    for (auto citer : vec)  {
        if (_is_nan<T>(citer))  {
            ++nan_count;
            continue;
        }

        auto    insert_result = values_map.emplace(std::ref(citer), 1);

        if (insert_result.second == false)
            insert_result.first->second += 1;
    }

    std::vector<T>          res_indices;
    std::vector<size_type>  counts;

    counts.reserve(values_map.size());
    res_indices.reserve(values_map.size());

    for (const auto citer : values_map)  {
        res_indices.push_back(citer.first);
        counts.emplace_back(citer.second);
    }
    if (nan_count > 0)  {
        res_indices.push_back(_get_nan<T>());
        counts.emplace_back(nan_count);
    }

    StdDataFrame<T> result_df;

    result_df.load_index(std::move(res_indices));
    result_df.load_column("counts", std::move(counts));

    return(result_df);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename F, typename ...types>
DataFrame<TS, HETERO>
DataFrame<TS, HETERO>::
bucketize (F &&func, const TimeStamp &bucket_interval) const  {

    DataFrame   df;

    for (const auto &iter : data_tb_)  {
        add_col_functor_<types ...> functor (iter.first.c_str(), df);

        data_[iter.second].change(functor);
    }

    for (const auto &iter : data_tb_)  {
        bucket_functor_<F, types...>   functor(
                                    iter.first.c_str(),
                                    indices_,
                                    bucket_interval,
                                    func,
                                    df);

        data_[iter.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename F, typename ...types>
std::future<DataFrame<TS, HETERO>>
DataFrame<TS, HETERO>::
bucketize_async (F &&func, const TimeStamp &bucket_interval) const  {

    return (std::async(std::launch::async,
                       &DataFrame::bucketize<F, types ...>,
                           this,
                           std::move(func),
                           std::cref(bucket_interval)));
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename T, typename V>
DataFrame<TS, HETERO> DataFrame<TS, HETERO>::
transpose(TSVec &&indices,
          const V &current_col_order,
          const V &new_col_names) const  {

    const size_type num_cols = data_tb_.size();

    if (current_col_order.size() != num_cols)
        throw InconsistentData ("DataFrame::transpose(): ERROR: "
                                "Length of current_col_order is not equal "
                                "to number of columns");
    if (new_col_names.size() != indices_.size())
        throw InconsistentData ("DataFrame::transpose(): ERROR: "
                                "Length of new_col_names is not equal "
                                "to number of rows");

    std::vector<const std::vector<T> *> current_cols;

    current_cols.reserve(num_cols);
    for (const auto citer : current_col_order)  {
        const auto  &data_citer = data_tb_.find(citer);

        if (data_citer == data_tb_.end())
            throw InconsistentData ("DataFrame::transpose(): ERROR: "
                                    "Cannot find at least one of the column "
                                    "names in the order vector");

        const DataVec   &hv = data_[data_citer->second];

        current_cols.push_back(&(hv.template get_vector<T>()));
    }


    std::vector<std::vector<T>> trans_cols(indices_.size());
    DataFrame                   df;
    size_type                   idx = 0;

    for (size_type i = 0; i < indices_.size(); ++i)  {
        trans_cols[i].reserve(num_cols);
        for (size_type j = 0; j < num_cols; ++j)  {
            if (current_cols[j]->size() > i)
                trans_cols[i].push_back((*(current_cols[j]))[i]);
            else
                trans_cols[i].push_back(_get_nan<T>());
        }
    }

    df.load_index(std::move(indices));
    for (size_type i = 0; i < new_col_names.size(); ++i)
        df.load_column(&(new_col_names[i][0]), std::move(trans_cols[i]));

    return (df);
}

// ----------------------------------------------------------------------------

template<typename S, typename T>
inline static S &_write_df_index_(S &o, const T &value)  {

    return (o << value);
}

template<typename S>
inline static S &_write_df_index_(S &o, const DateTime &value)  {

    return (o << value.time() << '.' << value.nanosec());
}

template<typename TS, typename HETERO>
template<typename S, typename ...types>
bool DataFrame<TS, HETERO>::
write (S &o, bool values_only, io_format iof) const  {

    if (! values_only)  {
        o << "INDEX:" << indices_.size() << ':';
        if (typeid(TS) == typeid(double))
            o << "<double>:";
        else if (typeid(TS) == typeid(int))
            o << "<int>:";
        else if (typeid(TS) == typeid(unsigned int))
            o << "<uint>:";
        else if (typeid(TS) == typeid(long))
            o << "<long>:";
        else if (typeid(TS) == typeid(unsigned long))
            o << "<ulong>:";
        else if (typeid(TS) == typeid(std::string))
            o << "<string>:";
        else if (typeid(TS) == typeid(bool))
            o << "<bool>:";
        else if (typeid(TS) == typeid(DateTime))
            o << "<DateTime>:";
        else
            o << "<N/A>:";

        for (size_type i = 0; i < indices_.size(); ++i)
            _write_df_index_(o, indices_[i]) << ',';
        o << '\n';
    }

    for (const auto &iter : data_tb_)  {
        print_functor_<types ...> functor (iter.first.c_str(), values_only, o);

        data_[iter.second].change(functor);
    }

    o << std::endl;
    return (true);
}

// ----------------------------------------------------------------------------

template<typename TS, typename HETERO>
template<typename S, typename ...Ts>
std::future<bool> DataFrame<TS, HETERO>::
write_async (S &o, bool values_only, io_format iof) const  {

    return (std::async(std::launch::async,
                       &DataFrame::write<S, Ts ...>,
                       this,
                       std::ref(o),
                       values_only,
                       iof));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
