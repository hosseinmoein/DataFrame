// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>

#include <algorithm>
#include <limits>
#include <random>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<size_t N, typename ... Ts>
void
DataFrame<I, H>::shuffle(const std::array<const char *, N> col_names,
                         bool also_shuffle_index)  {

    if (also_shuffle_index)  {
        std::random_device  rd;
        std::mt19937        g(rd());

        std::shuffle(indices_.begin(), indices_.end(), g);
    }

    shuffle_functor_<Ts ...>    functor;

    for (auto name_citer : col_names)  {
        const auto  citer = column_tb_.find (name_citer);

        if (citer == column_tb_.end())  {
            char buffer [512];

            sprintf(buffer,
                    "DataFrame::shuffle(): ERROR: Cannot find column '%s'",
                    name_citer);
            throw ColNotFound(buffer);
        }

        data_[citer->second].change(functor);
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
inline constexpr T DataFrame<I, H>::_get_nan()  {

    if (std::numeric_limits<T>::has_quiet_NaN)
        return (std::numeric_limits<T>::quiet_NaN());
    return (T());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
inline constexpr bool DataFrame<I, H>::_is_nan(const T &val)  {

    if (std::numeric_limits<T>::has_quiet_NaN)
        return (is_nan__(val));
    return (_get_nan<T>() == val);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
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

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
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

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
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

template<typename I, typename H>
template<typename T,
         typename std::enable_if<! std::is_arithmetic<T>::value ||
                                 ! std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::
fill_missing_linter_(std::vector<T> &, const IndexVecType &, int)  {

    throw NotFeasible("fill_missing_linter_(): ERROR: Interpolation is "
                      "not feasible on non-arithmetic types");
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T,
         typename std::enable_if<std::is_arithmetic<T>::value &&
                                 std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::
fill_missing_linter_(std::vector<T> &vec,
                     const IndexVecType &index,
                     int limit)  {

    const long  vec_size = static_cast<long>(vec.size());

    if (vec_size < 3)  return;

    int             count = 0;
    T               *y1 = &(vec[0]);
    T               *y2 = &(vec[2]);
    const IndexType *x = &(index[1]);
    const IndexType *x1 = &(index[0]);
    const IndexType *x2 = &(index[2]);

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

template<typename I, typename H>
template<typename T, size_t N>
void DataFrame<I, H>::
fill_missing(const std::array<const char *, N> col_names,
             fill_policy fp,
             const std::array<T, N> values,
             int limit)  {

    std::vector<std::future<void>>  futures(get_thread_level());
    size_type                       thread_count = 0;

    for (size_type i = 0; i < N; ++i)  {
        const auto  citer = column_tb_.find (col_names[i]);

        if (citer == column_tb_.end())  {
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

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
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

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::
drop_missing(drop_policy policy, size_type threshold)  {

    DropRowMap                      missing_row_map;
    std::vector<std::future<void>>  futures(get_thread_level());
    size_type                       thread_count = 0;
    const size_type                 data_size = data_.size();

    map_missing_rows_functor_<Ts ...>   functor (indices_.size(),
                                                 missing_row_map);

    for (size_type idx = 0; idx < data_size; ++idx)  {
        if (thread_count >= get_thread_level())
            data_[idx].change(functor);
        else  {
            auto    to_be_called =
                static_cast
                    <void(DataVec::*)(map_missing_rows_functor_<Ts ...> &&)>
                        (&DataVec::template
                             change<map_missing_rows_functor_<Ts ...>>);

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

    drop_missing_rows_functor_<Ts ...>  functor2 (missing_row_map,
                                                  policy,
                                                  threshold,
                                                  data_.size());

    for (size_type idx = 0; idx < data_size; ++idx)  {
        if (thread_count >= get_thread_level())
            data_[idx].change(functor2);
        else  {
            auto    to_be_called =
                static_cast
                    <void(DataVec::*)(drop_missing_rows_functor_<Ts ...>&&)>
                        (&DataVec::template
                             change<drop_missing_rows_functor_<Ts ...>>);

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

template<typename V, typename T, size_t N>
inline static void
_replace_vector_vals_(V &data_vec,
                      const std::array<T, N> &old_values,
                      const std::array<T, N> &new_values,
                      size_t &count,
                      int limit)  {

    const size_t    vec_s = data_vec.size();

    for (size_t i = 0; i < N; ++i)  {
        for (size_t j = 0; j < vec_s; ++j)  {
            if (limit >= 0 && count >= static_cast<size_t>(limit))  return;
            if (old_values[i] == data_vec[j])  {
                data_vec[j] = new_values[i];
                count += 1;
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, size_t N>
typename DataFrame<I, H>::size_type DataFrame<I, H>::
replace(const char *col_name,
        const std::array<T, N> old_values,
        const std::array<T, N> new_values,
        int limit)  {

    size_type   count = 0;
    const auto  citer = column_tb_.find (col_name);

    if (citer == column_tb_.end())  {
        char buffer [512];

        sprintf(buffer,
                "DataFrame::replace(): ERROR: Cannot find column '%s'",
                col_name);
        throw ColNotFound(buffer);
    }

    DataVec         &hv = data_[citer->second];
    std::vector<T>  &vec = hv.template get_vector<T>();

    _replace_vector_vals_<std::vector<T>, T, N>
        (vec, old_values, new_values, count, limit);

    return (count);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<size_t N>
typename DataFrame<I, H>::size_type DataFrame<I, H>::
replace_index(const std::array<IndexType, N> old_values,
              const std::array<IndexType, N> new_values,
              int limit)  {

    size_type   count = 0;

    _replace_vector_vals_<IndexVecType, IndexType, N>
        (indices_, old_values, new_values, count, limit);

    return (count);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename F>
void DataFrame<I, H>::
replace(const char *col_name, F &functor)  {

    const auto  citer = column_tb_.find (col_name);

    if (citer == column_tb_.end())  {
        char buffer [512];

        sprintf(buffer,
                "DataFrame::replace(): ERROR: Cannot find column '%s'",
                col_name);
        throw ColNotFound(buffer);
    }

    DataVec         &hv = data_[citer->second];
    std::vector<T>  &vec = hv.template get_vector<T>();
    const size_type vec_s = vec.size();

    for (size_type i = 0; i < vec_s; ++i)
        if (! functor(indices_[i], vec[i]))  break;

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, size_t N>
std::future<typename DataFrame<I, H>::size_type> DataFrame<I, H>::
replace_async(const char *col_name,
              const std::array<T, N> old_values,
              const std::array<T, N> new_values,
              int limit)  {

    return (std::async(std::launch::async,
                       &DataFrame::replace<T, N>,
                           this,
                           col_name,
                           old_values,
                           new_values,
                           limit));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename F>
std::future<void> DataFrame<I, H>::
replace_async(const char *col_name, F &functor)  {

    return (std::async(std::launch::async,
                       &DataFrame::replace<T, F>,
                           this,
                           col_name,
                           std::ref(functor)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ...Ts>
void DataFrame<I, H>::make_consistent ()  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call make_consistent()");

    const size_type             idx_s = indices_.size();
    consistent_functor_<Ts ...> functor (idx_s);

    for (auto &iter : data_)
        iter.change(functor);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ...Ts>
void DataFrame<I, H>::shrink_to_fit ()  {

    indices_.shrink_to_fit();

    shrink_to_fit_functor_<Ts ...>  functor;

    for (auto &iter : data_)
        iter.change(functor);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ...Ts>
void DataFrame<I, H>::sort(const char *by_name)  {

    make_consistent<Ts ...>();

    if (by_name == nullptr)  {
        sort_functor_<IndexType, Ts ...>    functor (indices_);

        for (auto &iter : data_)
            iter.change(functor);

        std::sort (indices_.begin(), indices_.end());
    }
    else  {
        const auto  iter = column_tb_.find (by_name);

        if (iter == column_tb_.end())  {
            char buffer [512];

            sprintf (buffer, "DataFrame::sort(): ERROR: "
                             "Cannot find column '%s'",
                     by_name);
            throw ColNotFound (buffer);
        }

        DataVec                     &hv = data_[iter->second];
        std::vector<T>              &idx_vec = hv.template get_vector<T>();
        sort_functor_<T, Ts ...>    functor (idx_vec);

        for (size_type i = 0; i < data_.size(); ++i)
            if (i != iter->second)
                data_[i].change(functor);
        functor(indices_);

        std::sort (idx_vec.begin(), idx_vec.end());
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ...Ts>
std::future<void> DataFrame<I, H>::sort_async(const char *by_name)  {

    return (std::async(std::launch::async,
                       &DataFrame::sort<T, Ts ...>,
                       this, by_name));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename F, typename T, typename ...Ts>
DataFrame<I, H>
DataFrame<I, H>:: groupby (F &&func,
                           const char *gb_col_name,
                           sort_state already_sorted) const  {

    DataFrame   tmp_df = *this;

    if (already_sorted == sort_state::not_sorted)  {
        if (gb_col_name == nullptr) { tmp_df.sort<T, Ts ...>(); }
        else { tmp_df.sort<T, Ts ...>(gb_col_name); }
    }

    DataFrame   df;

    for (const auto &iter : tmp_df.column_tb_)  {
        add_col_functor_<Ts ...>    functor (iter.first.c_str(), df);

        tmp_df.data_[iter.second].change(functor);
    }

    const size_type vec_size = tmp_df.indices_.size();
    size_type       marker = 0;

    if (gb_col_name == nullptr)  { // Index
        for (size_type i = 0; i < vec_size; ++i)  {
            if (tmp_df.indices_[i] != tmp_df.indices_[marker])  {
                df.append_index(tmp_df.indices_[marker]);
                for (const auto &iter : tmp_df.column_tb_)  {
                    groupby_functor_<F, Ts...>  functor(
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
            for (const auto &iter : tmp_df.column_tb_)  {
                groupby_functor_<F, Ts...>  functor(
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
                groupby_functor_<F, IndexType>  ts_functor(
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

                for (const auto &iter : tmp_df.column_tb_)  {
                    if (iter.first != gb_col_name)  {
                        groupby_functor_<F, Ts...>  functor(
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
            groupby_functor_<F, IndexType>  ts_functor(
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

            for (const auto &iter : tmp_df.column_tb_)  {
                if (iter.first != gb_col_name)  {
                    groupby_functor_<F, Ts...>  functor(
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

template<typename I, typename H>
template<typename F, typename T, typename ...Ts>
std::future<DataFrame<I, H>>
DataFrame<I, H>::groupby_async (F &&func,
                                const char *gb_col_name,
                                sort_state already_sorted) const  {

    return (std::async(std::launch::async,
                       &DataFrame::groupby<F, T, Ts ...>,
                           this,
                           std::move(func),
                           gb_col_name,
                           already_sorted));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
StdDataFrame<T>
DataFrame<I, H>::value_counts (const char *col_name) const  {

    auto iter = column_tb_.find (col_name);

    if (iter == column_tb_.end())  {
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

template<typename I, typename H>
template<typename F, typename ...Ts>
DataFrame<I, H>
DataFrame<I, H>::
bucketize (F &&func, const IndexType &bucket_interval) const  {

    DataFrame   df;

    for (const auto &iter : column_tb_)  {
        add_col_functor_<Ts ...>    functor (iter.first.c_str(), df);

        data_[iter.second].change(functor);
    }

    for (const auto &iter : column_tb_)  {
        bucket_functor_<F, Ts...>   functor(
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

template<typename I, typename H>
template<typename F, typename ...Ts>
std::future<DataFrame<I, H>>
DataFrame<I, H>::
bucketize_async (F &&func, const IndexType &bucket_interval) const  {

    return (std::async(std::launch::async,
                       &DataFrame::bucketize<F, Ts ...>,
                           this,
                           std::move(func),
                           std::cref(bucket_interval)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename V>
DataFrame<I, H> DataFrame<I, H>::
transpose(IndexVecType &&indices,
          const V &current_col_order,
          const V &new_col_names) const  {

    const size_type num_cols = column_tb_.size();

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
        const auto  &data_citer = column_tb_.find(citer);

        if (data_citer == column_tb_.end())
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
