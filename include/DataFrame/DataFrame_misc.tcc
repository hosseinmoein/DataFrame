// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DateTime.h>
#include <DataFrame/DataFrame.h>

#include <cstdio>

// ----------------------------------------------------------------------------

namespace hmdf
{

#if defined(WIN32) || defined (_WIN32)
#undef min
#undef max
#endif // defined(WIN32) || defined (_WIN32)

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::consistent_functor_<Ts ...>::operator() (T &vec) const  {

    using ValueType =
        typename std::remove_reference<decltype(vec)>::type::value_type;

    vec.resize(size, ValueType());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ... Ts>
template<typename T2>
void
DataFrame<I, H>::sort_functor_<T, Ts ...>::operator() (T2 &vec) const  {

    using VecType = typename std::remove_reference<decltype(vec)>::type;
    using DataValueType = typename VecType::value_type;

    std::multimap<T, DataValueType> tmp_map;
    const DataFrame::size_type      idx_s = idx_vec.size();

    for (size_t i = 0; i < idx_s; ++i)
        tmp_map.emplace(std::move(idx_vec[i]), std::move(vec[i]));
    vec.clear();
    for (auto &iter : tmp_map)
        vec.emplace_back(std::move(iter.second));

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::load_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    df.load_column<ValueType>(name,
                              { vec.begin() + begin, vec.begin() + end },
                              nan_policy::pad_with_nans);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::remove_functor_<Ts ...>::operator() (T &vec)  {

    vec.erase(vec.begin() + begin, vec.begin() + end);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::view_setup_functor_<Ts ...>::
operator() (T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    dfv.template setup_view_column_<ValueType, typename VecType::iterator>(
        name,
        { vec.begin() + begin, vec.begin() + end });
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::add_col_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    df.create_column<ValueType>(name);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename F, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::groupby_functor_<F, Ts ...>::operator() (const T &vec)  {

    for (std::size_t i = begin; i < end && i < vec.size(); ++i)
        functor (indices, name, vec[i]);

    if (! ::strcmp(name, "INDEX"))  {
        IndexType   v;

        functor.get_value(v);
        df.append_index(v);
    }
    else  {
        using VecType = typename std::remove_reference<T>::type;
        using ValueType = typename VecType::value_type;

        ValueType   v;

        functor.get_value(v);
        df.append_column<ValueType>(name, v, nan_policy::dont_pad_with_nans);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename F, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::bucket_functor_<F, Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const std::size_t   ts_s = indices.size();
    std::size_t         marker = 0;

    if (df.indices_.empty())
        for (std::size_t i = 0; i < ts_s; ++i)
            if (indices[i] - indices[marker] >= interval)  {
                df.indices_.push_back(indices[i - 1]);
                marker = i;
            }

    for (std::size_t i = 0, marker = 0; i < ts_s; ++i)  {
        if (indices[i] - indices[marker] >= interval)  {
            ValueType   v;

            functor.get_value(v);
            df.append_column<ValueType>(name,
                                        v,
                                        nan_policy::dont_pad_with_nans);
            functor.reset();
            marker = i;
        }
        functor (indices[i], name, vec[i]);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::print_functor_<Ts ...>::operator() (const T &vec)  {

    if (vec.empty())  return;

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    if (! values_only)  {
        os << name << ':' << vec.size() << ':';
        if (typeid(ValueType) == typeid(float))
            os << "<float>:";
        else if (typeid(ValueType) == typeid(double))
            os << "<double>:";
        else if (typeid(ValueType) == typeid(int))
            os << "<int>:";
        else if (typeid(ValueType) == typeid(unsigned int))
            os << "<uint>:";
        else if (typeid(ValueType) == typeid(long int))
            os << "<long>:";
        else if (typeid(ValueType) == typeid(long long int))
            os << "<longlong>:";
        else if (typeid(ValueType) == typeid(unsigned long int))
            os << "<ulong>:";
        else if (typeid(ValueType) == typeid(unsigned long long int))
            os << "<ulonglong>:";
        else if (typeid(ValueType) == typeid(std::string))
            os << "<string>:";
        else if (typeid(ValueType) == typeid(bool))
            os << "<bool>:";
        else if (typeid(ValueType) == typeid(DateTime))
            os << "<DateTime>:";
        else
            os << "<N/A>:";
    }
    for (std::size_t i = 0; i < vec.size(); ++i)
        os << vec[i] << ',';
    os << '\n';

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
equal_functor_<Ts ...>::operator() (const std::vector<T> &lhs_vec)  {

    const auto  &iter = df.column_tb_.find(name);

    if (iter == df.column_tb_.end())  {
        result = false;
        return;
    }

    const DataVec           &hv = df.data_[iter->second];
    const std::vector<T>    &rhs_vec = hv.template get_vector<T>();

    if (lhs_vec != rhs_vec)
        result = false;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
mod_by_idx_functor_<Ts ...>::operator() (std::vector<T> &lhs_vec) const  {

    const auto  &iter = rhs_df.column_tb_.find(name);

    if (iter != rhs_df.column_tb_.end())  {
        const std::vector<T>    &rhs_vec = rhs_df.get_column<T>(name);

        lhs_vec[lhs_idx] = rhs_vec[rhs_idx];
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts, typename F, std::size_t ... Is>
void DataFrame<I, H>::
for_each_in_tuple_ (const std::tuple<Ts ...> &tu,
                    F func,
                    std::index_sequence<Is ...>)  {

    using expander = int[];
    (void) expander { 0, (func(std::get<Is>(tu)), 0) ... };
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts, typename F, std::size_t ... Is>
void DataFrame<I, H>::
for_each_in_tuple_ (std::tuple<Ts ...> &tu,
                    F func,
                    std::index_sequence<Is ...>)  {

    using expander = int[];
    (void) expander { 0, (func(std::get<Is>(tu)), 0) ... };
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts, typename F>
void DataFrame<I, H>::
for_each_in_tuple_ (const std::tuple<Ts...> &tu, F func)  {

    for_each_in_tuple_(tu, func, std::make_index_sequence<sizeof...(Ts)>());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts, typename F>
void DataFrame<I, H>::for_each_in_tuple_ (std::tuple<Ts...> &tu, F func) {

    for_each_in_tuple_(tu, func, std::make_index_sequence<sizeof...(Ts)>());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
index_join_functor_common_<Ts ...>::operator()(const std::vector<T> &lhs_vec)  {

    const std::vector<T>    &rhs_vec = rhs.get_column<T>(name);
    std::vector<T>          lhs_result_col;
    std::vector<T>          rhs_result_col;

    lhs_result_col.reserve(joined_index_idx.size());
    rhs_result_col.reserve(joined_index_idx.size());
    for (auto &citer : joined_index_idx)  {
        const size_type left_i = std::get<0>(citer);
        const size_type right_i = std::get<1>(citer);

        lhs_result_col.push_back(
            left_i != std::numeric_limits<size_type>::max()
                ? lhs_vec[left_i] : DataFrame::_get_nan<T>());
        rhs_result_col.push_back(
            right_i != std::numeric_limits<size_type>::max()
                ? rhs_vec[right_i] : DataFrame::_get_nan<T>());
    }

    char    lhs_str[256];
    char    rhs_str[256];

    ::sprintf(lhs_str, "lhs.%s", name);
    ::sprintf(rhs_str, "rhs.%s", name);
    result.load_column(lhs_str, std::move(lhs_result_col));
    result.load_column(rhs_str, std::move(rhs_result_col));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<int SIDE, typename ... Ts>
template<typename T>
void DataFrame<I, H>::index_join_functor_oneside_<SIDE, Ts ...>::
operator()(const std::vector<T> &vec)  {

    std::vector<T>          result_col;

    result_col.reserve(joined_index_idx.size());
    for (auto &citer : joined_index_idx)  {
        const size_type i = std::get<SIDE>(citer);

        result_col.push_back(
            i != std::numeric_limits<size_type>::max()
                ? vec[i] : DataFrame::_get_nan<T>());
    }

    result.load_column(name, std::move(result_col));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void DataFrame<I, H>::vertical_shift_functor_<Ts ...>::
operator() (T &vec) const  {

    if (sp == shift_policy::up)
        DataFrame::shift_left_(vec, n);
    else
        DataFrame::shift_right_(vec, n);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void DataFrame<I, H>::rotate_functor_<Ts ...>::
operator() (T &vec) const  {

    if (sp == shift_policy::up)
        DataFrame::rotate_left_(vec, n);
    else
        DataFrame::rotate_right_(vec, n);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename TST, template<typename> class OPT, typename ... types>
template<typename T>
void
DataFrame<I, H>::
operator_functor_<TST, OPT, types ...>::
operator()(const std::vector<T> &lhs_vec)  {

    const auto  rhs_citer = rhs_df.column_tb_.find(col_name);

    if (rhs_citer == rhs_df.column_tb_.end())  return;

    const DataVec   &rhs_hv = rhs_df.data_[rhs_citer->second];
    const auto      &rhs_vec = rhs_hv.template get_vector<T>();
    const size_type new_col_size =
        std::min(std::min(lhs_vec.size(), rhs_vec.size()), new_idx.size());
    std::vector<T>  new_col;
    auto            Operator = OPT<T>();
    size_type       lcounter = 0;
    size_type       rcounter = 0;

    new_col.reserve(new_col_size);
    for (size_type idx = 0; idx < new_col_size; )  {
        if (lhs_idx[lcounter] == rhs_idx[rcounter])  {
            new_col.push_back(Operator(lhs_vec[lcounter], rhs_vec[rcounter]));
            lcounter += 1;
            rcounter += 1;
            idx += 1;
        }
        else if (lhs_idx[lcounter] > rhs_idx[rcounter])  {
            rcounter += 1;
        }
        else  {
            lcounter += 1;
        }

    }

    if (! new_col.empty())
        result_df.load_column(col_name, std::move(new_col));
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... types>
template<typename T>
void
DataFrame<I, H>::
map_missing_rows_functor_<types ...>::
operator()(const T &vec)  {

    const size_type vec_size = vec.size();

    for (size_type idx = 0; idx < index_rows; ++idx)  {
        if (idx >= vec_size || DataFrame::_is_nan(vec[idx]))  {
            auto result = missing_row_map.emplace(idx, 0);

            result.first->second += 1;
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... types>
template<typename T>
void
DataFrame<I, H>::
drop_missing_rows_functor_<types ...>::
operator()(T &vec)  {

    drop_missing_rows_(vec, missing_row_map, policy, threshold, col_num);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... types>
template<typename T>
void
DataFrame<I, H>::
get_row_functor_<types ...>::
operator()(T &vec)  {

    result.reserve<typename T::value_type>(1);
    if (row_num < vec.size())
        result.push_back(vec[row_num]);
    else
        result.push_back(_get_nan<typename T::value_type>());
    return;
}


} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
