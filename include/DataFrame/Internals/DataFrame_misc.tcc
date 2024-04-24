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
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::shrink_to_fit_functor_<Ts ...>::operator() (T &vec) const  {

    using value_type = typename T::value_type;

    vec.shrink_to_fit();

    const size_type s = vec.capacity() * sizeof(value_type);

    if (s && ! (s & (s - 1)))  // Avoid cache line aliasing misses
        vec.reserve(vec.size() + 1);  // Knock it off the power of 2
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::remove_column_functor_<Ts ...>::operator() (T &) const  {

    using value_type = typename T::value_type;

    df.remove_column<value_type>(col_name);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T2>
void
DataFrame<I, H>::sort_functor_<Ts ...>::operator() (T2 &vec)  {

    _sort_by_sorted_index_(vec, sorted_idxs, done_vec, idx_s);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::create_col_functor_<LHS, Ts ...>::operator() (const T &)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    df.template create_column<ValueType>(name, false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::load_functor_<LHS, Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const size_type col_s = vec.size() >= end ? end : vec.size();

    df.template load_column<ValueType>(
        name,
        { vec.begin() + begin, vec.begin() + col_s },
        nan_p,
        false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename DF, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::load_all_functor_<DF, Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    df.template load_column<ValueType>(name, { vec.begin(), vec.end() },
                                       nan_policy::pad_with_nans,
                                       false);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::remove_functor_<Ts ...>::operator() (T &vec)  {

    vec.erase(vec.begin() + begin, vec.begin() + end);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::view_setup_functor_<LHS, Ts ...>::
operator() (T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const size_type col_s = vec.size() >= end ? end : vec.size();

    dfv.template setup_view_column_<ValueType, typename VecType::iterator>(
        name,
        { vec.begin() + begin, vec.begin() + col_s });
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::add_col_functor_<Ts ...>::operator() (const T &)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    df.create_column<ValueType>(name, false);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::print_csv_functor_<Ts ...>::operator() (const T &vec)  {

    if (vec.empty())  return;

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    _write_csv_df_header_<std::ostream, ValueType>(os, name, vec.size()) << ':';

    const long  vec_size = vec.size();
    const long  sr = std::min(start_row, vec_size);
    const long  er = std::min(end_row, vec_size);

    if (vec_size > 0)  {
        for (long i = sr; i < er; ++i)
            _write_csv_df_index_(os, vec[i]) << ',';
    }
    os << '\n';

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::print_json_functor_<Ts ...>::operator() (const T &vec)  {

    if (vec.empty())  return;

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    if (need_pre_comma)
        os << ",\n";

    _write_json_df_header_<std::ostream, ValueType>(os, name, vec.size());

    const long  vec_size = vec.size();
    const long  sr = std::min(start_row, vec_size);
    const long  er = std::min(end_row, vec_size);

    os << "\"D\":[";
    if (vec_size > 0)  {
        _write_json_df_index_(os, vec[start_row]);
        for (long i = sr + 1; i < er; ++i)  {
            os << ',';
            _write_json_df_index_(os, vec[i]);
        }
    }
    os << "]}";

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ... Ts>
template<typename T>
void DataFrame<I, H>::
print_csv2_header_functor_<S, Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    _write_csv_df_header_<S, ValueType>(
        os, name, std::min(col_size, long(vec.size())));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ... Ts>
template<typename T>
void DataFrame<I, H>::
print_csv2_data_functor_<S, Ts ...>::operator() (const T &vec)  {

    if (vec.size() > index)  _write_csv_df_index_(os, vec[index]);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
equal_functor_<Ts ...>::operator() (const T &lhs_vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const auto  &iter = df.column_tb_.find(name);

    if (iter == df.column_tb_.end()) [[unlikely]]  {
        result = false;
        return;
    }

    const DataVec   &hv = df.data_[iter->second];
    const T         &rhs_vec = hv.template get_vector<ValueType>();

    if (lhs_vec != rhs_vec)
        result = false;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
mod_by_idx_functor_<Ts ...>::operator() (T &lhs_vec) const  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const auto  &iter = rhs_df.column_tb_.find(name);

    if (iter != rhs_df.column_tb_.end())  {
        const ColumnVecType<ValueType>  &rhs_vec =
            rhs_df.get_column<ValueType>(name, false);

        lhs_vec[lhs_idx] = rhs_vec[rhs_idx];
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
create_join_common_col_functor_<LHS, Ts ...>::operator() (const T &)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    char    lhs_str[256];
    char    rhs_str[256];

    ::snprintf(lhs_str, sizeof(lhs_str) - 1, "lhs.%s", name);
    ::snprintf(rhs_str, sizeof(rhs_str) - 1, "rhs.%s", name);
    df.template create_column<ValueType>(lhs_str, false);
    df.template create_column<ValueType>(rhs_str, false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RES_T, typename RHS_T, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::index_join_functor_common_<RES_T, RHS_T, Ts ...>::
operator()(const T &lhs_vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const auto              &rhs_vec = rhs.template get_column<ValueType>(name);
    StlVecType<ValueType>   lhs_result_col;
    StlVecType<ValueType>   rhs_result_col;

    lhs_result_col.reserve(joined_index_idx.size());
    rhs_result_col.reserve(joined_index_idx.size());
    for (const auto &citer : joined_index_idx)  {
        const size_type left_i = std::get<0>(citer);
        const size_type right_i = std::get<1>(citer);

        lhs_result_col.push_back(
            left_i != std::numeric_limits<size_type>::max()
                ? lhs_vec[left_i] : get_nan<ValueType>());
        rhs_result_col.push_back(
            right_i != std::numeric_limits<size_type>::max()
                ? rhs_vec[right_i] : get_nan<ValueType>());
    }

    char    lhs_str[256];
    char    rhs_str[256];

    ::snprintf(lhs_str, sizeof(lhs_str) - 1, "lhs.%s", name);
    ::snprintf(rhs_str, sizeof(rhs_str) - 1, "rhs.%s", name);
    result.template load_column<ValueType>(lhs_str,
                                           std::move(lhs_result_col),
                                           nan_policy::pad_with_nans,
                                           false);
    result.template load_column<ValueType>(rhs_str,
                                           std::move(rhs_result_col),
                                           nan_policy::pad_with_nans,
                                           false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<int SIDE, typename RES_T, typename ... Ts>
template<typename T>
void DataFrame<I, H>::index_join_functor_oneside_<SIDE, RES_T, Ts ...>::
operator()(const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    StlVecType<ValueType>   result_col;

    result_col.reserve(joined_index_idx.size());
    for (const auto &citer : joined_index_idx)  {
        const size_type i = std::get<SIDE>(citer);

        result_col.push_back(
            i != std::numeric_limits<size_type>::max()
                ? vec[i] : get_nan<ValueType>());
    }

    result.template load_column<ValueType>(name,
                                           std::move(result_col),
                                           nan_policy::pad_with_nans,
                                           false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename IT, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
concat_load_view_functor_<IT, Ts ...>::
operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const size_type rhs_s = vec.size();
    const bool      has_col = result.has_column(name);

    if (has_col)  {
        auto            &result_vec =
            result.template get_column<ValueType>(name);
        const size_type min_s =
            std::min(result_vec.size() + rhs_s, result.get_index().size());
        size_type       count { vec.size() };

        result_vec.reserve(min_s);
        for (size_type i = 0; i < rhs_s && count < min_s; ++i, ++count)
            result_vec.push_back(const_cast<ValueType *>(&(vec[i])));
    }
    else  {
        T   &nc_vec = const_cast<T &>(vec);

        result.template setup_view_column_<ValueType,
                                           typename VecType::iterator>(
            name, { nc_vec.begin(), nc_vec.end() });
    }

}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RES_T, typename ... Ts>
template<typename T>
void DataFrame<I, H>::concat_functor_<RES_T, Ts ...>::
operator()(const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;
    using res_vec =
        typename
        std::remove_reference<RES_T>::type::template ColumnVecType<ValueType>;

    if (insert_col)  {
        StlVecType<ValueType>   res_vec(original_index_s + vec.size(),
                                        get_nan<ValueType>());

        std::copy(vec.begin(), vec.end(), res_vec.begin() + original_index_s);
        result.template load_column<ValueType>(name,
                                               std::move(res_vec),
                                               nan_policy::pad_with_nans,
                                               false);
    }
    else  {
        res_vec &res_vec = result.template get_column<ValueType>(name);

        res_vec.insert(res_vec.end(), vec.begin(), vec.end());
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void DataFrame<I, H>::vertical_shift_functor_<Ts ...>::
operator() (T &vec) const  {

    if (sp == shift_policy::up)
        shift_left(vec, n);
    else
        shift_right(vec, n);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void DataFrame<I, H>::rotate_functor_<Ts ...>::
operator() (T &vec) const  {

    if (sp == shift_policy::up)  // Rotate left
        // There is no checking the value of n
        std::rotate(vec.begin(), vec.begin() + n, vec.end());
    else  // Rotate right
        // There is no checking the value of n
        std::rotate(vec.rbegin(), vec.rbegin() + n, vec.rend());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename TST, template<typename> class OPT, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
operator_functor_<TST, OPT, Ts ...>::
operator()(const T &lhs_vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const auto  rhs_citer = rhs_df.column_tb_.find(col_name);

    if (rhs_citer == rhs_df.column_tb_.end())  return;

    const DataVec           &rhs_hv = rhs_df.data_[rhs_citer->second];
    const auto              &rhs_vec = rhs_hv.template get_vector<ValueType>();
    const size_type         new_col_size =
        std::min(std::min(lhs_vec.size(), rhs_vec.size()), new_idx.size());
    StlVecType<ValueType>   new_col;
    auto                    opt = OPT<ValueType> { };
    size_type               lcounter = 0;
    size_type               rcounter = 0;

    new_col.reserve(new_col_size);
    for (size_type idx = 0; idx < new_col_size; )  {
        if (lhs_idx[lcounter] == rhs_idx[rcounter])  {
            new_col.push_back(opt(lhs_vec[lcounter], rhs_vec[rcounter]));
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
        result_df.template load_column<ValueType>(col_name,
                                                  std::move(new_col),
                                                  nan_policy::pad_with_nans,
                                                  false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ST, template<typename> class OPT, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
scaler_operator_functor_<ST, OPT, Ts ...>::
operator()(const T &lhs_vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    auto    new_col = lhs_vec;;
    auto    opt = OPT<ValueType> { };

    for (auto &item : new_col)
        item = opt(item, static_cast<ValueType>(value));

    if (! new_col.empty())
        result_df.template load_column<ValueType>(col_name,
                                                  std::move(new_col),
                                                  nan_policy::pad_with_nans,
                                                  false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
map_missing_rows_functor_<Ts ...>::
operator()(const T &vec)  {

    const size_type vec_size = vec.size();

    for (size_type idx = 0; idx < index_rows; ++idx)  {
        if (idx >= vec_size || is_nan(vec[idx]))  {
            auto result = missing_row_map.emplace(idx, 0);

            result.first->second += 1;
        }
    }

}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
drop_missing_rows_functor_<Ts ...>::
operator()(T &vec)  {

    if (thread_level  > 2)
        futures.emplace_back(
            thr_pool_.dispatch(false,
                               &DataFrame::drop_missing_rows_<T>,
                               std::ref(vec),
                               std::cref(missing_row_map),
                               policy,
                               threshold,
                               col_num));
    else
        drop_missing_rows_(vec, missing_row_map, policy, threshold, col_num);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
get_row_functor_<Ts ...>::
operator()(const T &vec)  {

    result.template reserve<typename T::value_type>(1);
    if (row_num < vec.size())
        result.push_back(vec[row_num]);
    else
        result.push_back(get_nan<typename T::value_type>());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename DF, typename IT, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
sel_load_functor_<DF, IT, Ts ...>::
operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    StlVecType<ValueType>   new_col;
    const size_type         vec_size = vec.size();

    new_col.reserve(std::min(sel_indices.size(), vec_size));
    for (auto citer : sel_indices) [[likely]]  {
        const size_type index =
            citer >= 0 ? citer : static_cast<IT>(indices_size) + citer;

        if (index < vec_size)
            new_col.push_back(vec[index]);
        else
            break;
    }

    df.template load_column<ValueType>(name,
                                       std::move(new_col),
                                       nan_policy::dont_pad_with_nans,
                                       false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename IT, typename DF, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
sel_load_view_functor_<IT, DF, Ts ...>::
operator() (T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;
    using ViewType = typename DF::template ColumnVecType<ValueType>; 

    ViewType        new_col;
    const size_type vec_size = vec.size();

    new_col.reserve(std::min(sel_indices.size(), vec_size));
    for (auto citer : sel_indices) [[likely]]  {
        const size_type index =
            citer >= 0 ? citer : static_cast<IT>(indices_size) + citer;

        if (index < vec_size)
            new_col.push_back(&(vec[index]));
        else
            break;
    }

    using data_vec_t = typename DF::DataVec;

    dfv.data_.emplace_back(data_vec_t(std::move(new_col)));
    dfv.column_tb_.emplace (name, dfv.data_.size() - 1);
    dfv.column_list_.emplace_back (name, dfv.data_.size() - 1);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
sel_remove_functor_<Ts ...>::
operator() (T &vec) const  {

    const size_type sel_indices_s = sel_indices.size();
    const size_type vec_s = vec.size();
    size_type       del_count = 0;

    for (size_type i = 0; i < sel_indices_s; ++i)  {
        if (sel_indices[i] < vec_s)  {
            if constexpr (std::is_base_of<HeteroVector<align_value>, H>::value)
                vec.erase(vec.begin() + (sel_indices[i] - del_count++));
			else 
                vec.erase(sel_indices[i] - del_count++);
		}
        else
            break;
	}
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
shuffle_functor_<Ts ...>::
operator() (T &vec) const  {

    std::shuffle(vec.begin(), vec.end(), g_);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename DF, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
random_load_data_functor_<DF, Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const size_type vec_s = vec.size();
    const size_type n_rows = rand_indices.size();
	typename DF::template ColumnVecType<ValueType>  new_vec;
    size_type       prev_value { 0 };

    new_vec.reserve(n_rows);
    for (size_type i = 0; i < n_rows; ++i) [[likely]]  {
        if (rand_indices[i] < vec_s)  {
            if (i == 0 || rand_indices[i] != prev_value)
                new_vec.push_back(vec[rand_indices[i]]);
            prev_value = rand_indices[i];
        }
        else
            break;
    }

    df.template load_column<ValueType>(name,
                                       std::move(new_vec),
                                       nan_policy::dont_pad_with_nans,
                                       false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename DF, typename ... Ts>
template<typename T>
void DataFrame<I, H>::
random_load_view_functor_<DF, Ts ...>::operator() (const T &vec) {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;
    using ViewType = typename DF::template ColumnVecType<ValueType>;

    const size_type vec_s = vec.size();
    const size_type n_rows = rand_indices.size();
    ViewType        new_vec;
    size_type       prev_value { 0 };

    new_vec.reserve(n_rows);
    for (size_type i = 0; i < n_rows; ++i) [[likely]]  {
        if (rand_indices[i] < vec_s)  {
            if (i == 0 || rand_indices[i] != prev_value)
                new_vec.push_back(
                    const_cast<ValueType *>(&(vec[rand_indices[i]])));
            prev_value = rand_indices[i];
        }
        else
            break;
    }

    using data_vec_t = typename DF::DataVec;

    dfv.data_.emplace_back(data_vec_t(std::move(new_vec)));
    dfv.column_tb_.emplace (name, dfv.data_.size() - 1);
    dfv.column_list_.emplace_back (name, dfv.data_.size() - 1);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::columns_info_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    result.emplace_back(name, vec.size(), std::type_index(typeid(ValueType)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename DF, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::copy_remove_functor_<DF, Ts ...>::
operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;
    using NewVecType = typename DF::template ColumnVecType<ValueType>;

    NewVecType  new_vec (vec.size() - to_delete.size());

    _remove_copy_if_(vec.begin(), vec.end(), new_vec.begin(),
                     [this] (std::size_t n) -> bool  {
                         return (std::find(this->to_delete.begin(),
                                           this->to_delete.end(),
                                           n) != this->to_delete.end());
                     });
    df.template load_column<ValueType>(name,
                                       std::move(new_vec),
                                       nan_policy::dont_pad_with_nans,
                                       false);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename DF, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::fill_missing_functor_<DF, Ts ...>::operator() (T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    try  {
        const auto      &rhs_vec =
            rhs.template get_column<ValueType>(col_name, false);
        const size_type col_s = std::min(
            { vec.size(), rhs_vec.size(), self_idx.size(), rhs_idx.size() });

        for (size_type i = 0; i < col_s; ++i)
            if (is_nan(vec[i]) && self_idx[i] == rhs_idx[i])
                vec[i] = rhs_vec[i];
    }
    catch (const ColNotFound &)  {   }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::describe_functor_<Ts ...>::operator() (const T &vec)  {

    const size_type vec_s = vec.size();

    if (vec_s < 3)  return;

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    StlVecType<double>  col_to_load;

    col_to_load.reserve(describe_index_col.size());
    col_to_load.push_back(double(vec_s));

    size_type   missing_cnt =  0;
    ValueType   minv = vec[0];
    ValueType   maxv = vec[0];
    ValueType   sum = vec[0];

    if (is_nan<ValueType>(vec[0]))  missing_cnt += 1;
    for (size_type i = 1; i < vec_s; ++i)  {
        if (is_nan<ValueType>(vec[i]))
            missing_cnt += 1;
        else  {
            if (vec[i] > maxv)  maxv = vec[i];
            if (vec[i] < minv)  minv = vec[i];
            sum += vec[i];
        }
    }
    col_to_load.push_back(double(missing_cnt));
    col_to_load.push_back(sum  / double(vec_s - missing_cnt));

    StdVisitor<ValueType, ValueType>    stdev;

    stdev.pre();
    stdev(vec.begin(), vec.end(), vec.begin(), vec.end());
    stdev.post();

    col_to_load.push_back(double(stdev.get_result()));
    col_to_load.push_back(double(minv));
    col_to_load.push_back(double(maxv));

    QuantileVisitor<ValueType, ValueType>   qt25(0.25);

    qt25.pre();
    qt25(vec.begin(), vec.end(), vec.begin(), vec.end());
    qt25.post();
    col_to_load.push_back(double(qt25.get_result()));

    QuantileVisitor<ValueType, ValueType>   qt50(0.5);

    qt50.pre();
    qt50(vec.begin(), vec.end(), vec.begin(), vec.end());
    qt50.post();
    col_to_load.push_back(double(qt50.get_result()));

    QuantileVisitor<ValueType, ValueType>   qt75(0.75);

    qt75.pre();
    qt75(vec.begin(), vec.end(), vec.begin(), vec.end());
    qt75.post();
    col_to_load.push_back(double(qt75.get_result()));

    result.template load_column<double>(name,
                                        std::move(col_to_load),
                                        nan_policy::pad_with_nans,
                                        false);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
