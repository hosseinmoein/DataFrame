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

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <random>

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
template<typename T2>
void
DataFrame<I, H>::sort_functor_<Ts ...>::operator() (T2 &vec)  {

    sorted_idxs_copy = sorted_idxs;
    _sort_by_sorted_index_(vec, sorted_idxs_copy, idx_s);
    return;
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
        nan_p);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::load_all_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    df.load_column<ValueType>(name, { vec.begin(), vec.end() },
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
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::print_csv_functor_<Ts ...>::operator() (const T &vec)  {

    if (vec.empty())  return;

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    os << name << ':' << vec.size() << ':';
    if (typeid(ValueType) == typeid(float))
        os << "<float>:";
    else if (typeid(ValueType) == typeid(double))
        os << "<double>:";
    else if (typeid(ValueType) == typeid(short int))
        os << "<short>:";
    else if (typeid(ValueType) == typeid(unsigned short int))
        os << "<ushort>:";
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
DataFrame<I, H>::print_json_functor_<Ts ...>::operator() (const T &vec)  {

    if (vec.empty())  return;

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    if (need_pre_comma)
        os << ",\n";

    os << '"' << name << "\":{\"N\":" << vec.size() << ',';
    if (typeid(ValueType) == typeid(float))
        os << "\"T\":\"float\",";
    else if (typeid(ValueType) == typeid(double))
        os << "\"T\":\"double\",";
    else if (typeid(ValueType) == typeid(short int))
        os << "\"T\":\"short\",";
    else if (typeid(ValueType) == typeid(unsigned short int))
        os << "\"T\":\"ushort\",";
    else if (typeid(ValueType) == typeid(int))
        os << "\"T\":\"int\",";
    else if (typeid(ValueType) == typeid(unsigned int))
        os << "\"T\":\"uint\",";
    else if (typeid(ValueType) == typeid(long int))
        os << "\"T\":\"long\",";
    else if (typeid(ValueType) == typeid(long long int))
        os << "\"T\":\"longlong\",";
    else if (typeid(ValueType) == typeid(unsigned long int))
        os << "\"T\":\"ulong\",";
    else if (typeid(ValueType) == typeid(unsigned long long int))
        os << "\"T\":\"ulonglong\",";
    else if (typeid(ValueType) == typeid(std::string))
        os << "\"T\":\"string\",";
    else if (typeid(ValueType) == typeid(bool))
        os << "\"T\":\"bool\",";
    else if (typeid(ValueType) == typeid(DateTime))
        os << "\"T\":\"DateTime\",";
    else
        os << "\"T\":\"N/A\",";

    os << "\"D\":[";
    if (! vec.empty())  {
        _write_json_df_index_(os, vec[0]);
        for (std::size_t i = 1; i < vec.size(); ++i)  {
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

    _write_csv2_df_header_<S, ValueType>(os, name, vec.size());
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ... Ts>
template<typename T>
void DataFrame<I, H>::
print_csv2_data_functor_<S, Ts ...>::operator() (const T &vec)  {

    if (vec.size() > index)  os << vec[index];
    return;
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

    if (iter == df.column_tb_.end())  {
        result = false;
        return;
    }

    const DataVec   &hv = df.data_[iter->second];
    SpinGuard       guard(lock_);
    const T         &rhs_vec = hv.template get_vector<ValueType>();

    guard.release();
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
            rhs_df.get_column<ValueType>(name);

        lhs_vec[lhs_idx] = rhs_vec[rhs_idx];
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RES_T, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
index_join_functor_common_<RES_T, Ts ...>::operator()(const T &lhs_vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const ColumnVecType<ValueType>  &rhs_vec = rhs.get_column<ValueType>(name);
    std::vector<ValueType>          lhs_result_col;
    std::vector<ValueType>          rhs_result_col;

    lhs_result_col.reserve(joined_index_idx.size());
    rhs_result_col.reserve(joined_index_idx.size());
    for (auto &citer : joined_index_idx)  {
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

    ::sprintf(lhs_str, "lhs.%s", name);
    ::sprintf(rhs_str, "rhs.%s", name);
    result.load_column(lhs_str, std::move(lhs_result_col));
    result.load_column(rhs_str, std::move(rhs_result_col));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<int SIDE, typename RES_T, typename ... Ts>
template<typename T>
void DataFrame<I, H>::index_join_functor_oneside_<SIDE, RES_T, Ts ...>::
operator()(const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    std::vector<ValueType>  result_col;

    result_col.reserve(joined_index_idx.size());
    for (auto &citer : joined_index_idx)  {
        const size_type i = std::get<SIDE>(citer);

        result_col.push_back(
            i != std::numeric_limits<size_type>::max()
                ? vec[i] : get_nan<ValueType>());
    }

    result.load_column(name, std::move(result_col));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RES_T, typename ... Ts>
template<typename T>
void DataFrame<I, H>::concat_functor_<RES_T, Ts ...>::
operator()(const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    if (insert_col)  {
        std::vector<ValueType>  res_vec(original_index_s + vec.size(),
                                        get_nan<ValueType>());

        std::copy(vec.begin(), vec.end(), res_vec.begin() + original_index_s);
        result.load_column(name, res_vec);
    }
    else  {
        std::vector<ValueType>  &res_vec =
            result.template get_column<ValueType>(name);

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

    const DataVec   &rhs_hv = rhs_df.data_[rhs_citer->second];
    SpinGuard       guard(lock_);
    const auto      &rhs_vec = rhs_hv.template get_vector<ValueType>();

    guard.release();

    const size_type new_col_size =
        std::min(std::min(lhs_vec.size(), rhs_vec.size()), new_idx.size());
    std::vector<ValueType>  new_col;
    auto                    Operator = OPT<ValueType>();
    size_type               lcounter = 0;
    size_type               rcounter = 0;

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

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
drop_missing_rows_functor_<Ts ...>::
operator()(T &vec)  {

    drop_missing_rows_(vec, missing_row_map, policy, threshold, col_num);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
get_row_functor_<Ts ...>::
operator()(const T &vec)  {

    result.reserve<typename T::value_type>(1);
    if (row_num < vec.size())
        result.push_back(vec[row_num]);
    else
        result.push_back(get_nan<typename T::value_type>());
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename IT, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
sel_load_functor_<IT, Ts ...>::
operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    std::vector<ValueType>  new_col;
    const size_type         vec_size = vec.size();

    new_col.reserve(std::min(sel_indices.size(), vec_size));
    for (const auto citer : sel_indices)  {
        const size_type index =
            citer >= 0 ? citer : static_cast<IT>(indices_size) + citer;

        if (index < vec_size)
            new_col.push_back(vec[index]);
        else
            break;
    }
    df.load_column(name, std::move(new_col), nan_policy::dont_pad_with_nans);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename IT, typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
sel_load_view_functor_<IT, Ts ...>::
operator() (T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    VectorPtrView<ValueType>    new_col;
    const size_type             vec_size = vec.size();

    new_col.reserve(std::min(sel_indices.size(), vec_size));
    for (const auto citer : sel_indices)  {
        const size_type index =
            citer >= 0 ? citer : static_cast<IT>(indices_size) + citer;

        if (index < vec_size)
            new_col.push_back(&(vec[index]));
        else
            break;
    }

    using data_vec_t = typename DataFramePtrView<I>::DataVec;

    SpinGuard   guard(lock_);

    dfv.data_.emplace_back(data_vec_t(std::move(new_col)));
    guard.release();
    dfv.column_tb_.emplace (name, dfv.data_.size() - 1);
    dfv.column_list_.emplace_back (name, dfv.data_.size() - 1);
    return;
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
        const size_type max_s =
            std::min(result_vec.size() + rhs_s, result.get_index().size());
        size_type       count { vec.size() };

        result_vec.reserve(max_s);
        for (size_type i = 0; i < rhs_s && count < max_s; ++i, ++count)
            result_vec.push_back(const_cast<ValueType *>(&(vec[i])));
    }
    else  {
        T   &nc_vec = const_cast<T &>(vec);
		
        result.template setup_view_column_<ValueType,
                                           typename VecType::iterator>(
            name, { nc_vec.begin(), nc_vec.end() });
    }

    return;
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

    for (size_type i = 0; i < sel_indices_s; ++i)
        if (sel_indices[i] < vec_s)
            vec.erase(vec.begin() + (sel_indices[i] - del_count++));
        else
            break;
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
shuffle_functor_<Ts ...>::
operator() (T &vec) const  {

    std::random_device  rd;
    std::mt19937        g(rd());

    std::shuffle(vec.begin(), vec.end(), g);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::
random_load_data_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const size_type vec_s = vec.size();
    const size_type n_rows = rand_indices.size();
    VecType         new_vec;
    size_type       prev_value;

    new_vec.reserve(n_rows);
    for (size_type i = 0; i < n_rows; ++i)  {
        if (rand_indices[i] < vec_s)  {
            if (i == 0 || rand_indices[i] != prev_value)
                new_vec.push_back(vec[rand_indices[i]]);
            prev_value = rand_indices[i];
        }
        else
            break;
    }

    df.load_column<ValueType>(name,
                              std::move(new_vec),
                              nan_policy::dont_pad_with_nans);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::random_load_view_functor_<Ts ...>::operator() (const T &vec) {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const size_type             vec_s = vec.size();
    const size_type             n_rows = rand_indices.size();
    VectorPtrView<ValueType>    new_vec;
    size_type                   prev_value;

    new_vec.reserve(n_rows);
    for (size_type i = 0; i < n_rows; ++i)  {
        if (rand_indices[i] < vec_s)  {
            if (i == 0 || rand_indices[i] != prev_value)
                new_vec.push_back(
                    const_cast<ValueType *>(&(vec[rand_indices[i]])));
            prev_value = rand_indices[i];
        }
        else
            break;
    }

    using data_vec_t = typename DataFramePtrView<I>::DataVec;

    SpinGuard   guard(lock_);

    dfv.data_.emplace_back(data_vec_t(std::move(new_vec)));
    guard.release();
    dfv.column_tb_.emplace (name, dfv.data_.size() - 1);
    dfv.column_list_.emplace_back (name, dfv.data_.size() - 1);
    return;
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
template<typename ... Ts>
template<typename T>
void
DataFrame<I, H>::copy_remove_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    VecType new_vec (vec.size() - to_delete.size());

    _remove_copy_if_(vec.begin(), vec.end(), new_vec.begin(),
                     [this] (std::size_t n) -> bool  {
                         return (std::find(this->to_delete.begin(),
                                           this->to_delete.end(),
                                           n) != this->to_delete.end());
                     });
    df.load_column<ValueType>(name,
                              std::move(new_vec),
                              nan_policy::dont_pad_with_nans);
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
        const auto      &rhs_vec = rhs.template get_column<ValueType>(col_name);
        const size_type col_s = std::min(
            { vec.size(), rhs_vec.size(), self_idx.size(), rhs_idx.size() });

        for (size_type i = 0; i < col_s; ++i)
            if (is_nan(vec[i]) && self_idx[i] == rhs_idx[i])
                vec[i] = rhs_vec[i];
    }
    catch (const ColNotFound &)  {   }
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
