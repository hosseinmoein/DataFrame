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
#include <DataFrame/DataFrameStatsVisitors.h>

#include <cmath>
#include <functional>
#include <random>
#include <unordered_set>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename  H>
std::pair<typename DataFrame<I, H>::size_type,
          typename DataFrame<I, H>::size_type>
DataFrame<I, H>::shape() const  {

    return (std::make_pair(indices_.size(), column_list_.size()));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
MemUsage DataFrame<I, H>::get_memory_usage(const char *col_name) const  {

    MemUsage    result;

    result.index_type_size = sizeof(IndexType);
    result.column_type_size = sizeof(T);
    _get_mem_numbers_(get_index(),
                      result.index_used_memory, result.index_capacity_memory);
    _get_mem_numbers_(get_column<T>(col_name),
                      result.column_used_memory,
                      result.column_capacity_memory);
    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::get_column (const char *name)  {

    auto    iter = column_tb_.find (name);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer, "DataFrame::get_column(): ERROR: "
                         "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    const SpinGuard guard(lock_);
    DataVec         &hv = data_[iter->second];
    auto            &data_vec = hv.template get_vector<T>();

    return (data_vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::get_column(size_type index)  {

    return (get_column<T>(column_list_[index].first.c_str()));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
bool DataFrame<I, H>::has_column (const char *name) const  {

    auto    iter = column_tb_.find (name);

    return (iter != column_tb_.end());
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
bool DataFrame<I, H>::
has_column(size_type index) const { return (index < column_list_.size()); }

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
const typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::get_column (const char *name) const  {

    return (const_cast<DataFrame *>(this)->get_column<T>(name));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
const typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::get_column(size_type index) const  {

    return (get_column<T>(column_list_[index].first.c_str()));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
const typename DataFrame<I, H>::IndexVecType &
DataFrame<I, H>::get_index() const  { return (indices_); }

// ----------------------------------------------------------------------------

template<typename I, typename  H>
typename DataFrame<I, H>::IndexVecType &
DataFrame<I, H>::get_index()  { return (indices_); }

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<size_t N, typename ... Ts>
HeteroVector DataFrame<I, H>::
get_row(size_type row_num, const std::array<const char *, N> col_names) const {

    if (row_num >= indices_.size())  {
        char buffer [512];

#ifdef _MSC_VER
        sprintf(buffer, "DataFrame::get_row(): ERROR: There aren't %zu rows",
#else
        sprintf(buffer, "DataFrame::get_row(): ERROR: There aren't %lu rows",
#endif // _MSC_VER
                row_num);
        throw BadRange(buffer);
    }

    HeteroVector ret_vec;

    ret_vec.reserve<IndexType>(1);
    ret_vec.push_back(indices_[row_num]);

    get_row_functor_<Ts ...>    functor(ret_vec, row_num);

    for (auto name_citer : col_names)  {
        const auto  citer = column_tb_.find (name_citer);

        if (citer == column_tb_.end())  {
            char buffer [512];

            sprintf(buffer,
                    "DataFrame::get_row(): ERROR: Cannot find column '%s'",
                    name_citer);
            throw ColNotFound(buffer);
        }

        const SpinGuard guard(lock_);

        data_[citer->second].change(functor);
    }

    return (ret_vec);
}




// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
HeteroVector DataFrame<I, H>::
get_row(size_type row_num) const {

    if (row_num >= indices_.size())  {
        char buffer [512];

#ifdef _MSC_VER
        sprintf(buffer, "DataFrame::get_row(): ERROR: There aren't %zu rows",
#else
        sprintf(buffer, "DataFrame::get_row(): ERROR: There aren't %lu rows",
#endif // _MSC_VER
                row_num);
        throw BadRange(buffer);
    }

    HeteroVector ret_vec;

    ret_vec.reserve<IndexType>(1);
    ret_vec.push_back(indices_[row_num]);

    get_row_functor_<Ts ...>    functor(ret_vec, row_num);

    for (auto citer : column_list_)  {
        const SpinGuard guard(lock_);

        data_[citer.second].change(functor);
    }
    return (ret_vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
std::vector<T> DataFrame<I, H>::
get_col_unique_values(const char *name) const  {

    const ColumnVecType<T>  &vec = get_column<T>(name);
    auto                    hash_func =
        [](std::reference_wrapper<const T> v) -> std::size_t  {
            return(std::hash<T>{}(v.get()));
    };
    auto                    equal_func =
        [](std::reference_wrapper<const T> lhs,
           std::reference_wrapper<const T> rhs) -> bool  {
            return(lhs.get() == rhs.get());
    };

    std::unordered_set<
        typename std::reference_wrapper<T>::type,
        decltype(hash_func),
        decltype(equal_func)>   table(vec.size(), hash_func, equal_func);
    bool                        counted_nan = false;
    std::vector<T>              result;

    result.reserve(vec.size());
    for (auto citer : vec)  {
        if (is_nan<T>(citer) && ! counted_nan)  {
            counted_nan = true;
            result.push_back(get_nan<T>());
            continue;
        }

        const auto  insert_ret = table.emplace(std::ref(citer));

        if (insert_ret.second)
            result.push_back(citer);
    }

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrame<I, H>
DataFrame<I, H>::get_data_by_idx (Index2D<IndexType> range) const  {

    const auto  &lower =
        std::lower_bound (indices_.begin(), indices_.end(), range.begin);
    const auto  &upper =
        std::upper_bound (indices_.begin(), indices_.end(), range.end);
    DataFrame   df;

    if (lower != indices_.end())  {
        df.load_index(lower, upper);

        const size_type b_dist = std::distance(indices_.begin(), lower);
        const size_type e_dist = std::distance(indices_.begin(),
                                               upper < indices_.end()
                                                   ? upper
                                                   : indices_.end());

        for (auto &iter : column_list_)  {
            load_functor_<DataFrame, Ts ...>    functor (iter.first.c_str(),
                                                         b_dist,
                                                         e_dist,
                                                         df);
            const SpinGuard                     guard(lock_);

            data_[iter.second].change(functor);
        }
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrame<I, H>
DataFrame<I, H>::get_data_by_idx(const std::vector<IndexType> &values) const  {

    const std::unordered_set<IndexType> val_table(values.begin(),
                                                  values.end());
    IndexVecType                        new_index;
    std::vector<size_type>              locations;
    const size_type                     values_s = values.size();
    const size_type                     idx_s = indices_.size();

    new_index.reserve(values_s);
    locations.reserve(values_s);
    for (size_type i = 0; i < idx_s; ++i)
        if (val_table.find(indices_[i]) != val_table.end())  {
            new_index.push_back(indices_[i]);
            locations.push_back(i);
        }

    DataFrame   df;

    df.load_index(std::move(new_index));
    for (auto col_citer : column_list_)  {
        sel_load_functor_<size_type, Ts ...>    functor (
            col_citer.first.c_str(),
            locations,
            idx_s,
            df);
        const SpinGuard                         guard(lock_);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrameView<I>
DataFrame<I, H>::get_view_by_idx (Index2D<IndexType> range) const  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_idx()");

    auto                        *nc_this = const_cast<DataFrame<I, H> *>(this);
    auto                        lower =
        std::lower_bound (nc_this->indices_.begin(),
                          nc_this->indices_.end(),
                          range.begin);
    auto                        upper =
        std::upper_bound (nc_this->indices_.begin(),
                          nc_this->indices_.end(),
                          range.end);
    DataFrameView<IndexType>    dfv;

    if (lower != indices_.end())  {
        dfv.indices_ =
            typename DataFrameView<IndexType>::IndexVecType(&*lower, &*upper);

        const size_type b_dist = std::distance(nc_this->indices_.begin(),
                                               lower);
        const size_type e_dist = std::distance(nc_this->indices_.begin(),
                                               upper < nc_this->indices_.end()
                                                   ? upper
                                                   : nc_this->indices_.end());

        for (auto &iter : nc_this->column_list_)  {
            view_setup_functor_<DataFrameView<IndexType>, Ts ...>   functor (
                iter.first.c_str(),
                b_dist,
                e_dist,
                dfv);
            const SpinGuard                                        guard(lock_);

            nc_this->data_[iter.second].change(functor);
        }
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFramePtrView<I> DataFrame<I, H>::
get_view_by_idx(const std::vector<IndexType> &values) const  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_idx()");

    using TheView = DataFramePtrView<IndexType>;

    const std::unordered_set<IndexType> val_table(values.begin(),
                                                  values.end());
    typename TheView::IndexVecType      new_index;
    std::vector<size_type>              locations;
    const size_type                     values_s = values.size();
    const size_type                     idx_s = indices_.size();

    new_index.reserve(values_s);
    locations.reserve(values_s);

    auto    *nc_this = const_cast<DataFrame<I, H> *>(this);

    for (size_type i = 0; i < idx_s; ++i)
        if (val_table.find(indices_[i]) != val_table.end())  {
            new_index.push_back(&(nc_this->indices_[i]));
            locations.push_back(i);
        }

    TheView dfv;

    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_list_)  {
        sel_load_view_functor_<size_type, Ts ...>   functor (
            col_citer.first.c_str(),
            locations,
            idx_s,
            dfv);
        const SpinGuard                             guard(lock_);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrame<I, H>
DataFrame<I, H>::get_data_by_loc (Index2D<long> range) const  {

    if (range.begin < 0)
        range.begin = static_cast<long>(indices_.size()) + range.begin;
    if (range.end < 0)
        range.end = static_cast<long>(indices_.size()) + range.end;

    if (range.end <= static_cast<long>(indices_.size()) &&
        range.begin <= range.end && range.begin >= 0)  {
        DataFrame   df;

        df.load_index(indices_.begin() + static_cast<size_type>(range.begin),
                      indices_.begin() + static_cast<size_type>(range.end));

        for (auto &iter : column_list_)  {
            load_functor_<DataFrame, Ts ...>    functor (
                iter.first.c_str(),
                static_cast<size_type>(range.begin),
                static_cast<size_type>(range.end),
                df);
            const SpinGuard                     guard(lock_);

            data_[iter.second].change(functor);
        }

        return (df);
    }

    char buffer [512];

    sprintf (buffer,
             "DataFrame::get_data_by_loc(): ERROR: "
             "Bad begin, end range: %ld, %ld",
             range.begin, range.end);
    throw BadRange (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrame<I, H>
DataFrame<I, H>::get_data_by_loc (const std::vector<long> &locations) const  {

    const size_type idx_s = indices_.size();
    DataFrame       df;
    IndexVecType    new_index;

    new_index.reserve(locations.size());
    for (const auto citer: locations)  {
        const size_type index =
            citer >= 0 ? (citer) : (citer) + static_cast<long>(idx_s);

        new_index.push_back(indices_[index]);
    }
    df.load_index(std::move(new_index));

    for (auto col_citer : column_list_)  {
        sel_load_functor_<long, Ts ...>  functor (
            col_citer.first.c_str(),
            locations,
            idx_s,
            df);
        const SpinGuard                  guard(lock_);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrameView<I>
DataFrame<I, H>::get_view_by_loc (Index2D<long> range) const  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_loc()");

    const long  idx_s = static_cast<long>(indices_.size());

    if (range.begin < 0)
        range.begin = idx_s + range.begin;
    if (range.end < 0)
        range.end = idx_s + range.end;

    if (range.end <= idx_s && range.begin <= range.end && range.begin >= 0)  {
        DataFrameView<IndexType>    dfv;
        auto                        *nc_this =
            const_cast<DataFrame<I, H> *>(this);

        dfv.indices_ =
            typename DataFrameView<IndexType>::IndexVecType(
                &*(nc_this->indices_.begin() + range.begin),
                &*(nc_this->indices_.begin() + range.end));
        for (const auto &iter : nc_this->column_list_)  {
            view_setup_functor_<DataFrameView<IndexType>, Ts ...>   functor (
                iter.first.c_str(),
                static_cast<size_type>(range.begin),
                static_cast<size_type>(range.end),
                dfv);
            const SpinGuard                                        guard(lock_);

            nc_this->data_[iter.second].change(functor);
        }

        return (dfv);
    }

    char buffer [512];

    sprintf (buffer,
             "DataFrame::get_view_by_loc(): ERROR: "
             "Bad begin, end range: %ld, %ld",
             range.begin, range.end);
    throw BadRange (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFramePtrView<I>
DataFrame<I, H>::get_view_by_loc (const std::vector<long> &locations) const  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_loc()");

    using TheView = DataFramePtrView<IndexType>;

    TheView         dfv;
    const size_type idx_s = indices_.size();

    typename TheView::IndexVecType  new_index;

    auto    *nc_this = const_cast<DataFrame<I, H> *>(this);

    new_index.reserve(locations.size());
    for (const auto citer: locations)  {
        const size_type index =
            citer >= 0 ? (citer) : (citer) + static_cast<long>(idx_s);

        new_index.push_back(&(nc_this->indices_[index]));
    }
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_list_)  {
        sel_load_view_functor_<long, Ts ...>    functor (
            col_citer.first.c_str(),
            locations,
            indices_.size(),
            dfv);
        const SpinGuard                         guard(lock_);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename F, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
get_data_by_sel (const char *name, F &sel_functor) const  {

    const ColumnVecType<T>  &vec = get_column<T>(name);
    const size_type         idx_s = indices_.size();
    const size_type         col_s = vec.size();
    std::vector<size_type>  col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i], vec[i]))
            col_indices.push_back(i);

    DataFrame       df;
    IndexVecType    new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(indices_[citer]);
    df.load_index(std::move(new_index));

    for (auto col_citer : column_list_)  {
        sel_load_functor_<size_type, Ts ...>    functor (
            col_citer.first.c_str(),
            col_indices,
            idx_s,
            df);
        const SpinGuard                         guard(lock_);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename F, typename ... Ts>
DataFramePtrView<I> DataFrame<I, H>::
get_view_by_sel (const char *name, F &sel_functor) const  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const ColumnVecType<T>  &vec = get_column<T>(name);
    const size_type         idx_s = indices_.size();
    const size_type         col_s = vec.size();
    std::vector<size_type>  col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i], vec[i]))
            col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView                         dfv;
    typename TheView::IndexVecType  new_index;

    auto    *nc_this = const_cast<DataFrame<I, H> *>(this);

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(&(nc_this->indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_list_)  {
        sel_load_view_functor_<size_type, Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            idx_s,
            dfv);
        const SpinGuard                             guard(lock_);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename F, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
get_data_by_sel (const char *name1, const char *name2, F &sel_functor) const  {

    const size_type         idx_s = indices_.size();
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2);
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s = std::max(col_s1, col_s2);
    std::vector<size_type>  col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : get_nan<T1>(),
                         i < col_s2 ? vec2[i] : get_nan<T2>()))
            col_indices.push_back(i);

    DataFrame       df;
    IndexVecType    new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(indices_[citer]);
    df.load_index(std::move(new_index));

    for (auto col_citer : column_list_)  {
        sel_load_functor_<size_type, Ts ...>    functor (
            col_citer.first.c_str(),
            col_indices,
            idx_s,
            df);
        const SpinGuard                         guard(lock_);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename F, typename ... Ts>
DataFramePtrView<I> DataFrame<I, H>::
get_view_by_sel (const char *name1, const char *name2, F &sel_functor) const  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const ColumnVecType<T1> &vec1 = get_column<T1>(name1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2);
    const size_type         idx_s = indices_.size();
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s = std::max(col_s1, col_s2);
    std::vector<size_type>  col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : get_nan<T1>(),
                         i < col_s2 ? vec2[i] : get_nan<T2>()))
            col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView                         dfv;
    typename TheView::IndexVecType  new_index;

    auto    *nc_this = const_cast<DataFrame<I, H> *>(this);

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(&(nc_this->indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_list_)  {
        sel_load_view_functor_<size_type, Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            idx_s,
            dfv);
        const SpinGuard                             guard(lock_);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename F, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
get_data_by_sel (const char *name1,
                 const char *name2,
                 const char *name3,
                 F &sel_functor) const  {

    const size_type         idx_s = indices_.size();
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3);
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s3 = vec3.size();
    const size_type         col_s = std::max(std::max(col_s1, col_s2), col_s3);
    std::vector<size_type>  col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : get_nan<T1>(),
                         i < col_s2 ? vec2[i] : get_nan<T2>(),
                         i < col_s3 ? vec3[i] : get_nan<T3>()))
            col_indices.push_back(i);

    DataFrame       df;
    IndexVecType    new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(indices_[citer]);
    df.load_index(std::move(new_index));

    for (auto col_citer : column_list_)  {
        sel_load_functor_<size_type, Ts ...>    functor (
            col_citer.first.c_str(),
            col_indices,
            idx_s,
            df);
        const SpinGuard                         guard(lock_);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename F, typename ... Ts>
DataFramePtrView<I> DataFrame<I, H>::
get_view_by_sel (const char *name1,
                 const char *name2,
                 const char *name3,
                 F &sel_functor) const  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const ColumnVecType<T1> &vec1 = get_column<T1>(name1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3);
    const size_type         idx_s = indices_.size();
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s3 = vec3.size();
    const size_type         col_s = std::max(std::max(col_s1, col_s2), col_s3);
    std::vector<size_type>  col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : get_nan<T1>(),
                         i < col_s2 ? vec2[i] : get_nan<T2>(),
                         i < col_s3 ? vec3[i] : get_nan<T3>()))
            col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView                         dfv;
    typename TheView::IndexVecType  new_index;

    auto    *nc_this = const_cast<DataFrame<I, H> *>(this);

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(&(nc_this->indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_list_)  {
        sel_load_view_functor_<size_type, Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            idx_s,
            dfv);
        const SpinGuard                             guard(lock_);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
get_data_by_rand (random_policy spec, double n, size_type seed) const  {

    bool            use_seed = false;
    size_type       n_rows = static_cast<size_type>(n);
    const size_type index_s = indices_.size();

    if (spec == random_policy::num_rows_with_seed)  {
        use_seed = true;
    }
    else if (spec == random_policy::frac_rows_with_seed)  {
        use_seed = true;
        n_rows = static_cast<size_type>(n * double(index_s));
    }
    else if (spec == random_policy::frac_rows_no_seed)  {
        n_rows = static_cast<size_type>(n * double(index_s));
    }

    if (index_s > 0 && n_rows < index_s - 1)  {
        std::random_device  rd;
        std::mt19937        gen(rd());

        if (use_seed)  gen.seed(static_cast<unsigned int>(seed));

        std::uniform_int_distribution<size_type>    dis(0, index_s - 1);
        std::vector<size_type>                      rand_indices(n_rows);

        for (size_type i = 0; i < n_rows; ++i)
            rand_indices[i] = dis(gen);
        std::sort(rand_indices.begin(), rand_indices.end());

        IndexVecType    new_index;
        size_type       prev_value;

        new_index.reserve(n_rows);
        for (size_type i = 0; i < n_rows; ++i)  {
            if (i == 0 || rand_indices[i] != prev_value)
                new_index.push_back(indices_[rand_indices[i]]);
            prev_value = rand_indices[i];
        }

        DataFrame   df;

        df.load_index(std::move(new_index));
        for (auto &iter : column_list_)  {
            random_load_data_functor_<Ts ...>   functor (
                iter.first.c_str(),
                rand_indices,
                df);
            const SpinGuard                     guard(lock_);

            data_[iter.second].change(functor);
        }

        return (df);
    }

    char buffer [512];

    sprintf (buffer,
             "DataFrame::get_data_by_rand(): ERROR: "
#ifdef _MSC_VER
             "Number of rows requested %zu is more than available rows %zu",
#else
             "Number of rows requested %lu is more than available rows %lu",
#endif // _MSC_VER
             n_rows, index_s);
    throw BadRange (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFramePtrView<I> DataFrame<I, H>::
get_view_by_rand (random_policy spec, double n, size_type seed) const  {

    bool            use_seed = false;
    size_type       n_rows = static_cast<size_type>(n);
    const size_type index_s = indices_.size();

    if (spec == random_policy::num_rows_with_seed)  {
        use_seed = true;
    }
    else if (spec == random_policy::frac_rows_with_seed)  {
        use_seed = true;
        n_rows = static_cast<size_type>(n * double(index_s));
    }
    else if (spec == random_policy::frac_rows_no_seed)  {
        n_rows = static_cast<size_type>(n * double(index_s));
    }

    if (index_s > 0 && n_rows < index_s - 1)  {
        std::random_device  rd;
        std::mt19937        gen(rd());

        if (use_seed)  gen.seed(static_cast<unsigned int>(seed));

        std::uniform_int_distribution<size_type>    dis(0, index_s - 1);
        std::vector<size_type>                      rand_indices(n_rows);

        for (size_type i = 0; i < n_rows; ++i)
            rand_indices[i] = dis(gen);
        std::sort(rand_indices.begin(), rand_indices.end());

        using TheView = DataFramePtrView<IndexType>;

        typename TheView::IndexVecType  new_index;
        size_type                       prev_value;

        new_index.reserve(n_rows);
        for (size_type i = 0; i < n_rows; ++i)  {
            if (i == 0 || rand_indices[i] != prev_value)
                new_index.push_back(
                    const_cast<I *>(&(indices_[rand_indices[i]])));
            prev_value = rand_indices[i];
        }

        TheView dfv;

        dfv.indices_ = std::move(new_index);
        for (auto &iter : column_list_)  {
            random_load_view_functor_<Ts ...>   functor (iter.first.c_str(),
                                                         rand_indices,
                                                         dfv);
            const SpinGuard                     guard(lock_);

            data_[iter.second].change(functor);
        }

        return (dfv);
    }

    char buffer [512];

    sprintf (buffer,
             "DataFrame::get_view_by_rand(): ERROR: "
#ifdef _MSC_VER
             "Number of rows requested %zu is more than available rows %zu",
#else
             "Number of rows requested %lu is more than available rows %lu",
#endif // _MSC_VER
             n_rows, index_s);
    throw BadRange (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename ... Ts>
StdDataFrame<T> DataFrame<I, H>::
get_reindexed(const char *col_to_be_index, const char *old_index_name) const  {

    StdDataFrame<T> result;
    const auto      &new_idx = get_column<T>(col_to_be_index);
    const size_type new_idx_s =
        result.load_index(new_idx.begin(), new_idx.end());

    if (old_index_name)  {
        const auto      &curr_idx = get_index();
        const size_type col_s =
            curr_idx.size() >= new_idx_s ? new_idx_s : curr_idx.size();

        result.template load_column<IndexType>(
            old_index_name, { curr_idx.begin(), curr_idx.begin() + col_s });
    }

    for (auto citer : column_list_)  {
        if (citer.first == col_to_be_index)  continue;

        load_functor_<StdDataFrame<T>, Ts ...>  functor (
            citer.first.c_str(),
            0,
            new_idx_s,
            result,
            nan_policy::dont_pad_with_nans);
        const SpinGuard                         guard(lock_);

        data_[citer.second].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename ... Ts>
DataFrameView<T> DataFrame<I, H>::
get_reindexed_view(const char *col_to_be_index,
                   const char *old_index_name) const  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_reindexed_view()");

    auto                *nc_this = const_cast<DataFrame<I, H> *>(this);
    DataFrameView<T>    result;
    auto                &new_idx =
        nc_this->template get_column<T>(col_to_be_index);
    const size_type     new_idx_s = new_idx.size();

    result.indices_ = typename DataFrameView<T>::IndexVecType();
    result.indices_.set_begin_end_special(&*(new_idx.begin()),
                                          &(new_idx.back()));
    if (old_index_name)  {
        auto            &curr_idx = nc_this->get_index();
        const size_type col_s =
            curr_idx.size() >= new_idx_s ? new_idx_s : curr_idx.size();

        result.template setup_view_column_<IndexType,
                                           typename IndexVecType::iterator>
            (old_index_name, { curr_idx.begin(), curr_idx.begin() + col_s });
    }

    for (auto citer : column_list_)  {
        if (citer.first == col_to_be_index)  continue;

        view_setup_functor_<DataFrameView<T>, Ts ...>   functor (
            citer.first.c_str(),
            0,
            new_idx_s,
            result);
        const SpinGuard                                 guard(lock_);

        nc_this->data_[citer.second].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
std::vector<std::tuple<typename DataFrame<I, H>::ColNameType,
                       typename DataFrame<I, H>::size_type,
                       std::type_index>>
DataFrame<I, H>::get_columns_info () const  {

    std::vector<std::tuple<ColNameType, size_type, std::type_index>> result;

    result.reserve(column_list_.size());
    for (auto &citer : column_list_)  {
        columns_info_functor_<Ts ...>   functor (result, citer.first.c_str());
        const SpinGuard                 guard(lock_);

        data_[citer.second].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
bool DataFrame<I, H>::
pattern_match(const char *col_name,
              pattern_spec pattern,
              double epsilon) const  {

    const auto  &col = get_column<T>(col_name);

    switch(pattern)  {
    case pattern_spec::monotonic_increasing:
        return (is_monotonic_increasing(col));
    case pattern_spec::strictly_monotonic_increasing:
        return (is_strictly_monotonic_increasing(col));
    case pattern_spec::monotonic_decreasing:
        return (is_monotonic_decreasing(col));
    case pattern_spec::strictly_monotonic_decreasing:
        return (is_strictly_monotonic_decreasing(col));
    case pattern_spec::normally_distributed:
        return (is_normal(col, epsilon, false));
    case pattern_spec::standard_normally_distributed:
        return (is_normal(col, epsilon, true));
    case pattern_spec::lognormally_distributed:
        return (is_lognormal(col, epsilon));
    default:
        throw NotImplemented("pattern_match(): "
                             "Requested pattern is not implemented");
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename DF, typename F>
std::vector<T> DataFrame<I, H>::
combine(const char *col_name, const DF &rhs, F &functor) const  {

    const auto      &lhs_col = get_column<T>(col_name);
    const auto      &rhs_col = rhs.template get_column<T>(col_name);
    const size_type col_s = std::min(lhs_col.size(), rhs_col.size());
    std::vector<T>  result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i)
        result.push_back(std::move(functor(lhs_col[i], rhs_col[i])));

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename DF1, typename DF2, typename F>
std::vector<T> DataFrame<I, H>::
combine(const char *col_name,
        const DF1 &df1,
        const DF2 &df2,
        F &functor) const  {

    const auto      &lhs_col = get_column<T>(col_name);
    const auto      &df1_col = df1.template get_column<T>(col_name);
    const auto      &df2_col = df2.template get_column<T>(col_name);
    const size_type col_s =
        std::min<size_type>({ lhs_col.size(), df1_col.size(), df2_col.size() });
    std::vector<T>  result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i)
        result.push_back(
            std::move(functor(lhs_col[i], df1_col[i], df2_col[i])));

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename DF1, typename DF2, typename DF3, typename F>
std::vector<T> DataFrame<I, H>::
combine(const char *col_name,
        const DF1 &df1,
        const DF2 &df2,
        const DF3 &df3,
        F &functor) const  {

    const auto      &lhs_col = get_column<T>(col_name);
    const auto      &df1_col = df1.template get_column<T>(col_name);
    const auto      &df2_col = df2.template get_column<T>(col_name);
    const auto      &df3_col = df3.template get_column<T>(col_name);
    const size_type col_s = std::min<size_type>(
        { lhs_col.size(), df1_col.size(), df2_col.size(), df3_col.size() });
    std::vector<T>  result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i)
        result.push_back(
            std::move(functor(lhs_col[i], df1_col[i], df2_col[i], df3_col[i])));

    return (result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
