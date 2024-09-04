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
#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/Utils/Utils.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
std::pair<typename DataFrame<I, H>::size_type,
          typename DataFrame<I, H>::size_type>
DataFrame<I, H>::shape() const  {

    return (std::make_pair(indices_.size(), column_list_.size()));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
MemUsage DataFrame<I, H>::get_memory_usage(const char *col_name) const  {

    MemUsage    result;

    result.index_type_size = sizeof(IndexType);
    result.column_type_size = sizeof(T);
    get_mem_numbers_(get_index(),
                     result.index_used_memory, result.index_capacity_memory);
    get_mem_numbers_(get_column<T>(col_name),
                     result.column_used_memory,
                     result.column_capacity_memory);
    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::col_name_to_idx (const char *col_name) const  {

    for (const auto &[name, idx] : column_list_) [[likely]]
        if (name == col_name)
            return (idx);

    char buffer [512];

    snprintf (buffer, sizeof(buffer) - 1,
              "DataFrame::col_name_to_idx(): ERROR: Cannot find column '%s'",
              col_name);
    throw ColNotFound (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
const char *
DataFrame<I, H>::col_idx_to_name (size_type col_idx) const  {

    for (const auto &[name, idx] : column_list_) [[likely]]
        if (idx == col_idx)
            return (name.c_str());

    char buffer [512];

    snprintf (buffer, sizeof(buffer) - 1,
              "DataFrame::col_idx_to_name(): ERROR: "
#ifdef _MSC_VER
              "Cannot find column index %zu",
#else
              "Cannot find column index %lu",
#endif // _MSC_VER
              col_idx);
    throw ColNotFound (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::get_column (const char *name, bool do_lock)  {

    const auto  iter = column_tb_.find (name);

    if (iter != column_tb_.end()) [[likely]]  {
        const SpinGuard guard (do_lock ? lock_ : nullptr);
        DataVec         &hv = data_[iter->second];
        auto            &data_vec = hv.template get_vector<T>();

        return (data_vec);
    }

    char buffer [512];

    snprintf (buffer, sizeof(buffer) - 1,
              "DataFrame::get_column(): ERROR: Cannot find column '%s'",
              name);
    throw ColNotFound (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::template ColumnVecType<typename T::type> &
DataFrame<I, H>::get_column ()  {

    return (get_column<typename T::type>(T::name));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::get_column(size_type index, bool do_lock)  {

    return (get_column<T>(column_list_[index].first.c_str(), do_lock));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool DataFrame<I, H>::has_column (const char *name) const  {

    return (column_tb_.contains(name));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool DataFrame<I, H>::
has_column(size_type index) const { return (index < column_list_.size()); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
const typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::get_column (const char *name, bool do_lock) const  {

    return (const_cast<DataFrame *>(this)->get_column<T>(name, do_lock));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
const typename DataFrame<I, H>::template ColumnVecType<typename T::type> &
DataFrame<I, H>::get_column () const  {

    return (const_cast<DataFrame *>(this)->get_column<typename T::type>(
                T::name));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
const typename DataFrame<I, H>::template ColumnVecType<T> &
DataFrame<I, H>::get_column(size_type index, bool do_lock) const  {

    return (get_column<T>(column_list_[index].first.c_str(), do_lock));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
const typename DataFrame<I, H>::IndexVecType &
DataFrame<I, H>::get_index() const  { return (indices_); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename DataFrame<I, H>::IndexVecType &
DataFrame<I, H>::get_index()  { return (indices_); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
HeteroVector<std::size_t(H::align_value)> DataFrame<I, H>::
get_row(size_type row_num, const StlVecType<const char *> &col_names) const {

    if (row_num < indices_.size()) [[likely]]  {
        HeteroVector<align_value>   ret_vec;

        ret_vec.template reserve<IndexType>(1);
        ret_vec.push_back(indices_[row_num]);

        get_row_functor_<Ts ...>    functor(ret_vec, row_num);
        const SpinGuard             guard(lock_);

        for (const auto &name_citer : col_names) [[likely]]  {
            const auto  &citer = column_tb_.find (name_citer);

            if (citer == column_tb_.end()) [[unlikely]]  {
                char buffer [512];

                snprintf(buffer, sizeof(buffer) - 1,
                        "DataFrame::get_row(): ERROR: Cannot find column '%s'",
                         name_citer);
                throw ColNotFound(buffer);
            }

            data_[citer->second].change(functor);
        }

        return (ret_vec);
    }

    char buffer [512];

#ifdef _MSC_VER
    snprintf(buffer, sizeof(buffer) - 1,
             "DataFrame::get_row(): ERROR: There aren't %zu rows",
#else
    snprintf(buffer, sizeof(buffer) - 1,
             "DataFrame::get_row(): ERROR: There aren't %lu rows",
#endif // _MSC_VER
             row_num);
    throw BadRange(buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
HeteroVector<std::size_t(H::align_value)> DataFrame<I, H>::
get_row(size_type row_num) const {

    if (row_num < indices_.size()) [[likely]]  {
        HeteroVector<align_value>   ret_vec;

        ret_vec.template reserve<IndexType>(1);
        ret_vec.push_back(indices_[row_num]);

        get_row_functor_<Ts ...>    functor(ret_vec, row_num);
        const SpinGuard             guard(lock_);

        for (const auto &citer : column_list_) [[likely]]
            data_[citer.second].change(functor);

        return (ret_vec);
    }

    char buffer [512];

#ifdef _MSC_VER
    snprintf(buffer, sizeof(buffer) - 1,
             "DataFrame::get_row(): ERROR: There aren't %zu rows",
#else
    snprintf(buffer, sizeof(buffer) - 1,
             "DataFrame::get_row(): ERROR: There aren't %lu rows",
#endif // _MSC_VER
             row_num);
    throw BadRange(buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T>
typename DataFrame<I, H>::template StlVecType<T> DataFrame<I, H>::
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

    using unset_t = DFUnorderedSet<typename std::reference_wrapper<T>::type,
                                   decltype(hash_func),
                                   decltype(equal_func)>;

    unset_t         table (vec.size(), hash_func, equal_func);
    bool            counted_nan = false;
    StlVecType<T>   result;

    result.reserve(vec.size() / 2);
    for (const auto &citer : vec) [[likely]]  {
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

template<typename I, typename H>
template<hashable_equal ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
duplication_mask (bool include_index, bool binary) const  {

    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    res_t   new_df;

    new_df.load_index(indices_.begin(), indices_.end());

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]
        new_df.template create_column<int>(name.c_str(), false);

    const auto  thread_level =
        (indices_.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();
    auto        lbd =
        [&new_df, this, include_index, binary]
        (const auto &begin, const auto &end) -> void  {
            for (auto citer = begin; citer < end; ++citer)  {
                dup_mask_functor_<Ts ...>   functor(citer->first.c_str(),
                                                    new_df,
                                                    new_df.indices_,
                                                    include_index,
                                                    binary);

                this->data_[citer->second].change(functor);
            }
        };

    if (thread_level > 2)  {
        auto    futuers = thr_pool_.parallel_loop(column_list_.begin(),
                                                  column_list_.end(),
                                                  std::move(lbd));

        for (auto &fut : futuers)  fut.get();
    }
    else  {
        lbd(column_list_.begin(), column_list_.end());
    }

    return (new_df);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
get_reindexed(const char *col_to_be_index, const char *old_index_name) const  {

    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    res_t           df;
    const auto      &new_idx = get_column<T>(col_to_be_index);
    const size_type new_idx_s = df.load_index(new_idx.begin(), new_idx.end());

    if (old_index_name)  {
        const auto      &curr_idx = get_index();
        const size_type col_s =
            curr_idx.size() >= new_idx_s ? new_idx_s : curr_idx.size();

        df.template load_column<IndexType>(
            old_index_name, { curr_idx.begin(), curr_idx.begin() + col_s });
    }

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        if (name == col_to_be_index)  continue;

        create_col_functor_<res_t, Ts ...>  functor (name.c_str(), df);

        data_[idx].change(functor);
    }

    const auto  thread_level =
        (new_idx_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (thread_level > 2)  {
        auto    lbd =
            [col_to_be_index = std::as_const(col_to_be_index),
             new_idx_s, &df, this]
            (const auto &begin, const auto &end) -> void  {
                for (auto citer = begin; citer < end; ++citer)  {
                    if (citer->first == col_to_be_index)  continue;

                    load_functor_<res_t, Ts ...>    functor(
                        citer->first.c_str(),
                        0,
                        new_idx_s,
                        df,
                        nan_policy::dont_pad_with_nans);

                    this->data_[citer->second].change(functor);
                }
            };

        auto    futuers =
            thr_pool_.parallel_loop(column_list_.begin(),
                                    column_list_.end(),
                                    std::move(lbd));

        for (auto &fut : futuers)  fut.get();
    }
    else  {
        for (const auto &[name, idx] : column_list_) [[likely]]  {
            if (name == col_to_be_index)  continue;

            load_functor_<res_t, Ts ...>    functor(
                name.c_str(),
                0,
                new_idx_s,
                df,
                nan_policy::dont_pad_with_nans);

            data_[idx].change(functor);
        }
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ... Ts>
typename DataFrame<T, H>::View DataFrame<I, H>::
get_reindexed_view(const char *col_to_be_index, const char *old_index_name)  {

    using result_t = typename DataFrame<T, H>::View;

    result_t        result;
    auto            &new_idx = get_column<T>(col_to_be_index);
    const size_type new_idx_s = new_idx.size();

    result.indices_ = typename result_t::IndexVecType();
    result.indices_.set_begin_end_special(&(new_idx.front()),
                                          &(new_idx.back()));
    if (old_index_name)  {
        auto            &curr_idx = get_index();
        const size_type col_s =
            curr_idx.size() >= new_idx_s ? new_idx_s : curr_idx.size();

        result.template setup_view_column_<IndexType,
                                           typename IndexVecType::iterator>
            (old_index_name, { curr_idx.begin(), curr_idx.begin() + col_s });
    }

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        if (name == col_to_be_index)  continue;

        view_setup_functor_<result_t, Ts ...>   functor (name.c_str(),
                                                         0,
                                                         new_idx_s,
                                                         result);

        data_[idx].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ... Ts>
typename DataFrame<T, H>::ConstView
DataFrame<I, H>::
get_reindexed_view(const char *col_to_be_index,
                   const char *old_index_name) const  {

    using result_t = typename DataFrame<T, H>::ConstView;

    result_t        result;
    const auto      &new_idx = get_column<T>(col_to_be_index);
    const size_type new_idx_s = new_idx.size();

    result.indices_ = typename result_t::IndexVecType();
    result.indices_.set_begin_end_special(&(new_idx.front()),
                                          &(new_idx.back()));
    if (old_index_name)  {
        auto            &curr_idx = get_index();
        const size_type col_s =
            curr_idx.size() >= new_idx_s ? new_idx_s : curr_idx.size();

        result.template setup_view_column_<
                IndexType,
                typename IndexVecType::const_iterator>
            (old_index_name, { curr_idx.begin(), curr_idx.begin() + col_s });
    }

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        if (name == col_to_be_index)  continue;

        view_setup_functor_<result_t, Ts ...>   functor (name.c_str(),
                                                         0,
                                                         new_idx_s,
                                                         result);

        data_[idx].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
change_freq(size_type new_freq,
            [[maybe_unused]] time_frequency time_unit) const  {

    static_assert(numeric_or_DateTime<I>,
                  "convert_freq(): "
                  "Index type must be either a numeric or DateTime");

    DataFrame       result;
    IndexVecType    new_idx;

    if constexpr (std::is_same_v<I, DateTime>)  {
#ifdef HMDF_SANITY_EXCEPTIONS
        if (time_unit == time_frequency::not_valid)
            throw NotFeasible(
                "convert_freq(): "
                "Index type of DateTime must have a valid time unit");
#endif // HMDF_SANITY_EXCEPTIONS
        new_idx =
            gen_datetime_index(
                indices_.front().string_format(DT_FORMAT::DT_TM2).c_str(),
                indices_.back().string_format(DT_FORMAT::DT_TM2).c_str(),
                time_unit,
                long(new_freq),
                indices_.front().get_timezone());
    }
    else  {
#ifdef HMDF_SANITY_EXCEPTIONS
        if (time_unit != time_frequency::not_valid)
            throw NotFeasible(
                "convert_freq(): "
                "Index type of numeric must have a not_valid time unit");
#endif // HMDF_SANITY_EXCEPTIONS

        new_idx =
            gen_sequence_index(indices_.front(),
                               indices_.back(),
                               long(new_freq));
    }
    new_idx.push_back(indices_.back());
    result.load_index(std::move(new_idx));

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        change_freq_functor_<Ts ...>    functor (
            name.c_str(), result, get_index());

        data_[idx].change(functor);
    }

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
typename DataFrame<I, H>::template StlVecType<
    std::tuple<typename DataFrame<I, H>::ColNameType,
               typename DataFrame<I, H>::size_type,
               std::type_index>>
DataFrame<I, H>::get_columns_info () const  {

    StlVecType<std::tuple<ColNameType, size_type, std::type_index>> result;

    result.reserve(column_list_.size());

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        columns_info_functor_<Ts ...>   functor (result, name.c_str());

        data_[idx].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
DataFrame<std::string, H>
DataFrame<I, H>::describe() const  {

    DataFrame<std::string, HeteroVector<align_value>>   result;

    result.load_index(describe_index_col.begin(), describe_index_col.end());

    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        describe_functor_<Ts ...>   functor (name.c_str(), result);

        data_[idx].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
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

template<typename I, typename H>
template<typename T, typename DF, typename F>
typename DataFrame<I, H>::template StlVecType<T> DataFrame<I, H>::
combine(const char *col_name, const DF &rhs, F &functor) const  {

    SpinGuard   guard (lock_);
    const auto  &lhs_col = get_column<T>(col_name, false);
    const auto  &rhs_col = rhs.template get_column<T>(col_name, false);

    guard.release();

    const size_type col_s = std::min(lhs_col.size(), rhs_col.size());
    StlVecType<T>   result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]
        result.push_back(std::move(functor(lhs_col[i], rhs_col[i])));

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename DF1, typename DF2, typename F>
typename DataFrame<I, H>::template StlVecType<T> DataFrame<I, H>::
combine(const char *col_name,
        const DF1 &df1,
        const DF2 &df2,
        F &functor) const  {

    SpinGuard   guard (lock_);
    const auto  &lhs_col = get_column<T>(col_name, false);
    const auto  &df1_col = df1.template get_column<T>(col_name, false);
    const auto  &df2_col = df2.template get_column<T>(col_name, false);

    guard.release();

    const size_type col_s =
        std::min<size_type>({ lhs_col.size(), df1_col.size(),
                              df2_col.size() });
    StlVecType<T>   result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]
        result.push_back(
            std::move(functor(lhs_col[i], df1_col[i], df2_col[i])));

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename DF1, typename DF2, typename DF3, typename F>
typename DataFrame<I, H>::template StlVecType<T> DataFrame<I, H>::
combine(const char *col_name,
        const DF1 &df1,
        const DF2 &df2,
        const DF3 &df3,
        F &functor) const  {

    SpinGuard   guard (lock_);
    const auto  &lhs_col = get_column<T>(col_name, false);
    const auto  &df1_col = df1.template get_column<T>(col_name, false);
    const auto  &df2_col = df2.template get_column<T>(col_name, false);
    const auto  &df3_col = df3.template get_column<T>(col_name, false);

    guard.release();

    const size_type col_s = std::min<size_type>(
        { lhs_col.size(), df1_col.size(), df2_col.size(), df3_col.size() });
    StlVecType<T>   result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]
        result.push_back(
            std::move(functor(lhs_col[i], df1_col[i], df2_col[i],
                              df3_col[i])));

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<StringOnly T>
StringStats
DataFrame<I, H>::get_str_col_stats(const char *col_name) const  {

    const auto              &col = get_column<T>(col_name);
    size_type               total_chars { 0 };
    size_type               total_digits { 0 };
    size_type               total_alphabets { 0 };
    size_type               total_spaces { 0 };
    size_type               total_arithmetic { 0 };
    size_type               total_puncts { 0 };
    size_type               total_caps { 0 };
    size_type               total_line_feed { 0 };
    const I                 def_index { };
    StdVisitor<double, I>   std_size { };

    std_size.pre();
    for (const auto &str : col)  {
        size_type   this_size { 0 };
        const char  *my_str { nullptr };

        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, VirtualString>)
            my_str = str.c_str();
        else
            my_str = str;

        for (auto c = my_str; *c; ++c)  {
            this_size += 1;

            if (std::isalpha(*c))  {
                total_alphabets += 1;
                if (std::isupper(*c))
                    total_caps += 1;
            }
            else if (std::isdigit(*c))
                total_digits += 1;
            else if (*c == '\n')
                total_line_feed += 1;
            else if (std::isspace(*c))
                total_spaces += 1;
            else if (*c == '+' || *c == '-' || *c == '/' || *c == '*')
                total_arithmetic += 1;
            else
                total_puncts += 1;
        }
        std_size(def_index, static_cast<double>(this_size));
        total_chars += this_size;
    }
    std_size.post();

    StringStats result;

    result.avg_size = double(total_chars) / double(col.size());
    result.std_size = std_size.get_result();
    result.avg_alphabets = double(total_alphabets) / double(total_chars);
    result.avg_digits = double(total_digits) / double(total_chars);
    result.avg_caps = double(total_caps) / double(total_chars);
    result.avg_puncts = double(total_puncts) / double(total_chars);
    result.avg_spaces = double(total_spaces) / double(total_chars);
    result.avg_arithmetic = double(total_arithmetic) / double(total_chars);
    result.avg_line_feed = double(total_line_feed) / double(total_chars);
    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename C>
typename DataFrame<I, H>::size_type
DataFrame<I, H>::inversion_count(const char *col_name) const  {

    const auto      &col = get_column<T>(col_name);
    const auto      col_s = col.size();
    StlVecType<T>   original(col.begin(), col.end());
    StlVecType<T>   temp(col_s);

    return (_inv_merge_sort_(original, temp, 0, col_s - 1, C { },
                             get_thread_level()));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<container T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::explode(const char *col_name) const  {

    const auto  &col = get_column<T>(col_name);
    size_type   total_cnt { 0 };

    for (const auto &cont : col)
        total_cnt += cont.size();

    StlVecType<size_type>   idx_mask;

    idx_mask.reserve(total_cnt);
    for (size_type i { 0 }; i < col.size(); ++i)
        idx_mask.insert(idx_mask.end(), col[i].size(), i);

    IndexVecType    new_index;

    new_index.reserve(total_cnt);
    for (size_type i { 0 }; i < total_cnt; ++i)
        new_index.push_back(indices_[idx_mask[i]]);

    DataFrame    result;

    result.load_index(std::move(new_index));

    if constexpr (is_mappish<T>::value)
        explode_helper_<
            std::pair<typename T::key_type, typename T::mapped_type>,
            decltype(col),
            Ts ...>
            (result, total_cnt, col_name, idx_mask, col);
    else
        explode_helper_<typename T::value_type, decltype(col), Ts ...>
            (result, total_cnt, col_name, idx_mask, col);

    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<equality_default_construct ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::difference(const DataFrame &other) const  {

#ifdef HMDF_SANITY_EXCEPTIONS
    if (indices_ != other.get_index())
        throw NotFeasible("difference(): "
                          "Self and other index columns are not identical");
#endif // HMDF_SANITY_EXCEPTIONS

    DataFrame           result;
    DFSet<size_type>    idx_set;

    {
        const SpinGuard guard(lock_);

        for (const auto &[name, idx] : column_list_) [[likely]]  {
            difference_functor_<Ts ...> functor(
                name.c_str(), other, result, idx_set);

            data_[idx].change(functor);
        }
    }

    IndexVecType    new_index;

    new_index.reserve(idx_set.size());
    for (const auto idx : idx_set)
        new_index.push_back(indices_[idx]);
    result.load_index(std::move(new_index));

    return (result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
