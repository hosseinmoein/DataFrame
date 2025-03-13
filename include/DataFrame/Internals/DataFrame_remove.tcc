// Hossein Moein
// September 12, 2019
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
#include <DataFrame/DataFrameTransformVisitors.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::remove_data_by_idx (Index2D<IndexType> range)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call remove_data_by_idx()");

    const auto  &lower =
        std::lower_bound (indices_.begin(), indices_.end(), range.begin);
    const auto  &upper =
        std::upper_bound (indices_.begin(), indices_.end(), range.end);

    if (lower != indices_.end()) [[likely]]  {
        const size_type b_dist = std::distance(indices_.begin(), lower);
        const size_type e_dist =
            std::distance(indices_.begin(),
                          upper < indices_.end() ? upper : indices_.end());

        make_consistent<Ts ...>();
        indices_.erase(lower, upper);

        const auto      thread_level =
            (indices_.size() < ThreadPool::MUL_THR_THHOLD)
                ? 0L : get_thread_level();
        const SpinGuard guard(lock_);

        if (thread_level > 2)  {
            auto    lbd =
                [b_dist, e_dist, this]
                (const auto &begin, const auto &end) -> void  {
                    remove_functor_<Ts ...> functor (b_dist, e_dist);

                    for (auto citer = begin; citer < end; ++citer)
                        this->data_[citer->second].change(functor);
                };
            auto    futures =
                thr_pool_.parallel_loop(column_list_.begin(),
                                        column_list_.end(),
                                        std::move(lbd));

            for (auto &fut : futures)  fut.get();
        }
        else  {
            remove_functor_<Ts ...> functor (b_dist, e_dist);

            for (const auto &citer : column_list_)
                data_[citer.second].change(functor);
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::remove_data_by_loc (Index2D<long> range)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call remove_data_by_loc()");

    if (range.begin < 0)
        range.begin = static_cast<long>(indices_.size()) + range.begin;
    if (range.end < 0)
        range.end = static_cast<long>(indices_.size()) + range.end;

    if (range.end <= static_cast<long>(indices_.size()) &&
        range.begin <= range.end && range.begin >= 0) [[likely]]  {
        make_consistent<Ts ...>();
        indices_.erase(indices_.begin() + range.begin,
                       indices_.begin() + range.end);

        const auto      thread_level =
            (indices_.size() < ThreadPool::MUL_THR_THHOLD)
                ? 0L : get_thread_level();
        const SpinGuard guard(lock_);

        if (thread_level > 2)  {
            auto    lbd =
                [&range = std::as_const(range), this]
                (const auto &begin, const auto &end) -> void  {
                    remove_functor_<Ts ...> functor (size_type(range.begin),
                                                     size_type(range.end));

                    for (auto citer = begin; citer < end; ++citer)
                        this->data_[citer->second].change(functor);
                };
            auto    futures =
                thr_pool_.parallel_loop(column_list_.begin(),
                                        column_list_.end(),
                                        std::move(lbd));

            for (auto &fut : futures)  fut.get();
        }
        else  {
            remove_functor_<Ts ...> functor (size_type(range.begin),
                                             size_type(range.end));

            for (const auto &citer : column_list_) [[likely]]
                data_[citer.second].change(functor);
        }

        return;
    }

    char buffer [512];

    snprintf(buffer, sizeof(buffer) - 1,
             "DataFrame::remove_data_by_loc(): ERROR: "
             "Bad begin, end range: %ld, %ld",
             range.begin, range.end);
    throw BadRange (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename F, typename ... Ts>
void DataFrame<I, H>::remove_data_by_sel (const char *name, F &sel_functor)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_sel()");

    const ColumnVecType<T>  &vec = get_column<T>(name);
    const size_type         col_s = vec.size();
    StlVecType<size_type>   col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i) [[likely]]
        if (sel_functor (indices_[i], vec[i]))
            col_indices.push_back(i);

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename F, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_sel (const char *name1, const char *name2, F &sel_functor)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_sel()");

    SpinGuard               guard (lock_);
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1, false);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2, false);
    const size_type         idx_s = indices_.size();
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         min_col_s = std::min(col_s1, col_s2);
    StlVecType<size_type>   col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < min_col_s; ++i) [[likely]]
        if (sel_functor (indices_[i], vec1[i], vec2[i]))
            col_indices.push_back(i);
    for (size_type i = min_col_s; i < idx_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : get_nan<T1>(),
                         i < col_s2 ? vec2[i] : get_nan<T2>()))
            col_indices.push_back(i);

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename F, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_sel (const char *name1,
                    const char *name2,
                    const char *name3,
                    F &sel_functor)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_sel()");

    SpinGuard               guard (lock_);
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1, false);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2, false);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3, false);
    const size_type         idx_s = indices_.size();
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s3 = vec3.size();
    const size_type         min_col_s = std::min({ col_s1, col_s2, col_s3 });
    StlVecType<size_type>   col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < min_col_s; ++i) [[likely]]
        if (sel_functor (indices_[i], vec1[i], vec2[i], vec3[i]))
            col_indices.push_back(i);
    for (size_type i = min_col_s; i < idx_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : get_nan<T1>(),
                         i < col_s2 ? vec2[i] : get_nan<T2>(),
                         i < col_s3 ? vec3[i] : get_nan<T3>()))
            col_indices.push_back(i);

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<StringOnly T, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_like (const char *name,
                     const char *pattern,
                     bool case_insensitive,
                     char esc_char)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_like()");

    const ColumnVecType<T>  &vec = get_column<T>(name);
    const size_type         col_s = vec.size();
    StlVecType<size_type>   col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, VirtualString>)  {
            if (_like_clause_compare_(pattern,
                                      vec[i].c_str(),
                                      case_insensitive,
                                      esc_char))
                col_indices.push_back(i);
        }
        else  {
            if (_like_clause_compare_(pattern,
                                      vec[i],
                                      case_insensitive,
                                      esc_char))
                col_indices.push_back(i);
        }
    }

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<StringOnly T, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_like(const char *name1,
                    const char *name2,
                    const char *pattern1,
                    const char *pattern2,
                    bool case_insensitive,
                    char esc_char)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_like()");

    SpinGuard               guard (lock_);
    const ColumnVecType<T>  &vec1 = get_column<T>(name1, false);
    const ColumnVecType<T>  &vec2 = get_column<T>(name2, false);
    const size_type         min_col_s = std::min(vec1.size(), vec2.size());
    StlVecType<size_type>   col_indices;

    col_indices.reserve(min_col_s / 2);
    for (size_type i = 0; i < min_col_s; ++i) [[likely]]  {
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, VirtualString>)  {
            if (_like_clause_compare_(pattern1,
                                      vec1[i].c_str(),
                                      case_insensitive,
                                      esc_char) &&
                _like_clause_compare_(pattern2,
                                      vec2[i].c_str(),
                                      case_insensitive,
                                      esc_char))
                col_indices.push_back(i);
        }
        else  {
            if (_like_clause_compare_(pattern1,
                                      vec1[i],
                                      case_insensitive,
                                      esc_char) &&
                _like_clause_compare_(pattern2,
                                      vec2[i],
                                      case_insensitive,
                                      esc_char))
                col_indices.push_back(i);
        }
    }

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T, typename ... Ts>
void DataFrame<I, H>::remove_top_n_data(const char *col_name, size_type n)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_top_n_data()");

    const ColumnVecType<T>  &vec = get_column<T>(col_name);
    NLargestVisitor<T, I>   nlv { n };

    nlv.pre();
    nlv(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    nlv.post();
    nlv.sort_by_index_idx();

    StlVecType<size_type>   col_indices;

    col_indices.reserve(n);
    for (const auto &res : nlv.get_result())
        col_indices.push_back(res.index_idx);

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T, typename ... Ts>
void DataFrame<I, H>::
remove_bottom_n_data(const char *col_name, size_type n)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_bottom_n_data()");

    const ColumnVecType<T>  &vec = get_column<T>(col_name);
    NSmallestVisitor<T, I>  nsv { n };

    nsv.pre();
    nsv(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    nsv.post();
    nsv.sort_by_index_idx();

    StlVecType<size_type>   col_indices;

    col_indices.reserve(n);
    for (const auto &res : nsv.get_result())
        col_indices.push_back(res.index_idx);

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T, typename ... Ts>
void DataFrame<I, H>::
remove_above_quantile_data(const char *col_name, double quantile)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_above_quantile_data()");

    const ColumnVecType<T>  &vec = get_column<T>(col_name);
    QuantileVisitor<T, I>   quant { quantile };

    quant.pre();
    quant(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    quant.post();

    StlVecType<size_type>   col_indices;
    const size_type         col_s = vec.size();

    col_indices.reserve(col_s / 3);
    for (size_type i = 0; i < col_s; ++i)
        if (vec[i] > quant.get_result())
            col_indices.push_back(i);

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T, typename ... Ts>
void DataFrame<I, H>::
remove_below_quantile_data(const char *col_name, double quantile)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_below_quantile_data()");

    const ColumnVecType<T>  &vec = get_column<T>(col_name);
    QuantileVisitor<T, I>   quant { quantile };

    quant.pre();
    quant(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    quant.post();

    StlVecType<size_type>   col_indices;
    const size_type         col_s = vec.size();

    col_indices.reserve(col_s / 3);
    for (size_type i = 0; i < col_s; ++i)
        if (vec[i] < quant.get_result())
            col_indices.push_back(i);

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<arithmetic T, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_stdev(const char *col_name, T above_stdev, T below_stdev)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_stdev()");

    const ColumnVecType<T>  &vec = get_column<T>(col_name);
    const size_type         col_s = vec.size();
    const auto              thread_level =
        (col_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();
    auto                    mean_lbd =
        [&vec = std::as_const(vec), this]() -> T  {
            MeanVisitor<T, I>   mean { true };

            mean.pre();
            mean(indices_.begin(), indices_.end(), vec.begin(), vec.end());
            mean.post();
            return (mean.get_result());
        };
    auto                    stdev_lbd =
        [&vec = std::as_const(vec), this]() -> T  {
            StdVisitor<T, I>    stdev { true, true };

            stdev.pre();
            stdev(indices_.begin(), indices_.end(), vec.begin(), vec.end());
            stdev.post();
            return (stdev.get_result());
        };
    T                       stdev;
    T                       mean;

    if (thread_level > 2)  {
        auto    stdev_fut = thr_pool_.dispatch(false, stdev_lbd);
        auto    mean_fut = thr_pool_.dispatch(false, mean_lbd);

        mean = mean_fut.get();
        stdev = stdev_fut.get();
    }
    else  {
        mean = mean_lbd();
        stdev = stdev_lbd();
    }

    StlVecType<size_type>   col_indices;

    col_indices.reserve(col_s / 3);
    for (size_type i = 0; i < col_s; ++i)  {
        const T z = (vec[i] - mean) / stdev;

        if (z > above_stdev || z < below_stdev)
            col_indices.push_back(i);
    }

    remove_data_by_sel_common_<Ts ...>(col_indices);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<arithmetic T, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_hampel(const char *col_name,
                      size_type window_size,
                      hampel_type htype,
                      T num_of_stdev)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_hampel()");

    using hampel_t = HampelFilterVisitor<T, I, std::size_t(H::align_value)>;

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    hampel_t            hampel { window_size, htype, num_of_stdev };

    hampel.pre();
    hampel(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    hampel.post();

    remove_data_by_sel_common_<Ts ...>(hampel.get_result());
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<arithmetic T, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_fft(const char *col_name,
                   size_type freq_num,
                   T anomaly_threshold,
                   normalization_type norm)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_fft()");

    using fftv_t =
        AnomalyDetectByFFTVisitor<T, I, std::size_t(H::align_value)>;

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    fftv_t              fft { freq_num, anomaly_threshold, norm };

    fft.pre();
    fft(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    fft.post();

    remove_data_by_sel_common_<Ts ...>(fft.get_result());
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<arithmetic T, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_iqr(const char *col_name, T high_fence, T low_fence)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_iqr()");

    using iqrv_t =
        AnomalyDetectByIQRVisitor<T, I, std::size_t(H::align_value)>;

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    iqrv_t              iqr { high_fence, low_fence };

    iqr.pre();
    iqr(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    iqr.post();

    remove_data_by_sel_common_<Ts ...>(iqr.get_result());
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<arithmetic T, typename ... Ts>
void DataFrame<I, H>::
remove_data_by_zscore(const char *col_name, T threshold)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value,
                  "Only a StdDataFrame or a PtrView can call "
                  "remove_data_by_zscore()");

    using zscorev_t =
        AnomalyDetectByZScoreVisitor<T, I, std::size_t(H::align_value)>;

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    zscorev_t           zscore { threshold };

    zscore.pre();
    zscore(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    zscore.post();

    remove_data_by_sel_common_<Ts ...>(zscore.get_result());
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
remove_duplicates (const char *name,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T &, const IndexType &>;
    using count_vec = StlVecType<size_type>;
    using map_t = DFUnorderedMap<data_tuple, count_vec, TupleHash>;

    const ColumnVecType<T>  *vec { nullptr };
    const auto              &index = get_index();

    if (! ::strcmp(name, DF_INDEX_COL_NAME))  {
        vec = (const ColumnVecType<T> *) &index;
        include_index = false;
    }
    else
        vec = (const ColumnVecType<T> *) &(get_column<T>(name));

    const size_type col_s = std::min(vec->size(), index.size());
    map_t           row_table;
    count_vec       dummy_vec;
    const IndexType dummy_idx { };

    row_table.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        const auto  insert_res =
            row_table.emplace(
                std::forward_as_tuple((*vec)[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)  insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<map_t, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T1, hashable_equal T2, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
remove_duplicates (const char *name1,
                   const char *name2,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &, const IndexType &>;
    using count_vec = StlVecType<size_type>;
    using map_t = DFUnorderedMap<data_tuple, count_vec, TupleHash>;

    SpinGuard               guard (lock_);
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1, false);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2, false);

    guard.release();

    const auto      &index = get_index();
    const size_type col_s =
        std::min<size_type>({ vec1.size(), vec2.size(), index.size() });
    map_t           row_table;
    count_vec       dummy_vec;
    const IndexType dummy_idx { };

    row_table.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        const auto  insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<map_t, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T1, hashable_equal T2, hashable_equal T3,
         typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
remove_duplicates (const char *name1,
                   const char *name2,
                   const char *name3,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple =
        std::tuple<const T1 &, const T2 &, const T3 &, const IndexType &>;
    using count_vec = StlVecType<size_type>;
    using map_t = DFUnorderedMap<data_tuple, count_vec, TupleHash>;

    SpinGuard               guard (lock_);
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1, false);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2, false);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3, false);

    guard.release();

    const auto      &index = get_index();
    const size_type col_s =
        std::min<size_type>(
            { vec1.size(), vec2.size(), vec3.size(), index.size() });
    map_t           row_table;
    count_vec       dummy_vec;
    const IndexType dummy_idx { };

    row_table.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        const auto  insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i], vec3[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<map_t, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T1, hashable_equal T2, hashable_equal T3,
         hashable_equal T4,
         typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
remove_duplicates (const char *name1,
                   const char *name2,
                   const char *name3,
                   const char *name4,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &,
                                  const T3 &, const T4 &,
                                  const IndexType &>;
    using count_vec = StlVecType<size_type>;
    using map_t = DFUnorderedMap<data_tuple, count_vec, TupleHash>;

    SpinGuard               guard (lock_);
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1, false);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2, false);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3, false);
    const ColumnVecType<T4> &vec4 = get_column<T4>(name4, false);

    guard.release();

    const auto      &index = get_index();
    const size_type col_s =
        std::min<size_type>(
            { vec1.size(), vec2.size(), vec3.size(), vec4.size(),
              index.size() });
    map_t           row_table;
    count_vec       dummy_vec;
    const IndexType dummy_idx { };

    row_table.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        const auto  insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i], vec3[i], vec4[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<map_t, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T1, hashable_equal T2, hashable_equal T3,
         hashable_equal T4, hashable_equal T5,
         typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
remove_duplicates (const char *name1,
                   const char *name2,
                   const char *name3,
                   const char *name4,
                   const char *name5,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &,
                                  const T3 &, const T4 &, const T5 &,
                                  const IndexType &>;
    using count_vec = StlVecType<size_type>;
    using map_t = DFUnorderedMap<data_tuple, count_vec, TupleHash>;

    SpinGuard               guard (lock_);
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1, false);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2, false);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3, false);
    const ColumnVecType<T4> &vec4 = get_column<T4>(name4, false);
    const ColumnVecType<T5> &vec5 = get_column<T5>(name5, false);

    guard.release();

    const auto      &index = get_index();
    const size_type col_s =
        std::min<size_type>(
            { vec1.size(), vec2.size(), vec3.size(), vec4.size(), vec5.size(),
              index.size() });
    map_t           row_table;
    count_vec       dummy_vec;
    const IndexType dummy_idx { };

    row_table.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        const auto  insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i], vec3[i],
                                      vec4[i], vec5[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<map_t, Ts ...>(*this, rds, row_table, index));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T1, hashable_equal T2, hashable_equal T3,
         hashable_equal T4, hashable_equal T5, hashable_equal T6,
         typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
remove_duplicates (const char *name1,
                   const char *name2,
                   const char *name3,
                   const char *name4,
                   const char *name5,
                   const char *name6,
                   bool include_index,
                   remove_dup_spec rds) const  {

    using data_tuple = std::tuple<const T1 &, const T2 &,
                                  const T3 &, const T4 &,
                                  const T5 &, const T6 &,
                                  const IndexType &>;
    using count_vec = StlVecType<size_type>;
    using map_t = DFUnorderedMap<data_tuple, count_vec, TupleHash>;

    SpinGuard               guard (lock_);
    const ColumnVecType<T1> &vec1 = get_column<T1>(name1, false);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2, false);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3, false);
    const ColumnVecType<T4> &vec4 = get_column<T4>(name4, false);
    const ColumnVecType<T5> &vec5 = get_column<T5>(name5, false);
    const ColumnVecType<T6> &vec6 = get_column<T6>(name6, false);

    guard.release();

    const auto      &index = get_index();
    const size_type col_s =
        std::min<size_type>(
            { vec1.size(), vec2.size(), vec3.size(), vec4.size(),
              vec5.size(), vec6.size(),
              index.size() });
    map_t           row_table;
    count_vec       dummy_vec;
    const IndexType dummy_idx { };

    row_table.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i) [[likely]]  {
        const auto  insert_res =
            row_table.emplace(
                std::forward_as_tuple(vec1[i], vec2[i], vec3[i],
                                      vec4[i], vec5[i], vec6[i],
                                      include_index ? index[i] : dummy_idx),
                dummy_vec);

        if (insert_res.second)
            insert_res.first->second.reserve(8);
        insert_res.first->second.push_back(i);
    }

    return(remove_dups_common_<map_t, Ts ...>(*this, rds, row_table, index));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
