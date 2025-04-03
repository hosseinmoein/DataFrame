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

#include <DataFrame/DataFrameMLVisitors.h>

#include <DataFrame/DataFrame.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

// Notice member variables are initialized twice, but that's cheap
//
template<typename I, typename H>
DataFrame<I, H>::DataFrame(const DataFrame &that)  { *this = that; }

template<typename I, typename H>
DataFrame<I, H>::DataFrame(DataFrame &&that)  { *this = std::move(that); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
DataFrame<I, H> &
DataFrame<I, H>::operator= (const DataFrame &that)  {

    if (this != &that)  {
        indices_ = that.indices_;
        column_tb_ = that.column_tb_;
        column_list_ = that.column_list_;

        const SpinGuard guard(lock_);

        data_ = that.data_;
    }
    return (*this);
}

// ----------------------------------------------------------------------------
template<typename I, typename H>
template<typename OTHER, typename ... Ts>
DataFrame<I, H> &
DataFrame<I, H>::assign(OTHER &rhs)  {

    indices_.clear();
    indices_.reserve(rhs.indices_.size());
    if constexpr (std::is_base_of<HeteroView<align_value>, H>::value)  {
        indices_.set_begin_end_special(&(rhs.indices_.front()),
                                       &(rhs.indices_.back()));
    }
    else if constexpr (std::is_base_of<HeteroPtrView<align_value>, H>::value) {
        for (auto &val : rhs.indices_)
            indices_.push_back(&val);
    }
    else  {
        for (const auto &val : rhs.indices_)  indices_.push_back(val);
    }

    column_tb_.clear();
    column_list_.clear();

    const SpinGuard guard(lock_);

    data_.clear();
    if constexpr (std::is_base_of<HeteroView<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value)  {
        for (const auto &[name, idx] : rhs.column_list_) [[likely]]  {
            view_setup_functor_<DataFrame, Ts ...>   functor(
                name.c_str(), 0, indices_.size(), *this);

            rhs.data_[idx].change(functor);
        }
    }
    else  {
        for (const auto &[rhs_name, rhs_idx] : rhs.column_list_)  {
            load_all_functor_<DataFrame, Ts ...> functor(
                rhs_name.c_str(), *this);

            rhs.data_[rhs_idx].change(functor);
        }
    }

    return (*this);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename OTHER, typename ... Ts>
DataFrame<I, H> &
DataFrame<I, H>::assign(const OTHER &rhs)  {

    indices_.clear();
    indices_.reserve(rhs.indices_.size());
    if constexpr (std::is_base_of<HeteroView<align_value>, H>::value)  {
        indices_.set_begin_end_special(&(rhs.indices_.front()),
                                       &(rhs.indices_.back()));
    }
    else if constexpr (std::is_base_of<HeteroPtrView<align_value>, H>::value) {
        for (auto &val : rhs.indices_)
            indices_.push_back(&val);
    }
    else  {
        for (const auto &val : rhs.indices_)  indices_.push_back(val);
    }

    column_tb_.clear();
    column_list_.clear();

    const SpinGuard guard(lock_);

    data_.clear();
    if constexpr (std::is_base_of<HeteroView<align_value>, H>::value ||
                  std::is_base_of<HeteroPtrView<align_value>, H>::value)  {
        for (const auto &[name, idx] : rhs.column_list_) [[likely]]  {
            view_setup_functor_<DataFrame, Ts ...>   functor(
                name.c_str(), 0, indices_.size(), *this);

            rhs.data_[idx].change(functor);
        }
    }
    else  {
        for (const auto &[rhs_name, rhs_idx] : rhs.column_list_)  {
            load_all_functor_<DataFrame, Ts ...> functor(
                rhs_name.c_str(), *this);

            rhs.data_[rhs_idx].change(functor);
        }
    }

    return (*this);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
DataFrame<I, H> &
DataFrame<I, H>::operator= (DataFrame &&that)  {

    if (this != &that)  {
        indices_ = std::exchange(that.indices_, IndexVecType { });
        column_tb_ = std::exchange(that.column_tb_, ColNameDict { });
        column_list_ = std::exchange(that.column_list_, ColNameList { });

        const SpinGuard guard(lock_);

        data_ = std::exchange(that.data_, DataVecVec { });
    }
    return (*this);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
DataFrame<I, H>::~DataFrame()  {

    const SpinGuard guard(lock_);

    data_.clear();
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool DataFrame<I, H>::empty() const noexcept  { return (indices_.empty()); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool DataFrame<I, H>::
shapeless() const noexcept  { return (empty() && column_list_.empty()); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::set_lock (SpinLock *sl)  { lock_ = sl; }

// ----------------------------------------------------------------------------

template<typename I, typename H>
SpinLock *DataFrame<I, H>::get_lock ()  { return (lock_); }

// ----------------------------------------------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::remove_lock ()  { lock_ = nullptr; }

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void
DataFrame<I, H>::shuffle(const StlVecType<const char *> &col_names,
                         bool also_shuffle_index,
                         seed_t seed)  {

    std::random_device  rd;
    std::mt19937        g ((seed != seed_t(-1)) ? seed : rd());
    std::future<void>   idx_future;
    const auto          thread_level =
        (indices_.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();

    if (also_shuffle_index)  {
        if (thread_level > 2)  {
            auto    lbd = [&g, this] () -> void  {
                std::shuffle(this->indices_.begin(), this->indices_.end(), g);
            };

            idx_future = thr_pool_.dispatch(false, lbd);
        }
        else
            std::shuffle(indices_.begin(), indices_.end(), g);
    }

    shuffle_functor_<Ts ...>    functor (g);
    auto                        lbd =
        [&functor, this](const auto &name_citer) -> void  {
            const auto  citer = this->column_tb_.find (name_citer);

            if (citer == this->column_tb_.end()) [[unlikely]]  {
                char buffer [512];

                snprintf(buffer, sizeof(buffer) - 1,
                        "DataFrame::shuffle(): ERROR: Cannot find column '%s'",
                         name_citer);
                throw ColNotFound(buffer);
            }

            this->data_[citer->second].change(functor);
        };
    const SpinGuard             guard (lock_);

    if (thread_level > 2)  {
        std::vector<std::future<void>>  futures;

        futures.reserve(col_names.size());
        for (const auto &name_citer : col_names) [[likely]]
            futures.emplace_back(thr_pool_.dispatch(
                false, lbd, std::cref(name_citer)));

        if (idx_future.valid())  idx_future.get();
        for (auto &fut : futures)  fut.get();
    }
    else  {
        for (const auto &name_citer : col_names) [[likely]]
            lbd(name_citer);
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void DataFrame<I, H>::
fill_missing(const StlVecType<const char *> &col_names,
             fill_policy fp,
             const StlVecType<T> &values,
             int limit)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call fill_missing()");

    const size_type                 count = col_names.size();
    const auto                      thread_level =
        (indices_.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();
    StlVecType<std::future<void>>   futures;

    if (thread_level > 2)
        futures.reserve(count);
    for (size_type i = 0; i < count; ++i)  {
        ColumnVecType<T>    &vec = get_column<T>(col_names[i]);

        if (thread_level <= 2)  {
            if constexpr (supports_arithmetic<T>::value &&
                          supports_arithmetic<IndexType>::value)  {
                if (fp == fill_policy::linear_interpolate)
                    fill_missing_linter_<T>(vec, indices_, limit);
                else if (fp == fill_policy::lagrange_interpolate)
                    fill_missing_lagrange_<T>(vec, indices_, limit);
            }
            if (fp == fill_policy::value)
                fill_missing_value_(vec, values[i], limit, indices_.size());
            else if (fp == fill_policy::fill_forward)
                fill_missing_ffill_<T>(vec, limit, indices_.size());
            else if (fp == fill_policy::fill_backward)
                fill_missing_bfill_<T>(vec, limit);
            else if (fp == fill_policy::mid_point)
                fill_missing_midpoint_<T>(vec, limit, indices_.size());
        }
        else  {
            if constexpr (supports_arithmetic<T>::value &&
                          supports_arithmetic<IndexType>::value)  {
                if (fp == fill_policy::linear_interpolate)
                    futures.emplace_back(
                        thr_pool_.dispatch(false,
                                           &DataFrame::fill_missing_linter_<T>,
                                               std::ref(vec),
                                               std::cref(indices_),
                                               limit));
                else if (fp == fill_policy::lagrange_interpolate)
                    futures.emplace_back(
                        thr_pool_.dispatch(
                            false,
                            &DataFrame::fill_missing_lagrange_<T>,
                                std::ref(vec),
                                indices_,
                                limit));
            }
            if (fp == fill_policy::value)
                futures.emplace_back(
                    thr_pool_.dispatch(false,
                                       &DataFrame::fill_missing_value_<T>,
                                           std::ref(vec),
                                           std::cref(values[i]),
                                           limit,
                                           indices_.size()));
            else if (fp == fill_policy::fill_forward)
                futures.emplace_back(
                    thr_pool_.dispatch(false,
                                       &DataFrame::fill_missing_ffill_<T>,
                                           std::ref(vec),
                                           limit,
                                           indices_.size()));
            else if (fp == fill_policy::fill_backward)
                futures.emplace_back(
                    thr_pool_.dispatch(false,
                                       &DataFrame::fill_missing_bfill_<T>,
                                           std::ref(vec),
                                           limit));
            else if (fp == fill_policy::mid_point)
                futures.emplace_back(
                    thr_pool_.dispatch(false,
                                       &DataFrame::fill_missing_midpoint_<T>,
                                           std::ref(vec),
                                           limit,
                                           indices_.size()));
        }
    }
    for (auto &fut : futures)  fut.get();

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename DF, typename ... Ts>
void DataFrame<I, H>::fill_missing (const DF &rhs)  {

    const auto      &self_idx = get_index();
    const auto      &rhs_idx = rhs.get_index();
    const SpinGuard guard(lock_);

    for (const auto &[name, idx] : column_list_) [[likely]]  {
        fill_missing_functor_<DF, Ts ...>   functor (self_idx,
                                                     rhs_idx,
                                                     rhs,
                                                     name.c_str());

        data_[idx].change(functor);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<arithmetic T>
void DataFrame<I, H>::
detect_and_change(const StlVecType<const char *> &col_names,
                  detect_method d_method,
                  fill_policy f_policy,
                  DetectAndChangeParams<T> params)  {

    const size_type                 count = col_names.size();
    // const auto                      thread_level =
    //     (indices_.size() < ThreadPool::MUL_THR_THHOLD)
    //         ? 0L : get_thread_level();
    // StlVecType<std::future<void>>   futures;

    for (size_type col = 0; col < count; ++col)  {
        ColumnVecType<T>        &vec = get_column<T>(col_names[col]);
        StlVecType<size_type>   rows {  };;

        if (vec.empty()) [[unlikely]]  continue;

        if (d_method == detect_method::fft)  {
            using fftv_t = and_fft_v<T, I, std::size_t(H::align_value)>;

            fftv_t  fft { params.freq_num, params.threshold, params.norm_type };

            fft.pre();
            fft(indices_.begin(), indices_.end(), vec.begin(), vec.end());
            fft.post();
            rows = std::move(fft.get_result());
        }
        else if (d_method == detect_method::iqr)  {
            using iqrv_t = and_iqr_v<T, I, std::size_t(H::align_value)>;

            iqrv_t  iqr { params.high_fence, params.low_fence };

            iqr.pre();
            iqr(indices_.begin(), indices_.end(), vec.begin(), vec.end());
            iqr.post();
            rows = std::move(iqr.get_result());
        }
        else if (d_method == detect_method::lof)  {
            using lofv_t = and_lof_v<T, I, std::size_t(H::align_value)>;

            lofv_t  lof { params.k, params.threshold,
                          params.norm_type, std::move(params.dist_fun) };

            lof.pre();
            lof(indices_.begin(), indices_.end(), vec.begin(), vec.end());
            lof.post();
            rows = std::move(lof.get_result());
        }
        else if (d_method == detect_method::hampel)  {
            using hampel_t = hamf_v<T, I, std::size_t(H::align_value)>;

            hampel_t    hampel { params.window_size, params.htype,
                                 params.num_stdev };

            hampel.pre();
            hampel(indices_.begin(), indices_.end(), vec.begin(), vec.end());
            hampel.post();
            rows = std::move(hampel.get_result());
        }
        else if (d_method == detect_method::zscore)  {
            using zscorev_t = and_zscr_v<T, I, std::size_t(H::align_value)>;

            zscorev_t   zscore { params.threshold };

            zscore.pre();
            zscore(indices_.begin(), indices_.end(), vec.begin(), vec.end());
            zscore.post();
            rows = std::move(zscore.get_result());
        }
        else  {
            throw NotImplemented("detect_and_change(): "
                                 "Detection method is not implemented");
        }

        if (rows.empty()) [[unlikely]]  continue;

        const size_type col_s { vec.size() };
        const size_type row_s { rows.size() };
        bool            processed { false };

        if constexpr (supports_arithmetic<IndexType>::value)  {
            if (f_policy == fill_policy::linear_interpolate)  {
                for (size_type i = 0; i < row_s; ++i)  {
                    const auto  row_idx { rows.at(i) };

                    if ((row_idx < (col_s - 1)) && (row_idx > 0))  {
                        const auto  &x1 { indices_[row_idx - 1] };
                        const auto  &x { indices_[row_idx] };
                        const auto  &x2 { indices_[row_idx + 1] };
                        const auto  &y1 { vec[row_idx - 1] };
                        const auto  &y2 { vec[row_idx + 1] };

                        vec[row_idx] =
                            y1 + (T(x - x1) / T(x2 - x1)) * (y2 - y1);
                    }
                }
                processed = true;
            }
        }
        if (f_policy == fill_policy::fill_forward)  {
            for (size_type i = 0; i < row_s; ++i)  {
                const auto  row_idx { rows.at(i) };

                if (row_idx > 0)
                    vec[row_idx] = vec[row_idx - 1];
            }
            processed = true;
        }
        else if (f_policy == fill_policy::fill_backward)  {
            for (size_type i = 0; i < row_s; ++i)  {
                const auto  row_idx { rows.at(i) };

                if (row_idx < (col_s - 1))
                    vec[row_idx] = vec[row_idx + 1];
            }
            processed = true;
        }
        else if (f_policy == fill_policy::mid_point)  {
            for (size_type i = 0; i < row_s; ++i)  {
                const auto  row_idx { rows.at(i) };

                if ((row_idx < (col_s - 1)) && (row_idx > 0))
                    vec[row_idx] =
                        (vec[row_idx + 1] + vec[row_idx - 1]) * T(0.5);
            }
            processed = true;
        }
        else if (! processed)  {
            throw NotImplemented("detect_and_change(): "
                                 "Fill policy is not implemented");
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::
drop_missing(drop_policy policy, size_type threshold)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call drop_missing()");

    DropRowMap                      missing_row_map;
    const size_type                 num_cols = data_.size();
    const auto                      thread_level =
        (indices_.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();
    std::vector<std::future<void>>  futures;

    if (thread_level > 2)  futures.reserve(num_cols + 1);

    map_missing_rows_functor_<Ts ...>   functor (
        indices_.size(), missing_row_map);
    const SpinGuard                     guard(lock_);

    for (size_type idx = 0; idx < num_cols; ++idx)
        data_[idx].change(functor);

    if (thread_level > 2)
        futures.emplace_back(
            thr_pool_.dispatch(
                false,
                &DataFrame::drop_missing_rows_<decltype(indices_)>,
                     std::ref(indices_),
                     std::cref(missing_row_map),
                     policy,
                     threshold,
                     num_cols));
    else
        drop_missing_rows_(indices_,
                           missing_row_map,
                           policy,
                           threshold,
                           num_cols);

    drop_missing_rows_functor_<Ts ...>  functor2 (missing_row_map,
                                                  policy,
                                                  threshold,
                                                  data_.size(),
                                                  thread_level,
                                                  futures);

    for (size_type idx = 0; idx < num_cols; ++idx)
        data_[idx].change(functor2);

    for (auto &fut : futures)  fut.get();
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T>
typename DataFrame<I, H>::size_type DataFrame<I, H>::
replace(const char *col_name,
        const StlVecType<T> &old_values,
        const StlVecType<T> &new_values,
        long limit)  {

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    size_type           count = 0;

    replace_vector_vals_<ColumnVecType<T>, T>
        (vec, old_values, new_values, count, limit);

    return (count);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename DataFrame<I, H>::size_type DataFrame<I, H>::
replace_index(const StlVecType<IndexType> &old_values,
              const StlVecType<IndexType> &new_values,
              long limit)  {

    size_type   count = 0;

    replace_vector_vals_<IndexVecType, IndexType>
        (indices_, old_values, new_values, count, limit);

    return (count);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, replace_callable<I, T> F>
void DataFrame<I, H>::
replace(const char *col_name, F &functor)  {

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    const size_type     vec_s = vec.size();

    for (size_type i = 0; i < vec_s; ++i) [[likely]]
        if (! functor(indices_[i], vec[i]))  break;

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T>
std::future<typename DataFrame<I, H>::size_type> DataFrame<I, H>::
replace_async(const char *col_name,
              const StlVecType<T> &old_values,
              const StlVecType<T> &new_values,
              long limit)  {

    return (thr_pool_.dispatch(
                       true,
                       &DataFrame::replace<T>,
                           this,
                           col_name,
                           std::forward<const StlVecType<T>>(old_values),
                           std::forward<const StlVecType<T>>(new_values),
                           limit));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, replace_callable<I, T> F>
std::future<void> DataFrame<I, H>::
replace_async(const char *col_name, F &functor)  {

    return (thr_pool_.dispatch(true,
                               &DataFrame::replace<T, F>,
                                   this,
                                   col_name,
                                   std::ref(functor)));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ...Ts>
void DataFrame<I, H>::make_consistent ()  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call make_consistent()");

    consistent_functor_<Ts ...> functor (indices_.size());
    const SpinGuard             guard(lock_);

    for (const auto &iter : data_) [[likely]]
        iter.change(functor);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ...Ts>
void DataFrame<I, H>::shrink_to_fit ()  {

    indices_.shrink_to_fit();

    shrink_to_fit_functor_<Ts ...>  functor;
    const auto                      thread_level =
        (indices_.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();
    const SpinGuard                 guard(lock_);

    if (thread_level > 2)  {
        auto    lbd =
            [&functor](const auto &begin, const auto &end) -> void  {
                for (auto citer = begin; citer < end; ++citer)
                    citer->change(functor);
            };
        auto    futures =
            thr_pool_.parallel_loop(data_.begin(), data_.end(), std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        for (const auto &citer : data_) [[likely]]
            citer.change(functor);
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T, typename I_V, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
groupby1(const char *col_name, I_V &&idx_visitor, Ts&& ... args) const  {

    const ColumnVecType<T>  *gb_vec { nullptr };

    if (! ::strcmp(col_name, DF_INDEX_COL_NAME))
        gb_vec = (const ColumnVecType<T> *) &(get_index());
    else
        gb_vec = (const ColumnVecType<T> *) &(get_column<T>(col_name));

    StlVecType<std::size_t> sort_v (gb_vec->size(), 0);

    std::iota(sort_v.begin(), sort_v.end(), 0);
    std::ranges::sort(sort_v,
                      [gb_vec](std::size_t i, std::size_t j) -> bool  {
                          return (gb_vec->at(i) < gb_vec->at(j));
                      });

    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    res_t   res;
    auto    args_tuple = std::tuple<Ts ...>(args ...);
    auto    func =
        [this,
         &res,
         gb_vec,
         &sort_v = std::as_const(sort_v),
         idx_visitor = std::forward<I_V>(idx_visitor),
         col_name](auto &triple) mutable -> void {
            _load_groupby_data_1_(*this,
                                  res,
                                  triple,
                                  idx_visitor,
                                  *gb_vec,
                                  sort_v,
                                  col_name);
        };

    const SpinGuard guard(lock_);

    for_each_in_tuple (args_tuple, func);
    return (res);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T1, comparable T2, typename I_V, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
groupby2(const char *col_name1,
         const char *col_name2,
         I_V &&idx_visitor,
         Ts&& ... args) const  {

    const ColumnVecType<T1> *gb_vec1 { nullptr };
    const ColumnVecType<T2> *gb_vec2 { nullptr };
    const SpinGuard         guard (lock_);

    if (! ::strcmp(col_name1, DF_INDEX_COL_NAME))  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_index());
        gb_vec2 =
            (const ColumnVecType<T2> *) &(get_column<T2>(col_name2, false));
    }
    else if (! ::strcmp(col_name2, DF_INDEX_COL_NAME))  {
        gb_vec1 =
            (const ColumnVecType<T1> *) &(get_column<T1>(col_name1, false));
        gb_vec2 = (const ColumnVecType<T2> *) &(get_index());
    }
    else  {
        gb_vec1 =
            (const ColumnVecType<T1> *) &(get_column<T1>(col_name1, false));
        gb_vec2 =
            (const ColumnVecType<T2> *) &(get_column<T2>(col_name2, false));
    }

    StlVecType<std::size_t> sort_v(
        std::min(gb_vec1->size(), gb_vec2->size()), 0);

    std::iota(sort_v.begin(), sort_v.end(), 0);
    std::ranges::sort(sort_v,
                      [gb_vec1, gb_vec2]
                      (std::size_t i, std::size_t j) -> bool  {
                          if (gb_vec1->at(i) < gb_vec1->at(j))
                              return (true);
                          else if (gb_vec1->at(i) > gb_vec1->at(j))
                              return (false);
                          return (gb_vec2->at(i) < gb_vec2->at(j));
                      });

    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    res_t   res;
    auto    args_tuple = std::tuple<Ts ...>(args ...);
    auto    func =
        [*this,
         &res,
         gb_vec1,
         gb_vec2,
         &sort_v = std::as_const(sort_v),
         idx_visitor = std::forward<I_V>(idx_visitor),
         col_name1,
         col_name2](auto &triple) mutable -> void {
            _load_groupby_data_2_(*this,
                                  res,
                                  triple,
                                  idx_visitor,
                                  *gb_vec1,
                                  *gb_vec2,
                                  sort_v,
                                  col_name1,
                                  col_name2);
        };

    for_each_in_tuple (args_tuple, func);
    return (res);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T1, comparable T2, comparable T3,
         typename I_V, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
groupby3(const char *col_name1,
         const char *col_name2,
         const char *col_name3,
         I_V &&idx_visitor,
         Ts&& ... args) const  {

    const ColumnVecType<T1> *gb_vec1 { nullptr };
    const ColumnVecType<T2> *gb_vec2 { nullptr };
    const ColumnVecType<T3> *gb_vec3 { nullptr };
    const SpinGuard         guard (lock_);

    if (! ::strcmp(col_name1, DF_INDEX_COL_NAME))  {
        gb_vec1 = (const ColumnVecType<T1> *) &(get_index());
        gb_vec2 =
            (const ColumnVecType<T2> *) &(get_column<T2>(col_name2, false));
        gb_vec3 =
            (const ColumnVecType<T3> *) &(get_column<T3>(col_name3, false));
    }
    else if (! ::strcmp(col_name2, DF_INDEX_COL_NAME))  {
        gb_vec1 =
            (const ColumnVecType<T1> *) &(get_column<T1>(col_name1, false));
        gb_vec2 =
            (const ColumnVecType<T2> *) &(get_index());
        gb_vec3 =
            (const ColumnVecType<T3> *) &(get_column<T3>(col_name3, false));
    }
    else if (! ::strcmp(col_name3, DF_INDEX_COL_NAME))  {
        gb_vec1 =
            (const ColumnVecType<T1> *) &(get_column<T1>(col_name1, false));
        gb_vec2 =
            (const ColumnVecType<T2> *) &(get_column<T2>(col_name2, false));
        gb_vec3 = (const ColumnVecType<T3> *) &(get_index());
    }
    else  {
        gb_vec1 =
            (const ColumnVecType<T1> *) &(get_column<T1>(col_name1, false));
        gb_vec2 =
            (const ColumnVecType<T2> *) &(get_column<T2>(col_name2, false));
        gb_vec3 =
            (const ColumnVecType<T3> *) &(get_column<T3>(col_name3, false));
    }

    StlVecType<std::size_t> sort_v(
        std::min({ gb_vec1->size(), gb_vec2->size(), gb_vec3->size() }), 0);

    std::iota(sort_v.begin(), sort_v.end(), 0);
    std::ranges::sort(sort_v,
                      [gb_vec1, gb_vec2, gb_vec3]
                      (std::size_t i, std::size_t j) -> bool  {
                          if (gb_vec1->at(i) < gb_vec1->at(j))
                              return (true);
                          else if (gb_vec1->at(i) > gb_vec1->at(j))
                              return (false);
                          else if (gb_vec2->at(i) < gb_vec2->at(j))
                              return (true);
                          else if (gb_vec2->at(i) > gb_vec2->at(j))
                              return (false);
                          return (gb_vec3->at(i) < gb_vec3->at(j));
                      });

    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    res_t   res;
    auto    args_tuple = std::tuple<Ts ...>(args ...);
    auto    func =
        [*this,
         &res,
         gb_vec1,
         gb_vec2,
         gb_vec3,
         &sort_v = std::as_const(sort_v),
         idx_visitor = std::forward<I_V>(idx_visitor),
         col_name1,
         col_name2,
         col_name3](auto &triple) mutable -> void {
            _load_groupby_data_3_(*this,
                                  res,
                                  triple,
                                  idx_visitor,
                                  *gb_vec1,
                                  *gb_vec2,
                                  *gb_vec3,
                                  sort_v,
                                  col_name1,
                                  col_name2,
                                  col_name3);
        };

    for_each_in_tuple (args_tuple, func);
    return (res);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T, typename I_V, typename ... Ts>
std::future<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
DataFrame<I, H>::
groupby1_async(const char *col_name, I_V &&idx_visitor, Ts&& ... args) const {

    return (thr_pool_.dispatch(
        true,
        [col_name, idx_visitor = std::forward<I_V>(idx_visitor),
         ... args = std::forward<Ts>(args), this]() mutable -> DataFrame  {
            return (this->groupby1<T, I_V, Ts ...>(
                        col_name,
                        std::forward<I_V>(idx_visitor),
                        std::forward<Ts>(args) ...));
        }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T1, comparable T2, typename I_V, typename ... Ts>
std::future<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
DataFrame<I, H>::
groupby2_async(const char *col_name1,
               const char *col_name2,
               I_V &&idx_visitor,
               Ts&& ... args) const  {

    return (thr_pool_.dispatch(
        true,
        [col_name1, col_name2,
         idx_visitor = std::forward<I_V>(idx_visitor),
         ... args = std::forward<Ts>(args),
         this]() mutable -> DataFrame  {
            return (this->groupby2<T1, T2, I_V, Ts ...>(
                        col_name1,
                        col_name2,
                        std::forward<I_V>(idx_visitor),
                        std::forward<Ts>(args) ...));
        }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T1, comparable T2, comparable T3,
         typename I_V, typename ... Ts>
std::future<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
DataFrame<I, H>::
groupby3_async(const char *col_name1,
               const char *col_name2,
               const char *col_name3,
               I_V &&idx_visitor,
               Ts&& ... args) const  {

    return (thr_pool_.dispatch(
        true,
        [col_name1, col_name2, col_name3,
         idx_visitor = std::forward<I_V>(idx_visitor),
         ... args = std::forward<Ts>(args),
         this]() mutable -> DataFrame  {
            return (this->groupby3<T1, T2, T3, I_V, Ts ...>(
                        col_name1,
                        col_name2,
                        col_name3,
                        std::forward<I_V>(idx_visitor),
                        std::forward<Ts>(args) ...));
        }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T>
DataFrame<T, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::value_counts (const char *col_name) const  {

    const ColumnVecType<T>  &vec = get_column<T>(col_name);
    auto                    hash_func =
        [](std::reference_wrapper<const T> v) -> std::size_t  {
            return(std::hash<T>{}(v.get()));
    };
    auto                    equal_func =
        [](std::reference_wrapper<const T> lhs,
           std::reference_wrapper<const T> rhs) -> bool  {
            return(lhs.get() == rhs.get());
    };

    using map_t =
        DFUnorderedMap<typename std::reference_wrapper<const T>::type,
                       size_type,
                       decltype(hash_func),
                       decltype(equal_func)>;
    using res_t = DataFrame<T, HeteroVector<align_value>>;

    map_t       values_map(vec.size(), hash_func, equal_func);
    size_type   nan_count = 0;

    // take care of nans
    for (const auto &citer : vec) [[likely]]  {
        if (is_nan<T>(citer)) [[unlikely]]  {
            ++nan_count;
            continue;
        }

        auto    insert_result = values_map.emplace(std::ref(citer), 1);

        if (insert_result.second == false)
            insert_result.first->second += 1;
    }

    typename res_t::IndexVecType                        res_indices;
    typename res_t::template ColumnVecType<size_type>   counts;

    counts.reserve(values_map.size());
    res_indices.reserve(values_map.size());

    for (const auto &citer : values_map) [[likely]]  {
        res_indices.push_back(citer.first);
        counts.emplace_back(citer.second);
    }
    if (nan_count > 0)  {
        res_indices.push_back(get_nan<T>());
        counts.emplace_back(nan_count);
    }

    res_t   result_df;

    result_df.load_index(std::move(res_indices));
    result_df.template load_column<size_type>("counts", std::move(counts));

    return(result_df);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<hashable_equal T>
DataFrame<T, HeteroVector<std::size_t(H::align_value)>>
DataFrame<I, H>::value_counts(size_type index) const  {

    return (value_counts<T>(column_list_[index].first.c_str()));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<binary_array T>
typename
DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
    StlVecType<char>
DataFrame<I, H>::starts_with(const char *col_name, const T &pattern) const  {

    using res_t = StlVecType<char>;

    const ColumnVecType<T>  *vec { nullptr };

    if (! ::strcmp(col_name, DF_INDEX_COL_NAME))
        vec = (const ColumnVecType<T> *) &(get_index());
    else
        vec = (const ColumnVecType<T> *) &(get_column<T>(col_name));

    const size_type col_s = vec->size();
    res_t           result (col_s, char{ 0 });
    const auto      thread_level =
        (col_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();
    auto            lbd =
        [&result, vec, &pattern = std::as_const(pattern)]
        (size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto      &val = (*vec)[idx];
                const size_type s = pattern.size();

                if (val.size() >= s)
                    result[idx] =
                        char(! std::memcmp(val.data(), pattern.data(), s));
            }
        };

    if (thread_level > 2)  {
        auto    futures =
            thr_pool_.parallel_loop(size_type(0), col_s, std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  lbd(size_type(0), col_s);

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<binary_array T>
typename
DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
    StlVecType<char>
DataFrame<I, H>::ends_with(const char *col_name, const T &pattern) const  {

    using res_t = StlVecType<char>;

    const ColumnVecType<T>  *vec { nullptr };

    if (! ::strcmp(col_name, DF_INDEX_COL_NAME))
        vec = (const ColumnVecType<T> *) &(get_index());
    else
        vec = (const ColumnVecType<T> *) &(get_column<T>(col_name));

    const size_type col_s = vec->size();
    res_t           result (col_s, char{ 0 });
    const auto      thread_level =
        (col_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();
    auto            lbd =
        [&result, vec, &pattern = std::as_const(pattern)]
        (size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto      &val = (*vec)[idx];
                const size_type p_s = pattern.size();
                const size_type v_s = val.size();

                if (v_s >= p_s)
                    result[idx] =
                        char(! std::memcmp(val.data() + (v_s - p_s),
                                           pattern.data(),
                                           p_s));
            }
        };

    if (thread_level > 2)  {
        auto    futures =
            thr_pool_.parallel_loop(size_type(0), col_s, std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  lbd(size_type(0), col_s);

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T>
typename
DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
    StlVecType<char>
DataFrame<I, H>::in_between(const char *col_name,
                            const T &lower_bound,
                            const T &upper_bound) const  {

    using res_t = StlVecType<char>;

    const ColumnVecType<T>  *vec { nullptr };

    if (! ::strcmp(col_name, DF_INDEX_COL_NAME))
        vec = (const ColumnVecType<T> *) &(get_index());
    else
        vec = (const ColumnVecType<T> *) &(get_column<T>(col_name));

    const size_type col_s = vec->size();
    res_t           result (col_s, char{ 0 });
    const auto      thread_level =
        (col_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();
    auto            lbd =
        [&result, vec,
         &lower_bound = std::as_const(lower_bound),
         &upper_bound = std::as_const(upper_bound)]
        (size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto  &val = (*vec)[idx];

                if (val >= lower_bound && val < upper_bound)
                    result[idx] = char{ 1 };
            }
        };

    if (thread_level > 2)  {
        auto    futures =
            thr_pool_.parallel_loop(size_type(0), col_s, std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  lbd(size_type(0), col_s);

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T>
typename
DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
    StlVecType<char>
DataFrame<I, H>::peaks(const char *col_name, size_type n) const  {

    using res_t = StlVecType<char>;

    const ColumnVecType<T>  *vec { nullptr };

    if (! ::strcmp(col_name, DF_INDEX_COL_NAME))
        vec = (const ColumnVecType<T> *) &(get_index());
    else
        vec = (const ColumnVecType<T> *) &(get_column<T>(col_name));

    const size_type col_s = vec->size();

#ifdef HMDF_SANITY_EXCEPTIONS
    if ((col_s <= (2 * n + 1)) || n < 1)
        throw DataFrameError("peaks: column size must be >= 2n+1 and n > 0");
#endif // HMDF_SANITY_EXCEPTIONS

    res_t       result (col_s, char{ 0 });
    const auto  thread_level =
        (col_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();
    auto        lbd1 =
        [&result, vec](size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto  &val = (*vec)[idx];

                if (val > (*vec)[idx + 1] && val > (*vec)[idx - 1])
                    result[idx] = char{ 1 };
            }
        };
    auto        lbd2 =
        [&result, vec](size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto  &val = (*vec)[idx];

                if (val > (*vec)[idx + 1] &&
                    val > (*vec)[idx + 2] &&
                    val > (*vec)[idx - 1] &&
                    val > (*vec)[idx - 2])
                    result[idx] = char{ 1 };
            }
        };
    auto        lbdn =
        [&result, vec, n](size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto  &val = (*vec)[idx];
                char        c { 1 };

                for (size_type j = 1; j <= n; ++j)  {
                    if (val <= (*vec)[idx + j] || val <= (*vec)[idx - j])  {
                        c = 0;
                        break;
                    }
                }
                result[idx] = c;
            }
        };

    if (thread_level > 2)  {
        std::vector<std::future<void>>  futures;

        if (n == 1)
            futures = thr_pool_.parallel_loop(n, col_s - n, std::move(lbd1));
        else if (n == 2)
            futures = thr_pool_.parallel_loop(n, col_s - n, std::move(lbd2));
        else
            futures = thr_pool_.parallel_loop(n, col_s - n, std::move(lbdn));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        if (n == 1)
            lbd1(n, col_s - n);
        else if (n == 2)
            lbd2(n, col_s - n);
        else
            lbdn(n, col_s - n);
    }

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<comparable T>
typename
DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
    StlVecType<char>
DataFrame<I, H>::valleys(const char *col_name, size_type n) const  {

    using res_t = StlVecType<char>;

    const ColumnVecType<T>  *vec { nullptr };

    if (! ::strcmp(col_name, DF_INDEX_COL_NAME))
        vec = (const ColumnVecType<T> *) &(get_index());
    else
        vec = (const ColumnVecType<T> *) &(get_column<T>(col_name));

    const size_type col_s = vec->size();

#ifdef HMDF_SANITY_EXCEPTIONS
    if ((col_s <= (2 * n + 1)) || n < 1)
        throw DataFrameError("valleys: column size must be >= 2n+1 and n > 0");
#endif // HMDF_SANITY_EXCEPTIONS

    res_t       result (col_s, char{ 0 });
    const auto  thread_level =
        (col_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();
    auto        lbd1 =
        [&result, vec](size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto  &val = (*vec)[idx];

                if (val < (*vec)[idx + 1] && val < (*vec)[idx - 1])
                    result[idx] = char{ 1 };
            }
        };
    auto        lbd2 =
        [&result, vec](size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto  &val = (*vec)[idx];

                if (val < (*vec)[idx + 1] &&
                    val < (*vec)[idx + 2] &&
                    val < (*vec)[idx - 1] &&
                    val < (*vec)[idx - 2])
                    result[idx] = char{ 1 };
            }
        };
    auto        lbdn =
        [&result, vec, n](size_type begin, size_type end) -> void  {
            for (auto idx = begin; idx < end; ++idx)  {
                const auto  &val = (*vec)[idx];
                char        c { 1 };

                for (size_type j = 1; j <= n; ++j)  {
                    if (val >= (*vec)[idx + j] || val >= (*vec)[idx - j])  {
                        c = 0;
                        break;
                    }
                }
                result[idx] = c;
            }
        };

    if (thread_level > 2)  {
        std::vector<std::future<void>>  futures;

        if (n == 1)
            futures = thr_pool_.parallel_loop(n, col_s - n, std::move(lbd1));
        else if (n == 2)
            futures = thr_pool_.parallel_loop(n, col_s - n, std::move(lbd2));
        else
            futures = thr_pool_.parallel_loop(n, col_s - n, std::move(lbdn));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        if (n == 1)
            lbd1(n, col_s - n);
        else if (n == 2)
            lbd2(n, col_s - n);
        else
            lbdn(n, col_s - n);
    }

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename V, typename I_V, typename ... Ts>
DataFrame<I, HeteroVector<std::size_t(H::align_value)>> DataFrame<I, H>::
bucketize(bucket_type bt,
          const V &value,
          I_V &&idx_visitor,
          Ts&& ... args) const  {

    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    res_t           result;
    auto            &dst_idx = result.get_index();
    const auto      &src_idx = get_index();
    const size_type idx_s = src_idx.size();

    _bucketize_core_(dst_idx, src_idx, src_idx, value, idx_visitor, idx_s, bt);

    std::vector<std::future<void>>  futures;
    const auto                      thread_level =
        (indices_.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : get_thread_level();

    if (thread_level > 2)  futures.reserve(column_list_.size());

    auto    args_tuple = std::tuple<Ts ...>(args ...);
    auto    func =
        [this, &result, &value = std::as_const(value), &futures, bt]
        (auto &triple) mutable -> void {
            _load_bucket_data_(*this, result, value, bt, triple, futures);
        };

    const SpinGuard guard(lock_);

    for_each_in_tuple (args_tuple, func);
    for (auto &fut : futures)  fut.get();
    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename V, typename I_V, typename ... Ts>
std::future<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
DataFrame<I, H>::
bucketize_async(bucket_type bt,
                const V &value,
                I_V &&idx_visitor,
                Ts&& ... args) const  {

    using res_t = DataFrame<I, HeteroVector<std::size_t(H::align_value)>>;

    return (thr_pool_.dispatch(
        true,
        [bt,
         &value,
         idx_visitor = std::forward<I_V>(idx_visitor),
         ... args = std::forward<Ts>(args),
         this]() mutable -> res_t  {
            return (this->bucketize<V, I_V, Ts ...>(
                        bt,
                        std::cref(value),
                        std::forward<I_V>(idx_visitor),
                        std::forward<Ts>(args) ...));
        }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
void
DataFrame<I, H>::make_stationary(const char *col_name,
                                 stationary_method method,
                                 const StationaryParams params)  {

    ColumnVecType<T>    &vec = get_column<T>(col_name);
    const size_type     col_s = vec.size();

#ifdef HMDF_SANITY_EXCEPTIONS
    if (col_s < 3)
        throw DataFrameError("make_stationary: Time-series is too short");
#endif // HMDF_SANITY_EXCEPTIONS

    const auto  thread_level =
        (col_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (method == stationary_method::differencing)  {
        ColumnVecType<T>    result(col_s);
        auto                lbd = [&result, &vec = std::as_const(vec)]
                                  (auto begin, auto end) -> void  {
            for (auto i = begin; i < end; ++i)
                result[i] = vec[i] - vec[i - 1];
        };

        if (thread_level > 2)  {
            auto    futures =
                thr_pool_.parallel_loop(size_type(1), col_s, std::move(lbd));

            for (auto &fut : futures)  fut.get();
        }
        else  lbd(size_type(1), col_s);
        vec = std::move(result);
        vec[0] = vec[1];
    }
    else if (method == stationary_method::log_trans)  {
        auto    lbd = [](auto begin, auto end) -> void  {
            for (auto citer = begin; citer < end; ++citer)
                *citer = std::log(*citer);
        };

        if (thread_level > 2)  {
            auto    futures =
                thr_pool_.parallel_loop(vec.begin(), vec.end(), std::move(lbd));

            for (auto &fut : futures)  fut.get();
        }
        else  lbd(vec.begin(), vec.end());
    }
    else if (method == stationary_method::sqrt_trans)  {
        auto    lbd = [](auto begin, auto end) -> void  {
            for (auto citer = begin; citer < end; ++citer)
                *citer = std::sqrt(*citer);
        };

        if (thread_level > 2)  {
            auto    futures =
                thr_pool_.parallel_loop(vec.begin(), vec.end(), std::move(lbd));

            for (auto &fut : futures)  fut.get();
        }
        else  lbd(vec.begin(), vec.end());
    }
    else if (method == stationary_method::boxcox_trans)  {
        bcox_v<T, I, align_value>   boxcox { params.bc_type,
                                             params.lambda,
                                             params.is_all_positive };

        boxcox.pre();
        boxcox(indices_.begin(), indices_.end(), vec.begin(), vec.end());
        boxcox.post();
        vec = std::move(boxcox.get_result());
    }
    else if (method == stationary_method::decomposition)  {
        decom_v<T, I, align_value>  decom { params.season_period,
                                            params.dcom_fraction,
                                            params.dcom_delta,
                                            params.dcom_type };

        decom.pre();
        decom(indices_.begin(), indices_.end(), vec.begin(), vec.end());
        decom.post();
        vec = std::move(decom.get_residual());
    }
    else if (method == stationary_method::smoothing)  {
        ewm_v<T, I, align_value>    ewm { params.decay_spec,
                                          params.decay_alpha,
                                          params.finite_adjust };

        ewm.pre();
        ewm(indices_.begin(), indices_.end(), vec.begin(), vec.end());
        ewm.post();
        vec = std::move(ewm.get_result());
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename V>
DataFrame<I, H> DataFrame<I, H>::
transpose(IndexVecType &&indices, const V &new_col_names) const  {

    const size_type num_cols = column_list_.size();

    if (new_col_names.size() != indices_.size())
        throw InconsistentData ("DataFrame::transpose(): ERROR: "
                                "Length of new_col_names is not equal "
                                "to number of rows");

    StlVecType<const ColumnVecType<T> *>    current_cols;

    current_cols.reserve(num_cols);
    for (const auto &citer : column_list_)
        current_cols.push_back(&(get_column<T>(citer.first.c_str())));

    StlVecType<StlVecType<T>>   trans_cols(indices_.size());
    DataFrame                   df;

    for (size_type i = 0; i < indices_.size(); ++i)  {
        trans_cols[i].reserve(num_cols);
        for (size_type j = 0; j < num_cols; ++j)  {
            if (current_cols[j]->size() > i)
                trans_cols[i].push_back((*(current_cols[j]))[i]);
            else
                trans_cols[i].push_back(get_nan<T>());
        }
    }

    df.load_index(std::move(indices));
    for (size_type i = 0; i < new_col_names.size(); ++i)
        df.template load_column<T>(&(new_col_names[i][0]),
                                   std::move(trans_cols[i]));

    return (df);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
