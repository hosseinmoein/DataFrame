// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DataFrame.h"
#include <type_traits>
#include <limits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename TS, template<typename DT, class... types> class DS>
template<typename T>
inline constexpr T DataFrame<TS, DS>::_get_nan()  {

    if (std::numeric_limits<T>::has_quiet_NaN)
        return (std::numeric_limits<T>::quiet_NaN());
    return (T());
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... types>
void DataFrame<TS, DS>::make_consistent ()  {

    const size_type                 idx_s = timestamps_.size();
    consistent_functor_<types ...>  functor (idx_s);

    for (auto &iter : data_)
        iter.change(functor);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T, typename ... types>
void DataFrame<TS, DS>::sort(const char *by_name)  {

    make_consistent<types ...>();

    if (by_name == nullptr)  {
        sort_functor_<TimeStamp, types ...> functor (timestamps_);

        for (auto &iter : data_)
            iter.change(functor);

        std::sort (timestamps_.begin(), timestamps_.end());
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
        DS<T>                       &idx_vec = hv.get_vector<T>();
        sort_functor_<T, types ...> functor (idx_vec);

        for (size_type i = 0; i < data_.size(); ++i)
            if (i != iter->second)
                data_[i].change(functor);
        functor(timestamps_);

        std::sort (idx_vec.begin(), idx_vec.end());
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T, typename ... types>
std::future<void> DataFrame<TS, DS>::sort_async(const char *by_name)  {

    return (std::async(std::launch::async,
                       &DataFrame::sort<T, types ...>,
                       this, by_name));
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename F, typename T, typename ... types>
DataFrame<TS, DS>
DataFrame<TS, DS>:: groupby (F &&func,
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

    const size_type vec_size = tmp_df.timestamps_.size();
    size_type       marker = 0;

    if (gb_col_name == nullptr)  { // Index
        for (size_type i = 0; i < vec_size; ++i)  {
            if (tmp_df.timestamps_[i] != tmp_df.timestamps_[marker])  {
                df.append_index(tmp_df.timestamps_[marker]);
                for (const auto &iter : tmp_df.data_tb_)  {
                    groupby_functor_<F, types...>   functor(
                                                iter.first.c_str(),
                                                marker,
                                                i,
                                                tmp_df.timestamps_[marker],
                                                func,
                                                df);

                    tmp_df.data_[iter.second].change(functor);
                    func.reset();
                }

                marker = i;
            }
        }
        if (marker < vec_size)  {
            df.append_index(tmp_df.timestamps_[vec_size - 1]);
            for (const auto &iter : tmp_df.data_tb_)  {
                groupby_functor_<F, types...>   functor(
                                            iter.first.c_str(),
                                            vec_size - 1,
                                            vec_size,
                                            tmp_df.timestamps_[vec_size - 1],
                                            func,
                                            df);

                tmp_df.data_[iter.second].change(functor);
            }
        }
    }
    else  { // Non-index column
        const DS<T> &gb_vec = tmp_df.get_column<T>(gb_col_name);

        for (size_type i = 0; i < vec_size; ++i)  {
            if (gb_vec[i] != gb_vec[marker])  {
                groupby_functor_<F, TimeStamp>  ts_functor(
                                            "INDEX",
                                            marker,
                                            i,
                                            tmp_df.timestamps_[marker],
                                            func,
                                            df);

                ts_functor(tmp_df.timestamps_);
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
                                                    tmp_df.timestamps_[marker],
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
                                        tmp_df.timestamps_[vec_size - 1],
                                        func,
                                        df);

            ts_functor(tmp_df.timestamps_);
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
                                            tmp_df.timestamps_[vec_size - 1],
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

template<typename TS, template<typename DT, class... types> class DS>
template<typename F, typename T, typename ... types>
std::future<DataFrame<TS, DS>>
DataFrame<TS, DS>::groupby_async (F &&func,
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

template<typename TS, template<typename DT, class... types> class DS>
template<typename F, typename ... types>
DataFrame<TS, DS>
DataFrame<TS, DS>::
bucketize (F &&func, const TimeStamp &bucket_interval) const  {

    DataFrame   df;

    for (const auto &iter : data_tb_)  {
        add_col_functor_<types ...> functor (iter.first.c_str(), df);

        data_[iter.second].change(functor);
    }

    for (const auto &iter : data_tb_)  {
        bucket_functor_<F, types...>   functor(
                                    iter.first.c_str(),
                                    timestamps_,
                                    bucket_interval,
                                    func,
                                    df);

        data_[iter.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename F, typename ... types>
std::future<DataFrame<TS, DS>>
DataFrame<TS, DS>::
bucketize_async (F &&func, const TimeStamp &bucket_interval) const  {

    return (std::async(std::launch::async,
                       &DataFrame::bucketize<F, types ...>,
                           this,
                           std::move(func),
                           std::cref(bucket_interval)));
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T, typename V>
DataFrame<TS, DS>
DataFrame<TS, DS>::transpose(TSVec &&indices, const V &col_names) const  {

    const size_type             num_cols = data_tb_.size();
    std::vector<const DS<T> *>  current_cols;

    current_cols.reserve(num_cols);
    for (const auto citer : data_tb_)  {
        const DataVec   &hv = data_[citer.second];

        current_cols.push_back(&(hv.get_vector<T>()));
    }

    if (col_names.size() != timestamps_.size())
        throw InconsistentData ("DataFrame::transpose(): ERROR: "
                                "Length of col_names is not equal "
                                "to number of rows");
    if (indices.size() != num_cols)
        throw InconsistentData ("DataFrame::transpose(): ERROR: "
                                "Length of index is not equal "
                                "to number of columns");

    std::vector<std::vector<T>> trans_cols(timestamps_.size());
    DataFrame                   df;
    size_type                   idx = 0;

    for (size_type i = 0; i < timestamps_.size(); ++i)  {
        trans_cols[i].reserve(num_cols);
        for (size_type j = 0; j < num_cols; ++j)  {
            if (current_cols[j]->size() > i)
                trans_cols[i].push_back((*(current_cols[j]))[i]);
            else
                trans_cols[i].push_back(_get_nan<T>());
        }
    }

    df.load_index(std::move(indices));
    for (size_type i = 0; i < col_names.size(); ++i)
        df.load_column(col_names[i].c_str(), std::move(trans_cols[i]));

    return (df);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename S, typename ... types>
bool DataFrame<TS, DS>::write (S &o, bool values_only) const  {

    if (! values_only)  o << "INDEX:";
    for (size_type i = 0; i < timestamps_.size(); ++i)
        o << timestamps_[i] << ',';
    o << '\n';

    for (const auto &iter : data_tb_)  {
        print_functor_<types ...> functor (iter.first.c_str(), values_only, o);

        data_[iter.second].change(functor);
    }

    o << std::endl;
    return (true);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename S, typename ... Ts>
std::future<bool> DataFrame<TS, DS>::
write_async (S &o, bool values_only) const  {

    return (std::async(std::launch::async,
                       &DataFrame::write<S, Ts ...>,
                           this,
                           std::ref(o),
                           values_only));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
