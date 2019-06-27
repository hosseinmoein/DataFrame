// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>

#include <unordered_set>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename  H>
template<typename T>
typename type_declare<H, T>::type &
DataFrame<I, H>::get_column (const char *name)  {

    auto iter = column_tb_.find (name);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer, "DataFrame::get_column(): ERROR: "
                         "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    DataVec &hv = data_[iter->second];

    return (hv.template get_vector<T>());
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
const typename type_declare<H, T>::type &
DataFrame<I, H>::get_column (const char *name) const  {

    return (const_cast<DataFrame *>(this)->get_column<T>(name));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<size_t N, typename ... Ts>
HeteroVector DataFrame<I, H>::
get_row(size_type row_num, const std::array<const char *, N> col_names) const {

    HeteroVector ret_vec;

    if (row_num >= indices_.size())  {
        char buffer [512];

#ifdef _WIN32
        sprintf(buffer, "DataFrame::get_row(): ERROR: There aren't %zu rows",
#else
        sprintf(buffer, "DataFrame::get_row(): ERROR: There aren't %lu rows",
#endif // _WIN32
                row_num);
        throw BadRange(buffer);
    }

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

        data_[citer->second].change(functor);
    }

    return (ret_vec);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T>
std::vector<T> DataFrame<I, H>::
get_col_unique_values(const char *name) const  {

    auto  iter = column_tb_.find (name);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_col_unique_values(): "
                 "ERROR: Cannot find column '%s'",
                 name);
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

    std::unordered_set<
        typename std::reference_wrapper<T>::type,
        decltype(hash_func),
        decltype(equal_func)>   table(vec.size(), hash_func, equal_func);
    bool                        counted_nan = false;
    std::vector<T>              result;

    result.reserve(vec.size());
    for (auto citer : vec)  {
        if (_is_nan<T>(citer) && ! counted_nan)  {
            counted_nan = true;
            result.push_back(_get_nan<T>());
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
void DataFrame<I, H>::multi_visit (Ts ... args)  {

    auto    args_tuple = std::tuple<Ts ...>(args ...);
    auto    fc = [this](auto &pa) mutable -> void {
        auto &functor = *(pa.second);

        using T =
            typename std::remove_reference<decltype(functor)>::type::value_type;
        using V =
            typename std::remove_const<
                typename std::remove_reference<decltype(functor)>::type>::type;

        this->visit<T, V>(pa.first, functor);
    };

    for_each_in_tuple_ (args_tuple, fc);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
V &DataFrame<I, H>::visit (const char *name, V &visitor)  {

    const auto  iter = column_tb_.find (name);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(1): ERROR: Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    DataVec         &hv = data_[iter->second];
    std::vector<T>  &vec = hv.template get_vector<T>();
    const size_type idx_s = indices_.size();
    const size_type data_s = vec.size();

    visitor.pre();
    for (size_type i = 0; i < idx_s; ++i)
        visitor (indices_[i], i < data_s ? vec[i] : _get_nan<T>());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
V &DataFrame<I, H>::
visit (const char *name1, const char *name2, V &visitor)  {

    const auto  iter1 = column_tb_.find (name1);
    const auto  iter2 = column_tb_.find (name2);

    if (iter1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(2): ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(2): ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }

    DataVec         &hv1 = data_[iter1->second];
    DataVec         &hv2 = data_[iter2->second];
    std::vector<T1> &vec1 = hv1.template get_vector<T1>();
    std::vector<T2> &vec2 = hv2.template get_vector<T2>();
    const size_type idx_s = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();

    visitor.pre();
    for (size_type i = 0; i < idx_s; ++i)
        visitor (indices_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
V &DataFrame<I, H>::
visit (const char *name1, const char *name2, const char *name3, V &visitor)  {

    const auto  iter1 = column_tb_.find (name1);
    const auto  iter2 = column_tb_.find (name2);
    const auto  iter3 = column_tb_.find (name3);

    if (iter1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(3): ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(3): ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }
    if (iter3 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(3): ERROR: Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }

    DataVec         &hv1 = data_[iter1->second];
    DataVec         &hv2 = data_[iter2->second];
    DataVec         &hv3 = data_[iter3->second];
    std::vector<T1> &vec1 = hv1.template get_vector<T1>();
    std::vector<T2> &vec2 = hv2.template get_vector<T2>();
    std::vector<T3> &vec3 = hv3.template get_vector<T3>();
    const size_type idx_s = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();

    visitor.pre();
    for (size_type i = 0; i < idx_s; ++i)
        visitor (indices_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>(),
                 i < data_s3 ? vec3[i] : _get_nan<T3>());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename V>
V &DataFrame<I, H>::
visit (const char *name1,
       const char *name2,
       const char *name3,
       const char *name4,
       V &visitor)  {

    const auto  iter1 = column_tb_.find (name1);
    const auto  iter2 = column_tb_.find (name2);
    const auto  iter3 = column_tb_.find (name3);
    const auto  iter4 = column_tb_.find (name4);

    if (iter1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(4): ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(4): ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }
    if (iter3 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(4): ERROR: Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }
    if (iter4 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(4): ERROR: Cannot find column '%s'",
                 name4);
        throw ColNotFound (buffer);
    }

    DataVec         &hv1 = data_[iter1->second];
    DataVec         &hv2 = data_[iter2->second];
    DataVec         &hv3 = data_[iter3->second];
    DataVec         &hv4 = data_[iter4->second];
    std::vector<T1> &vec1 = hv1.template get_vector<T1>();
    std::vector<T2> &vec2 = hv2.template get_vector<T2>();
    std::vector<T3> &vec3 = hv3.template get_vector<T3>();
    std::vector<T4> &vec4 = hv4.template get_vector<T4>();
    const size_type idx_s = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type data_s4 = vec4.size();

    visitor.pre();
    for (size_type i = 0; i < idx_s; ++i)
        visitor (indices_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>(),
                 i < data_s3 ? vec3[i] : _get_nan<T3>(),
                 i < data_s4 ? vec4[i] : _get_nan<T4>());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
V &DataFrame<I, H>::
visit (const char *name1,
       const char *name2,
       const char *name3,
       const char *name4,
       const char *name5,
       V &visitor)  {

    const auto  iter1 = column_tb_.find (name1);
    const auto  iter2 = column_tb_.find (name2);
    const auto  iter3 = column_tb_.find (name3);
    const auto  iter4 = column_tb_.find (name4);
    const auto  iter5 = column_tb_.find (name5);

    if (iter1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }
    if (iter3 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }
    if (iter4 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name4);
        throw ColNotFound (buffer);
    }
    if (iter5 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name5);
        throw ColNotFound (buffer);
    }

    DataVec         &hv1 = data_[iter1->second];
    DataVec         &hv2 = data_[iter2->second];
    DataVec         &hv3 = data_[iter3->second];
    DataVec         &hv4 = data_[iter4->second];
    DataVec         &hv5 = data_[iter5->second];
    std::vector<T1> &vec1 = hv1.template get_vector<T1>();
    std::vector<T2> &vec2 = hv2.template get_vector<T2>();
    std::vector<T3> &vec3 = hv3.template get_vector<T3>();
    std::vector<T4> &vec4 = hv4.template get_vector<T4>();
    std::vector<T5> &vec5 = hv5.template get_vector<T5>();
    const size_type idx_s = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type data_s4 = vec4.size();
    const size_type data_s5 = vec5.size();

    visitor.pre();
    for (size_type i = 0; i < idx_s; ++i)
        visitor (indices_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>(),
                 i < data_s3 ? vec3[i] : _get_nan<T3>(),
                 i < data_s4 ? vec4[i] : _get_nan<T4>(),
                 i < data_s5 ? vec5[i] : _get_nan<T5>());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name, V &visitor) const  {

    const auto  iter = column_tb_.find (name);

    if (iter == column_tb_.end())  {
        char buffer [512];

        sprintf(buffer,
                "DataFrame::single_act_visit: ERROR: Cannot find column '%s'",
                name);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv = data_[iter->second];
    const std::vector<T>    &vec = hv.template get_vector<T>();

    visitor.pre();
    visitor (indices_, vec);
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1, const char *name2, V &visitor)  {

    const auto  iter1 = column_tb_.find (name1);
    const auto  iter2 = column_tb_.find (name2);

    if (iter1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::single_act_visit(2): "
                 "ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::single_act_visit(2): "
                 "ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[iter1->second];
    const DataVec           &hv2 = data_[iter2->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();

    visitor.pre();
    visitor (indices_, vec1, vec2);
    visitor.post();

    return (visitor);
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

        for (auto &iter : column_tb_)  {
            load_functor_<Ts ...>   functor (iter.first.c_str(),
                                             b_dist,
                                             e_dist,
                                             df);

            data_[iter.second].change(functor);
        }
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrameView<I>
DataFrame<I, H>::get_view_by_idx (Index2D<IndexType> range)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_idx()");

    const auto          &lower =
        std::lower_bound (indices_.begin(), indices_.end(), range.begin);
    const auto          &upper =
        std::upper_bound (indices_.begin(), indices_.end(), range.end);
    DataFrameView<IndexType>    dfv;

    if (lower != indices_.end())  {
        dfv.indices_ =
            typename DataFrameView<IndexType>::IndexVecType(&*lower, &*upper);

        const size_type b_dist = std::distance(indices_.begin(), lower);
        const size_type e_dist = std::distance(indices_.begin(),
                                               upper < indices_.end()
                                                   ? upper
                                                   : indices_.end());

        for (auto &iter : column_tb_)  {
            view_setup_functor_<Ts ...> functor (iter.first.c_str(),
                                                 b_dist,
                                                 e_dist,
                                                 dfv);

            data_[iter.second].change(functor);
        }
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrame<I, H>
DataFrame<I, H>::get_data_by_loc (Index2D<int> range) const  {

    if (range.begin < 0)
        range.begin = static_cast<int>(indices_.size()) + range.begin;
    if (range.end < 0)
        range.end = static_cast<int>(indices_.size()) + range.end;

    if (range.end <= static_cast<int>(indices_.size()) &&
        range.begin <= range.end && range.begin >= 0)  {
        DataFrame   df;

        df.load_index(indices_.begin() + static_cast<size_type>(range.begin),
                      indices_.begin() + static_cast<size_type>(range.end));

        for (auto &iter : column_tb_)  {
            load_functor_<Ts ...>   functor (
                iter.first.c_str(),
                static_cast<size_type>(range.begin),
                static_cast<size_type>(range.end),
                df);

            data_[iter.second].change(functor);
        }

        return (df);
    }

    char buffer [512];

    sprintf (buffer,
             "DataFrame::get_data_by_loc(): ERROR: "
             "Bad begin, end range: %d, %d",
             range.begin, range.end);
    throw BadRange (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
DataFrameView<I>
DataFrame<I, H>::get_view_by_loc (Index2D<int> range)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_loc()");

    if (range.begin < 0)
        range.begin = static_cast<int>(indices_.size()) + range.begin;
    if (range.end < 0)
        range.end = static_cast<int>(indices_.size()) + range.end;

    if (range.end <= static_cast<int>(indices_.size()) &&
        range.begin <= range.end && range.begin >= 0)  {
        DataFrameView<IndexType>    dfv;

        dfv.indices_ =
            typename DataFrameView<IndexType>::IndexVecType(
                &*(indices_.begin() + range.begin),
                &*(indices_.begin() + range.end));
        for (const auto &iter : column_tb_)  {
            view_setup_functor_<Ts ...> functor (
                iter.first.c_str(),
                static_cast<size_type>(range.begin),
                static_cast<size_type>(range.end),
                dfv);

            data_[iter.second].change(functor);
        }

        return (dfv);
    }

    char buffer [512];

    sprintf (buffer,
             "DataFrame::get_view_by_loc(): ERROR: "
             "Bad begin, end range: %d, %d",
             range.begin, range.end);
    throw BadRange (buffer);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename F, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
get_data_by_sel (const char *name, F &sel_functor) const  {

    const auto  citer = column_tb_.find (name);

    if (citer == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(1): ERROR: "
                 "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv = data_[citer->second];
    const std::vector<T>    &vec = hv.template get_vector<T>();
    const size_type         idx_s = indices_.size();
    const size_type         col_s = vec.size();
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i], vec[i]))
            col_indices.push_back(i);

    DataFrame       df;
    IndexVecType    new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(indices_[citer]);
    df.load_index(std::move(new_index));

    for (auto col_citer : column_tb_)  {
        sel_load_functor_<Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            df);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename F, typename ... Ts>
DataFramePtrView<I> DataFrame<I, H>::
get_view_by_sel (const char *name, F &sel_functor)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const auto  citer = column_tb_.find (name);

    if (citer == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(1): ERROR: "
                 "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv = data_[citer->second];
    const std::vector<T>    &vec = hv.template get_vector<T>();
    const size_type         idx_s = indices_.size();
    const size_type         col_s = vec.size();
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i], vec[i]))
            col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView                         dfv;
    typename TheView::IndexVecType  new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(&(indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : column_tb_)  {
        sel_load_view_functor_<Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            dfv);

        data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename F, typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
get_data_by_sel (const char *name1, const char *name2, F &sel_functor) const  {

    const auto  citer1 = column_tb_.find (name1);
    const auto  citer2 = column_tb_.find (name2);

    if (citer1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (citer2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[citer1->second];
    const DataVec           &hv2 = data_[citer2->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s = std::max(col_s1, col_s2);
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : _get_nan<T1>(),
                         i < col_s2 ? vec2[i] : _get_nan<T2>()))
            col_indices.push_back(i);

    DataFrame       df;
    IndexVecType    new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(indices_[citer]);
    df.load_index(std::move(new_index));

    for (auto col_citer : column_tb_)  {
        sel_load_functor_<Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            df);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename F, typename ... Ts>
DataFramePtrView<I> DataFrame<I, H>::
get_view_by_sel (const char *name1, const char *name2, F &sel_functor)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const auto  citer1 = column_tb_.find (name1);
    const auto  citer2 = column_tb_.find (name2);

    if (citer1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_view_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (citer2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_view_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[citer1->second];
    const DataVec           &hv2 = data_[citer2->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s = std::max(col_s1, col_s2);
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : _get_nan<T1>(),
                         i < col_s2 ? vec2[i] : _get_nan<T2>()))
            col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView                         dfv;
    typename TheView::IndexVecType  new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(&(indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : column_tb_)  {
        sel_load_view_functor_<Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            dfv);

        data_[col_citer.second].change(functor);
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

    const auto  citer1 = column_tb_.find (name1);
    const auto  citer2 = column_tb_.find (name2);
    const auto  citer3 = column_tb_.find (name3);

    if (citer1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (citer2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }
    if (citer3 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_data_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[citer1->second];
    const DataVec           &hv2 = data_[citer2->second];
    const DataVec           &hv3 = data_[citer3->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const std::vector<T3>   &vec3 = hv3.template get_vector<T3>();
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s3 = vec3.size();
    const size_type         col_s = std::max(std::max(col_s1, col_s2), col_s3);
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : _get_nan<T1>(),
                         i < col_s2 ? vec2[i] : _get_nan<T2>(),
                         i < col_s3 ? vec3[i] : _get_nan<T3>()))
            col_indices.push_back(i);

    DataFrame       df;
    IndexVecType    new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(indices_[citer]);
    df.load_index(std::move(new_index));

    for (auto col_citer : column_tb_)  {
        sel_load_functor_<Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            df);

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
                 F &sel_functor)  {

    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const auto  citer1 = column_tb_.find (name1);
    const auto  citer2 = column_tb_.find (name2);
    const auto  citer3 = column_tb_.find (name3);

    if (citer1 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_view_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (citer2 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_view_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }
    if (citer3 == column_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::get_view_by_sel(2): ERROR: "
                 "Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[citer1->second];
    const DataVec           &hv2 = data_[citer2->second];
    const DataVec           &hv3 = data_[citer3->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const std::vector<T3>   &vec3 = hv3.template get_vector<T3>();
    const size_type         col_s1 = vec1.size();
    const size_type         col_s2 = vec2.size();
    const size_type         col_s3 = vec3.size();
    const size_type         col_s = std::max(std::max(col_s1, col_s2), col_s3);
    std::vector<size_type>  col_indices;

    col_indices.reserve(indices_.size() / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor (indices_[i],
                         i < col_s1 ? vec1[i] : _get_nan<T1>(),
                         i < col_s2 ? vec2[i] : _get_nan<T2>(),
                         i < col_s3 ? vec3[i] : _get_nan<T3>()))
            col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView                         dfv;
    typename TheView::IndexVecType  new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer: col_indices)
        new_index.push_back(&(indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : column_tb_)  {
        sel_load_view_functor_<Ts ...>   functor (
            col_citer.first.c_str(),
            col_indices,
            dfv);

        data_[col_citer.second].change(functor);
    }

    return (dfv);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
