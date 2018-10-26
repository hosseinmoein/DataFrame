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

template<typename TS, typename  HETERO>
template<typename T>
std::vector<T> &DataFrame<TS, HETERO>::get_column (const char *name)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call get_column()");

    auto iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
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

template<typename TS, typename  HETERO>
template<typename T>
const std::vector<T> &DataFrame<TS, HETERO>::
get_column (const char *name) const  {

    return (const_cast<DataFrame *>(this)->get_column<T>(name));
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T>
VectorView<T> &DataFrame<TS, HETERO>::get_view_column (const char *name)  {

    static_assert(std::is_base_of<HeteroView, HETERO>::value,
                  "Only a DataFrameView can call get_view_column()");

    auto iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
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

template<typename TS, typename  HETERO>
template<typename T>
const VectorView<T> &DataFrame<TS, HETERO>::
get_view_column (const char *name) const  {

    return (const_cast<DataFrame *>(this)->get_view_column<T>(name));
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ... Ts>
void DataFrame<TS, HETERO>::multi_visit (Ts ... args) const  {

    auto    args_tuple = std::tuple<Ts ...>(args ...);
    auto    fc = [this](auto &pa) mutable -> void {

        auto &functor = *(pa.second);

        using T =
            typename std::remove_reference<decltype(functor)>::type::value_type;
        using V =
            typename std::remove_const<
                typename std::remove_reference<decltype(functor)>::type>::type;

        this->visit<T, V>(pa.first, const_cast<V &>(functor));
    };

    for_each_in_tuple_ (args_tuple, fc);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T, typename V>
V &DataFrame<TS, HETERO>::visit (const char *name, V &visitor) const  {

    const auto  iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(1): ERROR: Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv = data_[iter->second];
    const std::vector<T>    &vec = hv.template get_vector<T>();
    const size_type         idx_s = timestamps_.size();
    const size_type         data_s = vec.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i], i < data_s ? vec[i] : _get_nan<T>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T1, typename T2, typename V>
V &&DataFrame<TS, HETERO>::
visit (const char *name1, const char *name2, V &&visitor) const  {

    const auto  iter1 = data_tb_.find (name1);
    const auto  iter2 = data_tb_.find (name2);

    if (iter1 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(2): ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(2): ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[iter1->second];
    const DataVec           &hv2 = data_[iter2->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const size_type         idx_s = timestamps_.size();
    const size_type         data_s1 = vec1.size();
    const size_type         data_s2 = vec2.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T1, typename T2, typename T3, typename V>
V &&DataFrame<TS, HETERO>::
visit (const char *name1,
       const char *name2,
       const char *name3,
       V &&visitor) const  {

    const auto  iter1 = data_tb_.find (name1);
    const auto  iter2 = data_tb_.find (name2);
    const auto  iter3 = data_tb_.find (name3);

    if (iter1 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(3): ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(3): ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }
    if (iter3 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(3): ERROR: Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[iter1->second];
    const DataVec           &hv2 = data_[iter2->second];
    const DataVec           &hv3 = data_[iter3->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const std::vector<T3>   &vec3 = hv3.template get_vector<T3>();
    const size_type         idx_s = timestamps_.size();
    const size_type         data_s1 = vec1.size();
    const size_type         data_s2 = vec2.size();
    const size_type         data_s3 = vec3.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>(),
                 i < data_s3 ? vec3[i] : _get_nan<T3>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T1, typename T2, typename T3, typename T4, typename V>
V &&DataFrame<TS, HETERO>::
visit (const char *name1,
       const char *name2,
       const char *name3,
       const char *name4,
       V &&visitor) const  {

    const auto  iter1 = data_tb_.find (name1);
    const auto  iter2 = data_tb_.find (name2);
    const auto  iter3 = data_tb_.find (name3);
    const auto  iter4 = data_tb_.find (name4);

    if (iter1 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(4): ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(4): ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }
    if (iter3 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(4): ERROR: Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }
    if (iter4 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(4): ERROR: Cannot find column '%s'",
                 name4);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[iter1->second];
    const DataVec           &hv2 = data_[iter2->second];
    const DataVec           &hv3 = data_[iter3->second];
    const DataVec           &hv4 = data_[iter4->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const std::vector<T3>   &vec3 = hv3.template get_vector<T3>();
    const std::vector<T4>   &vec4 = hv4.template get_vector<T4>();
    const size_type         idx_s = timestamps_.size();
    const size_type         data_s1 = vec1.size();
    const size_type         data_s2 = vec2.size();
    const size_type         data_s3 = vec3.size();
    const size_type         data_s4 = vec4.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>(),
                 i < data_s3 ? vec3[i] : _get_nan<T3>(),
                 i < data_s4 ? vec4[i] : _get_nan<T4>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
V &&DataFrame<TS, HETERO>::
visit (const char *name1,
       const char *name2,
       const char *name3,
       const char *name4,
       const char *name5,
       V &&visitor) const  {

    const auto  iter1 = data_tb_.find (name1);
    const auto  iter2 = data_tb_.find (name2);
    const auto  iter3 = data_tb_.find (name3);
    const auto  iter4 = data_tb_.find (name4);
    const auto  iter5 = data_tb_.find (name5);

    if (iter1 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name1);
        throw ColNotFound (buffer);
    }
    if (iter2 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name2);
        throw ColNotFound (buffer);
    }
    if (iter3 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name3);
        throw ColNotFound (buffer);
    }
    if (iter4 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name4);
        throw ColNotFound (buffer);
    }
    if (iter5 == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(5): ERROR: Cannot find column '%s'",
                 name5);
        throw ColNotFound (buffer);
    }

    const DataVec           &hv1 = data_[iter1->second];
    const DataVec           &hv2 = data_[iter2->second];
    const DataVec           &hv3 = data_[iter3->second];
    const DataVec           &hv4 = data_[iter4->second];
    const DataVec           &hv5 = data_[iter5->second];
    const std::vector<T1>   &vec1 = hv1.template get_vector<T1>();
    const std::vector<T2>   &vec2 = hv2.template get_vector<T2>();
    const std::vector<T3>   &vec3 = hv3.template get_vector<T3>();
    const std::vector<T4>   &vec4 = hv4.template get_vector<T4>();
    const std::vector<T5>   &vec5 = hv5.template get_vector<T5>();
    const size_type         idx_s = timestamps_.size();
    const size_type         data_s1 = vec1.size();
    const size_type         data_s2 = vec2.size();
    const size_type         data_s3 = vec3.size();
    const size_type         data_s4 = vec4.size();
    const size_type         data_s5 = vec5.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>(),
                 i < data_s3 ? vec3[i] : _get_nan<T3>(),
                 i < data_s4 ? vec4[i] : _get_nan<T4>(),
                 i < data_s5 ? vec5[i] : _get_nan<T5>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ... types>
DataFrame<TS, HETERO>
DataFrame<TS, HETERO>::get_data_by_idx (Index2D<TS> range) const  {

    const auto  &lower =
        std::lower_bound (timestamps_.begin(), timestamps_.end(), range.begin);
    const auto  &upper =
        std::upper_bound (timestamps_.begin(), timestamps_.end(), range.end);
    DataFrame   df;

    if (lower != timestamps_.end())  {
        df.load_index(lower, upper);

        const size_type b_dist = std::distance(timestamps_.begin(), lower);
        const size_type e_dist = std::distance(timestamps_.begin(),
                                               upper < timestamps_.end()
                                                   ? upper
                                                   : timestamps_.end());

        for (auto &iter : data_tb_)  {
            load_functor_<types ...> functor (iter.first.c_str(),
                                              b_dist,
                                              e_dist,
                                              df);

            data_[iter.second].change(functor);
        }
    }

    return (df);
}

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
template<typename ... types>
DataFrame<TS, HETERO>
DataFrame<TS, HETERO>::get_data_by_loc (Index2D<int> range) const  {

    if (range.begin < 0)
        range.begin = static_cast<int>(timestamps_.size()) + range.begin;
    if (range.end < 0)
        range.end = static_cast<int>(timestamps_.size()) + range.end;

    if (range.end <= static_cast<int>(timestamps_.size()) &&
        range.begin <= range.end && range.begin >= 0)  {
        DataFrame   df;

        df.load_index(timestamps_.begin() + static_cast<size_type>(range.begin),
                      timestamps_.begin() + static_cast<size_type>(range.end));

        for (auto &iter : data_tb_)  {
            load_functor_<types ...> functor (
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

template<typename TS, typename  HETERO>
template<typename ... types>
DataFrameView<TS>
DataFrame<TS, HETERO>::get_view_by_loc (Index2D<int> range)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call get_view_by_loc()");

    if (range.begin < 0)
        range.begin = static_cast<int>(timestamps_.size()) + range.begin;
    if (range.end < 0)
        range.end = static_cast<int>(timestamps_.size()) + range.end;

    if (range.end <= static_cast<int>(timestamps_.size()) &&
        range.begin <= range.end && range.begin >= 0)  {
        DataFrameView<TS>   dfv;

        dfv.timestamps_ =
            typename DataFrameView<TS>::TSVec(
                &*(timestamps_.begin() + range.begin),
                &*(timestamps_.begin() + range.end - 1));
        for (auto &iter : data_tb_)  {
            view_setup_functor_<types ...>  functor (
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
