// Hossein Moein
// September 12, 2017
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DataFrame.h"
#include <type_traits>
#include <limits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename TS, template<typename DT, class... types> class DS>
template<typename T>
DS<T> &DataFrame<TS, DS>::get_column (const char *name)  {

    auto iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer, "DataFrame::get_column(): ERROR: "
                         "Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    DataVec &hv = data_[iter->second];

    return (hv.get_vec<T, DS<T>>());
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T>
const DS<T> &DataFrame<TS, DS>::get_column (const char *name) const  {

    return (const_cast<DataFrame *>(this)->get_column<T>(name));
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts>
void DataFrame<TS, DS>::multi_visit (Ts ... args) const  {

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

template<typename TS, template<typename DT, class... types> class DS>
template<typename T, typename V>
V &DataFrame<TS, DS>::visit (const char *name, V &visitor) const  {

    const auto  iter = data_tb_.find (name);

    if (iter == data_tb_.end())  {
        char buffer [512];

        sprintf (buffer,
                 "DataFrame::visit(1): ERROR: Cannot find column '%s'",
                 name);
        throw ColNotFound (buffer);
    }

    const DataVec   &hv = data_[iter->second];
    const DS<T>     &vec = hv.get_vec<T, DS<T>>();
    const size_type idx_s = timestamps_.size();
    const size_type data_s = vec.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i], i < data_s ? vec[i] : _get_nan<T>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T1, typename T2, typename V>
V &&DataFrame<TS, DS>::
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

    const DataVec   &hv1 = data_[iter1->second];
    const DataVec   &hv2 = data_[iter2->second];
    const DS<T1>    &vec1 = hv1.get_vec<T1, DS<T1>>();
    const DS<T2>    &vec2 = hv2.get_vec<T2, DS<T2>>();
    const size_type idx_s = timestamps_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T1, typename T2, typename T3, typename V>
V &&DataFrame<TS, DS>::
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

    const DataVec   &hv1 = data_[iter1->second];
    const DataVec   &hv2 = data_[iter2->second];
    const DataVec   &hv3 = data_[iter3->second];
    const DS<T1>    &vec1 = hv1.get_vec<T1, DS<T1>>();
    const DS<T2>    &vec2 = hv2.get_vec<T2, DS<T2>>();
    const DS<T3>    &vec3 = hv3.get_vec<T3, DS<T3>>();
    const size_type idx_s = timestamps_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>(),
                 i < data_s3 ? vec3[i] : _get_nan<T3>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T1, typename T2, typename T3, typename T4, typename V>
V &&DataFrame<TS, DS>::
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

    const DataVec   &hv1 = data_[iter1->second];
    const DataVec   &hv2 = data_[iter2->second];
    const DataVec   &hv3 = data_[iter3->second];
    const DataVec   &hv4 = data_[iter4->second];
    const DS<T1>    &vec1 = hv1.get_vec<T1, DS<T1>>();
    const DS<T2>    &vec2 = hv2.get_vec<T2, DS<T2>>();
    const DS<T3>    &vec3 = hv3.get_vec<T3, DS<T3>>();
    const DS<T4>    &vec4 = hv4.get_vec<T4, DS<T4>>();
    const size_type idx_s = timestamps_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type data_s4 = vec4.size();

    for (size_type i = 0; i < idx_s; ++i)
        visitor (timestamps_[i],
                 i < data_s1 ? vec1[i] : _get_nan<T1>(),
                 i < data_s2 ? vec2[i] : _get_nan<T2>(),
                 i < data_s3 ? vec3[i] : _get_nan<T3>(),
                 i < data_s4 ? vec4[i] : _get_nan<T4>());

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
V &&DataFrame<TS, DS>::
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

    const DataVec   &hv1 = data_[iter1->second];
    const DataVec   &hv2 = data_[iter2->second];
    const DataVec   &hv3 = data_[iter3->second];
    const DataVec   &hv4 = data_[iter4->second];
    const DataVec   &hv5 = data_[iter5->second];
    const DS<T1>    &vec1 = hv1.get_vec<T1, DS<T1>>();
    const DS<T2>    &vec2 = hv2.get_vec<T2, DS<T2>>();
    const DS<T3>    &vec3 = hv3.get_vec<T3, DS<T3>>();
    const DS<T4>    &vec4 = hv4.get_vec<T4, DS<T4>>();
    const DS<T5>    &vec5 = hv5.get_vec<T5, DS<T5>>();
    const size_type idx_s = timestamps_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type data_s4 = vec4.size();
    const size_type data_s5 = vec5.size();

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

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... types>
DataFrame<TS, DS>
DataFrame<TS, DS>::get_data_by_idx (TS begin, TS end) const  {

    const auto  &lower =
        std::lower_bound (timestamps_.begin(), timestamps_.end(), begin);
    const auto  &upper =
        std::upper_bound (timestamps_.begin(), timestamps_.end(), end);
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

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... types>
DataFrame<TS, DS>
DataFrame<TS, DS>::get_data_by_loc (size_type begin, size_type end) const  {

    DataFrame   df;

    if (end < timestamps_.size() && begin <= end)  {
        df.load_index(timestamps_.begin() + begin, timestamps_.begin() + end);

        for (auto &iter : data_tb_)  {
            load_functor_<types ...> functor (iter.first.c_str(),
                                              begin,
                                              end,
                                              df);

            data_[iter.second].change(functor);
        }
    }

    return (df);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
