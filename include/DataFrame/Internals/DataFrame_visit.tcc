// Hossein Moein
// December 8, 2020
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

#include <future>
#include <tuple>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename  H>
template<typename ... Ts>
void DataFrame<I, H>::multi_visit (Ts ... args)  {

    auto    args_tuple = std::tuple<Ts ...>(args ...);
    auto    fc = [this](auto &pa) mutable -> void {
        auto &functor = *(pa.second);

        using T =
            typename std::remove_reference<
                decltype(functor)>::type::value_type;
        using V =
            typename std::remove_const<
                typename std::remove_reference<decltype(functor)>::type>::type;

        this->visit<T, V>(pa.first, functor);
    };

    for_each_in_tuple (args_tuple, fc);
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename ... Ts>
void DataFrame<I, H>::multi_visit(Ts ... args) const  {

    const_cast<DataFrame *>(this)->multi_visit<Ts ...>(args ...);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
V &DataFrame<I, H>::visit (const char *name, V &visitor, bool in_reverse)  {

    auto            &vec = get_column<T>(name);
    const size_type idx_s = indices_.size();
    const size_type min_s = std::min<size_type>(vec.size(), idx_s);
    size_type       i = 0;
    T               nan_val = get_nan<T>();

    visitor.pre();
    if (! in_reverse)  {
        for (; i < min_s; ++i)
            visitor (indices_[i], vec[i]);
        for (; i < idx_s; ++i)
            visitor (indices_[i], nan_val);
    }
    else  {
        const size_type diff = idx_s - min_s;
        const size_type idx_s_1 = idx_s - 1;

        for (; i < diff; ++i)
            visitor (indices_[idx_s_1 - i], nan_val);
        for (; i < min_s; ++i)
            visitor (indices_[idx_s_1 - i], vec[idx_s_1 - i]);
    }
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
V &DataFrame<I, H>::
visit (const char *name, V &visitor, bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->visit<T, V>
                (name, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name, V &visitor, bool in_reverse)  {

    return (std::async(std::launch::async,
                       static_cast<V &(DataFrame::*)(const char *, V &, bool)>
                          (&DataFrame::visit<T, V>),
                       this,
                       name,
                       std::ref(visitor),
                       in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name, V &visitor, bool in_reverse) const  {

    return (std::async(std::launch::async,
                       static_cast<V &(DataFrame::*)(const char *,
                                                     V &,
                                                     bool) const>
                          (&DataFrame::visit<T, V>),
                       this,
                       name,
                       std::ref(visitor),
                       in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
V &DataFrame<I, H>::
visit (const char *name1, const char *name2, V &visitor, bool in_reverse)  {

    auto            &vec1 = get_column<T1>(name1);
    auto            &vec2 = get_column<T2>(name2);
    const size_type idx_s = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type min_s = std::min<size_type>({ idx_s, data_s1, data_s2 });
    size_type       i = 0;
    T1              nan_val1 = get_nan<T1>();
    T2              nan_val2 = get_nan<T2>();

    visitor.pre();
    if (! in_reverse)  {
        for (; i < min_s; ++i)
            visitor (indices_[i], vec1[i], vec2[i]);
        for (; i < idx_s; ++i)
            visitor (indices_[i],
                     i < data_s1 ? vec1[i] : nan_val1,
                     i < data_s2 ? vec2[i] : nan_val2);
    }
    else  {
        const size_type diff = idx_s - min_s;
        const size_type idx_s_1 = idx_s - 1;

        for (; i < diff; ++i)
            visitor (indices_[i],
                     ((idx_s_1 - i) < data_s1) ? vec1[idx_s_1 - i] : nan_val1,
                     ((idx_s_1 - i) < data_s2) ? vec2[idx_s_1 - i] : nan_val2);
        for (; i < min_s; ++i)
            visitor (indices_[idx_s_1 - i],
                     vec1[idx_s_1 - i],
                     vec2[idx_s_1 - i]);
    }
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
V &DataFrame<I, H>::
visit (const char *name1,
       const char *name2,
       V &visitor,
       bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->visit<T1, T2, V>
                (name1, name2, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name1,
            const char *name2,
            V &visitor,
            bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *, const char *, V &, bool)>
            (&DataFrame::visit<T1, T2, V>),
        this,
        name1,
        name2,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name1,
            const char *name2,
            V &visitor,
            bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      V &,
                                      bool) const>
            (&DataFrame::visit<T1, T2, V>),
        this,
        name1,
        name2,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
V &DataFrame<I, H>::
visit (const char *name1,
       const char *name2,
       const char *name3,
       V &visitor,
       bool in_reverse)  {

    auto            &vec1 = get_column<T1>(name1);
    auto            &vec2 = get_column<T2>(name2);
    auto            &vec3 = get_column<T3>(name3);
    const size_type idx_s = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type min_s =
        std::min<size_type>({ idx_s, data_s1, data_s2, data_s3 });
    size_type       i = 0;
    T1              nan_val1 = get_nan<T1>();
    T2              nan_val2 = get_nan<T2>();
    T3              nan_val3 = get_nan<T3>();

    visitor.pre();
    if (! in_reverse)  {
        for (; i < min_s; ++i)
            visitor (indices_[i], vec1[i], vec2[i], vec3[i]);
        for (; i < idx_s; ++i)
            visitor (indices_[i],
                     i < data_s1 ? vec1[i] : nan_val1,
                     i < data_s2 ? vec2[i] : nan_val2,
                     i < data_s3 ? vec3[i] : nan_val3);
    }
    else  {
        const size_type diff = idx_s - min_s;
        const size_type idx_s_1 = idx_s - 1;

        for (; i < diff; ++i)
            visitor (indices_[i],
                     ((idx_s_1 - i) < data_s1) ? vec1[idx_s_1 - i] : nan_val1,
                     ((idx_s_1 - i) < data_s2) ? vec2[idx_s_1 - i] : nan_val2,
                     ((idx_s_1 - i) < data_s3) ? vec3[idx_s_1 - i] : nan_val3);
        for (; i < min_s; ++i)
            visitor (indices_[idx_s_1 - i],
                     vec1[idx_s_1 - i],
                     vec2[idx_s_1 - i],
                     vec3[idx_s_1 - i]);
    }
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
V &DataFrame<I, H>::
visit (const char *name1,
       const char *name2,
       const char *name3,
       V &visitor,
       bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->visit<T1, T2, T3, V>
                (name1, name2, name3, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name1,
            const char *name2,
            const char *name3,
            V &visitor,
            bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool)>
            (&DataFrame::visit<T1, T2, T3, V>),
        this,
        name1,
        name2,
        name3,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name1,
            const char *name2,
            const char *name3,
            V &visitor,
            bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool) const>
            (&DataFrame::visit<T1, T2, T3, V>),
        this,
        name1,
        name2,
        name3,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename V>
V &DataFrame<I, H>::
visit (const char *name1,
       const char *name2,
       const char *name3,
       const char *name4,
       V &visitor,
       bool in_reverse)  {

    auto            &vec1 = get_column<T1>(name1);
    auto            &vec2 = get_column<T2>(name2);
    auto            &vec3 = get_column<T3>(name3);
    auto            &vec4 = get_column<T4>(name4);
    const size_type idx_s = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type data_s4 = vec4.size();
    const size_type min_s =
        std::min<size_type>({ idx_s, data_s1, data_s2, data_s3, data_s4 });
    size_type       i = 0;
    T1              nan_val1 = get_nan<T1>();
    T2              nan_val2 = get_nan<T2>();
    T3              nan_val3 = get_nan<T3>();
    T4              nan_val4 = get_nan<T4>();

    visitor.pre();
    if (! in_reverse)  {
        for (; i < min_s; ++i)
            visitor (indices_[i], vec1[i], vec2[i], vec3[i], vec4[i]);
        for (; i < idx_s; ++i)
            visitor (indices_[i],
                     i < data_s1 ? vec1[i] : nan_val1,
                     i < data_s2 ? vec2[i] : nan_val2,
                     i < data_s3 ? vec3[i] : nan_val3,
                     i < data_s4 ? vec4[i] : nan_val4);
    }
    else  {
        const size_type diff = idx_s - min_s;
        const size_type idx_s_1 = idx_s - 1;

        for (; i < diff; ++i)
            visitor (indices_[i],
                     ((idx_s_1 - i) < data_s1) ? vec1[idx_s_1 - i] : nan_val1,
                     ((idx_s_1 - i) < data_s2) ? vec2[idx_s_1 - i] : nan_val2,
                     ((idx_s_1 - i) < data_s3) ? vec3[idx_s_1 - i] : nan_val3,
                     ((idx_s_1 - i) < data_s4) ? vec4[idx_s_1 - i] : nan_val4);
        for (; i < min_s; ++i)
            visitor (indices_[idx_s_1 - i],
                     vec1[idx_s_1 - i],
                     vec2[idx_s_1 - i],
                     vec3[idx_s_1 - i],
                     vec4[idx_s_1 - i]);
    }
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
       V &visitor,
       bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->visit<T1, T2, T3, T4, V>
                (name1, name2, name3, name4, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name1,
            const char *name2,
            const char *name3,
            const char *name4,
            V &visitor,
            bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool)>
            (&DataFrame::visit<T1, T2, T3, T4, V>),
        this,
        name1,
        name2,
        name3,
        name4,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name1,
            const char *name2,
            const char *name3,
            const char *name4,
            V &visitor,
            bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool) const>
            (&DataFrame::visit<T1, T2, T3, T4, V>),
        this,
        name1,
        name2,
        name3,
        name4,
        std::ref(visitor),
        in_reverse));
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
       V &visitor,
       bool in_reverse)  {

    auto            &vec1 = get_column<T1>(name1);
    auto            &vec2 = get_column<T2>(name2);
    auto            &vec3 = get_column<T3>(name3);
    auto            &vec4 = get_column<T4>(name4);
    auto            &vec5 = get_column<T5>(name5);
    const size_type idx_s = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type data_s4 = vec4.size();
    const size_type data_s5 = vec5.size();
    const size_type min_s =
        std::min<size_type>(
            { idx_s, data_s1, data_s2, data_s3, data_s4, data_s5 });
    size_type       i = 0;
    T1              nan_val1 = get_nan<T1>();
    T2              nan_val2 = get_nan<T2>();
    T3              nan_val3 = get_nan<T3>();
    T4              nan_val4 = get_nan<T4>();
    T5              nan_val5 = get_nan<T5>();

    visitor.pre();
    if (! in_reverse)  {
        for (; i < min_s; ++i)
            visitor (indices_[i], vec1[i], vec2[i], vec3[i], vec4[i], vec5[i]);
        for (; i < idx_s; ++i)
            visitor (indices_[i],
                     i < data_s1 ? vec1[i] : nan_val1,
                     i < data_s2 ? vec2[i] : nan_val2,
                     i < data_s3 ? vec3[i] : nan_val3,
                     i < data_s4 ? vec4[i] : nan_val4,
                     i < data_s5 ? vec5[i] : nan_val5);
    }
    else  {
        const size_type diff = idx_s - min_s;
        const size_type idx_s_1 = idx_s - 1;

        for (; i < diff; ++i)
            visitor (indices_[i],
                     ((idx_s_1 - i) < data_s1) ? vec1[idx_s_1 - i] : nan_val1,
                     ((idx_s_1 - i) < data_s2) ? vec2[idx_s_1 - i] : nan_val2,
                     ((idx_s_1 - i) < data_s3) ? vec3[idx_s_1 - i] : nan_val3,
                     ((idx_s_1 - i) < data_s4) ? vec4[idx_s_1 - i] : nan_val4,
                     ((idx_s_1 - i) < data_s5) ? vec5[idx_s_1 - i] : nan_val5);
        for (; i < min_s; ++i)
            visitor (indices_[idx_s_1 - i],
                     vec1[idx_s_1 - i],
                     vec2[idx_s_1 - i],
                     vec3[idx_s_1 - i],
                     vec4[idx_s_1 - i],
                     vec5[idx_s_1 - i]);
    }
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
       V &visitor,
       bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->visit<T1, T2, T3, T4, T5, V>
                (name1, name2, name3, name4, name5, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name1,
            const char *name2,
            const char *name3,
            const char *name4,
            const char *name5,
            V &visitor,
            bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool)>
            (&DataFrame::visit<T1, T2, T3, T4, T5, V>),
        this,
        name1,
        name2,
        name3,
        name4,
        name5,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
std::future<V &> DataFrame<I, H>::
visit_async(const char *name1,
            const char *name2,
            const char *name3,
            const char *name4,
            const char *name5,
            V &visitor,
            bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool) const>
            (&DataFrame::visit<T1, T2, T3, T4, T5, V>),
        this,
        name1,
        name2,
        name3,
        name4,
        name5,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name, V &visitor, bool in_reverse)  {

    auto    &vec = get_column<T>(name);

    visitor.pre();
    if (! in_reverse)
        visitor (indices_.begin(), indices_.end(), vec.begin(), vec.end());
    else
        visitor (indices_.rbegin(), indices_.rend(), vec.rbegin(), vec.rend());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name, V &visitor, bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->single_act_visit<T, V>
                (name, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name, V &visitor, bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *, V &, bool)>
           (&DataFrame::single_act_visit<T, V>),
        this,
        name,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T, typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name, V &visitor, bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *, V &, bool) const>
           (&DataFrame::single_act_visit<T, V>),
        this,
        name,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1,
                  const char *name2,
                  V &visitor,
                  bool in_reverse)  {

    const ColumnVecType<T1> &vec1 = get_column<T1>(name1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2);

    visitor.pre();
    if (! in_reverse)
        visitor (indices_.begin(), indices_.end(),
                 vec1.begin(), vec1.end(),
                 vec2.begin(), vec2.end());
    else
        visitor (indices_.rbegin(), indices_.rend(),
                 vec1.rbegin(), vec1.rend(),
                 vec2.rbegin(), vec2.rend());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name1,
                       const char *name2,
                       V &visitor,
                       bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *, const char *, V &, bool)>
            (&DataFrame::single_act_visit<T1, T2, V>),
        this,
        name1,
        name2,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1,
                  const char *name2,
                  V &visitor,
                  bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->single_act_visit<T1, T2, V>
                (name1, name2, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name1,
                       const char *name2,
                       V &visitor,
                       bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      V &,
                                      bool) const>
            (&DataFrame::single_act_visit<T1, T2, V>),
        this,
        name1,
        name2,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1,
                  const char *name2,
                  const char *name3,
                  V &visitor,
                  bool in_reverse)  {

    const ColumnVecType<T1> &vec1 = get_column<T1>(name1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3);

    visitor.pre();
    if (! in_reverse)
        visitor (indices_.begin(), indices_.end(),
                 vec1.begin(), vec1.end(),
                 vec2.begin(), vec2.end(),
                 vec3.begin(), vec3.end());
    else
        visitor (indices_.rbegin(), indices_.rend(),
                 vec1.rbegin(), vec1.rend(),
                 vec2.rbegin(), vec2.rend(),
                 vec3.rbegin(), vec3.rend());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1,
                  const char *name2,
                  const char *name3,
                  V &visitor,
                  bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->single_act_visit<T1, T2, T3, V>
                (name1, name2, name3, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name1,
                       const char *name2,
                       const char *name3,
                       V &visitor,
                       bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool)>
            (&DataFrame::single_act_visit<T1, T2, T3, V>),
        this,
        name1,
        name2,
        name3,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name1,
                       const char *name2,
                       const char *name3,
                       V &visitor,
                       bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool) const>
            (&DataFrame::single_act_visit<T1, T2, T3, V>),
        this,
        name1,
        name2,
        name3,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1,
                  const char *name2,
                  const char *name3,
                  const char *name4,
                  V &visitor,
                  bool in_reverse)  {

    const ColumnVecType<T1> &vec1 = get_column<T1>(name1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3);
    const ColumnVecType<T4> &vec4 = get_column<T4>(name4);

    visitor.pre();
    if (! in_reverse)
        visitor (indices_.begin(), indices_.end(),
                 vec1.begin(), vec1.end(),
                 vec2.begin(), vec2.end(),
                 vec3.begin(), vec3.end(),
                 vec4.begin(), vec4.end());
    else
        visitor (indices_.rbegin(), indices_.rend(),
                 vec1.rbegin(), vec1.rend(),
                 vec2.rbegin(), vec2.rend(),
                 vec3.rbegin(), vec3.rend(),
                 vec4.rbegin(), vec4.rend());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1,
                  const char *name2,
                  const char *name3,
                  const char *name4,
                  V &visitor,
                  bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->single_act_visit<T1, T2, T3, T4, V>
                (name1, name2, name3, name4, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name1,
                       const char *name2,
                       const char *name3,
                       const char *name4,
                       V &visitor,
                       bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool)>
            (&DataFrame::single_act_visit<T1, T2, T3, T4, V>),
        this,
        name1,
        name2,
        name3,
        name4,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name1,
                       const char *name2,
                       const char *name3,
                       const char *name4,
                       V &visitor,
                       bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool) const>
            (&DataFrame::single_act_visit<T1, T2, T3, T4, V>),
        this,
        name1,
        name2,
        name3,
        name4,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1,
                  const char *name2,
                  const char *name3,
                  const char *name4,
                  const char *name5,
                  V &visitor,
                  bool in_reverse)  {

    const ColumnVecType<T1> &vec1 = get_column<T1>(name1);
    const ColumnVecType<T2> &vec2 = get_column<T2>(name2);
    const ColumnVecType<T3> &vec3 = get_column<T3>(name3);
    const ColumnVecType<T4> &vec4 = get_column<T4>(name4);
    const ColumnVecType<T5> &vec5 = get_column<T5>(name5);

    visitor.pre();
    if (! in_reverse)
        visitor (indices_.begin(), indices_.end(),
                 vec1.begin(), vec1.end(),
                 vec2.begin(), vec2.end(),
                 vec3.begin(), vec3.end(),
                 vec4.begin(), vec4.end(),
                 vec5.begin(), vec5.end());
    else
        visitor (indices_.rbegin(), indices_.rend(),
                 vec1.rbegin(), vec1.rend(),
                 vec2.rbegin(), vec2.rend(),
                 vec3.rbegin(), vec3.rend(),
                 vec4.rbegin(), vec4.rend(),
                 vec5.rbegin(), vec5.rend());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
V &DataFrame<I, H>::
single_act_visit (const char *name1,
                  const char *name2,
                  const char *name3,
                  const char *name4,
                  const char *name5,
                  V &visitor,
                  bool in_reverse) const  {

    return (const_cast<DataFrame *>(this)->single_act_visit
                <T1, T2, T3, T4, T5, V>
                (name1, name2, name3, name4, name5, visitor, in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name1,
                       const char *name2,
                       const char *name3,
                       const char *name4,
                       const char *name5,
                       V &visitor,
                       bool in_reverse)  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool)>
        (&DataFrame::single_act_visit<T1, T2, T3, T4, T5, V>),
        this,
        name1,
        name2,
        name3,
        name4,
        name5,
        std::ref(visitor),
        in_reverse));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename V>
std::future<V &> DataFrame<I, H>::
single_act_visit_async(const char *name1,
                       const char *name2,
                       const char *name3,
                       const char *name4,
                       const char *name5,
                       V &visitor,
                       bool in_reverse) const  {

    return (std::async(
        std::launch::async,
        static_cast<V &(DataFrame::*)(const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      const char *,
                                      V &,
                                      bool) const>
        (&DataFrame::single_act_visit<T1, T2, T3, T4, T5, V>),
        this,
        name1,
        name2,
        name3,
        name4,
        name5,
        std::ref(visitor),
        in_reverse));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
