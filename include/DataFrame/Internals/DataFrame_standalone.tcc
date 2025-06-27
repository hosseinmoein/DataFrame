// Hossein Moein
// December 30, 2019
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

#pragma once

#include <DataFrame/Utils/DateTime.h>
#include <DataFrame/Utils/Endianness.h>
#include <DataFrame/Utils/FixedSizeString.h>
#include <DataFrame/Utils/Matrix.h>
#include <DataFrame/Utils/Threads/ThreadGranularity.h>

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <limits>
#include <map>
#include <ranges>
#include <set>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

using _TypeInfoRef_ = std::reference_wrapper<const std::type_info>;

struct  _TypeinfoHasher_  {

    std::size_t
    operator()(const _TypeInfoRef_ item) const  {

        return (item.get().hash_code());
    }
};

// -------------------------------------

struct  _TypeinfoEqualTo_  {

    bool
    operator()(const _TypeInfoRef_ lhs, const _TypeInfoRef_ rhs) const {

        return (lhs.get() == rhs.get());
    }
};

// -------------------------------------

// This is used for writing to files
//
static const
std::unordered_map<_TypeInfoRef_,
                   const char *const,
                   _TypeinfoHasher_,
                   _TypeinfoEqualTo_>
_typeinfo_name_  {

    // Numerics
    //
    { typeid(float), "float" },
    { typeid(double), "double" },
    { typeid(long double), "longdouble" },
    { typeid(short int), "short" },
    { typeid(unsigned short int), "ushort" },
    { typeid(int), "int" },
    { typeid(unsigned int), "uint" },
    { typeid(long int), "long" },
    { typeid(unsigned long int), "ulong" },
    { typeid(long long int), "longlong" },
    { typeid(unsigned long long int), "ulonglong" },
    { typeid(char), "char" },
    { typeid(unsigned char), "uchar" },
    { typeid(bool), "bool" },

    // Strings
    //
    { typeid(std::string), "string" },
    { typeid(const char *), "string" },
    { typeid(char *), "string" },
    { typeid(String32), "vstr32" },
    { typeid(String64), "vstr64" },
    { typeid(String128), "vstr128" },
    { typeid(String512), "vstr512" },
    { typeid(String1K), "vstr1K" },
    { typeid(String2K), "vstr2K" },

    // DateTime
    //
    { typeid(DateTime), "DateTime" },

    // Pairs
    //
    { typeid(std::pair<std::string, double>), "str_dbl_pair" },
    { typeid(std::pair<std::string, std::string>), "str_str_pair" },
    { typeid(std::pair<double, double>), "dbl_dbl_pair" },

    // Containers
    //
    { typeid(std::vector<double>), "dbl_vec" },
    { typeid(std::vector<std::string>), "str_vec" },
    { typeid(std::set<double>), "dbl_set" },
    { typeid(std::set<std::string>), "str_set" },
    { typeid(std::map<std::string, double>), "str_dbl_map" },
    { typeid(std::unordered_map<std::string, double>), "str_dbl_unomap" },
};

// -------------------------------------

// This is used for reading from files
//
static const
std::unordered_map<std::string, file_dtypes>    _typename_id_  {

    // Numerics
    //
    { "float", file_dtypes::FLOAT },
    { "double", file_dtypes::DOUBLE },
    { "longdouble", file_dtypes::LONG_DOUBLE },
    { "short", file_dtypes::SHORT },
    { "ushort", file_dtypes::USHORT },
    { "int", file_dtypes::INT },
    { "uint", file_dtypes::UINT },
    { "long", file_dtypes::LONG },
    { "ulong", file_dtypes::ULONG },
    { "longlong", file_dtypes::LONG_LONG },
    { "ulonglong", file_dtypes::ULONG_LONG },
    { "char", file_dtypes::CHAR },
    { "uchar", file_dtypes::UCHAR },
    { "bool", file_dtypes::BOOL },

    // Strings
    //
    { "string", file_dtypes::STRING },
    { "vstr32", file_dtypes::VSTR32 },
    { "vstr64", file_dtypes::VSTR64 },
    { "vstr128", file_dtypes::VSTR128 },
    { "vstr512", file_dtypes::VSTR512 },
    { "vstr1K", file_dtypes::VSTR1K },
    { "vstr2K", file_dtypes::VSTR2K },

    // DateTime
    //
    { "DateTime", file_dtypes::DATETIME },
    { "DateTimeAME", file_dtypes::DATETIME_AME },
    { "DateTimeEUR", file_dtypes::DATETIME_EUR },
    { "DateTimeISO", file_dtypes::DATETIME_ISO },

    // Pairs
    //
    { "str_dbl_pair", file_dtypes::STR_DBL_PAIR },
    { "str_str_pair", file_dtypes::STR_STR_PAIR },
    { "dbl_dbl_pair", file_dtypes::DBL_DBL_PAIR },

    // Containers
    //
    { "dbl_vec", file_dtypes::DBL_VEC },
    { "str_vec", file_dtypes::STR_VEC },
    { "dbl_set", file_dtypes::DBL_SET },
    { "str_set", file_dtypes::STR_SET },
    { "str_dbl_map", file_dtypes::STR_DBL_MAP },
    { "str_dbl_unomap", file_dtypes::STR_DBL_UNOMAP },
};

// -------------------------------------

// This is used for translating DateTime formats to strings for writing files
//
static const
std::unordered_map<DT_FORMAT, const char *const>    _dtformat_str_  {

    { DT_FORMAT::DT_PRECISE, "DateTime" },
    { DT_FORMAT::ISO_DT_TM, "DateTimeISO" },
    { DT_FORMAT::AMR_DT_TM, "DateTimeAME" },
    { DT_FORMAT::EUR_DT_TM, "DateTimeEUR" },
    { DT_FORMAT::ISO_DT, "DateTimeISO" },
    { DT_FORMAT::AMR_DT, "DateTimeAME" },
    { DT_FORMAT::EUR_DT, "DateTimeEUR" },
};

// ----------------------------------------------------------------------------

template<typename S, typename T>
static S &operator << (S &stream, const std::vector<T> &data)  {

    if (! data.empty())  {
        stream << data.size() << '[' << data[0];
        for (std::size_t i = 1; i < data.size(); ++i)
            stream << '|' << data[i];
        stream << ']';
    }
    return (stream);
}

// ----------------------------------------------------------------------------

template<typename S, typename T>
static S &operator << (S &stream, const std::set<T> &data)  {

    if (! data.empty())  {
        stream << data.size() << '[' << *(data.cbegin());
        for (auto citer = ++(data.cbegin()); citer != data.cend(); ++citer)
            stream << '|' << *citer;
        stream << ']';
    }
    return (stream);
}

// ----------------------------------------------------------------------------

template<typename S, typename T, std::size_t N>
static S &operator << (S &stream, const std::array<T, N> &data)  {

    if (! data.empty())  {
        stream << data.size() << '[' << data[0];
        for (std::size_t i = 1; i < data.size(); ++i)
            stream << '|' << data[i];
        stream << ']';
    }
    return (stream);
}

// ----------------------------------------------------------------------------

template<typename S, typename K, typename V>
static S &operator << (S &stream, const std::map<K, V> &data)  {

    if (! data.empty())  {
        stream << data.size() << '{'
               << data.cbegin()->first << ':' << data.cbegin()->second;
        for (auto citer = ++(data.cbegin()); citer != data.cend(); ++citer)
            stream << '|' << citer->first << ':' << citer->second;
        stream << '}';
    }
    return (stream);
}

// ----------------------------------------------------------------------------

template<typename S, typename K, typename V>
static S &operator << (S &stream, const std::unordered_map<K, V> &data)  {

    if (! data.empty())  {
        stream << data.size() << '{'
               << data.cbegin()->first << ':' << data.cbegin()->second;
        for (auto citer = ++(data.cbegin()); citer != data.cend(); ++citer)
            stream << '|' << citer->first << ':' << citer->second;
        stream << '}';
    }
    return (stream);
}

// ----------------------------------------------------------------------------

template<typename S, typename F, typename L>
static S &operator << (S &stream, const std::pair<F, L> &data)  {

    stream << '<' << data.first << ':' << data.second << '>';
    return (stream);
}

// ----------------------------------------------------------------------------

template<typename DF, typename T>
static inline auto &
_create_column_from_triple_(DF &df, T &triple) {

    using ValueType = typename std::tuple_element<2, T>::type::result_type;

    return (df.template create_column<ValueType>(std::get<1>(triple), false));
}

// ----------------------------------------------------------------------------

template<typename SRC_DF, typename DST_DF, typename T,
         typename I_V, typename V>
static inline void
_load_groupby_data_1_(
    const SRC_DF &source,
    DST_DF &dest,
    T &triple,
    I_V &&idx_visitor,
    const V &input_col,
    const typename SRC_DF::template StlVecType<std::size_t> &sort_v,
    const char *col_name)  {

    std::size_t         marker = 0;
    auto                &dst_idx = dest.get_index();
    const std::size_t   vec_size = input_col.size();
    const auto          &src_idx = source.get_index();

    if (dst_idx.empty())  {
        using ColValueType = typename V::value_type;

        auto    *col_vec =
            ::strcmp(col_name, DF_INDEX_COL_NAME)
                ? &(dest.template create_column<ColValueType>(col_name))
                : nullptr;

        dst_idx.reserve(vec_size / 2 + 1);
        if (col_vec)  col_vec->reserve(vec_size / 2 + 1);
        for (std::size_t i = 0; i < vec_size; ++i)  {
            if (input_col[sort_v[i]] != input_col[sort_v[marker]])  {
                idx_visitor.pre();
                for (std::size_t j = marker; j < i; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
                idx_visitor.post();
                dst_idx.push_back(idx_visitor.get_result());
                if (col_vec)  col_vec->push_back(input_col[sort_v[i - 1]]);
                marker = i;
            }
        }
        if (marker < vec_size || vec_size == 1)  {
            idx_visitor.pre();
            if (vec_size == 1)
                idx_visitor(src_idx[sort_v[0]], src_idx[sort_v[0]]);
            else
                for (std::size_t j = marker; j < vec_size; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
            idx_visitor.post();
            dst_idx.push_back(idx_visitor.get_result());
            if (col_vec)  col_vec->push_back(input_col[sort_v[vec_size - 1]]);
        }
    }

    using ValueType = typename std::tuple_element<2, T>::type::value_type;

    const auto          &src_vec =
        source.template get_column<ValueType>(std::get<0>(triple));
    const std::size_t   max_count =
        std::min(vec_size, std::size_t(src_vec.size()));
    auto                &dst_vec = _create_column_from_triple_(dest, triple);
    auto                &visitor = std::get<2>(triple);

    dst_vec.reserve(max_count / 2 + 1);
    marker = 0;
    for (std::size_t i = 0; i < max_count; ++i) [[likely]]  {
        if (input_col[sort_v[i]] != input_col[sort_v[marker]])  {
            visitor.pre();
            for (std::size_t j = marker; j < i; ++j)
                visitor(src_idx[sort_v[j]], src_vec[sort_v[j]]);
            visitor.post();
            dst_vec.push_back(visitor.get_result());
            marker = i;
        }
    }
    if (marker < max_count || max_count == 1)  {
        visitor.pre();
        if (max_count == 1)
            visitor(src_idx[sort_v[0]], src_vec[sort_v[0]]);
        else
            for (std::size_t j = marker; j < max_count; ++j)
                visitor(src_idx[sort_v[j]], src_vec[sort_v[j]]);
        visitor.post();
        dst_vec.push_back(visitor.get_result());
    }
}

// ----------------------------------------------------------------------------

template<typename SRC_DF, typename DST_DF, typename T, typename I_V,
         typename V1, typename V2>
static inline void
_load_groupby_data_2_(
    const SRC_DF &source,
    DST_DF &dest,
    T &triple,
    I_V &&idx_visitor,
    const V1 &input_col1,
    const V2 &input_col2,
    const typename SRC_DF::template StlVecType<std::size_t> &sort_v,
    const char *col_name1,
    const char *col_name2) {

    std::size_t         marker = 0;
    auto                &dst_idx = dest.get_index();
    const std::size_t   vec_size =
        std::min(input_col1.size(), input_col2.size());
    const auto          &src_idx = source.get_index();

    if (dst_idx.empty())  {
        using ColValueType1 = typename V1::value_type;
        using ColValueType2 = typename V2::value_type;

        auto    *col_vec1 =
            ::strcmp(col_name1, DF_INDEX_COL_NAME)
                ? &(dest.template create_column<ColValueType1>(col_name1))
                : nullptr;
        auto    *col_vec2 =
            ::strcmp(col_name2, DF_INDEX_COL_NAME)
                ? &(dest.template create_column<ColValueType2>(col_name2))
                : nullptr;

        dst_idx.reserve(vec_size / 2 + 1);
        if (col_vec1) col_vec1->reserve(vec_size / 2 + 1);
        if (col_vec2) col_vec2->reserve(vec_size / 2 + 1);
        for (std::size_t i = 0; i < vec_size; ++i)  {
            if (input_col1[sort_v[i]] != input_col1[sort_v[marker]] ||
                input_col2[sort_v[i]] != input_col2[sort_v[marker]])  {
                idx_visitor.pre();
                for (std::size_t j = marker; j < i; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
                idx_visitor.post();
                dst_idx.push_back(idx_visitor.get_result());
                if (col_vec1) col_vec1->push_back(input_col1[sort_v[i - 1]]);
                if (col_vec2) col_vec2->push_back(input_col2[sort_v[i - 1]]);
                marker = i;
            }
        }
        if (marker < vec_size || vec_size == 1)  {
            idx_visitor.pre();
            if (vec_size == 1)
                idx_visitor(src_idx[sort_v[0]], src_idx[sort_v[0]]);
            else
                for (std::size_t j = marker; j < vec_size; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
            idx_visitor.post();
            dst_idx.push_back(idx_visitor.get_result());
            if (col_vec1)
                col_vec1->push_back(input_col1[sort_v[vec_size - 1]]);
            if (col_vec2)
                col_vec2->push_back(input_col2[sort_v[vec_size - 1]]);
        }
    }

    using ValueType = typename std::tuple_element<2, T>::type::value_type;

    const auto          &src_vec =
        source.template get_column<ValueType>(std::get<0>(triple));
    const std::size_t   max_count =
        std::min(vec_size, std::size_t(src_vec.size()));
    auto                &dst_vec = _create_column_from_triple_(dest, triple);
    auto                &visitor = std::get<2>(triple);

    dst_vec.reserve(max_count / 2 + 1);
    marker = 0;
    for (std::size_t i = 0; i < max_count; ++i) [[likely]]  {
        if (input_col1[sort_v[i]] != input_col1[sort_v[marker]] ||
            input_col2[sort_v[i]] != input_col2[sort_v[marker]])  {
            visitor.pre();
            for (std::size_t j = marker; j < i; ++j)
                visitor(src_idx[sort_v[j]], src_vec[sort_v[j]]);
            visitor.post();
            dst_vec.push_back(visitor.get_result());
            marker = i;
        }
    }
    if (marker < max_count || max_count == 1)  {
        visitor.pre();
        if (max_count == 1)
            visitor(src_idx[sort_v[0]], src_vec[sort_v[0]]);
        else
            for (std::size_t j = marker; j < max_count; ++j)
                visitor(src_idx[sort_v[j]], src_vec[sort_v[j]]);
        visitor.post();
        dst_vec.push_back(visitor.get_result());
    }
}

// ----------------------------------------------------------------------------

template<typename SRC_DF, typename DST_DF, typename T, typename I_V,
         typename V1, typename V2, typename V3>
static inline void
_load_groupby_data_3_(
    const SRC_DF &source,
    DST_DF &dest,
    T &triple,
    I_V &&idx_visitor,
    const V1 &input_col1,
    const V2 &input_col2,
    const V3 &input_col3,
    const typename SRC_DF::template StlVecType<std::size_t> &sort_v,
    const char *col_name1,
    const char *col_name2,
    const char *col_name3) {

    std::size_t         marker = 0;
    auto                &dst_idx = dest.get_index();
    const std::size_t   vec_size =
        std::min({ input_col1.size(), input_col2.size(), input_col3.size() });
    const auto          &src_idx = source.get_index();

    if (dst_idx.empty())  {
        using ColValueType1 = typename V1::value_type;
        using ColValueType2 = typename V2::value_type;
        using ColValueType3 = typename V3::value_type;

        auto    *col_vec1 =
            ::strcmp(col_name1, DF_INDEX_COL_NAME)
                ? &(dest.template create_column<ColValueType1>(col_name1))
                : nullptr;
        auto    *col_vec2 =
            ::strcmp(col_name2, DF_INDEX_COL_NAME)
                ? &(dest.template create_column<ColValueType2>(col_name2))
                : nullptr;
        auto    *col_vec3 =
            ::strcmp(col_name3, DF_INDEX_COL_NAME)
                ? &(dest.template create_column<ColValueType3>(col_name3))
                : nullptr;

        dst_idx.reserve(vec_size / 2 + 1);
        if (col_vec1) col_vec1->reserve(vec_size / 2 + 1);
        if (col_vec2) col_vec2->reserve(vec_size / 2 + 1);
        if (col_vec3) col_vec3->reserve(vec_size / 2 + 1);
        for (std::size_t i = 0; i < vec_size; ++i)  {
            if (input_col1[sort_v[i]] != input_col1[sort_v[marker]] ||
                input_col2[sort_v[i]] != input_col2[sort_v[marker]] ||
                input_col3[sort_v[i]] != input_col3[sort_v[marker]])  {
                idx_visitor.pre();
                for (std::size_t j = marker; j < i; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
                idx_visitor.post();
                dst_idx.push_back(idx_visitor.get_result());
                if (col_vec1) col_vec1->push_back(input_col1[sort_v[i - 1]]);
                if (col_vec2) col_vec2->push_back(input_col2[sort_v[i - 1]]);
                if (col_vec3) col_vec3->push_back(input_col3[sort_v[i - 1]]);
                marker = i;
            }
        }
        if (marker < vec_size || vec_size == 1)  {
            idx_visitor.pre();
            if (vec_size == 1)
                idx_visitor(src_idx[sort_v[0]], src_idx[sort_v[0]]);
            else
                for (std::size_t j = marker; j < vec_size; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
            idx_visitor.post();
            dst_idx.push_back(idx_visitor.get_result());
            if (col_vec1)
                col_vec1->push_back(input_col1[sort_v[vec_size - 1]]);
            if (col_vec2)
                col_vec2->push_back(input_col2[sort_v[vec_size - 1]]);
            if (col_vec3)
                col_vec3->push_back(input_col3[sort_v[vec_size - 1]]);
        }
    }

    using ValueType = typename std::tuple_element<2, T>::type::value_type;

    const auto          &src_vec =
        source.template get_column<ValueType>(std::get<0>(triple));
    const std::size_t   max_count =
        std::min(vec_size, std::size_t(src_vec.size()));
    auto                &dst_vec = _create_column_from_triple_(dest, triple);
    auto                &visitor = std::get<2>(triple);

    dst_vec.reserve(max_count / 2 + 1);
    marker = 0;
    for (std::size_t i = 0; i < max_count; ++i) [[likely]]  {
        if (input_col1[sort_v[i]] != input_col1[sort_v[marker]] ||
            input_col2[sort_v[i]] != input_col2[sort_v[marker]] ||
            input_col3[sort_v[i]] != input_col3[sort_v[marker]])  {
            visitor.pre();
            for (std::size_t j = marker; j < i; ++j)
                visitor(src_idx[sort_v[j]], src_vec[sort_v[j]]);
            visitor.post();
            dst_vec.push_back(visitor.get_result());
            marker = i;
        }
    }
    if (marker < max_count || max_count == 1)  {
        visitor.pre();
        if (max_count == 1)
            visitor(src_idx[sort_v[0]], src_vec[sort_v[0]]);
        else
            for (std::size_t j = marker; j < max_count; ++j)
                visitor(src_idx[sort_v[j]], src_vec[sort_v[j]]);
        visitor.post();
        dst_vec.push_back(visitor.get_result());
    }
}

// ----------------------------------------------------------------------------

template<typename DV, typename SI, typename SV, typename V, typename VIS>
static inline void
_bucketize_core_(DV &dst_vec,
                 const SI &src_idx,
                 const SV &src_vec,
                 const V &value,
                 VIS &visitor,
                 std::size_t src_s,
                 bucket_type bt)  {

    using src_type = typename SI::value_type;

    dst_vec.reserve(src_s / 5);
    if (bt == bucket_type::by_distance)  {
        std::size_t marker { 0 };

        visitor.pre();
        for (std::size_t i = 0; i < src_s; ++i) [[likely]]  {
            if (src_idx[i] - src_idx[marker] >= src_type(value))  {
                visitor.post();
                dst_vec.push_back(visitor.get_result());
                visitor.pre();
                marker = i;
            }
            visitor(src_idx[i], src_vec[i]);
        }
    }
    else if (bt == bucket_type::by_count)  {
        if (src_s < src_type(value))  return;

        for (std::size_t i = 0; (i + value) < src_s; i += value) [[likely]]  {
            visitor.pre();
            for (std::size_t j = 0;
                 (j < std::size_t(value)) && ((i + j) < src_s); ++j)
                visitor(src_idx[i + j], src_vec[i + j]);
            visitor.post();
            dst_vec.push_back(visitor.get_result());
        }
    }
}

// ----------------------------------------------------------------------------

template<typename DF, typename RES_DF, typename I, typename T>
static inline void
_load_bucket_data_(const DF &source,
                   RES_DF &dest,
                   const I &value,
                   bucket_type bt,
                   T &triple,
                   std::vector<std::future<void>> &futures) {

    using ValueType = typename std::tuple_element<2, T>::type::value_type;

    const auto          &src_idx = source.get_index();
    const auto          &src_vec =
        source.template get_column<ValueType>(std::get<0>(triple));
    auto                &dst_vec = _create_column_from_triple_(dest, triple);
    const std::size_t   src_s = std::min(src_vec.size(), src_idx.size());
    auto                &visitor = std::get<2>(triple);
    const auto          thread_level =
        (src_idx.size() < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

    if (thread_level > 3)  {
        futures.emplace_back(
            ThreadGranularity::thr_pool_.dispatch(
                false,
                _bucketize_core_<std::decay_t<decltype(dst_vec)>,
                                 std::decay_t<decltype(src_idx)>,
                                 std::decay_t<decltype(src_vec)>,
                                 I,
                                 std::decay_t<decltype(visitor)>>,
                    std::ref(dst_vec),
                    std::cref(src_idx),
                    std::cref(src_vec),
                    std::cref(value),
                    std::ref(visitor),
                    src_s,
                    bt));
    }
    else  {
        _bucketize_core_(dst_vec, src_idx, src_vec, value, visitor, src_s, bt);
    }
}

// ----------------------------------------------------------------------------

template<typename S, typename T>
inline static S &_write_json_df_index_(S &o, const T &value)  {

    return (o << value);
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_json_df_index_(S &o, const DateTime &value)  {

    return (o << value.time() << '.' << value.nanosec());
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_json_df_index_(S &o, const std::string &value)  {

    return (o << '"' << value << '"');
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_json_df_index_(S &o, char value)  {

    if (std::isprint(value))
        return (o << value);
    else
        return (o << static_cast<int>(value));
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_json_df_index_(S &o, unsigned char value)  {

    if (std::isprint(value))
        return (o << value);
    else
        return (o << static_cast<unsigned int>(value));
}

// ----------------------------------------------------------------------------

inline static void
_get_token_from_file_ (std::istream &file,
                       char delim,
                       std::string &value,
                       char alt_delim = '\0') {

    char    c;

    value.clear();
    while (file.get(c)) [[likely]]
        if (c == delim)  {
            break;
        }
        else if (c == alt_delim)  {
            file.unget();
            break;
        }
        else  {
            value += c;
        }
}

// ----------------------------------------------------------------------------

inline static void
_get_token_from_string_ (std::string &str,
                         std::size_t &str_idx,
                         char delim,
                         std::string &value)  {

    std::size_t idx { 0 };

    for (const auto s : str | std::views::drop(str_idx))  {
        idx += 1;
        if (s == delim)
            break;
        else  [[likely]]
            value += s;
    }
    str_idx += idx;
}

// ----------------------------------------------------------------------------

inline static std::pair<std::string, double>
_get_str_dbl_pair_from_value_(const char *value)  {

    using val_t = std::pair<std::string, double>;

    std::size_t vcnt { 0 };
    val_t       data ("", std::numeric_limits<double>::quiet_NaN());

    while (value[vcnt] && value[vcnt] != '<')  ++vcnt;
    if (! value[vcnt])  return (data);
    vcnt += 1;  // skip <

    char        buffer[2048];
    std::size_t bcnt { 0 };

    buffer[0] = '\0';
    while (value[vcnt] && value[vcnt] != ':')
        buffer[bcnt++] = value[vcnt++];
    if (! value[vcnt])  return (data);
    buffer[bcnt] = '\0';
    data.first = buffer;
    vcnt += 1;  // skip :

    bcnt = 0;
    buffer[0] = '\0';
    while (value[vcnt] && value[vcnt] != '>')
        buffer[bcnt++] = value[vcnt++];
    if (! value[vcnt] || buffer[0] == '\0')  return (data);
    buffer[bcnt] = '\0';
    data.second = std::strtod(buffer, nullptr);

    return (data);
}

// ----------------------------------------------------------------------------

inline static std::pair<double, double>
_get_dbl_dbl_pair_from_value_(const char *value)  {

    using val_t = std::pair<double, double>;

    std::size_t vcnt { 0 };
    val_t       data (std::numeric_limits<double>::quiet_NaN(),
                      std::numeric_limits<double>::quiet_NaN());

    while (value[vcnt] && value[vcnt] != '<')  ++vcnt;
    if (! value[vcnt])  return (data);
    vcnt += 1;  // skip <

    char        buffer[2048];
    std::size_t bcnt { 0 };

    buffer[0] = '\0';
    while (value[vcnt] && value[vcnt] != ':')
        buffer[bcnt++] = value[vcnt++];
    if (! value[vcnt]) return (data);
    buffer[bcnt] = '\0';
    if (buffer[0])
        data.first = std::strtod(buffer, nullptr);
    vcnt += 1;  // skip :

    bcnt = 0;
    buffer[0] = '\0';
    while (value[vcnt] && value[vcnt] != '>')
        buffer[bcnt++] = value[vcnt++];
    if (! value[vcnt] || buffer[0] == '\0')  return (data);
    buffer[bcnt] = '\0';
    data.second = std::strtod(buffer, nullptr);

    return (data);
}

// ----------------------------------------------------------------------------

inline static std::pair<std::string, std::string>
_get_str_str_pair_from_value_(const char *value)  {

    using val_t = std::pair<std::string, std::string>;

    std::size_t vcnt { 0 };
    val_t       data ("", "");

    while (value[vcnt] && value[vcnt] != '<')  ++vcnt;
    if (! value[vcnt])  return (data);
    vcnt += 1;  // skip <

    char        buffer[2048];
    std::size_t bcnt { 0 };

    buffer[0] = '\0';
    while (value[vcnt] && value[vcnt] != ':')
        buffer[bcnt++] = value[vcnt++];
    if (! value[vcnt])  return (data);
    buffer[bcnt] = '\0';
    data.first = buffer;
    vcnt += 1;  // skip :

    bcnt = 0;
    buffer[0] = '\0';
    while (value[vcnt] && value[vcnt] != '>')
        buffer[bcnt++] = value[vcnt++];
    if (! value[vcnt] || buffer[0] == '\0')  return (data);
    buffer[bcnt] = '\0';
    data.second = buffer;

    return (data);
}

// ----------------------------------------------------------------------------

inline static std::vector<double>
_get_dbl_vec_from_value_(const char *value)  {

    using vec_t = std::vector<double>;

    std::size_t vcnt = 0;
    char        buffer[128];

    while (value[vcnt] != '[')  {
        buffer[vcnt] = value[vcnt];
        vcnt += 1;
    }
    buffer[vcnt] = '\0';

    vec_t       data;
    std::size_t bcnt;

    data.reserve(std::strtol(buffer, nullptr, 10));
    vcnt += 1;  // skip [
    while (value[vcnt] && value[vcnt] != ']')  {
        bcnt = 0;
        while (value[vcnt] != '|' && value[vcnt] != ']')
            buffer[bcnt++] = value[vcnt++];
        buffer[bcnt] = '\0';
        data.push_back(std::strtod(buffer, nullptr));
        vcnt += 1;  // skip separator
    }
    return (data);
}

// ----------------------------------------------------------------------------

inline static std::vector<std::string>
_get_str_vec_from_value_(const char *value)  {

    using vec_t = std::vector<std::string>;

    std::size_t vcnt { 0 };
    char        buffer[2048];

    while (value[vcnt] != '[')  {
        buffer[vcnt] = value[vcnt];
        vcnt += 1;
    }
    buffer[vcnt] = '\0';

    vec_t       data;
    std::size_t bcnt;

    data.reserve(std::strtol(buffer, nullptr, 10));
    vcnt += 1;  // skip [
    while (value[vcnt] && value[vcnt] != ']')  {
        bcnt = 0;
        while (value[vcnt] != '|' && value[vcnt] != ']')
            buffer[bcnt++] = value[vcnt++];
        buffer[bcnt] = '\0';
        data.push_back(buffer);
        vcnt += 1;  // skip separator
    }
    return (data);
}

// ----------------------------------------------------------------------------

inline static std::set<double>
_get_dbl_set_from_value_(const char *value)  {

    using set_t = typename std::set<double>;

    std::size_t vcnt = 0;
    char        buffer[128];

    while (value[vcnt] != '[')  {
        buffer[vcnt] = value[vcnt];
        vcnt += 1;
    }
    buffer[vcnt] = '\0';  // That is the count which is useless for sets

    set_t       data;
    std::size_t bcnt;

    vcnt += 1;  // skip [
    while (value[vcnt] && value[vcnt] != ']')  {
        bcnt = 0;
        while (value[vcnt] != '|' && value[vcnt] != ']')
            buffer[bcnt++] = value[vcnt++];
        buffer[bcnt] = '\0';
        data.insert(std::strtod(buffer, nullptr));
        vcnt += 1;  // skip separator
    }
    return (data);
}

// ----------------------------------------------------------------------------

inline static std::set<std::string>
_get_str_set_from_value_(const char *value)  {

    using set_t = typename std::set<std::string>;

    std::size_t vcnt = 0;
    char        buffer[2048];

    while (value[vcnt] != '[')  {
        buffer[vcnt] = value[vcnt];
        vcnt += 1;
    }
    buffer[vcnt] = '\0';  // That is the count which is useless for sets

    set_t       data;
    std::size_t bcnt;

    vcnt += 1;  // skip [
    while (value[vcnt] && value[vcnt] != ']')  {
        bcnt = 0;
        while (value[vcnt] != '|' && value[vcnt] != ']')
            buffer[bcnt++] = value[vcnt++];
        buffer[bcnt] = '\0';
        data.insert(buffer);
        vcnt += 1;  // skip separator
    }
    return (data);
}

// ----------------------------------------------------------------------------

template<typename MAP>
inline static MAP
_get_str_dbl_map_from_value_(const char *value)  {

    using map_t = MAP;
    using unomap_t = std::unordered_map<std::string, double>;

    std::size_t vcnt = 0;
    char        buffer[256];

    while (value[vcnt] != '{')  {
        buffer[vcnt] = value[vcnt];
        vcnt += 1;
    }
    buffer[vcnt] = '\0';

    map_t       data;
    std::size_t bcnt;

    if constexpr (std::is_base_of_v<unomap_t, map_t>)
        data.reserve(std::strtol(buffer, nullptr, 10));
    vcnt += 1;  // skip {
    while (value[vcnt] && value[vcnt] != '}')  {
        bcnt = 0;
        while (value[vcnt] != ':')
            buffer[bcnt++] = value[vcnt++];
        buffer[bcnt] = '\0';
        vcnt += 1;  // skip :

        std::string key = buffer;

        bcnt = 0;
        while (value[vcnt] != '|' && value[vcnt] != '}')
            buffer[bcnt++] = value[vcnt++];
        buffer[bcnt] = '\0';

        const double    local_value = std::strtod(buffer, nullptr);

        data.emplace(std::make_pair(std::move(key), local_value));
        vcnt += 1;  // skip separator
    }
    return (data);
}

// ----------------------------------------------------------------------------

template<typename S, typename T>
inline static S &
_write_csv_df_header_(S &o, const char *col_name, std::size_t col_size,
                      const char *dt_format = nullptr)  {

    o << col_name << ':' << col_size << ':';

    if (! dt_format) [[likely]]  {
        const auto  &citer = _typeinfo_name_.find(typeid(T));

        if (citer != _typeinfo_name_.end()) [[likely]]
            o << '<' << citer->second << '>';
        else
            o << "<N/A>";
    }
    else  {
        o << '<' << dt_format << '>';
    }
    return (o);
}

// ----------------------------------------------------------------------------

template<typename S, typename T>
inline static S &
_write_json_df_header_(S &o, const char *col_name, std::size_t col_size)  {

    o << '"' << col_name << "\":{\"N\":" << col_size << ',';

    const auto  &citer = _typeinfo_name_.find(typeid(T));

    if (citer != _typeinfo_name_.end()) [[likely]]
        o << "\"T\":\"" << citer->second << "\",";
    else
        o << "\"T\":\"N/A\",";
    return (o);
}

// ----------------------------------------------------------------------------

template<typename S, typename T>
inline static S &_write_csv_df_index_(S &o, const T &value)  {

    return (o << value);
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &
_write_csv_df_index_(S &o, const DateTime &value, DT_FORMAT format)  {

    String128   buffer;

    value.date_to_str(format, buffer);
    return (o << buffer);
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_csv_df_index_(S &o, char value)  {

    if (std::isprint(value))
        return (o << value);
    else
        return (o << static_cast<int>(value));
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_csv_df_index_(S &o, unsigned char value)  {

    if (std::isprint(value))
        return (o << value);
    else
        return (o << static_cast<unsigned int>(value));
}

// ----------------------------------------------------------------------------

template<typename STRM, typename V>
inline static void
_write_binary_common_(STRM &strm, [[maybe_unused]] const V &vec,
                      std::size_t start_row, std::size_t end_row)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    char        buffer[32];
    const auto  &citer = _typeinfo_name_.find(typeid(ValueType));

    if (citer != _typeinfo_name_.end()) [[likely]]
        std::strncpy(buffer, citer->second, sizeof(buffer) - 1);
    else
        std::strncpy(buffer, "N/A", sizeof(buffer) - 1);
    strm.write(buffer, sizeof(buffer));

    const uint64_t  vec_size = end_row - start_row;

    strm.write(reinterpret_cast<const char *>(&vec_size), sizeof(vec_size));
    return;
}

// ----------------------------------------------------------------------------

template<typename STRM, typename V>
inline static STRM &
_write_binary_string_(STRM &strm, const V &str_vec,
                      std::size_t start_row, std::size_t end_row)  {

    _write_binary_common_(strm, str_vec, start_row, end_row);

    // It is better for compression, if you write the alike data together
    //
    for (uint64_t i = start_row; i < end_row; ++i)  {
        const uint16_t  str_sz = static_cast<uint16_t>(str_vec[i].size());

        strm.write(reinterpret_cast<const char *>(&str_sz), sizeof(str_sz));
    }
    for (uint64_t i = start_row; i < end_row; ++i)  {
        const auto  &str = str_vec[i];

        strm.write(str.data(), str.size() * sizeof(char));
    }

    return (strm);
}

// ----------------------------------------------------------------------------

template<typename STRM, typename V>
inline static STRM &
_write_binary_data_(STRM &strm, const V &vec,
                    std::size_t start_row, std::size_t end_row)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    _write_binary_common_(strm, vec, start_row, end_row);

    if constexpr (std::is_same_v<ValueType, bool>)  {
        for (uint64_t i = start_row; i < end_row; ++i)  {
            const bool  bval = vec[i];

            strm.write(reinterpret_cast<const char *>(&bval), sizeof(bool));
        }
    }
    else  {
        // Views don't have the data() method
        //
        constexpr bool  has_data_method =
            requires(const VecType &v)  { v.data(); };

        if constexpr (has_data_method)  {
            strm.write(reinterpret_cast<const char *>(vec.data() + start_row),
                       (end_row - start_row) * sizeof(ValueType));
        }
        else  {
            for (uint64_t i = start_row; i < end_row; ++i)
                strm.write(reinterpret_cast<const char *>(&(vec[i])),
                           sizeof(ValueType));
        }
    }

    return (strm);
}

// ----------------------------------------------------------------------------

template<typename STRM, typename V>
inline static STRM &
_write_binary_datetime_(STRM &strm, const V &dt_vec,
                        std::size_t start_row, std::size_t end_row)  {

    _write_binary_common_(strm, dt_vec, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)  {
        const double    val = static_cast<double>(dt_vec[i]);

        strm.write(reinterpret_cast<const char *>(&val), sizeof(val));
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::pair<std::string, double>
//
template<typename STRM, typename V>
inline static STRM &
_write_binary_str_dbl_pair_(STRM &strm, const V &p_vec,
                            std::size_t start_row,
                            std::size_t end_row)  {

    _write_binary_common_(strm, p_vec, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)  {
        const uint16_t  str_sz = static_cast<uint16_t>(p_vec[i].first.size());

        strm.write(reinterpret_cast<const char *>(&str_sz), sizeof(str_sz));
    }
    for (uint64_t i = start_row; i < end_row; ++i)  {
        const auto      &str = p_vec[i].first;

        strm.write(str.data(), str.size() * sizeof(char));
        strm.write(reinterpret_cast<const char *>(&(p_vec[i].second)),
                   sizeof(double));
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::pair<std::string, std::string>
//
template<typename STRM, typename V>
inline static STRM &
_write_binary_str_str_pair_(STRM &strm, const V &p_vec,
                            std::size_t start_row,
                            std::size_t end_row)  {

    _write_binary_common_(strm, p_vec, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)  {
        const uint16_t  str_sz1 = static_cast<uint16_t>(p_vec[i].first.size());
        const uint16_t  str_sz2 =
            static_cast<uint16_t>(p_vec[i].second.size());

        strm.write(reinterpret_cast<const char *>(&str_sz1), sizeof(str_sz1));
        strm.write(reinterpret_cast<const char *>(&str_sz2), sizeof(str_sz2));
    }
    for (uint64_t i = start_row; i < end_row; ++i)  {
        const auto  &str1 = p_vec[i].first;
        const auto  &str2 = p_vec[i].second;

        strm.write(str1.data(), str1.size() * sizeof(char));
        strm.write(str2.data(), str2.size() * sizeof(char));
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::pair<double, double>
//
template<typename STRM, typename V>
inline static STRM &
_write_binary_dbl_dbl_pair_(STRM &strm, const V &p_vec,
                            std::size_t start_row,
                            std::size_t end_row)  {

    _write_binary_common_(strm, p_vec, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)  {
        strm.write(reinterpret_cast<const char *>(&(p_vec[i].first)),
                   sizeof(double));
        strm.write(reinterpret_cast<const char *>(&(p_vec[i].second)),
                   sizeof(double));
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of double vectors
//
template<typename STRM, typename V>
inline static STRM &
_write_binary_dbl_vec_(STRM &strm, const V &vecs,
                       std::size_t start_row, std::size_t end_row)  {

    _write_binary_common_(strm, vecs, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)
        _write_binary_data_(strm, vecs[i], 0, vecs[i].size());

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of string vectors
//
template<typename STRM, typename V>
inline static STRM &
_write_binary_str_vec_(STRM &strm, const V &vecs,
                       std::size_t start_row, std::size_t end_row)  {

    _write_binary_common_(strm, vecs, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)
        _write_binary_string_(strm, vecs[i], 0, vecs[i].size());

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of double sets
//
template<typename STRM, typename S>
inline static STRM &
_write_binary_dbl_set_(STRM &strm, const S &dbl_sets,
                       std::size_t start_row, std::size_t end_row)  {

    _write_binary_common_(strm, dbl_sets, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)  {
        const uint64_t  sz = dbl_sets[i].size();

        strm.write(reinterpret_cast<const char *>(&sz), sizeof(sz));
        for (const double val : dbl_sets[i])
            strm.write(reinterpret_cast<const char *>(&val), sizeof(val));
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of string sets
//
template<typename STRM, typename S>
inline static STRM &
_write_binary_str_set_(STRM &strm, const S &str_sets,
                       std::size_t start_row, std::size_t end_row)  {

    _write_binary_common_(strm, str_sets, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)  {
        const uint64_t  sz = str_sets[i].size();

        strm.write(reinterpret_cast<const char *>(&sz), sizeof(sz));
        for (const auto &str : str_sets[i])  {
            const uint16_t  str_sz = static_cast<uint16_t>(str.size());

            strm.write(reinterpret_cast<const char *>(&str_sz),
                       sizeof(str_sz));
        }

        for (const auto &str : str_sets[i])
            strm.write(str.data(), str.size() * sizeof(char));
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of string to double [unordered] maps
//
template<typename STRM, typename M>
inline static STRM &
_write_binary_str_dbl_map_(STRM &strm, const M &sd_maps,
                          std::size_t start_row, std::size_t end_row)  {

    _write_binary_common_(strm, sd_maps, start_row, end_row);

    for (uint64_t i = start_row; i < end_row; ++i)  {
        const uint64_t  sz = sd_maps[i].size();

        strm.write(reinterpret_cast<const char *>(&sz), sizeof(sz));
        for (const auto &[str, dbl] : sd_maps[i])  {
            const uint16_t  str_sz = static_cast<uint16_t>(str.size());

            strm.write(reinterpret_cast<const char *>(&str_sz),
                       sizeof(str_sz));
        }

        for (const auto &[str, dbl] : sd_maps[i])  {
            strm.write(str.data(), str.size() * sizeof(char));
            strm.write(reinterpret_cast<const char *>(&dbl), sizeof(double));
        }
    }

    return (strm);
}

// ----------------------------------------------------------------------------

template<typename STRM>
inline static uint64_t
_read_binary_common_(STRM &strm, bool needs_flipping, std::size_t start_row)  {

    uint64_t    vec_size { 0 };

    strm.read(reinterpret_cast<char *>(&vec_size), sizeof(vec_size));
    if (needs_flipping)
        vec_size =
            SwapBytes<decltype(vec_size), sizeof(vec_size)> { }(vec_size);

    if (start_row > vec_size)  {
        String1K    err;

        err.printf("_read_binary_common_(): ERROR: start_row %lu > "
                   "vec_size %lu",
                   start_row, vec_size);
        throw DataFrameError(err.c_str());
    }

    return (vec_size);
}
// ----------------------------------------------------------------------------

template<typename STR_T, typename STRM, typename V>
inline static STRM &
_read_binary_string_(STRM &strm, V &str_vec, bool needs_flipping,
                     std::size_t start_row, std::size_t num_rows)  {

    const uint64_t          vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    std::vector<uint16_t>   sizes (vec_size, 0);

    strm.read(reinterpret_cast<char *>(sizes.data()),
              vec_size * sizeof(uint16_t));
    if (needs_flipping)  {
        SwapBytes<uint16_t, sizeof(uint16_t)>   swaper { };

        for (auto &s : sizes)
            s = swaper(s);
    }

    const uint64_t  read_end =
        num_rows == std::numeric_limits<std::size_t>::max()
            ? vec_size : uint64_t(start_row + num_rows);

    // Now read the strings. We read all data regardless of num_rows
    // to advance the file pointer
    //
    str_vec.reserve(read_end > vec_size
                        ? vec_size - start_row : read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i)  {
        if (i >= start_row && i < read_end) [[likely]]  {
            STR_T   str (std::size_t(sizes[i]), 0);

            strm.read(str.data(), sizes[i] * sizeof(char));
            str_vec.emplace_back(std::move(str));
        }
        else
            strm.seekg(sizes[i], std::ios_base::cur);
    }

    return (strm);
}

// ----------------------------------------------------------------------------

template<typename STRM, typename V>
inline static STRM &
_read_binary_data_(STRM &strm, V &vec, bool needs_flipping,
                   std::size_t start_row, std::size_t num_rows)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    const uint64_t  vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    const uint64_t  read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);

    strm.seekg(start_row * sizeof(ValueType), std::ios_base::cur);
    if constexpr (std::is_same_v<ValueType, bool>)  {
        vec.reserve(read_end - start_row);
        for (uint64_t i = start_row; i < read_end; ++i)  {
            bool    val;

            strm.read(reinterpret_cast<char *>(&val), sizeof(val));
            vec.push_back(val);
        }
    }
    else  {
        vec.resize(read_end - start_row);
        strm.read(reinterpret_cast<char *>(vec.data()),
                  (read_end -  start_row) * sizeof(ValueType));
        if (needs_flipping)  flip_endianness(vec);
    }

    strm.seekg((vec_size - read_end) * sizeof(ValueType), std::ios_base::cur);
    return (strm);
}

// ----------------------------------------------------------------------------

template<typename STRM, typename V>
inline static STRM &
_read_binary_datetime_(STRM &strm, V &dt_vec, bool needs_flipping,
                       std::size_t start_row, std::size_t num_rows)  {

    using ValueType = double;

    const uint64_t  vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    const uint64_t  read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);

    strm.seekg(start_row * sizeof(ValueType), std::ios_base::cur);

    SwapBytes<ValueType, sizeof(ValueType)>   swaper { };

    dt_vec.reserve(read_end - start_row);
    for (uint64_t i = start_row; i < read_end; ++i)  {
        ValueType   val { 0 };

        strm.read(reinterpret_cast<char *>(&val), sizeof(val));
        if (needs_flipping)  val = swaper(val);

        DateTime                        dt;
        const DateTime::EpochType       tm =
            static_cast<DateTime::EpochType>(val);
        const DateTime::NanosecondType  nano =
            static_cast<DateTime::NanosecondType>(
                (val - static_cast<ValueType>(tm)) * 1'000'000'000.0);

        dt.set_time(tm, nano);
        dt_vec.emplace_back(dt);
    }

    strm.seekg((vec_size - read_end) * sizeof(ValueType), std::ios_base::cur);
    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::pair<std::string, double>
//
template<typename STRM, typename V>
inline static STRM &
_read_binary_str_dbl_pair_(STRM &strm, V &p_vec, bool needs_flipping,
                           std::size_t start_row, std::size_t num_rows)  {

    const uint64_t          vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    std::vector<uint16_t>   str_sizes (vec_size, 0);

    strm.read(reinterpret_cast<char *>(str_sizes.data()),
              vec_size * sizeof(uint16_t));
    if (needs_flipping)  {
        SwapBytes<uint16_t, sizeof(uint16_t)>   swaper { };

        for (auto &s : str_sizes)
            s = swaper(s);
    }

    const uint64_t  read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);

    p_vec.reserve(read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i)  {
        if (i >= start_row && i < read_end) [[likely]]  {
            std::string str (std::size_t(str_sizes[i]), 0);
            double      val { 0 };

            strm.read(str.data(), str_sizes[i] * sizeof(char));
            strm.read(reinterpret_cast<char *>(&val), sizeof(val));
            if (needs_flipping)
                val = SwapBytes<double, sizeof(val)> { }(val);
            p_vec.emplace_back(std::move(str), std::move(val));
        }
        else
            strm.seekg(str_sizes[i] + sizeof(double), std::ios_base::cur);
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::pair<std::string, std::string>
//
template<typename STRM, typename V>
inline static STRM &
_read_binary_str_str_pair_(STRM &strm, V &p_vec, bool needs_flipping,
                           std::size_t start_row, std::size_t num_rows)  {

    const uint64_t          vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    std::vector<uint16_t>   str_sizes (vec_size * 2, 0);

    strm.read(reinterpret_cast<char *>(str_sizes.data()),
              vec_size * 2 * sizeof(uint16_t));
    if (needs_flipping)  {
        SwapBytes<uint16_t, sizeof(uint16_t)>   swaper { };

        for (auto &s : str_sizes)
            s = swaper(s);
    }

    const uint64_t  read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);
    std::size_t     sizes_idx { 0 };

    p_vec.reserve(read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i, sizes_idx += 2)  {
        if (i >= start_row && i < read_end) [[likely]]  {
            std::string str1 (std::size_t(str_sizes[sizes_idx]), 0);
            std::string str2 (std::size_t(str_sizes[sizes_idx + 1]), 0);

            strm.read(str1.data(), str_sizes[sizes_idx] * sizeof(char));
            strm.read(str2.data(), str_sizes[sizes_idx + 1] * sizeof(char));
            p_vec.emplace_back(std::move(str1), std::move(str2));
        }
        else
            strm.seekg(str_sizes[sizes_idx] + str_sizes[sizes_idx + 1],
                       std::ios_base::cur);
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::pair<double, double>
//
template<typename STRM, typename V>
inline static STRM &
_read_binary_dbl_dbl_pair_(STRM &strm, V &p_vec, bool needs_flipping,
                           std::size_t start_row, std::size_t num_rows)  {

    const uint64_t  vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    const uint64_t  read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);

    p_vec.reserve(read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i)  {
        if (i >= start_row && i < read_end) [[likely]]  {
            double      val[2];

            strm.read(reinterpret_cast<char *>(val), sizeof(double) * 2);
            if (needs_flipping)  {
                val[0] = SwapBytes<double, sizeof(double)> { }(val[0]);
                val[1] = SwapBytes<double, sizeof(double)> { }(val[1]);
            }
            p_vec.emplace_back(val[0], val[1]);
        }
        else
            strm.seekg(sizeof(double) * 2, std::ios_base::cur);
    }

    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::vector<double>
//
template<typename STRM, typename V>
inline static STRM &
_read_binary_dbl_vec_(STRM &strm, V &vec, bool needs_flipping,
                      std::size_t start_row, std::size_t num_rows)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    const uint64_t  vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    const uint64_t  read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);

    vec.reserve(read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i)  {
        // Skip type name
        //
        strm.seekg(32 * sizeof(char), std::ios_base::cur);

        if (i >= start_row && i < read_end) [[likely]]  {
            ValueType   dbl_vec;

            _read_binary_data_(strm, dbl_vec, needs_flipping,
                               0, std::numeric_limits<std::size_t>::max());
            vec.push_back(std::move(dbl_vec));
        }
        else  {  // Skip the data
            const uint64_t  inner_vec_size =
                _read_binary_common_(strm, needs_flipping, 0);

            strm.seekg(inner_vec_size * sizeof(double), std::ios_base::cur);
        }
    }
    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::vector<std::string>
//
template<typename STRM, typename V>
inline static STRM &
_read_binary_str_vec_(STRM &strm, V &vec, bool needs_flipping,
                      std::size_t start_row, std::size_t num_rows)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    const uint64_t  vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    const uint64_t  read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);
    ValueType       str_vec;

    vec.reserve(read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i)  {
        // Skip type name
        //
        strm.seekg(32 * sizeof(char), std::ios_base::cur);

        str_vec.clear();
        _read_binary_string_<std::string>(
            strm, str_vec, needs_flipping, 0,
            std::numeric_limits<std::size_t>::max());
        if (i >= start_row && i < read_end) [[likely]]
            vec.push_back(std::move(str_vec));
    }
    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::set<double>
//
template<typename STRM, typename V>
inline static STRM &
_read_binary_dbl_set_(STRM &strm, V &set_vec, bool needs_flipping,
                      std::size_t start_row, std::size_t num_rows)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    const uint64_t                          vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    const uint64_t                          read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);
    SwapBytes<uint64_t, sizeof(uint64_t)>   int_swaper { };
    SwapBytes<double, sizeof(double)>       dbl_swaper { };

    set_vec.reserve(read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i)  {
        uint64_t    set_size { 0 };

        strm.read(reinterpret_cast<char *>(&set_size), sizeof(set_size));
        if (needs_flipping)  set_size = int_swaper(set_size);

        if (i >= start_row && i < read_end) [[likely]]  {
            ValueType   dbl_set;

            for (uint64_t i = 0; i < set_size; ++i)   {
                double  val { 0 };

                strm.read(reinterpret_cast<char *>(&val), sizeof(val));
                if (needs_flipping)  val = dbl_swaper(val);
                dbl_set.insert(val);
            }
            set_vec.push_back(std::move(dbl_set));
        }
        else  {  // Skip the data
            strm.seekg(set_size * sizeof(double), std::ios_base::cur);
        }
    }
    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of std::set<std::string>
//
template<typename STRM, typename V>
inline static STRM &
_read_binary_str_set_(STRM &strm, V &set_vec, bool needs_flipping,
                      std::size_t start_row, std::size_t num_rows)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    const uint64_t                          vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    const uint64_t                          read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);
    SwapBytes<uint64_t, sizeof(uint64_t)>   int_swaper { };

    set_vec.reserve(read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i)  {
        uint64_t    set_size { 0 };

        strm.read(reinterpret_cast<char *>(&set_size), sizeof(set_size));
        if (needs_flipping)  set_size = int_swaper(set_size);

        std::vector<uint16_t>   sizes (set_size, 0);

        strm.read(reinterpret_cast<char *>(sizes.data()),
                  set_size * sizeof(uint16_t));
        if (needs_flipping)  flip_endianness(sizes);

        ValueType   str_set;

        for (auto sz : sizes)  {
            if (i >= start_row && i < read_end) [[likely]]  {
                std::string str (std::size_t(sz), 0);

                strm.read(str.data(), sz * sizeof(char));
                str_set.emplace(std::move(str));
            }
            else  // Skip the data
                strm.seekg(sz * sizeof(char), std::ios_base::cur);
        }
        set_vec.push_back(std::move(str_set));
    }
    return (strm);
}

// ----------------------------------------------------------------------------

// Vector of string to double [unordered] maps
//
template<typename STRM, typename V>
inline static STRM &
_read_binary_str_dbl_map_(STRM &strm, V &map_vec, bool needs_flipping,
                          std::size_t start_row, std::size_t num_rows)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    const uint64_t                          vec_size =
        _read_binary_common_(strm, needs_flipping, start_row);
    const uint64_t                          read_end =
        (num_rows == std::numeric_limits<std::size_t>::max() ||
         (start_row + num_rows) > vec_size)
            ? vec_size : uint64_t(start_row + num_rows);
    SwapBytes<uint64_t, sizeof(uint64_t)>   int_swaper { };
    SwapBytes<double, sizeof(double)>       dbl_swaper { };

    map_vec.reserve(read_end - start_row);
    for (uint64_t i = 0; i < vec_size; ++i)  {
        uint64_t    map_size { 0 };

        strm.read(reinterpret_cast<char *>(&map_size), sizeof(map_size));
        if (needs_flipping)  map_size = int_swaper(map_size);

        std::vector<uint16_t>   sizes (map_size, 0);

        strm.read(reinterpret_cast<char *>(sizes.data()),
                  map_size * sizeof(uint16_t));
        if (needs_flipping)  flip_endianness(sizes);

        ValueType       str_dbl_map;
        constexpr bool  has_reserve_method =
            requires(ValueType &v)  { v.reserve(ValueType::size_type()); };

        if constexpr (has_reserve_method)  str_dbl_map.reserve(map_size);

        for (auto sz : sizes)  {
            if (i >= start_row && i < read_end) [[likely]]  {
                std::string str (std::size_t(sz), 0);
                double      val { 0 };

                strm.read(str.data(), sz * sizeof(char));
                strm.read(reinterpret_cast<char *>(&val), sizeof(val));
                if (needs_flipping)  val = dbl_swaper(val);
                str_dbl_map.emplace(std::move(str), val);
            }
            else  {  // Skip the data
                strm.seekg(sz * sizeof(char) + sizeof(double),
                           std::ios_base::cur);
            }
        }
        map_vec.push_back(std::move(str_dbl_map));
    }
    return (strm);
}

// ----------------------------------------------------------------------------

template<typename MA>
inline static typename std::remove_reference<MA>::type
_calc_centered_cov_(const MA &mat1, const MA &mat2)  {

    using mat_t = typename std::remove_reference<MA>::type;

    mat_t   X;
    mat_t   Y;

    mat1.get_centered(X);
    mat2.get_centered(Y);

    mat_t                               result = X.transpose2() * Y;
    const typename mat_t::value_type    denom = X.rows() - 1;

    if constexpr (result.orientation() == matrix_orient::column_major)  {
        for (long c = 0; c < result.cols(); ++c)
            for (long r = 0; r < result.rows(); ++r)
                result(r, c) /= denom;
    }
    else  {
        for (long r = 0; r < result.rows(); ++r)
            for (long c = 0; c < result.cols(); ++c)
                result(r, c) /= denom;
    }

    return (result);
}

// ----------------------------------------------------------------------------

//
// Specializing std::hash for tuples
//

// Code from boost
// Reciprocal of the golden ratio helps spread entropy and handles duplicates.
//
template<typename T>
inline void _hash_combine_(std::size_t &seed, T const &v)  {

    seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

// Recursive template code derived from Matthieu M.
//
template<typename Tuple, size_t I = std::tuple_size<Tuple>::value - 1>
struct  _hash_value_impl_  {

    static inline void apply(size_t &seed, Tuple const &in_tuple)  {

        _hash_value_impl_<Tuple, I - 1>::apply(seed, in_tuple);
        _hash_combine_(seed, std::get<I>(in_tuple));
    }
};

template<typename Tuple>
struct  _hash_value_impl_<Tuple, 0>  {

    static inline void apply(size_t &seed, Tuple const &in_tuple)  {

        _hash_combine_(seed, std::get<0>(in_tuple));
    }
};

struct  TupleHash  {

    template<typename ... TT>
    inline std::size_t operator()(std::tuple<TT ...> const &input) const  {

        std::size_t seed = 0;

        _hash_value_impl_<std::tuple<TT ...>>::apply(seed, input);
        return (seed);
    }
};

// ----------------------------------------------------------------------------

template<typename T, typename V, typename BV>
static inline void
_sort_by_sorted_index_(T &to_be_sorted,
                       const V &sorting_idxs,
                       BV &done_vec,
                       size_t idx_s) {

    std::ranges::fill(done_vec, 0);
    for (std::size_t i = 0; i < idx_s; ++i) [[likely]]  {
        if (! done_vec[i]) [[likely]]  {
            done_vec[i] = 1;

            std::size_t prev_j = i;
            std::size_t j = sorting_idxs[i];

            while (i != j) [[likely]]  {
                std::swap(to_be_sorted[prev_j], to_be_sorted[j]);
                done_vec[j] = 1;
                prev_j = j;
                j = sorting_idxs[j];
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename T>
inline static std::string _to_string_(const T &value)  {

    std::stringstream   ss;

    ss << value;
    return (ss.str());
}

// ----------------------------------------------------------------------------

template<typename T>
inline static T _string_to_(const char *value)  {

    std::stringstream   ss { value };
    T                   ret;

    ss >> ret;
    return (ret);
}

// ----------------------------------------------------------------------------

template<typename Con, typename Comp>
static std::size_t
_inv_merge_(Con &original,
            Con &temp,
            std::size_t left,
            std::size_t mid,
            std::size_t right,
            Comp &&comp)  {

    std::size_t i { left };
    std::size_t j { mid };
    std::size_t k { left };
    std::size_t inv_count { 0 };

    while ((i <= mid - 1) && (j <= right)) {
        if (comp (original[i], original[j])) {
            temp[k++] = original[i++];
        }
        else {
            temp[k++] = original[j++];
            inv_count += mid - i;
        }
    }

    // Copy the remaining elements of left sub-original (if there are any)
    // to temp
    //
    while (i <= mid - 1)
        temp[k++] = original[i++];

    // Copy the remaining elements of right sub-original (if there are any)
    // to temp
    //
    while (j <= right)
        temp[k++] = original[j++];

    // Copy back the merged elements to original original
    //
    for (i = left; i <= right; i++)
        original[i] = temp[i];

    return (inv_count);
}

// ----------------------------------------------------------------------------

template<typename Con, typename Comp>
static std::size_t
_inv_merge_sort_(Con &original,
               Con &temp,
               std::size_t left,
               std::size_t right,
               Comp comp,
               long thread_level)  {

    using fut_type = std::future<std::size_t>;

    std::size_t mid { 0 };
    std::size_t inv_count { 0 };

    if (right > left) {
        const auto  thr_lvl =
            ((right - left) < (ThreadPool::MUL_THR_THHOLD / 2))
                ? 0L : thread_level;

        // Divide the original into two parts and call _inv_merge_sort_()
        // for each of the parts
        //
        mid = (right + left) / 2;

        // Inversion count will be sum of inversions in left-part, right-part
        // and number of inversions in merging
        //
        if (thr_lvl > 2)  {
            fut_type    left_fut =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                        _inv_merge_sort_<Con, Comp>,
                    std::ref(original),
                    std::ref(temp),
                    left,
                    mid,
                    comp,
                    thread_level);
            fut_type    right_fut =
                ThreadGranularity::thr_pool_.dispatch(
                    false,
                    _inv_merge_sort_<Con, Comp>,
                    std::ref(original),
                    std::ref(temp),
                    mid + 1,
                    right,
                    comp,
                    thread_level);

            ThreadGranularity::thr_pool_.run_task();
            ThreadGranularity::thr_pool_.run_task();
            inv_count += left_fut.get() + right_fut.get();
        }
        else  {
            inv_count +=
                _inv_merge_sort_(original, temp, left, mid, comp,
                                 thread_level);
            inv_count +=
                _inv_merge_sort_(original, temp, mid + 1, right, comp,
                                 thread_level);
        }

        // Merge the two parts
        //
        inv_count += _inv_merge_(original, temp, left, mid + 1, right, comp);
    }

    return (inv_count);
}

// ----------------------------------------------------------------------------

struct _LikeClauseUtil_  {

    using value_type = unsigned char;

    // This lookup table is used to help decode the first byte of
    // a multibyte UTF8 character.
    //
    inline static const value_type  CHAR_TRANS1[] = {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
        0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x00, 0x01, 0x02, 0x03,
        0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x00, 0x01, 0x02, 0x03,
        0x00, 0x01, 0x00, 0x00,
    };

    inline static const value_type  UPPER_TO_LOWER[] = {
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
        38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
        56, 57, 58, 59, 60, 61, 62, 63, 64, 97, 98, 99, 100, 101, 102, 103,
        104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
        118,
        119, 120, 121, 122, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102,
        103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130,
        131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144,
        145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158,
        159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172,
        173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186,
        187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200,
        201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214,
        215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228,
        229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242,
        243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255,
    };

    static unsigned int char_read(const value_type **str_ptr_ptr)  {

        unsigned int    c = *((*str_ptr_ptr)++);

        // For this routine, we assume the char string is always
        // zero-terminated.
        //
        if (c >= 0xc0)  {
            c = CHAR_TRANS1[c - 0xc0];
            while ((*(*str_ptr_ptr) & 0xc0) == 0x80)
                c = (c << 6) + (0x3f & *((*str_ptr_ptr)++));
            if (c < 0x80 ||
                (c & 0xFFFFF800) == 0xD800 || (c & 0xFFFFFFFE) == 0xFFFE)
                c = 0xFFFD;
        }

        return (c);
    }

    static inline void upper_to_lower(unsigned int &val)  {

        if (! (val & ~0x7f))
            val = UPPER_TO_LOWER[val];
    }
};

// ----------------------------------------------------------------------------

// This compares two null-terminated strings for equality where the first
// string can potentially be a "glob" expression (forget about regular
// expressions).
// It returns true if they are matched-equal and false if they are not
// matched-equal.
//
// Globbing rules:
//
//      '*'       Matches any sequence of zero or more characters.
//
//      '?'       Matches exactly one character.
//
//     [...]      Matches one character from the enclosed list of
//                characters.
//
//     [^...]     Matches one character not in the enclosed list.
//
// With the [...] and [^...] matching, a ']' character can be included
// in the list by making it the first character after '[' or '^'.  A
// range of characters can be specified using '-'.  Example:
// "[a-z]" matches any single lower-case letter. To match a '-', make
// it the last character in the list.
//
// Hints: to match '*' or '?', put them in "[]". Like this:
//        abc[*]xyz matches "abc*xyz" only
//
// NOTE: This could be, in some cases, n-squared. But it is pretty fast with
//       moderately sized strings. I have not tested this with huge/massive
//       strings.
//
static inline bool
_like_clause_compare_(const char *pattern,
                      const char *input_str,
                      bool case_insensitive = false,
                      unsigned int esc_char = '\\')  {

    using value_type = _LikeClauseUtil_::value_type;

    const value_type    *upattern =
        reinterpret_cast<const value_type *>(pattern);
    const value_type    *uinput_str =
        reinterpret_cast<const value_type *>(input_str);
    unsigned int        c, c2;
    const value_type    match_one { '?' };
    const value_type    match_all { '*' };
    const value_type    match_set { '[' };
    // True if the previous character was escape
    //
    bool                prev_escape { false };

    while ((c = _LikeClauseUtil_::char_read(&upattern)) != 0)  {
        if (c == match_all && ! prev_escape)  {
            while ((c = _LikeClauseUtil_::char_read(&upattern)) == match_all ||
                   c == match_one)  {
                if (c == match_one &&
                    _LikeClauseUtil_::char_read(&uinput_str) == 0)
                    return (false);
            }
            if (c == 0)  {
                return (true);
            }
            else if (c == esc_char)  {
                c = _LikeClauseUtil_::char_read(&upattern);
                if (c == 0)
                    return (false);
            }
            else if (c == match_set)  {
                while (*uinput_str &&
                       _like_clause_compare_(
                           reinterpret_cast<const char *>(&upattern[-1]),
                           reinterpret_cast<const char *>(uinput_str),
                           case_insensitive,
                           esc_char) == 0)  {
                    if ((*(uinput_str++)) >= 0xc0)
                        while ((*uinput_str & 0xc0) == 0x80)
                            uinput_str++;
                }
                return (*uinput_str != 0);
            }
            while ((c2 = _LikeClauseUtil_::char_read(&uinput_str)) != 0)  {
                if (case_insensitive)  {
                    _LikeClauseUtil_::upper_to_lower(c2);
                    _LikeClauseUtil_::upper_to_lower(c);
                    while (c2 != 0 && c2 != c)  {
                        c2 = _LikeClauseUtil_::char_read(&uinput_str);
                        _LikeClauseUtil_::upper_to_lower(c2);
                    }
                }
                else  {
                    while (c2 != 0 && c2 != c)
                        c2 = _LikeClauseUtil_::char_read(&uinput_str);
                }
                if (c2 == 0)
                    return (false);
                if (_like_clause_compare_(
                        reinterpret_cast<const char *>(upattern),
                        reinterpret_cast<const char *>(uinput_str),
                        case_insensitive,
                        esc_char))
                    return (true);
            }
            return (false);
        }
        else if (c == match_one && ! prev_escape)  {
            if (_LikeClauseUtil_::char_read(&uinput_str) == 0)
                return (false);
        }
        else if (c == match_set)  {
            unsigned int    prior_c { 0 };
            int             seen { 0 };
            int             invert { 0 };

            c = _LikeClauseUtil_::char_read(&uinput_str);
            if (c == 0)
                return (false);
            c2 = _LikeClauseUtil_::char_read(&upattern);
            if (c2 == '^')  {
                invert = 1;
                c2 = _LikeClauseUtil_::char_read(&upattern);
            }
            if (c2 == ']')  {
                if (c == ']')
                    seen = 1;
                c2 = _LikeClauseUtil_::char_read(&upattern);
            }
            while (c2 && c2 != ']')  {
                if (c2 == '-' &&
                    upattern[0] != ']' &&
                    upattern[0] != 0 &&
                    prior_c > 0)  {
                    c2 = _LikeClauseUtil_::char_read(&upattern);
                    if (c >= prior_c && c <= c2)
                        seen = 1;
                    prior_c = 0;
                }
                else  {
                    if (c == c2)
                        seen = 1;
                    prior_c = c2;
                }
                c2 = _LikeClauseUtil_::char_read(&upattern);
            }
            if (c2 == 0 || (seen ^ invert) == 0)
                return (false);
        }
        else if (esc_char == c && ! prev_escape)  {
            prev_escape = true;
        }
        else  {
            c2 = _LikeClauseUtil_::char_read(&uinput_str);
            if (case_insensitive)  {
                _LikeClauseUtil_::upper_to_lower(c);
                _LikeClauseUtil_::upper_to_lower(c2);
            }
            if (c != c2)
                return (false);
            prev_escape = false;
        }
    }

    return (*uinput_str == 0);
}

// ----------------------------------------------------------------------------

template<typename T>
static inline T _atoi_(const char *str, int len)  {

    while (*str == ' ')  { ++str; --len; }

    int64_t sign { 1ll };

    if (*str == '-')  {  // Handle negative
        sign = -1ll;
        ++str;
        --len;
    }
    while (len > 0 &&  (! ::isdigit(str[len - 1])))  --len;

    int64_t value { 0 };

    switch(len)  {
        case 19: value += (str[len - 19] - '0') * 1'000'000'000'000'000'000ll;
        case 18: value += (str[len - 18] - '0') * 100'000'000'000'000'000ll;
        case 17: value += (str[len - 17] - '0') * 10'000'000'000'000'000ll;
        case 16: value += (str[len - 16] - '0') * 1'000'000'000'000'000ll;
        case 15: value += (str[len - 15] - '0') * 100'000'000'000'000ll;
        case 14: value += (str[len - 14] - '0') * 10'000'000'000'000ll;
        case 13: value += (str[len - 13] - '0') * 1'000'000'000'000ll;
        case 12: value += (str[len - 12] - '0') * 100'000'000'000ll;
        case 11: value += (str[len - 11] - '0') * 10'000'000'000ll;
        case 10: value += (str[len - 10] - '0') * 1'000'000'000ll;
        case  9: value += (str[len -  9] - '0') * 100'000'000ll;
        case  8: value += (str[len -  8] - '0') * 10'000'000ll;
        case  7: value += (str[len -  7] - '0') * 1'000'000ll;
        case  6: value += (str[len -  6] - '0') * 100'000ll;
        case  5: value += (str[len -  5] - '0') * 10'000ll;
        case  4: value += (str[len -  4] - '0') * 1'000ll;
        case  3: value += (str[len -  3] - '0') * 100ll;
        case  2: value += (str[len -  2] - '0') * 10ll;
        case  1: value += (str[len -  1] - '0');
    }

    return (static_cast<T>(value * sign));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
