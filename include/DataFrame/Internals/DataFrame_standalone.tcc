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

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <tuple>
#include <unordered_map>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

using _TypeInfoRef_ = std::reference_wrapper<const std::type_info>;

struct  _TypeinfoHasher_  {

    std::size_t
    operator()(_TypeInfoRef_ item) const  { return (item.get().hash_code()); }
};

struct  _TypeinfoEqualTo_  {

    bool
    operator()(_TypeInfoRef_ lhs, _TypeInfoRef_ rhs) const {

        return (lhs.get() == rhs.get());
    }
};

static const
std::unordered_map<_TypeInfoRef_,
                   const char *const,
                   _TypeinfoHasher_,
                   _TypeinfoEqualTo_>   _typeinfo_name_ {
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
    { typeid(std::string), "string" },
    { typeid(bool), "bool" },
    { typeid(DateTime), "DateTime" },
    { typeid(std::vector<double>), "dbl_vec" },
    { typeid(std::vector<std::string>), "str_vec" },
    { typeid(std::set<double>), "dbl_set" },
    { typeid(std::set<std::string>), "str_set" },
    { typeid(std::map<std::string, double>), "str_dbl_map" },
    { typeid(std::unordered_map<std::string, double>), "str_dbl_unomap" },
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

template<typename DF, typename T>
static inline auto &
_create_column_from_triple_(DF &df, T &triple) {

    using ValueType = typename std::tuple_element<2, T>::type::result_type;

    return (df.template create_column<ValueType>(std::get<1>(triple), false));
}

// ----------------------------------------------------------------------------

template<typename DF, typename T, typename I_V, typename V>
static inline void
_load_groupby_data_1_(
    const DF &source,
    DF &dest,
    T &triple,
    I_V &&idx_visitor,
    const V &input_v,
    const typename DF::template StlVecType<std::size_t> &sort_v,
    const char *col_name)  {

    std::size_t         marker = 0;
    auto                &dst_idx = dest.get_index();
    const std::size_t   vec_size = input_v.size();
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
            if (input_v[sort_v[i]] != input_v[sort_v[marker]])  {
                idx_visitor.pre();
                for (std::size_t j = marker; j < i; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
                idx_visitor.post();
                dst_idx.push_back(idx_visitor.get_result());
                if (col_vec)  col_vec->push_back(input_v[sort_v[i - 1]]);
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
            if (col_vec)  col_vec->push_back(input_v[sort_v[vec_size - 1]]);
        }
    }

    using ValueType = typename std::tuple_element<2, T>::type::value_type;

    const auto          &src_vec =
        source.template get_column<ValueType>(std::get<0>(triple));
    const std::size_t   max_count = std::min(vec_size, src_vec.size());
    auto                &dst_vec = _create_column_from_triple_(dest, triple);
    auto                &visitor = std::get<2>(triple);

    dst_vec.reserve(max_count / 2 + 1);
    marker = 0;
    for (std::size_t i = 0; i < max_count; ++i) [[likely]]  {
        if (input_v[sort_v[i]] != input_v[sort_v[marker]])  {
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

template<typename DF, typename T, typename I_V, typename V1, typename V2>
static inline void
_load_groupby_data_2_(
    const DF &source,
    DF &dest,
    T &triple,
    I_V &&idx_visitor,
    const V1 &input_v1,
    const V2 &input_v2,
    const typename DF::template StlVecType<std::size_t> &sort_v,
    const char *col_name1,
    const char *col_name2) {

    std::size_t         marker = 0;
    auto                &dst_idx = dest.get_index();
    const std::size_t   vec_size = std::min(input_v1.size(), input_v2.size());
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
            if (input_v1[sort_v[i]] != input_v1[sort_v[marker]] ||
                input_v2[sort_v[i]] != input_v2[sort_v[marker]])  {
                idx_visitor.pre();
                for (std::size_t j = marker; j < i; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
                idx_visitor.post();
                dst_idx.push_back(idx_visitor.get_result());
                if (col_vec1) col_vec1->push_back(input_v1[sort_v[i - 1]]);
                if (col_vec2) col_vec2->push_back(input_v2[sort_v[i - 1]]);
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
            if (col_vec1) col_vec1->push_back(input_v1[sort_v[vec_size - 1]]);
            if (col_vec2) col_vec2->push_back(input_v2[sort_v[vec_size - 1]]);
        }
    }

    using ValueType = typename std::tuple_element<2, T>::type::value_type;

    const auto          &src_vec =
        source.template get_column<ValueType>(std::get<0>(triple));
    const std::size_t   max_count = std::min(vec_size, src_vec.size());
    auto                &dst_vec = _create_column_from_triple_(dest, triple);
    auto                &visitor = std::get<2>(triple);

    dst_vec.reserve(max_count / 2 + 1);
    marker = 0;
    for (std::size_t i = 0; i < max_count; ++i) [[likely]]  {
        if (input_v1[sort_v[i]] != input_v1[sort_v[marker]] ||
            input_v2[sort_v[i]] != input_v2[sort_v[marker]])  {
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

template<typename DF, typename T, typename I_V,
         typename V1, typename V2, typename V3>
static inline void
_load_groupby_data_3_(
    const DF &source,
    DF &dest,
    T &triple,
    I_V &&idx_visitor,
    const V1 &input_v1,
    const V2 &input_v2,
    const V3 &input_v3,
    const typename DF::template StlVecType<std::size_t> &sort_v,
    const char *col_name1,
    const char *col_name2,
    const char *col_name3) {

    std::size_t         marker = 0;
    auto                &dst_idx = dest.get_index();
    const std::size_t   vec_size =
        std::min({ input_v1.size(), input_v2.size(), input_v3.size() });
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
            if (input_v1[sort_v[i]] != input_v1[sort_v[marker]] ||
                input_v2[sort_v[i]] != input_v2[sort_v[marker]] ||
                input_v3[sort_v[i]] != input_v3[sort_v[marker]])  {
                idx_visitor.pre();
                for (std::size_t j = marker; j < i; ++j)
                    idx_visitor(src_idx[sort_v[j]], src_idx[sort_v[j]]);
                idx_visitor.post();
                dst_idx.push_back(idx_visitor.get_result());
                if (col_vec1) col_vec1->push_back(input_v1[sort_v[i - 1]]);
                if (col_vec2) col_vec2->push_back(input_v2[sort_v[i - 1]]);
                if (col_vec3) col_vec3->push_back(input_v3[sort_v[i - 1]]);
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
            if (col_vec1) col_vec1->push_back(input_v1[sort_v[vec_size - 1]]);
            if (col_vec2) col_vec2->push_back(input_v2[sort_v[vec_size - 1]]);
            if (col_vec3) col_vec3->push_back(input_v3[sort_v[vec_size - 1]]);
        }
    }

    using ValueType = typename std::tuple_element<2, T>::type::value_type;

    const auto          &src_vec =
        source.template get_column<ValueType>(std::get<0>(triple));
    const std::size_t   max_count = std::min(vec_size, src_vec.size());
    auto                &dst_vec = _create_column_from_triple_(dest, triple);
    auto                &visitor = std::get<2>(triple);

    dst_vec.reserve(max_count / 2 + 1);
    marker = 0;
    for (std::size_t i = 0; i < max_count; ++i) [[likely]]  {
        if (input_v1[sort_v[i]] != input_v1[sort_v[marker]] ||
            input_v2[sort_v[i]] != input_v2[sort_v[marker]] ||
            input_v3[sort_v[i]] != input_v3[sort_v[marker]])  {
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
            for (std::size_t j = 0; j < std::size_t(value); ++j)
                visitor(src_idx[i + j], src_vec[i + j]);
            visitor.post();
            dst_vec.push_back(visitor.get_result());
        }
    }
}

// ----------------------------------------------------------------------------

template<typename DF, typename I, typename T>
static inline void
_load_bucket_data_(const DF &source,
                   DF &dest,
                   const I &value,
                   bucket_type bt,
                   T &triple) {

    using ValueType = typename std::tuple_element<2, T>::type::value_type;

    const auto          &src_idx = source.get_index();
    const auto          &src_vec =
        source.template get_column<ValueType>(std::get<0>(triple));
    auto                &dst_vec = _create_column_from_triple_(dest, triple);
    const std::size_t   src_s = std::min(src_vec.size(), src_idx.size());
    auto                &visitor = std::get<2>(triple);

    _bucketize_core_(dst_vec, src_idx, src_vec, value, visitor, src_s, bt);
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

template<typename DF>
inline static typename DF::template StlVecType<double>
_get_dbl_vec_from_value_(const char *value)  {

    using vec_t = typename DF::template StlVecType<double>;

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

template<typename DF>
inline static typename DF::template StlVecType<std::string>
_get_str_vec_from_value_(const char *value)  {

    using vec_t = typename DF::template StlVecType<std::string>;

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
    using unomap_t = std::unordered_map<std::string, double>;;

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

        const double    value = std::strtod(buffer, nullptr);

        data.emplace(std::make_pair(std::move(key), value));
        vcnt += 1;  // skip separator
    }
    return (data);
}

// ----------------------------------------------------------------------------

template<typename S, typename T>
inline static S &
_write_csv_df_header_(S &o, const char *col_name, std::size_t col_size)  {

    o << col_name << ':' << col_size << ':';

    const auto  &citer = _typeinfo_name_.find(typeid(T));

    if (citer != _typeinfo_name_.end()) [[likely]]
        o << '<' << citer->second << '>';
    else
        o << "<N/A>";
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
inline static S &_write_csv_df_index_(S &o, const DateTime &value)  {

    return (o << value.time() << '.' << value.nanosec());
}

// ----------------------------------------------------------------------------

//
// Specializing std::hash for tuples
//

// Code from boost
// Reciprocal of the golden ratio helps spread entropy and handles duplicates.

template<typename T>
inline void _hash_combine_(std::size_t &seed, T const &v)  {

    seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

// Recursive template code derived from Matthieu M.
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

// Specialized version of std::remove_copy
//

template<typename I, typename O, typename PRE>
inline static O _remove_copy_if_(I first, I last, O d_first, PRE predicate)  {

    for (I i = first; i != last; ++i) [[likely]]
        if (! predicate (std::distance(first, i)))
            *d_first++ = *i;

    return d_first;
}

// ----------------------------------------------------------------------------

template<typename T, typename V, typename BV>
static inline void
_sort_by_sorted_index_(T &to_be_sorted,
                       const V &sorting_idxs,
                       BV &done_vec,
                       size_t idx_s) {

    std::fill(done_vec.begin(), done_vec.end(), false);
    for (std::size_t i = 0; i < idx_s; ++i) [[likely]]
        if (! done_vec[i])  {
            done_vec[i] = true;

            std::size_t prev_j = i;
            std::size_t j = sorting_idxs[i];

            while (i != j)  {
                std::swap(to_be_sorted[prev_j], to_be_sorted[j]);
                done_vec[j] = true;
                prev_j = j;
                j = sorting_idxs[j];
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
