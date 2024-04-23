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
#include <DataFrame/Utils/Threads/ThreadGranularity.h>

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
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
                   _TypeinfoEqualTo_>   _typeinfo_name_  {
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
    { typeid(std::string), "string" },
    { typeid(const char *), "string" },
    { typeid(char *), "string" },
    { typeid(bool), "bool" },
    { typeid(DateTime), "DateTime" },

    // Containers
    //
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

template<typename SRC_DF, typename DST_DF, typename T, typename I_V, typename V>
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
            if (col_vec1) col_vec1->push_back(input_col1[sort_v[vec_size - 1]]);
            if (col_vec2) col_vec2->push_back(input_col2[sort_v[vec_size - 1]]);
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
            if (col_vec1) col_vec1->push_back(input_col1[sort_v[vec_size - 1]]);
            if (col_vec2) col_vec2->push_back(input_col2[sort_v[vec_size - 1]]);
            if (col_vec3) col_vec3->push_back(input_col3[sort_v[vec_size - 1]]);
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
            for (std::size_t j = 0; j < std::size_t(value); ++j)
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

    if (thread_level > 3)
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
    else
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

// Specialized version of std::remove_copy
//

template<typename I, typename O, typename PRE>
inline static void
_remove_copy_if_(I first, I last, O d_first, PRE predicate)  {

    for (I i = first; i != last; ++i) [[likely]]
        if (! predicate (std::distance(first, i)))
            *d_first++ = *i;

}

// ----------------------------------------------------------------------------

template<typename T, typename V, typename BV>
static inline void
_sort_by_sorted_index_(T &to_be_sorted,
                       const V &sorting_idxs,
                       BV &done_vec,
                       size_t idx_s) {

    std::ranges::fill(done_vec, false);
    for (std::size_t i = 0; i < idx_s; ++i) [[likely]]  {
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
                _inv_merge_sort_(original, temp, left, mid, comp, thread_level);
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
        56, 57, 58, 59, 60, 61, 62, 63, 64, 97, 98, 99, 100, 101, 102, 103, 104,
        105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118,
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

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
