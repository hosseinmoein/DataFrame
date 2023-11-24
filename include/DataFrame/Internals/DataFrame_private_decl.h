// Hossein Moein
// April 21, 2021
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

// ----------------------------------------------------------------------------

// This file was factored out so DataFrame.h doesn't become a huge file.
// This was meant to be included inside the private section of DataFrame class.
// This file, by itself, is not useable/compile-able.

// ----------------------------------------------------------------------------

template<typename DF, template<typename> class OPT, typename ... Ts>
friend DF
binary_operation(const DF &lhs, const DF &rhs);

template<typename T1, typename T2>
size_type
load_pair_(std::pair<T1, T2> &col_name_data, bool do_lock = true);

template<typename T>
size_type
append_row_(std::pair<const char *, T> &row_name_data);

void read_json_(std::istream &file, bool columns_only);
void read_csv_(std::istream &file, bool columns_only);
void read_csv2_(std::istream &file,
                bool columns_only,
                size_type starting_row,
                size_type num_rows);

template<typename T>
static void
fill_missing_value_(ColumnVecType<T> &vec,
                    const T &value,
                    int limit,
                    size_type col_num);

template<typename T>
static void
fill_missing_ffill_(ColumnVecType<T> &vec, int limit, size_type col_num);

template<typename T,
         typename std::enable_if<
             supports_arithmetic<T>::value &&
             supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_midpoint_(ColumnVecType<T> &vec, int limit, size_type col_num);

template<typename T,
         typename std::enable_if<
             ! supports_arithmetic<T>::value ||
             ! supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_midpoint_(ColumnVecType<T> &vec, int limit, size_type col_num);

template<typename T>
static void
fill_missing_bfill_(ColumnVecType<T> &vec, int limit);

template<typename T,
         typename std::enable_if<
             supports_arithmetic<T>::value &&
             supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_linter_(ColumnVecType<T> &vec,
                     const IndexVecType &index,
                     int limit);

template<typename T,
         typename std::enable_if<
             ! supports_arithmetic<T>::value ||
             ! supports_arithmetic<IndexType>::value>::type* = nullptr>
static void
fill_missing_linter_(ColumnVecType<T> &, const IndexVecType &, int);

// Maps row number -> number of missing column(s)
using DropRowMap = DFMap<size_type, size_type>;


template<typename T>
static void
drop_missing_rows_(T &vec,
                   const DropRowMap missing_row_map,
                   drop_policy policy,
                   size_type threshold,
                   size_type col_num);

template<typename T, typename ITR>
void
setup_view_column_(const char *name, Index2D<ITR> range);

using IndexIdxVector = StlVecType<std::tuple<size_type, size_type>>;
template<typename T>
using JoinSortingPair = std::pair<const T *, size_type>;

template<typename LHS_T, typename RHS_T, typename IDX_T, typename ... Ts>
static void
join_helper_common_(const LHS_T &lhs,
                    const RHS_T &rhs,
                    const IndexIdxVector &joined_index_idx,
                    DataFrame<
                        IDX_T,
                        HeteroVector<std::size_t(H::align_value)>> &result,
                    const char *skip_col_name = nullptr);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_join_helper_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const IndexIdxVector &joined_index_idx);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned int, HeteroVector<std::size_t(H::align_value)>>
column_join_helper_(const LHS_T &lhs,
                    const RHS_T &rhs,
                    const char *col_name,
                    const IndexIdxVector &joined_index_idx);

template<typename T>
static IndexIdxVector
get_inner_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_inner_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned int, HeteroVector<std::size_t(H::align_value)>>
column_inner_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                   const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename T>
static IndexIdxVector
get_left_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_left_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned int, HeteroVector<std::size_t(H::align_value)>>
column_left_join_(const LHS_T &lhs,
                  const RHS_T &rhs,
                  const char *col_name,
                  const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                  const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename T>
static IndexIdxVector
get_right_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_right_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned int, HeteroVector<std::size_t(H::align_value)>>
column_right_join_(const LHS_T &lhs,
                   const RHS_T &rhs,
                   const char *col_name,
                   const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
                   const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename MAP, typename ... Ts>
static DataFrame
remove_dups_common_(const DataFrame &s_df,
                    remove_dup_spec rds,
                    const MAP &row_table,
                    const IndexVecType &index);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static void
concat_helper_(LHS_T &lhs, const RHS_T &rhs, bool add_new_columns);

template<typename T>
static IndexIdxVector
get_left_right_index_idx_vector_(
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename ... Ts>
static DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
index_left_right_join_(
    const LHS_T &lhs, const RHS_T &rhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<IndexType>> &col_vec_rhs);

template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
static DataFrame<unsigned int, HeteroVector<std::size_t(H::align_value)>>
column_left_right_join_(
    const LHS_T &lhs,
    const RHS_T &rhs,
    const char *col_name,
    const StlVecType<JoinSortingPair<T>> &col_vec_lhs,
    const StlVecType<JoinSortingPair<T>> &col_vec_rhs);

// ----------------------------------------------------------------------------

template<typename V, typename T>
inline static void
replace_vector_vals_(V &data_vec,
                     const StlVecType<T> &old_values,
                     const StlVecType<T> &new_values,
                     std::size_t &count,
                     int limit)  {

    const std::size_t   vcnt = old_values.size();

    assert(vcnt == new_values.size());

    const std::size_t   vec_s = data_vec.size();

    for (std::size_t i = 0; i < vcnt; ++i) [[likely]]  {
        for (std::size_t j = 0; j < vec_s; ++j) [[likely]]  {
            if (limit >= 0 && count >= static_cast<std::size_t>(limit))
                return;
            if (old_values[i] == data_vec[j])  {
                data_vec[j] = new_values[i];
                count += 1;
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
inline static void
col_vector_push_back_func_(V &vec,
                           std::istream &file,
                           T (*converter)(const char *, char **, int),
                           io_format file_type = io_format::csv)  {

    std::string value;
    char        c = 0;

    value.reserve(1024);
    while (file.get(c)) [[likely]] {
        value.clear();
        if (file_type == io_format::csv && c == '\n')  break;
        else if (file_type == io_format::json && c == ']')  break;
        file.unget();
        _get_token_from_file_(file, ',', value,
                              file_type == io_format::json ? ']' : '\0');
        vec.push_back(static_cast<T>(converter(value.c_str(), nullptr, 0)));
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename V, typename Dummy = void>
struct  ColVectorPushBack_  {

    inline void
    operator ()(V &vec,
                std::istream &file,
                T (*converter)(const char *, char **),
                io_format file_type = io_format::csv) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            value.clear();
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            _get_token_from_file_(file, ',', value,
                                  file_type == io_format::json ? ']' : '\0');
            vec.push_back(static_cast<T>(converter(value.c_str(), nullptr)));
        }
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  ColVectorPushBack_<const char *, StlVecType<std::string>, Dummy>  {

    inline void
    operator ()(StlVecType<std::string> &vec,
                std::istream &file,
                const char * (*)(const char *, char **),
                io_format file_type = io_format::csv) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            value.clear();
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            _get_token_from_file_(file, ',', value,
                                  file_type == io_format::json ? ']' : '\0');
            vec.push_back(value);
        }
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  ColVectorPushBack_<DateTime, StlVecType<DateTime>, Dummy>  {

    inline void
    operator ()(StlVecType<DateTime> &vec,
             std::istream &file,
             DateTime (*)(const char *, char **),
             io_format file_type = io_format::csv) const  {

        std::string value;
        char        c = 0;

        value.reserve(1024);
        while (file.get(c)) [[likely]] {
            value.clear();
            if (file_type == io_format::csv && c == '\n')  break;
            else if (file_type == io_format::json && c == ']')  break;
            file.unget();
            _get_token_from_file_(file, ',', value,
                                  file_type == io_format::json ? ']' : '\0');

            time_t      t;
            int         n;
            DateTime    dt;

#ifdef _MSC_VER
            ::sscanf(value.c_str(), "%lld.%d", &t, &n);
#else
            ::sscanf(value.c_str(), "%ld.%d", &t, &n);
#endif // _MSC_VER
            dt.set_time(t, n);
            vec.emplace_back(std::move(dt));
        }
    }
};

// ----------------------------------------------------------------------------

inline static void
json_str_col_vector_push_back_(StlVecType<std::string> &vec,
                               std::istream &file)  {

    char    value[1024];
    char    c = 0;

    while (file.get(c))
        if (c != ' ' && c != '\n' && c != '\t')  {
            file.unget();
            break;
        }

    while (file.get(c)) [[likely]] {
        if (c == ']')  break;
        file.unget();

        std::size_t count = 0;

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '"')
            throw DataFrameError(
                "json_str_col_vector_push_back_(): ERROR: Expected '\"' (0)");

        while (file.get(c))
            if (c == '"')
                break;
            else
                value[count++] = c;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (1)");

        value[count] = 0;
        vec.push_back(value);

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c == ']')  break;
        else if (c != ',')
            throw DataFrameError(
                "json_str_col_vector_push_back_(): ERROR: Expected ',' (2)");
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename Dummy = void>
struct  IdxParserFunctor_  {

    void operator()(StlVecType<T> &, std::istream &, io_format)  {   }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<float, Dummy>  {

    inline void operator()(StlVecType<float> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        const ColVectorPushBack_<float, StlVecType<float>>  slug;

        slug()(vec, file, &::strtof, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<double, Dummy>  {

    inline void operator()(StlVecType<double> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        const ColVectorPushBack_<double, StlVecType<double>>  slug;

        slug(vec, file, &::strtod, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long double, Dummy>  {

    inline void operator()(StlVecType<long double> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        const ColVectorPushBack_<long double, StlVecType<long double>>  slug;

        slug(vec, file, &::strtold, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<int, Dummy>  {

    inline void operator()(StlVecType<int> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long, Dummy>  {

    inline void operator()(StlVecType<long> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<long long, Dummy>  {

    inline void operator()(StlVecType<long long> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtoll, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned int, Dummy>  {

    inline void operator()(StlVecType<unsigned int> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtoul, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned long, Dummy>  {

    inline void operator()(StlVecType<unsigned long> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtoul, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<unsigned long long, Dummy>  {

    inline void operator()(StlVecType<unsigned long long> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtoull, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<std::string, Dummy>  {

    inline void operator()(StlVecType<std::string> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        auto                   converter =
            [](const char *s, char **)-> const char * { return s; };
        const ColVectorPushBack_<const char *, StlVecType<std::string>>  slug;

        slug(vec, file, converter, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<DateTime, Dummy>  {

    inline void operator()(StlVecType<DateTime> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        auto                    converter =
            [](const char *, char **)-> DateTime  { return DateTime(); };
        const ColVectorPushBack_<DateTime, StlVecType<DateTime>>  slug;

        slug(vec, file, converter, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  IdxParserFunctor_<bool, Dummy>  {

    inline void operator()(StlVecType<bool> &vec,
                           std::istream &file,
                           io_format file_type = io_format::csv) const  {

        col_vector_push_back_func_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename T, typename Dummy = void>
struct  GenerateTSIndex_  {

    inline void
    operator ()(StlVecType<T> &index_vec,
                DateTime &start_di,
                time_frequency t_freq,
                long increment) const  {

        switch(t_freq)  {
        case time_frequency::annual:
            index_vec.push_back(static_cast<T>(start_di.date()));
            start_di.add_years(increment);
            break;
        case time_frequency::monthly:
            index_vec.push_back(static_cast<T>(start_di.date()));
            start_di.add_months(increment);
            break;
        case time_frequency::weekly:
            index_vec.push_back(static_cast<T>(start_di.date()));
            start_di.add_days(increment * 7);
            break;
        case time_frequency::daily:
            index_vec.push_back(static_cast<T>(start_di.date()));
            start_di.add_days(increment);
            break;
        case time_frequency::hourly:
            index_vec.push_back(static_cast<T>(start_di.time()));
            start_di.add_seconds(increment * 60 * 60);
            break;
        case time_frequency::minutely:
            index_vec.push_back(static_cast<T>(start_di.time()));
            start_di.add_seconds(increment * 60);
            break;
        case time_frequency::secondly:
            index_vec.push_back(static_cast<T>(start_di.time()));
            start_di.add_seconds(increment);
            break;
        case time_frequency::millisecondly:
            index_vec.push_back(static_cast<T>(start_di.long_time()));
            start_di.add_nanoseconds(increment * 1000000);
            break;
        default:
            break;
        }
    }
};

// ----------------------------------------------------------------------------

template<typename Dummy>
struct  GenerateTSIndex_<DateTime, Dummy>  {

    inline void
    operator ()(StlVecType<DateTime> &index_vec,
                DateTime &start_di,
                time_frequency t_freq,
                long increment) const  {

        index_vec.push_back(start_di);
        switch(t_freq)  {
        case time_frequency::annual:
            start_di.add_years(increment);
            break;
        case time_frequency::monthly:
            start_di.add_months(increment);
            break;
        case time_frequency::weekly:
            start_di.add_days(increment * 7);
            break;
        case time_frequency::daily:
            start_di.add_days(increment);
            break;
        case time_frequency::hourly:
            start_di.add_seconds(increment * 60 * 60);
            break;
        case time_frequency::minutely:
            start_di.add_seconds(increment * 60);
            break;
        case time_frequency::secondly:
            start_di.add_seconds(increment);
            break;
        case time_frequency::millisecondly:
            start_di.add_nanoseconds(increment * 1000000);
            break;
        default:
            break;
        }
    }
};

// ----------------------------------------------------------------------------

template<typename T>
inline static void
get_mem_numbers_(const VectorView<T, align_value> &,
                 size_t &used_mem,
                 size_t &capacity_mem) {

    used_mem = sizeof(T *) * 2;
    capacity_mem = sizeof(T *) * 2;
}

// ----------------------------------------------------------------------------

template<typename T>
inline static void
get_mem_numbers_(const VectorPtrView<T, align_value> &container,
                 size_t &used_mem,
                 size_t &capacity_mem) {

    used_mem = container.size() * sizeof(T *);
    capacity_mem = container.capacity() * sizeof(T *);
}

// ----------------------------------------------------------------------------

template<typename T>
inline static void
get_mem_numbers_(const StlVecType<T> &container,
                 size_t &used_mem,
                 size_t &capacity_mem) {

    used_mem = container.size() * sizeof(T);
    capacity_mem = container.capacity() * sizeof(T);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
