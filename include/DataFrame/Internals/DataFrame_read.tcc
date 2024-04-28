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

#include <DataFrame/DataFrame.h>
#include <DataFrame/Utils/FixedSizeString.h>
#include <DataFrame/Utils/Utils.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
void DataFrame<I, H>::read_json_(std::istream &stream, bool columns_only)  {

    char            c { '\0' };
    const SpinGuard guard(lock_);

    while (stream.get(c))
        if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
    if (c != '{') [[unlikely]]
        throw DataFrameError(
            "DataFrame::read_json_(): ERROR: Expected '{' (0)");

    bool        first_col = true;
    bool        has_index = true;
    std::string col_name;
    std::string col_type;
    std::string token;

    while (stream.get(c)) [[likely]] {
        col_name.clear();
        col_type.clear();
        token.clear();
        if (c == ' ' || c == '\n' || c == '\t' || c == '\r') [[unlikely]]
            continue;
        if (c != '"') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (1)");
        _get_token_from_file_(stream, '"', col_name);
        if (first_col && ! columns_only)  {
            if (col_name != DF_INDEX_COL_NAME)
                throw DataFrameError("DataFrame::read_json_(): ERROR: "
                                     "Expected column name 'INDEX'");
        }
        else if (! first_col) [[likely]]  {
            if (col_name == DF_INDEX_COL_NAME) [[unlikely]]
                throw DataFrameError("DataFrame::read_json_(): ERROR: "
                                     "column name 'INDEX' is not allowed");
        }
        if (first_col && col_name != DF_INDEX_COL_NAME)
            has_index = false;

        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (2)");
        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '{') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '{' (3)");

        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '"') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (4)");

        _get_token_from_file_(stream, '"', token);
        if (token != "N") [[unlikely]]
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'N' (5)");
        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ':') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (6)");
        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        _get_token_from_file_(stream, ',', token);

        const size_type col_size = ::atoi(token.c_str());

        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '"') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (7)");
        _get_token_from_file_(stream, '"', token);
        if (token != "T") [[unlikely]]
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'T' (8)");
        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ':') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (9)");
        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '"') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (10)");
        _get_token_from_file_(stream, '"', col_type);

        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ',') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ',' (11)");
        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '"') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (12)");
        _get_token_from_file_(stream, '"', token);
        if (token != "D") [[unlikely]]
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'D' (13)");
        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ':') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (14)");
        while (stream.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '[') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ']' (15)");

        // Is this the index column, and should we load it?
        //
        if (first_col && has_index) [[unlikely]]  {
            IndexVecType    vec;

            vec.reserve(col_size);
            IdxParserFunctor_<IndexType>()(vec, stream, io_format::json);
            if (! columns_only)
                load_index(std::forward<IndexVecType &&>(vec));
        }
        else  {
            if (col_type == "float")  {
                StlVecType<float>                                   &vec =
                    create_column<float>(col_name.c_str(), false);
                const ColVectorPushBack_<float, StlVecType<float>>  slug;

                vec.reserve(col_size);
                slug(vec, stream, &::strtof, io_format::json);
            }
            else if (col_type == "double") [[likely]]  {
                StlVecType<double>                                    &vec =
                    create_column<double>(col_name.c_str(), false);
                const ColVectorPushBack_<double, StlVecType<double>>  slug;

                vec.reserve(col_size);
                slug(vec, stream, &::strtod, io_format::json);
            }
            else if (col_type == "longdouble")  {
                StlVecType<long double>                     &vec =
                    create_column<long double>(col_name.c_str(), false);
                const ColVectorPushBack_
                    <long double, StlVecType<long double>>  slug;

                vec.reserve(col_size);
                slug(vec, stream, &::strtold, io_format::json);
            }
            else if (col_type == "int")  {
                StlVecType<int> &vec =
                    create_column<int>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_(vec,
                                           stream,
                                           &::strtol,
                                           io_format::json);
            }
            else if (col_type == "uint")  {
                StlVecType<unsigned int>   &vec =
                    create_column<unsigned int>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_(vec,
                                           stream,
                                           &::strtoul,
                                           io_format::json);
            }
            else if (col_type == "char")  {
                StlVecType<char>    &vec =
                    create_column<char>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_<char, StlVecType<char>>(
                    vec,
                    stream,
                    [](const char *tok, char **, int) -> char  {
                        if (tok[0] == '\0')
                            return ('\0');
                        else if (tok[1] == '\0')
                            return (static_cast<char>(int(tok[0])));
                        else
                            return (static_cast<char>(atoi(tok)));
                    },
                    io_format::json);
            }
            else if (col_type == "uchar")  {
                StlVecType<unsigned char>   &vec =
                    create_column<unsigned char>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_<unsigned char,
                                           StlVecType<unsigned char>>(
                    vec,
                    stream,
                    [](const char *tok, char **, int) -> unsigned char  {
                        if (tok[0] == '\0')
                            return ('\0');
                        else if (tok[1] == '\0')
                            return (static_cast<unsigned char>(int(tok[0])));
                        else
                            return (static_cast<unsigned char>(atoi(tok)));
                    },
                    io_format::json);
            }
            else if (col_type == "long")  {
                StlVecType<long>    &vec =
                    create_column<long>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_(vec,
                                           stream,
                                           &::strtol,
                                           io_format::json);
            }
            else if (col_type == "longlong")  {
                StlVecType<long long>   &vec =
                    create_column<long long>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_(vec,
                                           stream,
                                           &::strtoll,
                                           io_format::json);
            }
            else if (col_type == "ulong")  {
                StlVecType<unsigned long>   &vec =
                    create_column<unsigned long>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_(vec,
                                           stream,
                                           &::strtoul,
                                           io_format::json);
            }
            else if (col_type == "ulonglong")  {
                StlVecType<unsigned long long>  &vec =
                    create_column<unsigned long long>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_(vec,
                                           stream,
                                           &::strtoull,
                                           io_format::json);
            }
            else if (col_type == "string")  {
                StlVecType<std::string> &vec =
                    create_column<std::string>(col_name.c_str(), false);

                vec.reserve(col_size);
                json_str_col_vector_push_back_(vec, stream);
            }
            else if (col_type == "DateTime")  {
                StlVecType<DateTime>    &vec =
                    create_column<DateTime>(col_name.c_str(), false);
                auto                    converter =
                    [](const char *, char **) -> DateTime {
                        return DateTime();
                    };
                const ColVectorPushBack_<DateTime, StlVecType<DateTime>>  slug;

                vec.reserve(col_size);
                slug(vec, stream, converter, io_format::json);
            }
            else if (col_type == "bool")  {
                StlVecType<bool>    &vec =
                    create_column<bool>(col_name.c_str(), false);

                vec.reserve(col_size);
                col_vector_push_back_func_(vec,
                                           stream,
                                           &::strtol,
                                           io_format::json);
            }
            else
                throw DataFrameError (
                    "DataFrame::read_json_(): ERROR: Unknown column type");
        }
        while (stream.get(c))
            if (c != ' ' && c != '\r' && c != '\t' && c != '\n')  break;
        if (c != '}') [[unlikely]]
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '}' (16)");
        while (stream.get(c))
            if (c != ' ' && c != '\r' && c != '\t' && c != '\n')  break;
        if (c != ',')  {
            stream.unget();
            break;
        }

        first_col = false;
    }
    while (stream.get(c))
        if (c != ' ' && c != '\r' && c != '\t' && c != '\n')  break;
    if (c != '}') [[unlikely]]
        throw DataFrameError(
            "DataFrame::read_json_(): ERROR: Expected '}' (17)");
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::read_csv_(std::istream &stream, bool columns_only)  {

    std::string     col_name;
    std::string     value;
    std::string     type_str;
    char            c;
    const SpinGuard guard(lock_);

    col_name.reserve(128);
    value.reserve(1024);
    type_str.reserve(16);
    while (stream.get(c)) {
        col_name.clear();
        value.clear();
        type_str.clear();
        if (c == '#' || c == '\r' || c == '\0' || c == '\n') {
            if (c == '#')
                while (stream.get(c))
                    if (c == '\n') break;

            continue;
        }
        stream.unget();

        _get_token_from_file_(stream, ':', col_name);
        _get_token_from_file_(stream, ':', value); // Get the size
        stream.get(c);
        if (c != '<') [[unlikely]]
            throw DataFrameError("DataFrame::read_csv_(): ERROR: Expected "
                                 "'<' char to specify column type");
        _get_token_from_file_(stream, '>', type_str);
        stream.get(c);
        if (c != ':') [[unlikely]]
            throw DataFrameError("DataFrame::read_csv_(): ERROR: Expected "
                                 "':' char to start column values");

        if (col_name == DF_INDEX_COL_NAME)  {
            IndexVecType    vec;

            vec.reserve(::atoi(value.c_str()));
            IdxParserFunctor_<IndexType>()(vec, stream);
            if (! columns_only)
                load_index(std::forward<IndexVecType &&>(vec));
        }
        else [[likely]]  {
            if (type_str == "float")  {
                StlVecType<float>                                   &vec =
                    create_column<float>(col_name.c_str(), false);
                const ColVectorPushBack_<float, StlVecType<float>>  slug;

                vec.reserve(::atoi(value.c_str()));
                slug(vec, stream, &::strtof);
            }
            else if (type_str == "double") [[likely]]  {
                StlVecType<double>                                    &vec =
                    create_column<double>(col_name.c_str(), false);
                const ColVectorPushBack_<double, StlVecType<double>>  slug;

                vec.reserve(::atoi(value.c_str()));
                slug(vec, stream, &::strtod);
            }
            else if (type_str == "longdouble")  {
                StlVecType<long double>                     &vec =
                    create_column<long double>(col_name.c_str(), false);
                const ColVectorPushBack_
                    <long double, StlVecType<long double>>  slug;

                vec.reserve(::atoi(value.c_str()));
                slug(vec, stream, &::strtold);
            }
            else if (type_str == "int")  {
                StlVecType<int> &vec =
                    create_column<int>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_(vec, stream, &::strtol);
            }
            else if (type_str == "uint")  {
                StlVecType<unsigned int>   &vec =
                    create_column<unsigned int>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_(vec, stream, &::strtoul);
            }
            else if (type_str == "char")  {
                StlVecType<char>    &vec =
                    create_column<char>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_<char, StlVecType<char>>(
                    vec,
                    stream,
                    [](const char *tok, char **, int) -> char  {
                        if (tok[0] == '\0')
                            return ('\0');
                        else if (tok[1] == '\0')
                            return (static_cast<char>(int(tok[0])));
                        else
                            return (static_cast<char>(atoi(tok)));
                    });
            }
            else if (type_str == "uchar")  {
                StlVecType<unsigned char>   &vec =
                    create_column<unsigned char>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_<unsigned char,
                                           StlVecType<unsigned char>>(
                    vec,
                    stream,
                    [](const char *tok, char **, int) -> unsigned char  {
                        if (tok[0] == '\0')
                            return ('\0');
                        else if (tok[1] == '\0')
                            return (static_cast<unsigned char>(int(tok[0])));
                        else
                            return (static_cast<unsigned char>(atoi(tok)));
                    });
            }
            else if (type_str == "long")  {
                StlVecType<long>    &vec =
                    create_column<long>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_(vec, stream, &::strtol);
            }
            else if (type_str == "longlong")  {
                StlVecType<long long>   &vec =
                    create_column<long long>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_(vec, stream, &::strtoll);
            }
            else if (type_str == "ulong")  {
                StlVecType<unsigned long>   &vec =
                    create_column<unsigned long>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_(vec, stream, &::strtoul);
            }
            else if (type_str == "ulonglong")  {
                StlVecType<unsigned long long>  &vec =
                    create_column<unsigned long long>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_(vec, stream, &::strtoull);
            }
            else if (type_str == "string")  {
                StlVecType<std::string>                     &vec =
                    create_column<std::string>(col_name.c_str(), false);
                auto                                        converter =
                    [](const char *s, char **)-> const char * { return s; };
                const ColVectorPushBack_
                    <const char *, StlVecType<std::string>> slug;

                vec.reserve(::atoi(value.c_str()));
                slug(vec, stream, converter);
            }
            else if (type_str == "DateTime")  {
                StlVecType<DateTime>    &vec =
                    create_column<DateTime>(col_name.c_str(), false);
                auto                    converter =
                    [](const char *, char **) -> DateTime {
                        return DateTime();
                    };
                const ColVectorPushBack_<DateTime, StlVecType<DateTime>>  slug;

                vec.reserve(::atoi(value.c_str()));
                slug (vec, stream, converter);
            }
            else if (type_str == "bool")  {
                StlVecType<bool>    &vec =
                    create_column<bool>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_func_(vec, stream, &::strtol);
            }

            // Containers
            //
            else if (type_str == "dbl_vec")  {
                using vec_t = std::vector<double>;

                StlVecType<vec_t>   &vec =
                    create_column<vec_t>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_cont_func_(vec,
                                                stream,
                                                &_get_dbl_vec_from_value_);
            }
            else if (type_str == "str_vec")  {
                using vec_t = std::vector<std::string>;

                StlVecType<vec_t>   &vec =
                    create_column<vec_t>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_cont_func_(vec,
                                                stream,
                                                &_get_str_vec_from_value_);
            }
            else if (type_str == "dbl_set")  {
                using set_t = std::set<double>;

                StlVecType<set_t>   &vec =
                    create_column<set_t>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_cont_func_(vec,
                                                stream,
                                                &_get_dbl_set_from_value_);
            }
            else if (type_str == "str_set")  {
                using set_t = std::set<std::string>;

                StlVecType<set_t>   &vec =
                    create_column<set_t>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_cont_func_(vec,
                                                stream,
                                                &_get_str_set_from_value_);
            }
            else if (type_str == "str_dbl_map")  {
                using map_t = std::map<std::string, double>;

                StlVecType<map_t>   &vec =
                    create_column<map_t>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_cont_func_(
                    vec,
                    stream,
                    &_get_str_dbl_map_from_value_<map_t>);
            }
            else if (type_str == "str_dbl_unomap")  {
                using map_t = std::unordered_map<std::string, double>;

                StlVecType<map_t>   &vec =
                    create_column<map_t>(col_name.c_str(), false);

                vec.reserve(::atoi(value.c_str()));
                col_vector_push_back_cont_func_(
                    vec,
                    stream,
                    &_get_str_dbl_map_from_value_<map_t>);
            }
            else [[unlikely]]
                throw DataFrameError("DataFrame::read_csv_(): ERROR: Unknown "
                                     "column type");
        }
    }
}

// ----------------------------------------------------------------------------

struct _col_data_spec_  {

    std::any    col_vec { };
    String32    type_spec { };
    String64    col_name { };

    template<typename V>
    _col_data_spec_(V cv, const char *ts, const char *cn, std::size_t rs)
        : col_vec(cv), type_spec(ts), col_name(cn)  {

        std::any_cast<V &>(col_vec).reserve(rs);
    }
};

// --------------------------------------

template<typename I, typename H>
void DataFrame<I, H>::
read_csv2_(std::istream &stream,
           bool columns_only,
           size_type starting_row,
           size_type num_rows)  {

    std::string                 value;
    std::string                 col_name;
    std::string                 type_str;
    char                        c;
    StlVecType<_col_data_spec_> spec_vec;
    bool                        header_read = false;
    size_type                   col_index = 0;
    size_type                   col_count = 0;
    size_type                   data_rows_read = 0;

    spec_vec.reserve(32);
    value.reserve(1024);
    col_name.reserve(128);
    type_str.reserve(16);
    while (stream.get(c)) {
        value.clear();
        if (c == '#')  {
            while (stream.get(c))
                if (c == '\n') break;
        }
        else if (c == '\r' || c == '\n' || c == '\0')  {
            if (c == '\r' || c == '\n')  {
                if (header_read && ++data_rows_read >= num_rows)  break;

                if (col_index == col_count)  {
                    col_index = 0;
                    continue;
                }
            }
        }
        stream.unget();

        // First get the header which is column names, sizes and types
        //
        if (! header_read) [[unlikely]]  {
            col_name.clear();
            type_str.clear();
            _get_token_from_file_(stream, ':', col_name);
            _get_token_from_file_(stream, ':', value); // Get the size
            stream.get(c);
            if (c != '<') [[unlikely]]
                throw DataFrameError(
                    "DataFrame::read_csv2_(): ERROR: Expected "
                    "'<' char to specify column type");
            _get_token_from_file_(stream, '>', type_str);
            stream.get(c);
            if (c == '\r' || c == '\n' || c == '\0')  {
                if (c == '\r')  stream.get(c);
                header_read = true;

                size_type   row_cnt = 0;

                // Jump to the starting row
                //
                while (row_cnt < starting_row && stream.get(c))
                    if (c == '\r' || c == '\n')
                        row_cnt += 1;
            }

            const size_type nrows =
                num_rows == std::numeric_limits<size_type>::max()
                    ? size_type(atol(value.c_str())) : num_rows;

            if (type_str == "float")
                spec_vec.emplace_back(StlVecType<float>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "double") [[likely]]
                spec_vec.emplace_back(StlVecType<double>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "longdouble")
                spec_vec.emplace_back(StlVecType<long double>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "int")
                spec_vec.emplace_back(StlVecType<int>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "uint")
                spec_vec.emplace_back(StlVecType<unsigned int>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "char")
                spec_vec.emplace_back(StlVecType<char>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "uchar")
                spec_vec.emplace_back(StlVecType<unsigned char>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "long")
                spec_vec.emplace_back(StlVecType<long>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "longlong")
                spec_vec.emplace_back(StlVecType<long long>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "ulong")
                spec_vec.emplace_back(StlVecType<unsigned long>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "ulonglong")
                spec_vec.emplace_back(StlVecType<unsigned long long>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "string")
                spec_vec.emplace_back(StlVecType<std::string>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            // This includes DateTime, DateTimeAME, DateTimeEUR, DateTimeISO
            //
            else if (! ::strncmp(type_str.c_str(), "DateTime", 8))
                spec_vec.emplace_back(StlVecType<DateTime>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "bool")
                spec_vec.emplace_back(StlVecType<bool>(),
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            // Containers
            //
            else if (type_str == "dbl_vec")
                spec_vec.emplace_back(StlVecType<std::vector<double>>{ },
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "str_vec")
                spec_vec.emplace_back(StlVecType<std::vector<std::string>>{ },
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "dbl_set")
                spec_vec.emplace_back(StlVecType<std::set<double>>{ },
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "str_set")
                spec_vec.emplace_back(StlVecType<std::set<std::string>>{ },
                                      type_str.c_str(),
                                      col_name.c_str(),
                                      nrows);
            else if (type_str == "str_dbl_map")
                spec_vec.emplace_back(
                    StlVecType<std::map<std::string, double>>{ },
                    type_str.c_str(),
                    col_name.c_str(),
                    nrows);
            else if (type_str == "str_dbl_unomap")
                spec_vec.emplace_back(
                    StlVecType<std::unordered_map<std::string, double>>{ },
                    type_str.c_str(),
                    col_name.c_str(),
                    nrows);
            else
                throw DataFrameError("DataFrame::read_csv2_(): ERROR: "
                                     "Unknown column type");

            col_count += 1;
        }
        else [[likely]]  {  // Now read the data columns row by row
            _get_token_from_file_(stream, ',', value, '\n');

            _col_data_spec_ &col_spec = spec_vec[col_index];

            if (col_spec.type_spec == "float")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<float> &>
                        (col_spec.col_vec).push_back(
                            strtof(value.c_str(), nullptr));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<float> &>
                        (col_spec.col_vec).push_back(get_nan<float>());
                }
            }
            else if (col_spec.type_spec == "double") [[likely]]  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<double> &>
                        (col_spec.col_vec).push_back(
                            strtod(value.c_str(), nullptr));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<double> &>
                        (col_spec.col_vec).push_back(get_nan<double>());
                }
            }
            else if (col_spec.type_spec == "longdouble")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<long double> &>
                        (col_spec.col_vec).push_back(
                            strtold(value.c_str(), nullptr));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<long double> &>
                        (col_spec.col_vec).push_back(get_nan<long double>());
                }
            }
            else if (col_spec.type_spec == "int")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<int> &>
                        (col_spec.col_vec).push_back(
                            (int) strtol(value.c_str(), nullptr, 0));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<int> &>
                        (col_spec.col_vec).push_back(get_nan<int>());
                }
            }
            else if (col_spec.type_spec == "uint")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<unsigned int> &>
                        (col_spec.col_vec).push_back(
                            (unsigned int) strtoul(value.c_str(), nullptr, 0));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<unsigned int> &>
                        (col_spec.col_vec).push_back(get_nan<unsigned int>());
                }
            }
            else if (col_spec.type_spec == "char")  {
                if (value.size() > 1)  {
                    std::any_cast<StlVecType<char> &>
                        (col_spec.col_vec).push_back(
                            static_cast<char>(atoi(value.c_str())));
                }
                else if (! value.empty())  {
                    std::any_cast<StlVecType<char> &>
                        (col_spec.col_vec).push_back(value[0]);
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<char> &>
                        (col_spec.col_vec).push_back(get_nan<char>());
                }
            }
            else if (col_spec.type_spec == "uchar")  {
                if (value.size() > 1)  {
                    std::any_cast<StlVecType<unsigned char> &>
                        (col_spec.col_vec).push_back(
                            static_cast<unsigned char>(atoi(value.c_str())));
                }
                else if (! value.empty())  {
                    std::any_cast<StlVecType<unsigned char> &>
                        (col_spec.col_vec).push_back(
                            static_cast<unsigned char>(value[0]));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<unsigned char> &>
                        (col_spec.col_vec).push_back(get_nan<unsigned char>());
                }
            }
            else if (col_spec.type_spec == "long")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<long> &>
                        (col_spec.col_vec).push_back(
                            strtol(value.c_str(), nullptr, 0));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<long> &>
                        (col_spec.col_vec).push_back(get_nan<long>());
                }
            }
            else if (col_spec.type_spec == "longlong")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<long long> &>
                        (col_spec.col_vec).push_back(
                            strtoll(value.c_str(), nullptr, 0));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<long long> &>
                        (col_spec.col_vec).push_back(get_nan<long long>());
                }
            }
            else if (col_spec.type_spec == "ulong")  {
                if (! value.empty())  {
                    const unsigned long         v =
                        strtoul(value.c_str(), nullptr, 0);
                    StlVecType<unsigned long>  &vec =
                        std::any_cast<StlVecType<unsigned long> &>
                            (col_spec.col_vec);

                    vec.push_back(v);
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<unsigned long> &>
                        (col_spec.col_vec).push_back(get_nan<unsigned long>());
                }
            }
            else if (col_spec.type_spec == "ulonglong")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<unsigned long long> &>
                        (col_spec.col_vec).push_back(
                            strtoull(value.c_str(), nullptr, 0));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<unsigned long long> &>
                        (col_spec.col_vec).push_back(
                            get_nan<unsigned long long>());
                }
            }
            else if (col_spec.type_spec == "string")  {
                std::any_cast<StlVecType<std::string> &>
                    (col_spec.col_vec).emplace_back(value);
            }
            else if (col_spec.type_spec == "DateTime")  {
                if (! value.empty())  {
                    time_t      t;
                    int         n;
                    DateTime    dt;

#ifdef _MSC_VER
                    ::sscanf(value.c_str(), "%lld.%d", &t, &n);
#else
                    ::sscanf(value.c_str(), "%ld.%d", &t, &n);
#endif // _MSC_VER
                    dt.set_time(t, n);
                    std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec).emplace_back(std::move(dt));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec).push_back(get_nan<DateTime>());
                }
            }
            else if (col_spec.type_spec == "DateTimeAME")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec).emplace_back(
                            value.c_str(), DT_DATE_STYLE::AME_STYLE);
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec).push_back(get_nan<DateTime>());
                }
            }
            else if (col_spec.type_spec == "DateTimeEUR")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec).emplace_back(
                            value.c_str(), DT_DATE_STYLE::EUR_STYLE);
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec).push_back(get_nan<DateTime>());
                }
            }
            else if (col_spec.type_spec == "DateTimeISO")  {
                if (! value.empty())  {
                    std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec).emplace_back(
                            value.c_str(), DT_DATE_STYLE::ISO_STYLE);
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec).push_back(get_nan<DateTime>());
                }
            }
            else if (col_spec.type_spec == "bool")  {
                if (! value.empty())  {
                    const bool          v =
                        static_cast<bool>(strtoul(value.c_str(), nullptr, 0));
                    StlVecType<bool>   &vec =
                        std::any_cast<StlVecType<bool> &>(col_spec.col_vec);

                    vec.push_back(v);
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<bool> &>
                        (col_spec.col_vec).push_back(get_nan<bool>());
                }
            }

            // Containers
            //
            else if (col_spec.type_spec == "dbl_vec")  {
                if (! value.empty())  {
                    StlVecType<std::vector<double>>  &vec =
                        std::any_cast<StlVecType<std::vector<double>> &>
                            (col_spec.col_vec);

                    vec.push_back(
                        std::move(_get_dbl_vec_from_value_(value.c_str())));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<std::vector<double>> &>
                        (col_spec.col_vec).push_back(std::vector<double> { });
                }
            }
            else if (col_spec.type_spec == "str_vec")  {
                if (! value.empty())  {
                    StlVecType<std::vector<std::string>> &vec =
                        std::any_cast<StlVecType<std::vector<std::string>> &>
                            (col_spec.col_vec);

                    vec.push_back(
                        std::move(_get_str_vec_from_value_(value.c_str())));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<std::vector<std::string>> &>
                        (col_spec.col_vec).push_back(
                            std::vector<std::string> { });
                }
            }
            else if (col_spec.type_spec == "dbl_set")  {
                using set_t = std::set<double>;

                if (! value.empty())  {
                    StlVecType<set_t>   &vec =
                        std::any_cast<StlVecType<set_t> &>(col_spec.col_vec);

                    vec.push_back(std::move(_get_dbl_set_from_value_(
                                      value.c_str())));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<set_t> &>
                        (col_spec.col_vec).push_back(set_t { });
                }
            }
            else if (col_spec.type_spec == "str_set")  {
                using set_t = std::set<std::string>;

                if (! value.empty())  {
                    StlVecType<set_t>   &vec =
                        std::any_cast<StlVecType<set_t> &>(col_spec.col_vec);

                    vec.push_back(std::move(_get_str_set_from_value_(
                                      value.c_str())));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<set_t> &>
                        (col_spec.col_vec).push_back(set_t { });
                }
            }
            else if (col_spec.type_spec == "str_dbl_map")  {
                using map_t = std::map<std::string, double>;

                if (! value.empty())  {
                    StlVecType<map_t>   &vec =
                        std::any_cast<StlVecType<map_t> &>(col_spec.col_vec);

                    vec.push_back(
                        std::move(_get_str_dbl_map_from_value_<map_t>(
                        value.c_str())));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<map_t> &>
                        (col_spec.col_vec).push_back(map_t { });
                }
            }
            else if (col_spec.type_spec == "str_dbl_unomap")  {
                using map_t = std::unordered_map<std::string, double>;

                if (! value.empty())  {
                    StlVecType<map_t>   &vec =
                        std::any_cast<StlVecType<map_t> &> (col_spec.col_vec);

                    vec.push_back(
                        std::move(_get_str_dbl_map_from_value_<map_t>(
                        value.c_str())));
                }
                else [[unlikely]]  {
                    std::any_cast<StlVecType<map_t> &>
                        (col_spec.col_vec).push_back(map_t { });
                }
            }

            col_index += 1;
        }
    }

    const size_type spec_s = spec_vec.size();

    if (spec_s > 0)  {  // Now load the data into the DataFrame
        if (spec_vec[0].col_name != DF_INDEX_COL_NAME &&
            ! columns_only) [[unlikely]]
            throw DataFrameError("DataFrame::read_csv2_(): ERROR: "
                                 "Index column is not the first column");
        if (! columns_only) [[likely]]
            load_index(std::move(
                std::any_cast<IndexVecType &>(spec_vec[0].col_vec)));

        const size_type begin =
            spec_vec[0].col_name == DF_INDEX_COL_NAME ? 1 : 0;

        for (size_type i = begin; i < spec_s; ++i) [[likely]]  {
            _col_data_spec_ col_spec = spec_vec[i];

            if (col_spec.type_spec == "float")
                load_column<float>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<float> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "double") [[likely]]
                load_column<double>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<double> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "longdouble")
                load_column<long double>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<long double> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "int")
                load_column<int>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<int> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "uint")
                load_column<unsigned int>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<unsigned int> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "char")
                load_column<char>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<char> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "uchar")
                load_column<unsigned char>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<unsigned char> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "long")
                load_column<long>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<long> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "longlong")
                load_column<long long>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<long long> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "ulong")
                load_column<unsigned long>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<unsigned long>&>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "ulonglong")
                load_column<unsigned long long>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<unsigned long long> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "string")
                load_column<std::string>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<std::string> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (! ::strncmp(col_spec.type_spec.c_str(), "DateTime", 8))
                load_column<DateTime>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<DateTime> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "bool")
                load_column<bool>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<bool> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);

            // Containers
            //
            else if (col_spec.type_spec == "dbl_vec")
                load_column<std::vector<double>>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<std::vector<double>> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "str_vec")
                load_column<std::vector<std::string>>(
                    col_spec.col_name.c_str(),
                    std::move(
                        std::any_cast<StlVecType<std::vector<std::string>> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "dbl_set")  {
                using set_t = std::set<double>;

                load_column<set_t>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<set_t> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            }
            else if (col_spec.type_spec == "str_set")  {
                using set_t = std::set<std::string>;

                load_column<set_t>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<set_t> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            }
            else if (col_spec.type_spec == "str_dbl_map")  {
                using map_t = std::map<std::string, double>;

                load_column<map_t>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<map_t> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            }
            else if (col_spec.type_spec == "str_dbl_unomap")  {
                using map_t = std::unordered_map<std::string, double>;

                load_column<map_t>(
                    col_spec.col_name.c_str(),
                    std::move(std::any_cast<StlVecType<map_t> &>
                        (col_spec.col_vec)),
                    nan_policy::dont_pad_with_nans);
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool DataFrame<I, H>::
read (const char *file_name,
      io_format iof,
      bool columns_only,
      size_type starting_row,
      size_type num_rows)  {

    std::ifstream       stream;
    const IOStreamOpti  io_opti(stream, file_name);

    if (stream.fail()) [[unlikely]]  {
        String1K    err;

        err.printf("read(): ERROR: Unable to open file '%s'", file_name);
        throw DataFrameError(err.c_str());
    }

    read<std::istream>(stream, iof, columns_only, starting_row, num_rows);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S>
bool DataFrame<I, H>::
read (S &in_s,
      io_format iof,
      bool columns_only,
      size_type starting_row,
      size_type num_rows)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call read()");

    if (iof == io_format::csv) [[likely]]  {
        if (starting_row != 0 ||
            num_rows != std::numeric_limits<size_type>::max()) [[unlikely]]
            throw NotImplemented("read(): Reading files in chunks is currently"
                                 " only implemented for io_format::csv2");

        read_csv_ (in_s, columns_only);
    }
    else if (iof == io_format::csv2)  {
        read_csv2_ (in_s, columns_only, starting_row, num_rows);
    }
    else if (iof == io_format::json)  {
        if (starting_row != 0 ||
            num_rows != std::numeric_limits<size_type>::max()) [[unlikely]]
            throw NotImplemented("read(): Reading files in chunks is currently"
                                 " only implemented for io_format::csv2");

        read_json_ (in_s, columns_only);
    }
    else
        throw NotImplemented("read(): This io_format is not implemented");

    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool
DataFrame<I, H>::from_string (const char *data_frame)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call from_string()");

    std::stringstream   ss (std::string(data_frame), std::ios_base::in);

    read<std::istream>(ss, io_format::csv, false);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
std::future<bool> DataFrame<I, H>::
read_async(const char *file_name,
           io_format iof,
           bool columns_only,
           size_type starting_row,
           size_type num_rows) {

    return (thr_pool_.dispatch(
                true,
                [file_name,
                 iof,
                 columns_only,
                 starting_row,
                 num_rows,
                 this] () -> bool  {
                    return (this->read(file_name,
                                       iof,
                                       columns_only,
                                       starting_row,
                                       num_rows));
                }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S>
std::future<bool> DataFrame<I, H>::
read_async(S &in_s,
           io_format iof,
           bool columns_only,
           size_type starting_row,
           size_type num_rows) {

    return (thr_pool_.dispatch(
                true,
                [&in_s,
                 iof,
                 columns_only,
                 starting_row,
                 num_rows,
                 this] () -> bool  {
                    return (this->read<S>(in_s,
                                          iof,
                                          columns_only,
                                          starting_row,
                                          num_rows));
                }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
std::future<bool>
DataFrame<I, H>::from_string_async(const char *data_frame)  {

    return (thr_pool_.dispatch(true,
                               &DataFrame::from_string,
                                   this,
                                   data_frame));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
