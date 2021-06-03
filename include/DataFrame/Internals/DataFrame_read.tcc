// Hossein Moein
// September 12, 2017
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

#include <DataFrame/DataFrame.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <any>
#include <cstdlib>
#include <fstream>
#include <functional>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define gcc_likely(x)    __builtin_expect(!!(x), 1)
#define gcc_unlikely(x)  __builtin_expect(!!(x), 0)

// ----------------------------------------------------------------------------

template<typename I, typename  H>
void DataFrame<I, H>::read_json_(std::istream &file, bool columns_only)  {

    char    c { '\0' };
    char    col_name[256];
    char    col_type[256];
    char    token[256];

    while (file.get(c))
        if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
    if (c != '{')
        throw DataFrameError(
            "DataFrame::read_json_(): ERROR: Expected '{' (0)");

    bool    first_col = true;
    bool    has_index = true;

    while (file.get(c)) {
        if (c == ' ' || c == '\n' || c == '\t' || c == '\r')  continue;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (1)");
        _get_token_from_file_(file, '"', col_name);
        if (first_col && ! columns_only)  {
            if (strcmp(col_name, DF_INDEX_COL_NAME))
                throw DataFrameError("DataFrame::read_json_(): ERROR: "
                                     "Expected column name 'INDEX'");
        }
        else if (! first_col)  {
            if (! strcmp(col_name, DF_INDEX_COL_NAME))
                throw DataFrameError("DataFrame::read_json_(): ERROR: "
                                     "column name 'INDEX' is not allowed");
        }
        if (first_col && strcmp(col_name, DF_INDEX_COL_NAME))
            has_index = false;

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (2)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '{')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '{' (3)");

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (4)");

        _get_token_from_file_(file, '"', token);
        if (strcmp(token, "N"))
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'N' (5)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (6)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        _get_token_from_file_(file, ',', token);

        const size_type col_size = ::atoi(token);

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (7)");
        _get_token_from_file_(file, '"', token);
        if (strcmp(token, "T"))
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'T' (8)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (9)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (10)");
        _get_token_from_file_(file, '"', col_type);

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ',')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ',' (11)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (12)");
        _get_token_from_file_(file, '"', token);
        if (strcmp(token, "D"))
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'D' (13)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (14)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '[')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ']' (15)");

        // Is this the index column, and should we load it?
        if (first_col && has_index)  {
            IndexVecType    vec;

            vec.reserve(col_size);
            _IdxParserFunctor_<IndexType>()(vec, file, io_format::json);
            if (! columns_only)
                load_index(std::forward<IndexVecType &&>(vec));
        }
        else  {
            if (! ::strcmp(col_type, "float"))  {
                std::vector<float>  &vec = create_column<float>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtof, io_format::json);
            }
            else if (! ::strcmp(col_type, "double"))  {
                std::vector<double> &vec = create_column<double>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtod, io_format::json);
            }
            else if (! ::strcmp(col_type, "longdouble"))  {
                std::vector<long double>    &vec =
                    create_column<long double>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtold, io_format::json);
            }
            else if (! ::strcmp(col_type, "int"))  {
                std::vector<int> &vec = create_column<int>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtol, io_format::json);
            }
            else if (! ::strcmp(col_type, "uint"))  {
                std::vector<unsigned int>   &vec =
                    create_column<unsigned int>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtoul, io_format::json);
            }
            else if (! ::strcmp(col_type, "long"))  {
                std::vector<long>   &vec = create_column<long>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtol, io_format::json);
            }
            else if (! ::strcmp(col_type, "longlong"))  {
                std::vector<long long>  &vec =
                    create_column<long long>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtoll, io_format::json);
            }
            else if (! ::strcmp(col_type, "ulong"))  {
                std::vector<unsigned long>  &vec =
                    create_column<unsigned long>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtoul, io_format::json);
            }
            else if (! ::strcmp(col_type, "ulonglong"))  {
                std::vector<unsigned long long> &vec =
                    create_column<unsigned long long>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtoull, io_format::json);
            }
            else if (! ::strcmp(col_type, "string"))  {
                std::vector<std::string>    &vec =
                    create_column<std::string>(col_name);

                vec.reserve(col_size);
                _json_str_col_vector_push_back_(vec, file);
            }
            else if (! ::strcmp(col_type, "DateTime"))  {
                std::vector<DateTime>   &vec =
                    create_column<DateTime>(col_name);
                auto                    converter =
                    [](const char *, char **)-> DateTime { return DateTime(); };

                vec.reserve(col_size);
                _col_vector_push_back_<DateTime, std::vector<DateTime>>
                    (vec, file, converter, io_format::json);
            }
            else if (! ::strcmp(col_type, "bool"))  {
                std::vector<bool>   &vec = create_column<bool>(col_name);

                vec.reserve(col_size);
                _col_vector_push_back_(vec, file, &::strtol, io_format::json);
            }
            else
                throw DataFrameError (
                    "DataFrame::read_json_(): ERROR: Unknown column type");
        }
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != '}')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '}' (16)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
        if (c != ',')  {
            file.unget();
            break;
        }

        first_col = false;
    }
    while (file.get(c))
        if (c != ' ' && c != '\n' && c != '\t' && c != '\r')  break;
    if (c != '}')
        throw DataFrameError(
            "DataFrame::read_json_(): ERROR: Expected '}' (17)");
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
void DataFrame<I, H>::read_csv_(std::istream &file, bool columns_only)  {

    char    col_name[256];
    char    value[32];
    char    type_str[64];
    char    c;

    while (file.get(c)) {
        if (c == '#' || c == '\n' || c == '\0' || c == '\r') {
            if (c == '#')
                while (file.get(c))
                    if (c == '\n') break;

            continue;
        }
        file.unget();

        _get_token_from_file_(file, ':', col_name);
        _get_token_from_file_(file, ':', value); // Get the size
        file.get(c);
        if (c != '<')
            throw DataFrameError("DataFrame::read_csv_(): ERROR: Expected "
                                 "'<' char to specify column type");
        _get_token_from_file_(file, '>', type_str);
        file.get(c);
        if (c != ':')
            throw DataFrameError("DataFrame::read_csv_(): ERROR: Expected "
                                 "':' char to start column values");

        if (! ::strcmp(col_name, DF_INDEX_COL_NAME))  {
            IndexVecType    vec;

            vec.reserve(::atoi(value));
            _IdxParserFunctor_<IndexType>()(vec, file);
            if (! columns_only)
                load_index(std::forward<IndexVecType &&>(vec));
        }
        else  {
            if (! ::strcmp(type_str, "float"))  {
                std::vector<float>  &vec = create_column<float>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtof);
            }
            else if (! ::strcmp(type_str, "double"))  {
                std::vector<double> &vec = create_column<double>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtod);
            }
            else if (! ::strcmp(type_str, "longdouble"))  {
                std::vector<long double>    &vec =
                    create_column<long double>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtold);
            }
            else if (! ::strcmp(type_str, "int"))  {
                std::vector<int> &vec = create_column<int>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtol);
            }
            else if (! ::strcmp(type_str, "uint"))  {
                std::vector<unsigned int>   &vec =
                    create_column<unsigned int>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtoul);
            }
            else if (! ::strcmp(type_str, "long"))  {
                std::vector<long>   &vec = create_column<long>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtol);
            }
            else if (! ::strcmp(type_str, "longlong"))  {
                std::vector<long long>  &vec =
                    create_column<long long>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtoll);
            }
            else if (! ::strcmp(type_str, "ulong"))  {
                std::vector<unsigned long>  &vec =
                    create_column<unsigned long>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtoul);
            }
            else if (! ::strcmp(type_str, "ulonglong"))  {
                std::vector<unsigned long long> &vec =
                    create_column<unsigned long long>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtoull);
            }
            else if (! ::strcmp(type_str, "string"))  {
                std::vector<std::string>    &vec =
                    create_column<std::string>(col_name);
                auto                        converter =
                    [](const char *s, char **)-> const char * { return s; };

                vec.reserve(::atoi(value));
                _col_vector_push_back_<const char *, std::vector<std::string>>
                    (vec, file, converter);
            }
            else if (! ::strcmp(type_str, "DateTime"))  {
                std::vector<DateTime>   &vec =
                    create_column<DateTime>(col_name);
                auto                    converter =
                    [](const char *, char **)-> DateTime { return DateTime(); };

                vec.reserve(::atoi(value));
                _col_vector_push_back_<DateTime, std::vector<DateTime>>
                    (vec, file, converter);
            }
            else if (! ::strcmp(type_str, "bool"))  {
                std::vector<bool>   &vec = create_column<bool>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::strtol);
            }
            else
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

    template<typename T>
    inline _col_data_spec_(std::vector<T> cv,
                           const char *ts,
                           const char *cn,
                           std::size_t rs)
        : col_vec(cv), type_spec(ts), col_name(cn)  {

        std::any_cast<std::vector<T> &>(col_vec).reserve(rs);
    }
};

// --------------------------------------

template<typename I, typename  H>
void DataFrame<I, H>::read_csv2_(std::istream &file, bool columns_only)  {

    char                            value[8192];
    char                            c;
    std::vector<_col_data_spec_>    spec_vec;
    bool                            header_read = false;
    size_type                       col_index = 0;

    spec_vec.reserve(32);
    while (file.get(c)) {
        if (c == '#' || c == '\n' || c == '\r' || c == '\0') {
            if (c == '#')  {
                while (file.get(c))
                    if (c == '\n') break;
            }
            else if (c == '\n')
                col_index = 0;

            continue;
        }
        file.unget();

        // First get the header which is column names, sizes and types
        if (! header_read)  {
             char   col_name[256];
             char   type_str[64];

            _get_token_from_file_(file, ':', col_name);
            _get_token_from_file_(file, ':', value); // Get the size
            file.get(c);
            if (c != '<')
                throw DataFrameError("DataFrame::read_csv2_(): ERROR: Expected "
                                     "'<' char to specify column type");
            _get_token_from_file_(file, '>', type_str);
            file.get(c);
            if (c == '\n' || c == '\r')  header_read = true;

            if (! ::strcmp(type_str, "float"))
                spec_vec.emplace_back(std::vector<float>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "double"))
                spec_vec.emplace_back(std::vector<double>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "longdouble"))
                spec_vec.emplace_back(std::vector<long double>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "int"))
                spec_vec.emplace_back(std::vector<int>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "uint"))
                spec_vec.emplace_back(std::vector<unsigned int>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "long"))
                spec_vec.emplace_back(std::vector<long>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "longlong"))
                spec_vec.emplace_back(std::vector<long long>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "ulong"))
                spec_vec.emplace_back(std::vector<unsigned long>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "ulonglong"))
                spec_vec.emplace_back(std::vector<unsigned long long>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "string"))
                spec_vec.emplace_back(std::vector<std::string>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            // This includes DateTime, DateTimeAME, DateTimeEUR, DateTimeISO
            else if (! ::strncmp(type_str, "DateTime", 8))
                spec_vec.emplace_back(std::vector<DateTime>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else if (! ::strcmp(type_str, "bool"))
                spec_vec.emplace_back(std::vector<bool>(),
                                      type_str,
                                      col_name,
                                      ::atoi(value));
            else
                throw DataFrameError("DataFrame::read_csv2_(): ERROR: "
                                     "Unknown column type");
        }
        else  {  // Now read the data columns row by row
            _get_token_from_file_(file, ',', value, '\n');

            _col_data_spec_ &col_spec = spec_vec[col_index];

            if (col_spec.type_spec == "float")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<float> &>
                        (col_spec.col_vec).push_back(strtof(value, nullptr));
            }
            else if (col_spec.type_spec == "double")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<double> &>
                        (col_spec.col_vec).push_back(strtod(value, nullptr));
            }
            else if (col_spec.type_spec == "longdouble")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<long double> &>
                        (col_spec.col_vec).push_back(strtold(value, nullptr));
            }
            else if (col_spec.type_spec == "int")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<int> &>
                        (col_spec.col_vec).push_back(
                            (int) strtol(value, nullptr, 0));
            }
            else if (col_spec.type_spec == "uint")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<unsigned int> &>
                        (col_spec.col_vec).push_back(
                            (unsigned int) strtoul(value, nullptr, 0));
            }
            else if (col_spec.type_spec == "long")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<long> &>
                        (col_spec.col_vec).push_back(
                            strtol(value, nullptr, 0));
            }
            else if (col_spec.type_spec == "longlong")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<long long> &>
                        (col_spec.col_vec).push_back(
                            strtoll(value, nullptr, 0));
            }
            else if (col_spec.type_spec == "ulong")  {
                if (value[0] != '\0')  {
                    const unsigned long         v = strtoul(value, nullptr, 0);
                    std::vector<unsigned long>  &vec =
                        std::any_cast<std::vector<unsigned long> &>
                            (col_spec.col_vec);

                    vec.push_back(v);
                }
            }
            else if (col_spec.type_spec == "ulonglong")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<unsigned long long> &>
                        (col_spec.col_vec).push_back(
                            strtoull(value, nullptr, 0));
            }
            else if (col_spec.type_spec == "string")  {
                std::any_cast<std::vector<std::string> &>
                    (col_spec.col_vec).emplace_back(value);
            }
            else if (col_spec.type_spec == "DateTime")  {
                if (value[0] != '\0')  {
                    time_t      t;
                    int         n;
                    DateTime    dt;

#ifdef _MSC_VER
                    ::sscanf(value, "%lld.%d", &t, &n);
#else
                    ::sscanf(value, "%ld.%d", &t, &n);
#endif // _MSC_VER
                    dt.set_time(t, n);
                    std::any_cast<std::vector<DateTime> &>
                        (col_spec.col_vec).emplace_back(
                            std::move(dt));
                }
            }
            else if (col_spec.type_spec == "DateTimeAME")  {
                std::any_cast<std::vector<DateTime> &>
                    (col_spec.col_vec).emplace_back(
                        value, DT_DATE_STYLE::AME_STYLE);
            }
            else if (col_spec.type_spec == "DateTimeEUR")  {
                std::any_cast<std::vector<DateTime> &>
                    (col_spec.col_vec).emplace_back(
                        value, DT_DATE_STYLE::EUR_STYLE);
            }
            else if (col_spec.type_spec == "DateTimeISO")  {
                std::any_cast<std::vector<DateTime> &>
                    (col_spec.col_vec).emplace_back(
                        value, DT_DATE_STYLE::ISO_STYLE);
            }
            else if (col_spec.type_spec == "bool")  {
                if (value[0] != '\0')  {
                    const bool          v =
                        static_cast<bool>(strtoul(value, nullptr, 0));
                    std::vector<bool>   &vec =
                        std::any_cast<std::vector<bool> &>(col_spec.col_vec);

                    vec.push_back(v);
                }
            }
            col_index += 1;
        }
    }

    const size_type spec_s = spec_vec.size();

    if (spec_s > 0)  {
        if (spec_vec[0].col_name != DF_INDEX_COL_NAME && ! columns_only)
            throw DataFrameError("DataFrame::read_csv2_(): ERROR: "
                                 "Index column is not the first column");
        if (! columns_only)
            load_index(std::move(
                std::any_cast<IndexVecType &>(spec_vec[0].col_vec)));

        const size_type begin =
            spec_vec[0].col_name == DF_INDEX_COL_NAME ? 1 : 0;

        for (size_type i = begin; i < spec_s; ++i)  {
            _col_data_spec_ col_spec = spec_vec[i];

            if (col_spec.type_spec == "float")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<float> &>
                                      (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "double")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<double> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "longdouble")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<long double> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "int")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<int> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "uint")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<unsigned int> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "long")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<long> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "longlong")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<long long> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "ulong")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<unsigned long>&>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "ulonglong")
                load_column(col_spec.col_name.c_str(),
                            std::move(
                                std::any_cast<std::vector<unsigned long long> &>
                                    (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "string")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<std::string> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (! ::strncmp(col_spec.type_spec.c_str(), "DateTime", 8))
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<DateTime> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
            else if (col_spec.type_spec == "bool")
                load_column(col_spec.col_name.c_str(),
                            std::move(std::any_cast<std::vector<bool> &>
                                          (col_spec.col_vec)),
                            nan_policy::dont_pad_with_nans);
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
bool DataFrame<I, H>::
read (const char *file_name, io_format iof, bool columns_only)  {

    std::ifstream   file;

    file.open(file_name, std::ios::in);  // Open for reading
    if (file.fail())  {
        String1K    err;

        err.printf("read(): ERROR: Unable to open file '%s'", file_name);
        throw DataFrameError(err.c_str());
    }

    read<std::istream>(file, iof, columns_only);
    file.close();
    return(true);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename S>
bool DataFrame<I, H>::read (S &in_s, io_format iof, bool columns_only)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call read()");

    if (iof == io_format::csv)
        read_csv_ (in_s, columns_only);
    else if (iof == io_format::csv2)
        read_csv2_ (in_s, columns_only);
    else if (iof == io_format::json)
        read_json_ (in_s, columns_only);
    else
        throw NotImplemented("read(): This io_format is not implemented");

    return(true);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
std::future<bool> DataFrame<I, H>::
read_async(const char *file_name, io_format iof, bool columns_only) {

    return (std::async(std::launch::async,
                       [file_name, iof, columns_only, this] () -> bool  {
                           return (this->read(file_name, iof, columns_only));
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
template<typename S>
std::future<bool> DataFrame<I, H>::
read_async(S &in_s, io_format iof, bool columns_only) {

    return (std::async(std::launch::async,
                       [&in_s, iof, columns_only, this] () -> bool  {
                           return (this->read<S>(in_s, iof, columns_only));
                       }));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
