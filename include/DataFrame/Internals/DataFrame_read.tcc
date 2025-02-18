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
#include <DataFrame/Utils/Endianness.h>
#include <DataFrame/Utils/FixedSizeAllocator.h>
#include <DataFrame/Utils/FixedSizeString.h>
#include <DataFrame/Utils/Utils.h>

#include <any>
#include <cstdlib>
#include <cstring>
#include <sstream>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<typename S>
void DataFrame<I, H>::read_json_(S &stream, bool columns_only)  {

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
            const auto  citer = _typename_id_.find(col_type);

            if (citer == _typename_id_.end()) [[unlikely]]  {
                String1K    err;

                err.printf("read_json_(): ERROR: Cannot find Type '%s'",
                           col_type.c_str());
                throw DataFrameError(err.c_str());
            }

            switch(citer->second)  {
                case file_dtypes::FLOAT: {
                    StlVecType<float>                                   &vec =
                        create_column<float>(col_name.c_str(), false);
                    const ColVectorPushBack_<float, StlVecType<float>>  slug;

                    vec.reserve(col_size);
                    slug(vec, stream, &::strtof, io_format::json);
                    break;
                }
                case file_dtypes::DOUBLE: {
                    StlVecType<double>                  &vec =
                        create_column<double>(col_name.c_str(), false);
                    const ColVectorPushBack_
                        <double, StlVecType<double>>    slug;

                    vec.reserve(col_size);
                    slug(vec, stream, &::strtod, io_format::json);
                    break;
                }
                case file_dtypes::LONG_DOUBLE: {
                    StlVecType<long double>                     &vec =
                        create_column<long double>(col_name.c_str(), false);
                    const ColVectorPushBack_
                        <long double, StlVecType<long double>>  slug;

                    vec.reserve(col_size);
                    slug(vec, stream, &::strtold, io_format::json);
                    break;
                }
                case file_dtypes::SHORT: {
                    StlVecType<short int>   &vec =
                        create_column<short int>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtol,
                                               io_format::json);
                    break;
                }
                case file_dtypes::USHORT: {
                    StlVecType<unsigned short int>   &vec =
                        create_column<unsigned short int>(col_name.c_str(),
                                                          false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtol,
                                               io_format::json);
                    break;
                }
                case file_dtypes::INT: {
                    StlVecType<int> &vec =
                        create_column<int>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtol,
                                               io_format::json);
                break;
                }
                case file_dtypes::UINT: {
                    StlVecType<unsigned int>   &vec =
                        create_column<unsigned int>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtoul,
                                               io_format::json);
                    break;
                }
                case file_dtypes::LONG: {
                    StlVecType<long>    &vec =
                        create_column<long>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtol,
                                               io_format::json);
                    break;
                }
                case file_dtypes::ULONG: {
                    StlVecType<unsigned long>   &vec =
                        create_column<unsigned long>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtoul,
                                               io_format::json);
                    break;
                }
                case file_dtypes::LONG_LONG: {
                    StlVecType<long long>   &vec =
                        create_column<long long>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtoll,
                                               io_format::json);
                    break;
                }
                case file_dtypes::ULONG_LONG: {
                    StlVecType<unsigned long long>  &vec =
                        create_column<unsigned long long>(col_name.c_str(),
                                                          false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtoull,
                                               io_format::json);
                    break;
                }
                case file_dtypes::CHAR: {
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
                    break;
                }
                case file_dtypes::UCHAR: {
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
                                return (static_cast<unsigned char>(
                                            int(tok[0])));
                            else
                                return (static_cast<unsigned char>(atoi(tok)));
                        },
                        io_format::json);
                    break;
                }
                case file_dtypes::BOOL: {
                    StlVecType<bool>    &vec =
                        create_column<bool>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    col_vector_push_back_func_(vec,
                                               stream,
                                               &::strtol,
                                               io_format::json);
                    break;
                }
                case file_dtypes::STRING: {
                    StlVecType<std::string> &vec =
                        create_column<std::string>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    json_str_col_vector_push_back_(vec, stream);
                    break;
                }
                case file_dtypes::VSTR32: {
                    StlVecType<String32>    &vec =
                        create_column<String32>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    json_str_col_vector_push_back_(vec, stream);
                    break;
                }
                case file_dtypes::VSTR64: {
                    StlVecType<String64>    &vec =
                        create_column<String64>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    json_str_col_vector_push_back_(vec, stream);
                    break;
                }
                case file_dtypes::VSTR128: {
                    StlVecType<String128>   &vec =
                        create_column<String128>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    json_str_col_vector_push_back_(vec, stream);
                    break;
                }
                case file_dtypes::VSTR512: {
                    StlVecType<String512>   &vec =
                        create_column<String512>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    json_str_col_vector_push_back_(vec, stream);
                    break;
                }
                case file_dtypes::VSTR1K: {
                    StlVecType<String1K>    &vec =
                        create_column<String1K>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    json_str_col_vector_push_back_(vec, stream);
                    break;
                }
                case file_dtypes::VSTR2K: {
                    StlVecType<String2K>    &vec =
                        create_column<String2K>(col_name.c_str(), false);

                    vec.reserve(col_size);
                    json_str_col_vector_push_back_(vec, stream);
                    break;
                }
                case file_dtypes::DATETIME: {
                    StlVecType<DateTime>    &vec =
                        create_column<DateTime>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *, char **) -> DateTime {
                            return DateTime();
                        };
                    const ColVectorPushBack_<DateTime,
                                             StlVecType<DateTime>>  slug;

                    vec.reserve(col_size);
                    slug(vec, stream, converter, io_format::json);
                    break;
                }
                default:  {
                    String1K    err;

                    err.printf("read_json_(): ERROR: Type '%s' is not "
                               "supported", col_type.c_str());
                    throw DataFrameError(err.c_str());
                }
            }
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
template<typename S>
void DataFrame<I, H>::read_csv_(S &stream, bool columns_only)  {

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

            vec.reserve(_atoi_<size_type>(value.c_str(), value.size()));
            IdxParserFunctor_<IndexType>()(vec, stream);
            if (! columns_only)
                load_index(std::forward<IndexVecType &&>(vec));
        }
        else [[likely]]  {
            const auto  citer = _typename_id_.find(type_str);

            if (citer == _typename_id_.end())
                throw DataFrameError("DataFrame::read_csv_(): ERROR: Unknown "
                                     "column type");

            switch(citer->second)  {
                case file_dtypes::FLOAT: {
                    StlVecType<float>                                   &vec =
                        create_column<float>(col_name.c_str(), false);
                    const ColVectorPushBack_<float, StlVecType<float>>  slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, &::strtof);
                    break;
                }
                case file_dtypes::DOUBLE: {
                    StlVecType<double>  &vec =
                        create_column<double>(col_name.c_str(), false);
                    const ColVectorPushBack_<double, StlVecType<double>>  slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, &::strtod);
                    break;
                }
                case file_dtypes::LONG_DOUBLE: {
                    StlVecType<long double>                     &vec =
                        create_column<long double>(col_name.c_str(), false);
                    const ColVectorPushBack_
                        <long double, StlVecType<long double>>  slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, &::strtold);
                    break;
                }
                case file_dtypes::SHORT: {
                    StlVecType<short>   &vec =
                        create_column<short>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(&_atoi_<short>, stream, vec);
                    break;
                }
                case file_dtypes::USHORT: {
                    StlVecType<unsigned short>  &vec =
                        create_column<unsigned short>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(&_atoi_<unsigned short>,
                                               stream,
                                               vec);
                    break;
                }
                case file_dtypes::INT: {
                    StlVecType<int> &vec =
                        create_column<int>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(&_atoi_<int>, stream, vec);
                    break;
                }
                case file_dtypes::UINT: {
                    StlVecType<unsigned int>    &vec =
                        create_column<unsigned int>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(&_atoi_<unsigned int>,
                                               stream,
                                               vec);
                    break;
                }
                case file_dtypes::LONG: {
                    StlVecType<long>    &vec =
                        create_column<long>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(&_atoi_<long>, stream, vec);
                    break;
                }
                case file_dtypes::ULONG: {
                    StlVecType<unsigned long>   &vec =
                        create_column<unsigned long>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(&_atoi_<unsigned long>,
                                               stream,
                                               vec);
                    break;
                }
                case file_dtypes::LONG_LONG: {
                    StlVecType<long long>   &vec =
                        create_column<long long>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(&_atoi_<long long>,
                                               stream,
                                               vec);
                    break;
                }
                case file_dtypes::ULONG_LONG: {
                    StlVecType<unsigned long long>  &vec =
                        create_column<unsigned long long>(col_name.c_str(),
                                                          false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(&_atoi_<unsigned long long>,
                                               stream,
                                               vec);
                    break;
                }
                case file_dtypes::CHAR: {
                    StlVecType<char>    &vec =
                        create_column<char>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
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
                    break;
                }
                case file_dtypes::UCHAR: {
                    StlVecType<unsigned char>   &vec =
                        create_column<unsigned char>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_<unsigned char,
                                               StlVecType<unsigned char>>(
                        vec,
                        stream,
                        [](const char *tok, char **, int) -> unsigned char  {
                            if (tok[0] == '\0')
                                return ('\0');
                            else if (tok[1] == '\0')
                                return (static_cast<unsigned char>(
                                            int(tok[0])));
                            else
                                return (static_cast<unsigned char>(atoi(tok)));
                        });
                    break;
                }
                case file_dtypes::BOOL: {
                    StlVecType<bool>    &vec =
                        create_column<bool>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_func_(vec, stream, &::strtol);
                    break;
                }
                case file_dtypes::STRING: {
                    StlVecType<std::string> &vec =
                        create_column<std::string>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *s, char **)-> const char * {
                            return s;
                        };
                    const ColVectorPushBack_
                        <const char *, StlVecType<std::string>> slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, converter);
                    break;
                }
                case file_dtypes::VSTR32: {
                    StlVecType<String32>    &vec =
                        create_column<String32>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *s, char **)-> const char * {
                            return s;
                        };
                    const ColVectorPushBack_
                        <const char *, StlVecType<String32>>    slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, converter);
                    break;
                }
                case file_dtypes::VSTR64: {
                    StlVecType<String64>    &vec =
                        create_column<String64>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *s, char **)-> const char * {
                            return s;
                        };
                    const ColVectorPushBack_
                        <const char *, StlVecType<String64>>    slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, converter);
                    break;
                }
                case file_dtypes::VSTR128: {
                    StlVecType<String128>   &vec =
                        create_column<String128>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *s, char **)-> const char * {
                            return s;
                        };
                    const ColVectorPushBack_
                        <const char *, StlVecType<String128>>   slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, converter);
                    break;
                }
                case file_dtypes::VSTR512: {
                    StlVecType<String512>   &vec =
                        create_column<String512>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *s, char **)-> const char * {
                            return s;
                        };
                    const ColVectorPushBack_
                        <const char *, StlVecType<String512>>   slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, converter);
                    break;
                }
                case file_dtypes::VSTR1K: {
                    StlVecType<String1K>    &vec =
                        create_column<String1K>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *s, char **)-> const char * {
                            return s;
                        };
                    const ColVectorPushBack_
                        <const char *, StlVecType<String1K>> slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, converter);
                    break;
                }
                case file_dtypes::VSTR2K: {
                    StlVecType<String2K>    &vec =
                        create_column<String2K>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *s, char **)-> const char * {
                            return s;
                        };
                    const ColVectorPushBack_
                        <const char *, StlVecType<String2K>> slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug(vec, stream, converter);
                    break;
                }
                case file_dtypes::DATETIME: {
                    StlVecType<DateTime>    &vec =
                        create_column<DateTime>(col_name.c_str(), false);
                    auto                    converter =
                        [](const char *, char **) -> DateTime {
                            return DateTime();
                        };
                    const ColVectorPushBack_<DateTime,
                                             StlVecType<DateTime>>  slug;

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    slug (vec, stream, converter);
                    break;
                }
                case file_dtypes::STR_DBL_PAIR: {
                    using val_t = std::pair<std::string, double>;

                    StlVecType<val_t>   &vec =
                        create_column<val_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(
                        vec, stream, &_get_str_dbl_pair_from_value_);
                    break;
                }
                case file_dtypes::STR_STR_PAIR: {
                    using val_t = std::pair<std::string, std::string>;

                    StlVecType<val_t>   &vec =
                        create_column<val_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(
                        vec, stream, &_get_str_str_pair_from_value_);
                    break;
                }
                case file_dtypes::DBL_DBL_PAIR: {
                    using val_t = std::pair<double, double>;

                    StlVecType<val_t>   &vec =
                        create_column<val_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(
                        vec, stream, &_get_dbl_dbl_pair_from_value_);
                    break;
                }
                case file_dtypes::DBL_VEC: {
                    using vec_t = std::vector<double>;

                    StlVecType<vec_t>   &vec =
                        create_column<vec_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(vec,
                                                    stream,
                                                    &_get_dbl_vec_from_value_);
                    break;
                }
                case file_dtypes::STR_VEC: {
                    using vec_t = std::vector<std::string>;

                    StlVecType<vec_t>   &vec =
                        create_column<vec_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(vec,
                                                    stream,
                                                    &_get_str_vec_from_value_);
                    break;
                }
                case file_dtypes::DBL_SET: {
                    using set_t = std::set<double>;

                    StlVecType<set_t>   &vec =
                        create_column<set_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(vec,
                                                    stream,
                                                    &_get_dbl_set_from_value_);
                    break;
                }
                case file_dtypes::STR_SET: {
                    using set_t = std::set<std::string>;

                    StlVecType<set_t>   &vec =
                        create_column<set_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(vec,
                                                    stream,
                                                    &_get_str_set_from_value_);
                    break;
                }
                case file_dtypes::STR_DBL_MAP: {
                    using map_t = std::map<std::string, double>;

                    StlVecType<map_t>   &vec =
                        create_column<map_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(
                        vec,
                        stream,
                        &_get_str_dbl_map_from_value_<map_t>);
                    break;
                }
                case file_dtypes::STR_DBL_UNOMAP: {
                    using map_t = std::unordered_map<std::string, double>;

                    StlVecType<map_t>   &vec =
                        create_column<map_t>(col_name.c_str(), false);

                    vec.reserve(_atoi_<size_type>(value.c_str(),
                                                  value.size()));
                    col_vector_push_back_cont_func_(
                        vec,
                        stream,
                        &_get_str_dbl_map_from_value_<map_t>);
                    break;
                }
                default: {
                    throw DataFrameError("DataFrame::read_csv_(): ERROR: "
                                         "Type is not supported");
                }
            }
        }
    }
}

// ----------------------------------------------------------------------------

struct _col_data_spec_  {

    std::any    col_vec { };
    file_dtypes type_spec { 0 };
    String64    col_name { };

    template<typename V>
    _col_data_spec_(V cv, file_dtypes ts, const char *cn, std::size_t rs)
        : col_vec(cv), type_spec(ts), col_name(cn)  {

        std::any_cast<V &>(col_vec).reserve(rs);
    }
};

// --------------------------------------

template<typename I, typename H>
template<typename S>
void DataFrame<I, H>::
read_csv2_(S &stream,
           bool columns_only,
           size_type starting_row,
           size_type num_rows,
           bool skip_first_line,
           const std::vector<ReadSchema> &schema)  {

    constexpr unsigned long data_size = 64 * 1024;

    using SpecVec = StlVecType<_col_data_spec_>;

    char                line[data_size];
    std::string         value;
    SpecVec             spec_vec;
    bool                header_read { false };
    size_type           col_count { 0 };
    size_type           data_rows_read { 0 };
    size_type           row_cnt { 0 };
    std::stringstream   sstream;

    value.reserve(64);
    spec_vec.reserve(32);
    while (true)  {
        if constexpr (std::same_as<S, std::FILE *>)  {
            if (std::feof(stream))  break;
        }
        else  {
            if (stream.eof())  break;
        }

        line[0] = '\0';
        if constexpr (std::same_as<S, std::FILE *>)  {
            if (std::fgets(line, sizeof(line) - 1, stream) == nullptr)
                continue;
        }
        else  {
            stream.getline(line, sizeof(line) - 1);
            if (stream.fail())
                continue;
        }

        if (line[0] == '\0' || line[0] == '#' ||
            line[0] == '\n' || line[0] == '\r') [[unlikely]]  continue;

        sstream.clear();
        sstream.str(line);

        // Is the caller specifying the schema
        //
        if ((! header_read) && (! schema.empty())) [[unlikely]]  {
            col_count = schema.size();
            for (const auto &entry : schema)  {
                switch(entry.col_type)  {
                    case file_dtypes::FLOAT: {
                        spec_vec.emplace_back(StlVecType<float>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::DOUBLE: {
                        spec_vec.emplace_back(StlVecType<double>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::LONG_DOUBLE: {
                        spec_vec.emplace_back(StlVecType<long double>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::SHORT: {
                        spec_vec.emplace_back(StlVecType<short>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::USHORT: {
                        spec_vec.emplace_back(StlVecType<unsigned short>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::INT: {
                        spec_vec.emplace_back(StlVecType<int>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::UINT: {
                        spec_vec.emplace_back(StlVecType<unsigned int>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::LONG: {
                        spec_vec.emplace_back(StlVecType<long>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::ULONG: {
                        spec_vec.emplace_back(StlVecType<unsigned long>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::LONG_LONG: {
                        spec_vec.emplace_back(StlVecType<long long>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::ULONG_LONG: {
                        spec_vec.emplace_back(
                            StlVecType<unsigned long long>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                    case file_dtypes::CHAR: {
                        spec_vec.emplace_back(StlVecType<char>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::UCHAR: {
                        spec_vec.emplace_back(StlVecType<unsigned char>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::BOOL: {
                        spec_vec.emplace_back(StlVecType<bool>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::STRING: {
                        spec_vec.emplace_back(StlVecType<std::string>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::VSTR32: {
                        spec_vec.emplace_back(StlVecType<String32>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::VSTR64: {
                        spec_vec.emplace_back(StlVecType<String64>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::VSTR128: {
                        spec_vec.emplace_back(StlVecType<String128>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::VSTR512: {
                        spec_vec.emplace_back(StlVecType<String512>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::VSTR1K: {
                        spec_vec.emplace_back(StlVecType<String1K>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::VSTR2K: {
                        spec_vec.emplace_back(StlVecType<String2K>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::DATETIME:
                    case file_dtypes::DATETIME_AME:
                    case file_dtypes::DATETIME_EUR:
                    case file_dtypes::DATETIME_ISO: {
                        spec_vec.emplace_back(StlVecType<DateTime>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::STR_DBL_PAIR: {
                        spec_vec.emplace_back(
                            StlVecType<std::pair<std::string, double>>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                    case file_dtypes::STR_STR_PAIR: {
                        spec_vec.emplace_back(
                            StlVecType<std::pair<std::string, std::string>>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                    case file_dtypes::DBL_DBL_PAIR: {
                        spec_vec.emplace_back(
                            StlVecType<std::pair<double, double>>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                    case file_dtypes::DBL_VEC: {
                        spec_vec.emplace_back(
                            StlVecType<std::vector<double>>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                    case file_dtypes::STR_VEC: {
                        spec_vec.emplace_back(
                            StlVecType<std::vector<std::string>>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                    case file_dtypes::DBL_SET: {
                        spec_vec.emplace_back(StlVecType<std::set<double>>{ },
                                              entry.col_type,
                                              entry.col_name.c_str(),
                                              entry.num_rows);
                        break;
                    }
                    case file_dtypes::STR_SET: {
                        spec_vec.emplace_back(
                            StlVecType<std::set<std::string>>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                    case file_dtypes::STR_DBL_MAP: {
                        spec_vec.emplace_back(
                            StlVecType<std::map<std::string, double>>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                    case file_dtypes::STR_DBL_UNOMAP: {
                        spec_vec.emplace_back(
                            StlVecType<std::unordered_map<std::string,
                                                          double>>{ },
                            entry.col_type,
                            entry.col_name.c_str(),
                            entry.num_rows);
                        break;
                    }
                }
            }

            header_read = true;
            if (skip_first_line)  continue;
        }

        // The schema/header comes from the file
        //
        if (! header_read) [[unlikely]]  {
            std::string type_str;
            std::string col_name;
            std::string token;

            type_str.reserve(14);
            col_name.reserve(32);
            token.reserve(32);
            while (std::getline(sstream, token, ','))  {
                const size_type token_s = token.size();
                size_type       token_idx { 0 };

                value.clear();
                col_name.clear();
                type_str.clear();
                _get_token_from_string_(token, token_idx, ':', col_name);
                _get_token_from_string_(token, token_idx, ':', value); // size
                if (token_idx >= token_s ||
                    token[token_idx] != '<') [[unlikely]]
                    throw DataFrameError(
                        "DataFrame::read_csv2_(): ERROR: Expected "
                        "'<' char to specify column type");
                token_idx += 1;  // Get rid of <
                _get_token_from_string_(token, token_idx, '>', type_str);

                const size_type nrows =
                    num_rows == std::numeric_limits<size_type>::max()
                        ? _atoi_<size_type>(value.c_str(), int(value.size()))
                        : num_rows;
                const auto      citer = _typename_id_.find(type_str);

                if (citer == _typename_id_.end())
                    throw DataFrameError("DataFrame::read_csv2_(): ERROR: "
                                         "Unknown column type");

                switch(citer->second)  {
                    case file_dtypes::FLOAT: {
                        spec_vec.emplace_back(StlVecType<float>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::DOUBLE: {
                        spec_vec.emplace_back(StlVecType<double>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::LONG_DOUBLE: {
                        spec_vec.emplace_back(StlVecType<long double>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::SHORT: {
                        spec_vec.emplace_back(StlVecType<short>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::USHORT: {
                        spec_vec.emplace_back(StlVecType<unsigned short>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::INT: {
                        spec_vec.emplace_back(StlVecType<int>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::UINT: {
                        spec_vec.emplace_back(StlVecType<unsigned int>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::LONG: {
                        spec_vec.emplace_back(StlVecType<long>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::ULONG: {
                        spec_vec.emplace_back(StlVecType<unsigned long>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::LONG_LONG: {
                        spec_vec.emplace_back(StlVecType<long long>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::ULONG_LONG: {
                        spec_vec.emplace_back(
                            StlVecType<unsigned long long>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                    case file_dtypes::CHAR: {
                        spec_vec.emplace_back(StlVecType<char>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::UCHAR: {
                        spec_vec.emplace_back(StlVecType<unsigned char>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::BOOL: {
                        spec_vec.emplace_back(StlVecType<bool>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::STRING: {
                        spec_vec.emplace_back(StlVecType<std::string>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::VSTR32: {
                        spec_vec.emplace_back(StlVecType<String32>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::VSTR64: {
                        spec_vec.emplace_back(StlVecType<String64>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::VSTR128: {
                        spec_vec.emplace_back(StlVecType<String128>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::VSTR512: {
                        spec_vec.emplace_back(StlVecType<String512>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::VSTR1K: {
                        spec_vec.emplace_back(StlVecType<String1K>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::VSTR2K: {
                        spec_vec.emplace_back(StlVecType<String2K>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::DATETIME:
                    case file_dtypes::DATETIME_AME:
                    case file_dtypes::DATETIME_EUR:
                    case file_dtypes::DATETIME_ISO: {
                        spec_vec.emplace_back(StlVecType<DateTime>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::STR_DBL_PAIR: {
                        spec_vec.emplace_back(
                            StlVecType<std::pair<std::string, double>>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                    case file_dtypes::STR_STR_PAIR: {
                        spec_vec.emplace_back(
                            StlVecType<std::pair<std::string, std::string>>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                    case file_dtypes::DBL_DBL_PAIR: {
                        spec_vec.emplace_back(
                            StlVecType<std::pair<double, double>>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                    case file_dtypes::DBL_VEC: {
                        spec_vec.emplace_back(
                            StlVecType<std::vector<double>>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                    case file_dtypes::STR_VEC: {
                        spec_vec.emplace_back(
                            StlVecType<std::vector<std::string>>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                    case file_dtypes::DBL_SET: {
                        spec_vec.emplace_back(StlVecType<std::set<double>>{ },
                                              citer->second,
                                              col_name.c_str(),
                                              nrows);
                        break;
                    }
                    case file_dtypes::STR_SET: {
                        spec_vec.emplace_back(
                            StlVecType<std::set<std::string>>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                    case file_dtypes::STR_DBL_MAP: {
                        spec_vec.emplace_back(
                            StlVecType<std::map<std::string, double>>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                    case file_dtypes::STR_DBL_UNOMAP: {
                        spec_vec.emplace_back(
                            StlVecType<std::unordered_map<std::string,
                                                          double>>{ },
                            citer->second,
                            col_name.c_str(),
                            nrows);
                        break;
                    }
                }

                col_count += 1;
            }
            header_read = true;
            continue;
        }

        // Now read data rows
        //
        if (row_cnt++ >= starting_row) [[likely]]  {
            if (data_rows_read++ >= num_rows) [[unlikely]]  break;

            // Make sure we account for empty slots at the end of the line
            // and create NaN data points
            //
            for (size_type col_idx = 0; col_idx < col_count; ++col_idx)  {
                value.clear();
                if (! sstream.eof()) [[likely]]
                    std::getline(sstream, value, ',');

                _col_data_spec_ &col_spec = spec_vec[col_idx];
                const auto      val_size = value.size();

                switch(col_spec.type_spec)  {
                    case file_dtypes::FLOAT: {
                        if (val_size > 0) [[likely]]
                            std::any_cast<StlVecType<float> &>
                                (col_spec.col_vec).push_back(
                                     std::strtof(value.c_str(), nullptr));
                        else
                            std::any_cast<StlVecType<float> &>
                                (col_spec.col_vec).push_back(get_nan<float>());
                        break;
                    }
                    case file_dtypes::DOUBLE: {
                        if (val_size > 0) [[likely]]
                            std::any_cast<StlVecType<double> &>
                                (col_spec.col_vec).push_back(
                                     std::strtod(value.c_str(), nullptr));
                        else
                            std::any_cast<StlVecType<double> &>
                                (col_spec.col_vec).push_back(
                                     get_nan<double>());
                        break;
                    }
                    case file_dtypes::LONG_DOUBLE: {
                        if (val_size > 0) [[likely]]
                            std::any_cast<StlVecType<long double> &>
                                (col_spec.col_vec).push_back(
                                     std::strtold(value.c_str(), nullptr));
                        else
                            std::any_cast<StlVecType<long double> &>
                                (col_spec.col_vec).push_back(
                                     get_nan<long double>());
                        break;
                    }
                    case file_dtypes::SHORT: {
                        std::any_cast<StlVecType<short> &>
                            (col_spec.col_vec).push_back(
                                _atoi_<short>(value.c_str(), int(val_size)));
                        break;
                    }
                    case file_dtypes::USHORT: {
                        std::any_cast<StlVecType<unsigned short> &>
                            (col_spec.col_vec).push_back(
                                _atoi_<unsigned short>(value.c_str(),
                                                       int(val_size)));
                        break;
                    }
                    case file_dtypes::INT: {
                        std::any_cast<StlVecType<int> &>
                            (col_spec.col_vec).push_back(
                                _atoi_<int>(value.c_str(), int(val_size)));
                        break;
                    }
                    case file_dtypes::UINT: {
                        std::any_cast<StlVecType<unsigned int> &>
                            (col_spec.col_vec).push_back(
                                _atoi_<unsigned int>(value.c_str(),
                                                     int(val_size)));
                        break;
                    }
                    case file_dtypes::LONG: {
                        std::any_cast<StlVecType<long> &>
                            (col_spec.col_vec).push_back(
                                _atoi_<long>(value.c_str(), int(val_size)));
                        break;
                    }
                    case file_dtypes::ULONG: {
                        std::any_cast<StlVecType<unsigned long> &>
                            (col_spec.col_vec).push_back(
                                _atoi_<unsigned long>(value.c_str(),
                                                      int(val_size)));
                        break;
                    }
                    case file_dtypes::LONG_LONG: {
                        std::any_cast<StlVecType<long long> &>
                            (col_spec.col_vec).push_back(
                                _atoi_<long long>(value.c_str(),
                                                  int(val_size)));
                        break;
                    }
                    case file_dtypes::ULONG_LONG: {
                        std::any_cast<StlVecType<unsigned long long> &>
                            (col_spec.col_vec).push_back(
                                _atoi_<unsigned long long>(value.c_str(),
                                                           int(val_size)));
                        break;
                    }
                    case file_dtypes::CHAR: {
                        if (val_size > 1)  {
                            std::any_cast<StlVecType<char> &>
                                (col_spec.col_vec).push_back(
                                    static_cast<char>(
                                        _atoi_<int>(value.c_str(),
                                                    value.size())));
                        }
                        else if (val_size > 0)  {
                            std::any_cast<StlVecType<char> &>
                                (col_spec.col_vec).push_back(value[0]);
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<char> &>
                                (col_spec.col_vec).push_back(get_nan<char>());
                        }
                        break;
                    }
                    case file_dtypes::UCHAR: {
                        if (val_size > 1)  {
                            std::any_cast<StlVecType<unsigned char> &>
                                (col_spec.col_vec).push_back(
                                    static_cast<unsigned char>(
                                        _atoi_<int>(value.c_str(),
                                                    value.size())));
                        }
                        else if (val_size > 0)  {
                            std::any_cast<StlVecType<unsigned char> &>
                                    (col_spec.col_vec).push_back(
                                    static_cast<unsigned char>(value[0]));
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<unsigned char> &>
                                (col_spec.col_vec).push_back(
                                     get_nan<unsigned char>());
                        }
                        break;
                    }
                    case file_dtypes::BOOL: {
                        if (val_size > 0) [[likely]]  {
                            const bool          v =
                                static_cast<bool>
                                    (strtoul(value.c_str(), nullptr, 0));
                            StlVecType<bool>   &vec =
                                std::any_cast<StlVecType<bool> &>
                                    (col_spec.col_vec);

                            vec.push_back(v);
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<bool> &>
                                (col_spec.col_vec).push_back(get_nan<bool>());
                        }
                        break;
                    }
                    case file_dtypes::STRING: {
                        if (val_size > 0 &&
                            (value.back() == '\n' || value.back() == '\r')) {
                            value.pop_back();
                        }
                        std::any_cast<StlVecType<std::string> &>
                            (col_spec.col_vec).emplace_back(value);
                        break;
                    }
                    case file_dtypes::VSTR32: {
                        std::any_cast<StlVecType<String32> &>
                            (col_spec.col_vec).emplace_back(value.c_str());
                        break;
                    }
                    case file_dtypes::VSTR64: {
                        std::any_cast<StlVecType<String64> &>
                            (col_spec.col_vec).emplace_back(value.c_str());
                        break;
                    }
                    case file_dtypes::VSTR128: {
                        std::any_cast<StlVecType<String128> &>
                            (col_spec.col_vec).emplace_back(value.c_str());
                        break;
                    }
                    case file_dtypes::VSTR512: {
                        std::any_cast<StlVecType<String512> &>
                            (col_spec.col_vec).emplace_back(value.c_str());
                        break;
                    }
                    case file_dtypes::VSTR1K: {
                        std::any_cast<StlVecType<String1K> &>
                            (col_spec.col_vec).emplace_back(value.c_str());
                        break;
                    }
                    case file_dtypes::VSTR2K: {
                        std::any_cast<StlVecType<String2K> &>
                            (col_spec.col_vec).emplace_back(value.c_str());
                        break;
                    }
                    case file_dtypes::DATETIME: {
                        if (val_size > 0) [[likely]]  {
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
                                (col_spec.col_vec).push_back(
                                     get_nan<DateTime>());
                        }
                        break;
                    }
                    case file_dtypes::DATETIME_AME: {
                        if (val_size > 0) [[likely]]  {
                            std::any_cast<StlVecType<DateTime> &>
                                (col_spec.col_vec).emplace_back(
                                    value.c_str(), DT_DATE_STYLE::AME_STYLE);
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<DateTime> &>
                                (col_spec.col_vec).push_back(
                                     get_nan<DateTime>());
                        }
                        break;
                    }
                    case file_dtypes::DATETIME_EUR: {
                        if (val_size > 0) [[likely]]  {
                            std::any_cast<StlVecType<DateTime> &>
                                (col_spec.col_vec).emplace_back(
                                    value.c_str(), DT_DATE_STYLE::EUR_STYLE);
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<DateTime> &>
                                (col_spec.col_vec).push_back(
                                     get_nan<DateTime>());
                        }
                        break;
                    }
                    case file_dtypes::DATETIME_ISO: {
                        if (val_size > 0) [[likely]]  {
                            std::any_cast<StlVecType<DateTime> &>
                                (col_spec.col_vec).emplace_back(
                                    value.c_str(), DT_DATE_STYLE::ISO_STYLE);
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<DateTime> &>
                                (col_spec.col_vec).push_back(
                                     get_nan<DateTime>());
                        }
                        break;
                    }
                    case file_dtypes::STR_DBL_PAIR: {
                        using val_t = std::pair<std::string, double>;

                        StlVecType<val_t>   &vec =
                            std::any_cast<StlVecType<val_t> &>(
                                col_spec.col_vec);

                        vec.push_back(std::move(_get_str_dbl_pair_from_value_(
                                                    value.c_str())));
                        break;
                    }
                    case file_dtypes::STR_STR_PAIR: {
                        using val_t = std::pair<std::string, std::string>;

                        StlVecType<val_t>   &vec =
                            std::any_cast<StlVecType<val_t> &>(
                                col_spec.col_vec);

                        vec.push_back(std::move(_get_str_str_pair_from_value_(
                                                    value.c_str())));
                        break;
                    }
                    case file_dtypes::DBL_DBL_PAIR: {
                        using val_t = std::pair<double, double>;

                        StlVecType<val_t>   &vec =
                            std::any_cast<StlVecType<val_t> &>(
                                col_spec.col_vec);

                        vec.push_back(std::move(_get_dbl_dbl_pair_from_value_(
                                                    value.c_str())));
                        break;
                    }
                    case file_dtypes::DBL_VEC: {
                        if (val_size > 0) [[likely]]  {
                            StlVecType<std::vector<double>>  &vec =
                                std::any_cast<StlVecType<
                                    std::vector<double>> &>(col_spec.col_vec);

                            vec.push_back(
                                std::move(_get_dbl_vec_from_value_(
                                              value.c_str())));
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<std::vector<double>> &>
                                (col_spec.col_vec).push_back
                                    (std::vector<double> { });
                        }
                        break;
                    }
                    case file_dtypes::STR_VEC: {
                        if (val_size > 0) [[likely]]  {
                            StlVecType<std::vector<std::string>> &vec =
                                 std::any_cast<StlVecType<
                                     std::vector<std::string>> &>
                                         (col_spec.col_vec);

                            vec.push_back(
                                std::move(_get_str_vec_from_value_(
                                              value.c_str())));
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<
                                std::vector<std::string>> &>
                                    (col_spec.col_vec).push_back(
                                        std::vector<std::string> { });
                        }
                        break;
                    }
                    case file_dtypes::DBL_SET: {
                        using set_t = std::set<double>;

                        if (val_size > 0) [[likely]]  {
                            StlVecType<set_t>   &vec =
                                std::any_cast<StlVecType<set_t> &>
                                    (col_spec.col_vec);

                            vec.push_back(std::move(_get_dbl_set_from_value_(
                                              value.c_str())));
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<set_t> &>
                                (col_spec.col_vec).push_back(set_t { });
                        }
                        break;
                    }
                    case file_dtypes::STR_SET: {
                        using set_t = std::set<std::string>;

                        if (val_size > 0) [[likely]]  {
                            StlVecType<set_t>   &vec =
                                std::any_cast<StlVecType<set_t> &>
                                    (col_spec.col_vec);

                            vec.push_back(std::move(_get_str_set_from_value_(
                                              value.c_str())));
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<set_t> &>
                                (col_spec.col_vec).push_back(set_t { });
                        }
                        break;
                    }
                    case file_dtypes::STR_DBL_MAP: {
                        using map_t = std::map<std::string, double>;

                        if (val_size > 0) [[likely]]  {
                            StlVecType<map_t>   &vec =
                                std::any_cast<StlVecType<map_t> &>
                                    (col_spec.col_vec);

                            vec.push_back(
                                std::move(_get_str_dbl_map_from_value_<map_t>(
                                value.c_str())));
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<map_t> &>
                                (col_spec.col_vec).push_back(map_t { });
                        }
                        break;
                    }
                    case file_dtypes::STR_DBL_UNOMAP: {
                        using map_t = std::unordered_map<std::string, double>;

                        if (val_size > 0) [[likely]]  {
                            StlVecType<map_t>   &vec =
                                std::any_cast<StlVecType<map_t> &>
                                    (col_spec.col_vec);

                            vec.push_back(
                                std::move(_get_str_dbl_map_from_value_<map_t>(
                                value.c_str())));
                        }
                        else [[unlikely]]  {
                            std::any_cast<StlVecType<map_t> &>
                                (col_spec.col_vec).push_back(map_t { });
                        }
                        break;
                    }
                }
            }
        }
    }

    const size_type spec_s = spec_vec.size();

    // Now load the data into the DataFrame
    //
    if (spec_s > 0)  {
        if (spec_vec[0].col_name != DF_INDEX_COL_NAME && ! columns_only)
        [[unlikely]]
            throw DataFrameError("DataFrame::read_csv2_(): ERROR: "
                                 "Index column is not the first column");

        if (! columns_only) [[likely]]
            load_index(std::move(
                std::any_cast<IndexVecType &>(spec_vec[0].col_vec)));

        const size_type begin =
            spec_vec[0].col_name == DF_INDEX_COL_NAME ? 1 : 0;

        for (size_type i = begin; i < spec_s; ++i) [[likely]]  {
            _col_data_spec_ col_spec = spec_vec[i];

            switch(col_spec.type_spec)  {
                case file_dtypes::FLOAT: {
                    load_column<float>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<float> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::DOUBLE: {
                    load_column<double>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<double> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::LONG_DOUBLE: {
                    load_column<long double>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<long double> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::SHORT: {
                    load_column<short>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<short> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::USHORT: {
                    load_column<unsigned short>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<unsigned short> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::INT: {
                    load_column<int>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<int> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::UINT: {
                    load_column<unsigned int>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<unsigned int> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::LONG: {
                    load_column<long>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<long> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::ULONG: {
                    load_column<unsigned long>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<unsigned long>&>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::LONG_LONG: {
                    load_column<long long>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<long long> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::ULONG_LONG: {
                    load_column<unsigned long long>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<
                                      StlVecType<unsigned long long> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::CHAR: {
                    load_column<char>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<char> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::UCHAR: {
                    load_column<unsigned char>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<unsigned char> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::BOOL: {
                    load_column<bool>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<bool> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::STRING: {
                    load_column<std::string>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<std::string> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::VSTR32: {
                    load_column<String32>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<String32> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::VSTR64: {
                    load_column<String64>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<String64> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::VSTR128: {
                    load_column<String128>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<String128> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::VSTR512: {
                    load_column<String512>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<String512> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::VSTR1K: {
                    load_column<String1K>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<String1K> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::VSTR2K: {
                    load_column<String2K>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<String2K> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::DATETIME:
                case file_dtypes::DATETIME_AME:
                case file_dtypes::DATETIME_EUR:
                case file_dtypes::DATETIME_ISO: {
                    load_column<DateTime>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<DateTime> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::STR_DBL_PAIR: {
                    using val_t = std::pair<std::string, double>;

                    load_column<val_t>(col_spec.col_name.c_str(),
                                       std::move(std::any_cast<
                                           StlVecType<val_t> &>
                                               (col_spec.col_vec)),
                                       nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::STR_STR_PAIR: {
                    using val_t = std::pair<std::string, std::string>;

                    load_column<val_t>(col_spec.col_name.c_str(),
                                       std::move(std::any_cast<
                                           StlVecType<val_t> &>
                                               (col_spec.col_vec)),
                                       nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::DBL_DBL_PAIR: {
                    using val_t = std::pair<double, double>;

                    load_column<val_t>(col_spec.col_name.c_str(),
                                       std::move(std::any_cast<
                                           StlVecType<val_t> &>
                                               (col_spec.col_vec)),
                                       nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::DBL_VEC: {
                    load_column<std::vector<double>>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<
                            std::vector<double>> &>(col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::STR_VEC: {
                    load_column<std::vector<std::string>>(
                        col_spec.col_name.c_str(),
                        std::move(
                            std::any_cast<StlVecType<
                                std::vector<std::string>> &>
                                    (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::DBL_SET: {
                    using set_t = std::set<double>;

                    load_column<set_t>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<set_t> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::STR_SET: {
                    using set_t = std::set<std::string>;

                    load_column<set_t>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<set_t> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::STR_DBL_MAP: {
                    using map_t = std::map<std::string, double>;

                    load_column<map_t>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<map_t> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
                case file_dtypes::STR_DBL_UNOMAP: {
                    using map_t = std::unordered_map<std::string, double>;

                    load_column<map_t>(
                        col_spec.col_name.c_str(),
                        std::move(std::any_cast<StlVecType<map_t> &>
                            (col_spec.col_vec)),
                        nan_policy::dont_pad_with_nans);
                    break;
                }
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S>
void DataFrame<I, H>::
read_binary_(S &stream,
             bool columns_only,
             size_type starting_row,
             size_type num_rows)  {

    endians ed;

    stream.read(reinterpret_cast<char *>(&ed), sizeof(ed));

    const bool needs_flipping = ed != get_system_endian();
    uint16_t   col_num { 0 };

    stream.read(reinterpret_cast<char *>(&col_num), sizeof(col_num));
    if (needs_flipping)
        col_num = SwapBytes<decltype(col_num), sizeof(col_num)> { }(col_num);

    char    col_name[64];
    char    col_type[32];

    std::memset(col_name, 0, sizeof(col_name));
    std::memset(col_type, 0, sizeof(col_type));
    if (! columns_only) [[likely]]  {
        stream.read(col_name, sizeof(col_name));
        if (std::strcmp(col_name, DF_INDEX_COL_NAME))  {
            String1K    err;

            err.printf("read_binary_(): ERROR: Expecting name '%s'",
                       DF_INDEX_COL_NAME);
            throw DataFrameError(err.c_str());
        }

        stream.read(col_type, sizeof(col_type));

        IndexVecType    idx_vec;

        if constexpr (std::is_same_v<IndexType, std::string>)
            _read_binary_string_<std::string>(stream, idx_vec, needs_flipping,
                                              starting_row, num_rows);
        else if constexpr (std::is_same_v<IndexType, String32>)
            _read_binary_string_<String32>(stream, idx_vec, needs_flipping,
                                           starting_row, num_rows);
        else if constexpr (std::is_same_v<IndexType, String64>)
            _read_binary_string_<String64>(stream, idx_vec, needs_flipping,
                                           starting_row, num_rows);
        else if constexpr (std::is_same_v<IndexType, String128>)
            _read_binary_string_<String128>(stream, idx_vec, needs_flipping,
                                            starting_row, num_rows);
        else if constexpr (std::is_same_v<IndexType, String512>)
            _read_binary_string_<String512>(stream, idx_vec, needs_flipping,
                                            starting_row, num_rows);
        else if constexpr (std::is_same_v<IndexType, String1K>)
            _read_binary_string_<String1K>(stream, idx_vec, needs_flipping,
                                           starting_row, num_rows);
        else if constexpr (std::is_same_v<IndexType, String2K>)
            _read_binary_string_<String2K>(stream, idx_vec, needs_flipping,
                                           starting_row, num_rows);
        else if constexpr (std::is_same_v<IndexType, DateTime>)
            _read_binary_datetime_(stream, idx_vec, needs_flipping,
                                   starting_row, num_rows);
        else
            _read_binary_data_(stream, idx_vec, needs_flipping,
                               starting_row, num_rows);
        load_index(std::move(idx_vec));
    }

    for (uint16_t i = 0; i < col_num; ++i)  {
        stream.read(col_name, sizeof(col_name));
        stream.read(col_type, sizeof(col_type));

        const auto  citer = _typename_id_.find(col_type);

        if (citer == _typename_id_.end())  {
            String1K    err;

            err.printf("read_binary_(): ERROR: Type '%s' is not supported",
                       col_type);
            throw DataFrameError(err.c_str());
        }

        switch(citer->second)  {
            case file_dtypes::FLOAT: {
                ColumnVecType<float>    vec;

                _read_binary_data_(stream, vec, needs_flipping,
                               starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::DOUBLE: {
                ColumnVecType<double>   vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::SHORT: {
                ColumnVecType<short int>    vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::USHORT: {
                ColumnVecType<unsigned short int>   vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::INT: {
                ColumnVecType<int>  vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::UINT: {
                ColumnVecType<unsigned int> vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::LONG: {
                ColumnVecType<long int> vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::ULONG: {
                ColumnVecType<unsigned long int>    vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::LONG_LONG: {
                ColumnVecType<long long int>    vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::ULONG_LONG: {
                ColumnVecType<unsigned long long int>   vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::CHAR: {
                ColumnVecType<char> vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::UCHAR: {
                ColumnVecType<unsigned char>    vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::BOOL: {
                ColumnVecType<bool> vec;

                _read_binary_data_(stream, vec, needs_flipping,
                                   starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::STRING: {
                ColumnVecType<std::string>  vec;

                _read_binary_string_<std::string>(stream, vec, needs_flipping,
                                                  starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::VSTR32: {
                ColumnVecType<String32> vec;

                _read_binary_string_<String32>(stream, vec, needs_flipping,
                                               starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::VSTR64: {
                ColumnVecType<String64> vec;

                _read_binary_string_<String64>(stream, vec, needs_flipping,
                                               starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::VSTR128: {
                ColumnVecType<String128>    vec;

                _read_binary_string_<String128>(stream, vec, needs_flipping,
                                               starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::VSTR512: {
                ColumnVecType<String512>    vec;

                _read_binary_string_<String512>(stream, vec, needs_flipping,
                                               starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::VSTR1K: {
                ColumnVecType<String1K> vec;

                _read_binary_string_<String1K>(stream, vec, needs_flipping,
                                               starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::VSTR2K: {
                ColumnVecType<String2K> vec;

                _read_binary_string_<String2K>(stream, vec, needs_flipping,
                                               starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::DATETIME: {
                ColumnVecType<DateTime> vec;

                _read_binary_datetime_(stream, vec, needs_flipping,
                                       starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::STR_DBL_PAIR: {
                using val_t = std::pair<std::string, double>;

                ColumnVecType<val_t>    vec;

                _read_binary_str_dbl_pair_(stream, vec, needs_flipping,
                                           starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::STR_STR_PAIR: {
                using val_t = std::pair<std::string, std::string>;

                ColumnVecType<val_t>    vec;

                _read_binary_str_str_pair_(stream, vec, needs_flipping,
                                           starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::DBL_DBL_PAIR: {
                using val_t = std::pair<double, double>;

                ColumnVecType<val_t>    vec;

                _read_binary_dbl_dbl_pair_(stream, vec, needs_flipping,
                                           starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::DBL_VEC: {
                ColumnVecType<std::vector<double>>  vec;

                _read_binary_dbl_vec_(stream, vec, needs_flipping,
                                      starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::STR_VEC: {
                ColumnVecType<std::vector<std::string>> vec;

                _read_binary_str_vec_(stream, vec, needs_flipping,
                                      starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::DBL_SET: {
                ColumnVecType<std::set<double>> vec;

                _read_binary_dbl_set_(stream, vec, needs_flipping,
                                      starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::STR_SET: {
                ColumnVecType<std::set<std::string>>    vec;

                _read_binary_str_set_(stream, vec, needs_flipping,
                                      starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::STR_DBL_MAP: {
                ColumnVecType<std::map<std::string, double>>    vec;

                _read_binary_str_dbl_map_(stream, vec, needs_flipping,
                                          starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            case file_dtypes::STR_DBL_UNOMAP: {
                ColumnVecType<std::unordered_map<std::string, double>>  vec;

                _read_binary_str_dbl_map_(stream, vec, needs_flipping,
                                          starting_row, num_rows);
                load_column(col_name, std::move(vec),
                            nan_policy::dont_pad_with_nans);
                break;
            }
            default:  {
                String1K    err;

                err.printf("read_binary_(): ERROR: Type '%s' is not supported",
                           col_type);
                throw DataFrameError(err.c_str());
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool DataFrame<I, H>::
read(const char *file_name, io_format iof, const ReadParams params)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call read()");

    if (iof == io_format::csv2)  {
        IOFileOpti  io_opti(file_name);

        read_csv2_(io_opti.file,
                   params.columns_only,
                   params.starting_row,
                   params.num_rows,
                   params.skip_first_line,
                   params.schema);
    }
    else  {
        std::ifstream       stream;
        const IOStreamOpti  io_opti(
            stream, file_name, iof == io_format::binary);

        read<std::istream>(stream, iof, params);
    }
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S>
bool DataFrame<I, H>::
read(S &in_s, io_format iof, const ReadParams params)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call read()");

    if (iof == io_format::csv)  {
        if (params.starting_row != 0 ||
            params.num_rows != std::numeric_limits<size_type>::max())
            throw NotImplemented("read(): Reading files in chunks is currently"
                                 " only implemented for io_format::csv2");

        read_csv_(in_s, params.columns_only);
    }
    else if (iof == io_format::csv2)  {
        read_csv2_(in_s,
                   params.columns_only,
                   params.starting_row,
                   params.num_rows,
                   params.skip_first_line,
                   params.schema);
    }
    else if (iof == io_format::json)  {
        if (params.starting_row != 0 ||
            params.num_rows != std::numeric_limits<size_type>::max())
            throw NotImplemented("read(): Reading files in chunks is currently"
                                 " only implemented for io_format::csv2");

        read_json_(in_s, params.columns_only);
    }
    else if (iof == io_format::binary)  {
        read_binary_(in_s,
                     params.columns_only,
                     params.starting_row,
                     params.num_rows);
    }
    else
        throw NotImplemented("read(): This io_format is not implemented");

    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool
DataFrame<I, H>::from_string(const char *data_frame)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call from_string()");

    std::stringstream   ss (std::string(data_frame), std::ios_base::in);

    read<std::istream>(ss, io_format::csv);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool
DataFrame<I, H>::deserialize(const std::string &data_frame)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call deserialize()");

    std::stringstream   ss (data_frame, std::ios_base::in);

    read<std::istream>(ss, io_format::binary);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
std::future<bool> DataFrame<I, H>::
read_async(const char *file_name, io_format iof, const ReadParams params)  {

    return (thr_pool_.dispatch(
                true,
                [file_name,
                 iof,
                 params,
                 this] () -> bool  {
                    return (this->read(file_name, iof,
                                       std::forward<const ReadParams>(params)));
                }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S>
std::future<bool> DataFrame<I, H>::
read_async(S &in_s, io_format iof, const ReadParams params)  {

    return (thr_pool_.dispatch(
                true,
                [&in_s,
                 iof,
                 params,
                 this] () -> bool  {
                    return (this->read<S>(
                                in_s, iof,
                                std::forward<const ReadParams>(params)));
                }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
std::future<bool>
DataFrame<I, H>::from_string_async(const char *data_frame)  {

    return (thr_pool_.dispatch(true,
                               &DataFrame::from_string,
                                   this,
                                   data_frame)
            );
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
std::future<bool>
DataFrame<I, H>::deserialize_async(const std::string &data_frame)  {

    return (thr_pool_.dispatch(true,
                               &DataFrame::deserialize,
                                   this,
                                   std::forward<const std::string>(data_frame)
                               ));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
