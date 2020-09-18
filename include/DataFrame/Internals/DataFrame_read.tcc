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
#include <functional>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define gcc_likely(x)    __builtin_expect(!!(x), 1)
#define gcc_unlikely(x)  __builtin_expect(!!(x), 0)

// ----------------------------------------------------------------------------

template<typename I, typename  H>
void DataFrame<I, H>::read_json_(std::ifstream &file)  {

    char    c { '\0' };
    char    col_name[256];
    char    col_type[256];
    char    token[256];

    while (file.get(c))
        if (c != ' ' && c != '\n' && c != '\t')  break;
    if (c != '{')
        throw DataFrameError(
            "DataFrame::read_json_(): ERROR: Expected '{' (0)");

    bool    first_col = true;

    while (file.get(c)) {
        if (c == ' ' || c == '\n' || c == '\t')  continue;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (1)");
        _get_token_from_file_(file, '"', col_name);
        if (first_col)  {
            if (strcmp(col_name, DF_INDEX_COL_NAME))
                throw DataFrameError("DataFrame::read_json_(): ERROR: "
                                     "Expected column name 'INDEX'");
        }
        else {
            if (! strcmp(col_name, DF_INDEX_COL_NAME))
                throw DataFrameError("DataFrame::read_json_(): ERROR: "
                                     "column name 'INDEX' is not allowed");
        }

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (2)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '{')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '{' (3)");

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (4)");

        _get_token_from_file_(file, '"', token);
        if (strcmp(token, "N"))
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'N' (5)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (6)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        _get_token_from_file_(file, ',', token);

        const size_type col_size = ::atoi(token);

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (7)");
        _get_token_from_file_(file, '"', token);
        if (strcmp(token, "T"))
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'T' (8)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (9)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (10)");
        _get_token_from_file_(file, '"', col_type);

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != ',')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ',' (11)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '"')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '\"' (12)");
        _get_token_from_file_(file, '"', token);
        if (strcmp(token, "D"))
            throw DataFrameError(
                 "DataFrame::read_json_(): ERROR: Expected 'D' (13)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != ':')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ':' (14)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '[')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected ']' (15)");

        if (first_col)  {  // This is the index column
            IndexVecType    vec;

            vec.reserve(col_size);
            _IdxParserFunctor_<IndexType>()(vec, file, io_format::json);
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
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '}')
            throw DataFrameError(
                "DataFrame::read_json_(): ERROR: Expected '}' (16)");
        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != ',')  {
            file.unget();
            break;
        }

        first_col = false;
    }
    while (file.get(c))
        if (c != ' ' && c != '\n' && c != '\t')  break;
    if (c != '}')
        throw DataFrameError(
            "DataFrame::read_json_(): ERROR: Expected '}' (17)");
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
void DataFrame<I, H>::read_csv_(std::ifstream &file)  {

    char    col_name[256];
    char    value[32];
    char    type_str[64];
    char    c;

    while (file.get(c)) {
        if (c == '#' || c == '\n' || c == '\0') {
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

    String32    type_spec { };
    std::any    col_vec { };

    template<typename T>
    _col_data_spec_(T *cv, const char *ts) : type_spec(ts), col_vec(cv) {  }
};

template<typename I, typename  H>
void DataFrame<I, H>::read_csv2_(std::ifstream &file)  {

    char                            value[8192];
    char                            c;
    std::vector<_col_data_spec_>    spec_vec;
    bool                            header_read = false;
    size_type                       col_index = 0;

    spec_vec.reserve(32);
    while (file.get(c)) {
        if (c == '#' || c == '\n' || c == '\0') {
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
            if (c == '\n')  header_read = true;

            if (! ::strcmp(col_name, DF_INDEX_COL_NAME))  {
                IndexVecType    vec;

                vec.reserve(::atoi(value));
                load_index(std::move(vec));
                spec_vec.push_back(_col_data_spec_(&(get_index()), type_str));
            }
            else  {
                if (! ::strcmp(type_str, "float"))  {
                    std::vector<float>  &vec = create_column<float>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "double"))  {
                    std::vector<double> &vec = create_column<double>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "longdouble"))  {
                    std::vector<long double>    &vec =
                        create_column<long double>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "int"))  {
                    std::vector<int>    &vec = create_column<int>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "uint"))  {
                    std::vector<unsigned int>   &vec =
                        create_column<unsigned int>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "long"))  {
                    std::vector<long>   &vec = create_column<long>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "longlong"))  {
                    std::vector<long long>  &vec =
                        create_column<long long>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "ulong"))  {
                    std::vector<unsigned long>  &vec =
                        create_column<unsigned long>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "ulonglong"))  {
                    std::vector<unsigned long long> &vec =
                        create_column<unsigned long long>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "string"))  {
                    std::vector<std::string>    &vec =
                        create_column<std::string>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "DateTime"))  {
                    std::vector<DateTime>   &vec =
                        create_column<DateTime>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else if (! ::strcmp(type_str, "bool"))  {
                    std::vector<bool>   &vec = create_column<bool>(col_name);

                    vec.reserve(::atoi(value));
                    spec_vec.push_back(_col_data_spec_(&vec, type_str));
                }
                else
                    throw DataFrameError("DataFrame::read_csv2_(): ERROR: "
                                         "Unknown column type");
            }
        }
        else  {  // Now read the data columns row by row
            _get_token_from_file_(file, ',', value, '\n');
			std::cout << "XXXXXXDEBUG: " << spec_vec[col_index].type_spec
					  << ", " << col_index << ", " << value
				      << " -- " << spec_vec.size() << std::endl;
            if (spec_vec[col_index].type_spec == "float")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<float> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            strtof(value, nullptr));
            }
            else if (spec_vec[col_index].type_spec == "double")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<double> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            strtod(value, nullptr));
            }
            else if (spec_vec[col_index].type_spec == "longdouble")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<long double> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            strtold(value, nullptr));
            }
            else if (spec_vec[col_index].type_spec == "int")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<int> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            (int) strtol(value, nullptr, 0));
            }
            else if (spec_vec[col_index].type_spec == "uint")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<unsigned int> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            (unsigned int) strtoul(value, nullptr, 0));
            }
            else if (spec_vec[col_index].type_spec == "long")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<long> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            strtol(value, nullptr, 0));
            }
            else if (spec_vec[col_index].type_spec == "longlong")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<long long> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            strtoll(value, nullptr, 0));
            }
            else if (spec_vec[col_index].type_spec == "ulong")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<unsigned long> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            strtoul(value, nullptr, 0));
            }
            else if (spec_vec[col_index].type_spec == "ulonglong")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<unsigned long long> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            strtoull(value, nullptr, 0));
            }
            else if (spec_vec[col_index].type_spec == "string")  {
                std::any_cast<std::vector<std::string> *>
                    (spec_vec[col_index].col_vec)->emplace_back(value);
            }
            else if (spec_vec[col_index].type_spec == "DateTime")  {
                if (value[0] != '\0')  {
                    time_t      t;
                    int         n;
                    DateTime    dt;

#ifdef _WIN32
                    ::sscanf(value, "%lld.%d", &t, &n);
#else
                    ::sscanf(value, "%ld.%d", &t, &n);
#endif // _WIN32
                    dt.set_time(t, n);
                    std::any_cast<std::vector<DateTime> *>
                        (spec_vec[col_index].col_vec)->emplace_back(
                            std::move(dt));
                }
            }
            else if (spec_vec[col_index].type_spec == "bool")  {
                if (value[0] != '\0')
                    std::any_cast<std::vector<bool> *>
                        (spec_vec[col_index].col_vec)->push_back(
                            strtoul(value, nullptr, 0));
            }
            col_index += 1;
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
bool DataFrame<I, H>::read (const char *file_name, io_format iof)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call read()");

    std::ifstream   file;

    file.open(file_name, std::ios::in);  // Open for reading
    if (file.fail())  {
        String1K    err;

        err.printf("read(): ERROR: Unable to open file '%s'", file_name);
        throw DataFrameError(err.c_str());
    }

    if (iof == io_format::csv)
        read_csv_ (file);
    else if (iof == io_format::csv2)
        read_csv2_ (file);
    else if (iof == io_format::json)
        read_json_ (file);
    else
        throw NotImplemented("read(): This io_format is not implemented");

    file.close();
    return(true);
}

// ----------------------------------------------------------------------------

template<typename I, typename  H>
std::future<bool> DataFrame<I, H>::
read_async(const char *file_name, io_format iof) {

    return (std::async(std::launch::async,
                       &DataFrame::read,
                       this,
                       file_name,
                       iof));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
