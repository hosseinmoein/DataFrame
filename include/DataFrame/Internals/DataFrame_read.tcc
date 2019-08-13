// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <cstdlib>
#include <functional>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define gcc_likely(x)    __builtin_expect(!!(x), 1)
#define gcc_unlikely(x)  __builtin_expect(!!(x), 0)

// ----------------------------------------------------------------------------

inline static void
_get_token_from_file_ (std::ifstream &file,
                       char delim,
                       char *value,
                       char alt_delim = '\0') {

    char    c;
    int     count = 0;

    while (file.get (c))
        if (c == delim)  {
            break;
        }
        else if (c == alt_delim)  {
            file.unget();
            break;
        } 
        else  {
            value[count++] = c;
        }

    value[count] = 0;
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
inline static void
_col_vector_push_back_(V &vec,
                       std::ifstream &file,
                       T (*converter)(const char *, char **, int),
                       io_format file_type = io_format::csv)  {

    char    value[1024];
    char    c = 0;

    while (file.get(c)) {
        if (file_type == io_format::csv && c == '\n')  break;
        else if (file_type == io_format::json && c == ']')  break;
        file.unget();
        _get_token_from_file_(file, ',', value,
                              file_type == io_format::json ? ']' : '\0');
        vec.push_back(static_cast<T>(converter(value, nullptr, 0)));
    }
}

// -------------------------------------

template<typename T, typename V>
inline static void
_col_vector_push_back_(V &vec,
                       std::ifstream &file,
                       T (*converter)(const char *, char **),
                       io_format file_type = io_format::csv)  {

    char    value[1024];
    char    c = 0;

    while (file.get(c)) {
        if (file_type == io_format::csv && c == '\n')  break;
        else if (file_type == io_format::json && c == ']')  break;
        file.unget();
        _get_token_from_file_(file, ',', value,
                              file_type == io_format::json ? ']' : '\0');
        vec.push_back(static_cast<T>(converter(value, nullptr)));
    }
}

// -------------------------------------

template<>
inline void
_col_vector_push_back_<const char *, std::vector<std::string>>(
    std::vector<std::string> &vec,
    std::ifstream &file,
    const char * (*converter)(const char *, char **),
    io_format file_type)  {

    char    value[1024];
    char    c = 0;

    while (file.get(c)) {
        if (file_type == io_format::csv && c == '\n')  break;
        else if (file_type == io_format::json && c == ']')  break;
        file.unget();
        _get_token_from_file_(file, ',', value,
                              file_type == io_format::json ? ']' : '\0');
        vec.push_back(value);
    }
}

// -------------------------------------

inline void
_json_str_col_vector_push_back_(std::vector<std::string> &vec,
                                std::ifstream &file)  {

    char    value[1024];
    char    c = 0;

    while (file.get(c))
        if (c != ' ' && c != '\n' && c != '\t')  {
            file.unget();
            break;
        }

    while (file.get(c)) {
        if (c == ']')  break;
        file.unget();

        std::size_t count = 0;

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t')  break;
        if (c != '"')
            throw DataFrameError(
                "_json_str_col_vector_push_back_(): ERROR: Expected '\"' (0)");

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
                "_json_str_col_vector_push_back_(): ERROR: Expected ',' (2)");
    }
}

// -------------------------------------

template<>
inline void
_col_vector_push_back_<DateTime, std::vector<DateTime>>(
    std::vector<DateTime> &vec,
    std::ifstream &file,
    DateTime (*converter)(const char *, char **),
    io_format file_type)  {

    char    value[1024];
    char    c = 0;

    while (file.get(c)) {
        if (file_type == io_format::csv && c == '\n')  break;
        else if (file_type == io_format::json && c == ']')  break;
        file.unget();
        _get_token_from_file_(file, ',', value,
                              file_type == io_format::json ? ']' : '\0');

        time_t      t;
        int         n;
        DateTime    dt;

#ifdef _WIN32
        ::sscanf(value, "%lld.%d", &t, &n);
#else
        ::sscanf(value, "%ld.%d", &t, &n);
#endif // _WIN32
        dt.set_time(t, n);
        vec.emplace_back(std::move(dt));
    }
}

// ----------------------------------------------------------------------------

template<typename T>
struct  _IdxParserFunctor_  {

    void operator()(std::vector<T> &,
                    std::ifstream &file,
                    io_format file_type = io_format::csv)  {   }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<float>  {

    inline void operator()(std::vector<float> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtof, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<double>  {

    inline void operator()(std::vector<double> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtod, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<long double>  {

    inline void operator()(std::vector<long double> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtold, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<int>  {

    inline void operator()(std::vector<int> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<long>  {

    inline void operator()(std::vector<long> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<long long>  {

    inline void operator()(std::vector<long long> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtoll, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned int>  {

    inline void operator()(std::vector<unsigned int> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtoul, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned long>  {

    inline void operator()(std::vector<unsigned long> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtoul, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned long long>  {

    inline void operator()(std::vector<unsigned long long> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtoull, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<std::string>  {

    inline void operator()(std::vector<std::string> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        auto    converter =
            [](const char *s, char **)-> const char * { return s; };

        _col_vector_push_back_<const char *, std::vector<std::string>>
            (vec, file, converter, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<DateTime>  {

    inline void operator()(std::vector<DateTime> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        auto    converter =
            [](const char *, char **)-> DateTime  { return DateTime(); };

        _col_vector_push_back_<DateTime, std::vector<DateTime>>
            (vec, file, converter, file_type);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<bool>  {

    inline void operator()(std::vector<bool> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

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
            if (strcmp(col_name, "INDEX"))
                throw DataFrameError("DataFrame::read_json_(): ERROR: "
                                     "Expected column name 'INDEX'");
        }
        else {
            if (! strcmp(col_name, "INDEX"))
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

    if (iof == io_format::csv)  {
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
                throw DataFrameError("DataFrame::read(): ERROR: Expected "
                                     "'<' char to specify column type");
            _get_token_from_file_(file, '>', type_str);
            file.get(c);
            if (c != ':')
                throw DataFrameError("DataFrame::read(): ERROR: Expected "
                                     "':' char to start column values");

            if (! ::strcmp(col_name, "INDEX"))  {
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
                        [](const char *s, char **)-> const char * {
                            return s;
                        };

                    vec.reserve(::atoi(value));
                    _col_vector_push_back_
                        <const char *, std::vector<std::string>>
                        (vec, file, converter);
                }
                else if (! ::strcmp(type_str, "DateTime"))  {
                    std::vector<DateTime>   &vec =
                        create_column<DateTime>(col_name);
                    auto                    converter =
                        [](const char *, char **)-> DateTime {
                            return DateTime();
                        };

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
                    throw DataFrameError ("DataFrame::read(): ERROR: Unknown "
                                          "column type");
            }
        }
    }
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
