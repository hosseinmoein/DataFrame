// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DateTime.h>
#include <DataFrame/DataFrame.h>

#include <cstdlib>
#include <fstream>
#include <functional>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define gcc_likely(x)    __builtin_expect(!!(x), 1)
#define gcc_unlikely(x)  __builtin_expect(!!(x), 0)

// ----------------------------------------------------------------------------

inline static void
_get_token_from_file_  (std::ifstream &file, char delim, char *value) {

    char    c;
    int     count = 0;

    while (file.get (c))
        if (c == delim)
            break;
        else
            value[count++] = c;

    value[count] = 0;
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
inline static void
_col_vector_push_back_(V &vec,
                       std::ifstream &file,
                       T (*converter)(const char *))  {

    char    value[1024];
    char    c = 0;

    while (file.get(c)) {
        if (c == '\n')  break;
        file.unget();
        _get_token_from_file_(file, ',', value);
        vec.push_back(static_cast<T>(converter(value)));
    }
}

// -------------------------------------

template<>
inline void
_col_vector_push_back_<const char *, std::vector<std::string>>(
    std::vector<std::string> &vec,
    std::ifstream &file,
    const char * (*converter)(const char *))  {

    char    value[1024];
    char    c = 0;

    while (file.get(c)) {
        if (c == '\n')  break;
        file.unget();
        _get_token_from_file_(file, ',', value);
        vec.push_back(value);
    }
}

// -------------------------------------

template<>
inline void
_col_vector_push_back_<DateTime, std::vector<DateTime>>(
    std::vector<DateTime> &vec,
    std::ifstream &file,
    DateTime (*converter)(const char *))  {

    char    value[1024];
    char    c = 0;

    while (file.get(c)) {
        if (c == '\n')  break;
        file.unget();
        _get_token_from_file_(file, ',', value);

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

    void operator()(std::vector<T> &, std::ifstream &file)  {  }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<double>  {

    inline void operator()(std::vector<double> &vec, std::ifstream &file)  {

        _col_vector_push_back_(vec, file, &::atof);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<int>  {

    inline void operator()(std::vector<int> &vec, std::ifstream &file)  {

        _col_vector_push_back_(vec, file, &::atoi);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<long>  {

    inline void operator()(std::vector<long> &vec, std::ifstream &file)  {

        _col_vector_push_back_(vec, file, &::atol);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<long long>  {

    inline void operator()(std::vector<long long> &vec, std::ifstream &file) {

        _col_vector_push_back_(vec, file, &::atoll);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned long>  {

    inline void
    operator()(std::vector<unsigned long> &vec, std::ifstream &file)  {

        _col_vector_push_back_(vec, file, &::atol);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned long long>  {

    inline void
    operator()(std::vector<unsigned long long> &vec, std::ifstream &file)  {

        _col_vector_push_back_(vec, file, &::atoll);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<std::string>  {

    inline void
    operator()(std::vector<std::string> &vec, std::ifstream &file)  {

        auto    converter =
            [](const char *s)-> const char * { return s; };

        _col_vector_push_back_<const char *, std::vector<std::string>>
            (vec, file, converter);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<DateTime>  {

    inline void
    operator()(std::vector<DateTime> &vec, std::ifstream &file) {

        auto    converter =
            [](const char *)-> DateTime  { return DateTime(); };

        _col_vector_push_back_<DateTime, std::vector<DateTime>>
            (vec, file, converter);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<bool>  {

    inline void operator()(std::vector<bool> &vec, std::ifstream &file)  {

        _col_vector_push_back_(vec, file, &::atoi);
    }
};

// ----------------------------------------------------------------------------

template<typename I, typename  H>
bool DataFrame<I, H>::read (const char *file_name, io_format iof)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call read()");

    std::ifstream   file;

    file.open(file_name, std::ios::in);  // Open for reading

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
            _IdxParserFunctor_<typename IndexVecType::value_type>()(vec, file);
            load_index(std::forward<IndexVecType &&>(vec));
        }
        else  {
            if (! ::strcmp(type_str, "double"))  {
                std::vector<double> &vec = create_column<double>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, ::atof);
            }
            else if (! ::strcmp(type_str, "int"))  {
                std::vector<int> &vec = create_column<int>(col_name);

                _col_vector_push_back_(vec, file, &::atoi);
            }
            else if (! ::strcmp(type_str, "uint"))  {
                std::vector<unsigned int>   &vec =
                    create_column<unsigned int>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::atol);
            }
            else if (! ::strcmp(type_str, "long"))  {
                std::vector<long>   &vec = create_column<long>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::atol);
            }
            else if (! ::strcmp(type_str, "ulong"))  {
                std::vector<unsigned long>  &vec =
                    create_column<unsigned long>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::atoll);
            }
            else if (! ::strcmp(type_str, "string"))  {
                std::vector<std::string>    &vec =
                    create_column<std::string>(col_name);
                auto                        converter =
                    [](const char *s)-> const char * { return s; };

                vec.reserve(::atoi(value));
                _col_vector_push_back_<const char *, std::vector<std::string>>
                    (vec, file, converter);
            }
            else if (! ::strcmp(type_str, "DateTime"))  {
                std::vector<DateTime>   &vec =
                    create_column<DateTime>(col_name);
                auto                    converter =
                    [](const char *)-> DateTime { return DateTime(); };

                vec.reserve(::atoi(value));
                _col_vector_push_back_<DateTime, std::vector<DateTime>>
                    (vec, file, converter);
            }
            else if (! ::strcmp(type_str, "bool"))  {
                std::vector<bool>   &vec = create_column<bool>(col_name);

                vec.reserve(::atoi(value));
                _col_vector_push_back_(vec, file, &::atoi);
            }
            else
                throw DataFrameError ("DataFrame::read(): ERROR: Unknown "
                                      "column type");
        }
    }

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
