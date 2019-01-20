// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "DateTime.h"
#include "DataFrame.h"
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
#include "DMScu_MMapFile.h"
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
#include <cstdlib>
#include <fstream>
#include <functional>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define gcc_likely(x)    __builtin_expect(!!(x), 1)
#define gcc_unlikely(x)  __builtin_expect(!!(x), 0)

// ----------------------------------------------------------------------------

#ifdef _WIN32
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
#endif // _WIN32
	
// ----------------------------------------------------------------------------

template<typename T, typename V>
inline static void
_col_vector_push_back_(V &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                       DMScu_MMapFile &file,
#elif _WIN32
                       std::ifstream &file,
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                       T (*converter)(const char *))  {

    char    value[1024];

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    while (! file.is_eof ())  {
        char    c = static_cast<char>(file.get_char());

        if (gcc_unlikely(c == '\n'))  break;
        file.put_back();
        file.get_token(',', value);
        vec.push_back(converter(value));
    }
#elif _WIN32
    char    c = 0;

    while (file.get(c)) {
        if (c == '\n')  break;
        file.unget();
        _get_token_from_file_(file, ',', value);
        vec.push_back(converter(value));
    }
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
}

// -------------------------------------

template<>
inline void
_col_vector_push_back_<const char *, std::vector<std::string>>(
    std::vector<std::string> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    DMScu_MMapFile &file,
#elif _WIN32
    std::ifstream &file,
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    const char * (*converter)(const char *))  {

    char    value[1024];

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    while (! file.is_eof ())  {
        char    c = static_cast<char>(file.get_char());

        if (gcc_unlikely(c == '\n'))
            break;
        file.put_back();
        file.get_token(',', value);
        vec.push_back(value);
    }
#elif _WIN32
    char    c = 0;

    while (file.get(c)) {
        if (c == '\n')  break;
        file.unget();
        _get_token_from_file_(file, ',', value);
        vec.push_back(value);
    }
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
}

// -------------------------------------

template<>
inline void
_col_vector_push_back_<DateTime, std::vector<DateTime>>(
    std::vector<DateTime> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    DMScu_MMapFile &file,
#elif _WIN32
    std::ifstream &file,
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    DateTime (*converter)(const char *))  {

    char    value[1024];

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    while (! file.is_eof ())  {
        char    c = static_cast<char>(file.get_char());

        if (gcc_unlikely(c == '\n'))
            break;
        file.put_back();
        file.get_token(',', value);

        time_t      t;
        int         n;
        DateTime    dt;

        ::sscanf(value, "%ld.%d", &t, &n);
        dt.set_time(t, n);
        vec.push_back(dt);
    }
#elif _WIN32
    char    c = 0;

    while (file.get(c)) {
        if (c == '\n')  break;
        file.unget();
        _get_token_from_file_(file, ',', value);

        time_t      t;
        int         n;
        DateTime    dt;

        ::sscanf(value, "%ld.%d", &t, &n);
        dt.set_time(t, n);
        vec.emplace_back(std::move(dt));
    }
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
}

// ----------------------------------------------------------------------------

template<typename T>
struct  _IdxParserFunctor_  {

    void operator()(std::vector<T> &,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                    DMScu_MMapFile &file
#elif _WIN32
                    std::ifstream &file
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                   )  {  }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<double>  {

    inline void operator()(std::vector<double> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                    DMScu_MMapFile &file
#elif _WIN32
                    std::ifstream &file
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                   )  {

        _col_vector_push_back_(vec, file, &::atof);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<int>  {

    inline void operator()(std::vector<int> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                    DMScu_MMapFile &file
#elif _WIN32
                    std::ifstream &file
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                   )  {

        _col_vector_push_back_(vec, file, &::atoi);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<long>  {

    inline void operator()(std::vector<long> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                    DMScu_MMapFile &file
#elif _WIN32
                    std::ifstream &file
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                   )  {

        _col_vector_push_back_(vec, file, &::atol);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned long>  {

    inline void
    operator()(std::vector<unsigned long> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                    DMScu_MMapFile &file
#elif _WIN32
                    std::ifstream &file
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                   )  {

        _col_vector_push_back_(vec, file, &::atoll);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<std::string>  {

    inline void
    operator()(std::vector<std::string> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                    DMScu_MMapFile &file
#elif _WIN32
                    std::ifstream &file
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                   )  {

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
    operator()(std::vector<DateTime> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                    DMScu_MMapFile &file
#elif _WIN32
                    std::ifstream &file
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                   ) {

        auto    converter =
            [](const char *)-> DateTime  { return DateTime(); };

        _col_vector_push_back_<DateTime, std::vector<DateTime>>
            (vec, file, converter);
    }
};

// -------------------------------------

template<>
struct  _IdxParserFunctor_<bool>  {

    inline void operator()(std::vector<bool> &vec,
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                    DMScu_MMapFile &file
#elif _WIN32
                    std::ifstream &file
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
                   )  {

        _col_vector_push_back_(vec, file, &::atoi);
    }
};

// ----------------------------------------------------------------------------

template<typename TS, typename  HETERO>
bool DataFrame<TS, HETERO>::read (const char *file_name, io_format iof)  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value,
                  "Only a StdDataFrame can call read()");

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    DMScu_MMapFile  file (file_name,
                          DMScu_MMapFile::_read_,
                          DMScu_MMapBase::SYSTEM_PAGE_SIZE * 2);
#elif _WIN32
    std::ifstream   file;

    file.open(file_name, std::ios::in);  // Open for reading
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)

    char    col_name[256];
    char    value[1024];
    char    type_str[64];
    char    c;

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
    while (! file.is_eof ())  {
        c = static_cast<char>(file.get_char());

        if (c == '#' || c == '\n' || c == '\0')  {
            if (c == '#')
                while (! file.is_eof ())  {
                    c = static_cast<char>(file.get_char());
                    if (c == '\n')
                        break;
                }
            continue;
        }
        file.put_back();

        file.get_token(':', col_name);
        c = static_cast<char>(file.get_char());
        if (c != '<')
            throw DataFrameError ("DataFrame::read(): ERROR: Expected "
                                  "'<' char to specify column type");
        file.get_token('>', type_str);
        c = static_cast<char>(file.get_char());
        if (c != ':')
            throw DataFrameError ("DataFrame::read(): ERROR: Expected "
                                  "':' char to start column values");
#elif _WIN32
    while (file.get(c)) {
        if (c == '#' || c == '\n' || c == '\0') {
            if (c == '#')
                while (file.get(c))
                    if (c == '\n') break;

            continue;
        }
        file.unget();

        _get_token_from_file_(file, ':', col_name);
        file.get(c);
        if (c != '<')
            throw DataFrameError("DataFrame::read(): ERROR: Expected "
                                 "'<' char to specify column type");
        _get_token_from_file_(file, '>', type_str);
        file.get(c);
        if (c != ':')
            throw DataFrameError("DataFrame::read(): ERROR: Expected "
                                 "':' char to start column values");
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)

        if (! ::strcmp(col_name, "INDEX"))  {
            TSVec   vec;

            _IdxParserFunctor_<typename TSVec::value_type>()(vec, file);
            load_index(std::forward<TSVec &&>(vec));
        }
        else  {
            if (! ::strcmp(type_str, "double"))  {
                std::vector<double> &vec = create_column<double>(col_name);

                _col_vector_push_back_(vec, file, ::atof);
            }
            else if (! ::strcmp(type_str, "int"))  {
                std::vector<int> &vec = create_column<int>(col_name);

                _col_vector_push_back_(vec, file, &::atoi);
            }
            else if (! ::strcmp(type_str, "uint"))  {
                std::vector<unsigned int>   &vec =
                    create_column<unsigned int>(col_name);

                _col_vector_push_back_(vec, file, &::atol);
            }
            else if (! ::strcmp(type_str, "long"))  {
                std::vector<long>   &vec = create_column<long>(col_name);

                _col_vector_push_back_(vec, file, &::atol);
            }
            else if (! ::strcmp(type_str, "ulong"))  {
                std::vector<unsigned long>  &vec =
                    create_column<unsigned long>(col_name);

                _col_vector_push_back_(vec, file, &::atoll);
            }
            else if (! ::strcmp(type_str, "string"))  {
                std::vector<std::string>    &vec =
                    create_column<std::string>(col_name);
                auto                        converter =
                    [](const char *s)-> const char * { return s; };

                _col_vector_push_back_<const char *, std::vector<std::string>>
                    (vec, file, converter);
            }
            else if (! ::strcmp(type_str, "DateTime"))  {
                std::vector<DateTime>   &vec =
                    create_column<DateTime>(col_name);
                auto                    converter =
                    [](const char *)-> DateTime { return DateTime(); };

                _col_vector_push_back_<DateTime, std::vector<DateTime>>
                    (vec, file, converter);
            }
            else if (! ::strcmp(type_str, "bool"))  {
                std::vector<bool>   &vec = create_column<bool>(col_name);

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

template<typename TS, typename  HETERO>
std::future<bool> DataFrame<TS, HETERO>::
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
