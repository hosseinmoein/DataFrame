// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

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

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
template<typename TS>
bool DataFrame<TS>::read (const char *file_name)  {

    DMScu_MMapFile  file (file_name,
                          DMScu_MMapFile::_read_,
                          DMScu_MMapBase::SYSTEM_PAGE_SIZE * 2);

    bool    beg_line = true;
    char    value[1024];
    char    type[64];

    while (! file.is_eof ())  {
        char  c = static_cast<char>(file.get_char());

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

        file.get_token(':', value);
        if (! ::strcmp(value, "INDEX"))  {
            TSVec   vec;

            while (! file.is_eof ())  {
                c = static_cast<char>(file.get_char());
                if (gcc_unlikely(c == '\n'))
                    break;
                file.put_back();
                file.get_token(',', value);
                vec.push_back(static_cast<TimeStamp>(atoll(value)));
            }
            load_index(std::forward<TSVec &&>(vec));
        }
        else  {
            c = static_cast<char>(file.get_char());
            if (c != '<')
                throw DataFrameError ("DataFrame::read(): ERROR: Expected "
                                      "'<' char to specify column type");
            file.get_token('>', type);
            c = static_cast<char>(file.get_char());
            if (c != ':')
                throw DataFrameError ("DataFrame::read(): ERROR: Expected "
                                      "':' char to start column values");

            if (! ::strcmp(type, "double"))  {
                std::vector<double> &vec = create_column<double>(value);

                while (! file.is_eof ())  {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n'))
                        break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(atof(value));
                }
            }
            else if (! ::strcmp(type, "int"))  {
                std::vector<int> &vec = create_column<int>(value);

                while (! file.is_eof ())  {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n'))
                        break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(atoi(value));
                }
            }
            else if (! ::strcmp(type, "uint"))  {
                std::vector<unsigned int>   &vec =
                    create_column<unsigned int>(value);

                while (! file.is_eof ())  {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n'))
                        break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(static_cast<unsigned int>(atol(value)));
                }
            }
            else if (! ::strcmp(type, "long"))  {
                std::vector<long>   &vec = create_column<long>(value);

                while (! file.is_eof ())  {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n'))
                        break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(atol(value));
                }
            }
            else if (! ::strcmp(type, "ulong"))  {
                std::vector<unsigned long>  &vec =
                    create_column<unsigned long>(value);

                while (! file.is_eof ())  {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n'))
                        break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(static_cast<unsigned long>(atoll(value)));
                }
            }
            else if (! ::strcmp(type, "string"))  {
                std::vector<std::string>    &vec =
                    create_column<std::string>(value);

                while (! file.is_eof ())  {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n'))
                        break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(value);
                }
            }
            else if (! ::strcmp(type, "bool"))  {
                std::vector<bool>   &vec = create_column<bool>(value);
            }
            else
                throw DataFrameError ("DataFrame::read(): ERROR: Unknown "
                                      "column type");
        }
    }

    file.close();
    return(true);
}
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)

#ifdef _WIN32
template <typename TS>
bool DataFrame<TS>::read(const char* file_name)  {

    auto get_token = [](const char& delim, std::ifstream& file) {
        std::string token;
        char c;
        while (file.get(c)) {
            if (c == delim)
                break;
            else
                token.push_back(c);
        }
        return token;
    };
    std::ifstream file;
    file.open(file_name, std::ios::in);  // Open for reading
    if (!file) {
        std::cerr << "Unable to open csv file";
        exit(1);
    }
    char value[1024];
    std::string value_str;  // Store value as a string
    std::string type_str;  // Store value as a string
    char c;
    while (file.get(c)) {
        if (c == '#' || c == '\n' || c == '\0') {
            if (c == '#')
                while (file.get(c))
                    if (c == '\n') break;
            continue;
        }
        file.unget();
        value_str = get_token(':', file);
        // This is done so the DS vector created can use the the
        // value char array
        strcpy_s(value, value_str.c_str());
        if (value_str == "INDEX") {
            TSVec vec;
            while (file.get(c)) {
                if (c == '\n') break;
                file.unget();
                value_str = get_token(',', file);
                strcpy_s(value, value_str.c_str());
                vec.push_back(static_cast<TimeStamp>(atoll(value)));
            }
            load_index(std::forward<TSVec&&>(vec));
        } else {
            file.get(c);
            if (c != '<')
                throw DataFrameError(
                    "DataFrame::read(): ERROR: Expected "
                    "'<' char to specify column type");
            type_str = get_token('>', file);
            file.get(c);
            if (c != ':')
                throw DataFrameError(
                    "DataFrame::read(): ERROR: Expected "
                    "':' char to start column values");
            if (type_str == "double") {
                std::vector<double>& vec = create_column<double>(value);
                while (file.get(c)) {
                    if (c == '\n') break;
                    file.unget();
                    value_str = get_token(',', file);
                    strcpy_s(value, value_str.c_str());
                    vec.push_back(atof(value));
                }
            } else if (type_str == "int") {
                std::vector<int>& vec = create_column<int>(value);
                while (file.get(c)) {
                    if (c == '\n') break;
                    file.unget();
                    value_str = get_token(',', file);
                    strcpy_s(value, value_str.c_str());
                    vec.push_back(atoi(value));
                }
            } else if (type_str == "uint") {
                std::vector<unsigned int>& vec =
                    create_column<unsigned int>(value);
                while (file.get(c)) {
                    if (c == '\n') break;
                    file.unget();
                    value_str = get_token(',', file);
                    strcpy_s(value, value_str.c_str());
                    vec.push_back(static_cast<unsigned int>(atol(value)));
                }
            } else if (type_str == "long") {
                std::vector<long>& vec = create_column<long>(value);
                while (file.get(c)) {
                    if (c == '\n') break;
                    file.unget();
                    value_str = get_token(',', file);
                    strcpy_s(value, value_str.c_str());
                    vec.push_back(atol(value));
                }
            } else if (type_str == "ulong") {
                std::vector<unsigned long>& vec =
                    create_column<unsigned long>(value);
                while (file.get(c)) {
                    if (c == '\n') break;
                    file.unget();
                    value_str = get_token(',', file);
                    strcpy_s(value, value_str.c_str());
                    vec.push_back(static_cast<unsigned long>(atoll(value)));
                }
            } else if (type_str == "string") {
                std::vector<std::string>& vec =
                    create_column<std::string>(value);
                while (file.get(c)) {
                    if (c == '\n') break;
                    file.unget();
                    value_str = get_token(',', file);
                    strcpy_s(value, value_str.c_str());
                    vec.push_back(value);
                }
            } else if (type_str == "bool") {
                std::vector<bool>& vec = create_column<bool>(value);
            } else
                throw DataFrameError(
                    "DataFrame::read(): ERROR: Unknown "
                    "column type");
        }
    }

    file.close();
    return (true);
}
#endif  // _WIN32

// ----------------------------------------------------------------------------

template <typename TS>
std::future<bool> DataFrame<TS>::read_async(const char *file_name) {
    return (std::async(std::launch::async, &DataFrame::read, this, file_name));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
