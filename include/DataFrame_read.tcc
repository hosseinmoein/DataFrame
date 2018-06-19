// Hossein Moein
// September 12, 2017
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame.h>
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
#include <DMScu_MMapFile.h>
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)
#include <cstdlib>
#include <fstream>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define gcc_likely(x)    __builtin_expect(!!(x), 1)
#define gcc_unlikely(x)  __builtin_expect(!!(x), 0)

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
template<typename TS, template<typename DT, class... types> class DS>
bool DataFrame<TS, DS>::read (const char *file_name)  {

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
                DS<double>  &vec = create_column<double>(value);

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
                DS<int> &vec = create_column<int>(value);

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
                DS<unsigned int>    &vec = create_column<unsigned int>(value);

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
                DS<long>    &vec = create_column<long>(value);

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
                DS<unsigned long>   &vec = create_column<unsigned long>(value);

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
                DS<std::string> &vec = create_column<std::string>(value);

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
                DS<bool>    &vec = create_column<bool>(value);
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
template <typename TS, template <typename DT, class... types> class DS>
bool DataFrame<TS, DS>::read(const char* file_name)
{
    std::ifstream file;
    file.open(file_name, std::ios::in);  // Open for reading
    if (!file) {
        std::cerr << "Unable to open csv file";
        exit(1);
    }

    bool beg_line = true;
    char value[1024];
    char type[64];

    char c;
    while (file.get(c)) {
        if (c == '#' || c == '\n' || c == '\0') {
            if (c == '#')
				while (file.get(c))
                    if (c == '\n') break;
            continue;
        }
		file.putback(c); // Put back the character after checking if it is #
		std::string value_str;
		std::getline(file, value_str, ':');
        if (!::strcmp(value, "INDEX")) {
            TSVec vec;
            while (std::getline(file, value_str, ',')) {
                vec.push_back(static_cast<TimeStamp>(atoll(value_str.c_str())));
            }
            load_index(std::forward<TSVec&&>(vec));
        } else {
            c = static_cast<char>(file.get_char());
            if (c != '<')
                throw DataFrameError(
                    "DataFrame::read(): ERROR: Expected "
                    "'<' char to specify column type");
            file.get_token('>', type);
            c = static_cast<char>(file.get_char());
            if (c != ':')
                throw DataFrameError(
                    "DataFrame::read(): ERROR: Expected "
                    "':' char to start column values");

            if (!::strcmp(type, "double")) {
                DS<double>& vec = create_column<double>(value);

                while (!file.is_eof()) {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n')) break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(atof(value));
                }
            } else if (!::strcmp(type, "int")) {
                DS<int>& vec = create_column<int>(value);

                while (!file.is_eof()) {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n')) break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(atoi(value));
                }
            } else if (!::strcmp(type, "uint")) {
                DS<unsigned int>& vec = create_column<unsigned int>(value);

                while (!file.is_eof()) {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n')) break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(static_cast<unsigned int>(atol(value)));
                }
            } else if (!::strcmp(type, "long")) {
                DS<long>& vec = create_column<long>(value);

                while (!file.is_eof()) {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n')) break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(atol(value));
                }
            } else if (!::strcmp(type, "ulong")) {
                DS<unsigned long>& vec = create_column<unsigned long>(value);

                while (!file.is_eof()) {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n')) break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(static_cast<unsigned long>(atoll(value)));
                }
            } else if (!::strcmp(type, "string")) {
                DS<std::string>& vec = create_column<std::string>(value);

                while (!file.is_eof()) {
                    c = static_cast<char>(file.get_char());
                    if (gcc_unlikely(c == '\n')) break;
                    file.put_back();
                    file.get_token(',', value);
                    vec.push_back(value);
                }
            } else if (!::strcmp(type, "bool")) {
                DS<bool>& vec = create_column<bool>(value);
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

template <typename TS, template <typename DT, class... types> class DS>
std::future<bool> DataFrame<TS, DS>::read_async(const char *file_name) {
	return (std::async(std::launch::async, &DataFrame::read, this, file_name));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
