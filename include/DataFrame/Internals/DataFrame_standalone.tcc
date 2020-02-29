// Hossein Moein
// December 30, 2019
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

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
static inline void
_sort_by_sorted_index_(std::vector<T> &to_be_sorted,
                       std::vector<size_t> &sorting_idxs,
                       size_t idx_s)  {

    if (idx_s > 0)  {
        idx_s -= 1;
        for (size_t i = 0; i < idx_s; ++i)  {
            // while the element i is not yet in place
            while (sorting_idxs[i] != sorting_idxs[sorting_idxs[i]])  {
                // swap it with the element at its final place
                const size_t    j = sorting_idxs[i];

                std::swap(to_be_sorted[j], to_be_sorted[sorting_idxs[j]]);
                std::swap(sorting_idxs[i], sorting_idxs[j]);
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename V, typename T, size_t N>
inline static void
_replace_vector_vals_(V &data_vec,
                      const std::array<T, N> &old_values,
                      const std::array<T, N> &new_values,
                      size_t &count,
                      int limit)  {

    const size_t    vec_s = data_vec.size();

    for (size_t i = 0; i < N; ++i)  {
        for (size_t j = 0; j < vec_s; ++j)  {
            if (limit >= 0 && count >= static_cast<size_t>(limit))  return;
            if (old_values[i] == data_vec[j])  {
                data_vec[j] = new_values[i];
                count += 1;
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename S, typename T>
inline static S &_write_json_df_index_(S &o, const T &value)  {

    return (o << value);
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_json_df_index_(S &o, const DateTime &value)  {

    return (o << value.time() << '.' << value.nanosec());
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_json_df_index_(S &o, const std::string &value)  {

    return (o << '"' << value << '"');
}

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

// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<float>  {

    inline void operator()(std::vector<float> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtof, file_type);
    }
};

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<double>  {

    inline void operator()(std::vector<double> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtod, file_type);
    }
};

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<long double>  {

    inline void operator()(std::vector<long double> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtold, file_type);
    }
};

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<int>  {

    inline void operator()(std::vector<int> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<long>  {

    inline void operator()(std::vector<long> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<long long>  {

    inline void operator()(std::vector<long long> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtoll, file_type);
    }
};

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned int>  {

    inline void operator()(std::vector<unsigned int> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtoul, file_type);
    }
};

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned long>  {

    inline void operator()(std::vector<unsigned long> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtoul, file_type);
    }
};

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<unsigned long long>  {

    inline void operator()(std::vector<unsigned long long> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtoull, file_type);
    }
};

// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------

template<>
struct  _IdxParserFunctor_<bool>  {

    inline void operator()(std::vector<bool> &vec,
                           std::ifstream &file,
                           io_format file_type = io_format::csv)  {

        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template<typename T>
inline static void
_generate_ts_index_(std::vector<T> &index_vec,
                    DateTime &start_di,
                    time_frequency t_freq,
                    long increment)  {

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

// ----------------------------------------------------------------------------

template<>
inline void
_generate_ts_index_<DateTime>(std::vector<DateTime> &index_vec,
                              DateTime &start_di,
                              time_frequency t_freq,
                              long increment)  {

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

// ----------------------------------------------------------------------------

template<typename S, typename T>
inline static S &_write_csv_df_index_(S &o, const T &value)  {

    return (o << value);
}

// ----------------------------------------------------------------------------

template<typename S>
inline static S &_write_csv_df_index_(S &o, const DateTime &value)  {

    return (o << value.time() << '.' << value.nanosec());
}

// ----------------------------------------------------------------------------

template<typename T>
inline static void _get_mem_numbers_(const VectorView<T> &container,
                                     size_t &used_mem,
                                     size_t &capacity_mem) {

    used_mem = sizeof(T *) * 2;
    capacity_mem = sizeof(T *) * 2;
}

// ----------------------------------------------------------------------------

template<typename T>
inline static void _get_mem_numbers_(const VectorPtrView<T> &container, 
                                     size_t &used_mem,
                                     size_t &capacity_mem) {

    used_mem = container.size() * sizeof(T *);
    capacity_mem = container.capacity() * sizeof(T *);
}

// ----------------------------------------------------------------------------

template<typename T>
inline static void _get_mem_numbers_(const std::vector<T> &container,
                                     size_t &used_mem,
                                     size_t &capacity_mem) {

    used_mem = container.size() * sizeof(T);
    capacity_mem = container.capacity() * sizeof(T);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
