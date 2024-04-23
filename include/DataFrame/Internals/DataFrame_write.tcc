// Hossein Moein
// July 24, 2019
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
#include <DataFrame/Utils/Utils.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<typename ... Ts>
bool DataFrame<I, H>::
write(const char *file_name,
      io_format iof,
      std::streamsize precision,
      bool columns_only,
      long max_recs) const  {

    std::ofstream       stream;
    const IOStreamOpti  io_opti(stream, file_name);

    if (stream.fail()) [[unlikely]]  {
        String1K    err;

        err.printf("write(): ERROR: Unable to open file '%s'", file_name);
        throw DataFrameError(err.c_str());
    }
    write<std::ostream, Ts ...>(stream, iof, precision, columns_only, max_recs);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
std::string
DataFrame<I, H>::to_string(std::streamsize precision) const  {

    std::stringstream   ss (std::ios_base::out);

    write<std::ostream, Ts ...>(ss, io_format::csv, precision);
    return (ss.str());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ... Ts>
bool DataFrame<I, H>::
write(S &o,
      io_format iof,
      std::streamsize precision,
      bool columns_only,
      long max_recs) const  {

    if (iof != io_format::csv &&
        iof != io_format::json &&
        iof != io_format::csv2)
        throw NotImplemented("write(): This io_format is not implemented");

    bool    need_pre_comma = false;
    long    end_row = indices_.size();
    long    start_row = 0;

    if (max_recs >= 0)
        end_row = std::min(end_row, max_recs);
    else
        start_row = std::max(long(0), end_row + max_recs);

    o.precision(precision);
    if (iof == io_format::json)  {
        o << "{\n";
        if (! columns_only)  {
            _write_json_df_header_<S, IndexType>(o,
                                                 DF_INDEX_COL_NAME,
                                                 end_row - start_row);

            o << "\"D\":[";
            if (end_row > start_row)  {
                _write_json_df_index_(o, indices_[start_row]);
                for (long i = start_row + 1; i < end_row; ++i)  {
                    o << ',';
                    _write_json_df_index_(o, indices_[i]);
                }
            }
            o << "]}";
            need_pre_comma = true;
        }

        const SpinGuard guard(lock_);

        for (const auto &[name, idx] : column_list_) [[likely]]  {
            print_json_functor_<Ts ...> functor (name.c_str(),
                                                 need_pre_comma,
                                                 o,
                                                 start_row,
                                                 end_row);

            data_[idx].change(functor);
            need_pre_comma = true;
        }
    }
    else if (iof == io_format::csv)  {
        if (! columns_only)  {
            _write_csv_df_header_<S, IndexType>(o,
                                                DF_INDEX_COL_NAME,
                                                end_row - start_row) << ':';

            for (long i = start_row; i < end_row; ++i)
                _write_csv_df_index_(o, indices_[i]) << ',';
            o << '\n';
        }

        const SpinGuard guard(lock_);

        for (const auto &[name, idx] : column_list_) [[likely]]  {
            print_csv_functor_<Ts ...>  functor (name.c_str(),
                                                 o,
                                                 start_row,
                                                 end_row);

            data_[idx].change(functor);
        }
    }
    else if (iof == io_format::csv2)  {
        if (! columns_only)  {
            _write_csv_df_header_<S, IndexType>(o,
                                                DF_INDEX_COL_NAME,
                                                end_row - start_row);
            need_pre_comma = true;
        }

        const SpinGuard guard_1(lock_);

        for (const auto &[name, idx] : column_list_) [[likely]]  {
            if (need_pre_comma)  o << ',';
            else  need_pre_comma = true;
            print_csv2_header_functor_<S, Ts ...>   functor(
                name.c_str(), o, end_row - start_row);

            data_[idx].change(functor);
        }
        o << '\n';

        need_pre_comma = false;
        for (long i = start_row; i < end_row; ++i)  {
            size_type   count = 0;

            if (! columns_only)  {
                o << indices_[i];
                need_pre_comma = true;
                count += 1;
            }

            const SpinGuard guard_2(lock_);

            for (auto citer = column_list_.begin();
                 citer != column_list_.end(); ++citer, ++count)  {
                print_csv2_data_functor_<S, Ts ...>  functor (i, o);

                if (need_pre_comma && count > 0)  o << ',';
                else  need_pre_comma = true;
                data_[citer->second].change(functor);
            }
            o << '\n';
        }
    }

    if (iof == io_format::json)
        o << "\n}";
    o << std::endl;
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
std::future<bool> DataFrame<I, H>::
write_async (const char *file_name,
             io_format iof,
             std::streamsize precision,
             bool columns_only,
             long max_recs) const  {

    return (thr_pool_.dispatch(
                true,
                [file_name,
                 iof,
                 precision,
                 columns_only,
                 max_recs,
                 this] () -> bool  {
                    return (this->write<Ts ...>(file_name,
                                                iof,
                                                precision,
                                                columns_only,
                                                max_recs));
                }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ... Ts>
std::future<bool> DataFrame<I, H>::
write_async (S &o,
             io_format iof,
             std::streamsize precision,
             bool columns_only,
             long max_recs) const  {

    return (thr_pool_.dispatch(
                true,
                [&o,
                 iof,
                 precision,
                 columns_only,
                 max_recs,
                 this] () -> bool  {
                    return (this->write<S, Ts ...>(o,
                                                   iof,
                                                   precision,
                                                   columns_only,
                                                   max_recs));
                }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
std::future<std::string> DataFrame<I, H>::
to_string_async (std::streamsize precision) const  {

    return (thr_pool_.dispatch(true,
                               &DataFrame::to_string<Ts ...>,
                                   this,
                                   precision));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
