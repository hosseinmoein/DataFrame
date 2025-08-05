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
#include <DataFrame/Utils/Endianness.h>
#include <DataFrame/Utils/PrettyPrint.h>
#include <DataFrame/Utils/Utils.h>

#include <format>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<typename ... Ts>
bool DataFrame<I, H>::
write(const char *file_name, io_format iof, const WriteParams<> params) const  {

    std::ofstream       stream;
    const IOStreamOpti  io_opti(stream, file_name, iof == io_format::binary);

    write<std::ostream, Ts ...>(stream, iof, params);
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
std::string
DataFrame<I, H>::to_string(std::streamsize precision) const  {

    std::stringstream   ss (std::ios_base::out);

    write<std::ostream, Ts ...>(ss, io_format::csv, { .precision = precision });
    return (ss.str());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
std::string
DataFrame<I, H>::serialize() const  {

    std::stringstream   ss (std::ios_base::out);

    write<std::ostream, Ts ...>(ss, io_format::binary);
    return (ss.str());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ... Ts>
bool DataFrame<I, H>::
write(S &o, io_format iof, const WriteParams<> params) const  {

    if (iof != io_format::csv &&
        iof != io_format::json &&
        iof != io_format::csv2 &&
        iof != io_format::binary &&
        iof != io_format::pretty_prt)
        throw NotImplemented("write(): This io_format is not implemented");

    bool    need_pre_comma = false;
    long    end_row = indices_.size();
    long    start_row = 0;

    if (params.max_recs >= 0)
        end_row = std::min(end_row, params.max_recs);
    else
        start_row = std::max(long(0), end_row + params.max_recs);

    if (iof != io_format::binary)  o.precision(params.precision);

    if (iof == io_format::json)  {
        o << "{\n";
        if (! params.columns_only) [[likely]]  {
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
        if (! params.columns_only) [[likely]]  {
            _write_csv_df_header_<S, IndexType>(o,
                                                DF_INDEX_COL_NAME,
                                                end_row - start_row) << ':';

            if constexpr (std::same_as<IndexType, DateTime>)  {
                for (long i = start_row; i < end_row; ++i)
                    _write_csv_df_index_(o, indices_[i], DT_FORMAT::DT_TM2)
                        << params.delim;
            }
            else  {
                for (long i = start_row; i < end_row; ++i)
                    _write_csv_df_index_(o, indices_[i]) << params.delim;
            }
            o << '\n';
        }

        const SpinGuard guard(lock_);

        for (const auto &[name, idx] : column_list_) [[likely]]  {
            print_csv_functor_<Ts ...>  functor (name.c_str(),
                                                 o,
                                                 start_row,
                                                 end_row,
                                                 params.delim);

            data_[idx].change(functor);
        }
    }
    else if (iof == io_format::csv2)  {
        if (! params.columns_only) [[likely]]  {
            if constexpr (std::same_as<IndexType, DateTime>)  {
                _write_csv_df_header_<S, IndexType>(
                    o,
                    DF_INDEX_COL_NAME,
                    end_row - start_row,
                    _dtformat_str_.at(params.dt_format));
            }
            else  {
                _write_csv_df_header_<S, IndexType>(o,
                                                    DF_INDEX_COL_NAME,
                                                    end_row - start_row);
            }
            need_pre_comma = true;
        }

        const SpinGuard guard_1(lock_);

        for (const auto &[name, idx] : column_list_) [[likely]]  {
            if (need_pre_comma)  o << params.delim;
            else  need_pre_comma = true;
            print_csv2_header_functor_<S, Ts ...>   functor(
                name.c_str(), o, end_row - start_row, params.dt_format);

            data_[idx].change(functor);
        }
        o << '\n';

        need_pre_comma = false;

        const SpinGuard guard_2(lock_);

        for (long i = start_row; i < end_row; ++i)  {
            size_type   count = 0;

            if (! params.columns_only) [[likely]]  {
                if constexpr (std::same_as<IndexType, DateTime>)
                    _write_csv_df_index_(o, indices_[i], params.dt_format);
                else
                    o << indices_[i];

                need_pre_comma = true;
                count += 1;
            }

            for (auto citer = column_list_.begin();
                 citer != column_list_.end(); ++citer, ++count)  {
                print_csv2_data_functor_<S, Ts ...>  functor (
                    i, o, params.dt_format);

                if (need_pre_comma && count > 0)  o << params.delim;
                else  need_pre_comma = true;
                data_[citer->second].change(functor);
            }
            o << '\n';
        }
    }
    else if (iof == io_format::binary)  {
        const auto  ed = get_system_endian();

        o.write(reinterpret_cast<const char *>(&ed), sizeof(ed));

        const uint16_t  col_num = static_cast<uint16_t>(column_list_.size());

        o.write(reinterpret_cast<const char *>(&col_num), sizeof(col_num));

        if (! params.columns_only) [[likely]]  {
            print_binary_functor_<Ts ...>   idx_functor (
                DF_INDEX_COL_NAME, o, start_row, end_row);

            idx_functor(indices_);
        }

        const SpinGuard guard(lock_);

        for (const auto &[name, idx] : column_list_) [[likely]]  {
            print_binary_functor_<Ts ...>   functor (name.c_str(),
                                                     o,
                                                     start_row,
                                                     end_row);

            data_[idx].change(functor);
        }
    }
    else if (iof == io_format::pretty_prt)  {
        const std::ios_base::fmtflags   original_f { o.flags() };

        std::vector<std::vector<std::string>>   data;
        std::vector<std::string>                col_names;

        data.reserve(column_list_.size() + (params.columns_only ? 0 : 1));
        col_names.reserve(data.capacity());

        if (! params.columns_only)  {
            col_names.push_back(DF_INDEX_COL_NAME);
            data.push_back(_stringfy_(indices_,
                                      params.dt_format,
                                      start_row,
                                      end_row));
        }

        {
            const SpinGuard guard(lock_);

            for (const auto &[name, idx] : column_list_) [[likely]]  {
                stringfy_functor_<Ts ...>   functor (data,
                                                     col_names,
                                                     name.c_str(),
                                                     params.dt_format,
                                                     start_row,
                                                     end_row);

                data_[idx].change(functor);
            }
        }

        const auto  widths { _get_max_string_len_(data) };
        const auto  num_rows {
            std::min(params.max_recs < 0
                         ? long(indices_.size())
                         : params.max_recs,
                     long(indices_.size()))
        };
        const auto  gutter_width {
            num_rows > 0
                ? static_cast<size_type>(std::ceil(std::log10(num_rows))) + 1
                : 1
        };

        o << std::boolalpha;
        o << _get_space_(gutter_width);

        const long  num_columns { long(col_names.size()) };

        for (long i = 0; i < num_columns; ++i)  {
            const auto  &name { col_names[i] };
            const auto  width { std::max(widths[i], name.size()) };

            // o << "| " << std::setw(width) << name << ' ';
            o << "| " << std::format("{:^{}}", name, width) << ' ';
        }
        o << '\n';

        o << _get_horz_rule_(gutter_width);
        for (long i = 0; i < num_columns; ++i) {
            const auto  width { std::max(widths[i], col_names[i].size()) };

            o << '|' << _get_horz_rule_(width + 2);
        }
        o << '\n';

        const std::string   blank { " " };

        for (long row = 0; row < num_rows; ++row) {
            o << std::setw(gutter_width) << row;
            for (long col = 0; col < num_columns; ++col) {
                const std::string   &datum {
                    (row < long(data[col].size())) ? data[col][row] : blank
                };
                const auto          width {
                    std::max(widths[col], col_names[col].size())
	            };

                o << "| " << std::setw(width) << datum << ' ';
            }
            o << '\n';
        }

        o.flags(original_f);
    }

    if (iof == io_format::json)
        o << "\n}";
    // if (iof != io_format::binary)
    //     o << std::endl;
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
std::future<bool> DataFrame<I, H>::
write_async (const char *file_name,
             io_format iof,
             const WriteParams<> params) const  {

    return (thr_pool_.dispatch(
                true,
                [file_name, iof, params, this] () -> bool  {
                    return (this->write<Ts ...>(
                        file_name,
                        iof,
                        std::forward<const WriteParams<>>(params)));
                }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ... Ts>
std::future<bool> DataFrame<I, H>::
write_async (S &o, io_format iof, const WriteParams<> params) const  {

    return (thr_pool_.dispatch(
                true,
                [&o, iof, params, this] () -> bool  {
                    return (this->write<S, Ts ...>(
                        o,
                        iof,
                        std::forward<const WriteParams<>>(params)));
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

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
std::future<std::string> DataFrame<I, H>::
serialize_async () const  {

    return (thr_pool_.dispatch(true, &DataFrame::serialize<Ts ...>, this));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
