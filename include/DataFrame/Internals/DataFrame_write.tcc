// Hossein Moein
// July 24, 2019
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

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H, typename CLT>
template<typename S, typename ...Ts>
bool DataFrame<I, H, CLT>::write (S &o, io_format iof) const  {

    if (iof != io_format::csv &&
        iof != io_format::json &&
        iof != io_format::csv2)
        throw NotImplemented("write(): This io_format is not implemented");

    if (iof == io_format::json)
        o << "{\n";

    bool            need_pre_comma = false;
    const size_type index_s = indices_.size();

    if (iof == io_format::json)
        _write_json_df_header_<S, IndexType>(o, DF_INDEX_COL_NAME, index_s);
    else if (iof == io_format::csv)
        _write_csv2_df_header_<S, IndexType>
            (o, DF_INDEX_COL_NAME, index_s, ':');
    else if (iof == io_format::csv2)
        _write_csv2_df_header_<S, IndexType>
            (o, DF_INDEX_COL_NAME, index_s, '\0');

    if (iof == io_format::json)  {
        o << "\"D\":[";
        if (index_s != 0)  {
            _write_json_df_index_(o, indices_[0]);
            for (size_type i = 1; i < index_s; ++i)  {
                o << ',';
                _write_json_df_index_(o, indices_[i]);
            }
        }
        o << "]}";
        need_pre_comma = true;
    }
    else if (iof == io_format::csv)  {
        for (size_type i = 0; i < index_s; ++i)
            _write_csv_df_index_(o, indices_[i]) << ',';
        o << '\n';
    }
    else if (iof == io_format::csv2)  {
        for (const auto &iter : column_tb_)  {
            o << ',';
            print_csv2_header_functor_<S, Ts ...>   functor(
                iter.first.c_str(), o);

            data_[iter.second].change(functor);
        }
        o << '\n';
    }

    if (iof == io_format::json)  {
        for (const auto &iter : column_tb_)  {
            print_json_functor_<Ts ...> functor (iter.first.c_str(),
                                                 need_pre_comma,
                                                 o);

            data_[iter.second].change(functor);
            need_pre_comma = true;
        }
    }
    else if (iof == io_format::csv)  {
        for (const auto &iter : column_tb_)  {
            print_csv_functor_<Ts ...>  functor (iter.first.c_str(), o);

            data_[iter.second].change(functor);
        }
    }
    else if (iof == io_format::csv2)  {
        for (size_type i = 0; i < index_s; ++i)  {
            o << indices_[i];
            for (auto citer = column_tb_.begin();
                 citer != column_tb_.end(); ++citer)  {
                print_csv2_data_functor_<S, Ts ...>  functor (i, o);

                o << ',';
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

template<typename I, typename H, typename CLT>
template<typename S, typename ...Ts>
std::future<bool> DataFrame<I, H, CLT>::
write_async (S &o, io_format iof) const  {

    return (std::async(std::launch::async,
                       &DataFrame::write<S, Ts ...>,
                       this,
                       std::ref(o),
                       iof));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
