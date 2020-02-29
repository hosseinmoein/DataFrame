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

template<typename I, typename H>
template<typename S, typename ...Ts>
bool DataFrame<I, H>::
write (S &o, bool values_only, io_format iof) const  {

    if (iof != io_format::csv && iof != io_format::json)
        throw NotImplemented("write(): This io_format is not implemented");

    if (iof == io_format::json)
        o << "{\n";

    bool    need_pre_comma = false;

    if (! values_only)  {
        if (iof == io_format::json)
            o << "\"INDEX\":{\"N\":" << indices_.size() << ',';
        else
            o << "INDEX:" << indices_.size() << ':';

        if (typeid(IndexType) == typeid(float))
            o << (iof == io_format::csv ? "<float>:" : "\"T\":\"float\",");
        else if (typeid(IndexType) == typeid(double))
            o << (iof == io_format::csv ? "<double>:" : "\"T\":\"double\",");
        else if (typeid(IndexType) == typeid(long double))
            o << (iof == io_format::csv
                      ? "<longdouble>:"
                      : "\"T\":\"longdouble\",");
        else if (typeid(IndexType) == typeid(short int))
            o << (iof == io_format::csv ? "<short>:" : "\"T\":\"short\",");
        else if (typeid(IndexType) == typeid(unsigned short int))
            o << (iof == io_format::csv ? "<ushort>:" : "\"T\":\"ushort\",");
        else if (typeid(IndexType) == typeid(int))
            o << (iof == io_format::csv ? "<int>:" : "\"T\":\"int\",");
        else if (typeid(IndexType) == typeid(unsigned int))
            o << (iof == io_format::csv ? "<uint>:" : "\"T\":\"uint\",");
        else if (typeid(IndexType) == typeid(long int))
            o << (iof == io_format::csv ? "<long>:" : "\"T\":\"long\",");
        else if (typeid(IndexType) == typeid(long long int))
            o << (iof == io_format::csv
                      ? "<longlong>:"
                      : "\"T\":\"longlong\",");
        else if (typeid(IndexType) == typeid(unsigned long int))
            o << (iof == io_format::csv ? "<ulong>:" : "\"T\":\"ulong\",");
        else if (typeid(IndexType) == typeid(unsigned long long int))
            o << (iof == io_format::csv
                      ? "<ulonglong>:"
                      : "\"T\":\"ulonglong\",");
        else if (typeid(IndexType) == typeid(std::string))
            o << (iof == io_format::csv ? "<string>:" : "\"T\":\"string\",");
        else if (typeid(IndexType) == typeid(bool))
            o << (iof == io_format::csv ? "<bool>:" : "\"T\":\"bool\",");
        else if (typeid(IndexType) == typeid(DateTime))
            o << (iof == io_format::csv
                      ? "<DateTime>:"
                      : "\"T\":\"DateTime\",");
        else
            o << (iof == io_format::csv ? "<N/A>:" : "\"T\":\"N/A\",");

        if (iof == io_format::json)  {
            o << "\"D\":[";
            if (! indices_.empty())  {
                _write_json_df_index_(o, indices_[0]);
                for (size_type i = 1; i < indices_.size(); ++i)  {
                    o << ',';
                    _write_json_df_index_(o, indices_[i]);
                }
            }
            o << "]}";
            need_pre_comma = true;
        }
        else  {
            for (size_type i = 0; i < indices_.size(); ++i)
                _write_csv_df_index_(o, indices_[i]) << ',';
            o << '\n';
        }
    }

    if (iof == io_format::json)  {
        for (const auto &iter : column_tb_)  {
            print_json_functor_<Ts ...> functor (iter.first.c_str(),
                                                 values_only,
                                                 need_pre_comma,
                                                 o);

            data_[iter.second].change(functor);
            need_pre_comma = true;
        }
    }
    else {
        for (const auto &iter : column_tb_)  {
            print_csv_functor_<Ts ...>  functor (iter.first.c_str(),
                                                 values_only,
                                                 o);

            data_[iter.second].change(functor);
        }
    }

    if (iof == io_format::json)
        o << "\n}";
    o << std::endl;
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ...Ts>
std::future<bool> DataFrame<I, H>::
write_async (S &o, bool values_only, io_format iof) const  {

    return (std::async(std::launch::async,
                       &DataFrame::write<S, Ts ...>,
                       this,
                       std::ref(o),
                       values_only,
                       iof));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
