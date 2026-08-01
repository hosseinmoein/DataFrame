// Hossein Moein
// July 27, 2026
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

#include <DataFrame/Utils/IO/StreamWriter.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename S, typename DF>
StreamWriter<S, DF>::
StreamWriter(stream_t &stream, io_format iof, wparam_t params)
    : stream_(stream), iof_(iof), params_(params)  {

    if (iof_ != io_format::csv2)
        throw NotImplemented(
            "StreamWriter: Currently, only io_format::csv2 is supported "
            "for incremental/streaming writes");
}

// ----------------------------------------------------------------------------

template<typename S, typename DF>
StreamWriter<S, DF>::~StreamWriter()  { close(); }

// ----------------------------------------------------------------------------

template<typename S, typename DF>
template<typename ... Ts>
bool StreamWriter<S, DF>::write_chunk(const DF &chunk_df)  {

    if (closed_)
        throw DataFrameError(
            "StreamWriter::write_chunk(): ERROR: This StreamWriter has "
            "already been closed");

    wparam_t    this_call_params { params_ };

    this_call_params.write_header = first_call_;
    first_call_ = false;

    // write()'s internal helper functions always return std::ostream&
    // (the base stream type) regardless of the concrete stream type, so
    // write<S, Ts...> only compiles when S is std::ostream itself. This
    // matches the convention DataFrame::write(const char *, ...) already
    // uses internally: instantiate with std::ostream, and let stream_
    // (whatever concrete S the caller chose, e.g. std::ofstream) upcast
    // to it at the call.
    //
    return (chunk_df.template write<std::ostream, Ts ...>(stream_, iof_,
                                                          this_call_params));
}

// ----------------------------------------------------------------------------

template<typename S, typename DF>
void StreamWriter<S, DF>::close()  {

    if (closed_)  return;

    if constexpr (requires (stream_t &s) { s.flush(); })
        stream_.flush();

    closed_ = true;
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
