// Hossein Moein
// July 22, 2026
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

#include <DataFrame/Utils/IO/ChunkedReader.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename S, typename DF>
ChunkedReader<S, DF>::
ChunkedReader(S &stream, io_format iof, ReadParams params)
    : stream_(stream), iof_(iof), params_(params)  {

    if (iof_ != io_format::csv2 && iof_ != io_format::binary)
        throw NotImplemented(
            "ChunkedReader: Currently, only io_format::csv2 "
            "and io_format::binary are supported for resumable/"
            "chunked reads");
}

// ----------------------------------------------------------------------------

template<typename S, typename DF>
bool ChunkedReader<S, DF>::next_chunk(DF &out, size_type chunk_size)  {

    if (is_eof_)  return (false);

    size_type   rows_read { 0 };

    if (iof_ == io_format::csv2)  {
        const size_type this_starting_row =
            first_call_ ? params_.starting_row : 0;

        rows_read =
            out.template read_csv2_<S>(stream_,
                                       params_.columns_only,
                                       this_starting_row,
                                       chunk_size,
                                       params_.skip_first_line,
                                       params_.schema,
                                       params_.delim,
                                       &state_csv2_);
    }
    else  {  // io_format::binary
        // Starting_row is only actually consulted by read_binary_() on
        // the very first call (every call after that uses its own
        // persisted, cumulative row count instead -- see
        // BinaryReadState/read_binary_()), so it's fine to pass
        // params_.starting_row unconditionally here.
        //
        rows_read =
            out.template read_binary_<S>(stream_,
                                         params_.columns_only,
                                         params_.starting_row,
                                         chunk_size,
                                         &state_bin_);
    }

    first_call_ = false;

    if (rows_read == 0)  {
        is_eof_ = true;
        return (false);
    }

    // For io_format::csv2, the stream may have hit end-of-file while
    // still yielding a final, partial chunk -- deliver that chunk now
    // (return true) and report eof on the *next* call. (For
    // io_format::binary, exhaustion is already fully captured by
    // rows_read == 0 above, via BinaryReadState's own row-count
    // bookkeeping, so no separate check is needed here.)
    //
    if (iof_ == io_format::csv2)  {
        if constexpr (std::same_as<S, std::FILE *>)  {
            if (std::feof(stream_))  is_eof_ = true;
        }
        else  {
            if (stream_.eof())  is_eof_ = true;
        }
    }

    return (true);
}

// ----------------------------------------------------------------------------

template<typename S, typename DF>
bool ChunkedReader<S, DF>::is_eof() const noexcept  { return (is_eof_); }

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
