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

#pragma once

#include <DataFrame/Utils/IO/CSV2State.h>
#include <DataFrame/Utils/IO/BinaryState.h>

// ----------------------------------------------------------------------------

// ChunkedReader is a resumable cursor over an io_format::csv2 or
// io_format::binary source (an open std::istream, std::FILE *, or
// similar). Repeated calls to next_chunk() each pick up exactly where the
// previous call left off: the header/column-spec is parsed once, and
// there is no re-scanning of previously consumed rows.
//
// This is different from calling DataFrame::read() several times with
// increasing ReadParams::starting_row values. That approach re-opens (or
// re-reads from the beginning of) the source on every single call. For
// io_format::csv2 that also means re-scanning and discarding every row
// before starting_row each time -- reading a window at offset 900 costs
// 900 rows of re-scanning, every time.
//
// The two formats are resumable in different ways, reflecting how
// differently they're laid out on disk/in the stream:
//
//   - io_format::csv2 is row-oriented and line-delimited. Its cursor is
//     simply "wherever the stream currently is" -- next_chunk() never
//     seeks, it just keeps reading forward. This means it even works
//     over non-seekable sources (pipes, sockets).
//
//   - io_format::binary is column-oriented: all of the index's data is
//     written contiguously, then all of column A's, then all of column
//     B's, etc. Every existing binary-column read helper always consumes
//     a column's *entire* on-disk block in one call (header, requested
//     window, then a skip past whatever wasn't requested) so it can move
//     on to the next column's header -- so simply continuing from
//     wherever the stream is positioned does NOT work for a second chunk
//     of the same column (the stream is already at the *next* column's
//     header). Instead, next_chunk() seeks back to each column's own
//     recorded start offset before re-reading it. This means
//     ChunkedReader requires a genuinely seekable stream
//     (absolute std::ios_base::beg seeks) when used with
//     io_format::binary -- in practice essentially always true (reading
//     a binary DataFrame file from disk).
//
// io_format::csv/json don't support windowed/chunked reads at all yet
// (DataFrame::read() throws NotImplemented for them);
//
// Example (identical usage for either supported format):
//
//     using cr_t = ChunkedReader<std::ifstream, StdDataFrame<unsigned long>>;
//
//     std::ifstream               stream("huge_file.csv2");
//     cr_t                        reader(stream, io_format::csv2);
//     StdDataFrame<unsigned long> chunk;
//
//     while (reader.next_chunk(chunk, 100000))  {
//         ... process chunk (e.g. accumulate a running statistic) ...
//     }
//     On EOF, if the total row count wasn't an exact multiple of the
//     chunk size, the final next_chunk() call already delivered the
//     last, smaller chunk before returning false.

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename S, typename DF>
class   ChunkedReader  {

public:

    using size_type = typename DF::size_type;

    ChunkedReader() = delete;
    ChunkedReader(const ChunkedReader &) = delete;
    ChunkedReader &operator = (const ChunkedReader &) = delete;

    // stream:
    //   An already-open source, positioned at the start of the data to be
    //   read (e.g. right after opening a file, or right after any bytes
    //   the caller has already consumed for its own purposes). It must
    //   remain valid for the ChunkedReader's lifetime.
    //   ChunkedReader does not open, close, or own it. For
    //   io_format::binary it must additionally be seekable (support
    //   absolute std::ios_base::beg seeks) -- see the file-level comment
    //   above.
    // iof:
    //   Must be io_format::csv2 or io_format::binary -- see the
    //   file-level comment above for why the other formats aren't (yet)
    //   supported here.
    // params:
    //   The same ReadParams used by DataFrame::read().
    //   - params.starting_row, if nonzero, is honored once, as an initial
    //     offset applied before the very first chunk (matching what
    //     DataFrame::read() would do with the same value).
    //   - params.num_rows is ignored: each next_chunk() call supplies its
    //     own chunk_size instead.
    //   - columns_only / skip_first_line / schema / delim behave exactly
    //     as they do for DataFrame::read() (skip_first_line/schema/delim
    //     only matter for io_format::csv2).
    //
    ChunkedReader(S &stream, io_format iof, ReadParams params = { });

    // Reads up to chunk_size more data rows into out, continuing from
    // wherever the previous next_chunk() call (or, on the first call,
    // params.starting_row) left off.
    //
    // Returns true if any rows were read into out. Returns false once the
    // source is exhausted -- note the source can become exhausted in the
    // middle of delivering a final, smaller-than-chunk_size chunk, in
    // which case that last chunk is still fully written to out and this
    // function returns true; the *next* call after that returns false
    // with out left untouched (see is_eof()).
    //
    // out is populated exactly the way DataFrame::read() would populate
    // it for this slice of rows: any prior content of out is discarded.
    //
    bool next_chunk(DF &out, size_type chunk_size);

    // True once the source has been fully consumed (i.e. once
    // next_chunk() has returned false).
    //
    [[nodiscard]] bool is_eof() const noexcept;

private:

    S               &stream_;
    io_format       iof_;
    ReadParams      params_;
    CSV2ReadState   state_csv2_ { };
    BinaryReadState state_bin_ { };
    bool            first_call_ { true };
    bool            is_eof_ { false };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/IO/ChunkedReader.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
