// Hossein Moein
// July 23, 2026
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

#include <DataFrame/Utils/FixedSizeString.h>

#include <ios>
#include <vector>

// ----------------------------------------------------------------------------

// Persisted parser state for resumable io_format::binary chunked reads,
// shared between DataFrame::read_binary_() (in DataFrame_read.tcc) and
// ChunkedReader (in ChunkedReader.h).
//
// io_format::binary's on-disk layout is column-major: an endianness
// marker and column count, then, for the index (if present) and each
// data column in turn, a name, a type tag, an 8-byte row count, and that
// column's raw data. Critically, every existing _read_binary_*_() helper
// always consumes a column's *entire* on-disk block in one call --
// reading the row-count header, seeking past any rows before the
// requested window, reading the window, and then seeking past whatever
// rows remain after it -- regardless of how small the requested window
// was. That's what lets the existing column loop move on to the next
// column's header unconditionally. It also means that, unlike csv2,
// simply continuing from wherever the stream is positioned after one
// chunk does NOT work for a second chunk of the same column: the stream
// is already sitting at the start of the *next* column's header.
//
// So resuming a later chunk means seeking back to each column's own
// start (right at its row-count header) and re-reading from there with
// an updated row offset. This is what BinaryReadState exists to make
// possible: it records, once (on the very first chunk), each column's
// absolute byte offset, name, and type tag; and it tracks, cumulatively,
// how many rows have been delivered so far (the same figure applies to
// every column, since all of a DataFrame's columns are always the same
// length).
//
// This does mean ChunkedReader requires a genuinely seekable
// stream (an absolute std::ios_base::beg seek) when used with
// io_format::binary -- unlike io_format::csv2, which only ever needs to
// read forward. In practice this is essentially always the case (a
// binary DataFrame file being read from disk).

// ----------------------------------------------------------------------------

namespace hmdf
{

// One column's persisted identity + seek anchor.
//
struct  BinaryColumnState  {

    String64        col_name { };
    String32        col_type { };

    // Absolute offset of this column's row-count header -- seek here
    // (std::ios_base::beg) before re-reading this column on any chunk after
    // the first.
    //
    std::streamoff  data_offset { 0 };
};

// ----------------------------------------------------------------------------

struct  BinaryReadState  {

    bool                            header_read { false };
    bool                            needs_flipping { false };
    bool                            has_index { false };
    BinaryColumnState               index_col { };
    std::vector<BinaryColumnState>  data_cols { };

    // Total row count, discovered once (from whichever column is
    // encountered first -- the index, if present, otherwise the first
    // data column) and reused afterward to know when the source is
    // exhausted.
    //
    std::size_t                     total_rows { 0 };

    // Cumulative number of rows already delivered to the caller, across
    // all next_chunk() calls sharing this state so far.
    //
    std::size_t                     rows_consumed { 0 };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
