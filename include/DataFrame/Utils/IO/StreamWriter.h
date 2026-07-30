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

#pragma once

// ----------------------------------------------------------------------------

// StreamWriter is the write-side counterpart to ChunkedReader. It lets a
// caller build up an output incrementally, one chunk (a small DataFrame
// sharing the same schema every time) at a time, instead of needing the
// entire result in memory before calling DataFrame::write() once. This is
// useful for an ETL-style pipeline: read a huge source in chunks via
// ChunkedReader, transform each chunk, and write each transformed chunk
// out via StreamWriter, all in bounded memory throughout.
//
// Only io_format::csv2 is currently supported. Two format-specific reasons
// rule the others out for now:
//
//   - io_format::csv2's header line embeds a row count per column
//     (name:count:<type>), but that count is purely a read-side
//     .reserve() hint -- read_csv2_() never uses it to bound how much
//     data it actually reads, it just keeps reading lines until the
//     stream runs out (or its own num_rows limit is hit). That means the
//     header can safely be written once, using the very first chunk's
//     row count as that hint, and every later chunk can simply append
//     more data lines with no header and no footer required.
//
//   - io_format::binary has no such luxury: its on-disk layout is
//     column-major (all of the index's data, then all of column A's data,
//     then all of column B's, etc.), and each column's row count is
//     authoritative there -- read_binary_() uses it to know exactly how
//     many elements to read. Incremental writing would require either
//     patching each column's row-count field after the fact (which in
//     turn would require each new chunk's column-A data to land
//     immediately after the previous chunk's column-A data on disk --
//     impossible without rewriting everything after it, since column B's
//     data from the previous chunk is sitting in between) or buffering an
//     entire run's worth of data before writing anything (which defeats
//     the purpose of a bounded-memory streaming writer). Genuinely
//     streaming io_format::binary would need a different on-disk layout
//     (e.g. writing count-prefixed blocks per chunk, per column) -- a
//     breaking format change, out of scope here.
//
// Example:
//
//     using sw_t = StreamWriter<std::ofstream, StdDataFrame<unsigned long>>;
//
//     std::ofstream                stream("huge_output.csv2");
//     sw_t                         writer(stream, io_format::csv2);
//     StdDataFrame<unsigned long>  chunk;
//
//     for (...)  {
//         // ... build up chunk (same columns every time) ...
//         writer.write_chunk<double, int, std::string>(chunk);
//     }
//     writer.close();

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename S, typename DF>
class   StreamWriter  {

    using wparam_t = WriteParams<>;

public:

    using size_type = typename DF::size_type;
    using stream_t = S;

    StreamWriter() = delete;
    StreamWriter(const StreamWriter &) = delete;
    StreamWriter &operator = (const StreamWriter &) = delete;

    // stream:
    //   An already-open destination (e.g. a freshly-opened std::ofstream).
    //   It must remain valid for the StreamWriter's lifetime.
    //   StreamWriter does not open, close, or own it.
    // iof:
    //   Must be io_format::csv2 -- see the file-level comment above.
    // params:
    //   The same WriteParams used by DataFrame::write(). params.max_recs
    //   is honored per chunk (as it always has been for write()), not
    //   across the whole stream; params.write_header is managed
    //   internally by StreamWriter and any value the caller sets there is
    //   overridden.
    //
    StreamWriter(stream_t &stream, io_format iof, wparam_t params = { });

    ~StreamWriter();

    // Writes chunk_df's rows to the stream, appending after anything
    // already written via this StreamWriter. The very first call also
    // writes the csv2 header (derived from chunk_df's columns); every
    // call after that must be given a DataFrame with the same column
    // names, types (both the runtime column set and the Ts... template
    // pack), and order as the first call -- csv2 has no way to represent
    // a change of schema partway through a file.
    //
    template<typename ... Ts>
    bool write_chunk(const DF &chunk_df);

    // Flushes the underlying stream. There is currently nothing to
    // finalize/patch for io_format::csv2 (that's the whole point of
    // choosing a format with no footer and an advisory-only header row
    // count), but calling close() once done is still good practice --
    // in particular, it's what a future io_format that *does* need
    // finalization would hook into. After calling this, the StreamWriter
    // must not be used again. Safe to call more than once.
    //
    void close();

private:

    stream_t    &stream_;
    io_format   iof_;
    wparam_t    params_;
    bool        first_call_ { true };
    bool        closed_ { false };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/IO/StreamWriter.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
