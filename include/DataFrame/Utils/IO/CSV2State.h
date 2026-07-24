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

#include <DataFrame/DataFrameTypes.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <any>
#include <vector>

// ----------------------------------------------------------------------------

// This header factors out the parser state used by DataFrame::read_csv2_()
// so that state can optionally be persisted across multiple calls instead
// of being rebuilt from scratch every time. This is what lets ChunkedReader
// (see <ChunkedReader.h>) issue a sequence of next_chunk() calls that each
// pick up exactly where the previous one left off -- no re-parsing the header,
// no re-scanning rows already consumed -- rather than DataFrame::read()'s
// (starting_row, num_rows) windowing, which re-opens/re-scans the source from
// the // beginning on every call.
//
// _col_data_spec_ itself is unchanged from its previous, function-local
// definition inside DataFrame_read.tcc; it is only relocated here so both
// DataFrame_read.tcc and ChunkedReader.h can share the same type.

// ----------------------------------------------------------------------------

namespace hmdf
{

// A type-erased descriptor for a single column being parsed out of a CSV2
// stream: its eventual C++ type (behind std::any), its declared type-tag,
// its name, and its ordinal position in the file.
//
struct  _col_data_spec_  {

    std::any    col_vec { };
    file_dtypes type_spec { 0 };
    String64    col_name { };
    int         col_idx { -1 };

    template<typename V>
    _col_data_spec_(V cv,
                    file_dtypes ts,
                    const char *cn,
                    std::size_t rs,
                    int ci = -1)
        : col_vec(cv), type_spec(ts), col_name(cn), col_idx(ci)  {

        std::any_cast<V &>(col_vec).reserve(rs);
    }
};

// ----------------------------------------------------------------------------

// Note this deliberately uses a plain std::vector (not DataFrame's
// alignment-aware StlVecType alias) to hold the column-spec descriptors
// themselves. That alias only matters for the actual per-column numeric
// data buffers stashed inside each _col_data_spec_'s std::any (those are
// still built with the DataFrame's own StlVecType<T>). The outer
// vector-of-specs is just per-file bookkeeping (one entry per column, not per
// row), so using a plain std::vector here costs nothing and lets CSV2ReadState
// stay a simple, non-template type that any DataFrame<I, H> instantiation can
// hand a pointer to.
//
using CSV2SpecVec = std::vector<_col_data_spec_>;

// Bundles the pieces of DataFrame::read_csv2_()'s parsing state that need
// to survive between one chunk-read call and the next:
//   - header_read: whether the column header/schema has already been
//     parsed (so a resumed call skips straight to data rows).
//   - requested_col_count: the column count derived from the header/
//     user-supplied schema.
//   - spec_vec: the column descriptors themselves. Note that after each
//     chunk is handed off to a DataFrame (via load_column()'s moves), the
//     std::vector-typed data held inside each std::any is left empty (but
//     still correctly typed and reserved), ready to be refilled by the
//     next chunk-read call.
//
struct  CSV2ReadState  {

    bool         header_read { false };
    int          requested_col_count { 0 };
    CSV2SpecVec  spec_vec { };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
