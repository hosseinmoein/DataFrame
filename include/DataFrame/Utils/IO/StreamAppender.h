// Hossein Moein
// July 29, 2026
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

#include <DataFrame/DataFrame.h>

#include <type_traits>
#include <utility>

// ----------------------------------------------------------------------------

// StreamAppender is the live feed counterpart to ChunkedReader and
// StreamWriter. Those two both assume a bounded source or destination --
// something with a definite end (a file, or however many chunks an ETL job
// produces). StreamAppender is for the opposite case: a source that never
// ends (a market-data handler, a socket, a queue) that you want to keep
// feeding into a DataFrame while other code (on the same or another
// thread) queries it.
//
// It's a thin, locking wrapper around a DataFrame you already own -- it
// does not read from or write to any external stream itself. Once you
// start feeding a DataFrame through a StreamAppender, every subsequent
// access to that DataFrame (pushing new rows, and reading/visiting it)
// should go through this StreamAppender (push_row() / push_chunk() /
// with_lock()) rather than touching the underlying DataFrame directly --
// StreamAppender's lock only protects access that actually goes through
// it.
//
// Two ingestion paths are provided:
//   - push_row(): a thin, lock-guarded wrapper around
//     DataFrame::append_row() for one row at a time. Convenient, but pays
//     the lock (and append_row()'s own, documented, per-call overhead)
//     every single row.
//   - push_chunk(): appends every row of a small DataFrame in one locked
//     operation, via DataFrame::self_concat(). Prefer this over many
//     push_row() calls when your source naturally arrives in small
//     batches (most real feeds do) -- it takes the lock once per batch
//     instead of once per row.
//
// Optionally bounds memory with a sliding window: once the row count
// exceeds window_size by more than a small margin, the oldest rows are
// trimmed back down to window_size. Eviction works by removing rows whose
// index falls in a value range (see DataFrame::remove_data_by_idx()), so
// it assumes the index is unique and monotonically increasing -- true for
// the common case of a timestamp or sequence-counter index, but not
// guaranteed correct if the index has duplicates or arrives out of order.

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H = HeteroVector<0>>
class   StreamAppender  {

public:

    using DataFrameType = DataFrame<I, H>;
    using IndexType = typename DataFrameType::IndexType;
    using size_type = long;

    StreamAppender() = delete;
    StreamAppender(const StreamAppender &) = delete;
    StreamAppender &operator = (const StreamAppender &) = delete;

    // target:
    //   The DataFrame this StreamAppender manages. Must outlive the
    //   StreamAppender. See the file-level comment above about routing
    //   all further access through this StreamAppender.
    // window_size:
    //   0 (the default) means unbounded -- rows accumulate forever, and
    //   it is entirely up to the caller whether/when to trim. A nonzero
    //   value enables the sliding-window eviction described above.
    // eviction_margin:
    //   Only meaningful when window_size > 0. Eviction doesn't fire the
    //   instant the row count exceeds window_size; it waits until it
    //   exceeds window_size by this many extra rows, then trims back down
    //   to window_size in one go. This amortizes the O(row count) cost of
    //   eviction over many pushes instead of paying it on every single
    //   one. 0 means "trim back down to window_size on every push once
    //   over the limit" (simplest, but the most eviction calls).
    //
    explicit StreamAppender(DataFrameType &target,
                            size_type window_size = 0,
                            size_type eviction_margin = 0);

    // Pushes a single row. Thread-safe with respect to any other
    // push_row()/push_chunk()/with_lock() call on this same
    // StreamAppender.
    //
    // This is a thin, lock-guarded wrapper around
    // DataFrame::append_row() -- see that method's own documentation for
    // the {std::make_pair(col_name, value), ...} calling convention and
    // its documented per-call overhead. For higher-throughput ingestion,
    // prefer batching rows into a small DataFrame and calling
    // push_chunk() instead.
    //
    // If this StreamAppender has a window_size, eviction is checked (and
    // applied if needed) after the row is appended, using the actual
    // column value types (not the std::pair wrapper types) deduced from
    // args.
    //
    template<typename ... Ts>
    void push_row(IndexType index_val, Ts && ... args);

    // Appends every row of chunk onto the managed DataFrame in one
    // locked operation (via DataFrame::self_concat()). chunk's columns
    // are matched by name; add_new_columns controls whether columns
    // present in chunk but not in the managed DataFrame are created
    // (prepended with NaN for prior rows) or ignored -- see
    // self_concat()'s own documentation for details.
    //
    // Ts... must list the (actual, non-pair) types of chunk's data
    // columns, same as for self_concat()/remove_data_by_idx().
    //
    template<typename RHS_T, typename ... Ts>
    void push_chunk(const RHS_T &chunk, bool add_new_columns = true);

    // Runs func(target) with the lock held, where target is the managed
    // DataFrame -- e.g. to safely run a visitor, take a snapshot, or copy
    // out a window of rows, without any concurrent push_row()/
    // push_chunk() call landing mid-read.
    //
    // func should be quick: it blocks every push_row()/push_chunk() call
    // on this StreamAppender for as long as it runs. The lock is
    // reentrant, so it's safe for func to itself call push_row()/
    // push_chunk()/with_lock() again from the same thread.
    //
    template<typename F>
    decltype(auto) with_lock(F &&func);

    template<typename F>
    decltype(auto) with_lock(F &&func) const;

    // Current row count, taken under the lock.
    //
    [[nodiscard]] size_type size() const;

    [[nodiscard]] bool is_bounded() const noexcept;
    [[nodiscard]] size_type window_size() const noexcept;

private:

    // Must be called with lock_ already held. Ts... are the managed
    // DataFrame's actual data-column value types.
    //
    template<typename ... Ts>
    void trim_if_needed_();

    DataFrameType   &target_;
    size_type       window_size_;
    size_type       eviction_margin_;
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/IO/StreamAppender.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
