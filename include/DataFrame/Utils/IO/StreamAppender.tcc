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

#include <DataFrame/Utils/IO/StreamAppender.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
StreamAppender<I, H>::
StreamAppender(DataFrameType &target,
              size_type window_size,
              size_type eviction_margin)
    : target_(target),
      window_size_(window_size),
      eviction_margin_(eviction_margin)  {  }

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void StreamAppender<I, H>::push_row(IndexType index_val, Ts && ... args)  {

    const SpinGuard guard { target_.get_lock() };

    // append_row()/append_column() require each column to already exist;
    // create_column() is idempotent (a no-op if the column is already
    // there), so doing this first makes push_row() safe to call against
    // a brand new, empty DataFrame as well as one that's already been
    // populated.
    //
    (target_.template create_column<typename std::decay_t<Ts>::second_type>
        (args.first), ...);

    target_.append_row(&index_val, std::forward<Ts>(args) ...);
    if (window_size_ > 0)
        trim_if_needed_<typename std::decay_t<Ts>::second_type ...>();
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... Ts>
void StreamAppender<I, H>::
push_chunk(const RHS_T &chunk, bool add_new_columns)  {

    const SpinGuard guard { target_.get_lock() };

    target_.template self_concat<RHS_T, Ts ...>(chunk, add_new_columns);
    if (window_size_ > 0)
        trim_if_needed_<Ts ...>();
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename F>
decltype(auto) StreamAppender<I, H>::with_lock(F &&func)  {

    const SpinGuard guard { target_.get_lock() };

    return (func(target_));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename F>
decltype(auto) StreamAppender<I, H>::with_lock(F &&func) const  {

    const SpinGuard guard { target_.get_lock() };

    return (func(target_));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename StreamAppender<I, H>::size_type
StreamAppender<I, H>::size() const  {

    const SpinGuard guard { target_.get_lock() };

    return (target_.get_index().size());
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void StreamAppender<I, H>::trim_if_needed_()  {

    // Caller (push_row()/push_chunk()) already holds lock_.
    //
    const size_type current_size { size_type(target_.get_index().size()) };

    if (current_size <= window_size_ + eviction_margin_)  return;

    // Assumes a unique, monotonically increasing index -- see the
    // file-level and constructor documentation. Removes every row whose
    // index falls in [front, the value right before the first row we
    // want to keep], i.e. exactly the oldest
    // (current_size - window_size_) rows in the well-behaved case.
    //
    const auto      &idx_vec { target_.get_index() };
    const size_type cutoff_count { current_size - window_size_ };

    if (cutoff_count > 1)
        target_.template remove_data_by_idx<Ts ...>(
            { idx_vec.front(), idx_vec[cutoff_count - 1] });
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
bool StreamAppender<I, H>::is_bounded() const noexcept {

    return (window_size_ > 0);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
typename StreamAppender<I, H>::size_type
StreamAppender<I, H>::window_size() const noexcept {

    return (window_size_);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
