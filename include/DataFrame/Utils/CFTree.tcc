// Hossein Moein
// January 17, 2026
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

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
CF<T>::CF(value_type value) : n(1), ls(value), ss(value * value)  {  }

// ----------------------------------------------------------------------------

template<typename T>
void
CF<T>::merge(const CF &rhs)  {

    n += rhs.n;
    ls += rhs.ls;
    ss += rhs.ss;
}

// ----------------------------------------------------------------------------

template<typename T>
void
CF<T>::add_value(value_type value)  {

    n += 1;
    ls += value;
    ss += value * value;
}

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::value_type
CF<T>::centroid() const  { return (n > 0 ? ls / T(n) : T(0)); }

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::value_type
CF<T>::variance() const  {

    if (n == 0)  return (0);

    const value_type    mean { centroid() };

    return (std::max(T(0), ss / T(n) - (mean * mean)));
}

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::value_type
CF<T>::radius() const  { return (std::sqrt(variance())); }

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::value_type
CF<T>::diameter() const  {

    if (n <= 1)  return (0);

    const value_type    mean { centroid() };

    return (std::sqrt(
                std::max(T(0), T(2) * T(n) * (ss / T(n) - (mean * mean)))));
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template<typename T>
CFTree<T>::CFTree(value_type t, size_type max_e, dist_func_t &&f)
    : threshold_(t), max_entries_(max_e), dist_func_(f)  {  }

// ----------------------------------------------------------------------------

template<typename T>
bool
CFTree<T>::insert(const value_type &point)  {

    if (entries_.empty())  {
        CF_t    new_cf;  // First point - create new entry

        new_cf.add_value(point);
        entries_.reserve(128);
        entries_.push_back(new_cf);
        return (true);
    }

    // Find closest entry
    //
    size_type       closest_idx { -1 };
    double          min_dist { std::numeric_limits<double>::max() };
    const size_type sz { size_type(entries_.size()) };

    for (size_type i { 0 }; i < sz; ++i)  {
        const value_type    centroid { entries_[i].centroid() };
        const double        dist { dist_func_(point, centroid) };

        if (dist < min_dist)  {
            min_dist = dist;
            closest_idx = i;
        }
    }

    // Try to absorb point into closest entry
    //
    CF_t    test_cf { entries_[closest_idx] };

    test_cf.add_value(point);
    if (test_cf.radius() <= threshold_)  {  // Point fits within threshold
        entries_[closest_idx] = test_cf;
        return (true);
    }

    // Create new entry if space available
    // sz is still valid, since nothing was added to entries_
    //
    if (sz < max_entries_)  {
        CF_t    new_cf;

        new_cf.add_value(point);
        entries_.push_back(new_cf);
        return (true);
    }

    return (false);  // Tree is full, rebuild with larger threshold
}

// ----------------------------------------------------------------------------

template<typename T>
const std::vector<typename CFTree<T>::CF_t> &
CFTree<T>::get_entries() const  { return (entries_); }

// ----------------------------------------------------------------------------

template<typename T>
std::vector<typename CFTree<T>::CF_t> &
CFTree<T>::get_entries()  { return (entries_); }

// ----------------------------------------------------------------------------

template<typename T>
typename CFTree<T>::size_type
CFTree<T>::size() const  { return (entries_.size()); }

// ----------------------------------------------------------------------------

template<typename T>
void
CFTree<T>::clear()  { entries_.clear(); }

// ----------------------------------------------------------------------------

template<typename T>
typename CFTree<T>::value_type
CFTree<T>::get_threshold() const  { return (threshold_); }

// ----------------------------------------------------------------------------

template<typename T>
void
CFTree<T>::set_threshold(value_type t)  { threshold_ = t; }

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
