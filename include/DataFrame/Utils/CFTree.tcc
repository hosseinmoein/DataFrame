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
#include <DataFrame/Utils/Concepts.h>

#include <type_traits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
CF<T>::CF(size_type dimension) : n_(0), ss_(0)  {

    if constexpr (std::is_arithmetic_v<feature_type>)  {
        ls_ = 0;
    }
    else  {
        if constexpr (Resizable<feature_type>)
            ls_.resize(dimension, 0);
    }
}

// ----------------------------------------------------------------------------

template<typename T>
void
CF<T>::merge(const CF &rhs)  {

    n_ += rhs.n_;
    ss_ += rhs.ss_;
    if constexpr (std::is_arithmetic_v<feature_type>)  {
        ls_ += rhs.ls_;
    }
    else  {
        for (size_type i { 0 }; i < size_type(ls_.size()); ++i)
            ls_[i] += rhs.ls_[i];
    }
}

// ----------------------------------------------------------------------------

template<typename T>
void
CF<T>::add_value(const feature_type &value)  {

    n_ += 1;
    if constexpr (std::is_arithmetic_v<feature_type>)  {
        ls_ += value;
        ss_ += value * value;
    }
    else  {
        for (size_type i { 0 }; i < size_type(value.size()); ++i)  {
            ls_[i] += value[i];
            ss_ += value[i] * value[i];
        }
    }
}

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::feature_type
CF<T>::centroid() const  {

    if constexpr (std::is_arithmetic_v<feature_type>)  {
        return (n_ > 0 ? ls_ / n_ : 0.0);
    }
    else  {
        feature_type    ret;

        if constexpr (Resizable<feature_type>)
            ret.resize(ls_.size(), 0);
        if (n_ > 0)  {
            for (size_type i { 0 }; i < size_type(ls_.size()); ++i)
                ret[i] = ls_[i] / n_;
        }
        return (ret);
    }
}

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::value_type
CF<T>::variance() const  {

    if (n_ == 0)  return (0);

    const feature_type  mean { centroid() };

    if constexpr (std::is_arithmetic_v<feature_type>)  {
        return (std::max(0.0, (ss_ / n_) - (mean * mean)));
    }
    else  {
        value_type  sum { 0 };

        for (size_type i { 0 }; i < size_type(mean.size()); ++i)
            sum += mean[i] * mean[i];
        return (std::max(0.0, (ss_ / n_) - sum));
    }
}

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::value_type
CF<T>::radius() const  { return (std::sqrt(variance())); }

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::value_type
CF<T>::diameter() const  {

    if (n_ <= 1)  return (0);

    const feature_type  mean { centroid() };

    if constexpr (std::is_arithmetic_v<feature_type>)  {
        return (std::sqrt(
                    std::max(0.0, 2.0 * n_ * (ss_ / n_ - (mean * mean)))));
    }
    else  {
        value_type  sum { 0 };

        for (size_type i { 0 }; i < mean.size(); ++i)
            sum += mean[i] * mean[i];
        return (std::sqrt(std::max(0.0, 2.0 * n_ * (ss_ / n_ - sum))));
    }
}

// ----------------------------------------------------------------------------

template<typename T>
typename CF<T>::size_type
CF<T>::size() const  { return (n_); }

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template<typename T>
CFTree<T>::CFTree(double t, size_type dimen, dist_func_t &&f, size_type max_e)
    : threshold_(t),
      dimension_(dimen),
      max_entries_(max_e),
      dist_func_(f)  {  }

// ----------------------------------------------------------------------------

template<typename T>
bool
CFTree<T>::insert(const feature_type &point)  {

    if (entries_.empty())  {
        CF_t    new_cf { dimension_ };  // First point - create new entry

        new_cf.add_value(point);
        entries_.reserve(128);
        entries_.push_back(new_cf);
        return (true);
    }

    // Find closest entry
    //
    size_type       closest_idx { -1 };
    value_type      min_dist { std::numeric_limits<value_type>::max() };
    const size_type sz { size_type(entries_.size()) };

    for (size_type i { 0 }; i < sz; ++i)  {
        const feature_type  centroid { entries_[i].centroid() };
        const value_type    dist { dist_func_(point, centroid) };

        if (dist < min_dist)  {
            min_dist = dist;
            closest_idx = i;
        }
    }

    // Try to absorb point into closest entry
    //
    CF_t    test_cf { dimension_ };

    test_cf.merge(entries_[closest_idx]);
    test_cf.add_value(point);
    if (test_cf.radius() <= threshold_)  {  // Point fits within threshold
        entries_[closest_idx] = test_cf;
        return (true);
    }

    // Create new entry if space available
    // sz is still valid, since nothing was added to entries_
    //
    if (sz < max_entries_)  {
        CF_t    new_cf { dimension_ };

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
