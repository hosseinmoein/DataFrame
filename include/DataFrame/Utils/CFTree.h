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

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

// Clustering Feature for scalar values
//
template<typename T>
struct  CF  {

    using size_type = long;
    using feature_type = T;
    using value_type = double;

private:

    size_type       n_ { 0 };   // number of points
    feature_type    ls_ { };    // linear sum
    value_type      ss_ { 0 };  // sum of squares

public:

    explicit CF(size_type dimension = 1);

    // Add another CF to this CF
    //
    inline void
    merge(const CF &rhs);

    // Add a single value
    //
    inline void
    add_value(const feature_type &value);

    // Get centroid
    //
    inline feature_type
    centroid() const;

    // Get variance
    //
    inline value_type
    variance() const;

    // Get standard deviation (radius)
    //
    inline value_type
    radius() const;

    // Calculate diameter (range estimation)
    //
    inline value_type
    diameter() const;

    inline size_type
    size() const;
};

// ----------------------------------------------------------------------------

template<typename T>
class CFTree {

public:

    using size_type = long;
    using feature_type = T;
    using value_type = double;
    using dist_func_t =
        std::function<value_type(const feature_type &, const feature_type &)>;
    using CF_t = CF<feature_type>;

    explicit
    CFTree(double t,
           size_type dimen,
           dist_func_t &&f,
           size_type max_entries = 1000);
    CFTree() = default;
    CFTree(const CFTree &) = default;
    CFTree(CFTree &&) = default;
    CFTree &operator =(const CFTree &) = default;
    CFTree &operator =(CFTree &&) = default;
    ~CFTree() = default;

    // Insert a point into the tree
    //
    bool
    insert(const feature_type &point);

    // Get all CF entries
    //
    const std::vector<CF_t> &get_entries() const;
    std::vector<CF_t> &get_entries();
    size_type size() const;
    void clear();
    value_type get_threshold() const;
    void set_threshold(value_type t);

private:

    value_type          threshold_ { };
    size_type           dimension_ { };
    size_type           max_entries_ { };
    dist_func_t         dist_func_ {
        [](const T &, const T &) -> double { return (0); }
    };
    std::vector<CF_t>   entries_ { };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/CFTree.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
