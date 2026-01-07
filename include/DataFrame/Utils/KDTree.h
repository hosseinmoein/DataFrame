// Hossein Moein
// January 5, 2026
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
#include <array>
#include <cmath>
#include <functional>
#include <limits>
#include <queue>
#include <stack>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

// K-Dimensional (KD) Tree
//
template<typename T>
class   KDTree {

public:

    using value_type = T;
    using size_type = std::size_t;
    using point_t = std::vector<value_type>;
    using points_vec = std::vector<point_t>;

    static constexpr size_type  NULL_IDX {
        std::numeric_limits<size_type>::max()
    };

    explicit KDTree(size_type k);
    KDTree() = delete;
    KDTree(const KDTree &that) = default;
    KDTree(KDTree &&that) = default;
    KDTree &operator= (const KDTree &that) = delete;
    KDTree &operator= (KDTree &&that) = default;
    ~KDTree() = default;

    struct  Node {
        point_t     point { };
        size_type   left { NULL_IDX };
        size_type   right { NULL_IDX };

        explicit Node(const point_t &p) : point(p)  {  }
    };

    // Build tree from vector of points
    //
    inline void
    build(points_vec &points);

    // Find nearest_ neighbor
    //
    [[nodiscard]] point_t
    find_nearest(const point_t &target) const;

    // Find k nearest neighbors
    //
    [[nodiscard]] points_vec
    find_k_nearest(const point_t &target, size_type k) const;

    // Range search: find all points within [lower, upper] box
    //
    [[nodiscard]] points_vec
    find_in_range(const point_t &lower, const point_t &upper) const;

    // Check if tree is empty
    //
    [[nodiscard]] bool
    empty() const;

    // Get number of nodes_
    //
    [[nodiscard]] size_type
    size() const;

    // Get memory usage in bytes
    //
    [[nodiscard]] size_type
    memory_usage() const;

private:

    std::vector<Node>   nodes_ { };
    size_type           root_idx_ { NULL_IDX };
    const size_type     k_;

    // Helper to compute squared distance between two points
    //
    [[nodiscard]] value_type
    distance_sq_(const point_t &a, const point_t &b) const;

    // Build tree iteratively using array-based storage
    //
    struct  BuildTask  {
        size_type   node_idx;
        size_type   start;
        size_type   end;
        size_type   depth;
        bool        is_left;
        size_type   parent_idx;
    };

    void
    build_tree_(points_vec &points);

    // Iterative nearest neighbor search
    //
    struct  SearchState  {
        size_type   node_idx;
        size_type   depth;
        bool        visited_near;

        SearchState(size_type idx, size_type d, bool v = false)
            : node_idx(idx), depth(d), visited_near(v)  {   }
    };

    [[nodiscard]] point_t
    nearest_(const point_t& target) const;

    // Iterative k-nearest neighbors search
    //
    [[nodiscard]] points_vec
    k_nearest_(const point_t &target, size_type k) const;

    // Iterative range search
    //
    struct  RangeState  {
        size_type   node_idx;
        size_type   depth;
    };

    [[nodiscard]] points_vec
    range_search_(const point_t &lower, const point_t &upper) const;
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/KDTree.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
