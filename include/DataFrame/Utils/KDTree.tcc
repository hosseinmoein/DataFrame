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

#include <DataFrame/DataFrameTypes.h>

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

template<typename T>
KDTree<T>::KDTree(size_type k, dist_func_t &&dist_func)
    : k_(k), dist_func_(dist_func)  {   }

// ----------------------------------------------------------------------------

template<typename T>
void KDTree<T>::
build(points_vec &points)  {

#ifdef HMDF_SANITY_EXCEPTIONS
    for (const auto &vec : points)
        if (vec.size() != k_)
            throw DataFrameError("KDTree<T>::build(): "
                                 "All data vectors must have exactly "
                                 "K datapoints");
#endif // HMDF_SANITY_EXCEPTIONS

    build_tree_(points);
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree<T>::point_t KDTree<T>::
find_nearest(const point_t &target) const { return (nearest_(target)); }

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree<T>::points_vec KDTree<T>::
find_k_nearest(const point_t &target, size_type k) const {

    return (k_nearest_(target, k));
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree<T>::points_vec KDTree<T>::
find_in_range(const point_t &lower, const point_t &upper) const  {

    return (range_search_(lower, upper));
}

// ----------------------------------------------------------------------------

template<typename T>
bool KDTree<T>::
empty() const  { return (root_idx_ == NULL_IDX); }

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree<T>::size_type KDTree<T>::
size() const  { return (nodes_.size()); }

// ----------------------------------------------------------------------------

template<typename T>
void KDTree<T>::build_tree_(points_vec &points)  {

    if (points.empty())  {
        root_idx_ = NULL_IDX;
        return;
    }

    nodes_.clear();
    nodes_.reserve(points.size());

    std::stack<BuildTask>   tasks;

    tasks.push({ NULL_IDX, 0, points.size(), 0, false, NULL_IDX });

    while (! tasks.empty())  {
        const BuildTask task { tasks.top() };

        tasks.pop();

        if (task.start >= task.end)  continue;

        const size_type axis { task.depth % k_ };
        const size_type mid { task.start + (task.end - task.start) / 2 };

        // Partition around median
        //
        std::nth_element(points.begin() + task.start,
                         points.begin() + mid,
                         points.begin() + task.end,
                         [axis]
                         (const point_t &lhs, const point_t &rhs) -> bool  {
                             return (lhs[axis] < rhs[axis]);
                         });

        // Create node
        //
        const size_type current_idx { nodes_.size() };

        nodes_.emplace_back(points[mid]);

        // Update parent's child pointer
        //
        if (task.parent_idx != NULL_IDX)  {
            if (task.is_left)
                nodes_[task.parent_idx].left = current_idx;
            else
                nodes_[task.parent_idx].right = current_idx;
        }
        else  {
            root_idx_ = current_idx;
        }

        // Push right subtree first (so left is processed first)
        //
        if ((mid + 1) < task.end)
            tasks.push({ NULL_IDX,
                         mid + 1,
                         task.end,
                         task.depth + 1,
                         false,
                         current_idx });

        if (task.start < mid)
            tasks.push({ NULL_IDX,
                         task.start,
                         mid,
                         task.depth + 1,
                         true,
                         current_idx });

    }
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree<T>::point_t KDTree<T>::
nearest_(const point_t &target) const  {

    if (root_idx_ == NULL_IDX)
        throw DataFrameError("KDTree<T>::nearest_(): Tree is empty");

    point_t                 best (nodes_[root_idx_].point);
    value_type              best_dist {
        dist_func_(nodes_[root_idx_].point, target)
    };
    std::stack<SearchState> stack;

    stack.push(SearchState { root_idx_, 0 });
    while (! stack.empty())  {
        const SearchState   state { stack.top() };

        stack.pop();
        if (state.node_idx == NULL_IDX)  continue;

        const Node  &node { nodes_[state.node_idx] };

        if (! state.visited_near)  {
            const value_type    dist { dist_func_(node.point, target) };

            if (dist < best_dist)  {
                best_dist = dist;
                best = node.point;
            }

            const size_type     axis { state.depth % k_ };
            const value_type    diff { target[axis] - node.point[axis] };
            const size_type     near_idx {
                (diff < 0) ? node.left : node.right
            };
            const size_type     far_idx {
                (diff < 0) ? node.right : node.left
            };

            // Push current state back to check far side later
            //
            if ((far_idx != NULL_IDX) && ((diff * diff) < best_dist))
                stack.push(SearchState { state.node_idx, state.depth, true });

            // Push near side to explore first
            //
            if (near_idx != NULL_IDX)
                stack.push(SearchState { near_idx, state.depth + 1 });
        }
        else  { // Visiting far side
            const size_type     axis { state.depth % k_ };
            const value_type    diff { target[axis] - node.point[axis] };
            const size_type     far_idx {
                (diff < 0) ? node.right : node.left
            };

            if ((far_idx != NULL_IDX) && ((diff * diff) < best_dist))
                stack.push(SearchState { far_idx, state.depth + 1 });
        }
    }

    return (best);
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree<T>::points_vec KDTree<T>::
k_nearest_(const point_t &target, size_type k) const  {

    points_vec    result;

    if (root_idx_ == NULL_IDX)  return (result);

    std::priority_queue<std::pair<value_type, point_t>> pq;
    std::stack<SearchState>                             stack;

    stack.push(SearchState { root_idx_, 0 });
    while (! stack.empty())  {
        const SearchState   state { stack.top() };

        stack.pop();

        if (state.node_idx == NULL_IDX)  continue;

        const Node &node { nodes_[state.node_idx] };

        if (! state.visited_near)  {
            const value_type    dist { dist_func_(node.point, target) };

            if (pq.size() < k)  {
                pq.push({ dist, node.point });
            }
            else if (dist < pq.top().first)  {
                pq.pop();
                pq.push({ dist, node.point });
            }

            const size_type     axis { state.depth % k_ };
            const value_type    diff { target[axis] - node.point[axis] };
            const size_type     near_idx {
                (diff < 0) ? node.left : node.right
            };
            const size_type     far_idx {
                (diff < 0) ? node.right : node.left
            };

            // Check if we need to explore far side
            //
            if (far_idx != NULL_IDX &&
                (pq.size() < k || diff * diff < pq.top().first))
                stack.push(SearchState { state.node_idx, state.depth, true });

            if (near_idx != NULL_IDX)
                stack.push(SearchState { near_idx, state.depth + 1 });
        }
        else  { // Visiting far side
            const size_type     axis { state.depth % k_ };
            const value_type    diff { target[axis] - node.point[axis] };
            const size_type     far_idx {
                (diff < 0) ? node.right : node.left
            };

            if (far_idx != NULL_IDX &&
                (pq.size() < k || diff * diff < pq.top().first))
                stack.push(SearchState { far_idx, state.depth + 1 });
        }
    }

    while (! pq.empty())  {
        result.push_back(pq.top().second);
        pq.pop();
    }

    std::reverse(result.begin(), result.end());
    return (result);
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree<T>::points_vec KDTree<T>::
range_search_(const point_t &lower, const point_t &upper) const  {

    points_vec    result;

    if (root_idx_ == NULL_IDX)  return (result);

    std::stack<RangeState>  stack;

    stack.push({ root_idx_, 0 });
    while (! stack.empty())  {
        const RangeState    state { stack.top() };

        stack.pop();
        if (state.node_idx == NULL_IDX)  continue;

        const Node  &node { nodes_[state.node_idx] };
        bool        in_range { true };  // Check if point is in range

        for (size_type i = 0; i < k_; ++i)  {
            if (node.point[i] < lower[i] || node.point[i] > upper[i])  {
                in_range = false;
                break;
            }
        }
        if (in_range)  result.push_back(node.point);

        const size_type axis { state.depth % k_ };

        // Push children if they might contain points in range
        //
        if (upper[axis] >= node.point[axis] && node.right != NULL_IDX)
            stack.push({ node.right, state.depth + 1 });

        if (lower[axis] <= node.point[axis] && node.left != NULL_IDX)
            stack.push({ node.left, state.depth + 1 });
    }

    return (result);
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template<typename T>
KDTree1D<T>::KDTree1D(dist_func_t &&dist_func) : dist_func_(dist_func)  {   }

// ----------------------------------------------------------------------------

template<typename T>
void KDTree1D<T>::
build(points_vec &points)  { build_tree_(points); }

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree1D<T>::point_t KDTree1D<T>::
find_nearest(const point_t &target) const { return (nearest_(target)); }

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree1D<T>::points_vec KDTree1D<T>::
find_k_nearest(const point_t &target, size_type k) const {

    return (k_nearest_(target, k));
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree1D<T>::points_vec KDTree1D<T>::
find_in_range(const point_t &lower, const point_t &upper) const  {

    return (range_search_(lower, upper));
}

// ----------------------------------------------------------------------------

template<typename T>
bool KDTree1D<T>::
empty() const  { return (root_idx_ == NULL_IDX); }

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree1D<T>::size_type KDTree1D<T>::
size() const  { return (nodes_.size()); }

// ----------------------------------------------------------------------------

template<typename T>
void KDTree1D<T>::build_tree_(points_vec &points)  {

    if (points.empty())  {
        root_idx_ = NULL_IDX;
        return;
    }

    nodes_.clear();
    nodes_.reserve(points.size());

    std::stack<BuildTask>   tasks;

    tasks.push({ NULL_IDX, 0, points.size(), false, NULL_IDX });

    while (! tasks.empty())  {
        const BuildTask task { tasks.top() };

        tasks.pop();

        if (task.start >= task.end)  continue;

        const size_type mid { task.start + (task.end - task.start) / 2 };

        // Partition around median
        //
        std::nth_element(points.begin() + task.start,
                         points.begin() + mid,
                         points.begin() + task.end,
                         [](const point_t &lhs, const point_t &rhs) -> bool  {
                             return (lhs < rhs);
                         });

        // Create node
        //
        const size_type current_idx { nodes_.size() };

        nodes_.emplace_back(points[mid]);

        // Update parent's child pointer
        //
        if (task.parent_idx != NULL_IDX)  {
            if (task.is_left)
                nodes_[task.parent_idx].left = current_idx;
            else
                nodes_[task.parent_idx].right = current_idx;
        }
        else  {
            root_idx_ = current_idx;
        }

        // Push right subtree first (so left is processed first)
        //
        if ((mid + 1) < task.end)
            tasks.push({ NULL_IDX,
                         mid + 1,
                         task.end,
                         false,
                         current_idx });

        if (task.start < mid)
            tasks.push({ NULL_IDX,
                         task.start,
                         mid,
                         true,
                         current_idx });

    }
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree1D<T>::point_t KDTree1D<T>::
nearest_(const point_t &target) const  {

    if (root_idx_ == NULL_IDX)
        throw DataFrameError("KDTree1D<T>::nearest_(): Tree is empty");

    point_t                 best (nodes_[root_idx_].point);
    value_type              best_dist {
        dist_func_(nodes_[root_idx_].point, target)
    };
    std::stack<SearchState> stack;

    stack.push(SearchState { root_idx_ });
    while (! stack.empty())  {
        const SearchState   state { stack.top() };

        stack.pop();
        if (state.node_idx == NULL_IDX)  continue;

        const Node  &node { nodes_[state.node_idx] };

        if (! state.visited_near)  {
            const value_type    dist { dist_func_(node.point, target) };

            if (dist < best_dist)  {
                best_dist = dist;
                best = node.point;
            }

            const value_type    diff { target - node.point };
            const size_type     near_idx {
                (diff < 0) ? node.left : node.right
            };
            const size_type     far_idx {
                (diff < 0) ? node.right : node.left
            };

            // Push current state back to check far side later
            //
            if ((far_idx != NULL_IDX) && (std::abs(diff) < best_dist))
                stack.push(SearchState { state.node_idx, true });

            // Push near side to explore first
            //
            if (near_idx != NULL_IDX)
                stack.push(SearchState { near_idx });
        }
        else  { // Visiting far side
            const value_type    diff { target - node.point };
            const size_type     far_idx {
                (diff < 0) ? node.right : node.left
            };

            if ((far_idx != NULL_IDX) && (std::abs(diff) < best_dist))
                stack.push(SearchState { far_idx });
        }
    }

    return (best);
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree1D<T>::points_vec KDTree1D<T>::
k_nearest_(const point_t &target, size_type k) const  {

    points_vec    result;

    if (root_idx_ == NULL_IDX)  return (result);

    std::priority_queue<std::pair<value_type, point_t>> pq;
    std::stack<SearchState>                             stack;

    stack.push(SearchState { root_idx_ });
    while (! stack.empty())  {
        const SearchState   state { stack.top() };

        stack.pop();

        if (state.node_idx == NULL_IDX)  continue;

        const Node &node { nodes_[state.node_idx] };

        if (! state.visited_near)  {
            const value_type    dist { dist_func_(node.point, target) };

            if (pq.size() < k)  {
                pq.push({ dist, node.point });
            }
            else if (dist < pq.top().first)  {
                pq.pop();
                pq.push({ dist, node.point });
            }

            const value_type    diff { target - node.point };
            const size_type     near_idx {
                (diff < 0) ? node.left : node.right
            };
            const size_type     far_idx {
                (diff < 0) ? node.right : node.left
            };

            // Check if we need to explore far side
            //
            if (far_idx != NULL_IDX &&
                (pq.size() < k || std::abs(diff) < pq.top().first))
                stack.push(SearchState { state.node_idx, true });

            if (near_idx != NULL_IDX)
                stack.push(SearchState { near_idx });
        }
        else  { // Visiting far side
            const value_type    diff { target - node.point };
            const size_type     far_idx {
                (diff < 0) ? node.right : node.left
            };

            if (far_idx != NULL_IDX &&
                (pq.size() < k || std::abs(diff) < pq.top().first))
                stack.push(SearchState { far_idx });
        }
    }

    while (! pq.empty())  {
        result.push_back(pq.top().second);
        pq.pop();
    }

    std::reverse(result.begin(), result.end());
    return (result);
}

// ----------------------------------------------------------------------------

template<typename T>
typename KDTree1D<T>::points_vec KDTree1D<T>::
range_search_(const point_t &lower, const point_t &upper) const  {

    points_vec    result;

    if (root_idx_ == NULL_IDX)  return (result);

    std::stack<size_type>   stack;

    stack.push(root_idx_);
    while (! stack.empty())  {
        const size_type state { stack.top() };

        stack.pop();
        if (state == NULL_IDX)  continue;

        const Node  &node { nodes_[state] };

        if (node.point > lower && node.point < upper)
            result.push_back(node.point);

        // Push children if they might contain points in range
        //
        if (upper >= node.point && node.right != NULL_IDX)
            stack.push(node.right);

        if (lower <= node.point && node.left != NULL_IDX)
            stack.push(node.left);
    }

    return (result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
