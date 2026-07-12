// Hossein Moein
// March 4, 2025
/*
Copyright (c) 2023-2028, Hossein Moein
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

#include <DataFrame/Utils/IsolationTree.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
IsoTree<T>::IsoTree(size_type max_depth) : max_depth_(max_depth)  {

    iso_tree_.reserve(
        (max_depth_ > 100) ? max_depth_ * (max_depth_ / 100) : max_depth_);
}

// ----------------------------------------------------------------------------

template<typename T>
IsoTree<T>::size_type IsoTree<T>::
build_tree(std::vector<value_type> &data,
           std::mt19937 &gen,
           size_type left,
           size_type right,
           size_type depth)  {

    if (left >= right || depth >= max_depth_ || right == 0) [[unlikely]]  {
        iso_tree_.push_back(IsoNode<T> { });
    }
    else  {
        const auto  [min_it, max_it] =
            std::minmax_element(data.begin() + left, data.begin() + right);
        const value_type    min_val { *min_it };
        const value_type    max_val { *max_it };

        if (min_val == max_val) [[unlikely]]  {
            // All elements identical — no valid split possible.  Treat as
            // external node.  This also covers the duplicate-value case that
            // made the original random-anchor approach degenerate (anchor =
            // min → nothing goes left → right = full range → max_depth hit).
            //
            iso_tree_.push_back(IsoNode<T> { });
        }
        else  {
            // The split point must be drawn uniformly from
            // (min_val, max_val) — the VALUE range of the current partition —
            // not from the data points themselves.
            //
            // Selecting a random data point as anchor is wrong because:
            //   1. Choosing the minimum guarantees an empty left partition,
            //      wasting the entire depth budget on non-splits.
            //   2. Duplicate values pile up on one side, starving the other.
            // Drawing a uniform real from (min, max) ensures both sides are
            // always non-empty and splits are unbiased within the range.
            //
            value_type  split_val;

            if constexpr (std::floating_point<value_type>)  {
                std::uniform_real_distribution<value_type>  dis(min_val,
                                                                max_val);

                split_val = dis(gen);
            }
            else  {
                // For integral T: pick a random integer in [min, max-1].
                // Because the predicate is v < split_val, split_val = max_val
                // would put everything on the left (all < max) and nothing on
                // the right, so we exclude max_val.
                //
                using dist_t =
                    std::uniform_int_distribution<
                        std::conditional_t<std::integral<value_type>,
                                           value_type, long>>;

                dist_t  dis {
                    static_cast<typename dist_t::result_type>(min_val),
                    static_cast<typename dist_t::result_type>(max_val) - 1
                };

                split_val = static_cast<value_type>(dis(gen));
            }

            const auto                  citer {
                std::partition(data.begin() + left, data.begin() + right,
                               [&split_val = std::as_const(split_val)]
                               (const value_type &v) -> bool {
                                   return (v < split_val);
                               })
            };
            const size_type             mid {
                size_type(std::distance(data.begin(), citer))
            };
            const IsoNode<value_type>   node {
                split_val,
                build_tree(data, gen, left, mid, depth + 1),
                build_tree(data, gen, mid, right, depth + 1)
            };

            iso_tree_.push_back(std::move(node));
        }
    }
    return (iso_tree_.size() - 1);
}

// ----------------------------------------------------------------------------

template<typename T>
double IsoTree<T>::
path_len(const value_type &value, size_type node, size_type depth) const  {

    const IsoNode<value_type>   &node_val { iso_tree_[node] };

    if (node_val.left >= 0 || node_val.right >= 0) [[likely]]  {
        if (value < node_val.split_value && node_val.left >= 0)
            return (path_len(value, node_val.left, depth + 1));
        else if (node_val.right >= 0)
            return (path_len(value, node_val.right, depth + 1));
    }
    return (double(depth));
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template<typename T>
inline double IsoForest<T>::calc_depth_(size_type size)  {

    if (size <= 1)  return (0);
    return ((2.0 * (std::log(double(size - 1)) + 0.5772156649)) -
            (2.0 * (double(size - 1)) / double(size)));
}

// ----------------------------------------------------------------------------

template<typename T>
IsoForest<T>::IsoForest(size_type num_trees, size_type max_depth)
    : forest_(num_trees, IsoTree<T> { max_depth })  { gen_.seed(123);   }

// ----------------------------------------------------------------------------

template<typename T>
template<typename I>
void IsoForest<T>::
fit(const I &begin, const I &end)  {

    std::vector<value_type> data_copy(begin, end);

    for (auto &tree : forest_)  {
        std::ranges::shuffle(data_copy.begin(), data_copy.end(), gen_);
        tree.build_tree(data_copy, gen_, 0, size_type(data_copy.size()), 0);
    }
}

// ----------------------------------------------------------------------------

template<typename T>
double IsoForest<T>::
outlier_score(const value_type &value, size_type data_size) const  {

    double  avg_path_len { 0 };

    for (const auto &tree : forest_)
        avg_path_len += tree.path_len(value, tree.root_idx(), 0);
    avg_path_len /= double(forest_.size());

    return (std::pow(2.0, -avg_path_len / calc_depth_(data_size)));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
