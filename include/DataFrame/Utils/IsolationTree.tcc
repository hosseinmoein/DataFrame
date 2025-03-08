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

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <numeric>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
IsoTree<T>::IsoTree(size_type max_depth) : max_depth_(max_depth)  {

    iso_tree_.reserve(max_depth_ * 2);
}

// ----------------------------------------------------------------------------

template<typename T>
inline double IsoTree<T>::calc_depth(size_type size)  {

    if (size <= 1)  return (0);
    return ((2.0 * (std::log(double(size - 1)) + 0.5772156649)) -
            (2.0 * (double(size - 1.0)) / double(size)));
}

// ----------------------------------------------------------------------------

template<typename T>
IsoTree<T>::size_type IsoTree<T>::
build_tree(std::vector<value_type> &data,
           std::mt19937 &gen,
           size_type left,
           size_type right,
           size_type depth)  {

    if (left >= right || depth >= max_depth_)  {
        iso_tree_.push_back(IsoNode<T> { .leaf_size = 0 });
        return (iso_tree_.size() - 1);
    }

    std::uniform_int_distribution<size_type>    dis { left, right };
    const value_type                            &split_value = data[dis(gen)];
    const auto                                  citer =
        std::partition(data.begin() + left, data.begin() + right,
                       [split_value](const value_type &v) -> bool {
                           return (v < split_value);
                       });
    const size_type                             mid =
        size_type(std::distance(data.begin(), citer));
    const IsoNode<value_type>                   node {
        split_value,
        right - left,
        build_tree(data, gen, left, mid, depth + 1),
        build_tree(data, gen, mid, right, depth + 1)
    };

    iso_tree_.push_back(std::move(node));
    return (iso_tree_.size() - 1);
}

// ----------------------------------------------------------------------------

template<typename T>
double IsoTree<T>::
path_len(const value_type &value,
         size_type node,
         size_type depth) const  {

    const IsoNode<value_type>   &node_val = iso_tree_[node];

    if (node_val.left >= 0 || node_val.right >= 0)  {
        if (value < node_val.split_value && node_val.left >= 0)
            return (path_len(value, node_val.left, depth + 1));
        else if (node_val.right >= 0)
            return (path_len(value, node_val.right, depth + 1));
    }
    return (double(depth) + calc_depth(node_val.leaf_size));
}

// ----------------------------------------------------------------------------

template<typename T>
IsoForest<T>::IsoForest(size_type num_trees, size_type max_depth)
    : trees_(num_trees, IsoTree<T> { max_depth })  {   }

// ----------------------------------------------------------------------------

template<typename T>
void IsoForest<T>::
fit(const std::vector<T> &data)  {

    std::vector<value_type> sample;

    for (auto &tree : trees_)  {
        sample = data;
        std::ranges::shuffle(sample.begin(), sample.end(), gen_);
        tree.build_tree(sample, gen_, 0, size_type(sample.size()), 0);
    }
}

// ----------------------------------------------------------------------------

template<typename T>
double IsoForest<T>::
outlier_score(const value_type &value, size_type data_size) const  {

    double  avg_path_len { 0 };

    for (const auto &tree : trees_)  {
        avg_path_len += tree.path_len(value, tree.root_idx(), 0);
    }
    avg_path_len /= double(trees_.size());

    const double    adj_path_len {
        -avg_path_len / IsoTree<T>::calc_depth(data_size - 1)
    };

    return (std::pow(2.0, adj_path_len));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
