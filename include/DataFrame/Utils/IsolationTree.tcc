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

#include <cmath>
#include <cstdlib>
#include <numeric>
#include <random>

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
build_tree(const matrix_t &data,
           const std::vector<size_type> &cols,
           std::mt19937 &gen,
           size_type depth)  {

    if (cols.size() <= 1 || depth >= max_depth_)  {
        iso_tree_.push_back(IsoNode<T> { size_type(cols.size()) });
        return (iso_tree_.size() - 1);
    }

    const size_type dimension = std::rand() % data.rows(); // Random dimension
    value_type      min_value = data(dimension, cols[0]);
    value_type      max_value = min_value;

    for (size_type c = 1; c < size_type(cols.size()); ++c)  {
        min_value = std::min(min_value, data(dimension, cols[c]));
        max_value = std::max(max_value, data(dimension, cols[c]));
    }
    if (max_value == min_value)  {
        iso_tree_.push_back(IsoNode<T> { size_type(cols.size()) });
        return (iso_tree_.size() - 1);
    }

    std::uniform_real_distribution<T>   dist(min_value, max_value);
    value_type                          split_value = dist(gen);
    std::vector<size_type>              left_col_idx { };
    std::vector<size_type>              right_col_idx { };

    left_col_idx.reserve(cols.size() / 2);
    right_col_idx.reserve(cols.size() / 2);
    for (size_type c = 0; c < size_type(cols.size()); ++c)  {
        if (data(dimension, cols[c]) < split_value)
            left_col_idx.push_back(c);
        else
            right_col_idx.push_back(c);
    }

    IsoNode<value_type> node { size_type(cols.size()) };

    node.dimension = dimension;
    node.split_value = split_value;
    node.left = build_tree(data, left_col_idx, gen, depth + 1);
    node.right = build_tree(data, right_col_idx, gen, depth + 1);
    iso_tree_.push_back(std::move(node));
    return (iso_tree_.size() - 1);
}

// ----------------------------------------------------------------------------

template<typename T>
IsoTree<T>::size_type IsoTree<T>::
path_len(const matrix_t &data,
         size_type col,
         size_type tree_node,
         size_type depth) const  {

    const IsoNode<value_type>   &node = iso_tree_[tree_node];

    if (node.left < 0 && node.right < 0)
        return (depth + calc_depth(node.leaf_size));

    if (data(node.dimension, col) < node.split_value)
        return (path_len(data, col, node.left, depth + 1));
    return (path_len(data, col, node.right, depth + 1));
}

// ----------------------------------------------------------------------------

template<typename T>
IsoForest<T>::IsoForest(size_type num_trees, size_type max_depth)
    : trees_(num_trees, IsoTree<T> { max_depth })  {   }

// ----------------------------------------------------------------------------

template<typename T>
void IsoForest<T>::
fit(const matrix_t &data)  {

    std::vector<size_type>  cols(data.cols());

    std::iota(cols.begin(), cols.end(), 0);
    for (auto &tree : trees_)
        tree.build_tree(data, cols, gen_, 0);
}

// ----------------------------------------------------------------------------

template<typename T>
double IsoForest<T>::
outlier_score(const matrix_t &data, size_type col) const  {

    double  avg_path_len { 0 };

    for (const auto &tree : trees_)
        avg_path_len += tree.path_len(data, col, 0, 0);
    avg_path_len /= double(trees_.size());

    const double    adj_path_len {
        -avg_path_len / IsoTree<T>::calc_depth(data.cols())
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
