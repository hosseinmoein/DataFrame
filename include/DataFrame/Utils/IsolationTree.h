// Hossein Moein
// March 3, 2025
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

#include <DataFrame/Utils/Matrix.h>

#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
struct  IsoNode {

    using size_type = long;
    using value_type = T;

    value_type  split_value { 0 };
    size_type   left { -1 };
    size_type   right { -1 };
    size_type   leaf_size { 0 }; // Leaf node size
    size_type   feature { -1 };  // Index to matrix columns

    explicit IsoNode(size_type size) : leaf_size(size)  {  }
    IsoNode() = default;
    IsoNode(const IsoNode &) = default;
    IsoNode(IsoNode &&) = default;
    IsoNode &operator =(const IsoNode &) = default;
    IsoNode &operator =(IsoNode &&) = default;
    ~IsoNode() = default;
};

// ----------------------------------------------------------------------------

template<typename T>
class   IsoTree {

public:

    using size_type = long;
    using value_type = T;
    using vec_t = std::vector<IsoNode<value_type>>;
    using matrix_t = Matrix<value_type, matrix_orient::column_major>;

    explicit
    IsoTree(size_type max_depth = 10);
    IsoTree(const IsoTree &) = default;
    IsoTree(IsoTree &&) = default;
    IsoTree &operator =(const IsoTree &) = default;
    IsoTree &operator =(IsoTree &&) = default;
    ~IsoTree() {  }

    static inline double calc_depth(size_type size);

    size_type build_tree(
        const matrix_t &data,
        const std::vector<size_type> &rows,  // Row indices into data
        std::mt19937 &gen,
        size_type depth);

    size_type path_len(const matrix_t &data,
                       size_type row,        // Row index into data
                       size_type tree_node,  // Index into iso_tree
                       size_type depth) const;

private:

    vec_t           iso_tree_ { };
    const size_type max_depth_;
};

// ----------------------------------------------------------------------------

template<typename T>
class   IsoForest {

public:

    using size_type = long;
    using value_type = T;
    using tree_t = IsoTree<T>;
    using matrix_t = Matrix<value_type, matrix_orient::column_major>;

    IsoForest(size_type num_trees, size_type max_depth);
    IsoForest() = delete;
    IsoForest(const IsoForest &) = default;
    IsoForest(IsoForest &&) = default;
    IsoForest &operator =(const IsoForest &) = default;
    IsoForest &operator =(IsoForest &&) = default;
    ~IsoForest() {  }

    void fit(const matrix_t &data);
    double outlier_score(const matrix_t &data, size_type row) const;

private:

    std::random_device  rd_ { };
    std::mt19937        gen_ { rd_() };
    std::vector<tree_t> trees_;
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/IsolationTree.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
