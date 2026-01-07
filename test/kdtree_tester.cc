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

#include <DataFrame/Utils/KDTree.h>

#include <cassert>
#include <iostream>

using namespace hmdf;

// ----------------------------------------------------------------------------

int main(int, char *[]) {

    KDTree<double>                      tree { 2 };
    std::vector<std::vector<double>>    points = {
        { 2, 3 }, { 5, 4 }, { 9, 6 }, { 4, 7 }, { 8, 1 }, { 7, 2 }, { 1, 2 },
        { -1, -2 }, { -7, 2 }, { 7, -2 }, { 8, 2 }
    };

    tree.build(points);
    assert(tree.size() == 11);

    // Size of std::vector in debug and non-debug modes
	// 
    assert(tree.memory_usage() == 792 || tree.memory_usage() == 440);

    // Find nearest neighbor
    //
    std::vector<double> query = { 9, 2 };
    const auto          nearest { tree.find_nearest(query) };

    assert(nearest[0] == 8.0 && nearest[1] == 2.0);

    // Find 3 nearest neighbors
    //
    const auto  knn { tree.find_k_nearest(query, 3) };

    assert(knn.size() == 3);
    assert(knn[0][0] == 8.0 && knn[0][1] == 2.0);
    assert(knn[1][0] == 8.0 && knn[1][1] == 1.0);
    assert(knn[2][0] == 7.0 && knn[2][1] == 2.0);

    // Range search
    //
    std::vector<double> lower = { 3, 2 };
    std::vector<double> upper = { 8, 7 };
    const auto          in_range { tree.find_in_range(lower, upper) };

    assert(in_range.size() == 4);
    assert(in_range[0][0] == 5.0 && in_range[0][1] == 4.0);
    assert(in_range[1][0] == 4.0 && in_range[1][1] == 7.0);
    assert(in_range[2][0] == 8.0 && in_range[2][1] == 2.0);
    assert(in_range[3][0] == 7.0 && in_range[3][1] == 2.0);

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
