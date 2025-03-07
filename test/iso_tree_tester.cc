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

#include <DataFrame/Utils/IsolationTree.h>

#include <cassert>
#include <chrono>
#include <iostream>

using namespace hmdf;

// ----------------------------------------------------------------------------

using matrix_t = Matrix<double, matrix_orient::column_major>;

// ----------------------------------------------------------------------------

int main(int, char *[]) {

    // This matrix has 10 features each having 3 dimensions (observations)
    //
    matrix_t   data { 3, 10 };

    data(0, 0) = 1.2;
    data(1, 0) = 10.2;
    data(2, 0) = 0.2;

    data(0, 1) = 1.8;
    data(1, 1) = 12.0;
    data(2, 1) = 0.17;

    data(0, 2) = 0.99;
    data(1, 2) = 11.1;
    data(2, 2) = 0.45;

    data(0, 3) = 100.04;
    data(1, 3) = 10.89;  // <--
    data(2, 3) = 50.5;

    data(0, 4) = 2.0;
    data(1, 4) = 10.556;
    data(2, 4) = 0.254;

    data(0, 5) = 1.86;
    data(1, 5) = 11.23;
    data(2, 5) = -8.5;  // <--

    data(0, 6) = 0.899;
    data(1, 6) = 11.5;
    data(2, 6) = 0.56;

    data(0, 7) = 1.3;
    data(1, 7) = 11.8;
    data(2, 7) = 0.345;

    data(0, 8) = 1.5;
    data(1, 8) = 0.99;  // <--
    data(2, 8) = 0.71;

    data(0, 9) = 1.345;
    data(1, 9) = 10.654;
    data(2, 9) = 0.56;

    IsoForest<double>   forest { 25, 10 };

    forest.fit(data);
    for (long c { 0 }; c < data.cols(); ++c)  {
        std::cout << forest.outlier_score(data, c) << std::endl;
    }

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
