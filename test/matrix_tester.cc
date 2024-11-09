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

#include <DataFrame/Utils/Matrix.h>

#include <cassert>
#include <iostream>

using namespace hmdf;

// -----------------------------------------------------------------------------

using row_mat_t = Matrix<std::size_t, matrix_orient::row_major>;
using col_mat_t = Matrix<std::size_t, matrix_orient::column_major>;

static constexpr std::size_t    ROWS = 5;
static constexpr std::size_t    COLS = 6;

// -----------------------------------------------------------------------------

int main(int, char *[]) {

    row_mat_t   row_mat { ROWS, COLS };
    col_mat_t   col_mat { ROWS, COLS };
    std::size_t value { 0 };

    for (std::size_t r = 0; r < row_mat.rows(); ++r)
        for (std::size_t c = 0; c < row_mat.cols(); ++c)
            row_mat(r, c) = value++;

    value = 0;
    for (std::size_t c = 0; c < col_mat.cols(); ++c)
        for (std::size_t r = 0; r < col_mat.rows(); ++r)
            col_mat(r, c) = value++;

    value = 0;
    for (std::size_t r = 0; r < row_mat.rows(); ++r)
        for (std::size_t c = 0; c < row_mat.cols(); ++c)
            assert(row_mat(r, c) == value++);

    value = 0;
    for (std::size_t c = 0; c < col_mat.cols(); ++c)
        for (std::size_t r = 0; r < col_mat.rows(); ++r)
            assert(col_mat(r, c) == value++);

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
