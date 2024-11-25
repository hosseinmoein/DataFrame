// Hossein Moein
// August 9, 2023
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

#include <DataFrame/DataFrameTypes.h>
#include <DataFrame/Utils/Matrix.h>

#include <cmath>
#include <cstdlib>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T,  matrix_orient MO>
Matrix<T, MO>::Matrix(size_type rows, size_type cols, const_reference def_v)
    : rows_(rows), cols_(cols), matrix_(rows * cols, def_v)  {   }

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
void
Matrix<T, MO>::clear() noexcept  {

    rows_ = cols_ = 0;
    matrix_.clear();
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
void
Matrix<T, MO>::swap(Matrix &rhs) noexcept  {

    std::swap(rows_, rhs.rows_);
    std::swap(cols_, rhs.cols_);
    matrix_.swap(rhs.matrix_);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
bool
Matrix<T, MO>::empty() const noexcept  {

    return (rows_ == 0 && cols_ == 0);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
void
Matrix<T, MO>::reserve(size_type rows, size_type cols)  {

    matrix_.reserve(rows * cols);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>::size_type
Matrix<T, MO>::rows() const noexcept  { return (rows_); }

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>::size_type
Matrix<T, MO>::cols() const noexcept  { return (cols_); }

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
void
Matrix<T, MO>::resize(size_type rows, size_type cols, const_reference def_v) {

    rows_ = rows;
    cols_ = cols;
    matrix_.resize(rows * cols, def_v);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>::reference
Matrix<T, MO>::at(size_type row, size_type col)  {

    if constexpr (MO == matrix_orient::column_major)
        return (matrix_[col * rows_ + row]);
    else
        return (matrix_[row * cols_ + col]);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>::const_reference
Matrix<T, MO>::at(size_type row, size_type col) const  {

    if constexpr (MO == matrix_orient::column_major)
        return (matrix_[col * rows_ + row]);
    else
        return (matrix_[row * cols_ + col]);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>::reference
Matrix<T, MO>::operator() (size_type row, size_type col)  {

    return (at(row, col));
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>::const_reference
Matrix<T, MO>::operator() (size_type row, size_type col) const  {

    return (at(row, col));
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
template<typename I>
void
Matrix<T, MO>::set_column(I col_data, size_type col)  {

    for (size_type r = 0; r < rows(); ++r)
        at(r, col) = *col_data++;
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
template<typename I>
void
Matrix<T, MO>::set_row(I row_data, size_type row)  {

    for (size_type c = 0; c < cols(); ++c)
        at(row, c) = *row_data++;
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
constexpr matrix_orient
Matrix<T, MO>::orientation()  { return (MO); }

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>::trans_result_t
Matrix<T, MO>::transpose() const noexcept  {

    trans_result_t  result { cols(), rows() };

    if constexpr (MO == matrix_orient::column_major)  {
        for (size_type c = 0; c < cols(); ++c)
            for (size_type r = 0; r < rows(); ++r)
                result(c, r) = at(r, c);
    }
    else  {
        for (size_type r = 0; r < rows(); ++r)
            for (size_type c = 0; c < cols(); ++c)
                result(c, r) = at(r, c);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>
Matrix<T, MO>::transpose2() const noexcept  {

    Matrix  result { cols(), rows() };

    if constexpr (MO == matrix_orient::column_major)  {
        for (size_type c = 0; c < cols(); ++c)
            for (size_type r = 0; r < rows(); ++r)
                result(c, r) = at(r, c);
    }
    else  {
        for (size_type r = 0; r < rows(); ++r)
            for (size_type c = 0; c < cols(); ++c)
                result(c, r) = at(r, c);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>::size_type
Matrix<T, MO>::ppivot_(size_type pivot_row) noexcept  {

    size_type   max_row { pivot_row };
    value_type  max_value { value_type(std::fabs(at(pivot_row, pivot_row))) };

    for (size_type r = pivot_row + 1; r < rows(); ++r)  {
        const value_type    tmp { value_type(std::fabs(at(r, pivot_row))) };

        if (tmp > max_value && tmp != value_type(0))  {
            max_value = tmp;
            max_row = r;
        }
    }

    if (at(max_row, pivot_row) == value_type(0))
        return (NOPOS_);

    if (max_row != pivot_row)  {
        for (size_type c = 0; c < cols(); ++c)
            std::swap(at(pivot_row, c), at(max_row, c));
        return (max_row);
    }

    return (0);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO>
Matrix<T, MO>::inverse() const  {

    if (rows() != cols())
        throw DataFrameError("Matrix::inverse(): Matrix must be squared");

    Matrix  aux_mat = *this;
    Matrix  result { rows(), cols(), 0 };

    // First make identity matrix
    //
    for (size_type d = 0; d < result.cols(); ++d)
        result(d, d) = value_type(1);

    for (size_type r = 0; r < aux_mat.rows (); ++r)  {
        const size_type idx = aux_mat.ppivot_(r);

        if (idx == NOPOS_)
            throw DataFrameError("Matrix::inverse(): Singular matrix");

        if (idx != 0)
            for (size_type c = 0; c < aux_mat.cols(); ++c)
                std::swap(result(r, c), result(idx, c));

        const value_type    diag = aux_mat.at(r, r);

        for (size_type c = 0; c < aux_mat.cols(); ++c)  {
            aux_mat.at(r, c) /= diag;
            result(r, c) /= diag;
        }

        for (size_type rr = 0; rr < aux_mat.rows (); ++rr)  {
            if (rr != r)  {
                const value_type    off_diag = aux_mat.at(rr, r);

                for (size_type c = 0; c < aux_mat.cols(); ++c)  {
                    aux_mat.at(rr, c) -= off_diag * aux_mat.at(r, c);
                    result(rr, c) -= off_diag * result(r, c);
                }
            }
        }
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
