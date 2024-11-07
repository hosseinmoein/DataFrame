// Hossein Moein
// November 7, 2024
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

#include <DataFrame/Vectors/VectorPtrView.h>
#include <DataFrame/Vectors/VectorView.h>

#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

enum class  matrix_orient : unsigned char  {

    column_major = 1,
    row_major = 2,
};

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO = matrix_orient::column_major>
class   Matrix  {

public:

    using size_type = std::size_t;
    using value_type = T;
    using reference = value_type &;
    using const_reference = const value_type &;
    using pointer = value_type *;
    using const_pointer = const value_type *;

    using self_t = Matrix<value_type, MO>;

    Matrix() = default;
    Matrix(size_type rows, size_type cols, const_reference def_v = T());
    Matrix(const Matrix &) = default;
    Matrix(Matrix &&) = default;
    ~Matrix() = default;
    Matrix &operator = (const Matrix &) = default;
    Matrix &operator = (Matrix &&) = default;

    void clear() noexcept;
    void swap(Matrix &rhs) noexcept;
    bool empty() const noexcept;
    void reserve(size_type rows, size_type cols);

    size_type rows() const noexcept;
    size_type columns() const noexcept;

public:

    void resize(size_type rows, size_type cols, const_reference def_v = T());

    reference at (size_type r, size_type c);
    const_reference at (size_type r, size_type c) const;
    reference operator() (size_type r, size_type c);
    const_reference operator() (size_type r, size_type c) const;

    // Set the given column or row from the given iterator.
    // col_data/row_Data iterators must be valid for the length of
    // cols_/rows_.
    //
    template<typename I>
    void set_column(I col_data, size_type col);

    template<typename I>
    void set_row(I row_data, size_type row);

private:

    using storage_t = std::vector<value_type>;

    size_type   rows_ { 0 };
    size_type   cols_ { 0 };
    storage_t   matrix_ { };

public:

};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifdef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/Matrix.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
