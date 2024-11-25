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

#include <DataFrame/Utils/Threads/ThreadGranularity.h>

#include <cassert>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

enum class  matrix_orient : unsigned char  {

    column_major = 1,
    row_major = 2,
};

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO = matrix_orient::column_major>
class   Matrix  {

public:

    using size_type = long;
    using value_type = T;
    using reference = value_type &;
    using const_reference = const value_type &;
    using pointer = value_type *;
    using const_pointer = const value_type *;
    using self_t = Matrix<value_type, MO>;

    using trans_result_t =
        typename std::conditional<
            MO == matrix_orient::column_major,
            Matrix<T, matrix_orient::row_major>,
            Matrix<T, matrix_orient::column_major>>::type;

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
    size_type cols() const noexcept;

    static constexpr matrix_orient orientation();

public:

    void resize(size_type rows, size_type cols, const_reference def_v = T());

    reference at(size_type r, size_type c);
    const_reference at(size_type r, size_type c) const;
    reference operator() (size_type r, size_type c);
    const_reference operator() (size_type r, size_type c) const;

    // Set the given column or row from the given iterator.
    // col_data/row_Data iterators must be valid for the length of
    // columns/rows.
    //
    template<typename I>
    void set_column(I col_data, size_type col);

    template<typename I>
    void set_row(I row_data, size_type row);

    trans_result_t transpose() const noexcept;
    Matrix transpose2() const noexcept;

    Matrix inverse() const;

private:

    static constexpr size_type  NOPOS_ = static_cast<size_type>(-9);

    using storage_t = std::vector<value_type>;

    size_type ppivot_(size_type pivot_row) noexcept;

    size_type   rows_ { 0 };
    size_type   cols_ { 0 };
    storage_t   matrix_ { };

    //
    // Iterators
    //

public:

    class   row_iterator;
    class   row_const_iterator : public std::random_access_iterator_tag  {

    public:

        using iterator_category = std::random_access_iterator_tag;
        using value_type = T;
        using size_type = long;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using reference = value_type &;
        using const_reference = const value_type &;
        using difference_type = typename std::vector<T>::difference_type;

        inline row_const_iterator () = default;

        inline row_const_iterator(const self_t *m,
                                  size_type row = 0,
                                  size_type col = 0)
            : mptr_ (m), row_ (row), col_(col)  {   }

        inline bool operator == (const row_const_iterator &rhs) const  {

            return (mptr_ == rhs.mptr_ &&
                    row_ == rhs.row_ &&
                    col_ == rhs.col_);
        }
        inline bool operator != (const row_const_iterator &rhs) const  {

            return (! (*this == rhs));
        }
        inline bool
        operator > (const row_const_iterator &rhs) const noexcept  {

            return (row_ > rhs.row_ || (row_ == rhs.row_ && col_ > rhs.col_));
        }
        inline bool
        operator >= (const row_const_iterator &rhs) const noexcept  {

            return (row_ > rhs.row_ || (row_ == rhs.row_ && col_ >= rhs.col_));
        }
        inline bool
        operator < (const row_const_iterator &rhs) const noexcept  {

            return (! (*this >= rhs));
        }
        inline bool
        operator <= (const row_const_iterator &rhs) const noexcept  {

            return (! (*this > rhs));
        }

        // Following STL style, this iterator appears as a pointer
        // to value_type.
        //
        inline const_pointer operator -> () const noexcept  {

            return (&(mptr_->at(row_, col_)));
        }
        inline const_reference operator * () const noexcept  {

            return (mptr_->at(row_, col_));
        }
        inline operator const_pointer () const noexcept  {

            return (&(mptr_->at(row_, col_)));
        }

       // We are following STL style iterator interface.
       //
        inline row_const_iterator &operator ++ () noexcept  {    // ++Prefix

            col_ += 1;
            if (col_ >= mptr_->cols())  { col_ = 0; row_ += 1; }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (*this);
        }
        inline row_const_iterator operator ++ (int) noexcept  {  // Postfix++

            const size_type row = row_;
            const size_type col = col_;

            col_ += 1;
            if (col_ >= mptr_->cols())  { col_ = 0; row_ += 1; }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (row_const_iterator (mptr_, row, col));
        }

        inline row_const_iterator &operator += (size_type i) noexcept  {

            col_ += i;
            if (col_ >= mptr_->cols())  {
                row_ += col_ / mptr_->cols();
                col_ %= mptr_->cols();
            }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (*this);
        }

        inline row_const_iterator &operator -- () noexcept  {    // --Prefix

            col_ -= 1;
            if (col_ < 0)  { col_ = mptr_->cols() - 1; row_ -= 1; }
            return (*this);
        }
        inline row_const_iterator operator -- (int) noexcept  {  // Postfix--

            const size_type row = row_;
            const size_type col = col_;

            col_ -= 1;
            if (col_ < 0)  { col_ = mptr_->cols() - 1; row_ -= 1; }
            return (row_const_iterator (mptr_, row, col));
        }

        inline row_const_iterator &operator -= (int i) noexcept  {

            col_ -= i;
            if (col_ < 0)  {
                row_ -= col_ / mptr_->cols();
                col_ %= mptr_->cols();
            }
            return (*this);
        }

        inline row_const_iterator operator + (size_type i) noexcept  {

            const size_type row = row_;
            const size_type col = col_;

            col_ += i;
            if (col_ >= mptr_->cols())  {
                row_ += col_ / mptr_->cols();
                col_ %= mptr_->cols();
            }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (row_const_iterator (mptr_, row, col));
        }

        inline row_const_iterator operator - (size_type i) noexcept  {

            const size_type row = row_;
            const size_type col = col_;

            col_ -= i;
            if (col_ < 0)  {
                row_ -= col_ / mptr_->cols();
                col_ %= mptr_->cols();
            }
            return (row_const_iterator (mptr_, row, col));
        }

        inline row_const_iterator operator + (int i) noexcept  {

            return (*this + size_type(i));
        }

        inline row_const_iterator operator - (int i) noexcept  {

            return (*this - size_type(i));
        }

        friend difference_type
        operator - (row_const_iterator lhs, row_const_iterator rhs) noexcept  {

            const size_type row_diff = lhs.row_ - rhs.row_;
            const size_type col_diff = lhs.col_ - rhs.col_;

            assert(lhs.mptr_ == rhs.mptr_);
            return (difference_type(
                        std::abs(row_diff) * rhs.mptr_->cols() - col_diff));
        }

    private:

        const self_t    *mptr_ { nullptr };
        size_type       row_ { 0 };
        size_type       col_ { 0 };
    };

    // It goes through the matrix row-by-row starting at [0, 0]
    //
    class   row_iterator : public std::random_access_iterator_tag  {

    public:

        using iterator_category = std::random_access_iterator_tag;
        using value_type = T;
        using size_type = long;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using reference = value_type &;
        using const_reference = const value_type &;
        using difference_type = typename std::vector<T>::difference_type;

    public:

        inline row_iterator () = default;

        inline row_iterator (self_t *m, size_type row = 0, size_type col = 0)
            : mptr_ (m), row_ (row), col_(col)  {   }

        inline bool operator == (const row_iterator &rhs) const  {

            return (mptr_ == rhs.mptr_ &&
                    row_ == rhs.row_ &&
                    col_ == rhs.col_);
        }
        inline bool operator != (const row_iterator &rhs) const  {

            return (! (*this == rhs));
        }
        inline bool operator > (const row_iterator &rhs) const noexcept  {

            return (row_ > rhs.row_ || (row_ == rhs.row_ && col_ > rhs.col_));
        }
        inline bool operator >= (const row_iterator &rhs) const noexcept  {

            return (row_ > rhs.row_ || (row_ == rhs.row_ && col_ >= rhs.col_));
        }
        inline bool operator < (const row_iterator &rhs) const noexcept  {

            return (! (*this >= rhs));
        }
        inline bool operator <= (const row_iterator &rhs) const noexcept  {

            return (! (*this > rhs));
        }

        // Following STL style, this iterator appears as a pointer
        // to value_type.
        //
        inline pointer operator -> () const noexcept  {

            return (&(mptr_->at(row_, col_)));
        }
        inline reference operator * () const noexcept  {

            return (mptr_->at(row_, col_));
        }
        inline operator pointer () const noexcept  {

            return (&(mptr_->at(row_, col_)));
        }

       // We are following STL style iterator interface.
       //
        inline row_iterator &operator ++ () noexcept  {    // ++Prefix

            col_ += 1;
            if (col_ >= mptr_->cols())  { col_ = 0; row_ += 1; }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (*this);
        }
        inline row_iterator operator ++ (int) noexcept  {  // Postfix++

            const size_type row = row_;
            const size_type col = col_;

            col_ += 1;
            if (col_ >= mptr_->cols())  { col_ = 0; row_ += 1; }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (row_iterator (mptr_, row, col));
        }

        inline row_iterator &operator += (size_type i) noexcept  {

            col_ += i;
            if (col_ >= mptr_->cols())  {
                row_ += col_ / mptr_->cols();
                col_ %= mptr_->cols();
            }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (*this);
        }

        inline row_iterator &operator -- () noexcept  {    // --Prefix

            col_ -= 1;
            if (col_ < 0)  { col_ = mptr_->cols() - 1; row_ -= 1; }
            return (*this);
        }
        inline row_iterator operator -- (int) noexcept  {  // Postfix--

            const size_type row = row_;
            const size_type col = col_;

            col_ -= 1;
            if (col_ < 0)  { col_ = mptr_->cols() - 1; row_ -= 1; }
            return (row_iterator (mptr_, row, col));
        }

        inline row_iterator &operator -= (int i) noexcept  {

            col_ -= i;
            if (col_ < 0)  {
                row_ -= col_ / mptr_->cols();
                col_ %= mptr_->cols();
            }
            return (*this);
        }

        inline row_iterator operator + (size_type i) noexcept  {

            const size_type row = row_;
            const size_type col = col_;

            col_ += i;
            if (col_ >= mptr_->cols())  {
                row_ += col_ / mptr_->cols();
                col_ %= mptr_->cols();
            }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (row_iterator (mptr_, row, col));
        }

        inline row_iterator operator - (size_type i) noexcept  {

            const size_type row = row_;
            const size_type col = col_;

            col_ -= i;
            if (col_ < 0)  {
                row_ -= col_ / mptr_->cols();
                col_ %= mptr_->cols();
            }
            return (row_iterator (mptr_, row, col));
        }

        inline row_iterator operator + (int i) noexcept  {

            return (*this + size_type(i));
        }

        inline row_iterator operator - (int i) noexcept  {

            return (*this - size_type(i));
        }

        friend difference_type
        operator - (row_iterator lhs, row_iterator rhs) noexcept  {

            assert(lhs.mptr_ == rhs.mptr_);

            const size_type row_diff = lhs.row_ - rhs.row_;
            const size_type col_diff = lhs.col_ - rhs.col_;

            return (difference_type(
                        std::abs(row_diff) * rhs.mptr_->cols() - col_diff));
        }

    private:

        self_t      *mptr_ { nullptr };
        size_type   row_ { 0 };
        size_type   col_ { 0 };
    };

    class   col_iterator;
    class   col_const_iterator : public std::random_access_iterator_tag  {

    public:

        using iterator_category = std::random_access_iterator_tag;
        using value_type = T;
        using size_type = long;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using reference = value_type &;
        using const_reference = const value_type &;
        using difference_type = typename std::vector<T>::difference_type;

    public:

        inline col_const_iterator () = default;

        inline col_const_iterator(const self_t *m,
                                  size_type row = 0,
                                  size_type col = 0)
            : mptr_ (m), row_ (row), col_(col)  {   }

        inline bool operator == (const col_const_iterator &rhs) const  {

            return (mptr_ == rhs.mptr_ &&
                    row_ == rhs.row_ &&
                    col_ == rhs.col_);
        }
        inline bool operator != (const col_const_iterator &rhs) const  {

            return (! (*this == rhs));
        }
        inline bool
        operator > (const col_const_iterator &rhs) const noexcept  {

            return (col_ > rhs.col_ || (col_ == rhs.col_ && row_ > rhs.row_));
        }
        inline bool
        operator >= (const col_const_iterator &rhs) const noexcept  {

            return (col_ > rhs.col_ || (col_ == rhs.col_ && row_ >= rhs.row_));
        }
        inline bool
        operator < (const col_const_iterator &rhs) const noexcept  {

            return (! (*this >= rhs));
        }
        inline bool
        operator <= (const col_const_iterator &rhs) const noexcept  {

            return (! (*this > rhs));
        }

        // Following STL style, this iterator appears as a pointer
        // to value_type.
        //
        inline const_pointer operator -> () const noexcept  {

            return (&(mptr_->at(row_, col_)));
        }
        inline const_reference operator * () const noexcept  {

            return (mptr_->at(row_, col_));
        }
        inline operator const_pointer () const noexcept  {

            return (&(mptr_->at(row_, col_)));
        }

       // We are following STL style iterator interface.
       //
        inline col_const_iterator &operator ++ () noexcept  {    // ++Prefix

            row_ += 1;
            if (row_ >= mptr_->rows())  { row_ = 0; col_ += 1; }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (*this);
        }
        inline col_const_iterator operator ++ (int) noexcept  {  // Postfix++

            const size_type row = row_;
            const size_type col = col_;

            row_ += 1;
            if (row_ >= mptr_->rows())  { row_ = 0; col_ += 1; }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (col_const_iterator (mptr_, row, col));
        }

        inline col_const_iterator &operator += (size_type i) noexcept  {

            row_ += i;
            if (row_ >= mptr_->rows())  {
                col_ += row_ / mptr_->rows();
                row_ %= mptr_->rows();
            }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (*this);
        }

        inline col_const_iterator &operator -- () noexcept  {    // --Prefix

            row_ -= 1;
            if (row_ < 0)  { row_ = mptr_->rows() - 1; col_ -= 1; }
            return (*this);
        }
        inline col_const_iterator operator -- (int) noexcept  {  // Postfix--

            const size_type row = row_;
            const size_type col = col_;

            row_ -= 1;
            if (row_ < 0)  { row_ = mptr_->rows() - 1; col_ -= 1; }
            return (col_const_iterator (mptr_, row, col));
        }

        inline col_const_iterator &operator -= (int i) noexcept  {

            row_ -= i;
            if (row_ < 0)  {
                col_ -= row_ / mptr_->rows();
                row_ %= mptr_->rows();
            }
            return (*this);
        }

        inline col_const_iterator operator + (size_type i) noexcept  {

            const size_type row = row_;
            const size_type col = col_;

            row_ += i;
            if (row_ >= mptr_->rows())  {
                col_ += row_ / mptr_->rows();
                row_ %= mptr_->rows();
            }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (col_const_iterator (mptr_, row, col));
        }

        inline col_const_iterator operator - (size_type i) noexcept  {

            const size_type row = row_;
            const size_type col = col_;

            row_ -= i;
            if (row_ < 0)  {
                col_ -= row_ / mptr_->rows();
                row_ %= mptr_->rows();
            }
            return (col_const_iterator (mptr_, row, col));
        }

        inline col_const_iterator operator + (int i) noexcept  {

            return (*this + size_type(i));
        }

        inline col_const_iterator operator - (int i) noexcept  {

            return (*this - size_type(i));
        }

        friend difference_type
        operator - (col_const_iterator lhs, col_const_iterator rhs) noexcept  {

            assert(lhs.mptr_ == rhs.mptr_);

            const size_type row_diff = lhs.row_ - rhs.row_;
            const size_type col_diff = lhs.col_ - rhs.col_;

            return (difference_type(
                        std::abs(col_diff) * rhs.mptr_->rows() - row_diff));
        }

    private:

        const self_t    *mptr_ { nullptr };
        size_type       row_ { 0 };
        size_type       col_ { 0 };
    };

    // It goes through the matrix row-by-row starting at [0, 0]
    //
    class   col_iterator : public std::random_access_iterator_tag  {

    public:

        using iterator_category = std::random_access_iterator_tag;
        using value_type = T;
        using size_type = long;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using reference = value_type &;
        using const_reference = const value_type &;
        using difference_type = typename std::vector<T>::difference_type;

    public:

        inline col_iterator() = default;

        inline col_iterator (self_t *m, size_type row = 0, size_type col = 0)
            : mptr_ (m), row_ (row), col_(col)  {   }

        inline bool operator == (const col_iterator &rhs) const  {

            return (mptr_ == rhs.mptr_ &&
                    row_ == rhs.row_ &&
                    col_ == rhs.col_);
        }
        inline bool operator != (const col_iterator &rhs) const  {

            return (! (*this == rhs));
        }
        inline bool operator > (const col_iterator &rhs) const noexcept  {

            return (col_ > rhs.col_ || (col_ == rhs.col_ && row_ > rhs.row_));
        }
        inline bool operator >= (const col_iterator &rhs) const noexcept  {

            return (col_ > rhs.col_ || (col_ == rhs.col_ && row_ >= rhs.row_));
        }
        inline bool operator < (const col_iterator &rhs) const noexcept  {

            return (! (*this >= rhs));
        }
        inline bool operator <= (const col_iterator &rhs) const noexcept  {

            return (! (*this > rhs));
        }

        // Following STL style, this iterator appears as a pointer
        // to value_type.
        //
        inline pointer operator -> () const noexcept  {

            return (&(mptr_->at(row_, col_)));
        }
        inline reference operator * () const noexcept  {

            return (mptr_->at(row_, col_));
        }
        inline operator pointer () const noexcept  {

            return (&(mptr_->at(row_, col_)));
        }

       // We are following STL style iterator interface.
       //
        inline col_iterator &operator ++ () noexcept  {    // ++Prefix

            row_ += 1;
            if (row_ >= mptr_->rows())  { row_ = 0; col_ += 1; }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (*this);
        }
        inline col_iterator operator ++ (int) noexcept  {  // Postfix++

            const size_type row = row_;
            const size_type col = col_;

            row_ += 1;
            if (row_ >= mptr_->rows())  { row_ = 0; col_ += 1; }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (col_iterator (mptr_, row, col));
        }

        inline col_iterator &operator += (size_type i) noexcept  {

            row_ += i;
            if (row_ >= mptr_->rows())  {
                col_ += row_ / mptr_->rows();
                row_ %= mptr_->rows();
            }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (*this);
        }

        inline col_iterator &operator -- () noexcept  {    // --Prefix

            row_ -= 1;
            if (row_ < 0)  { row_ = mptr_->rows() - 1; col_ -= 1; }
            return (*this);
        }
        inline col_iterator operator -- (int) noexcept  {  // Postfix--

            const size_type row = row_;
            const size_type col = col_;

            row_ -= 1;
            if (row_ < 0)  { row_ = mptr_->rows() - 1; col_ -= 1; }
            return (col_iterator (mptr_, row, col));
        }

        inline col_iterator &operator -= (int i) noexcept  {

            row_ -= i;
            if (row_ < 0)  {
                col_ -= row_ / mptr_->rows();
                row_ %= mptr_->rows();
            }
            return (*this);
        }

        inline col_iterator operator + (size_type i) noexcept  {

            const size_type row = row_;
            const size_type col = col_;

            row_ += i;
            if (row_ >= mptr_->rows())  {
                col_ += row_ / mptr_->rows();
                row_ %= mptr_->rows();
            }
            if (col_ >= mptr_->cols() || row_ >= mptr_->rows())  {
                col_ = mptr_->cols();
                row_ = mptr_->rows();
            }
            return (col_iterator (mptr_, row, col));
        }

        inline col_iterator operator - (size_type i) noexcept  {

            const size_type row = row_;
            const size_type col = col_;

            row_ -= i;
            if (row_ < 0)  {
                col_ -= row_ / mptr_->rows();
                row_ %= mptr_->rows();
            }
            return (col_iterator (mptr_, row, col));
        }

        inline col_iterator operator + (int i) noexcept  {

            return (*this + size_type(i));
        }

        inline col_iterator operator - (int i) noexcept  {

            return (*this - size_type(i));
        }

        friend difference_type
        operator - (col_iterator lhs, col_iterator rhs) noexcept  {

            const size_type row_diff = lhs.row_ - rhs.row_;
            const size_type col_diff = lhs.col_ - rhs.col_;

            assert(lhs.mptr_ == rhs.mptr_);
            return (difference_type(
                        std::abs(col_diff) * rhs.mptr_->rows() - row_diff));
        }

    private:

        self_t      *mptr_ { nullptr };
        size_type   row_ { 0 };
        size_type   col_ { 0 };
    };

public:

    // Forwards
    //
    inline row_iterator row_begin() noexcept  {

        return (row_iterator(this, 0, 0));
    }
    inline row_const_iterator row_cbegin() const noexcept  {

        return (row_const_iterator(this, 0, 0));
    }
    inline row_iterator row_end() noexcept  {

        return (row_iterator(this, rows(), cols()));
    }
    inline row_const_iterator row_cend() const noexcept  {

        return (row_const_iterator(this, rows(), cols()));
    }

    inline col_iterator col_begin() noexcept  {

        return (col_iterator(this, 0, 0));
    }
    inline col_const_iterator col_cbegin() const noexcept  {

        return (col_const_iterator(this, 0, 0));
    }
    inline col_iterator col_end() noexcept  {

        return (col_iterator(this, rows(), cols()));
    }
    inline col_const_iterator col_cend() const noexcept  {

        return (col_const_iterator(this, rows(), cols()));
    }

    using reverse_row_iterator = std::reverse_iterator<row_iterator>;
    using reverse_row_const_iterator =
        std::reverse_iterator<row_const_iterator>;
    using reverse_col_iterator = std::reverse_iterator<col_iterator>;
    using reverse_col_const_iterator =
        std::reverse_iterator<col_const_iterator>;

    // Reverses
    //
    inline reverse_row_iterator row_rbegin() noexcept  {

        return (std::make_reverse_iterator(row_end()));
    }
    inline reverse_row_const_iterator row_crbegin() const noexcept  {

        return (std::make_reverse_iterator(row_cend()));
    }
    inline reverse_row_iterator row_rend() noexcept  {

        return (std::make_reverse_iterator(row_begin()));
    }
    inline reverse_row_const_iterator row_crend() const noexcept  {

        return (std::make_reverse_iterator(row_cbegin()));
    }

    inline reverse_col_iterator col_rbegin() noexcept  {

        return (std::make_reverse_iterator(col_end()));
    }
    inline reverse_col_const_iterator col_crbegin() const noexcept  {

        return (std::make_reverse_iterator(col_cend()));
    }
    inline reverse_col_iterator col_rend() noexcept  {

        return (std::make_reverse_iterator(col_begin()));
    }
    inline reverse_col_const_iterator col_crend() const noexcept  {

        return (std::make_reverse_iterator(col_cbegin()));
    }
};

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2>
static inline bool
operator != (const Matrix<T, MO1> &lhs, const Matrix<T, MO2> &rhs)  {

    if (lhs.rows() == rhs.rows() && lhs.cols() == rhs.cols())  {
        if constexpr (MO1 == matrix_orient::column_major)  {
            for (long c = 0; c < lhs.cols(); ++c)
                for (long r = 0; r < lhs.rows(); ++r)
                    if (lhs(r, c) != rhs(r, c))
                        return (true);
        }
        else  {
            for (long r = 0; r < lhs.rows(); ++r)
                for (long c = 0; c < lhs.cols(); ++c)
                    if (lhs(r, c) != rhs(r, c))
                        return (true);
        }
    }
    else  return (true);

    return (false);
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2>
static inline bool
operator == (const Matrix<T, MO1> &lhs, const Matrix<T, MO2> &rhs)  {

    return (! (lhs != rhs));
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2>
static inline Matrix<T, MO1>
operator + (const Matrix<T, MO1> &lhs, const Matrix<T, MO2> &rhs)  {

    assert(lhs.rows() == rhs.rows() && lhs.cols() == rhs.cols());

    auto    result = lhs;

    if constexpr (MO1 == matrix_orient::column_major)  {
        for (long c = 0; c < lhs.cols(); ++c)
            for (long r = 0; r < lhs.rows(); ++r)
                result(r, c) += rhs(r, c);
    }
    else  {
        for (long r = 0; r < lhs.rows(); ++r)
            for (long c = 0; c < lhs.cols(); ++c)
                result(r, c) += rhs(r, c);
    }
    return (result);
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2>
static inline Matrix<T, MO1>
operator - (const Matrix<T, MO1> &lhs, const Matrix<T, MO2> &rhs)  {

    assert(lhs.rows() == rhs.rows() && lhs.cols() == rhs.cols());

    auto    result = lhs;

    if constexpr (MO1 == matrix_orient::column_major)  {
        for (long c = 0; c < lhs.cols(); ++c)
            for (long r = 0; r < lhs.rows(); ++r)
                result(r, c) -= rhs(r, c);
    }
    else  {
        for (long r = 0; r < lhs.rows(); ++r)
            for (long c = 0; c < lhs.cols(); ++c)
                result(r, c) -= rhs(r, c);
    }
    return (result);
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2>
static inline Matrix<T, MO1> &
operator += (Matrix<T, MO1> &lhs, const Matrix<T, MO2> &rhs)  {

    assert(lhs.rows() == rhs.rows() && lhs.cols() == rhs.cols());

    if constexpr (MO1 == matrix_orient::column_major)  {
        for (long c = 0; c < lhs.cols(); ++c)
            for (long r = 0; r < lhs.rows(); ++r)
                lhs(r, c) += rhs(r, c);
    }
    else  {
        for (long r = 0; r < lhs.rows(); ++r)
            for (long c = 0; c < lhs.cols(); ++c)
                lhs(r, c) += rhs(r, c);
    }
    return (lhs);
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2>
static inline Matrix<T, MO1> &
operator -= (Matrix<T, MO1> &lhs, const Matrix<T, MO2> &rhs)  {

    assert(lhs.rows() == rhs.rows() && lhs.cols() == rhs.cols());

    if constexpr (MO1 == matrix_orient::column_major)  {
        for (long c = 0; c < lhs.cols(); ++c)
            for (long r = 0; r < lhs.rows(); ++r)
                lhs(r, c) -= rhs(r, c);
    }
    else  {
        for (long r = 0; r < lhs.rows(); ++r)
            for (long c = 0; c < lhs.cols(); ++c)
                lhs(r, c) -= rhs(r, c);
    }
    return (lhs);
}

// ----------------------------------------------------------------------------

// Naïve but cache friendly O(n^3) algorithm
//
template<typename T, matrix_orient MO1, matrix_orient MO2>
static Matrix<T, MO1>
operator * (const Matrix<T, MO1> &lhs, const Matrix<T, MO2> &rhs)  {


    const long  lhs_rows { lhs.rows() };
    const long  lhs_cols { lhs.cols() };
    const long  rhs_cols { rhs.cols() };

    assert(lhs_cols == rhs.rows());

    Matrix<T, MO1>  result { lhs_rows, rhs_cols };
    const long      thread_level =
        (lhs_cols >= 500L && rhs_cols >= 500L)
            ? ThreadGranularity::get_thread_level() : 0;

    auto    col_lbd =
        [lhs_rows, lhs_cols, &result,
         &lhs = std::as_const(lhs), &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            for (long c = begin; c < end; ++c)
                for (long r = 0; r < lhs_rows; ++r)
                    for (long k = 0; k < lhs_cols; ++k)
                        result(r, c) += lhs(k, r) * rhs(c, k);
        };
    auto    row_lbd =
        [lhs_cols, rhs_cols, &result,
         &lhs = std::as_const(lhs), &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            for (long r = begin; r < end; ++r)
                for (long c = 0; c < rhs_cols; ++c)
                    for (long k = 0; k < lhs_cols; ++k)
                        result(r, c) += lhs(r, k) * rhs(k, c);
        };

    if (thread_level > 2)  {
        std::vector<std::future<void>>  futures;

        if constexpr (MO1 == matrix_orient::column_major)
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, rhs_cols, std::move(col_lbd));
        else  // matrix_orient::row_major
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_rows, std::move(row_lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        if constexpr (MO1 == matrix_orient::column_major)
            col_lbd(0L, rhs_cols);
        else  // matrix_orient::row_major
            row_lbd(0L, lhs_rows);
    }

    return (result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/Matrix.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
