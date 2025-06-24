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

#include <DataFrame/DataFrameTypes.h>
#include <DataFrame/Utils/Threads/ThreadGranularity.h>

#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

enum class  matrix_orient : unsigned char  {

    column_major = 1,
    row_major = 2,
};

// ----------------------------------------------------------------------------

template<typename T,
         matrix_orient MO = matrix_orient::column_major,
         bool IS_SYM = false>
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

    template<typename M2>
    Matrix &operator = (const M2 &lhs);

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

    reference operator[] (size_type r, size_type c);
    const_reference operator[] (size_type r, size_type c) const;

    // This returns another matrix with top n-rows and n-columns of self.
    // Rows starts at top (index 0) and columns start at left (index 0).
    //
    Matrix corner(size_type nrows, size_type ncols) const;

    // Set the given column or row from the given iterator.
    // col_data/row_Data iterators must be valid for the length of
    // columns/rows.
    //
    template<typename I>
    void set_column(I col_data, size_type col);

    template<typename I>
    void set_row(I row_data, size_type row);

    // They return requested row or column in std::vector
    //
    std::vector<value_type> get_column_vec(size_type col) const noexcept;
    std::vector<value_type> get_row_vec(size_type row) const noexcept;

    // It returns requested column in a one-column matrix
    //
    Matrix<T, MO> get_column_mat(size_type col) const noexcept;

    // It returns requested row in a one-row matrix
    //
    Matrix<T, MO> get_row_mat(size_type row) const noexcept;

    bool is_square() const noexcept;

    // A matrix is either structurally symmetric (IS_SYM is true) or the data
    // in the matrix is symmetric. In both cases this returns true.
    //
    bool is_symmetric() const noexcept;

    trans_result_t transpose() const noexcept;
    Matrix transpose2() const noexcept;

    // Inverse(A) * A = A * Inverse(A) = I
    //
    // Also:
    //                       1
    //   Inverse(A) = ---------------- * Adjoint(A)
    //                 Determinant(A)
    //
    Matrix inverse() const;

    // Frobenius Norm:
    // The Frobenius norm of a matrix is the square root of the sum of
    // the squares of the values of the elements of the matrix.
    //
    value_type norm() const noexcept;

    // Mean of the entire values in the matrix
    //
    value_type mean() const noexcept;

    // Degree matrix is a square diagonal matrix where each diagonal value
    // represents the number of connections in a row of an adjacency matrix.
    //
    // Adjacency matrix (ADJ) is a square matrix that represents a connected
    // graph. A none zero value at i, j in an ADJ means there is an edge
    // between vertices i and j. ADJ may contain only 0s and 1s indicating
    // absence or presence of edges between vertices. Or it may contain real
    // numbers which are the weights of edges between vertices. A 0 weight
    // means there is no edge.
    //
    // degree_matrix() assumes self is an Adjacency matrix.
    //
    Matrix degree_matrix() const;

    // A Laplacian matrix is a square matrix that represents a graph, where
    // each entry signifies the relationship between vertices based on their
    // connections, essentially showing how much a vertex differs from its
    // neighbors, and is calculated by subtracting the adjacency matrix from
    // the degree matrix of the graph; it's often used in graph theory and
    // related fields like machine learning to analyze graph properties and
    // structure based on its eigenvalues and eigenvectors.
    //
    // laplacian_matrix() assumes self is an Adjacency matrix.
    //
    Matrix laplacian_matrix() const;

    // Variance/Covariance matrix.
    // The columns of the matrix are assumed to be observations of some
    // random variable. So the covariance matrix is a square matrix
    // containing the covariances of the above random variables.
    //
    // If it is unbiased the estimate is divided by n - 1, otherwise by n,
    // n being the number of rows.
    //
    // The reason for dividing by n - 1 is because we are dividing by
    // degrees of freedom (i.e. number of independent observations).
    // For example, if we have two observations, when calculating the mean
    // we have two independent observations; however, when calculating the
    // variance, we have only one independent observation, since the two
    // observations are equally distant from the mean.
    //
    // For a nXm matrix, you will get a mXm covariance matrix
    //
    Matrix
    covariance(bool is_unbiased = true) const;

    // Let A be an nXn matrix. The number l is an eigenvalue of A if there
    // exists a non-zero vector v such that
    //     Av = lv
    // In this case, vector v is called an eigenvector of A corresponding
    // to l. For each eigenvalue l, the set of all vectors v satisfying
    // Av = lv is called the eigenspace of A corresponding to l.
    //
    // Putting it in more laymen's term:
    //
    // Place a coin on its side and set it spinning. Take some butter and
    // spread it over a slice of bread. Pick up an elastic band and stretch
    // it. In each of these cases we do something that affects the shape or
    // orientation of the object in question. The elastic band and butter
    // have been deformed and the coin has been rotated.
    // When we investigate transformations mathematically we find that there
    // are directions that remain the same after the deformation has
    // occurred. In the case of the stretching of the elastic band, if you
    // had drawn an arrow on the band before you stretched it, would it
    // point in the same direction afterwards? The answer depends on how
    // you drew the original arrow. The transformation preserves the
    // direction in which you stretch the band but some other arrows have
    // their direction changed. The preserved direction is called an
    // EIGENVECTOR of the transformation and the associated amount by which
    // it has been stretched is an EIGENVALUE.
    // Eigenvalues are multipliers. They are numbers that represent how much
    // stretching has taken place or, in other words, how much something has
    // been scaled up by. In the sentence 'I am 3.2 times taller than when
    // I was born' the number 3.2 is acting as an eigenvalue.
    // To make sense of an eigenvalue it must have an associated 'operation'
    // (the transformation that has occurred) and an associated 'direction'
    // (the eigenvector). It doesn't mean anything by itself to say that 3.2
    // is an eigenvalue. You need to know the operation 'enlarged' and the
    // direction 'up' to say that I am now 3.2 times bigger in the
    // up-direction!
    // If you rotate a coin by 360 degrees you preserve all directions and
    // so each direction is an eigenvector. Because no stretching has
    // occurred, all of these eigenvectors have eigenvalue 1. Rotating the
    // coin by 60 degrees destroys all directions and this transformation
    // has no eigenvectors or eigenvalues at all. Careful spreading of
    // butter on bread, by moving the knife in one direction only, is a
    // transformation with an eigenvector in the direction of spreading.
    // The associated eigenvalue depends on how far the butter is spread.
    // The elastic band has an eigenvector in the left-right direction
    // because that arrow still points in that direction after the
    // transformation. The band has been stretched by approximately the same
    // amount that I have grown since I was born - so the eigenvector has
    // eigenvalue 3.2!
    // The idea of eigenvalues and eigenvectors can be extended to any
    // operator H. (H might be 'rotate by 360 degrees' or 'stretch in
    // direction of y-axis' or operators in Quantum theory or elsewhere).
    // We write H(x) to mean 'the action of H on x'. (So x might be a
    // particular vector that we are rotating or stretching or it might be a
    // quantum state or some other object). If we can find an object x and a
    // number k so that the following equation is true:
    //     H(x) = k * x
    // Then we know that x has been preserved by H apart from a scalar
    // multiplier k. It is k times bigger than what it was before H acted
    // upon it. Therefore we call x an eigenvector of H with eigenvalue k.
    //
    //
    // This method finds all the eigenvalues and eigenvectors.
    // If matrix is symmetric:
    //     first tridiagonalize, then diagonalize.
    // else:
    //     reduce to Hessenberg form, then reduce to real Schur form.
    //
    template<typename MA1, typename MA2>
    void eigen_space(MA1 &eigenvalues,
                     MA2 &eigenvectors,
                     bool sort_values) const;

    // In linear algebra, the Singular Value Decomposition (SVD) is an
    // important factorization of a rectangular real or complex matrix,
    // with several applications in signal processing and statistics.
    // Applications which employ the SVD include computing the
    // pseudoinverse, matrix approximation, and determining the rank,
    // range and null space of a matrix.
    //
    // Suppose M is an mXn matrix whose entries come from the field K,
    // which is either the field of real numbers or the field of complex
    // numbers. Then there exists a factorization of the form
    //     M = U*Σ*~V
    //
    // where U is an mXm unitary matrix over K, the matrix Σ is mXn
    // with nonnegative numbers on the diagonal (as defined for a
    // rectangular matrix) and zeros off the diagonal, and ~V denotes the
    // conjugate transpose of V (transpose of V in case of real matrices),
    // an nXn unitary matrix over K. Such a factorization is called a
    // Singular Value Decomposition of M.
    //
    //  -- The matrix V thus contains a set of orthonormal "input" or
    //     "analysing" basis vector directions for M
    //  -- The matrix U contains a set of orthonormal "output" basis vector
    //     directions for M
    //  -- The matrix Σ contains the singular values, which can be thought
    //     of as scalar "gain controls" by which each corresponding input
    //     is multiplied to give a corresponding output.
    //
    // A common convention is to order the values Σi,i in non-increasing
    // fashion. In this case, the diagonal matrix Σ is uniquely determined
    // by M (though the matrices U and V are not).
    //
    template<typename MA1, typename MA2, typename MA3>
    inline void svd(MA1 &U, MA2 &S, MA3 &V, bool full_size = true) const;

    // In linear algebra, the LU decomposition is a matrix decomposition
    // which writes a matrix as the product of a lower and upper triangular
    // amtrices. This decomposition is used in numerical analysis to solve
    // systems of linear equations or calculate the determinant.
    //
    // LU decomposition of a square matrix A is a decomposition of A as
    //     A = LU
    //
    // where L and U are lower and upper triangular matrices (of the same
    // size), respectively. This means that L has only zeros above the
    // diagonal and U has only zeros below the diagonal.
    //
    // The LU decomposition is twice as efficient as the QR decomposition.
    //
    // NOTE: The LU decompostion with pivoting always exists, even if the
    //       matrix is singular. The primary use of the LU decomposition is
    //       in the solution of square systems of simultaneous linear
    //       equations. This will fail if is_singular() returns false.
    //
    template<typename MA1, typename MA2>
    inline void lud(MA1 &L, MA2 &U) const;

    // In 2-D, when you talk about the point (2, 4), you can think of the
    // 2 and 4 as directions to get from the origin to the point
    // (move 2 units in the x direction and 4 in the y direction).  In
    // a 3-D system, the same idea holds - (1, 3, 7) means start at the
    // origin (0, 0, 0), go 1 unit in the x direction, 3 in the y direction,
    // and 7 in the z direction.
    // The determinant of a 1x1 matrix is the signed length of the line
    // from the origin to the point. It's positive if the point is in the
    // positive x direction, negative if in the other direction.
    // In 2-D, look at the matrix as two 2-dimensional points on the plane,
    // and complete the parallelogram that includes those two points and
    // the origin. The (signed) area of this parallelogram is the
    // determinant.  If you sweep clockwise from the first to the second,
    // the determinant is negative, otherwise, positive.
    // In 3-D, look at the matrix as 3 3-dimensional points in space.
    // Complete the parallepipe that includes these points and the origin,
    // and the determinant is the (signed) volume of the parallelepipe.
    // The same idea works in any number of dimensions.  The determinant
    // is just the (signed) volume of the n-dimensional parallelepipe.
    // Note that length, area, volume are the _volumes_ in 1, 2, and
    // 3-dimensional spaces.  A similar concept of volume exists for
    // Euclidean space of any dimensionality.
    //
    // [HM]: A matrix that has determinant equals zero is singular.
    //       It means it has at least 2 vectors that are linearly
    //       dependent, therefore they cannot enclose an area or a space.
    //
    // For how determinant of N X N matrices are calulated
    //     see: http://mathworld.wolfram.com/Determinant.html
    // For Laplace Expension
    //     see: http://en.wikipedia.org/wiki/Cofactor_expansion
    //
    // NOTE: This is a relatively _expensive_ calculation. Its complexity
    //       is O(n!), where n is the number of rows or columns.
    //
    inline value_type determinant() const;

    // Minor of a matrix is the same matrix with the specified row
    // and column taken out.
    //
    // NOTE: Linear algebrically speaking, a minor of a matrix is the
    //       _detereminant_ of some smaller square matrix, cut down from
    //       the original matrix.
    //
    template<typename MA>
    inline MA &
    get_minor(MA &mmatrix, size_type drow, size_type dcol) const noexcept;

    // A Cofactor of a matrix is the determinant of a minor of the matrix.
    // You can also say cofactor is the signed minor of the matrix.
    //
    inline value_type cofactor(size_type row, size_type column) const;

    // The Adjoint of a matrix is formed by taking the transpose of the
    // cofactors matrix of the original matrix.
    //
    template<typename MA>
    inline MA &adjoint(MA &amatrix) const;

    // A "centered matrix" is a matrix where the mean of each column has been
    // adjusted to be zero, meaning that the data within each column is
    // centered around the value of zero
    //
    inline void center() noexcept;
    template<typename MA>
    inline void get_centered(MA &cmatrix) const noexcept;

    // The square root of a matrix, in the context of linear algebra, is
    // another matrix that, when multiplied by itself, results in the original
    // matrix. It's not simply element-wise square rooting. It's a more
    // involved process involving techniques like diagonalization or
    // Cholesky decomposition.
    // NOTE: Matrix must be symmetric positive semi-definite (PSD), otherwise
    //       you may have negatve Eigen values which results in NaN values.
    //       A good acceptable input is a convariance matrix.
    //
    Matrix &sqrt() noexcept;
    template<typename MA>
    inline MA &get_sqrt(MA &that) const noexcept;

    // Whitening a data matrix, also known as sphering, is a data preprocessing
    // technique that transforms the data so that its covariance matrix becomes
    // the identity matrix. This process effectively decorrelates the features
    // and ensures they have equal variance. In simpler terms, it changes the
    // input vector into a white noise vector, where all frequencies have the
    // same amplitude.
    // If center is false, it means self is already centered (mean is
    // subtracted).
    //
    Matrix &whiten(bool center = false) noexcept;
    template<typename MA>
    inline MA &get_whiten(MA &that, bool center = false) const noexcept;

    // The following methods apply the corresponding functions to all matrix
    // elements one by one. They return a reference to self.
    // ew = Element Wise
    //
    Matrix &ew_square() noexcept;
    Matrix &ew_cube() noexcept;
    Matrix &ew_sqrt() noexcept;
    Matrix &ew_tanh() noexcept;
    Matrix &ew_inverse() noexcept;  // 1/x
    Matrix &ew_add(value_type val) noexcept;
    Matrix &ew_minus(value_type val) noexcept;
    Matrix &ew_multiply(value_type val) noexcept;
    Matrix &ew_divide(value_type val) noexcept;

private:

    static constexpr size_type  NOPOS_ = static_cast<size_type>(-9);

    using storage_t = std::vector<value_type>;

    inline static constexpr double  EPSILON_ { double(2.220446e-16) };

    // Partial pivoting for Gaussian elimination:
    //
    // Pivoting is a process performed on a matrix in order to improve
    // numerical stability. Partial pivoting of an nXn matrix is the
    // sorting of the rows of the matrix so that row r contains the maximum
    // absolute column value for column c, among all rows r ... n.
    //
    inline size_type
    ppivot_(size_type pivot_row,
            size_type self_rows,
            size_type self_cols) noexcept;

    // Symmetric Householder reduction to tridiagonal form.
    //
    // This is derived from the Algol procedures tred2 by Bowdler, Martin,
    // Reinsch, and Wilkinson, Handbook for Auto. Comp., Vol.ii-Linear
    // Algebra, and the corresponding Fortran subroutine in EISPACK.
    //
    template<typename MA1, typename MA2, typename MA3>
    static inline void
    tridiagonalize_(MA1 &e_vecs, MA2 &e_vals, MA3 &imagi) noexcept;

    // Symmetric tridiagonal QL algorithm.
    //
    // This is derived from the Algol procedures tql2 by Bowdler, Martin,
    // Reinsch, and Wilkinson, Handbook for Auto. Comp., Vol.ii-Linear
    // Algebra, and the corresponding Fortran subroutine in EISPACK.
    //
    template<typename MA1, typename MA2, typename MA3>
    static inline void
    diagonalize_ (MA1 &e_vecs, MA2 &e_vals, MA3 &imagi) noexcept;

    // Nonsymmetric reduction to Hessenberg form.
    //
    // This is derived from the Algol procedures orthes and ortran, by
    // Martin and Wilkinson, Handbook for Auto. Comp., Vol.ii-Linear
    // Algebra, and the corresponding Fortran subroutines in EISPACK.
    //
    template<typename MA1, typename MA2>
    static inline void
    red_to_hessenberg_(MA1 &e_vecs, MA2 &hess_form) noexcept;

    // Nonsymmetric reduction from Hessenberg to real Schur form.
    //
    // This is derived from the Algol procedure hqr2, by Martin and
    // Wilkinson, Handbook for Auto. Comp., Vol.ii-Linear Algebra, and the
    // corresponding Fortran subroutines in EISPACK.
    //
    template<typename MA1, typename MA2, typename MA3, typename MA4>
    static inline void
    hessenberg_to_schur_(MA1 &e_vecs,
                         MA2 &e_vals,
                         MA3 &imagi,
                         MA4 &hess_form) noexcept;

     // It returns the quotient of two complex numbers:
     // (a + ib) / (c + id)
     //
     // Admittedly this method doesn't belong here
     //
     static inline void
     cdiv_(value_type xr,
           value_type xi,
           value_type yr,
           value_type yi,
           value_type &cdivr,
           value_type &cdivi) noexcept;

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
        operator - (row_const_iterator lhs, row_const_iterator rhs)  {

            if (lhs.mptr_ != rhs.mptr_)
                throw NotFeasible("- operation in iterator is not feasible");

            const size_type row_diff = lhs.row_ - rhs.row_;
            const size_type col_diff = lhs.col_ - rhs.col_;

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
        operator - (row_iterator lhs, row_iterator rhs)  {

            if (lhs.mptr_ != rhs.mptr_)
                throw NotFeasible(
                    "- operation between iterators is not feasible");

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

            if (lhs.mptr_ != rhs.mptr_)
                throw NotFeasible(
                    "- operation between iterators is not feasible");

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
        operator - (col_iterator lhs, col_iterator rhs)  {

            if (lhs.mptr_ != rhs.mptr_)
                throw NotFeasible(
                    "- operation between iterators is not feasible");

            const size_type row_diff = lhs.row_ - rhs.row_;
            const size_type col_diff = lhs.col_ - rhs.col_;

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

template<typename T, matrix_orient MO1, matrix_orient MO2,
         bool IS_SYM1, bool IS_SYM2>
static inline bool
operator != (const Matrix<T, MO1, IS_SYM1> &lhs,
             const Matrix<T, MO2, IS_SYM2> &rhs)  {

    if (lhs.rows() == rhs.rows() && lhs.cols() == rhs.cols())  {
        if constexpr (IS_SYM1 && IS_SYM2)  {
            if constexpr (MO1 == matrix_orient::column_major)  {
                for (long c = 0; c < lhs.cols(); ++c)
                    for (long r = c; r < lhs.rows(); ++r)
                        if (lhs(r, c) != rhs(r, c))
                            return (true);
            }
            else  {
                for (long r = 0; r < lhs.rows(); ++r)
                    for (long c = r; c < lhs.cols(); ++c)
                        if (lhs(r, c) != rhs(r, c))
                            return (true);
            }
        }
        else  {
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
    }
    else  return (true);

    return (false);
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2,
         bool IS_SYM1, bool IS_SYM2>
static inline bool
operator == (const Matrix<T, MO1, IS_SYM1> &lhs,
             const Matrix<T, MO2, IS_SYM2> &rhs)  {

    return (! (lhs != rhs));
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2,
         bool IS_SYM1, bool IS_SYM2>
static inline typename std::conditional<IS_SYM1 && IS_SYM2,
                                        Matrix<T, MO1, true>,
                                        Matrix<T, MO1, false>>::type
operator + (const Matrix<T, MO1, IS_SYM1> &lhs,
            const Matrix<T, MO2, IS_SYM2> &rhs)  {

    using result_t =
        typename std::conditional<IS_SYM1 && IS_SYM2,
                                  Matrix<T, MO1, true>,
                                  Matrix<T, MO1, false>>::type;

    const long  lhs_rows = lhs.rows();
    const long  lhs_cols = lhs.cols();

#ifdef HMDF_SANITY_EXCEPTIONS
    if (lhs_rows != rhs.rows() || lhs_cols != rhs.cols())
        throw NotFeasible(
            "+ operation between these two metrices is not feasible");
#endif // HMDF_SANITY_EXCEPTIONS

    result_t    result;

    if constexpr (IS_SYM1 && (! IS_SYM2))  {
        result.resize(lhs.rows(), lhs.cols());
        for (long r = 0; r < lhs.rows(); ++r)
            for (long c = 0; c < lhs.cols(); ++c)
                result(r, c) = lhs(r, c);
    }
    else  {
        result = lhs;
    }

    auto        col_lbd =
        [lhs_rows, &result, &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            if constexpr (IS_SYM1 && IS_SYM2)  {
                for (long c = begin; c < end; ++c)
                    for (long r = c; r < lhs_rows; ++r)
                        result(r, c) += rhs(r, c);
            }
            else  {
                for (long c = begin; c < end; ++c)
                    for (long r = 0; r < lhs_rows; ++r)
                        result(r, c) += rhs(r, c);
            }
        };
    auto        row_lbd =
        [lhs_cols, &result, &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            if constexpr (IS_SYM1 && IS_SYM2)  {
                for (long r = begin; r < end; ++r)
                    for (long c = r; c < lhs_cols; ++c)
                        result(r, c) += rhs(r, c);
            }
            else  {
                for (long r = begin; r < end; ++r)
                    for (long c = 0; c < lhs_cols; ++c)
                        result(r, c) += rhs(r, c);
            }
        };
    const long  thread_level =
        (lhs_cols >= 2000L || lhs_rows >= 2000L)
            ? ThreadGranularity::get_thread_level() : 0;

    if (thread_level > 2)  {
        std::vector<std::future<void>>  futures;

        if constexpr (MO1 == matrix_orient::column_major)
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_cols, std::move(col_lbd));
        else  // matrix_orient::row_major
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_rows, std::move(row_lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        if constexpr (MO1 == matrix_orient::column_major)
            col_lbd(0L, lhs_cols);
        else  // matrix_orient::row_major
            row_lbd(0L, lhs_rows);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2,
         bool IS_SYM1, bool IS_SYM2>
static inline typename std::conditional<IS_SYM1 && IS_SYM2,
                                        Matrix<T, MO1, true>,
                                        Matrix<T, MO1, false>>::type
operator - (const Matrix<T, MO1, IS_SYM1> &lhs,
            const Matrix<T, MO2, IS_SYM2> &rhs)  {

    using result_t =
        typename std::conditional<IS_SYM1 && IS_SYM2,
                                  Matrix<T, MO1, true>,
                                  Matrix<T, MO1, false>>::type;

    const long  lhs_rows = lhs.rows();
    const long  lhs_cols = lhs.cols();

#ifdef HMDF_SANITY_EXCEPTIONS
    if (lhs_rows != rhs.rows() || lhs_cols != rhs.cols())
        throw NotFeasible(
            "- operation between these two metrices is not feasible");
#endif // HMDF_SANITY_EXCEPTIONS

    result_t    result;

    if constexpr (IS_SYM1 && (! IS_SYM2))  {
        result.resize(lhs.rows(), lhs.cols());
        for (long r = 0; r < lhs.rows(); ++r)
            for (long c = 0; c < lhs.cols(); ++c)
                result(r, c) = lhs(r, c);
    }
    else  {
        result = lhs;
    }

    auto        col_lbd =
        [lhs_rows, &result, &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            if constexpr (IS_SYM1 && IS_SYM2)  {
                for (long c = begin; c < end; ++c)
                    for (long r = c; r < lhs_rows; ++r)
                        result(r, c) -= rhs(r, c);
            }
            else  {
                for (long c = begin; c < end; ++c)
                    for (long r = 0; r < lhs_rows; ++r)
                        result(r, c) -= rhs(r, c);
            }
        };
    auto        row_lbd =
        [lhs_cols, &result, &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            if constexpr (IS_SYM1 && IS_SYM2)  {
                for (long r = begin; r < end; ++r)
                    for (long c = r; c < lhs_cols; ++c)
                        result(r, c) -= rhs(r, c);
            }
            else  {
                for (long r = begin; r < end; ++r)
                    for (long c = 0; c < lhs_cols; ++c)
                        result(r, c) -= rhs(r, c);
            }
        };
    const long  thread_level =
        (lhs_cols >= 2000L || lhs_rows >= 2000L)
            ? ThreadGranularity::get_thread_level() : 0;

    if (thread_level > 2)  {
        std::vector<std::future<void>>  futures;

        if constexpr (MO1 == matrix_orient::column_major)
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_cols, std::move(col_lbd));
        else  // matrix_orient::row_major
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_rows, std::move(row_lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        if constexpr (MO1 == matrix_orient::column_major)
            col_lbd(0L, lhs_cols);
        else  // matrix_orient::row_major
            row_lbd(0L, lhs_rows);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2,
         bool IS_SYM1, bool IS_SYM2>
Matrix<T, MO1, IS_SYM1> &
operator += (Matrix<T, MO1, IS_SYM1> &lhs,
             const Matrix<T, MO2, IS_SYM2> &rhs)  {

    static_assert((IS_SYM1 && IS_SYM2) ||
                  ((! IS_SYM1) && IS_SYM2) ||
                  ((! IS_SYM1) && (! IS_SYM2)),
                  "+= operation between these two metrices is not feasible");

    const long  lhs_rows = lhs.rows();
    const long  lhs_cols = lhs.cols();

#ifdef HMDF_SANITY_EXCEPTIONS
    if (lhs_rows != rhs.rows() || lhs_cols != rhs.cols())
        throw NotFeasible(
            "+= operation between these two metrices is not feasible");
#endif // HMDF_SANITY_EXCEPTIONS

    auto        col_lbd =
        [lhs_rows, &lhs, &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            if constexpr (IS_SYM1 && IS_SYM2)  {
                for (long c = begin; c < end; ++c)
                    for (long r = c; r < lhs_rows; ++r)
                        lhs(r, c) += rhs(r, c);
            }
            else  {
                for (long c = begin; c < end; ++c)
                    for (long r = 0; r < lhs_rows; ++r)
                        lhs(r, c) += rhs(r, c);
            }
        };
    auto        row_lbd =
        [lhs_cols, &lhs, &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            if constexpr (IS_SYM1 && IS_SYM2)  {
                for (long r = begin; r < end; ++r)
                    for (long c = r; c < lhs_cols; ++c)
                        lhs(r, c) += rhs(r, c);
            }
            else  {
                for (long r = begin; r < end; ++r)
                    for (long c = 0; c < lhs_cols; ++c)
                        lhs(r, c) += rhs(r, c);
            }
        };
    const long  thread_level =
        (lhs_cols >= 2000L || lhs_rows >= 2000L)
            ? ThreadGranularity::get_thread_level() : 0;

    if (thread_level > 2)  {
        std::vector<std::future<void>>  futures;

        if constexpr (MO1 == matrix_orient::column_major)
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_cols, std::move(col_lbd));
        else  // matrix_orient::row_major
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_rows, std::move(row_lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        if constexpr (MO1 == matrix_orient::column_major)
            col_lbd(0L, lhs_cols);
        else  // matrix_orient::row_major
            row_lbd(0L, lhs_rows);
    }

    return (lhs);
}

// ----------------------------------------------------------------------------

template<typename T, matrix_orient MO1, matrix_orient MO2,
         bool IS_SYM1, bool IS_SYM2>
Matrix<T, MO1, IS_SYM1> &
operator -= (Matrix<T, MO1, IS_SYM1> &lhs,
             const Matrix<T, MO2, IS_SYM2> &rhs)  {

    static_assert((IS_SYM1 && IS_SYM2) ||
                  ((! IS_SYM1) && IS_SYM2) ||
                  ((! IS_SYM1) && (! IS_SYM2)),
                  "-= operation between these two metrices is not feasible");

    const long  lhs_rows = lhs.rows();
    const long  lhs_cols = lhs.cols();

#ifdef HMDF_SANITY_EXCEPTIONS
    if (lhs_rows != rhs.rows() || lhs_cols != rhs.cols())
        throw NotFeasible(
            "-= operation between these two metrices is not feasible");
#endif // HMDF_SANITY_EXCEPTIONS

    auto        col_lbd =
        [lhs_rows, &lhs, &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            if constexpr (IS_SYM1 && IS_SYM2)  {
                for (long c = begin; c < end; ++c)
                    for (long r = c; r < lhs_rows; ++r)
                        lhs(r, c) -= rhs(r, c);
            }
            else  {
                for (long c = begin; c < end; ++c)
                    for (long r = 0; r < lhs_rows; ++r)
                        lhs(r, c) -= rhs(r, c);
            }
        };
    auto        row_lbd =
        [lhs_cols, &lhs, &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            if constexpr (IS_SYM1 && IS_SYM2)  {
                for (long r = begin; r < end; ++r)
                    for (long c = r; c < lhs_cols; ++c)
                        lhs(r, c) -= rhs(r, c);
            }
            else  {
                for (long r = begin; r < end; ++r)
                    for (long c = 0; c < lhs_cols; ++c)
                        lhs(r, c) -= rhs(r, c);
            }
        };
    const long  thread_level =
        (lhs_cols >= 2000L || lhs_rows >= 2000L)
            ? ThreadGranularity::get_thread_level() : 0;

    if (thread_level > 2)  {
        std::vector<std::future<void>>  futures;

        if constexpr (MO1 == matrix_orient::column_major)
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_cols, std::move(col_lbd));
        else  // matrix_orient::row_major
            futures = ThreadGranularity::thr_pool_.parallel_loop(
                          0L, lhs_rows, std::move(row_lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        if constexpr (MO1 == matrix_orient::column_major)
            col_lbd(0L, lhs_cols);
        else  // matrix_orient::row_major
            row_lbd(0L, lhs_rows);
    }

    return (lhs);
}

// ----------------------------------------------------------------------------

// Naïve but cache friendly O(n^3) algorithm
//
template<typename T, matrix_orient MO1, matrix_orient MO2,
         bool IS_SYM1, bool IS_SYM2>
static Matrix<T, MO1, false>
operator * (const Matrix<T, MO1, IS_SYM1> &lhs,
            const Matrix<T, MO2, IS_SYM2> &rhs)  {

    const long  lhs_rows { lhs.rows() };
    const long  lhs_cols { lhs.cols() };
    const long  rhs_cols { rhs.cols() };

#ifdef HMDF_SANITY_EXCEPTIONS
    if (lhs_cols != rhs.rows())
        throw NotFeasible(
            "* operation between these two metrices is not feasible");
#endif // HMDF_SANITY_EXCEPTIONS

    Matrix<T, MO1, false>   result { lhs_rows, rhs_cols, 0 };
    const long              thread_level =
        (lhs_cols >= 400L || rhs_cols >= 400L)
            ? ThreadGranularity::get_thread_level() : 0;

    auto    col_lbd =
        [lhs_rows, lhs_cols, &result,
         &lhs = std::as_const(lhs), &rhs = std::as_const(rhs)]
        (auto begin, auto end) -> void  {
            for (long c = begin; c < end; ++c)
                for (long r = 0; r < lhs_rows; ++r)
                    for (long k = 0; k < lhs_cols; ++k)
                        result(r, c) += lhs(r, k) * rhs(k, c);
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
