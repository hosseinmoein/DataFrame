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
bool
Matrix<T, MO>::is_square() const noexcept  { return (rows() == cols()); }

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
bool
Matrix<T, MO>::is_symmetric() const noexcept  {

    if (! is_square ())
        return (false);

    for (size_type r = 1; r < rows(); ++r)
        for (size_type c = 0; c < r; ++c)
            if (at(r, c) != at(c, r))
                return (false);

    return (true);
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
inline Matrix<T, MO>::size_type
Matrix<T, MO>::ppivot_(size_type pivot_row,
                       size_type self_rows,
                       size_type self_cols) noexcept  {

    size_type   max_row { pivot_row };
    value_type  max_value { value_type(std::fabs(at(pivot_row, pivot_row))) };

    for (size_type r = pivot_row + 1; r < self_rows; ++r)  {
        const value_type    tmp { value_type(std::fabs(at(r, pivot_row))) };

        if (tmp > max_value && tmp != value_type(0))  {
            max_value = tmp;
            max_row = r;
        }
    }

    if (at(max_row, pivot_row) == value_type(0))
        return (NOPOS_);

    if (max_row != pivot_row)  {
        for (size_type c = 0; c < self_cols; ++c)
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

    const size_type self_rows = rows();
    const size_type self_cols = cols();
    Matrix          aux_mat { *this };
    Matrix          result { self_rows, self_cols, 0 };

    // First make identity matrix
    //
    for (size_type d = 0; d < self_cols; ++d)
        result.at(d, d) = value_type(1);

    for (size_type r = 0; r < self_rows; ++r)  {
        const size_type idx = aux_mat.ppivot_(r, self_rows, self_cols);

        if (idx == NOPOS_)
            throw DataFrameError("Matrix::inverse(): Singular matrix");

        if (idx != 0)
            for (size_type c = 0; c < self_cols; ++c)
                std::swap(result.at(r, c), result.at(idx, c));

        const value_type    diag = aux_mat.at(r, r);

        for (size_type c = 0; c < self_cols; ++c)  {
            aux_mat.at(r, c) /= diag;
            result.at(r, c) /= diag;
        }

        for (size_type r2 = 0; r2 < self_rows; ++r2)  {
            if (r2 != r)  {
                const value_type    off_diag = aux_mat.at(r2, r);

                for (size_type c = 0; c < self_cols; ++c)  {
                    aux_mat.at(r2, c) -= off_diag * aux_mat.at(r, c);
                    result.at(r2, c) -= off_diag * result.at(r, c);
                }
            }
        }
    }

    return (result);
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
template<typename MA1, typename MA2>
void Matrix<T, MO>::
red_to_hessenberg_(MA1 &e_vecs, MA2 &hess_form) noexcept  {

    Matrix<T, matrix_orient::row_major> ortho { 1, e_vecs.cols() };

    for (size_type c = 1; c <= e_vecs.cols() - 2; ++c)  {
        value_type  scale { 0 };

        // Scale column.
        //
        for (size_type r = c; r <= e_vecs.rows() - 1; ++r)
            scale += std::fabs(hess_form (r, c - 1));

        if (scale != value_type(0))  {
            value_type  h { 0 };

            // Compute Householder transformation.
            //
            for (size_type cc = e_vecs.cols() - 1; cc >= c; --cc)  {
                const auto  val = hess_form(cc, c - 1) / scale;

                ortho(0, cc) = val;
                h += val * val;
            }

            const value_type    g =
                ortho(0, c) > value_type(0) ? -std::sqrt(h) : std::sqrt(h);

            h -= ortho(0, c) * g;
            ortho(0, c) -= g;

            // Apply Householder similarity transformation
            // H = (I - u * u' / h) * H * (I - u * u') / h)
            //
            for (size_type cc = c; cc < e_vecs.cols(); ++cc)  {
                value_type  f { 0 };

                for (size_type r = e_vecs.rows() - 1; r >= c; --r)
                    f += ortho(0, r) * hess_form(r, cc);
                f /= h;

                for (size_type r = c; r <= e_vecs.rows() - 1; ++r)
                    hess_form(r, cc) -= f * ortho(0, r);
            }

            for (size_type r = 0; r <= e_vecs.rows() - 1; ++r)  {
                value_type  f { 0 };

                for (size_type cc = e_vecs.cols() - 1; cc >= c; --cc)
                    f += ortho(0, cc) * hess_form(r, cc);
                f /= h;

                for (size_type cc = c; cc <= e_vecs.cols() - 1; ++cc)
                    hess_form(r, cc) -= f * ortho(0, cc);
            }

            ortho(0, c) *= scale;
            hess_form(c, c - 1) = scale * g;
        }
    }

    // Accumulate transformations (Algol's ortran).
    //
    for (size_type r = 0; r < e_vecs.rows(); ++r)
        for (size_type c = 0; c < e_vecs.cols(); ++c)
            e_vecs(r, c) = r == c ? value_type(1) : value_type(0);

    for (size_type c = e_vecs.cols() - 2; c >= 1; --c)
        if (hess_form(c, c - 1) != value_type(0))  {
            for (size_type r = c + 1; r <= e_vecs.rows() - 1; ++r)
                ortho(0, r) = hess_form(r, c - 1);

            for (size_type cc = c; cc <= e_vecs.cols() - 1; ++cc)  {
                value_type  g { 0 };

                for (size_type r = c; r <= e_vecs.rows() - 1; ++r)
                    g += ortho(0, r) * e_vecs(r, cc);

                // Double division avoids possible underflow
                //
                g = (g / ortho(0, c)) / hess_form(c, c - 1);
                for (size_type r = c; r <= e_vecs.rows() - 1; ++r)
                    e_vecs(r, cc) += g * ortho(0, r);
            }
        }

    return;
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
template<typename MA1, typename MA2, typename MA3, typename MA4>
void Matrix<T, MO>::
hessenberg_to_schur_(MA1 &e_vecs,
                     MA2 &e_vals,
                     MA3 &imagi,
                     MA4 &hess_form) noexcept  {

    // Store roots isolated by balanc and compute matrix norm
    //
    value_type  norm { 0 };

    for (size_type r = 0; r < e_vecs.rows(); ++r)
        for (size_type c = r; c < e_vecs.cols(); ++c)
            norm += std::fabs(hess_form(r, c));

    size_type   iter { 0 };
    size_type   n { e_vecs.cols() - 1 };
    value_type  exshift { 0 };
    value_type  p;
    value_type  q;
    value_type  oo;
    value_type  s;
    value_type  z;
    value_type  w;
    value_type  x;
    value_type  y;

    // Outer loop over eigenvalue index
    //
    while (n >= 0)  {
        size_type   l { n };

        // Look for single small sub-diagonal element
        //
        while (l > 0)  {
            s = std::fabs(hess_form(l - 1, l - 1)) +
                std::fabs(hess_form(l, l));

            if (s == value_type(0))
                s = norm;
            if (std::fabs(hess_form(l, l - 1)) < (EPSILON_ * s))
                break;

            l -= 1;
        }

        // Check for convergence
        //
        if (l == n)  {  // One root found
            hess_form(n, n) += exshift;
            e_vals(0, n) = hess_form(n, n);
            imagi (0, n--) = value_type(0);
            iter = 0;
        }
        else if (l == n - 1)  {  // Two roots found
            w = hess_form(n, n - 1) * hess_form(n - 1, n);
            p = (hess_form(n - 1, n - 1) - hess_form(n, n)) / value_type(2);
            q = p * p + w;
            z = std::sqrt(std::fabs(q));

            hess_form(n, n) += exshift;
            hess_form(n - 1, n - 1) += exshift;

            const value_type    &xx { hess_form(n, n) };

            if (q >= value_type(0))  {  // Real pair
                z = p >= value_type(0) ? p + z : p - z;

                e_vals(0, n - 1) = xx + z;
                e_vals(0, n) =
                    z != value_type(0) ? xx - w / z : e_vals (0, n - 1);

                imagi(0, n - 1) = 0;
                imagi(0, n) = 0;

                const value_type    &cref { hess_form(n, n - 1) };

                s = std::fabs(cref) + std::fabs(z);
                p = cref / s;
                q = z / s;
                oo = std::sqrt(p * p + q * q);
                p /= oo;
                q /= oo;

                // Row modification
                //
                for (size_type c = n - 1; c < e_vecs.cols (); ++c)  {
                    const value_type    &cref { hess_form(n - 1, c) };

                    hess_form(n - 1, c) = q * cref + p * hess_form(n, c);
                    hess_form(n, c) = q * hess_form(n, c) - p * cref;
                }

                // Column modification
                //
                for (size_type r = 0; r <= n; ++r)  {
                    const value_type    &cref { hess_form(r, n - 1) };

                    hess_form(r, n - 1) = q * cref + p * hess_form(r, n);
                    hess_form(r, n) = q * hess_form(r, n) - p * cref;
                }

                // Accumulate transformations
                //
                for (size_type r = 0; r <= e_vecs.rows() - 1; ++r)  {
                    const value_type    &cref { e_vecs(r, n - 1) };

                    e_vecs(r, n - 1) = q * cref + p * e_vecs(r, n);
                    e_vecs(r, n) = q * e_vecs(r, n) - p * cref;
                }
            }
            else  {  // Complex pair
                e_vals(0, n - 1) = xx + p;
                e_vals(0, n) = xx + p;
                imagi(0, n - 1) = z;
                imagi(0, n) = -z;
            }

            n -= 2;
            iter = 0;
        }
        else  {  // No convergence yet

            // Form shift
            //
            x = hess_form(n, n);
            y = 0;
            w = 0;
            if (l < n)  {
                y = hess_form(n - 1, n - 1);
                w = hess_form(n, n - 1) * hess_form(n - 1, n);
            }

            // Wilkinson's original ad hoc shift
            //
            if (iter == 10)  {
                exshift += x;
                for (size_type r = 0; r <= n; ++r)
                    hess_form(r, r) -= x;

                s = std::fabs(hess_form(n, n - 1)) +
                    std::fabs(hess_form(n - 1, n - 2));
                x = y = value_type(0.75) * s;
                w = value_type(-0.4375) * s * s;
            }

            // MATLAB's new ad hoc shift
            //
            if (iter == 30)  {
                s = (y - x) / value_type(2);
                s = s * s + w;

                if (s > 0)  {
                    s = y < x ? -std::sqrt(s) : std::sqrt(s);
                    s = x - w / ((y - x) / value_type(2) + s);

                    for (size_type r = 0; r <= n; ++r)
                        hess_form(r, r) -= s;

                    exshift += s;
                    x = y = w = value_type(0.964);
                }
            }

            iter += 1;  // Could check iteration count here?

            // Look for two consecutive small sub-diagonal elements
            //
            size_type   m { n - 2 };

            while (m >= l)  {
                const value_type    &cref { hess_form(m, m) };

                oo = x - cref;
                s = y - cref;
                p = (oo * s - w) / hess_form(m + 1, m) + hess_form(m, m + 1);
                q = hess_form(m + 1, m + 1) - cref - oo - s;
                oo = hess_form(m + 2, m + 1);
                s = std::fabs(p) + std::fabs(q) + std::fabs(oo);

                p /= s;
                q /= s;
                oo /= s;

                if (m == l)
                    break;

                if (std::fabs(hess_form(m, m - 1)) *
                    (std::fabs(q) + std::fabs(oo)) <
                        EPSILON_ *
                        (std::fabs(p) *
                         (std::fabs(hess_form (m - 1, m - 1)) +
                          std::fabs(cref) +
                          std::fabs(hess_form (m + 1, m + 1)))))
                    break;

                m -= 1;
            }

            for (size_type r = m + 2; r <= n; ++r)  {
                hess_form (r, r - 2) = 0;

                if (r > m + 2)
                    hess_form (r, r - 3) = 0;
            }

            // Double QR step involving rows l to n and columns m to n
            //
            for (size_type k = m; k <= n - 1; ++k)  {
                const bool  notlast { k != n - 1 };

                if (k != m)  {
                    p = hess_form(k, k - 1);
                    q = hess_form(k + 1, k - 1);
                    oo = notlast ? hess_form(k + 2, k - 1) : value_type(0);
                    x = std::fabs(p) + std::fabs(q) + std::fabs(oo);

                    if (x != value_type(0))  {
                        p /= x;
                        q /= x;
                        oo /= x;
                    }
                }
                if (x == value_type(0))
                    break;

                s = p < value_type(0) ? -std::sqrt(p * p + q * q + oo * oo)
                                      :  std::sqrt(p * p + q * q + oo * oo);

                if (s != value_type(0))  {
                    if (k != m)
                        hess_form(k, k - 1) = -s * x;
                    else if (l != m)
                        hess_form(k, k - 1) = -hess_form(k, k - 1);

                    p += s;
                    x = p / s;
                    y = q / s;
                    z = oo / s;

                    q /= p;
                    oo /= p;

                    // Row modification
                    //
                    for (size_type c = k; c < e_vecs.cols(); ++c)  {
                        p = hess_form(k, c) + q * hess_form(k + 1, c);

                        if (notlast)  {
                            p += oo * hess_form(k + 2, c);
                            hess_form(k + 2, c) -= p * z;
                        }

                        hess_form(k, c) -= p * x;
                        hess_form(k + 1, c) -= p * y;
                    }

                    // Column modification
                    //
                    for (size_type r = 0;
                         r <= std::min(n, k + 3); ++r)  {
                        p = x * hess_form(r, k) + y * hess_form(r, k + 1);

                        if (notlast)  {
                            p += z * hess_form(r, k + 2);
                            hess_form(r, k + 2) -= p * oo;
                         }

                         hess_form(r, k) -= p;
                         hess_form(r, k + 1) -= p * q;
                    }

                    // Accumulate transformations
                    //
                    for (size_type r = 0; r <= e_vecs.rows () - 1; ++r)  {
                        p = x * e_vecs(r, k) + y * e_vecs(r, k + 1);

                        if (notlast)  {
                            p += z * e_vecs(r, k + 2);
                            e_vecs(r, k + 2) -= p * oo;
                        }

                        e_vecs(r, k) -= p;
                        e_vecs(r, k + 1) -= p * q;
                    }
                }
            }
        }
    }

    if (norm == value_type(0))
        return;

    // Backsubstitute to find vectors of upper triangular form
    //
    for (size_type c = e_vecs.cols() - 1; c >= 0; --c)  {
        p = e_vals(0, c);
        q = imagi(0, c);

        if (q == value_type(0))  {  // Real vector
            size_type   l { c };

            hess_form(c, c) = 1;

            for (size_type r = c - 1; r >= 0; --r)  {
                const value_type    ww { hess_form(r, r) - p };

                oo = 0;
                for (size_type cc = l; cc <= c; ++cc)
                    oo += hess_form(r, cc) * hess_form(cc, c);

                if (imagi(0, r) < value_type(0))  {
                    z = ww;
                    s = oo;
                }
                else  {
                    l = r;

                    if (imagi(0, r) == value_type(0))
                        hess_form(r, c) =
                            ww != value_type(0) ? -oo / ww
                                                : -oo / (EPSILON_ * norm);
                    else  {  // Solve real equations
                        q = (e_vals(0, r) - p) * (e_vals(0, r) - p) +
                            imagi(0, r) * imagi(0, r);

                        const value_type    &xx { hess_form(r, r + 1) };
                        const value_type    tt { (xx * s - z * oo) / q };

                        hess_form(r, c) = tt;

                        hess_form(r + 1, c) =
                            std::fabs(xx) > std::fabs(z)
                                ? (-oo - ww * tt) / xx
                                : (-s - hess_form(r + 1, r) * tt) / z;
                    }

                    // Overflow control
                    //
                    const value_type    t { std::fabs(hess_form(r, c)) };

                    if ((EPSILON_ * t * t) > value_type(1))
                        for (size_type rr = r; rr <= c; ++rr)
                            hess_form(rr, c) /= t;
                }
            }
        }
        else if (q < value_type(0))  {  // Complex vector
            size_type   l { c - 1 };
            value_type  cdivr;
            value_type  cdivi;

            // Last vector component imaginary so matrix is triangular
            //
            if (std::fabs(hess_form(c, c - 1)) >
                    std::fabs(hess_form(c - 1, c)))  {
                hess_form(c - 1, c - 1) = q / hess_form(c, c - 1);
                hess_form(c - 1, c) =
                    -(hess_form(c, c) - p) / hess_form(c, c - 1);
            }
            else  {
                cdiv_(0,
                      -hess_form(c - 1, c),
                      hess_form(c - 1, c - 1) - p,
                      q,
                      cdivr,
                      cdivi);
                hess_form(c - 1, c - 1) = cdivr;
                hess_form(c - 1, c) = cdivi;
            }

            hess_form(c, c - 1) = 0;
            hess_form(c, c) = 1;

            for (size_type r = c - 2; r >= 0; --r)  {
                value_type  ra { 0 };
                value_type  sa { 0 };

                for (size_type cc = l; cc <= c; ++cc)  {
                    ra += hess_form(r, cc) * hess_form(cc, c - 1);
                    sa += hess_form(r, cc) * hess_form(cc, c);
                }

                const value_type    ww { hess_form(r, r) - p };

                if (imagi(0, r) < value_type(0))  {
                    z = ww;
                    oo = ra;
                    s = sa;
                }
                else  {
                    l = r;

                    if (imagi(0, r) == value_type(0))  {
                        cdiv_(-ra, -sa, ww, q, cdivr, cdivi);
                        hess_form(r, c - 1) = cdivr;
                        hess_form(r, c) = cdivi;
                    }
                    else  {  // Solve complex equations
                        const value_type    &xx { hess_form(r, r + 1) };
                        const value_type    &yy { hess_form(r + 1, r) };
                        const value_type    vi {
                            (e_vals (0, r) - p) * value_type(2.0) * q };
                        value_type          vr {
                            (e_vals (0, r) - p) * (e_vals (0, r) - p) +
                            imagi (0, r) * imagi (0, r) -
                            q * q };

                        if (vr == value_type(0) && vi == value_type(0))
                            vr = EPSILON_ *
                                 norm *
                                 (std::fabs(ww) +
                                  std::fabs(q) +
                                  std::fabs(xx) +
                                  std::fabs(yy) +
                                  std::fabs(z));

                        cdiv_(xx * oo - z * ra + q * sa,
                              xx * s - z * sa - q * ra,
                              vr,
                              vi,
                              cdivr, cdivi);
                        hess_form(r, c - 1) = cdivr;
                        hess_form(r, c) = cdivi;

                        if (std::fabs(xx) > std::fabs(z) + std::fabs(q))  {
                            hess_form(r + 1, c - 1) =
                                (-ra -
                                 ww * hess_form(r, c - 1) +
                                 q * hess_form(r, c)) /
                                xx;
                            hess_form(r + 1, c) =
                                (-sa - ww * hess_form (r, c) -
                                 q * hess_form(r, c - 1)) /
                                xx;
                        }
                        else  {
                            cdiv_(-oo - yy * hess_form (r, c - 1),
                                  -s - yy * hess_form (r, c),
                                  z,
                                  q,
                                  cdivr, cdivi);
                            hess_form(r + 1, c - 1) = cdivr;
                            hess_form(r + 1, c) = cdivi;
                        }
                    }

                   // Overflow control
                   //
                    const value_type    t {
                        std::max(std::fabs(hess_form (r, c - 1)),
                                 std::fabs(hess_form (r, c))) };

                    if (EPSILON_ * t * t > 1)
                        for (size_type rr = r; rr <= c; ++rr)  {
                            hess_form(rr, c - 1) /= t;
                            hess_form(rr, c) /= t;
                        }
                }
            }
        }
    }

    // Back transformation to get eigenvectors of original matrix
    //
    for (size_type c { e_vecs.cols() - 1 }; c >= 0; --c)
        for (size_type r = 0; r <= e_vecs.rows() - 1; ++r)  {
            z = 0;

            for (size_type k = 0; k <= std::min(c, e_vecs.cols() - 1); ++k)
                z += e_vecs(r, k) * hess_form(k, c);

            e_vecs(r, c) = z;
        }

    return;
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
void Matrix<T, MO>::
cdiv_(value_type xr,
      value_type xi,
      value_type yr,
      value_type yi,
      value_type &cdivr,
      value_type &cdivi) noexcept  {

    if (std::fabs(yr) > std::fabs(yi))  {
        const value_type    r { yi / yr };
        const value_type    d { yr + r * yi };

        cdivr = (xr + r * xi) / d;
        cdivi = (xi - r * xr) / d;
    }
    else  {
        const value_type    r { yr / yi };
        const value_type    d { yi + r * yr };

        cdivr = (r * xr + xi) / d;
        cdivi = (r * xi - xr) / d;
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
template<typename MA1, typename MA2, typename MA3>
void Matrix<T, MO>::
tridiagonalize_(MA1 &e_vecs, MA2 &e_vals, MA3 &imagi) noexcept  {

    for (size_type r = e_vecs.rows() - 1; r > 0; --r)  {
        // Scale to avoid under/overflow.
        //
        value_type  scale { 0 };

        for (size_type c = 0; c < r; ++c)
            scale += std::fabs(e_vals(0, c));

        value_type  h { 0 };

        if (scale == value_type(0))  {
            imagi(0, r) = e_vals(0, r - 1);
            for (size_type c = 0; c < r; ++c)  {
                e_vals(0, c) = e_vecs(r - 1, c);
                e_vecs(r, c) = 0;
                e_vecs(c, r) = 0;
            }
        }
        else  {
            // Generate Householder vector.
            //
            for (size_type c = 0; c < r; ++c)  {
                e_vals(0, c) /= scale;
                h += e_vals(0, c) * e_vals(0, c);
            }

            const value_type    &f { e_vals(0, r - 1) };
            value_type          g {
                f > value_type(0) ? -std::sqrt(h) : std::sqrt(h) };

            imagi(0, r) = scale * g;
            h -= f * g;
            e_vals(0, r - 1) = f - g;

            for (size_type c = 0; c < r; ++c)
                imagi (0, c) = 0;

            // Apply similarity transformation to remaining columns.
            //
            for (size_type c = 0; c < r; ++c)  {
                const value_type    &ff { e_vals(0, c) };

                e_vecs(c, r) = ff;
                g = imagi(0, c) + e_vecs(c, c) * ff;

                for (size_type cc = c + 1; cc <= r - 1; ++cc)  {
                    g += e_vecs(cc, c) * e_vals(0, cc);
                    imagi(0, cc) += e_vecs(cc, c) * ff;
                }
                imagi(0, c) = g;
            }

            value_type  ff { 0 };

            for (size_type c = 0; c < r; ++c)  {
                imagi(0, c) /= h;
                ff += imagi(0, c) * e_vals(0, c);
            }

            const value_type    hh { ff / (h + h) };

            for (size_type c = 0; c < r; ++c)
                imagi(0, c) -= hh * e_vals(0, c);

            for (size_type c = 0; c < r; ++c)  {
                for (size_type cc = c; cc <= r - 1; ++cc)
                    e_vecs(cc, c) -= e_vals(0, c) * imagi(0, cc) +
                                     imagi(0, c) * e_vals(0, cc);

                e_vals(0, c) = e_vecs(r - 1, c);
                e_vecs(r, c) = 0;
            }
        }

        e_vals(0, r) = h;
    }

    // Accumulate transformations.
    //
    for (size_type r = 0; r < e_vecs.rows() - 1; ++r)  {
        e_vecs(e_vecs.rows() - 1, r) = e_vecs(r, r);
        e_vecs(r, r) = 1;

        const value_type    &h = e_vals(0, r + 1);

        if (h != value_type(0))  {
            for (size_type c = 0; c <= r; ++c)
                e_vals(0, c) = e_vecs(c, r + 1) / h;

            for (size_type c = 0; c <= r; ++c)  {
                value_type  g { 0 };

                for (size_type rr = 0; rr <= r; ++rr)
                    g += e_vecs(rr, r + 1) * e_vecs(rr, c);

                for (size_type rr = 0; rr <= r; ++rr)
                    e_vecs(rr, c) -= g * e_vals(0, rr);
            }
        }
        for (size_type rr = 0; rr <= r; ++rr)
            e_vecs(rr, r + 1) = 0;
    }

    for (size_type c = 0; c < e_vecs.cols(); ++c)  {
        e_vals(0, c) = e_vecs(e_vecs.rows() - 1, c);
        e_vecs(e_vecs.rows() - 1, c) = 0;
    }
    e_vecs(e_vecs.rows() - 1, e_vecs.cols() - 1) = 1;
    imagi(0, 0) = 0;

    return;
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
template<typename MA1, typename MA2, typename MA3>
void Matrix<T, MO>::
diagonalize_ (MA1 &e_vecs, MA2 &e_vals, MA3 &imagi) noexcept  {

    for (size_type c = 1; c < e_vecs.cols(); ++c)
        imagi(0, c - 1) = imagi(0, c);
    imagi(0, e_vecs.cols() - 1) = 0;

    value_type  f { 0 };
    value_type  tst1 { 0 };

    for (size_type c = 0; c < e_vecs.cols(); ++c)  {
        // Find small subdiagonal element
        //
        tst1 = std::max(tst1, std::fabs(e_vals(0, c)) + std::fabs(imagi(0, c)));

        size_type   m { c };

        while (m < e_vecs.cols())  {
            if (std::fabs(imagi(0, m)) <= (EPSILON_ * tst1))
                break;
            m += 1;
        }

        // If m == l, e_vals(0, c) is an eigenvalue,
        // otherwise, iterate.
        //
        if (m > c)  {
            do  {
               // Compute implicit shift
               //
                value_type  g { e_vals (0, c) };
                value_type  p { (e_vals(0, c + 1) - g) /
                                (value_type(2) * imagi(0, c)) };

                // Euclidean distance func
                //
                value_type  dis =
                    { p < value_type(0) ? -hypot(p, value_type(1))
                                        : hypot(p, value_type(1)) };

                e_vals(0, c) = imagi(0, c) / (p + dis);
                e_vals(0, c + 1) = imagi(0, c) * (p + dis);

                const value_type    dl1 { e_vals(0, c + 1) };
                value_type          h { g - e_vals(0, c) };

                for (size_type cc = c + 2; cc < e_vecs.cols(); ++cc)
                    e_vals(0, cc) -= h;

                f += h;

                // Implicit QL transformation.
                //
                p = e_vals (0, m);

                value_type  cunit { 1 };
                value_type  c2 { 1 };
                value_type  c3 { 1 };
                value_type  el1 { imagi(0, c + 1) };
                value_type  s { 0 };
                value_type  s2 { 0 };

                for (size_type cc = m - 1; cc >= c; --cc)  {
                    c3 = c2;
                    c2 = cunit;
                    s2 = s;
                    g = cunit * imagi(0, cc);
                    h = cunit * p;
                    dis = hypot(p, imagi (0, cc));
                    imagi(0, cc + 1) = s * dis;
                    s = imagi(0, cc) / dis;
                    cunit = p / dis;
                    p = cunit * e_vals(0, cc) - s * g;
                    e_vals(0, cc + 1) = h + s * (cunit * g + s * e_vals(0, cc));

                    // Accumulate transformation.
                    //
                    for (size_type r = 0; r < e_vecs.rows (); ++r)  {
                        h = e_vecs(r, cc + 1);
                        e_vecs(r, cc + 1) = s * e_vecs(r, cc) + cunit * h;
                        e_vecs(r, cc) = cunit * e_vecs(r, cc) - s * h;
                    }
                }

                p = -s * s2 * c3 * el1 * imagi(0, c) / dl1;
                imagi(0, c) = s * p;
                e_vals(0, c) = cunit * p;

               // Check for convergence.
               //
            }  while (std::fabs(imagi(0, c)) > (EPSILON_ * tst1));
        }

        e_vals(0, c) = e_vals(0, c) + f;
        imagi(0, c) = 0;
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
template<typename MA1, typename MA2>
void Matrix<T, MO>::
eigen_space(MA1 &eigenvalues, MA2 &eigenvectors, bool sort_values) const  {

    if (! is_square() || cols() < 2)
        throw DataFrameError("Matrix::eigen_space(): Matrix must be squared");

    MA1     tmp_evals { 1, cols() };
    MA2     tmp_evecs { rows(), cols() };
    Matrix  imagi { 1, cols() }; // Imaginary part

    if (is_symmetric())  {
        for (size_type r = 0; r < rows(); ++r)
            for (size_type c = 0; c < cols(); ++c)
                tmp_evecs(r, c) = at(r, c);

        for (size_type c = 0; c < cols(); ++c)
            tmp_evals(0, c) = at(rows() - 1, c);

        tridiagonalize_(tmp_evecs, tmp_evals, imagi);
        diagonalize_(tmp_evecs, tmp_evals, imagi);
    }
    else  {
        Matrix  hess_form { rows(), cols() };

        for (size_type r = 0; r < rows(); ++r)
            for (size_type c = 0; c < cols(); ++c)
                hess_form(r, c) = at(r, c);

        red_to_hessenberg_(tmp_evecs, hess_form);
        hessenberg_to_schur_(tmp_evecs, tmp_evals, imagi, hess_form);
    }

    if (sort_values)  {
        for (size_type c = 0; c < cols() - 1; ++c)  {
            size_type   tmp_c { c };
            value_type  p { tmp_evals(0, c) };

            for (size_type cc = c + 1; cc < cols(); ++cc)
                if (tmp_evals(0, cc) < p)  {
                    tmp_c = cc;
                    p = tmp_evals(0, cc);
                }

            if (tmp_c != c)  {
                tmp_evals(0, tmp_c) = tmp_evals(0, c);
                tmp_evals(0, c) = p;
                for (size_type r = 0; r < rows(); ++r)  {
                    p = tmp_evecs(r, c);
                    tmp_evecs(r, c) = tmp_evecs(r, tmp_c);
                    tmp_evecs(r, tmp_c) = p;
                }
            }
        }
    }

    eigenvalues.swap(tmp_evals);
    eigenvectors.swap(tmp_evecs);
    return;
}

// ----------------------------------------------------------------------------

template<typename T,  matrix_orient MO>
Matrix<T, MO> Matrix<T, MO>::
covariance(bool is_unbiased) const  {

    const value_type    denom = is_unbiased ? rows() - 1 : rows();

    if (denom <= value_type(0))
        throw DataFrameError("Matrix::covariance(): Not solvable");

    Matrix  result (cols(), cols(), T(0));

    for (size_type cr = 0; cr < cols(); ++cr)  {
        value_type  col_mean { 0 };

       for (size_type r = 0; r < rows(); ++r)
           col_mean += at(r, cr);
        col_mean /= value_type(rows());

        for (size_type c = 0; c < cols(); ++c)  {
            value_type  var_covar { 0 };

            for (size_type r = 0; r < rows(); ++r)
                var_covar += (at(r, cr) - col_mean) * (at(r, c) - col_mean);

            result(cr, c) = var_covar / denom;
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
