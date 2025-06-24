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
#include <chrono>
#include <iomanip>
#include <iostream>

using namespace hmdf;

// ----------------------------------------------------------------------------

using row_mat_t = Matrix<std::size_t, matrix_orient::row_major>;
using col_mat_t = Matrix<std::size_t, matrix_orient::column_major>;

static constexpr long   ROWS = 5;
static constexpr long   COLS = 6;

void test_thread_pool();

// ----------------------------------------------------------------------------

int main(int, char *[]) {

    // ThreadGranularity::set_optimum_thread_level();

    row_mat_t   row_mata { 3, 3 };
    col_mat_t   col_mata { 3, 3 };
    std::size_t value { 0 };

    for (long r = 0; r < row_mata.rows(); ++r)
        for (long c = 0; c < row_mata.cols(); ++c)  {
            row_mata(r, c) = ++value;
            col_mata(r, c) = value;
        }

    row_mat_t   row_matb  = row_mata * row_mata;
    col_mat_t   col_matb  = col_mata * col_mata;

    assert((row_matb(0, 0) == col_matb(0, 0) && row_matb(0, 0) == 30));
    assert((row_matb(0, 2) == col_matb(0, 2) && row_matb(0, 2) == 42));
    assert((row_matb(1, 1) == col_matb(1, 1) && row_matb(1, 1) == 81));
    assert((row_matb(2, 1) == col_matb(2, 1) && row_matb(2, 1) == 126));
    assert((row_matb(2, 2) == col_matb(2, 2) && row_matb(2, 2) == 150));

    row_mat_t   row_mat { ROWS, COLS };
    col_mat_t   col_mat { ROWS, COLS };

    value = 0;
    for (long r = 0; r < row_mat.rows(); ++r)
        for (long c = 0; c < row_mat.cols(); ++c)
            row_mat(r, c) = value++;

    value = 0;
    for (long c = 0; c < col_mat.cols(); ++c)
        for (long r = 0; r < col_mat.rows(); ++r)
            col_mat(r, c) = value++;

    // Print the stuff out
    //
    std::cout << "Row matrix\n";
    for (long r = 0; r < row_mat.rows(); ++r)  {
        for (long c = 0; c < row_mat.cols(); ++c)  {
            std::cout << row_mat(r, c) << ", ";
        }
        std::cout << '\n';
    }
    std::cout << "\n\n";
    std::cout << "Column matrix\n";
    for (long r = 0; r < col_mat.rows(); ++r)  {
        for (long c = 0; c < col_mat.cols(); ++c)  {
            std::cout << col_mat(r, c) << ", ";
        }
        std::cout << '\n';
    }

    // Testing symmetrix matrices
    //
    {
        using scm_mat_t =
            Matrix<std::size_t, matrix_orient::column_major, true>;
        using srm_mat_t =
            Matrix<std::size_t, matrix_orient::column_major, true>;

        scm_mat_t   sym_col_mat { 5, 5 };

        value = 0;
        for (long c = 0; c < sym_col_mat.cols(); ++c)
            for (long r = c; r < sym_col_mat.rows(); ++r)
                sym_col_mat(r, c) = value++;

        assert(sym_col_mat.is_symmetric());
        assert(sym_col_mat(2, 3) == sym_col_mat(3, 2));
        assert(sym_col_mat(2, 3) == 10);
        assert(sym_col_mat(3, 4) == sym_col_mat(4, 3));
        assert(sym_col_mat(3, 4) == 13);

        srm_mat_t   sym_row_mat { 5, 5 };

        value = 0;
        for (long r = 0; r < sym_row_mat.rows(); ++r)
            for (long c = r; c < sym_row_mat.cols(); ++c)
                sym_row_mat(r, c) = value++;

        assert(sym_row_mat.is_symmetric());
        assert(sym_row_mat(2, 3) == sym_row_mat(3, 2));
        assert(sym_row_mat(2, 3) == 10);
        assert(sym_row_mat(3, 4) == sym_row_mat(4, 3));
        assert(sym_row_mat(3, 4) == 13);

        auto    plus_mat = sym_row_mat + sym_col_mat;

        assert(plus_mat.is_symmetric());
        assert(plus_mat(2, 3) == plus_mat(3, 2));
        assert(plus_mat(2, 3) == 20);
        assert(plus_mat(3, 4) == plus_mat(4, 3));
        assert(plus_mat(3, 4) == 26);

        col_mat_t   col_mat { 5, 5 };

        value = 0;
        for (long c = 0; c < col_mat.cols(); ++c)
            for (long r = 0; r < col_mat.rows(); ++r)
                col_mat(r, c) = value++;

        auto    plus_mat2 = sym_row_mat + col_mat;

        assert((! plus_mat2.is_symmetric()));
        assert(plus_mat2(2, 3) == 27);
        assert(plus_mat2(3, 4) == 36);

        auto    plus_mat3 = col_mat + sym_row_mat;

        assert(plus_mat3 == plus_mat2);
    }

    value = 0;
    for (long r = 0; r < row_mat.rows(); ++r)
        for (long c = 0; c < row_mat.cols(); ++c)
            assert(row_mat(r, c) == value++);

    value = 0;
    for (long c = 0; c < col_mat.cols(); ++c)
        for (long r = 0; r < col_mat.rows(); ++r)
            assert(col_mat(r, c) == value++);

    value = 0;
    for (auto citer = row_mat.row_cbegin();
             citer != row_mat.row_cend(); ++citer)
        assert(*citer == value++);

    value = 0;
    for (auto citer = col_mat.col_cbegin();
             citer != col_mat.col_cend(); ++citer)
        assert(*citer == value++);

    col_mat_t   empty_mat { };

    assert(empty_mat.empty());
    assert(empty_mat.rows() == 0);
    assert(empty_mat.cols() == 0);
    for (auto citer = empty_mat.row_cbegin();
             citer != empty_mat.row_cend(); ++citer)
        assert(*citer == value++);

    auto    col_iter1 = col_mat.col_begin();
    auto    col_iter2 = col_mat.col_begin();
    auto    row_iter1 = col_mat.row_begin();
    auto    row_iter2 = col_mat.row_begin();

    col_iter2 += 7;
    row_iter2 += 7;

    assert(*col_iter1 == 0);
    assert(*row_iter1 == 0);
    assert(*col_iter2 == 7);
    assert(*row_iter2 == 6);

    assert(((col_iter1 - col_iter2) == 7));
    assert(((row_iter1 - row_iter2) == 7));

    const auto  col_mat2 = col_mat;

    assert(col_mat != row_mat);
    assert(col_mat == col_mat2);

    auto    tran_mat = col_mat.transpose();
    auto    tran_mat2 = col_mat.transpose2();

    assert(tran_mat == tran_mat2);
    for (long r = 0; r < tran_mat.rows(); ++r)
        for (long c = 0; c < tran_mat.cols(); ++c)
            assert(tran_mat(r, c) == col_mat(c, r));

    // Test arithmetic functions
    //
    {
        auto    sum_mat = col_mat + row_mat;

        assert(sum_mat(0, 0) == 0);
        assert(sum_mat(4, 5) == 58);
        assert(sum_mat(1, 1) == 13);
        assert(sum_mat(3, 4) == 45);

        sum_mat += col_mat;
        assert(sum_mat(0, 0) == 0);
        assert(sum_mat(4, 5) == 87);
        assert(sum_mat(1, 1) == 19);
        assert(sum_mat(3, 4) == 68);

        row_mat_t   lhs_mat { ROWS, COLS };
        col_mat_t   rhs_mat { COLS, COLS };

        value = 0;
        for (long r = 0; r < lhs_mat.rows(); ++r)
            for (long c = 0; c < lhs_mat.cols(); ++c)
                lhs_mat(r, c) = value++;
        value = 0;
        for (long c = 0; c < rhs_mat.cols(); ++c)
            for (long r = 0; r < rhs_mat.rows(); ++r)
                rhs_mat(r, c) = value++;

        auto    multi_mat = lhs_mat * rhs_mat;

        assert(multi_mat(0, 0) == 55);
        assert(multi_mat(4, 5) == 5185);
        assert(multi_mat(1, 1) == 451);
        assert(multi_mat(3, 4) == 3277);

        col_mat_t   big_lhs_mat { 100, 100 };
        col_mat_t   big_rhs_mat { 100, 100 };

        for (long c = 0; c < 100; ++c)
            for (long r = 0; r < 100; ++r)  {
                big_lhs_mat(r, c) = c + 1;
                big_rhs_mat(r, c) = c + 1;
            }

        auto    big_multi_mat = big_lhs_mat * big_rhs_mat;

        assert(big_multi_mat(0, 0) == 5050);
        assert(big_multi_mat(99, 99) == 505000);
        assert(big_multi_mat(98, 2) == 15150);
        assert(big_multi_mat(2, 5) == 30300);
    }

    // Test Inverse
    //
    {
        using row_dmat_t = Matrix<double, matrix_orient::row_major>;

        row_dmat_t  mat2 { 3, 3 };

        mat2(0, 0) = 2.0;
        mat2(0, 1) = 3.0;
        mat2(0, 2) = 2.0;

        mat2(1, 0) = 3.0;
        mat2(1, 1) = 2.0;
        mat2(1, 2) = 3.0;

        mat2(2, 0) = 4.0;
        mat2(2, 1) = 2.0;
        mat2(2, 2) = 2.0;

        row_dmat_t  mat2_inv = mat2.inverse();
        auto        mat3 = mat2 * mat2_inv;

        // It must result to identity matrix
        //
        assert((std::fabs(mat3(0, 0) - 1.0) < 0.00000001));
        assert((std::fabs(mat3(1, 1) - 1.0) < 0.00000001));
        assert((std::fabs(mat3(2, 2) - 1.0) < 0.00000001));
        assert((std::fabs(mat3(0, 1) - 0.0) < 0.00000001));
        assert((std::fabs(mat3(0, 2) - 0.0) < 0.00000001));
        assert((std::fabs(mat3(1, 0) - 0.0) < 0.00000001));
        assert((std::fabs(mat3(1, 2) - 0.0) < 0.00000001));
        assert((std::fabs(mat3(2, 0) - 0.0) < 0.00000001));
        assert((std::fabs(mat3(2, 1) - 0.0) < 0.00000001));
    }

    // Test Eigen space
    //
    {
        using col_dmat_t = Matrix<double, matrix_orient::column_major>;

        col_dmat_t  col_mat { 10, 10 };
        col_dmat_t  eigenvals;
        col_dmat_t  eigenvecs;
        std::size_t value { 0 };

        // Symmetric matrix
        //
        for (long r = 0; r < col_mat.rows(); ++r)
            for (long c = 0; c < col_mat.cols(); ++c)
                col_mat(r, c) = col_mat(c, r) = double(++value);

        col_mat.eigen_space (eigenvals, eigenvecs, true);

        assert(eigenvals.cols() == 10);
        assert(eigenvals.rows() == 1);

        assert((std::fabs(eigenvals(0, 0) - -2.30681) < 0.001));
        assert((std::fabs(eigenvals(0, 1) - -2.48865) < 0.0001));
        assert((std::fabs(eigenvals(0, 5) - -6.56515) < 0.0001));
        assert((std::fabs(eigenvals(0, 9) - 687.09) < 0.01));

        assert(eigenvecs.cols() == 10);
        assert(eigenvecs.rows() == 10);
        assert((std::fabs(eigenvecs(0, 0) - 0.0698916) < 0.000001));
        assert((std::fabs(eigenvecs(2, 4) - -0.320417) < 0.000001));
        assert((std::fabs(eigenvecs(5, 6) - 0.181206) < 0.000001));
        assert((std::fabs(eigenvecs(8, 2) - 0.44074) < 0.00001));
        assert((std::fabs(eigenvecs(9, 9) - 0.432927) < 0.000001));

        // non-symmetric matrix
        //
        value = 0;
        for (long r = 0; r < col_mat.rows(); ++r)
            for (long c = 0; c < col_mat.cols(); ++c)
                col_mat(r, c) = double(++value);

        col_mat.eigen_space (eigenvals, eigenvecs, true);

        assert(eigenvals.cols() == 10);
        assert(eigenvals.rows() == 1);
        assert(eigenvals(0, 0) > -0.00000000001); // -2.87473e-15
        assert(eigenvals(0, 1) > -0.00000000001); // -2.87473e-15
        assert(eigenvals(0, 5) < 0.00000000001);  // 6.12134e-15
        assert((std::fabs(eigenvals(0, 9) - 520.84) < 0.01));

        assert(eigenvecs.cols() == 10);
        assert(eigenvecs.rows() == 10);

        assert((std::fabs(eigenvecs[0, 0] - -0.0833988) < 0.000001));
        assert((std::fabs(eigenvecs[2, 4] - 0.32935) < 0.00001));
        assert((std::fabs(eigenvecs[5, 6] - -0.410279) < 0.000001));
        assert((std::fabs(eigenvecs[8, 2] - 9.34286) < 0.00001));
        assert((std::fabs(eigenvecs[9, 9] - -0.51616) < 0.00001));
    }

    // Test Covariance matrix
    //
    {
        using col_dmat_t = Matrix<double, matrix_orient::column_major>;

        col_dmat_t  col_mat { 5, 4 };

        col_mat(0, 0) = 4.0;
        col_mat(0, 1) = 2.0;
        col_mat(0, 2) = 0.6;
        col_mat(0, 3) = 3.0;

        col_mat(1, 0) = 4.2;
        col_mat(1, 1) = 2.1;
        col_mat(1, 2) = 0.59;
        col_mat(1, 3) = 3.2;

        col_mat(2, 0) = 3.9;
        col_mat(2, 1) = 2.0;
        col_mat(2, 2) = 0.58;
        col_mat(2, 3) = 2.89;

        col_mat(3, 0) = 4.3;
        col_mat(3, 1) = 2.1;
        col_mat(3, 2) = 0.62;
        col_mat(3, 3) = 3.298;

        col_mat(4, 0) = 4.1;
        col_mat(4, 1) = 2.2;
        col_mat(4, 2) = 0.63;
        col_mat(4, 3) = 3.098;

        const auto  cov = col_mat.covariance(true);

        assert(cov.cols() == 4);
        assert(cov.rows() == 4);
        assert((std::fabs(cov(0, 0) - 0.025) < 0.001));
        assert((std::fabs(cov(0, 3) - 0.0254) < 0.0001));
        assert((std::fabs(cov(2, 3) - 0.001789) < 0.000001));
        assert((std::fabs(cov(3, 1) - 0.00763) < 0.00001));
        assert((std::fabs(cov(3, 3) - 0.0258172) < 0.0000001));
    }

    // Test SVD decomposition
    //
    {
        using col_dmat_t = Matrix<double, matrix_orient::column_major>;

        col_dmat_t  col_mat { 8, 4 };
        std::size_t value { 0 };

        for (long r = 0; r < col_mat.rows(); ++r)
            for (long c = 0; c < col_mat.cols(); ++c)
                col_mat(r, c) = double(++value);

        col_dmat_t  U;
        col_dmat_t  S;
        col_dmat_t  V;

        col_mat.svd (U, S, V, true);

        assert(U.rows() == 8);
        assert(U.cols() == 4);
        assert(S.rows() == 4);  // All the zeros at the end are eliminated
        assert(S.cols() == 4);
        assert(V.rows() == 4);
        assert(V.cols() == 4);

        const auto  col_mat2 = U * S * V.transpose();

        for (long r = 0; r < col_mat2.rows(); ++r)
            for (long c = 0; c < col_mat2.cols(); ++c)
                assert((std::fabs(col_mat(r, c) - col_mat2(r, c)) < 0.000001));
    }

    // Test degree matrix and Laplacian matrix
    //
    {
        using col_dmat_t = Matrix<double, matrix_orient::column_major>;

        col_dmat_t  adj_mat { 5, 5 };

        adj_mat(0, 0) = 0;
        adj_mat(0, 1) = 1;
        adj_mat(0, 2) = 0;
        adj_mat(0, 3) = 1;
        adj_mat(0, 4) = 0;

        adj_mat(1, 0) = 0;
        adj_mat(1, 1) = 0;
        adj_mat(1, 2) = 0;
        adj_mat(1, 3) = 0;
        adj_mat(1, 4) = 1;

        adj_mat(2, 0) = 0;
        adj_mat(2, 1) = 1;
        adj_mat(2, 2) = 0;
        adj_mat(2, 3) = 0;
        adj_mat(2, 4) = 0;

        adj_mat(3, 0) = 1;
        adj_mat(3, 1) = 0;
        adj_mat(3, 2) = 1;
        adj_mat(3, 3) = 1;
        adj_mat(3, 4) = 1;

        adj_mat(4, 0) = 0;
        adj_mat(4, 1) = 0;
        adj_mat(4, 2) = 1;
        adj_mat(4, 3) = 1;
        adj_mat(4, 4) = 0;

        const auto  degree_mat = adj_mat.degree_matrix();

        assert(degree_mat.rows() == 5);
        assert(degree_mat.cols() == 5);
        assert(degree_mat(0, 0) == 2.0);
        assert(degree_mat(1, 1) == 1.0);
        assert(degree_mat(2, 2) == 1.0);
        assert(degree_mat(3, 3) == 4.0);
        assert(degree_mat(4, 4) == 2.0);
        assert(degree_mat(0, 1) == 0.0);
        assert(degree_mat(4, 3) == 0.0);

        const auto  laplacian_mat = adj_mat.laplacian_matrix();

        assert(laplacian_mat.rows() == 5);
        assert(laplacian_mat.cols() == 5);
        assert(laplacian_mat(0, 0) == 2.0);
        assert(laplacian_mat(0, 1) == -1.0);
        assert(laplacian_mat(1, 3) == 0.0);
        assert(laplacian_mat(3, 3) == 3.0);
        assert(laplacian_mat(4, 2) == -1.0);
        assert(laplacian_mat(4, 4) == 2.0);
    }

    // Test Determinant and get_centered
    //
    {
        using row_dmat_t = Matrix<double, matrix_orient::row_major>;

        row_dmat_t  mat1 { 2, 2 };
        long        value { 0 };

        for (long r = 0; r < mat1.rows(); ++r)
            for (long c = 0; c < mat1.cols(); ++c)
                mat1(r, c) = double(++value);
        assert(mat1.determinant() == -2.0);

        row_dmat_t          mat2 { 5, 5 };
        std::vector<double> row0 = { 3.2, 11, 4.4, 3.3, 11 };
        std::vector<double> row1 = { 4.5, 10.8, 8.3, 3.4, 12 };
        std::vector<double> row2 = { 4.8, 9.1, 7.1, 3.6, 10.8 };
        std::vector<double> row3 = { 5.1, 2, 7.8, 4.1, 1.1 };
        std::vector<double> row4 = { 5.5, 0.5, 1.1, 4.12, 8 };

        mat2.set_row(row0.begin(), 0);
        mat2.set_row(row1.begin(), 1);
        mat2.set_row(row2.begin(), 2);
        mat2.set_row(row3.begin(), 3);
        mat2.set_row(row4.begin(), 4);
        assert((std::fabs(mat2.determinant() - 288.973) < 0.001));

        row_dmat_t  centered;

        mat2.get_centered(centered);
        assert((std::fabs(centered(0, 0) - 2.56) < 0.01));
        assert((std::fabs(centered(2, 0) - -0.64) < 0.01));
        assert((std::fabs(centered(4, 0) - -0.64) < 0.01));
    }

    // Test norm
    //
    {
        using row_dmat_t = Matrix<double, matrix_orient::row_major>;

        row_dmat_t  mat { 100, 100 };
        long        value { 0 };

        for (long r = 0; r < mat.rows(); ++r)
            for (long c = 0; c < mat.cols(); ++c)
                mat(r, c) = double(++value);

        const auto  norm = mat.norm();

        assert((std::fabs(norm - 577393.5703) < 0.0001));
    }

    // Test matrix sqrt
    //
    {
        using col_dmat_t = Matrix<double, matrix_orient::column_major>;

        col_dmat_t  mat { 4, 4 };

        mat(0, 0) = 1.0;
        mat(0, 1) = 0.2;
        mat(0, 2) = 0.3;
        mat(0, 3) = 0.1;
        mat(1, 0) = 0.2;
        mat(1, 1) = 2.0;
        mat(1, 2) = 0.4;
        mat(1, 3) = 0.2;
        mat(2, 0) = 0.3;
        mat(2, 1) = 0.4;
        mat(2, 2) = 3.0;
        mat(2, 3) = 0.3;
        mat(3, 0) = 0.1;
        mat(3, 1) = 0.2;
        mat(3, 2) = 0.3;
        mat(3, 3) = 4.0;

        mat.sqrt();
        assert((std::fabs(mat(0, 0) - 0.990905) < 0.00001));
        assert((std::fabs(mat(0, 2) - 0.10619) < 0.0001));
        assert((std::fabs(mat(1, 1) - 1.40557) < 0.0001));
        assert((std::fabs(mat(1, 3) - 0.0552678) < 0.000001));
        assert((std::fabs(mat(2, 0) - 0.10619) < 0.0001));
        assert((std::fabs(mat(2, 3) - 0.0779677) < 0.000001));
        assert((std::fabs(mat(3, 2) - 0.0779677) < 0.000001));
        assert((std::fabs(mat(3, 3) - 1.9975) < 0.0001));

        const auto  mat2 = mat * mat;

        assert((std::fabs(mat2(0, 0) - 1.0) < 0.0001));
        assert((std::fabs(mat2(0, 2) - 0.3) < 0.0001));
        assert((std::fabs(mat2(1, 1) - 2.0) < 0.0001));
        assert((std::fabs(mat2(1, 3) - 0.2) < 0.0001));
        assert((std::fabs(mat2(2, 0) - 0.3) < 0.0001));
        assert((std::fabs(mat2(2, 3) - 0.3) < 0.0001));
        assert((std::fabs(mat2(3, 2) - 0.3) < 0.0001));
        assert((std::fabs(mat2(3, 3) - 4.0) < 0.0001));
    }

    // Test whiten
    //
    {
        using col_dmat_t = Matrix<double, matrix_orient::column_major>;

        col_dmat_t  mat { 5, 5 };

        mat(0, 0) = -2.5;
        mat(0, 1) = -0.001;
        mat(0, 2) = 3;
        mat(0, 3) = -4;
        mat(0, 4) = 1.5;
        mat(1, 0) = 0.5;
        mat(1, 1) = 4.4;
        mat(1, 2) = 2.2;
        mat(1, 3) = 5.1;
        mat(1, 4) = 0.12;
        mat(2, 0) = 0.4;
        mat(2, 1) = 1;
        mat(2, 2) = -1.1;
        mat(2, 3) = 0;
        mat(2, 4) = -2.1;
        mat(3, 0) = 0.9;
        mat(3, 1) = -1.6;
        mat(3, 2) = 3.2;
        mat(3, 3) = 8.8;
        mat(3, 4) = -0.001;
        mat(4, 0) = -0.2;
        mat(4, 1) = 0.8;
        mat(4, 2) = -4.1;
        mat(4, 3) = -0.9;
        mat(4, 4) = 0.1;

        col_dmat_t  white;

        mat.get_whiten(white, true);

        const auto  cov = white.covariance();

        for (long r = 0; r < cov.rows(); ++r)  {
            for (long c = 0; c < cov.cols(); ++c)  {
                if (r == c && r != 1)  { //  Some singular issue here
                    assert((std::fabs(cov(r, c) - 1.0) < 0.00001));
                }
                else  {
                    assert((std::fabs(cov(r, c) - 0.0) < 0.00001));
                }
            }
        }
    }

    // Test mean
    //
    {
        using col_dmat_t = Matrix<double, matrix_orient::column_major>;

        col_dmat_t  mat { 2, 2 };

        mat(0, 0) = 1.0;
        mat(0, 1) = 2.0;
        mat(1, 0) = 3.0;
        mat(1, 1) = 4.0;

        assert((std::fabs(mat.mean() - 2.5) < 0.0000001));
    }

    // Test get_row and get_column
    //
    {
        using col_mat_t = Matrix<int, matrix_orient::column_major>;
        using row_mat_t = Matrix<int, matrix_orient::row_major>;

		col_mat_t   col_mat { 10, 10 };
		row_mat_t   row_mat { 10, 10 };
        int         value { 0 };

        for (long r = 0; r < row_mat.rows(); ++r)
            for (long c = 0; c < row_mat.cols(); ++c)  {
                row_mat(r, c) = ++value;
                col_mat(r, c) = value;
            }

        const auto  row_res1 = row_mat.get_row_vec(2);
        const auto  col_res1 = col_mat.get_row_vec(2);

        assert((row_res1 ==
                std::vector<int> { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 })); 
        assert((col_res1 ==
                std::vector<int> { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 })); 

        const auto  row_res2 = row_mat.get_column_vec(9);
        const auto  col_res2 = col_mat.get_column_vec(9);

        assert((row_res2 ==
                std::vector<int> { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 })); 
        assert((col_res2 ==
                std::vector<int> { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 })); 

        const auto  row_res3 = row_mat.get_row_mat(2);
        const auto  col_res3 = col_mat.get_row_mat(2);

		assert(row_res3.rows() == 1);
		assert(row_res3.cols() == 10);
		assert(col_res3.rows() == 1);
		assert(col_res3.cols() == 10);
		assert(row_res3(0, 8) == 29);
		assert(col_res3(0, 5) == 26);

        const auto  row_res4 = row_mat.get_column_mat(9);
        const auto  col_res4 = col_mat.get_column_mat(9);

		assert(row_res4.rows() == 10);
		assert(row_res4.cols() == 1);
		assert(col_res4.rows() == 10);
		assert(col_res4.cols() == 1);
		assert(row_res4(8, 0) == 90);
		assert(col_res4(5, 0) == 60);
    }

    test_thread_pool();
    return (0);
}

// ----------------------------------------------------------------------------

void test_thread_pool()  {

    ThreadGranularity::set_thread_level(3);

    // Let threads start
    //
    std::this_thread::sleep_for(std::chrono::seconds(2));
    {
        std::vector<int>    values = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        auto                lbd = [&values] (auto begin, auto end) -> void  {
            for (auto i = begin; i < end; ++i)
                values[i] += 1;
        };
        auto                futures =
            ThreadGranularity::thr_pool_.parallel_loop(0, int(values.size()),
                                                       std::move(lbd));

        for (auto &fut : futures)  fut.get();
        for (int i = 0; i < int(values.size()); ++i)
            assert((values[i] == i + 2));

        std::vector<int>    values2 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
        auto                lbd2 = [&values2] (auto begin, auto end) -> void  {
            for (auto i = begin; i < end; ++i)
                values2[i] += 1;
        };
        auto                futures2 =
            ThreadGranularity::thr_pool_.parallel_loop(0, int(values2.size()),
                                                       std::move(lbd2));

        for (auto &fut : futures2)  fut.get();
        for (int i = 0; i < int(values2.size()); ++i)
            assert((values2[i] == i + 2));

        std::vector<int>    values3 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        auto                lbd3 = [&values3] (auto begin, auto end) -> void  {
            for (auto i = begin; i < end; ++i)
                values3[i] += 1;
        };
        auto                futures3 =
            ThreadGranularity::thr_pool_.parallel_loop(0, int(values3.size()),
                                                       std::move(lbd3));

        for (auto &fut : futures3)  fut.get();
        for (int i = 0; i < int(values3.size()); ++i)
            assert((values3[i] == i + 2));
    }

    {
        std::vector<int>    values = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        auto                lbd = [&values] (auto begin, auto end) -> void  {
            for (auto i = end; i >= begin; --i)
                values[i] += 1;
        };
        auto                futures =
            ThreadGranularity::thr_pool_.parallel_loop(
                int(values.size()) - 1, 0, std::move(lbd));

        for (auto &fut : futures)  fut.get();
        for (int i = 0; i < int(values.size()); ++i)
            assert((values[i] == i + 2));

        std::vector<int>    values2 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
        auto                lbd2 = [&values2] (auto begin, auto end) -> void  {
            for (auto i = end; i >= begin; --i)
                values2[i] += 1;
        };
        auto                futures2 =
            ThreadGranularity::thr_pool_.parallel_loop(
                int(values2.size()) - 1, 0, std::move(lbd2));

        for (auto &fut : futures2)  fut.get();
        for (int i = 0; i < int(values2.size()); ++i)
            assert((values2[i] == i + 2));

        std::vector<int>    values3 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        auto                lbd3 = [&values3] (auto begin, auto end) -> void  {
            for (auto i = end; i >= begin; --i)
                values3[i] += 1;
        };
        auto                futures3 =
            ThreadGranularity::thr_pool_.parallel_loop(
                int(values3.size()) - 1, 0, std::move(lbd3));

        for (auto &fut : futures3)  fut.get();
        for (int i = 0; i < int(values3.size()); ++i)
            assert((values3[i] == i + 2));
    }
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
