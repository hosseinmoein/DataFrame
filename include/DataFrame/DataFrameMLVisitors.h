// Hossein Moein
// October 30, 2019
/*
Copyright (c) 2019-2022, Hossein Moein
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

#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/DataFrameTypes.h>
#include <DataFrame/Vectors/VectorPtrView.h>

#include <algorithm>
#include <complex>
#include <functional>
#include <iterator>
#include <type_traits>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<size_t K, typename T, typename I = unsigned long>
struct  KMeansVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    using result_type = std::array<value_type, K>;
    using cluster_type = std::array<VectorPtrView<value_type>, K>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    const size_type iter_num_;
    distance_func   dfunc_;
    result_type     result_ { };  // K-Means

    template<typename H>
    inline void calc_k_means_(const H &column_begin, size_type col_size)  {

        std::random_device                          rd;
        std::mt19937                                gen(rd());
        std::uniform_int_distribution<size_type>    rd_gen(0, col_size - 1);

        // Pick centroids as random points from the col.
        for (auto &k_mean : result_)
            k_mean = *(column_begin + rd_gen(gen));

        std::vector<size_type>  assignments(col_size, 0);

        for (size_type iter = 0; iter < iter_num_; ++iter) {
            // Find assignments.
            for (size_type point = 0; point < col_size; ++point) {
                double      best_distance = std::numeric_limits<double>::max();
                size_type   best_cluster = 0;

                for (size_type cluster = 0; cluster < K; ++cluster) {
                    const double    distance =
                        dfunc_(*(column_begin + point), result_[cluster]);

                    if (distance < best_distance) {
                        best_distance = distance;
                        best_cluster = cluster;
                    }
                }
                assignments[point] = best_cluster;
            }

            // Sum up and count points for each cluster.
            result_type             new_means { value_type() };
            std::array<double, K>   counts { 0.0 };

            for (size_type point = 0; point < col_size; ++point) {
                const size_type cluster = assignments[point];

                new_means[cluster] =
                    new_means[cluster] + *(column_begin + point);
                counts[cluster] += 1.0;
            }

            bool    done = true;

            // Divide sums by counts to get new centroids.
            for (size_type cluster = 0; cluster < K; ++cluster) {
                // Turn 0/0 into 0/1 to avoid zero division.
                const double        count =
                    std::max<double>(1.0, counts[cluster]);
                const value_type    value = new_means[cluster] / count;

                if (dfunc_(value, result_[cluster]) > 0.0000001)  {
                    done = false;
                    result_[cluster] = value;
                }
            }

            if (done)  break;
        }
    }

public:

    template<typename IV, typename H>
    inline void
    operator() (const IV &idx_begin,
                const IV &idx_end,
                const H &column_begin,
                const H &column_end)  {

        GET_COL_SIZE

        calc_k_means_(column_begin, col_s);
    }

    // Using the calculated means, separate the given column into clusters
    //
    template<typename IV, typename H>
    inline cluster_type
    get_clusters(const IV &idx_begin,
                 const IV &idx_end,
                 const H &column_begin,
                 const H &column_end)  {

        GET_COL_SIZE
        cluster_type    clusters;

        for (size_type i = 0; i < K; ++i)  {
            clusters[i].reserve(col_s / K + 2);
            clusters[i].push_back(const_cast<value_type *>(&(result_[i])));
        }

        for (size_type j = 0; j < col_s; ++j)  {
            double      min_dist = std::numeric_limits<double>::max();
            size_type   min_idx;

            for (size_type i = 0; i < K; ++i)  {
                const double    dist = dfunc_(*(column_begin + j), result_[i]);

                if (dist < min_dist)  {
                    min_dist = dist;
                    min_idx = i;
                }
            }
            clusters[min_idx].push_back(
                const_cast<value_type *>(&*(column_begin + j)));
        }

        return (clusters);
    }

    inline void pre ()  {  }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

    explicit
    KMeansVisitor(
        size_type num_of_iter,
        distance_func f =
            [](const value_type &x, const value_type &y) -> double {
                return ((x - y) * (x - y));
            })
        : iter_num_(num_of_iter), dfunc_(f)  {   }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  AffinityPropVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    using result_type = VectorPtrView<value_type>;
    using cluster_type = std::vector<VectorPtrView<value_type>>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    const size_type iter_num_;
    distance_func   dfunc_;
    const double    dfactor_;
    result_type     result_ { };  // Centers

    template<typename H>
    inline std::vector<double>
    get_similarity_(const H &column_begin, size_type csize)  {

        std::vector<double> simil((csize * (csize + 1)) / 2, 0.0);
        double              min_val = std::numeric_limits<double>::max();

        // Compute similarity between distinct data points i and j
        for (size_type i = 0; i < csize - 1; ++i)
            for (size_type j = i + 1; j < csize; ++j)  {
                const double    val =
                    -dfunc_(*(column_begin + i), *(column_begin + j));

                simil[(i * csize) + j - ((i * (i + 1)) >> 1)] = val;
                if (val < min_val)  min_val = val;
            }

        // Assign min to diagonals
        for (size_type i = 0; i < csize; ++i)
            simil[(i * csize) + i - ((i * (i + 1)) >> 1)] = min_val;

        return (simil);
    }

    inline void
    get_avail_and_respon(const std::vector<double> &simil,
                         size_type csize,
                         std::vector<double> &avail,
                         std::vector<double> &respon)  {

        avail.resize(csize * csize, 0.0);
        respon.resize(csize * csize, 0.0);

        for (size_type m = 0; m < iter_num_; ++m)  {
            // Update responsibility
            for (size_type i = 0; i < csize; ++i)  {
                for (size_type j = 0; j < csize; ++j)  {
                    double  max_diff = -std::numeric_limits<double>::max();

                    for (size_type jj = 0; jj < j; ++jj)  {
                        const double    value =
                            simil[(i * csize) + jj - ((i * (i + 1)) >> 1)] +
                            avail[jj * csize + i];

                        if (value > max_diff)
                            max_diff = value;
                    }
                    for (size_type jj = j + 1; jj < csize; ++jj)  {
                        const double    value =
                            simil[(i * csize) + jj - ((i * (i + 1)) >> 1)] +
                            avail[jj * csize + i];

                        if (value > max_diff)
                            max_diff = value;
                    }

                    respon[j * csize + i] =
                        (1.0 - dfactor_) *
                        (simil[(i * csize) + j - ((i * (i + 1)) >> 1)] -
                        max_diff) +
                        dfactor_ * respon[j * csize + i];
                }
            }

            // Update availability
            for (size_type i = 0; i < csize; ++i)  {
                for (size_type j = 0; j < csize; ++j)  {
                    const size_type s1 = j * csize;

                    if (i == j)  {
                        double  sum = 0.0;

                        for (size_type ii = 0; ii < i; ++ii)
                            sum += std::max(0.0, respon[s1 + ii]);
                        for(size_type ii = i + 1; ii < csize; ++ii)
                            sum += std::max(0.0, respon[s1 + ii]);

                        avail[s1 + i] =
                            (1.0 - dfactor_) * sum + dfactor_ * avail[s1 + i];
                    }
                    else  {
                        double          sum = 0.0;
                        const size_type max_i_j = std::max(i, j);
                        const size_type min_i_j = std::min(i, j);

                        for (size_type ii = 0; ii < min_i_j; ++ii)
                            sum += std::max(0.0, respon[s1 + ii]);
                        for (size_type ii = min_i_j + 1; ii < max_i_j; ++ii)
                            sum += std::max(0.0, respon[s1 + ii]);
                        for (size_type ii = max_i_j + 1; ii < csize; ++ii)
                            sum += std::max(0.0, respon[s1 + ii]);

                        avail[s1 + i] =
                            (1.0 - dfactor_) *
                            std::min(0.0, respon[s1 + j] + sum) + dfactor_ *
                            avail[s1 + i];
                    }
                }
            }
        }

        return;
    }

public:

    template<typename IV, typename H>
    inline void
    operator() (const IV &idx_begin,
                const IV &idx_end,
                const H &column_begin,
                const H &column_end)  {

        GET_COL_SIZE
        const std::vector<double>   simil =
            std::move(get_similarity_(column_begin, col_s));
        std::vector<double>         avail;
        std::vector<double>         respon;

        get_avail_and_respon(simil, col_s, avail, respon);

        result_.reserve(std::min(col_s / 100, size_type(16)));
        for (size_type i = 0; i < col_s; ++i)  {
            if (respon[i * col_s + i] + avail[i * col_s + i] > 0.0)
                result_.push_back(
                    const_cast<value_type *>(&*(column_begin + i)));
        }
    }

    // Using the calculated means, separate the given column into clusters
    //
    template<typename IV, typename H>
    inline cluster_type
    get_clusters(const IV &idx_begin,
                 const IV &idx_end,
                 const H &column_begin,
                 const H &column_end)  {

        GET_COL_SIZE
        const size_type centers_size = result_.size();
        cluster_type    clusters;

        if (centers_size > 0)  {
            clusters.resize(centers_size);
            for (size_type i = 0; i < centers_size; ++i)
                clusters[i].reserve(col_s / centers_size);

            for (size_type j = 0; j < col_s; ++j)  {
                double      min_dist = std::numeric_limits<double>::max();
                size_type   min_idx;

                for (size_type i = 0; i < centers_size; ++i)  {
                    const double    dist =
                        dfunc_(*(column_begin + j), result_[i]);

                    if (dist < min_dist)  {
                        min_dist = dist;
                        min_idx = i;
                    }
                }
                clusters[min_idx].push_back(
                    const_cast<value_type *>(&*(column_begin + j)));
            }
        }

        return (clusters);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    AffinityPropVisitor(
        size_type num_of_iter,
        distance_func f =
            [](const value_type &x, const value_type &y) -> double {
                return ((x - y) * (x - y));
            },
        double damping_factor = 0.9)
        : iter_num_(num_of_iter), dfunc_(f), dfactor_(damping_factor)  {   }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct FastFourierTransVisitor {

public:

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        typename std::conditional<is_complex<T>::value,
                                  std::vector<T>,
                                  std::vector<std::complex<T>>>::type;
    using real_t = typename result_type::value_type::value_type;

private:

    using cplx = typename result_type::value_type;

    static inline void fft_radix2_(result_type &column)  {

        // Discrete Fourier Transform
        //
        const size_type col_s = column.size();
        const real_t    theta = real_t(M_PI) / real_t(col_s);
        cplx            phi = cplx (std::cos(theta), -std::sin(theta));
        size_type       k = col_s;

        while (k > 1)  {
            const size_type n = k;

            k >>= 1;
            phi *= phi;

            cplx    phi_pow = real_t(1);

            for (size_type l = 0; l < k; ++l)  {
                for (size_type a = l; a < col_s; a += n)  {
                    const size_type b = a + k;

                    if (b < col_s)  {
                        const cplx  ab_diff = column[a] - column[b];

                        column[a] += column[b];
                        column[b] = ab_diff * phi_pow;
                    }
                }
                phi_pow *= phi;
            }
        }

        // Decimate
        //
        const size_type m = static_cast<size_type>(std::log2(col_s));

        for (size_type a = 0; a < col_s; ++a)  {
            size_type   b = a;

            // Reverse bits
            //
            b = (((b & 0xAAAAAAAA) >> 1) | ((b & 0x55555555) << 1));
            b = (((b & 0xCCCCCCCC) >> 2) | ((b & 0x33333333) << 2));
            b = (((b & 0xF0F0F0F0) >> 4) | ((b & 0x0F0F0F0F) << 4));
            b = (((b & 0xFF00FF00) >> 8) | ((b & 0x00FF00FF) << 8));
            b = ((b >> 16) | (b << 16)) >> (32 - m);

            if (b > a && b < col_s)  std::swap(column[a], column[b]);
        }

        // Normalize
        //
        /*
        if (normalize_)  {
            const cplx  f = real_t(1) / std::sqrt(real_t(col_s));

            for (auto &iter : column)  iter *= f;
        }
        */
    }

    static inline void ifft_(result_type &column)  {

        // Conjugate the complex numbers
        //
        for (auto &iter : column)  iter = std::conj(iter);

        // Forward fft
        //
        transform_(column, false);

        // Conjugate the complex numbers again
        //
        for (auto &iter : column)  iter = std::conj(iter);

        const size_type col_s = column.size();

        // Scale the numbers
        //
        for (auto &iter : column)  iter /= real_t(col_s);
    }

    static inline result_type convolve_(result_type xvec, result_type yvec)  {

        transform_(xvec, false);
        transform_(yvec, false);

        const size_type col_s = xvec.size();

        for (size_type i = 0; i < col_s; i++)
            xvec[i] *= yvec[i];
        transform_(xvec, true);

        // Scaling (because this FFT implementation omits it)
        //
        for (size_type i = 0; i < col_s; i++)
            xvec[i] /= real_t(col_s);
        return (xvec);
    }

    static inline void fft_gen_(result_type &column) {

        const size_type col_s = column.size();

        // Trigonometric table
        //
        result_type     exp_table(col_s);
        const size_type col_s_2 = col_s * 2;

        for (size_type i = 0; i < col_s; i++) {
            const size_type sq = (i * i) % col_s_2;

            exp_table[i] =
                std::polar(real_t(1),
                           real_t(M_PI) * real_t(sq) / real_t(col_s));
        }

        // Find a power-of-2 convolution length m such that m >= col_s * 2 + 1
        //
        size_type   m = 1;

        while (m / 2 <= col_s)   m *= 2;

        // Temporary vectors and preprocessing
        //
        result_type avec (m, cplx(0, 0));

        for (size_type i = 0; i < col_s; i++)
            avec[i] = column[i] * exp_table[i];

        result_type bvec(m, cplx(0, 0));

        bvec[0] = exp_table[0];
        for (size_type i = 1; i < col_s; i++)
            bvec[i] = bvec[m - i] = std::conj(exp_table[i]);

        // Convolution
        //
        const result_type   cvec = convolve_(std::move(avec), std::move(bvec));

        // Postprocessing
        //
        for (size_type i = 0; i < col_s; i++)
            column[i] = cvec[i] * exp_table[i];
    }

    static inline void transform_(result_type &column, bool inverse) {

        const size_type col_s = column.size();

        if (inverse)  ifft_(column);
        else if ((col_s & (col_s - 1)) == 0)  // Is power of 2
            fft_radix2_(column);
        else  // More complicated algorithm for arbitrary sizes
            fft_gen_(column);
    }

public:

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        GET_COL_SIZE
        result_type result (col_s);

        if constexpr (is_complex<T>::value)  {
            std::transform(column_begin, column_end,
                           result.begin(),
                           [] (T v) { return (v); });
        }
        else  {
            std::transform(column_begin, column_end,
                           result.begin(),
                           [] (T v) { return (std::complex<T>(v, 0)); });
        }

        transform_(result, inverse_);
        result_.swap(result);
    }

    inline void pre ()  {

        result_.clear();
        magnitude_.clear();
        angle_.clear();
    }
    inline void post ()  {  }

    DEFINE_RESULT
    inline const std::vector<real_t> &
    get_magnitude() const  {

        return (const_cast<FastFourierTransVisitor<T, I> *>
                    (this)->get_magnitude());
    }
    inline std::vector<real_t> &
    get_magnitude()  {

        if (magnitude_.empty())  {
            magnitude_.reserve(result_.size());
            for (const auto &citer : result_)
                magnitude_.push_back(std::sqrt(std::norm(citer)));
        }
        return (magnitude_);
    }
    inline const std::vector<real_t> &
    get_angle() const  {

        return (const_cast<FastFourierTransVisitor<T, I> *>
                    (this)->get_angle());
    }
    inline std::vector<real_t> &
    get_angle()  {

        if (angle_.empty())  {
            angle_.reserve(result_.size());
            for (const auto &citer : result_)
                angle_.push_back(std::arg(citer));
        }
        return (angle_);
    }

    explicit
    FastFourierTransVisitor(bool inverse = false) : inverse_(inverse)  {   }

private:

    const bool          inverse_;
    result_type         result_ {  };
    std::vector<real_t> magnitude_ {  };
    std::vector<real_t> angle_ {  };
};

template<typename T, typename I = unsigned long>
using fft_v = FastFourierTransVisitor<T, I>;

} // namespace hmdf

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
