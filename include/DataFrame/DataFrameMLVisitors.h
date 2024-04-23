// Hossein Moein
// October 30, 2019
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

#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/Vectors/VectorPtrView.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <complex>
#include <functional>
#include <limits>
#include <numeric>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

// One pass simple linear regression
//
template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  SLRegressionVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx,
                            const value_type &x, const value_type &y)  {

        if (skip_nan_ && (is_nan__(x) || is_nan__(y))) [[unlikely]]  return;

        s_xy_ += (x_stats_.get_mean() - x) *
                 (y_stats_.get_mean() - y) *
                 value_type(n_) / value_type(n_ + 1);

        x_stats_(idx, x);
        y_stats_(idx, y);
        n_ += 1;
    }
    PASS_DATA_ONE_BY_ONE_2

    inline void pre ()  {

        n_ = 0;
        s_xy_ = 0;
        x_stats_.pre();
        y_stats_.pre();
    }
    inline void post ()  {  }

    inline size_type get_count () const  { return (n_); }
    inline result_type get_slope () const  {

        // Sum of the squares of the difference between each x and
        // the mean x value.
        //
        const value_type    s_xx =
            x_stats_.get_variance() * value_type(n_ - 1);

        return (s_xy_ / s_xx);
    }
    inline result_type get_intercept () const  {

        return (y_stats_.get_mean() - get_slope() * x_stats_.get_mean());
    }
    inline result_type get_corr () const  {

        const value_type    t = x_stats_.get_std() * y_stats_.get_std();

        return (s_xy_ / (value_type(n_ - 1) * t));
    }

    explicit SLRegressionVisitor(bool skipnan = true)
        : x_stats_(skipnan), y_stats_(skipnan), skip_nan_(skipnan)  {   }

private:

    size_type                               n_ { 0 };

    // Sum of the product of the difference between x and its mean and
    // the difference between y and its mean.
    //
    value_type                              s_xy_ { 0 };
    StatsVisitor<value_type, index_type>    x_stats_ {  };
    StatsVisitor<value_type, index_type>    y_stats_ {  };
    const bool                              skip_nan_;
};

// ----------------------------------------------------------------------------

template<std::size_t K,
         typename T, typename I = unsigned long, std::size_t A = 0>
struct  KMeansVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    using result_type = std::array<value_type, K>;
    using cluster_type = std::array<VectorConstPtrView<value_type, A>, K>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;
    using seed_t = std::random_device::result_type;

private:

    const size_type iter_num_;
    const bool      cc_;
    const seed_t    seed_;
    distance_func   dfunc_;
    result_type     result_ { };    // K Means
    cluster_type    clusters_ { };  // K Clusters

    template<forward_iterator H>
    inline void calc_k_means_(const H &column_begin, size_type col_s)  {

        std::random_device                          rd;
        std::mt19937                                gen(
            (seed_ != seed_t(-1)) ? seed_ : rd());
        std::uniform_int_distribution<size_type>    rd_gen(0, col_s - 1);

        // Pick centroids as random points from the col.
        //
        for (auto &k_mean : result_) [[likely]]  {
            const value_type    &value = *(column_begin + rd_gen(gen));

            if (! is_nan__(value)) [[likely]]
                k_mean = value;
        }

        std::vector<size_type, typename allocator_declare<size_type, A>::type>
            assignments(col_s, 0);

        for (size_type iter = 0; iter < iter_num_; ++iter) [[likely]]  {
            result_type             new_means { value_type() };
            std::array<double, K>   counts { 0.0 };

            // Find assignments.
            //
            for (size_type point = 0; point < col_s; ++point) [[likely]]  {
                const value_type    &value = *(column_begin + point);

                if (! is_nan__(value)) [[likely]]  {
                    double      best_distance =
                        std::numeric_limits<double>::max();
                    size_type   best_cluster = 0;

                    for (size_type cluster = 0; cluster < K;
                         ++cluster) [[likely]]  {
                        const double    distance =
                            dfunc_(value, result_[cluster]);

                        if (distance < best_distance)  {
                            best_distance = distance;
                            best_cluster = cluster;
                        }
                    }
                    assignments[point] = best_cluster;

                    // Sum up and count points for each cluster.
                    //
                    const size_type cluster = assignments[point];

                    new_means[cluster] = new_means[cluster] + value;
                    counts[cluster] += 1.0;
                }
            }

            bool    done = true;

            // Divide sums by counts to get new centroids.
            //
            for (size_type cluster = 0; cluster < K; ++cluster) [[likely]]  {
                // Turn 0/0 into 0/1 to avoid zero division.
                const double        count =
                    std::max<double>(1.0, counts[cluster]);
                const value_type    value = new_means[cluster] / count;
                value_type          &result = result_[cluster];

                if (dfunc_(value, result) > 0.0000001)  {
                    done = false;
                    result = value;
                }
            }

            if (done)  break;
        }
    }

    // Using the calculated means, separate the given column into clusters
    //
    template<forward_iterator H>
    inline void
    calc_clusters_(const H &column_begin, size_type col_s)  {

        cluster_type    clusters;

        for (size_type i = 0; i < K; ++i) [[likely]]  {
            clusters[i].reserve(col_s / K + 2);
            clusters[i].push_back(&(result_[i]));
        }

        for (size_type j = 0; j < col_s; ++j) [[likely]]  {
            const value_type    &value = *(column_begin + j);

            if (! is_nan__(value)) [[likely]]  {
                double      min_dist = std::numeric_limits<double>::max();
                size_type   min_idx;

                for (size_type i = 0; i < K; ++i)  {
                    const double    dist = dfunc_(value, result_[i]);

                    if (dist < min_dist)  {
                        min_dist = dist;
                        min_idx = i;
                    }
                }
                clusters[min_idx].push_back(&value);
            }
        }

        clusters_.swap(clusters);
    }

public:

    template<typename IV, typename H>
    inline void
    operator() (const IV &idx_begin, const IV &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        calc_k_means_(column_begin, col_s);
        if (cc_)
            calc_clusters_(column_begin, col_s);
    }

    inline void pre ()  { for (auto &iter : clusters_) iter.clear();  }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }
    inline const cluster_type &get_clusters () const  { return (clusters_); }
    inline cluster_type &get_clusters ()  { return (clusters_); }

    explicit
    KMeansVisitor(
        size_type num_of_iter,
        bool calc_clusters = true,
        distance_func f =
            [](const value_type &x, const value_type &y) -> double  {
                return ((x - y) * (x - y));
            },
        seed_t seed = seed_t(-1))
        : iter_num_(num_of_iter),
          cc_(calc_clusters),
          seed_(seed),
          dfunc_(f) {  }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  AffinityPropVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    template<typename U>
    using vec_t = std::vector<U, typename allocator_declare<U, A>::type>;

    using result_type = VectorConstPtrView<value_type, A>;
    using cluster_type = vec_t<VectorConstPtrView<value_type, A>>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    const size_type iter_num_;
    distance_func   dfunc_;
    const double    dfactor_;
    result_type     result_ { };  // Centers

    template<forward_iterator H>
    inline vec_t<double>
    get_similarity_(const H &column_begin, size_type csize)  {

        vec_t<double>   simil((csize * (csize + 1)) / 2, 0.0);
        double          min_dist = std::numeric_limits<double>::max();

        // Compute similarity between distinct data points i and j
        //
        for (size_type i = 0; i < csize - 1; ++i) [[likely]]  {
            const value_type    &i_val = *(column_begin + i);

            for (size_type j = i + 1; j < csize; ++j)  [[likely]]  {
                const double    dist = -dfunc_(i_val, *(column_begin + j));

                simil[(i * csize) + j - ((i * (i + 1)) >> 1)] = dist;
                if (dist < min_dist)  min_dist = dist;
            }
        }

        // Assign min to diagonals
        //
        for (size_type i = 0; i < csize; ++i)
            simil[(i * csize) + i - ((i * (i + 1)) >> 1)] = min_dist;

        return (simil);
    }

    inline void
    get_avail_and_respon(const vec_t<double> &simil,
                         size_type csize,
                         vec_t<double> &avail,
                         vec_t<double> &respon)  {

        avail.resize(csize * csize, 0.0);
        respon.resize(csize * csize, 0.0);

        for (size_type m = 0; m < iter_num_; ++m) [[likely]]  {
            // Update responsibility
            //
            for (size_type i = 0; i < csize; ++i) [[likely]]  {
                for (size_type j = 0; j < csize; ++j) [[likely]]  {
                    double  max_diff = -std::numeric_limits<double>::max();

                    for (size_type jj = 0; jj < csize; ++jj)  {
                        if (jj ^ j) [[likely]]   {
                            const double    value =
                               simil[(i * csize) + jj - ((i * (i + 1)) >> 1)] +
                               avail[jj * csize + i];

                            if (value > max_diff)
                                max_diff = value;
                        }
                    }

                    respon[j * csize + i] =
                        (1.0 - dfactor_) *
                        (simil[(i * csize) + j - ((i * (i + 1)) >> 1)] -
                        max_diff) +
                        dfactor_ * respon[j * csize + i];
                }
            }

            // Update availability
            // Do diagonals first
            //
            for (size_type i = 0; i < csize; ++i) [[likely]]  {
                const size_type s1 = i * csize;
                double          sum = 0.0;

                for (size_type ii = 0; ii < csize; ++ii) [[likely]]
                    if (ii ^ i)
                        sum += std::max(0.0, respon[s1 + ii]);

                avail[s1 + i] =
                    (1.0 - dfactor_) * sum + dfactor_ * avail[s1 + i];
            }
            for (size_type i = 0; i < csize; ++i) [[likely]]  {
                for (size_type j = 0; j < csize; ++j)  [[likely]] {
                    if (i ^ j) [[likely]]  {  // Not equal
                        const size_type s1 = j * csize;
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

    }

public:

    template<forward_iterator IV, forward_iterator H>
    inline void
    operator() (const IV &idx_begin, const IV &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        const vec_t<double> simil =
            std::move(get_similarity_(column_begin, col_s));
        vec_t<double>       avail;
        vec_t<double>       respon;

        get_avail_and_respon(simil, col_s, avail, respon);

        result_.reserve(std::min(col_s / 100, size_type(16)));
        for (size_type i = 0; i < col_s; ++i) [[likely]]  {
            if (respon[i * col_s + i] + avail[i * col_s + i] > 0.0)
                result_.push_back(&*(column_begin + i));
        }
    }

    // Using the calculated means, separate the given column into clusters
    //
    template<forward_iterator IV, forward_iterator H>
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

            for (size_type j = 0; j < col_s; ++j) [[likely]]  {
                double              min_dist =
                    std::numeric_limits<double>::max();
                size_type           min_idx;
                const value_type    &j_val = *(column_begin + j);

                for (size_type i = 0; i < centers_size; ++i)  {
                    const double    dist = dfunc_(j_val, result_[i]);

                    if (dist < min_dist)  {
                        min_dist = dist;
                        min_idx = i;
                    }
                }
                clusters[min_idx].push_back(&j_val);
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
            [](const value_type &x, const value_type &y) -> double  {
                return ((x - y) * (x - y));
            },
        double damping_factor = 0.9)
        : iter_num_(num_of_iter), dfunc_(f), dfactor_(damping_factor)  {   }
};

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  FastFourierTransVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    template<typename U>
    using vec_t = std::vector<U, typename allocator_declare<U, A>::type>;
    using result_type =
        typename std::conditional<is_complex<T>::value,
                                  vec_t<T>,
                                  vec_t<std::complex<T>>>::type;
    using real_t = typename result_type::value_type::value_type;

private:

    using cplx_t = typename result_type::value_type;

    static inline result_type
    convolve_(result_type xvec, result_type yvec, long thread_level)  {

        transform_(xvec, false, thread_level);
        transform_(yvec, false, thread_level);

        const real_t    col_s = real_t(xvec.size());

        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    size_type(col_s),
                    [&xvec, &yvec](auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            xvec[i] *= yvec[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(xvec.begin(), xvec.end(),
                           yvec.begin(),
                           xvec.begin(),
                           std::multiplies<cplx_t>());
        }

        transform_(xvec, true, thread_level);

        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    size_type(col_s),
                    [&xvec, col_s](auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            xvec[i] /= col_s;
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(xvec.begin(), xvec.end(),
                           xvec.begin(),
                           [col_s] (const cplx_t &v) -> cplx_t  {
                               return (v / col_s);
                           });
        }
        return (xvec);
    }

    static inline size_type
    reverse_bits_(size_type val, size_type width)  {

        size_type   result { 0 };

        for (size_type i = 0; i < width; i++, val >>= 1) [[likely]]
            result = (result << 1) | (val & 1U);
        return (result);
    }

    static inline void
    fft_radix2_(result_type &column, bool reverse, long thread_level)  {

        const size_type col_s { column.size() };
        size_type       levels { 0 };

        // Compute levels = floor(log2(col_s))
        //
        for (size_type i = col_s; i > 1; i >>= 1) [[likely]]
            levels += 1;

        // Trigonometric table
        //
        const size_type half_col_s { col_s / 2 };
        const real_t    two_pi
            { (reverse ? real_t(2) : -real_t(2)) * real_t(M_PI) };
        result_type     exp_table (half_col_s);

        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    half_col_s,
                    [&exp_table, two_pi, col_s]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            exp_table[i] =
                                std::polar(real_t(1),
                                           two_pi * real_t(i) / real_t(col_s));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i = 0; i < half_col_s; i++) [[likely]]
                exp_table[i] =
                    std::polar(real_t(1), two_pi * real_t(i) / real_t(col_s));
        }

        // Bit-reversed addressing permutation
        //
        for (size_type i = 0; i < col_s; i++) [[likely]]  {
            const size_type rb { reverse_bits_(i, levels) };

            if (rb > i)  std::swap(column[i], column[rb]);
        }

        // Cooley-Tukey decimation-in-time radix-2 FFT
        //
        for (size_type s = 2; s <= col_s; s *= 2) [[likely]]  {
            const size_type half_size { s / 2 };
            const size_type table_step { col_s / s };

            for (size_type i = 0; i < col_s; i += s) [[likely]]  {
                for (size_type j = i, k = 0; j < i + half_size;
                     j++, k += table_step) [[likely]]  {
                    const cplx_t    temp
                        { column[j + half_size] * exp_table[k] };

                    column[j + half_size] = column[j] - temp;
                    column[j] += temp;
                }
            }
        }
    }

    static inline void
    fft_bluestein_(result_type &column, bool reverse, long thread_level)  {

        const size_type col_s { column.size() };

        // Trigonometric table
        //
        result_type     exp_table (col_s);
        const size_type col_s_2 { col_s * 2 };
        const real_t    pi { reverse ? real_t(M_PI) : -real_t(M_PI) };

        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&exp_table, pi, col_s, col_s_2]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            const real_t    sq = real_t((i * i) % col_s_2);

                            exp_table[i] =
                                std::polar(real_t(1), pi * sq / real_t(col_s));
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i = 0; i < col_s; i++) [[likely]]  {
                const real_t    sq = real_t((i * i) % col_s_2);

                exp_table[i] = std::polar(real_t(1), pi * sq / real_t(col_s));
            }
        }

        // Find a power of 2 convolution length m such that m >= col_s * 2 + 1
        //
        size_type   m { 1 };

        while (m / 2 <= col_s)   m *= 2;

        // Temporary vectors and preprocessing
        //
        result_type xvec (m, cplx_t(0, 0));

        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&exp_table, &xvec, &column]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            xvec[i] = column[i] * exp_table[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i = 0; i < col_s; i++) [[likely]]
                xvec[i] = column[i] * exp_table[i];
        }

        result_type yvec(m, cplx_t(0, 0));

        yvec[0] = exp_table[0];
        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(1),
                    col_s,
                    [&exp_table, &yvec, m]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            yvec[i] = yvec[m - i] = std::conj(exp_table[i]);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            for (size_type i = 1; i < col_s; i++) [[likely]]
                yvec[i] = yvec[m - i] = std::conj(exp_table[i]);
        }

        // Convolution
        //
        const result_type   conv (convolve_(std::move(xvec),
                                            std::move(yvec),
                                            thread_level));

        // Postprocessing
        //
        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    exp_table.size(),
                    [&exp_table, &conv, &column]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            column[i] = exp_table[i] * conv[i];
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(exp_table.begin(), exp_table.end(),
                           conv.begin(), column.begin(),
                           std::multiplies<cplx_t>());
        }
    }

    static inline void
    transform_(result_type &column, bool reverse, long thread_level)  {

        const size_type col_s { column.size() };

        if (col_s == 0) [[unlikely]]
            return;
        if ((col_s & (col_s - 1)) == 0)  // Is power of 2
            fft_radix2_(column, reverse, thread_level);
        else  // More complicated algorithm for arbitrary sizes
            fft_bluestein_(column, reverse, thread_level);
    }

    static inline void
    itransform_(result_type &column, long thread_level)  {

        const size_type col_s { column.size() };

        // Conjugate the complex numbers
        //
        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            auto    &val = column[i];

                            val = std::conj(val);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column.begin(), column.end(),
                           column.begin(),
                           [] (const cplx_t &v) -> cplx_t  {
                               return (std::conj(v));
                           });
        }

        // Forward fft
        //
        if ((col_s & (col_s - 1)) == 0)  // Is power of 2
            fft_radix2_(column, false, thread_level);
        else  // More complicated algorithm for arbitrary sizes
            fft_bluestein_(column, false, thread_level);

        // Conjugate the complex numbers again
        // Then scale the numbers
        //
        if (thread_level > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]  {
                            auto    &val = column[i];

                            val = std::conj(val);
                        }
                    });

            for (auto &fut : futures)  fut.get();
            futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column, col_s]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i) [[likely]]
                            column[i] /= real_t(col_s);
                    });
            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column.begin(), column.end(),
                           column.begin(),
                           [] (const cplx_t &v) -> cplx_t  {
                               return (std::conj(v));
                           });
            std::transform(column.begin(), column.end(),
                           column.begin(),
                           [col_s] (const cplx_t &v) -> cplx_t  {
                               return (v / real_t(col_s));
                           });
        }
    }

public:

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        result_type result (col_s);

        if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
            std::vector<std::future<void>>  futures;

            if constexpr (is_complex<T>::value)  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, &result]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i) [[likely]]
                                result[i]= *(column_begin + i);
                        });
            }
            else  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, &result]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i) [[likely]]
                                result[i] =
                                    std::complex<T>(*(column_begin + i), 0);
                        });
            }
            for (auto &fut : futures)  fut.get();
        }
        else  {
            if constexpr (is_complex<T>::value)  {
                std::transform(column_begin, column_end,
                               result.begin(),
                               [] (T v) -> cplx_t  { return (v); });
            }
            else  {
                std::transform(column_begin, column_end,
                               result.begin(),
                               [] (T v) -> cplx_t  {
                                   return (std::complex<T>(v, 0));
                               });
            }
        }

        if (inverse_)
            itransform_(result, thread_level_);
        else
            transform_(result, false, thread_level_);
        result_.swap(result);
    }

    inline void pre ()  {

        result_.clear();
        magnitude_.clear();
        angle_.clear();
    }
    inline void post ()  {  }

    DEFINE_RESULT
    inline const vec_t<real_t> &
    get_magnitude() const  {

        return (const_cast<FastFourierTransVisitor<T, I> *>
                    (this)->get_magnitude());
    }
    inline vec_t<real_t> &
    get_magnitude()  {

        if (magnitude_.empty())  {
            const size_type col_s = result_.size();

            if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
                magnitude_.resize(col_s);

                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)
                                this->magnitude_[i] =
                                    std::sqrt(std::norm(this->result_[i]));
                        });

                for (auto &fut : futures)  fut.get();
            }
            else  {
                magnitude_.reserve(col_s);
                for (const auto &citer : result_) [[likely]]
                    magnitude_.push_back(std::sqrt(std::norm(citer)));
            }
        }
        return (magnitude_);
    }
    inline const vec_t<real_t> &
    get_angle() const  {

        return (const_cast<FastFourierTransVisitor<T, I> *>
                    (this)->get_angle());
    }
    inline vec_t<real_t> &
    get_angle()  {

        if (angle_.empty())  {
            const size_type col_s = result_.size();

            if (thread_level_ > 2 && col_s >= ThreadPool::MUL_THR_THHOLD)  {
                angle_.resize(col_s);

                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)
                                this->angle_[i] = std::arg(this->result_[i]);
                        });

                for (auto &fut : futures)  fut.get();
            }
            else  {
                angle_.reserve(col_s);
                for (const auto &citer : result_) [[likely]]
                    angle_.push_back(std::arg(citer));
            }
        }
        return (angle_);
    }

    explicit
    FastFourierTransVisitor(bool inverse = false)
        : inverse_(inverse),
          thread_level_(ThreadGranularity::get_thread_level())  {   }

private:

    const bool      inverse_;
    const long      thread_level_;
    result_type     result_ {  };
    vec_t<real_t>   magnitude_ {  };
    vec_t<real_t>   angle_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using fft_v = FastFourierTransVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  EntropyVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ == 0)  return;

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>  sum_v(SumVisitor<T, I>(),
                                                            roll_count_);

        sum_v.pre();
        sum_v (idx_begin, idx_end, column_begin, column_end);
        sum_v.post();

        result_type result = std::move(sum_v.get_result());

        if (result.size() >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        result.size(),
                        [&column_begin, &result, this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                value_type          &r = result[i];
                                const value_type    val =
                                    *(column_begin + i) / r;

                                r = -val * std::log(val) /
                                    std::log(this->log_base_);
                            }
                        });

                for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result.begin(),
                           result.begin(),
                           [this](auto c, auto r) -> value_type  {
                               const value_type    val = c / r;

                               return (-val * std::log(val) /
                                       std::log(this->log_base_));
                           });
        }

        sum_v.pre();
        sum_v (idx_begin, idx_end,  // the idx iterators are unused
               result.begin() + (roll_count_ - 1), result.end());
        sum_v.post();

        for (size_type i = 0; i < roll_count_ - 1; ++i) [[likely]]
            result[i] = get_nan<value_type>();
        for (size_type i = 0; i < sum_v.get_result().size(); ++i) [[likely]]
            result[i + roll_count_ - 1] = sum_v.get_result()[i];

        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    EntropyVisitor(size_type roll_count, value_type log_base = 2)
        : roll_count_(roll_count), log_base_(log_base)  {   }

private:

    OBO_PORT_DECL

    const size_type     roll_count_;
    const value_type    log_base_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using ent_v = EntropyVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  ImpurityVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        std::vector<double, typename allocator_declare<double, A>::type>;

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

        if (roll_count_ == 0 || roll_count_ > col_s)  return;

        map_t   table (roll_count_ / 2 + 1);

        for (size_type i = 0; i < roll_count_; ++i) [[likely]]  {
            auto    ret = table.insert(std::pair(*(column_begin + i), 0));

            ret.first->second += 1.0;
        }

        result_type result;
        auto        func =
            [this, col_s, &table, &result, &column_begin]
            (auto i, auto sum) -> bool  {
                result.push_back(sum);

                const size_type roll_end = i + this->roll_count_;

                if (roll_end > col_s)  return (false);

                auto    find_ret = table.find(*(column_begin + (i - 1)));

                find_ret->second -= 1.0; // It must find it -- no need to check
                if (find_ret->second == 0)
                    table.erase(find_ret);

                auto    insert_ret =
                    table.insert(
                        std::pair(*(column_begin + (roll_end - 1)), 0));

                insert_ret.first->second += 1.0;
                return (true);
            };

        result.reserve(col_s);
        if (imt_ == impurity_type::gini_index)  {
            for (size_type i = 1; i < col_s; ++i) [[likely]]  {
                double  sum = 0;

                for (const auto &citer : table)  {
                    const auto  prob = citer.second / double(roll_count_);

                    sum += prob * prob;
                }
                sum = 1.0 - sum;
                if (! func(i, sum))  break;
            }
        }
        else  {  // impurity_type::info_entropy
            for (size_type i = 1; i < col_s; ++i) [[likely]]  {
                double  sum = 0;

                for (const auto &citer : table)  {
                    const auto  prob = citer.second / double(roll_count_);

                    sum += prob * std::log2(prob);
                }
                sum = -sum;
                if (! func(i, sum))  break;
            }
        }
        result_.swap(result);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    ImpurityVisitor(size_type roll_count, impurity_type it)
        : roll_count_(roll_count), imt_(it)  {   }

private:

    using map_t = std::unordered_map<
        T, double,
        std::hash<T>,
        std::equal_to<T>,
        typename allocator_declare<std::pair<const T, double>, A>::type>;

    OBO_PORT_DECL

    result_type         result_ { };
    const size_type     roll_count_;
    const impurity_type imt_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using impu_v = ImpurityVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  SigmoidVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

private:

    template <forward_iterator H>
    inline void logistic_(const H &column_begin, const H &column_end,
                          size_type col_s, long thread_level)  {

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)
                           this->result_[i] =
                               T(1) / (T(1) + std::exp(-*(column_begin + i)));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result_.begin(),
                           [](auto val) -> value_type  {
                               return (T(1) / (T(1) + std::exp(-val)));
                           });
        }
    }
    template <forward_iterator H>
    inline void algebraic_(const H &column_begin, const H &column_end,
                           size_type col_s, long thread_level)  {

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)
                           this->result_[i] =
                               T(1) /
                               std::sqrt(T(1) +
                                         std::pow(*(column_begin + i), T(2)));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result_.begin(),
                           [](auto val) -> value_type  {
                           return (T(1) /
                                   std::sqrt(T(1) + std::pow(val, T(2))));
                           });
        }
    }
    template <forward_iterator H>
    inline void hyperbolic_tan_(const H &column_begin, const H &column_end,
                                size_type col_s, long thread_level)  {

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)
                            this->result_[i] = std::tanh(*(column_begin + i));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result_.begin(),
                           [](auto val) -> value_type  {
                               return (std::tanh(val));
                           });
        }
    }
    template <forward_iterator H>
    inline void arc_tan_(const H &column_begin, const H &column_end,
                         size_type col_s, long thread_level)  {

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)
                            this->result_[i] = std::atan(*(column_begin + i));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result_.begin(),
                           [](auto val) -> value_type  {
                               return (std::atan(val));
                           });
        }
    }
    template <forward_iterator H>
    inline void error_function_(const H &column_begin, const H &column_end,
                                size_type col_s, long thread_level)  {

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)
                            this->result_[i] = std::erf(*(column_begin + i));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result_.begin(),
                           [](auto val) -> value_type  {
                               return (std::erf(val));
                           });
        }
    }
    template <forward_iterator H>
    inline void gudermannian_(const H &column_begin, const H &column_end,
                              size_type col_s, long thread_level)  {

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)
                            this->result_[i] =
                                std::atan(std::sinh(*(column_begin + i)));
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result_.begin(),
                           [](auto val) -> value_type  {
                               return (std::atan(std::sinh(val)));
                           });
        }
    }
    template <forward_iterator H>
    inline void smoothstep_(const H &column_begin, const H &column_end,
                            size_type col_s, long thread_level)  {

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&column_begin, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)  {
                            const value_type    val = *(column_begin + i);

                            if (val <= 0)
                                this->result_[i] = 0;
                            else if (val >= T(1))
                                this->result_[i] = T(1);
                            else
                                this->result_[i] =
                                    val * val * (T(3) - T(2) * val);
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result_.begin(),
                           [](auto val) -> value_type  {
                               if (val <= 0)
                                   return (0);
                               else if (val >= T(1))
                                   return (T(1));
                               else
                                   return (val * val * (T(3) - T(2) * val));
                           });
        }
    }

public:

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &, const K &,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        result_.resize(std::distance(column_begin, column_end));
        if (sigmoid_type_ == sigmoid_type::logistic)
            logistic_(column_begin, column_end, col_s, thread_level);
        else if (sigmoid_type_ == sigmoid_type::algebraic)
            algebraic_(column_begin, column_end, col_s, thread_level);
        else if (sigmoid_type_ == sigmoid_type::hyperbolic_tan)
            hyperbolic_tan_(column_begin, column_end, col_s, thread_level);
        else if (sigmoid_type_ == sigmoid_type::arc_tan)
            arc_tan_(column_begin, column_end, col_s, thread_level);
        else if (sigmoid_type_ == sigmoid_type::error_function)
            error_function_(column_begin, column_end, col_s, thread_level);
        else if (sigmoid_type_ == sigmoid_type::gudermannian)
            gudermannian_(column_begin, column_end, col_s, thread_level);
        else if (sigmoid_type_ == sigmoid_type::smoothstep)
            smoothstep_(column_begin, column_end, col_s, thread_level);
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    SigmoidVisitor(sigmoid_type st) : sigmoid_type_(st)  {   }

private:

    OBO_PORT_DECL

    result_type         result_ {  }; // Sigmoids
    const sigmoid_type  sigmoid_type_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using sigm_v = SigmoidVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  RectifyVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

public:

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            std::vector<std::future<void>>  futures;

            result_.resize(col_s);
            if (rtype_ == rectify_type::ReLU)  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)
                                this->result_[i] =
                                    std::max(T(0), *(column_begin + i));
                        });
            }
            else if (rtype_ == rectify_type::param_ReLU)  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    v = *(column_begin + i);

                                this->result_[i] =
                                    std::max(v * this->param_, v);
                            }
                        });
            }
            else if (rtype_ == rectify_type::GeLU)  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    v = *(column_begin + i);

                                this->result_[i] =
                                    v * this->standard_normal_dist_(v);
                            }
                        });
            }
            else if (rtype_ == rectify_type::SiLU)  {
                sigm_v<T, I, A> sigm(sigmoid_type::logistic);

                sigm.pre();
                sigm(idx_begin, idx_end, column_begin, column_end);
                sigm.post();
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, &sigm = std::as_const(sigm), this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    col = *(column_begin + i);
                                const value_type    sig = sigm.get_result()[i];

                                this->result_[i] = col * sig;
                            }
                        });
            }
            else if (rtype_ == rectify_type::softplus)  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    v = *(column_begin + i);

                                this->result_[i] = softp_(v, this->param_);
                            }
                        });
            }
            else if (rtype_ == rectify_type::elu)  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    v = *(column_begin + i);

                                if (v > 0)
                                    this->result_[i] = v;
                                else
                                    this->result_[i] =
                                        this->param_ * (std::exp(v) - T(1));
                            }
                        });
            }
            else if (rtype_ == rectify_type::mish)  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    v = *(column_begin + i);

                                this->result_[i] =
                                    v * std::tanh(softp_(v, this->param_));
                            }
                        });
            }
            else if (rtype_ == rectify_type::metallic_mean)  {
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&column_begin, this]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    v = *(column_begin + i);

                                this->result_[i] =
                                    (v + std::sqrt(v * v + T(4))) / T(2);
                            }
                        });
            }

            for (auto &fut : futures)  fut.get();
        }
        else  {
            result_.reserve(col_s);
            if (rtype_ == rectify_type::ReLU)  {
                std::for_each(column_begin, column_end,
                              [this](const value_type &v) -> void  {
                                  this->result_.push_back(std::max(T(0), v));
                              });
            }
            else if (rtype_ == rectify_type::param_ReLU)  {
                std::for_each(column_begin, column_end,
                              [this](const value_type &v) -> void  {
                                  this->result_.push_back(
                                      std::max(v * this->param_, v));
                              });
            }
            else if (rtype_ == rectify_type::GeLU)  {
                std::for_each(column_begin, column_end,
                              [this](const value_type &v) -> void  {
                                  this->result_.push_back(
                                      v * this->standard_normal_dist_(v));
                              });
            }
            else if (rtype_ == rectify_type::SiLU)  {
                sigm_v<T, I, A> sigm(sigmoid_type::logistic);

                sigm.pre();
                sigm(idx_begin, idx_end, column_begin, column_end);
                sigm.post();

                std::transform(column_begin, column_end,
                               sigm.get_result().begin(),
                               std::back_inserter(result_),
                               [](auto col, auto sig) -> value_type  {
                                   return (col * sig);
                               });
            }
            else if (rtype_ == rectify_type::softplus)  {
                std::for_each(column_begin, column_end,
                              [this](const value_type &v) -> void  {
                                  this->result_.push_back(
                                      softp_(v, this->param_));
                              });
            }
            else if (rtype_ == rectify_type::elu)  {
                std::for_each(column_begin, column_end,
                              [this](const value_type &v) -> void   {
                                  if (v > 0)
                                      this->result_.push_back(v);
                                  else
                                      this->result_.push_back(
                                          this->param_ * (std::exp(v) - T(1)));
                              });
            }
            else if (rtype_ == rectify_type::mish)  {
                std::for_each(column_begin, column_end,
                              [this](const value_type &v) -> void  {
                                  this->result_.push_back(
                                      v * std::tanh(softp_(v, this->param_)));
                              });
            }
            else if (rtype_ == rectify_type::metallic_mean)  {
                std::for_each(column_begin, column_end,
                              [this](const value_type &v) -> void  {
                                  this->result_.push_back(
                                      (v + std::sqrt(v * v + T(4))) / T(2));
                              });
            }
        }
    }

    OBO_PORT_OPT

    inline void pre ()  {

        OBO_PORT_PRE
        result_.clear();
    }
    inline void post ()  { OBO_PORT_POST }
    DEFINE_RESULT

    explicit
    RectifyVisitor(rectify_type r_type, value_type param = 1)
        : param_(param), rtype_(r_type)  {   }

private:

    inline static value_type
    softp_(const value_type &v, const value_type &p)  {

        return(std::log(T(1) + std::exp(p * v)) / p);
    }
    inline static value_type
    standard_normal_dist_(const value_type &v)  {

        static constexpr value_type two = 2;
        static const     value_type sqrt_dbl_pi = std::sqrt(two * M_PI);

        return (std::exp(-(v * v) / two) / sqrt_dbl_pi);
    }

    OBO_PORT_DECL

    result_type         result_ {  };
    const value_type    param_;
    const rectify_type  rtype_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using recf_v = RectifyVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  PolicyLearningLossVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K & /*idx_begin*/, const K & /*idx_end*/,
                const H &action_prob_begin, const H &action_prob_end,
                const H &reward_begin, const H &reward_end)  {

        const size_type col_s =
            std::distance(action_prob_begin, action_prob_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(reward_begin, reward_end)))
            throw DataFrameError("PolicyLearningLossVisitor: All columns must "
                                 "be of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        // Negative Log Likelihood
        //
        result_.resize(col_s);
        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {

            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&action_prob_begin, &reward_begin, this]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)  {
                            const value_type    ap = *(action_prob_begin + i);
                            const value_type    r = *(reward_begin + i);

                            this->result_[i] = -std::log(ap) * r;
                        }
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(action_prob_begin, action_prob_end,
                           reward_begin,
                           result_.begin(),
                           [](const T &ap, const T &r) -> T  {
                               return (-std::log(ap) * r);
                           });
        }
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    PolicyLearningLossVisitor() = default;

private:

    result_type result_ {  };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using plloss_v = PolicyLearningLossVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long>
struct  LossFunctionVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES_2

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &actual_begin, const H &actual_end,
                const H &model_begin, const H &model_end)  {

        const size_type col_s = std::distance(actual_begin, actual_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s != size_type(std::distance(model_begin, model_end)))
            throw DataFrameError("LossFunctionVisitor: All columns must be of "
                                 "equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS

        // The linear and parallel versions on this type are the same.
        // So, I am taking it out of the if-else chain
        //
        if (lft_ == loss_function_type::cosine_similarity)  {
            DotProdVisitor<T, I>    dot_v;

            dot_v.pre();
            dot_v (idx_begin, idx_end,
                   actual_begin, actual_end, model_begin, model_end);
            dot_v.post();

            result_ = dot_v.get_result() /
                      (dot_v.get_magnitude1() * dot_v.get_magnitude2());
            return;
        }

        if (col_s >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
            std::vector<std::future<void>>  futures;

            if (lft_ == loss_function_type::kullback_leibler)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    a = *(actual_begin + i);
                                const value_type    m = *(model_begin + i);

                                sum += a * std::log(a / m);
                            }
                            return (sum);
                        });

                for (auto &fut : futures)  result_ += fut.get();
            }
            else if (lft_ == loss_function_type::mean_abs_error)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    a = *(actual_begin + i);
                                const value_type    m = *(model_begin + i);

                                sum += std::fabs(a - m);
                            }
                            return (sum);
                        });

                for (auto &fut : futures)  result_ += fut.get();
                result_ /= col_s;
            }
            else if (lft_ == loss_function_type::mean_sqr_error)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    val =
                                    *(actual_begin + i) - *(model_begin + i);

                                sum += val * val;
                            }
                            return (sum);
                        });

                for (auto &fut : futures)  result_ += fut.get();
                result_ /= col_s;
            }
            else if (lft_ == loss_function_type::mean_sqr_log_error)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    val =
                                    std::log(T(1) + *(actual_begin + i)) -
                                    std::log(T(1) + *(model_begin + i));

                                sum += val * val;
                            }
                            return (sum);
                        });

                for (auto &fut : futures)  result_ += fut.get();
                result_ /= col_s;
            }
            else if (lft_ == loss_function_type::cross_entropy)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    a = *(actual_begin + i);
                                const value_type    m = *(model_begin + i);

                                sum += a * std::log(m);
                            }
                            return (sum);
                        });

                for (auto &fut : futures)  result_ += fut.get();
                result_ = -(result_ / col_s);
            }
            else if (lft_ == loss_function_type::binary_cross_entropy)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    a = *(actual_begin + i);
                                const value_type    m = *(model_begin + i);

                                sum += -(a * std::log(m)) +
                                       (1 - a) * std::log(1 - m);
                            }
                            return (sum);
                        });

                for (auto &fut : futures)  result_ += fut.get();
                result_ /= col_s;
            }
            else if (lft_ == loss_function_type::categorical_hinge)  {
                auto        futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    a = *(actual_begin + i);
                                const value_type    m = *(model_begin + i);

                                sum += (T(1) - a) * m;
                            }
                            return (sum);
                        });
                value_type  neg { 0 };

                for (auto &fut : futures)  neg += fut.get();
                futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    a = *(actual_begin + i);
                                const value_type    m = *(model_begin + i);

                                sum += a * m;
                            }
                            return (sum);
                        });

                value_type  pos { 0 };

                for (auto &fut : futures)  pos += fut.get();
                result_ = std::max(neg - pos + T(1), T(0));;
            }
            else if (lft_ == loss_function_type::log_cosh)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        col_s,
                        [&actual_begin, &model_begin]
                        (auto begin, auto end) -> value_type  {
                            value_type  sum { 0 };

                            for (size_type i = begin; i < end; ++i)  {
                                const value_type    a = *(actual_begin + i);
                                const value_type    m = *(model_begin + i);

                                sum += std::log(std::cosh(m - a));
                            }
                            return (sum);
                        });

                for (auto &fut : futures)  result_ += fut.get();
                result_ /= col_s;
            }
        }
        else  {
            if (lft_ == loss_function_type::kullback_leibler)  {
                result_ =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T  {
                                              return (a * std::log(a / m));
                                          });
            }
            else if (lft_ == loss_function_type::mean_abs_error)  {
                result_ =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T {
                                              return (std::fabs(a - m));
                                          });
                result_ /= col_s;
            }
            else if (lft_ == loss_function_type::mean_sqr_error)  {
                result_ =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T  {
                                              const T   val = a - m;

                                              return (val * val);
                                          });
                result_ /= col_s;
            }
            else if (lft_ == loss_function_type::mean_sqr_log_error)  {
                result_ =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T  {
                                              const T   val =
                                                  std::log(T(1) + a) -
                                                  std::log(T(1) + m);

                                              return (val * val);
                                          });
                result_ /= col_s;
            }
            else if (lft_ == loss_function_type::cross_entropy)  {
                result_ =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T  {
                                              return (a * std::log(m));
                                          });
                result_ = -(result_ / col_s);
            }
            else if (lft_ == loss_function_type::binary_cross_entropy)  {
                result_ =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T  {
                                              return (
                                                  -(a * std::log(m)) +
                                                  (1 - a) * std::log(1 - m));
                                          });
                result_ /= col_s;
            }
            else if (lft_ == loss_function_type::categorical_hinge)  {
                const result_type   neg =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T  {
                                              return ((T(1) - a) * m);
                                          });
                const result_type   pos =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T  {
                                              return (a * m);
                                          });

                result_ = std::max(neg - pos + T(1), T(0));;
            }
            else if (lft_ == loss_function_type::log_cosh)  {
                result_ =
                    std::transform_reduce(actual_begin, actual_end,
                                          model_begin, T(0), std::plus { },
                                          [](const T &a, const T &m) -> T  {
                                              return (
                                                  std::log(std::cosh(m - a)));
                                          });
                result_ /= col_s;
            }
        }
    }

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }

    inline result_type get_result() const  { return (result_); }

    explicit
    LossFunctionVisitor(loss_function_type lft) : lft_(lft)  {   }

private:

    result_type                 result_ { 0 };
    const loss_function_type    lft_;
};

template<typename T, typename I = unsigned long>
using loss_v = LossFunctionVisitor<T, I>;

// ----------------------------------------------------------------------------

template<vector_sim_type TYP, typename T, typename I = unsigned long>
struct  VectorSimilarityVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type = double;

    template <forward_iterator K, forward_iterator H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin1, const H &column_end1,
                const H &column_begin2, const H &column_end2)  {

        const size_type col_s1 = std::distance(column_begin1, column_end1);
        const size_type col_s2 = std::distance(column_begin2, column_end2);

        if constexpr (TYP != vector_sim_type::jaccard_similarity &&
                      TYP != vector_sim_type::hamming_dist)  {
            DotProdVisitor<T, I>    dot_v;

            dot_v.pre();
            dot_v (idx_begin, idx_end,
                   column_begin1, column_end1, column_begin2, column_end2);
            dot_v.post();

            if constexpr (TYP == vector_sim_type::euclidean_dist)
                result_ = dot_v.get_euclidean_dist();
            else if constexpr (TYP == vector_sim_type::manhattan_dist)
                result_ = dot_v.get_manhattan_dist();
            else if constexpr (TYP == vector_sim_type::dot_product)
                result_ = dot_v.get_result();
            else if constexpr (TYP == vector_sim_type::cosine_similarity)
                result_ = dot_v.get_result() /
                          (dot_v.get_magnitude1() * dot_v.get_magnitude2());
            else if constexpr (TYP == vector_sim_type::simple_similarity)  {
#ifdef HMDF_SANITY_EXCEPTIONS
                if (col_s1 != col_s2)
                    throw DataFrameError("VectorSimilarityVisitor: "
                                         "All columns must be of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS
                result_ = (T(1) - dot_v.get_result() * dot_v.get_result()) /
                          T(col_s1);
            }
        }
        else if constexpr (TYP == vector_sim_type::jaccard_similarity)   {
            using map_t = std::unordered_map<T, size_type>;

            map_t   tbl;

            tbl.reserve(col_s1 + col_s2);
            for (size_type i = 0; i < col_s1; ++i)
                tbl.insert(std::make_pair(*(column_begin1 + i), size_type(1)));

            size_type   intersection { 0 };

            for (size_type i = 0; i < col_s2; ++i)  {
                const auto  res =
                    tbl.insert(std::make_pair(*(column_begin2 + i),
                                              size_type(0)));

                if (! res.second && res.first->second == 1)  {
                    res.first->second += 1;
                    intersection += 1;
                }
            }
            result_ = result_type(intersection) /
                      result_type(col_s1 + col_s2 - intersection);
        }
        else  {  // Hamming distance
#ifdef HMDF_SANITY_EXCEPTIONS
            if (col_s1 != col_s2)
               throw DataFrameError("VectorSimilarityVisitor: "
                                    "All columns must be of equal sizes");
#endif // HMDF_SANITY_EXCEPTIONS
            for (size_type i = 0; i < col_s1; ++i)
                if (*(column_begin1 + i) != *(column_begin2 + i))
                    result_ += 1;
        }
    }

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }
    [[nodiscard]] inline result_type get_result () const  { return (result_); }

    VectorSimilarityVisitor() = default;

private:

    result_type result_ { 0 };
};

template<vector_sim_type TYP, typename T, typename I = unsigned long>
using vs_v = VectorSimilarityVisitor<TYP, T, I>;

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
