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
#include <DataFrame/Utils/Matrix.h>
#include <DataFrame/Vectors/VectorPtrView.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <complex>
// #include <flat_map>
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
    using order_type =
        std::array<std::vector<
                       size_type,
                       typename allocator_declare<size_type, A>::type>, K>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;
    using seed_t = std::random_device::result_type;

private:

    const size_type iter_num_;
    const bool      cc_;
    const seed_t    seed_;
    distance_func   dfunc_;
    result_type     result_ { };         // K Means
    cluster_type    clusters_ { };       // K Clusters
    order_type      clusters_idxs_ { };  // K Clusters indices

    template<typename H>
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

                    // Sum up and count points for each cluster.
                    //
                    auto    &nm = new_means[best_cluster];

                    nm = nm + value;
                    counts[best_cluster] += 1.0;
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
    template<typename H>
    inline void
    calc_clusters_(const H &column_begin, size_type col_s)  {

        cluster_type    clusters;
        order_type      clusters_idxs;

        for (size_type i = 0; i < K; ++i) [[likely]]  {
            clusters[i].reserve(col_s / K + 2);
            clusters[i].push_back(&(result_[i]));
            clusters_idxs[i].reserve(col_s / K + 2);
        }

        for (size_type j = 0; j < col_s; ++j) [[likely]]  {
            const value_type    &value = *(column_begin + j);

            if (! is_nan__(value)) [[likely]]  {
                double      min_dist { std::numeric_limits<double>::max() };
                size_type   min_idx { 0 };

                for (size_type i = 0; i < K; ++i)  {
                    const double    dist = dfunc_(value, result_[i]);

                    if (dist < min_dist)  {
                        min_dist = dist;
                        min_idx = i;
                    }
                }
                clusters[min_idx].push_back(&value);
                clusters_idxs[min_idx].push_back(j);
            }
        }

        clusters_.swap(clusters);
        clusters_idxs_.swap(clusters_idxs);
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

    inline void pre ()  {

        for (auto &iter : clusters_) iter.clear();
        for (auto &iter : clusters_idxs_) iter.clear();
    }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }
    inline const cluster_type &get_clusters () const  { return (clusters_); }
    inline cluster_type &get_clusters ()  { return (clusters_); }
    inline const order_type &
    get_clusters_idxs () const  { return (clusters_idxs_); }

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

// Time complexity is O(I*n^2) where I is number of iterations
// Space complexity is O(n^2)
//
template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  AffinityPropVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    template<typename U>
    using vec_t = std::vector<U, typename allocator_declare<U, A>::type>;

    using result_type = VectorConstPtrView<value_type, A>;
    using cluster_type = vec_t<VectorConstPtrView<value_type, A>>;
    using order_type =
        std::vector<std::vector<
                        size_type,
                        typename allocator_declare<size_type, A>::type>>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    const size_type iter_num_;
    const bool      cc_;
    const double    dfactor_;
    distance_func   dfunc_;
    result_type     result_ { };         // Centers
    cluster_type    clusters_ { };       // Clusters
    order_type      clusters_idxs_ { };  // Clusters indices

    template<typename H>
    inline vec_t<double>
    get_similarity_(const H &column_begin, long col_s)  {

        vec_t<double>   simil((col_s * (col_s + 1)) / 2, 0.0);
        double          min_dist = std::numeric_limits<double>::max();

        // Compute similarity between distinct data points i and j
        //
        for (long i = 0; i < col_s - 1; ++i) [[likely]]  {
            const value_type    &i_val = *(column_begin + i);
            const long          i_idx = (i * col_s) - ((i * (i + 1)) >> 1);

            for (long j = i + 1; j < col_s; ++j)  [[likely]]  {
                const double    dist = -dfunc_(i_val, *(column_begin + j));

                simil[i_idx + j] = dist;
                if (dist < min_dist)  min_dist = dist;
            }
        }

        // Assign min to diagonals
        //
        for (long i = 0; i < col_s; ++i)
            simil[(i * col_s) + i - ((i * (i + 1)) >> 1)] = min_dist;

        return (simil);
    }

    inline void
    get_avail_and_respon_(const vec_t<double> &simil,
                          long col_s,
                          vec_t<double> &avail,
                          vec_t<double> &respon)  {

        avail.resize(col_s * col_s, 0.0);
        respon.resize(col_s * col_s, 0.0);

        const double    one_df = 1.0 - dfactor_;

        for (size_type m = 0; m < iter_num_; ++m) [[likely]]  {
            // Update responsibility
            //
            for (long i = 0; i < col_s; ++i) [[likely]]  {
                const long  i_idx = (i * col_s) - ((i * (i + 1)) >> 1);

                for (long j = 0; j < col_s; ++j) [[likely]]  {
                    double  max_diff = -std::numeric_limits<double>::max();

                    for (long jj = 0; jj < col_s; ++jj)  {
                        if (jj ^ j) [[likely]]   {
                            const double    value =
                               simil[i_idx + jj] + avail[jj * col_s + i];

                            if (value > max_diff)
                                max_diff = value;
                        }
                    }

                    const long j_idx = j * col_s + i;

                    respon[j_idx] = one_df * (simil[i_idx + j] - max_diff) +
                                    dfactor_ * respon[j_idx];
                }
            }

            // Update availability
            // Do diagonals first
            //
            for (long i = 0; i < col_s; ++i) [[likely]]  {
                const long  s1 = i * col_s;
                const long  s2 = i * col_s + i;
                double      sum = 0.0;

                for (long ii = 0; ii < col_s; ++ii) [[likely]]
                    if (ii ^ i)
                        sum += std::max(0.0, respon[s1 + ii]);

                avail[s2] = one_df * sum + dfactor_ * avail[s2];
            }
            for (long i = 0; i < col_s; ++i) [[likely]]  {
                for (long j = 0; j < col_s; ++j)  [[likely]] {
                    if (i ^ j) [[likely]]  {  // Not equal
                        const long  s1 = j * col_s;
                        const long  s2 = j * col_s + i;
                        double      sum = 0.0;
                        const long  max_i_j = std::max(i, j);
                        const long  min_i_j = std::min(i, j);

                        for (long ii = 0; ii < min_i_j; ++ii)
                            sum += std::max(0.0, respon[s1 + ii]);
                        for (long ii = min_i_j + 1; ii < max_i_j; ++ii)
                            sum += std::max(0.0, respon[s1 + ii]);
                        for (long ii = max_i_j + 1; ii < col_s; ++ii)
                            sum += std::max(0.0, respon[s1 + ii]);

                        avail[s2] =
                            one_df *
                            std::min(0.0, respon[s1 + j] + sum) + dfactor_ *
                            avail[s2];
                    }
                }
            }
        }
    }

    // Using the calculated means, separate the given column into clusters
    //
    template<typename H>
    inline void
    calc_clusters_(const H &column_begin, long col_s)  {

        const long  centers_size = result_.size();

        if (! centers_size)  return;

        const auto  resv = col_s / centers_size;

        clusters_.resize(centers_size);
        clusters_idxs_.resize(centers_size);
        for (long i = 0; i < centers_size; ++i)  {
            clusters_[i].reserve(resv);
            clusters_idxs_[i].reserve(resv);
        }

        for (long j = 0; j < col_s; ++j) [[likely]]  {
            const value_type    &j_val = *(column_begin + j);
            double              min_dist = dfunc_(j_val, result_[0]);
            long                min_idx = 0;

            for (long i = 1; i < centers_size; ++i)  {
                const double    dist = dfunc_(j_val, result_[i]);

                if (dist < min_dist)  {
                    min_dist = dist;
                    min_idx = i;
                }
            }
            clusters_[min_idx].push_back(&j_val);
            clusters_idxs_[min_idx].push_back(size_type(j));
        }
    }

public:

    template<typename IV, typename H>
    inline void
    operator() (const IV &idx_begin, const IV &idx_end,
                const H &column_begin, const H &column_end)  {

        const long  col_s = std::min(std::distance(idx_begin, idx_end),
                                     std::distance(column_begin, column_end));

        const vec_t<double> simil = get_similarity_(column_begin, col_s);
        vec_t<double>       avail;
        vec_t<double>       respon;

        get_avail_and_respon_(simil, col_s, avail, respon);

        result_.reserve(std::min(col_s / 100, long(16)));
        for (long i = 0; i < col_s; ++i) [[likely]]  {
            const long  idx = i * col_s + i;

            if (respon[idx] + avail[idx] > 0.0)
                result_.push_back(&*(column_begin + i));
        }

        if (cc_)  calc_clusters_(column_begin,  col_s);
    }

    inline void pre ()  {

        result_.clear();
        clusters_.clear();
        clusters_idxs_.clear();
    }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }
    inline const cluster_type &get_clusters () const  { return (clusters_); }
    inline cluster_type &get_clusters ()  { return (clusters_); }
    inline const order_type &
    get_clusters_idxs () const  { return (clusters_idxs_); }

    explicit
    AffinityPropVisitor(
        size_type num_of_iter,
        bool calc_clusters = true,
        distance_func f =
            [](const value_type &x, const value_type &y) -> double  {
                return ((x - y) * (x - y));
            },
        double damping_factor = 0.9)
        : iter_num_(num_of_iter),
          cc_(calc_clusters),
          dfactor_(damping_factor), dfunc_(f)  {   }
};

// ----------------------------------------------------------------------------

// Density-Based Spatial Clustering of Applications with Noise
// Average runtime complexity is O(n log n). The worst case is O(n^2).
//
template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  DBSCANVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    template<typename U>
    using vec_t = std::vector<U, typename allocator_declare<U, A>::type>;

    using result_type = vec_t<VectorConstPtrView<value_type, A>>;
    using order_type =
        std::vector<std::vector<
                        size_type,
                        typename allocator_declare<size_type, A>::type>>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    using id_t = long;

    static constexpr id_t   UNCLASSIFIED { -1 };
    static constexpr id_t   NOISE { -2 };

    template<typename H>
    inline void
    calculate_cluster_(const H &column_begin,
                       const id_t column_idx,
                       const id_t col_s,
                       vec_t<id_t> &cluster_index)  {

        const value_type    &value = *(column_begin + column_idx);

        cluster_index.clear();
        for (id_t i = 0; i < col_s; ++i)  {
            if (dfunc_(value, *(column_begin + i)) <= max_dist_)
                cluster_index.push_back(i);
        }
    }

    template<typename H>
    inline bool
    expand_cluster_(const H &column_begin,
                    const id_t column_idx,
                    vec_t<id_t> &cluster_ids,
                    const id_t col_s,
                    const id_t cluster_id,
                    vec_t<id_t> &seeds,
                    vec_t<id_t> &cluster_neighors)  {

        calculate_cluster_(column_begin, column_idx, col_s, seeds);

        const id_t          seeds_s = id_t(seeds.size());
        const value_type    &value = *(column_begin + column_idx);

        if (seeds_s < min_mems_)  {
            cluster_ids[column_idx] = NOISE;
            return (false);
        }

        id_t    core_index { 0 };

        for (id_t i = 0; i < seeds_s; ++i)  {
            const auto  seed_val = seeds[i];

            cluster_ids[seed_val] = cluster_id;
            if (*(column_begin + seed_val) == value) [[unlikely]]
                core_index = i;
        }

        seeds.erase(seeds.begin() + core_index);
        for (id_t i = 0, n = seeds.size(); i < n; ++i)  {
            calculate_cluster_(column_begin, seeds[i], col_s, cluster_neighors);

            if (id_t(cluster_neighors.size()) >= min_mems_)  {
                for (id_t j = 0; j < id_t(cluster_neighors.size()); ++j)  {
                    auto    &cluster_val = cluster_ids[j];

                    if (cluster_val < 0)  {  // NOISE or UNCLASSIFIED
                        if (cluster_val == UNCLASSIFIED)  {
                            seeds.push_back(cluster_neighors[j]);
                            n = id_t(seeds.size());
                        }
                        cluster_val = cluster_id;
                    }
                }
            }
        }

        return (true);
    }

public:

    template<typename IV, typename H>
    inline void
    operator() (const IV &idx_begin, const IV &idx_end,
                const H &column_begin, const H &column_end)  {

        const id_t  col_s = std::min(std::distance(idx_begin, idx_end),
                                     std::distance(column_begin, column_end));
        vec_t<id_t> cluster_ids (col_s, UNCLASSIFIED);
        vec_t<id_t> seeds;
        vec_t<id_t> cluster_neighors;
        id_t        cluster_id { 0 };

        seeds.reserve(col_s / 20);
        cluster_neighors.reserve(col_s / 20);
        for (id_t i = 0; i < col_s; ++i)  {
            if (cluster_ids[i] == UNCLASSIFIED &&
                expand_cluster_(column_begin,
                                i,
                                cluster_ids,
                                col_s,
                                cluster_id,
                                seeds,
                                cluster_neighors))  {
                cluster_id += 1;
            }
        }

        const auto  resv = col_s / cluster_id;

        clusters_.resize(cluster_id);
        clusters_idxs_.resize(cluster_id);
        noisey_idxs_.reserve(std::max(id_t(8), id_t(col_s / 500)));
        for (long i = 0; i < cluster_id; ++i)  {
            clusters_[i].reserve(resv);
            clusters_idxs_[i].reserve(resv);
        }
        for (id_t i = 0; i < col_s; ++i)  {
            const auto  this_id = cluster_ids[i];

            if (this_id >= 0) [[likely]]  {
                clusters_[this_id].push_back(&(*(column_begin + i)));
                clusters_idxs_[this_id].push_back(i);
            }
            else [[unlikely]]  { noisey_idxs_.push_back(size_type(i)); }
        }
    }

    inline void pre ()  {

        clusters_.clear();
        clusters_idxs_.clear();
        noisey_idxs_.clear();
    }
    inline void post ()  {  }

    inline const result_type &get_result () const  { return (clusters_); }
    inline const order_type &
    get_clusters_idxs () const  { return (clusters_idxs_); }
    inline const vec_t<size_type> &
    get_noisey_idxs () const  { return (noisey_idxs_); }

    DBSCANVisitor(id_t min_mems,
                  double max_dist,
                  distance_func &&f =
                      [](const value_type &x, const value_type &y) -> double  {
                          return ((x - y) * (x - y));
                      })
        : min_mems_(min_mems),
          max_dist_(max_dist),
          dfunc_(std::forward<distance_func>(f))  {   }

private:

    const id_t          min_mems_;
    const double        max_dist_;
    distance_func       dfunc_;
    result_type         clusters_ { };       // Clusters
    order_type          clusters_idxs_ { };  // Clusters indices
    vec_t<size_type>    noisey_idxs_ { };    // Indices of noisey elements
};

// ----------------------------------------------------------------------------

// Runtime complexity is O(I * n^2) where I is number of iterations.
//
// Type T must have arithmetic operators and default constructor well defined
//
template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  MeanShiftVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    template<typename U>
    using vec_t = std::vector<U, typename allocator_declare<U, A>::type>;

    using inner_ptr_view_t = VectorConstPtrView<value_type, A>;
    using result_type = vec_t<inner_ptr_view_t>;
    using inner_order_t =
        std::vector<size_type, typename allocator_declare<size_type, A>::type>;
    using order_type = std::vector<inner_order_t>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    inline static double uniform_kernel_(double d)  {

        return (d <= 1.0 ? 1.0 : 0.0);
    }

    inline static double triangular_kernel_(double d)  {

        return (d <= 1.0 ? 1.0 - std::fabs(d) : 0.0);
    }

    inline static double parabolic_kernel_(double d)  {

        return (d <= 1.0 ? 1.0 - d * d : 0.0);
    }

    inline static double biweight_kernel_(double d)  {

        const auto  x = 1.0 - d * d;

        return (d <= 1.0 ? x * x : 0.0);
    }

    inline static double triweight_kernel_(double d)  {

        const auto  x = 1.0 - d * d;

        return (d <= 1.0 ? x * x * x : 0.0);
    }

    inline static double tricube_kernel_(double d)  {

        const auto  x = 1.0 - d * d * d;

        return (d <= 1.0 ? x * x * x : 0.0);
    }

    inline static double gaussian_kernel_(double d)  {

        return (std::exp(-0.5 * d * d));
    }

    inline static double cosin_kernel_(double d)  {

        return (d <= 1.0 ? std::cos(M_PI_2 * d) : 0.0);
    }

    inline static double logistic_kernel_(double d)  {

        return (1.0 / (2.0 + std::exp(d) + std::exp(-d)));
    }

    inline static double sigmoid_kernel_(double d)  {

        return (1.0 / (std::exp(d) + std::exp(-d)));
    }

    inline static double silverman_kernel_(double d)  {

        const auto  x = M_SQRT1_2 * std::abs(d);

        return (std::exp(-x) * std::sin(x + M_PI_4));
    }

    template<typename H>
    inline void shift_(const H &column_begin,
                       size_type index,
                       const value_type &val,
                       vec_t<value_type> &shifted,
                       vec_t<bool> &shifting)  {

        if (d_func_(val, *(column_begin + index)) <= max_dist_)
            shifting[index] = false;
        else
            shifted[index] = val;
    }

    template<typename H>
    inline void
    build_cluster_(const H &column_begin,
                   size_type col_s,
                   vec_t<value_type> &shifted)  {

        vec_t<value_type>   centriods;

        // Shifted points with distance <= max_dist_ go in the same cluster
        //
        centriods.reserve(32);
        clusters_.reserve(32);
        clusters_idxs_.reserve(32);
        for (size_type i = 0; i < shifted.size(); ++i)  {
            const auto  &shifted_val = shifted[i];
            auto        cbegin = clusters_.begin();
            auto        cend = clusters_.end();
            auto        ibegin = clusters_idxs_.begin();
            size_type   cnt_idx { 0 };

            while (cbegin != cend)  {
                if (d_func_(centriods[cnt_idx], shifted_val) <= max_dist_)  {
                    // The point belongs to a cluster already created
                    //
                    cbegin->push_back(&(*(column_begin + i)));
                    ibegin->push_back(i);
                    break;
                }
                ++cbegin;
                ++ibegin;
                ++cnt_idx;
            }
            if (cbegin == cend)  {  // create a new cluster
                clusters_.push_back(inner_ptr_view_t { });
                clusters_idxs_.push_back(inner_order_t { });
                clusters_.back().reserve(
                    std::max(size_type(8), col_s / size_type(32)));
                clusters_idxs_.back().reserve(
                    std::max(size_type(8), col_s / size_type(32)));
                clusters_.back().push_back(&(*(column_begin + i)));
                clusters_idxs_.back().push_back(i);
                centriods.push_back(shifted_val);
            }
        }
    }

public:

    template<typename IV, typename H>
    inline void
    operator() (const IV &idx_begin, const IV &idx_end,
                const H &column_begin, const H &column_end)  {

        const size_type     col_s =
            std::min(std::distance(idx_begin, idx_end),
                     std::distance(column_begin, column_end));
        auto                k_func =
            (kernel_ == mean_shift_kernel::uniform) ? &uniform_kernel_
            : (kernel_ == mean_shift_kernel::triangular) ? &triangular_kernel_
            : (kernel_ == mean_shift_kernel::parabolic) ? &parabolic_kernel_
            : (kernel_ == mean_shift_kernel::biweight) ? &biweight_kernel_
            : (kernel_ == mean_shift_kernel::triweight) ? &triweight_kernel_
            : (kernel_ == mean_shift_kernel::tricube) ? &tricube_kernel_
            : (kernel_ == mean_shift_kernel::gaussian) ? &gaussian_kernel_
            : (kernel_ == mean_shift_kernel::cosin) ? &cosin_kernel_
            : (kernel_ == mean_shift_kernel::logistic) ? &logistic_kernel_
            : (kernel_ == mean_shift_kernel::sigmoid) ? &sigmoid_kernel_
            : &silverman_kernel_;
        vec_t<value_type>   shifted (column_begin, column_end);
        vec_t<bool>         shifting (col_s, true);
        size_type           iterations { 0 };
        const double        radius { kband_ * 3.0 };
        const double        dbl_sq_bw { 2.0 * kband_ * kband_ };

        while (iterations++ < max_iter_ &&
               std::any_of(shifting.begin(), shifting.end(),
                           [](bool v) -> bool { return (v); }))  {
            for (size_type i = 0; i < col_s; ++i)  {
                if (! shifting[i])  continue;

                value_type          new_val { };
                const value_type    &val_to_shift { shifted[i] };
                double              total_w { 0 };

                for (size_type j = 0; j < col_s; ++j)  {
                    const value_type    &this_val = *(column_begin + j);
                    const double        dist = d_func_(val_to_shift, this_val);

                    if (dist <= radius)  {
                        const double    weight = k_func(dist) / dbl_sq_bw;

                        new_val = new_val + this_val * weight;
                        total_w += weight;
                    }
                }

                // The new position of value is the weighted average of
                // its neighbors
                //
                new_val = new_val / total_w;
                shift_(column_begin, i, new_val, shifted, shifting);
            }
        }

        build_cluster_(column_begin, col_s, shifted);
    }

    inline void pre ()  { clusters_.clear(); clusters_idxs_.clear(); }
    inline void post ()  {  }

    inline const result_type &get_result () const  { return (clusters_); }
    inline const order_type &
    get_clusters_idxs () const  { return (clusters_idxs_); }

    MeanShiftVisitor(
        double kernel_bandwidth,
        double max_dist,
        mean_shift_kernel kernel = mean_shift_kernel::gaussian,
        distance_func &&f =
            [](const value_type &x, const value_type &y) -> double  {
                return ((x - y) * (x - y));
            },
        size_type max_iteration = 50)
        : kband_(kernel_bandwidth),
          kernel_(kernel),
          max_iter_(max_iteration),
          max_dist_(max_dist),
          d_func_(std::forward<distance_func>(f))  {  }

private:

    const double            kband_; // Kernel is fancy name for distance weight
    const mean_shift_kernel kernel_;
    const size_type         max_iter_;
    const double            max_dist_;
    distance_func           d_func_;
    result_type             clusters_ { };       // Clusters
    order_type              clusters_idxs_ { };  // Clusters indices
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

    template <typename K, typename H>
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
                            for (size_type i = begin; i < end; ++i)  {
                                if constexpr (is_complex<T>::value)
                                    result[i] = *(column_begin + i);
                                else
                                    result[i] =
                                        std::complex<T>(*(column_begin + i), 0);
                            }
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

/*
template<std::signed_integral T, typename I = unsigned long, std::size_t A = 0>
struct  NumTheoreticTransVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES_2

public:

    // Inverse = false — Turns a vector of coefficients into its NTT form
    //                   (evaluation at roots of unity).
    // Inverse = true — Turns back from NTT domain to original coefficients.
    //
    //
    // Vectors will be resized to the smallest power of 2 equal or larger than
    // vector size. They will be appended with zeros.
    //
    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {


    }

    // Multiply two polynomials
    // {1, 2, 3} represents 1 + 2x + 3x^2
    // {4, 5, 6} represents 4 + 5x + 6x^2
    //
    // Vectors will be resized to the smallest power of 2 equal or larger than
    // vector size. They will be appended with zeros.
    //
    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column1_begin, const H &column1_end,
                const H &column2_begin, const H &column2_end)  {


    }

    inline void pre ()  {  }
    inline void post ()  {  }

    DEFINE_RESULT

    explicit
    NumTheoreticTransVisitor(bool inverse = false)
        : inverse_(inverse),
          thread_level_(ThreadGranularity::get_thread_level())  {   }

private:

    const bool      inverse_;
    const long      thread_level_;
    result_type     result_ {  };
};

template<std::signed_integral T, typename I = unsigned long, std::size_t A = 0>
using ntt_v = NumTheoreticTransVisitor<T, I, A>;
*/

// ----------------------------------------------------------------------------

template<arithmetic T, typename I = unsigned long, std::size_t A = 0>
struct  EntropyVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ == 0)  return;

        SimpleRollAdopter<SumVisitor<T, I>, T, I, A>  sum_v(SumVisitor<T, I>(),
                                                            roll_count_);

        sum_v.pre();
        sum_v (idx_begin, idx_end, column_begin, column_end);
        sum_v.post();

        result_type         result = std::move(sum_v.get_result());
        const value_type    log_log_base = std::log(log_base_);

        if (result.size() >= ThreadPool::MUL_THR_THHOLD &&
            ThreadGranularity::get_thread_level() > 2)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(
                        size_type(0),
                        result.size(),
                        [&column_begin, &result, log_log_base]
                        (auto begin, auto end) -> void  {
                            for (size_type i = begin; i < end; ++i)  {
                                value_type          &r = result[i];
                                const value_type    val =
                                    *(column_begin + i) / r;

                                r = -val * (std::log(val) / log_log_base);
                            }
                        });

                for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(column_begin, column_end,
                           result.begin(),
                           result.begin(),
                           [log_log_base](auto c, auto r) -> value_type  {
                               const value_type    val = c / r;

                               return (-val * (std::log(val) / log_log_base));
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

    template <typename K, typename H>
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

    template <typename H>
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
    template <typename H>
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
    template <typename H>
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
    template <typename H>
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
    template <typename H>
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
    template <typename H>
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
    template <typename H>
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

    template <typename K, typename H>
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

    template <typename K, typename H>
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

    template <typename K, typename H>
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

    template <typename K, typename H>
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

    template <typename K, typename H>
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

// ----------------------------------------------------------------------------

template<std::size_t K,
         typename T, typename I = unsigned long, std::size_t A = 0>
struct  SpectralClusteringVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES

    using similarity_func =
        std::function<double(const value_type &x,
                             const value_type &y,
                             double sigma)>;
    using seed_t = std::random_device::result_type;
    using cluster_type = std::array<VectorConstPtrView<value_type, A>, K>;
    using order_type =
        std::array<std::vector<
                       size_type,
                       typename allocator_declare<size_type, A>::type>, K>;

private:

    using sym_mat_t = Matrix<T, matrix_orient::row_major, true>;
    using mat_t = Matrix<T, matrix_orient::row_major, false>;
    template<typename U>
    using vec_t = std::vector<U, typename allocator_declare<U, A>::type>;

    const size_type iter_num_;
    const seed_t    seed_;
    const double    sigma_;
    similarity_func sfunc_;
    cluster_type    clusters_ { };       // K Clusters
    order_type      clusters_idxs_ { };  // K Clusters indices

    template<typename H>
    inline sym_mat_t
    calc_similarity_(const H &column_begin, size_type col_s)  {

        sym_mat_t   sim_mat(col_s, col_s);

        for (long r = 0; r < sim_mat.rows(); ++r)
            for (long c = r; c < sim_mat.cols(); ++c)
                sim_mat(r, c) =
                    sfunc_(*(column_begin + c), *(column_begin + r), sigma_);

        return (sim_mat);
    }

    inline vec_t<T>
    calc_degree_(const sym_mat_t &sim_mat)  {

        vec_t<T>    deg_mat(sim_mat.rows());

        for (long r = 0; r < sim_mat.rows(); ++r)  {
            value_type  sum { 0 };

            for (long c = 0; c < sim_mat.cols(); ++c)
                sum += sim_mat(r, c);
            deg_mat[r] = sum;
            // deg_mat[r] = T(1) / std::sqrt(sum); // needs I - D * W * D
        }

        return (deg_mat);
    }

    inline mat_t
    calc_laplacian_(const vec_t<T> &deg_mat, const sym_mat_t &sim_mat)  {

        mat_t   lap_mat(sim_mat.rows(), sim_mat.cols());

        for (long r = 0; r < sim_mat.rows(); ++r)
            for (long c = 0; c < sim_mat.cols(); ++c)
                if (r == c)
                    lap_mat(r, r) = deg_mat[r] - sim_mat(r, r);
                else
                    lap_mat(r, c) = -sim_mat(r, c);

        return (lap_mat);
    }

    inline double
    distance_func_(const mat_t &x, const mat_t &y,
                   long xr, long yr, long cols)  {

        value_type  val { 0 };

        for (long c = 0; c < cols; ++c) {
            const value_type    diff = x(xr, c) - y(yr, c);

            val += diff * diff;
        }
        return (val);
    }

    inline vec_t<long>
    do_kmeans_(const mat_t &eigenvecs)  {

        const long      rows = eigenvecs.rows();  // Samples
        vec_t<long>     cluster_idxs (rows, -1L);
        constexpr long  k = long(K);

        std::random_device                  rd;
        std::mt19937                        gen (
            (seed_ != seed_t(-1)) ? seed_ : rd());
        std::uniform_int_distribution<long> rd_gen (0, rows - 1);

        // Copy the top k rows of eigen vector.
        //
        mat_t   means { k, k };

        for (long r = 0; r < k; ++r)  {
            const auto rr = rd_gen(gen);

            for (long c = 0; c < k; ++c)
                means(r, c) = eigenvecs(rr, c);
        }

        for (size_type iter = 0; iter < iter_num_; ++iter)  {
             // Assign cluster_idxs based on closest means
             //
             for (long r = 0; r < rows; ++r) {
                 double best_distance = std::numeric_limits<double>::max();

                 for (long rr = 0; rr < k; ++rr) {
                     const double   distance =
                         distance_func_(eigenvecs, means, r, rr, k);

                     if (distance < best_distance) {
                         best_distance = distance;
                         cluster_idxs[r] = rr;
                     }
                 }
             }

             // Update means
             //
             mat_t          new_means { k, k };
             vec_t<long>    counts (k, 0L);

             for (long r = 0; r < rows; ++r) {
                 for (long c = 0; c < k; ++c)
                     new_means(cluster_idxs[r], c) += eigenvecs(r, c);
                 counts[cluster_idxs[r]]++;
             }

             for (int r = 0; r < k; ++r) {
                 if (counts[r] > 0)  {
                     for (long c = 0; c < k; ++c)
                         new_means(r, c) /= T(counts[r]);
                 }
                 else  { // Reinitialize centroid if no points assigned
                     const auto rr = rd_gen(gen);

                     for (long c = 0; c < k; ++c)
                         new_means(r, c) = eigenvecs(rr, c);
                 }
             }
             if ((means - new_means).norm() <= 0.0000001)  break;

             means = new_means;
        }

        return (cluster_idxs);
    }

public:

    template<typename IV, typename H>
    inline void
    operator() (const IV &idx_begin, const IV &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s <= K)
           throw DataFrameError("SpectralClusteringVisitor: "
                                "Data size must be bigger than K");
#endif // HMDF_SANITY_EXCEPTIONS

        mat_t   eigenvecs;

        {
            const auto  sim_mat = calc_similarity_(column_begin, col_s);
            const auto  deg_mat = calc_degree_(sim_mat);
            const auto  lap_mat = calc_laplacian_(deg_mat, sim_mat);
            mat_t       eigenvals;

            lap_mat.eigen_space(eigenvals, eigenvecs, true);
        }  // Getting rid of the big things we don't need anymore

        const auto  cluster_idxs = do_kmeans_(eigenvecs);

        // Update the accessible data
        //
        for (size_type i = 0; i < K; ++i)  {
            const auto  val  = col_s / K + 10;

            clusters_[i].reserve(val);
            clusters_idxs_[i].reserve(val);
        }
        for (size_type i = 0; i < cluster_idxs.size(); ++i)  {
            clusters_[cluster_idxs[i]].push_back(&(*(column_begin + i)));
            clusters_idxs_[cluster_idxs[i]].push_back(i);
        }
    }

    inline void pre()  {

        for (auto &iter : clusters_) iter.clear();
        for (auto &iter : clusters_idxs_) iter.clear();
    }
    inline void post()  {  }
    inline const cluster_type &get_result() const  { return (clusters_); }
    inline cluster_type &get_result()  { return (clusters_); }
    inline const order_type &
    get_clusters_idxs() const  { return (clusters_idxs_); }

    SpectralClusteringVisitor(
        size_type num_of_iter,
        double sigma,
        seed_t seed = seed_t(-1),
        similarity_func sf =
            [](const value_type &x,
               const value_type &y,
               double sigma) -> double  {
                return (std::exp(-((x - y) * (x - y)) / (2 * sigma * sigma)));
            })
        : iter_num_(num_of_iter),
          seed_(seed),
          sigma_(sigma),
          sfunc_(sf)  {  }
};

template<std::size_t K, typename T,
         typename I = unsigned long, std::size_t A = 0>
using spect_v = SpectralClusteringVisitor<K, T, I, A>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  SeasonalPeriodVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type = T;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        const size_type         col_s = std::distance(column_begin, column_end);
        std::vector<value_type> data (column_begin, column_end);

        if (params_.detrend)  {  // Take trend out
            std::vector<value_type> xvals (col_s);
            value_type              xvalue { 0 };

            for (auto &val : xvals)  {
                val = xvalue;
                xvalue += T(params_.sampling_rate);
            }

            LowessVisitor<T, I> l_v {
                params_.num_loops,
                params_.frac,
                params_.delta, //  * value_type(col_s),
                true
            };

            l_v.pre();
            l_v (idx_begin, idx_end,
                 data.begin(), data.end(), xvals.begin(), xvals.end());
            l_v.post();

            auto    lbd =
                [&data, &l_v = std::as_const(l_v)]
                (auto begin, auto end) -> void  {

                for (size_type i { begin }; i < end; ++i)
                    data[i] -= l_v.get_result()[i];
            };

            if (col_s >= ThreadPool::MUL_THR_THHOLD &&
                ThreadGranularity::get_thread_level() > 2)  {
                auto    futures =
                    ThreadGranularity::thr_pool_.parallel_loop(size_type(0),
                                                               data.size(),
                                                               std::move(lbd));

                for (auto &fut : futures)  fut.get();
            }
            else lbd (size_type(0), data.size());
        }

        if (params_.de_serial_corr)  {  // Take serial correlation out
            value_type  prev_val { data[0] };

            for (auto &datum : data)  {
                const value_type    tmp = datum;

                datum -= prev_val;
                prev_val = tmp;
            }
            data[0] = data[1];
        }

        fft_v<T, I> fft;

        fft.pre();
        fft (idx_begin, idx_end, data.begin(), data.end());
        fft.post();

        const auto  &mags = fft.get_magnitude();

        for (size_type i { 0 }; const auto &mag : mags)  {
            const double    val = std::fabs(mag);

            if (val > max_mag_)  {
                max_mag_ = val;
                dom_idx_ = i;
            }
            i += 1;
        }
        dom_freq_ = T(dom_idx_) * T(params_.sampling_rate) / T(mags.size());
        result_ = T(1.0) / dom_freq_;
    }

    inline void pre ()  {

        result_ = max_mag_ = dom_freq_ = 0;
        dom_idx_ = 0;
    }
    inline void post ()  {  }

    [[nodiscard]] inline result_type get_result () const  { return (result_); }
    [[nodiscard]] inline result_type get_period () const  { return (result_); }
    [[nodiscard]] inline result_type
    get_max_magnitude () const  { return (max_mag_); }
    [[nodiscard]] inline result_type
    get_dominant_frequency () const  { return (dom_freq_); }
    [[nodiscard]] inline size_type
    get_dominant_index () const  { return (dom_idx_); }

    explicit
    SeasonalPeriodVisitor(const SeasonalityParams<T> params = { })
        : params_(params)  {   }

private:

    const SeasonalityParams<T>  params_;
    result_type                 result_ { 0 };    // period
    result_type                 max_mag_ { 0 };   // Max magnitude
    result_type                 dom_freq_ { 0 };  // Dominant frequency
    size_type                   dom_idx_ { 0 };   // Dominant index
};

template<typename T, typename I = unsigned long>
using ssp_v = SeasonalPeriodVisitor<T, I>;

// ----------------------------------------------------------------------------

// Dynamic Time Warping (DTW) is an algorithm for measuring similarity between
// two temporal sequences, which may vary in speed.
//
template<typename T, typename I = unsigned long>
struct  DynamicTimeWarpVisitor  {

private:

    using matrix_t = Matrix<double, matrix_orient::row_major>;

    template<typename H>
    inline void calc_(const H &x_begin, const H &x_end,
                      const H &y_begin, const H &y_end)  {

        const long  x_col_s = long(std::distance(x_begin, x_end));
        const long  y_col_s = long(std::distance(y_begin, y_end));
        matrix_t    mt (x_col_s + 1, y_col_s + 1,
                        std::numeric_limits<double>::infinity());

        mt(0, 0) = 0.0;
        for (long i { 1 }; i <= x_col_s; ++i)  {
            const value_type    &xval = *(x_begin + (i - 1));

            for (long j { 1 }; j <= y_col_s; ++j)  {
                const double    cost = dfunc_(xval, *(y_begin + (j - 1)));

                mt(i, j) =
                    cost +
                    std::min({ mt(i - 1, j), mt(i, j - 1), mt(i - 1, j - 1) });
            }
        }

        result_ = mt(x_col_s, y_col_s);
    }

public:

    DEFINE_VISIT_BASIC_TYPES

    using result_type = double;
    using distance_func = std::function<double(const T &x, const T &y)>;

    template<typename IV, typename H>
    inline void
    operator() (const IV &idx_begin, const IV &idx_end,
                const H &x_begin, const H &x_end,
                const H &y_begin, const H &y_end)  {

        if (nt_ > normalization_type::none)  {
            NormalizeVisitor<T, I>  x_norm_v { nt_ };
            NormalizeVisitor<T, I>  y_norm_v { nt_ };

            x_norm_v.pre();
            x_norm_v(idx_begin, idx_end, x_begin, x_end);
            x_norm_v.post();

            y_norm_v.pre();
            y_norm_v(idx_begin, idx_end, y_begin, y_end);
            y_norm_v.post();

            calc_(x_norm_v.get_result().begin(), x_norm_v.get_result().end(),
                  y_norm_v.get_result().begin(), y_norm_v.get_result().end());
        }
        else  calc_(x_begin, x_end, y_begin, y_end);
    }

    inline void pre ()  { result_ = 0.0; }
    inline void post ()  {  }

    inline result_type get_result () const  { return (result_); }

    explicit
    DynamicTimeWarpVisitor(
        normalization_type norm_type = normalization_type::none,
        distance_func &&f =
            [](const value_type &x, const value_type &y) -> double  {
                return (std::fabs(x - y));
            })
        : nt_(norm_type), dfunc_(std::forward<distance_func>(f))  {   }

private:

    const normalization_type    nt_;
    distance_func               dfunc_;
    result_type                 result_ { 0 };
};

template<typename T, typename I = unsigned long>
using dtw_v = DynamicTimeWarpVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  AnomalyDetectByFFTVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        std::vector<size_type, typename allocator_declare<size_type, A>::type>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (freq_num_ >= col_s)
           throw DataFrameError("AnomalyDetectByFFTVisitor: "
                                "freq num must be < column length");
#endif // HMDF_SANITY_EXCEPTIONS

        NormalizeVisitor<T, I, A>   norm { nt_ };

        if (nt_ > normalization_type::none)  {
            norm.pre();
            norm(idx_begin, idx_end, column_begin, column_end);
            norm.post();
        }

        fft_v<T, I, A>  fft;

        fft.pre();
        if (nt_ > normalization_type::none)
            fft(idx_begin, idx_end,
                norm.get_result().begin(), norm.get_result().end());
        else
            fft(idx_begin, idx_end, column_begin, column_end);
        fft.post();

        auto    fft_res = std::move(fft.get_result());

        for (size_type i { freq_num_ }; i < col_s; ++i)
            fft_res[i] = T(0);

        fft_v<std::complex<T>, I, A>    ifft(true); // Inverse FFT

        ifft.pre();
        ifft(idx_begin, idx_end, fft_res.begin(), fft_res.end());
        ifft.post();

        result_.reserve(32);
        for (size_type i { 0 }; i < col_s; ++i)
            if (std::abs(*(column_begin + i) - ifft.get_result()[i]) > ath_)
                result_.push_back(i);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    AnomalyDetectByFFTVisitor(size_type freq_num,
                              value_type anomaly_threshold = T(1),
                              normalization_type norm_type =
                                  normalization_type::none)
        : ath_(anomaly_threshold), freq_num_(freq_num), nt_(norm_type)  {   }

private:

    const value_type            ath_;
    const size_type             freq_num_;
    const normalization_type    nt_;
    result_type                 result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using and_fft_v = AnomalyDetectByFFTVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  HampelFilterVisitor {

public:

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        std::vector<size_type, typename allocator_declare<size_type, A>::type>;

private:

    template<typename K, typename H, typename AR>
    inline void
    hampel_(K idx_begin, K idx_end, H column_begin, H column_end, AR &&aggr)  {

        GET_COL_SIZE

        aggr.pre();
        aggr(idx_begin, idx_end, column_begin, column_end);
        aggr.post();

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        vec_t       diff(col_s);

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0),
                    col_s,
                    [&aggr = std::as_const(aggr.get_result()), &diff,
                     &column_begin]
                    (auto begin, auto end) -> void  {
                        for (size_type i = begin; i < end; ++i)
                            diff[i] = aggr[i] - *(column_begin + i);
                    });

            for (auto &fut : futures)  fut.get();
        }
        else  {
            std::transform(aggr.get_result().begin(), aggr.get_result().end(),
                           column_begin,
                           diff.begin(),
                           [](const T &agr, const T &col) -> T  {
                               return (std::fabs(agr - col));
                           });
        }

        // Calculate median absolute deviation
        //
        aggr.pre();
        aggr(idx_begin, idx_end, diff.begin(), diff.end());
        aggr.post();

        const value_type    factor = num_of_std_ * unbiased_factor_;
        const auto          &aggr_res = aggr.get_result();

        result_.reserve(32);
        for (size_type i = 0; i < col_s; ++i)  {
            if (diff[i] > (aggr_res[i] * factor))
                result_.push_back(i);
        }
    }

public:

    template<typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        if (type_ == hampel_type::median)
            hampel_(idx_begin, idx_end, column_begin, column_end,
                    SimpleRollAdopter<MedianVisitor<T, I>, T, I>
                        (MedianVisitor<T, I> { }, window_size_));
        else if (type_ == hampel_type::mean)
            hampel_(idx_begin, idx_end, column_begin, column_end,
                    SimpleRollAdopter<MeanVisitor<T, I>, T, I>
                        (MeanVisitor<T, I> { true }, window_size_));
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    HampelFilterVisitor(size_type window_size,
                        hampel_type ht = hampel_type::median,
                        value_type num_of_std = 3)
        : window_size_(window_size),
          type_(ht),
          num_of_std_(num_of_std)  {  }

private:

    using vec_t = std::vector<T, typename allocator_declare<T, A>::type>;

    static constexpr value_type unbiased_factor_ { value_type(1.4826) };
    const size_type             window_size_;
    const hampel_type           type_;
    const value_type            num_of_std_;
    result_type                 result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using hamf_v = HampelFilterVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  AnomalyDetectByIQRVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        std::vector<size_type, typename allocator_declare<size_type, A>::type>;

    template <typename K, typename H>
    inline void
    operator() (const K &, const K &,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s < 20)
           throw DataFrameError("AnomalyDetectByIQRVisitor: "
                                "Time-series is too short");
#endif // HMDF_SANITY_EXCEPTIONS

        vec_t   data(column_begin, column_end);

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        if (thread_level > 2)
            ThreadGranularity::thr_pool_.parallel_sort(
                data.begin(), data.end());
        else
            std::sort(data.begin(), data.end());

        const size_type     mid = col_s / 2;
        const value_type    q1 { median_(data.begin(), data.begin() + mid) };
        const value_type    q3 {
            (col_s & size_type(0x01))
                ? median_(data.begin() + (mid + 1), data.end())  // Odd
                : median_(data.begin() + mid, data.end())        // Even
        };
        const value_type    iqr { q3 - q1 };
        const value_type    low_bound { q1 - low_fence_ * iqr };
        const value_type    high_bound { q3 + high_fence_ * iqr };

        result_.reserve(32);
        for (size_type i { 0 }; i < col_s; ++i)  {
            const value_type    &val = *(column_begin + i);

            if (val < low_bound || val > high_bound) [[unlikely]]
                result_.push_back(i);
        }
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    AnomalyDetectByIQRVisitor(value_type high_fence = T(1.5),
                              value_type low_fence = T(1.5))
        : high_fence_(high_fence), low_fence_(low_fence)  {   }

private:

    using vec_t = std::vector<T, typename allocator_declare<T, A>::type>;

    template <typename H>
    static inline T
    median_(const H &data_begin, const H &data_end)  {

        const size_type     s = std::distance(data_begin, data_end);
        const size_type     mid = s / 2;
        const value_type    &mid_val = *(data_begin + mid);

        if (s & size_type(0x01))  // Odd
            return (mid_val);
        else                      // Even
            return ((*(data_begin + (mid - 1)) + mid_val) / T(2));
    }

    const value_type    high_fence_;
    const value_type    low_fence_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using and_iqr_v = AnomalyDetectByIQRVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  AnomalyDetectByZScoreVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        std::vector<size_type, typename allocator_declare<size_type, A>::type>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s < 10)
           throw DataFrameError("AnomalyDetectByZScoreVisitor: "
                                "Time-series is too short");
#endif // HMDF_SANITY_EXCEPTIONS

        MeanVisitor<T, I>   mvisit { true };
        StdVisitor<T, I>    svisit;
        const auto          thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();

        mvisit.pre();
        svisit.pre();
        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                      false,
                      [&svisit,
                       &idx_begin, &idx_end,
                       &column_begin, &column_end]() -> void  {
                          svisit(idx_begin, idx_end, column_begin, column_end);
                      });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                      false,
                      [&mvisit,
                       &idx_begin, &idx_end,
                       &column_begin, &column_end]() -> void  {
                          mvisit(idx_begin, idx_end, column_begin, column_end);
                      });

            fut1.get();
            fut2.get();
        }
        else  {
            mvisit(idx_begin, idx_end, column_begin, column_end);
            svisit(idx_begin, idx_end, column_begin, column_end);
        }
        mvisit.post();
        svisit.post();

        const value_type    m = mvisit.get_result();
        const value_type    s = svisit.get_result();

        result_.reserve(32);
        for (size_type i { 0 }; i < col_s; ++i)
            if (std::abs((*(column_begin + i) - m) / s) > threshold_)
                result_.push_back(i);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    AnomalyDetectByZScoreVisitor(value_type threshold)
        : threshold_(threshold)  {   }

private:

    result_type         result_ { };
    const value_type    threshold_;
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using and_zscr_v = AnomalyDetectByZScoreVisitor<T, I, A>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, std::size_t A = 0>
struct  AnomalyDetectByLOFVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    using result_type =
        std::vector<size_type, typename allocator_declare<size_type, A>::type>;
    using distance_func = std::function<double(const T &x, const T &y)>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE2

#ifdef HMDF_SANITY_EXCEPTIONS
        if (col_s < 20 || k_ >= (col_s - 1))
            throw DataFrameError("AnomalyDetectByLOFVisitor: "
                                 "Time-series is too short or k is too large");
#endif // HMDF_SANITY_EXCEPTIONS

        NormalizeVisitor<T, I, A>   norm { nt_ };

        if (nt_ > normalization_type::none)  {
            norm.pre();
            norm(idx_begin, idx_end, column_begin, column_end);
            norm.post();
        }

        pvec_t      dists(col_s);
        knn_mat_t   neighbors { long(col_s), long(k_) };
        rch_mat_t   reach_dists { long(col_s), long(k_) };
        dist_vec_t  lrd(col_s, 0);  // Local Reachability Density

        for (size_type i { 0 }; i < col_s; ++i)  {
            if (nt_ > normalization_type::none)
                get_knn_(norm.get_result().begin(), col_s, i, neighbors, dists);
            else
                get_knn_(column_begin, col_s, i, neighbors, dists);

            double          sum_reach_dist { 0 };

            for (long j { 0 }; j < neighbors.cols(); ++j)  {
                const double    reach_d =
                    std::max(neighbors(i, j).first,
                             dfunc_(*(column_begin + j), *(column_begin + i)));

                reach_dists(long(i), j) = reach_d;
                sum_reach_dist += reach_d;
            }

            lrd[i] = double(k_) / sum_reach_dist;
        }

        result_.reserve(32);
        for (size_type i { 0 }; i < col_s; ++i)  {
            double  sum_ratio { 0 };

            for (long j { 0 }; j < neighbors.cols(); ++j)
                sum_ratio += lrd[neighbors(i, j).second] / lrd[i];

            // std::cout << sum_ratio / double(k_) << "\n";
            if ((sum_ratio / double(k_)) > threshold_)
                result_.push_back(i);
        }
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    AnomalyDetectByLOFVisitor(
        size_type k,
        double threshold,
        normalization_type norm_type = normalization_type::none,
        distance_func &&f =
            [](const value_type &x, const value_type &y) -> double  {
                return (std::fabs(x - y));
            })
        : k_(k),
          threshold_(threshold),
          nt_(norm_type),
          dfunc_(std::forward<distance_func>(f))  {   }

private:

    using dist_vec_t = std::vector<double>;
    using pvec_t = std::vector<std::pair<double, size_type>> ;
    using knn_mat_t =
        Matrix<std::pair<double, size_type>, matrix_orient::row_major>;
    using rch_mat_t = Matrix<double, matrix_orient::row_major>;

    template <typename H>
    inline void
    get_knn_(const H &column_begin,
             size_type col_s, size_type index,
             knn_mat_t &neighbors, pvec_t &dists) const  {

        const auto  thread_level = (col_s < ThreadPool::MUL_THR_THHOLD)
            ? 0L : ThreadGranularity::get_thread_level();
        const auto  &idx_val = *(column_begin + index);
        auto        lbd =
            [&dists, this,
             &column_begin = std::as_const(column_begin),
             &idx_val = std::as_const(idx_val)]
            (auto begin, auto end) -> void  {
                for (size_type i { begin }; i < end; ++i)
                    dists[i] = { dfunc_(idx_val, *(column_begin + i)), i };
            };

        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0), col_s, std::move(lbd));

            for (auto &fut : futures)  fut.get();
            ThreadGranularity::thr_pool_.parallel_sort(
                dists.begin(), dists.end(),
                [] (const auto &lhs, const auto &rhs)  {
                    return (lhs.first < rhs.first);
                });
        }
        else  {
            lbd(size_type(0), col_s);
            std::sort(dists.begin(), dists.end(),
                      [] (const auto &lhs, const auto &rhs)  {
                          return (lhs.first < rhs.first);
                      });
        }

        // Skip the first element which is 0 (distance of index with itself)
        //
        for (size_type i { 1 }; i <= k_; ++i)
            neighbors(long(index), long(i - 1)) = dists[i];
        return;
    }

    const size_type             k_;
    const double                threshold_;
    const normalization_type    nt_;
    distance_func               dfunc_;
    result_type                 result_ { };
};

template<typename T, typename I = unsigned long, std::size_t A = 0>
using and_lof_v = AnomalyDetectByLOFVisitor<T, I, A>;

// ----------------------------------------------------------------------------

// Mutual Information
//
template<arithmetic T, typename I = unsigned long>
struct  MutualInfoVisitor  {

    DEFINE_VISIT_BASIC_TYPES
    using result_type = double;

    template<typename K, typename H>
    inline void
    operator() (const K & /*idx_begin*/, const K & /*idx_end*/,
                const H &column1_begin, const H &column1_end,
                const H &column2_begin, const H &column2_end)  {

        const size_type col_s = std::distance(column1_begin, column1_end);

#ifdef HMDF_SANITY_EXCEPTIONS
        const size_type col2_s = std::distance(column2_begin, column2_end);

        if (col_s != col2_s)
            throw DataFrameError("MutualInfoVisitor: "
                                 "The two variables must have the same "
                                 "number of observations");
#endif // HMDF_SANITY_EXCEPTIONS

        std::unordered_map<value_type, double>              count_x;
        std::unordered_map<value_type, double>              count_y;
        std::unordered_map<
            std::pair<value_type, value_type>, double,
            PairHash_<std::pair<value_type, value_type>>,
            PairEqual_<std::pair<value_type, value_type>>>  count_xy;

        const auto  thread_level = (col_s < (ThreadPool::MUL_THR_THHOLD / 3))
            ? 0L : ThreadGranularity::get_thread_level();

        if (thread_level > 2)  {
            auto    fut1 =
                ThreadGranularity::thr_pool_.dispatch(
                      false,
                      [&column1_begin = std::as_const(column1_begin),
                       col_s, &count_x]() -> void  {
                          for (size_type i { 0 }; i < col_s; ++i)  {
                              const auto    &val = *(column1_begin + i);

                              count_x.insert({ val, 0 }).first->second += 1.0;
                          }
                      });
            auto    fut2 =
                ThreadGranularity::thr_pool_.dispatch(
                      false,
                      [&column2_begin = std::as_const(column2_begin),
                       col_s, &count_y]() -> void  {
                          for (size_type i { 0 }; i < col_s; ++i)  {
                              const auto    &val = *(column2_begin + i);

                              count_y.insert({ val, 0 }).first->second += 1.0;
                          }
                      });
            auto    fut3 =
                ThreadGranularity::thr_pool_.dispatch(
                      false,
                      [&column1_begin = std::as_const(column1_begin),
                       &column2_begin = std::as_const(column2_begin),
                       col_s, &count_xy]() -> void  {
                          for (size_type i { 0 }; i < col_s; ++i)  {
                              const auto  &val1 = *(column1_begin + i);
                              const auto  &val2 = *(column2_begin + i);

                              count_xy.insert(
                                  { { val1, val2 }, 0 }).first->second += 1.0;
                          }
                      });

            fut1.get();
            fut2.get();
            fut3.get();
        }
        else  {
            for (size_type i { 0 }; i < col_s; ++i)  {
                const auto  &val1 = *(column1_begin + i);
                const auto  &val2 = *(column2_begin + i);

                count_x.insert({ val1, 0 }).first->second += 1.0;
                count_y.insert({ val2, 0 }).first->second += 1.0;
                count_xy.insert({ { val1, val2 }, 0 }).first->second += 1.0;
            }
        }

        double  mi { 0 };
        auto    lbd =
            [&column1_begin = std::as_const(column1_begin),
             &column2_begin = std::as_const(column2_begin),
             &count_x = std::as_const(count_x),
             &count_y = std::as_const(count_y),
             &count_xy = std::as_const(count_xy),
             col_s]
            (auto begin, auto end) -> double  {
                double  mi { 0 };

                for (size_type i { begin }; i < end; ++i)  {
                    const auto  &val1 = *(column1_begin + i);
                    const auto  &val2 = *(column2_begin + i);
                    const auto  p_x = count_x.find(val1)->second / col_s;
                    const auto  p_y = count_y.find(val2)->second / col_s;
                    const auto  p_xy =
                        count_xy.find({ val1, val2 })->second / col_s;

                    mi += p_xy * std::log(p_xy / (p_x * p_y));
                }
                return (mi);
            };


        if (thread_level > 2)  {
            auto    futures =
                ThreadGranularity::thr_pool_.parallel_loop(
                    size_type(0), col_s, std::move(lbd));

            for (auto &fut : futures)  mi += fut.get();
        }
        else  {
            mi = lbd(size_type(0), col_s);
        }

        // Convert from nat (natural unit) to bits (base 2)
        //
        result_ = mi / log_log_base_;
    }

    inline void pre()  { result_ = 0; }
    inline void post()  {  }
    inline result_type get_result() const  { return (result_); }

    explicit
    MutualInfoVisitor(double log_base = 2)
        : log_log_base_(std::log(log_base))  {   }

private:

    template<typename M>
    inline static void map_reserve_(M &in_map, size_type in_size)  {

        auto    c { std::move(in_map).extract() };
        auto    key_vec { std::move(c.keys) };
        auto    val_vec { std::move(c.values) };

        key_vec.reserve(in_size);
        val_vec.reserve(in_size);
        in_map.replace(std::move(key_vec), std::move(val_vec));
    }

    template<typename P>
    struct  PairEqual_  {
        inline bool operator()(const P &lhs, const P &rhs) const  {

            return ((lhs.first == rhs.first) && (lhs.second == rhs.second));
        }
    };

    template<typename P>
    struct  PairHash_  {
        inline std::size_t operator()(const P &lhs) const  {

            return (std::hash<typename P::first_type>{}(lhs.first) ^
                    (std::hash<typename P::second_type>{}(lhs.second) << 1));
        }
    };

    result_type     result_ { 0 };
    const double    log_log_base_;
};

template<typename T, typename I = unsigned long>
using mut_i_v = MutualInfoVisitor<T, I>;

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
