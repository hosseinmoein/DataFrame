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
#include <DataFrame/Vectors/VectorPtrView.h>

#include <functional>
#include <iterator>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<size_t K, typename T, typename I = unsigned long>
struct  KMeansVisitor  {

public:

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::array<value_type, K>;
    using cluster_type = std::array<VectorPtrView<value_type>, K>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    const size_type iter_num_;
    distance_func   dfunc_;
    result_type     k_means_ { };

    template<typename H>
    inline void calc_k_means_(const H &column_begin, size_type col_size)  {

        std::random_device                          rd;
        std::mt19937                                gen(rd());
        std::uniform_int_distribution<size_type>    rd_gen(0, col_size - 1);

        // Pick centroids as random points from the col.
        for (auto &k_mean : k_means_)
            k_mean = *(column_begin + rd_gen(gen));

        std::vector<size_type>  assignments(col_size, 0);

        for (size_type iter = 0; iter < iter_num_; ++iter) {
            // Find assignments.
            for (size_type point = 0; point < col_size; ++point) {
                double      best_distance = std::numeric_limits<double>::max();
                size_type   best_cluster = 0;

                for (size_type cluster = 0; cluster < K; ++cluster) {
                    const double    distance =
                        dfunc_(*(column_begin + point), k_means_[cluster]);

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

                if (dfunc_(value, k_means_[cluster]) > 0.0000001)  {
                    done = false;
                    k_means_[cluster] = value;
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

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);

        calc_k_means_(column_begin, std::min(idx_size, col_size));
    }

    // Using the calculated means, separate the given column into clusters
    //
    template<typename IV, typename H>
    inline cluster_type
    get_clusters(const IV &idx_begin,
                 const IV &idx_end,
                 const H &column_begin,
                 const H &column_end)  {

        const size_type idx_s = std::distance(idx_begin, idx_end);
        const size_type col_s = std::distance(column_begin, column_end);
        const size_type col_size = std::min(idx_s, col_s);
        cluster_type    clusters;

        for (size_type i = 0; i < K; ++i)  {
            clusters[i].reserve(col_size / K + 2);
            clusters[i].push_back(const_cast<value_type *>(&(k_means_[i])));
        }

        for (size_type j = 0; j < col_size; ++j)  {
            double      min_dist = std::numeric_limits<double>::max();
            size_type   min_idx;

            for (size_type i = 0; i < K; ++i)  {
                const double    dist = dfunc_(*(column_begin + j), k_means_[i]);

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
    inline const result_type &get_result () const  { return (k_means_); }

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

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = VectorPtrView<value_type>;
    using cluster_type = std::vector<VectorPtrView<value_type>>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    const size_type iter_num_;
    distance_func   dfunc_;
    const double    dfactor_;
    result_type     centers_ { };

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
                    if (i == j)  {
                        double  sum = 0.0;

                        for (size_type ii = 0; ii < i; ++ii)
                            sum += std::max(0.0, respon[j * csize + ii]);
                        for(size_type ii = i + 1; ii < csize; ++ii)
                            sum += std::max(0.0, respon[j * csize + ii]);

                        avail[j * csize + i] =
                            (1.0 - dfactor_) * sum +
                            dfactor_ * avail[j * csize + i];
                    }
                    else  {
                        double          sum = 0.0;
                        const size_type max_i_j = std::max(i, j);
                        const size_type min_i_j = std::min(i, j);

                        for (size_type ii = 0; ii < min_i_j; ++ii)
                            sum += std::max(0.0, respon[j * csize + ii]);
                        for (size_type ii = min_i_j + 1; ii < max_i_j; ++ii)
                            sum += std::max(0.0, respon[j * csize + ii]);
                        for (size_type ii = max_i_j + 1; ii < csize; ++ii)
                            sum += std::max(0.0, respon[j * csize + ii]);

                        avail[j * csize + i] =
                            (1.0 - dfactor_) *
                            std::min(0.0, respon[j * csize + j] + sum) +
                            dfactor_ * avail[j * csize + i];
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

        const size_type             idx_size =
            std::distance(idx_begin, idx_end);
        const size_type             col_size =
            std::distance(column_begin, column_end);
        const size_type             csize = std::min(idx_size, col_size);
        const std::vector<double>   simil =
            std::move(get_similarity_(column_begin, csize));
        std::vector<double>         avail;
        std::vector<double>         respon;

        get_avail_and_respon(simil, csize, avail, respon);

        centers_.reserve(std::min(csize / 100, size_type(16)));
        for (size_type i = 0; i < csize; ++i)  {
            if (respon[i * csize + i] + avail[i * csize + i] > 0.0)
                centers_.push_back(
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

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);
        const size_type csize = std::min(idx_size, col_size);
        const size_type centers_size = centers_.size();
        cluster_type    clusters;

        if (centers_size > 0)  {
            clusters.resize(centers_size);
            for (size_type i = 0; i < centers_size; ++i)
                clusters[i].reserve(csize / centers_size);

            for (size_type j = 0; j < csize; ++j)  {
                double      min_dist = std::numeric_limits<double>::max();
                size_type   min_idx;

                for (size_type i = 0; i < centers_size; ++i)  {
                    const double    dist =
                        dfunc_(*(column_begin + j), centers_[i]);

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

    inline void pre ()  {  }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (centers_); }

    AffinityPropVisitor(
        size_type num_of_iter,
        distance_func f =
            [](const value_type &x, const value_type &y) -> double {
                return ((x - y) * (x - y));
            },
        double damping_factor = 0.9)
        : iter_num_(num_of_iter), dfunc_(f), dfactor_(damping_factor)  {   }
};

} // namespace hmdf

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
