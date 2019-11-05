// Hossein Moein
// October 30, 2019
// Copyright (C) 2019-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <DataFrame/DataFrameTypes.h>

#include <cmath>
#include <functional>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<size_t K, typename T, typename I = unsigned long>
struct  KMeansVisitor {

public:

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using result_type = std::array<std::vector<value_type>, K>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    using means_type = std::array<value_type, K>;

    const size_type iter_num_;
    distance_func   dfunc_;
    result_type     clusters_ { };

    inline void calc_clusters_(const std::vector<value_type> &col,
                               const means_type &k_means)  {

        const size_type col_size = col.size();

        for (size_type i = 0; i < K; ++i)  {
            clusters_[i].reserve(col_size / K + 2);
            clusters_[i].push_back(k_means[i]);
        }
 
        for (const auto &citer : col)  {
            double      min_dist = std::numeric_limits<double>::max();
            size_type   min_idx;

            for (size_type i = 0; i < K; ++i)  {
                const double    dist = dfunc_(citer, k_means[i]);

                if (dist < min_dist)  {
                    min_dist = dist;
                    min_idx = i;
                }
            }
            clusters_[min_idx].push_back(citer);
        }
    }

    inline void calc_k_means_(const std::vector<value_type> &col)  {

        const size_type col_size = col.size();
        means_type      k_means { };

        std::random_device                          rd;
        std::mt19937                                gen(rd());
        std::uniform_int_distribution<size_type>    rd_gen(0, col_size - 1);

        // Pick centroids as random points from the col.
        for (auto &k_mean : k_means)
            k_mean = col[rd_gen(gen)];

        std::vector<size_type>  assignments(col_size, 0);

        for (size_type iter = 0; iter < iter_num_; ++iter) {
            // Find assignments.
            for (size_type point = 0; point < col_size; ++point) {
                double      best_distance = std::numeric_limits<double>::max();
                size_type   best_cluster = 0;

                for (size_type cluster = 0; cluster < K; ++cluster) {
                    const double    distance =
                        dfunc_(col[point], k_means[cluster]);

                    if (distance < best_distance) {
                        best_distance = distance;
                        best_cluster = cluster;
                    }
                }
                assignments[point] = best_cluster;
            }

            // Sum up and count points for each cluster.
            means_type              new_means { value_type() };
            std::array<double, K>   counts { 0.0 };

            for (size_type point = 0; point < col_size; ++point) {
                const size_type cluster = assignments[point];

                new_means[cluster] = new_means[cluster] + col[point];
                counts[cluster] += 1.0;
            }

            bool    done = true;

            // Divide sums by counts to get new centroids.
            for (size_type cluster = 0; cluster < K; ++cluster) {
                // Turn 0/0 into 0/1 to avoid zero division.
                const double        count =
                    std::max<double>(1.0, counts[cluster]);
                const value_type    value = new_means[cluster] / count;

                if (::fabs(value - k_means[cluster]) > 0.0000001)  { 
                    done = false;
                    k_means[cluster] = value;
                }
            }

            if (done)  break;
        }

        calc_clusters_(col, k_means);
    }

public:

    inline void
    operator() (const std::vector<index_type> &,
                const std::vector<value_type> &col)  { calc_k_means_(col); }
    inline void pre ()  {  }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (clusters_); }

    KMeansVisitor(
        size_type num_of_iter,
        distance_func f =
            [](const value_type &x, const value_type &y) -> double {
                return ((x - y) * (x - y));
            })
        : iter_num_(num_of_iter), dfunc_(f)  {   }
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
