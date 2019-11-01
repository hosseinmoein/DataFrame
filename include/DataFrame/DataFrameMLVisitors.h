// Hossein Moein
// October 30, 2019
// Copyright (C) 2019-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <DataFrame/DataFrameTypes.h>

#include <functional>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<size_t K, typename T, typename I = unsigned long>
struct KMeansVisitor {

public:

    using value_type = T;
    using index_type = I;
    using size_type = std::size_t;
    using means_type = std::array<value_type, K>;
    using result_type = std::array<std::vector<value_type>, K>;
    using distance_func =
        std::function<double(const value_type &x, const value_type &y)>;

private:

    const size_type iter_num_;
    distance_func   dfunc_;
    result_type     clusters_ { };
    means_type      k_means_ { };

    // k_means_ must be calculated before calling this function
	//
    inline void calc_clusters_(const std::vector<value_type> &col)  {

    }

    inline void calc_k_means_(const std::vector<value_type> &col)  {

        const size_type col_size = col.size();

        std::random_device                          rd;
        std::mt19937                                gen(rd());
        std::uniform_int_distribution<size_type>    rd_gen(0, col_size - 1);

        // Pick centroids as random points from the col.
        for (auto &cluster : k_means_)
            cluster = col[rd_gen(gen)];

        std::vector<size_type>  assignments(col_size, 0);

        for (size_type iter = 0; iter < iter_num_; ++iter) {
            // Find assignments.
            for (size_type point = 0; point < col_size; ++point) {
                double      best_distance = std::numeric_limits<double>::max();
                size_type   best_cluster = 0;

                for (size_type cluster = 0; cluster < K; ++cluster) {
                    const double    distance =
                        dfunc_(col[point], k_means_[cluster]);

                    if (distance < best_distance) {
                        best_distance = distance;
                        best_cluster = cluster;
                    }
                }
                assignments[point] = best_cluster;
            }

            // Sum up and count points for each cluster.
            means_type                  new_means;
            std::array<size_type, K>    counts { 0 };

            for (size_type point = 0; point < col_size; ++point) {
                const size_type cluster = assignments[point];

                new_means[cluster] += col[point];
                counts[cluster] += 1;
            }

            bool    done = true;
			
            // Divide sums by counts to get new centroids.
            for (size_type cluster = 0; cluster < K; ++cluster) {
                // Turn 0/0 into 0/1 to avoid zero division.
                const double        count =
                    std::max<size_type>(1, counts[cluster]);
                const value_type    value = new_means[cluster] / count;

                if (::fabs(value - k_means_[cluster]) < 0.000001)  { 
                    done = false;
                    k_means_[cluster] = value;
                }
            }

            if (done)  break;
        }
    }

public:

    inline void
    operator() (const std::vector<index_type> &,
                const std::vector<value_type> &col)  {


        if (skip_nan_ && is_nan__(val))  return;

        mean_ += val;
        cnt_ +=1;
    }
    inline void pre ()  { ; }
    inline void post ()  {  }
    inline const means_type &get_k_means () const  { return (k_means_); }
    inline const result_type &get_result () const  { return (clusters_); }

    KMeansVisitor(
        size_type num_of_iterations,
        distance_func f =
            [](const value_type &x, const value_type &y) -> double {
                return ((x - y) * (x - y));
            })
        : iter_num_(num_of_iterations), dfunc_(f)  {   }
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
