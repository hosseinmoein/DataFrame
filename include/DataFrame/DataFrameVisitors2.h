// Hossein Moein
// October 30, 2019
// Copyright (C) 2019-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <DataFrame/DataFrameVisitors.h>

#include <functional>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<size_t K, typename T, typename I = unsigned long>
struct  KMeansVisitor  {

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

    template<typename H>
    inline void calc_clusters_(const H &col,
                               const means_type &k_means,
                               size_type col_size)  {

        for (size_type i = 0; i < K; ++i)  {
            clusters_[i].reserve(col_size / K + 2);
            clusters_[i].push_back(k_means[i]);
        }
 
        for (size_type j = 0; j < col_size; ++j)  {
            double      min_dist = std::numeric_limits<double>::max();
            size_type   min_idx;

            for (size_type i = 0; i < K; ++i)  {
                const double    dist = dfunc_(col[j], k_means[i]);

                if (dist < min_dist)  {
                    min_dist = dist;
                    min_idx = i;
                }
            }
            clusters_[min_idx].push_back(col[j]);
        }
    }

    template<typename H>
    inline void calc_k_means_(const H &col, size_type col_size)  {

        means_type  k_means { };

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

                if (dfunc_(value, k_means[cluster]) > 0.0000001)  { 
                    done = false;
                    k_means[cluster] = value;
                }
            }

            if (done)  break;
        }

        calc_clusters_(col, k_means, col_size);
    }

public:

    template<typename IV, typename H>
    inline void operator() (const IV &idx, const H &col)  {

        calc_k_means_(col, std::min(idx.size(), col.size()));
    }

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

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  AffinityPropagationVisitor  {

};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

//N is the number of two-dimension data points
//S is the similarity matrix
//R is the responsibility matrix
//A is the availabiltiy matrix
//iter is the maximum number of iterations
//damping_factor is the damping factor
const size_type N = 25;

double      S[N][N] = {0};
double      R[N][N] = {0};
double      A[N][N] = {0};
size_type   iter = 230;
double      damping_factor = 0.9;

const char  *dataFileName = "ToyProblemData.txt";

void readS(double S[N][N], const char *dfn) {

    // read data 
    ifstream    myfile(dfn);
    
    double  dataPoint[N][2] = {0};

    for (size_type i = 0; i < N; i++)  {
        myfile >> dataPoint[i][0] >> dataPoint[i][1];
    }
    myfile.close();
    
    size_type       size = N * (N - 1) / 2;
    vector<double>  tmpS;

    // compute similarity between data point i and j (i is not equal to j)
    for (size_type i = 0; i < N - 1; i++)
        for (size_type j = i + 1; j < N; j++) {
            S[i][j] = -((dataPoint[i][0] - dataPoint[j][0]) *
                        (dataPoint[i][0] - dataPoint[j][0]) +
                        (dataPoint[i][1] - dataPoint[j][1]) *
                        (dataPoint[i][1] - dataPoint[j][1]));
            S[j][i] = S[i][j];
            tmpS.push_back(S[i][j]); 
        }

    // compute preferences for all data points: median 
    sort(tmpS.begin(), tmpS.end());

    double  median = 0;
    
    if (size % 2 == 0) 
        median = (tmpS[size / 2] + tmpS[size / 2 - 1]) / 2;
    else 
        median = tmpS[size / 2];

    for (size_type i = 0; i < N; i++)  S[i][i] = median;
}

int main()  {

    readS(S, dataFileName);
    
    for (size_type m = 0; m < iter; m++)  {
        // update responsibility
        for (size_type i = 0; i < N; i++)
            for (size_type k = 0; k < N; k++)  {
                double  max = -1e100;

                for (size_type kk = 0; kk < k; kk++)  {
                    if (S[i][kk] + A[i][kk] > max) 
                        max = S[i][kk] + A[i][kk];
                }
                for (size_type kk = k + 1; kk < N; kk++)  {
                    if (S[i][kk] + A[i][kk] > max) 
                        max = S[i][kk] + A[i][kk];
                }

                R[i][k] = (1 - damping_factor) * (S[i][k] - max) +
                          damping_factor * R[i][k];
            }

        // update availability
        for (size_type i = 0; i < N; i++)
            for (size_type k = 0; k < N; k++)  {
                if (i == k)  {
                    double  sum = 0.0;

                    for (size_type ii = 0; ii < i; ii++)
                        sum += max(0.0, R[ii][k]);
                    for(size_type ii = i + 1; ii < N; ii++)
                        sum += max(0.0, R[ii][k]);

                    A[i][k] = (1 - damping_factor) * sum +
                              damping_factor * A[i][k];
                }
                else  {
                    double      sum = 0.0;
                    size_type   maxik = max(i, k);
                    size_type   minik = min(i, k);

                    for (size_type ii = 0; ii < minik; ii++)
                        sum += max(0.0, R[ii][k]);
                    for (size_type ii = minik + 1; ii < maxik; ii++)
                        sum += max(0.0, R[ii][k]);
                    for (size_type ii = maxik + 1; ii < N; ii++)
                        sum += max(0.0, R[ii][k]);

                    A[i][k] = (1 - damping_factor) * min(0.0, R[k][k]+sum) +
                              damping_factor * A[i][k];
                }
            }
    }
    
    // find the exemplar
    vector<size_type> center;

    for (size_type i = 0; i < N; i++)
        if (R[i][i] + A[i][i] > 0)
            center.push_back(i);

    // data point assignment, idx[i] is the exemplar for data point i
    size_type idx[N] = { 0 };

    for (size_type i = 0; i < N; i++)  {
        size_type   idxForI = 0;
        double      maxSim = -1e100; 

        for (size_type j = 0; j < center.size(); j++) {
            size_type c = center[j];

            if (S[i][c] > maxSim)  {
                maxSim = S[i][c];
                idxForI = c;
            }
        }
        idx[i] = idxForI;
    }

    // output the assignment
    for (size_type i = 0; i < N; i++)
        cout << idx[i] << endl; 
}

