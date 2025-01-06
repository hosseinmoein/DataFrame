/*
  fastcluster: Fast hierarchical clustering routines for R and Python

  Copyright:
    * Until package version 1.1.23: © 2011 Daniel Müllner <https://danifold.net>
    * All changes from version 1.1.24 on: © Google Inc. <https://www.google.com>

  This library implements various fast algorithms for hierarchical,
  agglomerative clustering methods:

  (1) Algorithms for the "stored matrix approach": the input is the array of
      pairwise dissimilarities.

      MST_linkage_core: single linkage clustering with the "minimum spanning
      tree algorithm (Rohlfs)

      NN_chain_core: nearest-neighbor-chain algorithm, suitable for single,
      complete, average, weighted and Ward linkage (Murtagh)

      generic_linkage: generic algorithm, suitable for all distance update
      formulas (Müllner)

  (2) Algorithms for the "stored data approach": the input are points in a
      vector space.

      MST_linkage_core_vector: single linkage clustering for vector data

      generic_linkage_vector: generic algorithm for vector data, suitable for
      the Ward, centroid and median methods.

      generic_linkage_vector_alternative: alternative scheme for updating the
      nearest neighbors. This method seems faster than "generic_linkage_vector"
      for the centroid and median methods but slower for the Ward method.

  All these implementation treat infinity values correctly. They throw an
  exception if a NaN distance value occurs.
*/

#pragma once

#include <cmath>
#include <cstddef>
#include <limits>
#include <algorithm>
#include <stdexcept>

// ----------------------------------------------------------------------------

namespace hmdf
{

/* Method codes.

   These codes must agree with the METHODS array in fastcluster.R and the
   dictionary mthidx in fastcluster.py.
*/
// Non-Euclidean methods
enum class  method_codes : unsigned char  {

    METHOD_METR_SINGLE = 0,
    METHOD_METR_COMPLETE = 1,
    METHOD_METR_AVERAGE = 2,
    METHOD_METR_WEIGHTED = 3,
    METHOD_METR_WARD = 4,
    METHOD_METR_WARD_D = METHOD_METR_WARD,
    METHOD_METR_CENTROID = 5,
    METHOD_METR_MEDIAN = 6,
    METHOD_METR_WARD_D2 = 7,

    MIN_METHOD_CODE = method_codes::METHOD_METR_SINGLE,
    MAX_METHOD_CODE = method_codes::METHOD_METR_WARD_D2
};

// Euclidean methods
enum class  method_codes_vector : unsigned char  {

    METHOD_VECTOR_SINGLE = 0,
    METHOD_VECTOR_WARD = 1,
    METHOD_VECTOR_CENTROID = 2,
    METHOD_VECTOR_MEDIAN = 3,

    MIN_METHOD_VECTOR_CODE = METHOD_VECTOR_SINGLE,
    MAX_METHOD_VECTOR_CODE = METHOD_VECTOR_MEDIAN
};

/* Metric codes.

   These codes must agree with the dictionary mtridx in fastcluster.py.
*/
enum class  metric_codes : unsigned char  {

  METRIC_EUCLIDEAN =  0,
  METRIC_MINKOWSKI =  1,
  METRIC_CITYBLOCK =  2,
  METRIC_SEUCLIDEAN =  3,
  METRIC_SQEUCLIDEAN =  4,
  METRIC_COSINE =  5,
  METRIC_HAMMING =  6,
  METRIC_JACCARD =  7,
  METRIC_CHEBYCHEV =  8,
  METRIC_CANBERRA =  9,
  METRIC_BRAYCURTIS = 10,
  METRIC_MAHALANOBIS = 11,
  METRIC_YULE = 12,
  METRIC_MATCHING = 13,
  METRIC_DICE = 14,
  METRIC_ROGERSTANIMOTO = 15,
  METRIC_RUSSELLRAO = 16,
  METRIC_SOKALSNEATH = 17,
  METRIC_KULSINSKI = 18,
  METRIC_USER = 19,
  METRIC_INVALID = 20, // sentinel
  METRIC_JACCARD_BOOL = 21, // separate function for Jaccard metric on
};                          // Boolean input data

// ----------------------------------------------------------------------------

struct  node {

    std::size_t node1, node2;
    double      dist;
};

static inline bool
operator < (const node &a, const node &b) { return (a.dist < b.dist); }

// ----------------------------------------------------------------------------

class   ClusterResult {

public:

    std::vector<node>   Z { };
    std::size_t         pos { 0 };

public:

    ClusterResult(std::size_t size) : Z(size) , pos(0) {  }

    void append(std::size_t node1, std::size_t node2, double dist) {

        Z[pos].node1 = node1;
        Z[pos].node2 = node2;
        Z[pos].dist  = dist;
        ++pos;
    }

    node *operator[] (std::size_t idx)  { return Z.data() + idx; }

    /* Define several methods to postprocess the distances. All these functions
       are monotone, so they do not change the sorted order of distances. */

    void sqrt()  {

        for (std::size_t i = 0; i < pos; ++i)
            Z[i].dist = std::sqrt(Z[i].dist);
    }

    void sqrt(double)  { // ignore the argument

        sqrt();
    }

    void sqrtdouble(double)  { // ignore the argument

        for (std::size_t i = 0; i < pos; ++i)
            Z[i].dist = std::sqrt(2.0 * Z[i].dist);
    }

    void power(double p)  {

        const double    q = 1.0 / p;

        for (std::size_t i = 0; i < pos; ++i)
            Z[i].dist = std::pow(Z[i].dist, q);
    }

    void plusone(double)  { // ignore the argument

        for (std::size_t i = 0; i < pos; ++i)
            Z[i].dist += 1;
    }

    void divide(double denom)  {

        for (std::size_t i = 0; i < pos; ++i)
            Z[i].dist /= denom;
    }
};

// ----------------------------------------------------------------------------

/*
  Convenience class for the output array: automatic counter.
*/
class   LinkageOutput {

private:

    double  *Z;

public:

    LinkageOutput(double *Z_) : Z(Z_) {  }

    void append(std::size_t node1,
                std::size_t node2,
                double dist,
                double size) {

        if (node1 < node2) {
            *(Z++) = static_cast<double>(node1);
            *(Z++) = static_cast<double>(node2);
        }
        else {
            *(Z++) = static_cast<double>(node2);
            *(Z++) = static_cast<double>(node1);
        }
        *(Z++) = dist;
        *(Z++) = size;
    }
};

// ----------------------------------------------------------------------------

/*
  Lookup function for a union-find data structure.

  The function finds the root of idx by going iteratively through all
  parent elements until a root is found. An element i is a root if
  nodes[i] is zero. To make subsequent searches faster, the entry for
  idx and all its parents is updated with the root element.
 */
class   UnionFind {

private:

    std::vector<std::size_t>    parent;
    std::size_t                 nextparent;

public:

    UnionFind(std::size_t size)
        : parent(size > 0 ? 2 * size - 1 : 0, 0), nextparent(size) { }

    std::size_t Find (std::size_t idx)  {

        if (parent[idx] != 0) { // a -> b
            std::size_t p = idx;

            idx = parent[idx];
            if (parent[idx] != 0) { // a -> b -> c
                do { idx = parent[idx]; } while (parent[idx] != 0);
                do {
                    const std::size_t   tmp = parent[p];

                    parent[p] = idx;
                    p = tmp;
                } while (parent[p] != idx);
            }
        }
        return idx;
    }

    void Union (std::size_t node1, std::size_t node2) {

        parent[node1] = parent[node2] = nextparent++;
    }
};

// ----------------------------------------------------------------------------

/*
  Generate the SciPy-specific output format for a dendrogram from the
  clustering output.

  The list of merging steps can be sorted or unsorted.
*/
// The size of a node is either 1 (a single point) or is looked up from
// one of the clusters.
#define size_(r_) (((r_ < N) ? 1 : Z[(r_ - N) * 4]))

// ----------------------------------------------------------------------------

template <bool sorted>
static void
GenerateDenrogram(double *const Z, ClusterResult &Z2, std::size_t N) {

    if constexpr (! sorted)
        std::stable_sort(Z2[0], Z2[N - 1]);

    LinkageOutput   output(Z);
    std::size_t     node1, node2;

    for (node const *NN = Z2[0]; NN != Z2[N - 1]; ++NN) {
        // Get two data points whose clusters are merged in step i.
        node1 = NN->node1;
        node2 = NN->node2;
        output.append(node1, node2, NN->dist, size_(node1) + size_(node2));
    }
}

// ----------------------------------------------------------------------------

class   Dissimilarity {

private:

    double              *Xa;
    std::size_t         dim;
    std::size_t         N;
    std::vector<double> Xnew;
    std::size_t         *members;
    double              postprocessarg;

    void (ClusterResult::*postprocessfn)(double);
    double (Dissimilarity::*distfn)(std::size_t, std::size_t) const;

    std::vector<double> precomputed;
    // double              *precomputed2;

public:

    Dissimilarity() = delete;
    Dissimilarity(Dissimilarity const &) = delete;
    Dissimilarity &operator=(Dissimilarity const &) = delete;

    Dissimilarity (double *xa,
                   std::size_t d,
                   std::size_t n,
                   std::size_t *const members_,
                   const method_codes method,
                   const metric_codes metric,
                   bool temp_point_array = true)
        : Xa(xa),
          dim(d),
          N(n),
          Xnew(temp_point_array ? (N - 1) * dim : 0),
          members(members_),
          postprocessfn(nullptr)  {

        switch (method)  {
        case method_codes::METHOD_METR_SINGLE:
            postprocessfn = nullptr; // default

            switch (metric)  {
            case metric_codes::METRIC_EUCLIDEAN:
                set_euclidean();
                break;
            case metric_codes::METRIC_SQEUCLIDEAN:
                distfn = &Dissimilarity::sqeuclidean;
                break;
            case metric_codes::METRIC_CITYBLOCK:
                set_cityblock();
                break;
            case metric_codes::METRIC_CHEBYCHEV:
                set_chebychev();
                break;
            case metric_codes::METRIC_MINKOWSKI:
                set_minkowski();
                break;
            case metric_codes::METRIC_COSINE:
                distfn = &Dissimilarity::cosine;
                postprocessfn = &ClusterResult::plusone;
                // precompute norms
                precomputed.resize(N);
                for (std::size_t i = 0; i < N; ++i) {
                    double  sum = 0;

                    for (std::size_t k = 0; k < dim; ++k)
                        sum += X(i, k) * X(i, k);
                    precomputed[i] = 1.0 / std::sqrt(sum);
                }
                break;
            case metric_codes::METRIC_HAMMING:
                distfn = &Dissimilarity::hamming;
                postprocessfn = &ClusterResult::divide;
                postprocessarg = static_cast<double>(dim);
                break;
            case metric_codes::METRIC_JACCARD:
                distfn = &Dissimilarity::jaccard;
                break;
            case metric_codes::METRIC_CANBERRA:
                distfn = &Dissimilarity::canberra;
                break;
            case metric_codes::METRIC_BRAYCURTIS:
                distfn = &Dissimilarity::braycurtis;
                break;
            case metric_codes::METRIC_YULE:
                distfn = &Dissimilarity::yule;
                break;
            case metric_codes::METRIC_MATCHING:
                distfn = &Dissimilarity::matching;
                postprocessfn = &ClusterResult::divide;
                postprocessarg = static_cast<double>(dim);
                break;
            case metric_codes::METRIC_DICE:
                distfn = &Dissimilarity::dice;
                break;
            case metric_codes::METRIC_ROGERSTANIMOTO:
                distfn = &Dissimilarity::rogerstanimoto;
                break;
            case metric_codes::METRIC_RUSSELLRAO:
                distfn = &Dissimilarity::russellrao;
                postprocessfn = &ClusterResult::divide;
                postprocessarg = static_cast<double>(dim);
                break;
            case metric_codes::METRIC_SOKALSNEATH:
                distfn = &Dissimilarity::sokalsneath;
                break;
            case metric_codes::METRIC_KULSINSKI:
                distfn = &Dissimilarity::kulsinski;
                postprocessfn = &ClusterResult::plusone;
                precomputed.resize(N);
                for (std::size_t i = 0; i < N; ++i) {
                    std::size_t sum = 0;

                    for (std::size_t k = 0; k < dim; ++k)
                        sum += Xb(i, k);
                    precomputed[i] = -0.5 / static_cast<double>(sum);
                }
                break;
            default: // case METRIC_JACCARD_BOOL:
                distfn = &Dissimilarity::jaccard_bool;
            }
            break;

        case method_codes::METHOD_METR_WARD:
            postprocessfn = &ClusterResult::sqrtdouble;
            break;

        default:
            postprocessfn = &ClusterResult::sqrt;
        }
    }

    ~Dissimilarity() {  }

    inline double operator() (std::size_t i, std::size_t j) const {

        return (this->*distfn)(i, j);
    }

    inline double X(std::size_t i, std::size_t j) const {

        return Xa[i * dim + j];
    }

    inline char Xb(std::size_t i, std::size_t j) const {

        return  char(reinterpret_cast<bool *>(Xa)[i * dim + j]);
    }

    inline double *Xptr(std::size_t i, std::size_t j) const {

        return Xa + (i * dim + j);
    }

    void merge(std::size_t i, std::size_t j, std::size_t newnode)  {

        const double    *Pi =
            i < N ? Xa + i * dim : Xnew.data() + ((i - N) * dim);
        const double    *Pj =
            j < N ? Xa + j * dim : Xnew.data() + ((j - N) * dim);

        for (std::size_t k = 0; k < dim; ++k)
            Xnew[(newnode - N) * dim + k] =
               (Pi[k] * static_cast<double>(members[i]) +
                Pj[k] * static_cast<double>(members[j])) /
               static_cast<double>(members[i] + members[j]);

        members[newnode] = members[i] + members[j];
    }

    void merge_weighted(std::size_t i, std::size_t j, std::size_t newnode)  {

        const double    *Pi =
            i < N ? Xa + i * dim : Xnew.data() + ((i - N) * dim);
        const double    *Pj =
            j < N ? Xa + j * dim : Xnew.data() + ((j - N) * dim);

        for (std::size_t k = 0; k < dim; ++k)
            Xnew[(newnode - N) * dim + k] = (Pi[k] + Pj[k]) * 0.5;
    }

    void merge_inplace(std::size_t i, std::size_t j)  {

        double  *Pi = Xa + i * dim;
        double  *Pj = Xa + j * dim;

        for (std::size_t k = 0; k < dim; ++k)
            Pj[k] = (Pi[k] * static_cast<double>(members[i]) +
                     Pj[k] * static_cast<double>(members[j])) /
                    static_cast<double>(members[i] + members[j]);

        members[j] += members[i];
    }

    void merge_inplace_weighted(std::size_t i, std::size_t j)  {

        double  *Pi = Xa + i * dim;
        double  *Pj = Xa + j * dim;

        for (std::size_t k = 0; k < dim; ++k)
            Pj[k] = (Pi[k] + Pj[k]) * 0.5;
    }

    void postprocess(ClusterResult &Z2) const {

        if (postprocessfn != nullptr)
            (Z2.*postprocessfn)(postprocessarg);
    }

    inline double ward(std::size_t i, std::size_t j) const {

        const double    mi = static_cast<double>(members[i]);
        const double    mj = static_cast<double>(members[j]);

        return sqeuclidean(i, j) * mi * mj / (mi + mj);
    }

    inline double ward_initial(std::size_t i, std::size_t j) const {

        // alias for sqeuclidean
        // Factor 2!!!
        return sqeuclidean(i, j);
    }

    // This method must not produce NaN if the input is non-NaN.
    inline static double ward_initial_conversion(double min) {

        return min * 0.5;
    }

    inline double ward_extended(std::size_t i, std::size_t j) const {

        const double    mi = static_cast<double>(members[i]);
        const double    mj = static_cast<double>(members[j]);

        return sqeuclidean_extended(i, j) * mi * mj / (mi + mj);
    }

    double sqeuclidean(std::size_t i, std::size_t j) const {

        double          sum = 0;
        const double    *Pi = &(Xa[i * dim]);
        const double    *Pj = &(Xa[j * dim]);

        for (std::size_t k = 0; k < dim; ++k) {
            const double    diff = Pi[k] - Pj[k];

            sum += diff * diff;
        }
        return sum;
    }

    double sqeuclidean_extended(std::size_t i, std::size_t j) const {

        double          sum = 0;
        const double    *Pi =
            i < N ? &(Xa[i * dim]) : Xnew.data() + ((i - N) * dim); // TBD
        const double    *Pj =
            j < N ? &(Xa[j * dim]) : Xnew.data() + ((j - N) * dim);

        for (std::size_t k = 0; k < dim; ++k) {
            const double    diff = Pi[k] - Pj[k];

            sum += diff * diff;
        }
        return sum;
    }

private:

    void set_minkowski() {

        if (postprocessarg == std::numeric_limits<double>::infinity())
            set_chebychev();
        else if (postprocessarg == 1.0)
            set_cityblock();
        else if (postprocessarg == 2.0)
            set_euclidean();
        else  {
            distfn = &Dissimilarity::minkowski;
            postprocessfn = &ClusterResult::power;
        }
    }

    void set_euclidean() {

        distfn = &Dissimilarity::sqeuclidean;
        postprocessfn = &ClusterResult::sqrt;
    }

    void set_cityblock() {

        distfn = &Dissimilarity::cityblock;
    }

    void set_chebychev() {

        distfn = &Dissimilarity::chebychev;
    }


    double cityblock(std::size_t i, std::size_t j) const {

        double  sum = 0;

        for (std::size_t k = 0; k < dim; ++k)
            sum += std::abs(X(i, k) - X(j, k));
        return sum;
    }

    double minkowski(std::size_t i, std::size_t j) const {

        double  sum = 0;

        for (std::size_t k = 0; k < dim; ++k)
            sum += std::pow(std::abs(X(i, k) - X(j, k)), postprocessarg);
        return sum;
    }

    double chebychev(std::size_t i, std::size_t j) const {

        double  max = 0;

        for (std::size_t k = 0; k < dim; ++k) {
            const double    diff = std::abs(X(i, k) - X(j, k));

            if (diff > max)
                max = diff;
        }
        return max;
    }

    double cosine(std::size_t i, std::size_t j) const {

        double  sum = 0;

        for (std::size_t k = 0; k < dim; ++k)
            sum -= X(i, k) * X(j, k);
        return sum * precomputed[i] * precomputed[j];
    }

    double hamming(std::size_t i, std::size_t j) const {

        double  sum = 0;

        for (std::size_t k = 0; k < dim; ++k)
            sum += (X(i, k) != X(j, k));
        return sum;
    }

    // Differs from scipy.spatial.distance: equal vectors correctly
    // return distance 0.
    double jaccard(std::size_t i, std::size_t j) const {

        std::size_t sum1 = 0;
        std::size_t sum2 = 0;

        for (std::size_t k = 0; k < dim; ++k) {
            sum1 += (X(i, k) != X(j, k));
            sum2 += ((X(i, k) != 0) || (X(j, k) != 0));
        }
        return sum1 == 0
                   ? 0 : static_cast<double>(sum1) / static_cast<double>(sum2);
    }

    double canberra(std::size_t i, std::size_t j) const {

        double  sum = 0;

        for (std::size_t k = 0; k < dim; ++k) {
            double  numerator = std::abs(X(i, k) - X(j, k));

            sum += numerator == 0
                       ? 0
                       : numerator / (std::abs(X(i, k)) + std::abs(X(j, k)));
        }
        return sum;
    }

    double braycurtis(std::size_t i, std::size_t j) const {

        double  sum1 = 0;
        double  sum2 = 0;

        for (std::size_t k = 0; k < dim; ++k) {
            sum1 += std::abs(X(i, k) - X(j, k));
            sum2 += std::abs(X(i, k) + X(j, k));
        }
        return sum1 / sum2;
    }

    std::size_t mutable   NTT; // 'local' variables
    std::size_t mutable   NXO;
    std::size_t mutable   NTF;
#   define NTFFT NTF
#   define NFFTT NTT

    void nbool_correspond(std::size_t i, std::size_t j) const {

        NTT = 0;
        NXO = 0;
        for (std::size_t k = 0; k < dim; ++k) {
            NTT += (Xb(i, k) & Xb(j, k)) ;
            NXO += (Xb(i, k) ^ Xb(j, k)) ;
        }
    }

    void nbool_correspond_tfft(std::size_t i, std::size_t j) const {

        NTT = 0;
        NXO = 0;
        NTF = 0;
        for (std::size_t k = 0; k < dim; ++k) {
            NTT += (Xb(i, k) & Xb(j, k));
            NXO += (Xb(i, k) ^ Xb(j, k));
            NTF += (Xb(i, k) & (! Xb(j, k)));
        }
        NTF *= (NXO - NTF); // NTFFT
        NTT *= (static_cast<std::size_t>(dim) - NTT-NXO); // NFFTT
    }

    void nbool_correspond_xo(std::size_t i, std::size_t j) const {

        NXO = 0;
        for (std::size_t k = 0; k < dim; ++k)
            NXO += (Xb(i, k) ^ Xb(j, k));
    }

    void nbool_correspond_tt(std::size_t i, std::size_t j) const {

        NTT = 0;
        for (std::size_t k = 0; k < dim; ++k)
            NTT += (Xb(i, k) & Xb(j, k)) ;
    }

    double yule(std::size_t i, std::size_t j) const {

        nbool_correspond_tfft(i, j);
        return (NTFFT == 0)
                   ? 0
                   : static_cast<double>(2.0 * NTFFT) /
                     static_cast<double>(NTFFT + NFFTT);
    }

    // Prevent a zero denominator for equal vectors.
    double dice(std::size_t i, std::size_t j) const {

        nbool_correspond(i, j);
        return (NXO == 0) ? 0 :
            static_cast<double>(NXO) / static_cast<double>(NXO + 2.0 * NTT);
    }

    double rogerstanimoto(std::size_t i, std::size_t j) const {

        nbool_correspond_xo(i, j);
        return static_cast<double>(2.0 * NXO) / static_cast<double>(NXO + dim);
    }

    double russellrao(std::size_t i, std::size_t j) const {

        nbool_correspond_tt(i, j);
        return static_cast<double>(dim - NTT);
    }

    // Prevent a zero denominator for equal vectors.
    double sokalsneath(std::size_t i, std::size_t j) const {

        nbool_correspond(i, j);
        return (NXO == 0)
                   ? 0
                   : static_cast<double>(2.0 * NXO) /
                     static_cast<double>(NTT + 2.0 * NXO);
    }

    double kulsinski(std::size_t i, std::size_t j) const {

        nbool_correspond_tt(i, j);
        return static_cast<double>(NTT) * (precomputed[i] + precomputed[j]);
    }

    // 'matching' distance = Hamming distance
    double matching(std::size_t i, std::size_t j) const {

        nbool_correspond_xo(i, j);
        return static_cast<double>(NXO);
    }

    // Prevent a zero denominator for equal vectors.
    double jaccard_bool(std::size_t i, std::size_t j) const {

        nbool_correspond(i, j);
        return (NXO == 0)
                   ? 0
                   : static_cast<double>(NXO) / static_cast<double>(NXO + NTT);
    }
};

// ----------------------------------------------------------------------------

class   DLinkedList {
  /*
    Class for a doubly linked list. Initially, the list is the integer range
    [0, size]. We provide a forward iterator and a method to delete an index
    from the list.

    Typical use: for (i = L.start; L < size; i = L.succ[I])
    or
    for (i = somevalue; L < size; i = L.succ[I])
  */

public:

    std::size_t                 start;
    std::vector<std::size_t>    succ;

private:

    std::vector<std::size_t>    pred;

public:

    // Initialize to the given size.
    DLinkedList(std::size_t size) : start(0), succ(size + 1), pred(size + 1)  {

        for (std::size_t i = 0; i < size; ++i) {
            pred[i + 1] = i;
            succ[i] = i + 1;
        }
        // pred[0] is never accessed!
        // succ[size] is never accessed!
    }

    ~DLinkedList() {  }

    void remove(std::size_t idx) {

        // Remove an index from the list.
        if (idx == start) {
            start = succ[idx];
        }
        else {
            succ[pred[idx]] = succ[idx];
            pred[succ[idx]] = pred[idx];
        }
        succ[idx] = 0; // Mark as inactive
    }

    bool is_inactive(std::size_t idx) const { return (succ[idx] == 0); }
};

// ----------------------------------------------------------------------------

// Indexing functions
// D is the upper triangular part of a symmetric (N x N) - matrix
// We require r_ < c_ !
#define D_(r_, c_) \
    (D[(static_cast<std::ptrdiff_t>(2 * N - 3 - (r_)) * (r_) >> 1) + (c_) - 1])
// Z is an ((N - 1) x 4) - array
#define Z_(_r, _c) (Z[(_r) * 4 + (_c)])

// ----------------------------------------------------------------------------

inline static void
MST_linkage_core(std::size_t N, const double *const D, ClusterResult &Z2) {

/*
    N: integer, number of data points
    D: condensed distance matrix N * (N - 1) / 2
    Z2: output data structure

    The basis of this algorithm is an algorithm by Rohlf:

    F. James Rohlf, Hierarchical clustering using the minimum spanning tree,
    The Computer Journal, vol. 16, 1973, p. 93–95.
*/

    std::size_t         i;
    std::size_t         idx2 = 1;
    DLinkedList         active_nodes(N);
    std::vector<double> d(N);
    std::size_t         prev_node;
    double              min = std::numeric_limits<double>::infinity();

    // first iteration
    for (i = 1; i < N; ++i) {
        d[i] = D[i - 1];
        if (d[i] < min) {
            min = d[i];
            idx2 = i;
        }
    }
    Z2.append(0, idx2, min);

    for (std::size_t j = 1; j < N - 1; ++j) {
        prev_node = idx2;
        active_nodes.remove(prev_node);

        idx2 = active_nodes.succ[0];
        min = d[idx2];
        for (i = idx2; i < prev_node; i = active_nodes.succ[i]) {
            double tmp = D_(i, prev_node);

            if (tmp < d[i])
                d[i] = tmp;
            if (d[i] < min) {
                min = d[i];
                idx2 = i;
            }
        }
        for (; i < N; i = active_nodes.succ[i]) {
            const double    tmp = D_(prev_node, i);

            if (d[i] > tmp)
                d[i] = tmp;
            if (d[i] < min) {
                min = d[i];
                idx2 = i;
            }
        }
        Z2.append(prev_node, idx2, min);
    }
}

// ----------------------------------------------------------------------------

/* Functions for the update of the dissimilarity array */

inline static void
f_single(double *const b, double a) { if (*b > a) *b = a; }

// ----------------------------------------------------------------------------

inline static void
f_complete(double *const b, double a) { if (*b < a) *b = a; }

// ----------------------------------------------------------------------------

inline static void
f_average(double *const b, double a, double s, double t) {

    *b = s * a + t * (*b);
}

// ----------------------------------------------------------------------------

inline static void
f_weighted(double *const b, double a) { *b = (a + *b) * 0.5; }

// ----------------------------------------------------------------------------

inline static void
f_ward(double *const b, double a, double c, double s, double t, double v) {

    *b = ((v + s) * a - v * c + (v + t) * (*b)) / (s + t + v);
}

// ----------------------------------------------------------------------------

inline static void
f_centroid(double *const b, double a, double stc, double s, double t) {

    *b = s * a - stc + t * (*b);
}

// ----------------------------------------------------------------------------

inline static void
f_median(double *const b, double a, double c_4) {

    *b = (a + (*b)) * 0.5 - c_4;
}

// ----------------------------------------------------------------------------

template<method_codes method>
static void NN_chain_core(std::size_t N,
                          double *const D,
                          std::size_t *const members,
                          ClusterResult &Z2) {

/*
    N: integer
    D: condensed distance matrix N * (N - 1) / 2
    Z2: output data structure

    This is the NN-chain algorithm, described on page 86 in the following book:

    Fionn Murtagh, Multidimensional Clustering Algorithms,
    Vienna, Würzburg: Physica-Verlag, 1985.
*/

    std::size_t                 i;
    std::vector<std::size_t>    NN_chain(N);
    std::size_t                 NN_chain_tip = 0;
    std::size_t                 idx1, idx2;
    double                      size1, size2;
    DLinkedList                 active_nodes(N);
    double                      min;

    for (std::size_t j = 0; j < N - 1; ++j) {
        if (NN_chain_tip <= 3) {
            NN_chain[0] = idx1 = active_nodes.start;
            NN_chain_tip = 1;

            idx2 = active_nodes.succ[idx1];
            min = D_(idx1, idx2);
            for (i = active_nodes.succ[idx2]; i < N;
                 i = active_nodes.succ[i]) {
                if (D_(idx1, i) < min) {
                    min = D_(idx1, i);
                    idx2 = i;
                }
            }
        }  // a: idx1   b: idx2
        else {
            NN_chain_tip -= 3;
            idx1 = NN_chain[NN_chain_tip - 1];
            idx2 = NN_chain[NN_chain_tip];
            min = idx1 < idx2 ? D_(idx1, idx2) : D_(idx2, idx1);
        }  // a: idx1   b: idx2

        do {
            NN_chain[NN_chain_tip] = idx2;

            for (i = active_nodes.start; i < idx2; i = active_nodes.succ[i]) {
                if (D_(i, idx2) < min) {
                    min = D_(i, idx2);
                    idx1 = i;
                }
            }
            for (i = active_nodes.succ[idx2]; i < N;
                 i = active_nodes.succ[i]) {
                if (D_(idx2, i) < min) {
                    min = D_(idx2, i);
                    idx1 = i;
                }
            }

            idx2 = idx1;
            idx1 = NN_chain[NN_chain_tip++];

        } while (idx2 != NN_chain[NN_chain_tip - 2]);

        Z2.append(idx1, idx2, min);

        if (idx1 > idx2) {
            const std::size_t   tmp = idx1;

            idx1 = idx2;
            idx2 = tmp;
        }

        if constexpr (method == method_codes::METHOD_METR_AVERAGE ||
                      method == method_codes::METHOD_METR_WARD) {
            size1 = static_cast<double>(members[idx1]);
            size2 = static_cast<double>(members[idx2]);
            members[idx2] += members[idx1];
        }

        // Remove the smaller index from the valid indices (active_nodes).
        active_nodes.remove(idx1);

        if constexpr (method == method_codes::METHOD_METR_SINGLE)  {
           /*
            Single linkage.
            Characteristic: new distances are never longer than the
            old distances.
            */
            // Update the distance matrix in the range [start, idx1).
            for (i = active_nodes.start; i < idx1; i = active_nodes.succ[i])
                f_single(&D_(i, idx2), D_(i, idx1));
            // Update the distance matrix in the range (idx1, idx2).
            for (; i < idx2; i = active_nodes.succ[i])
                f_single(&D_(i, idx2), D_(idx1, i));
            // Update the distance matrix in the range (idx2, N).
            for (i = active_nodes.succ[idx2]; i < N; i = active_nodes.succ[i])
                f_single(&D_(idx2, i), D_(idx1, i));
        }
        else if constexpr (method == method_codes::METHOD_METR_COMPLETE)  {
            /*
            Complete linkage.
            Characteristic: new distances are never shorter than the
            old distances.
            */
            // Update the distance matrix in the range [start, idx1).
            for (i = active_nodes.start; i < idx1; i = active_nodes.succ[i])
                f_complete(&D_(i, idx2), D_(i, idx1));
            // Update the distance matrix in the range (idx1, idx2).
            for (; i < idx2; i = active_nodes.succ[i])
                f_complete(&D_(i, idx2), D_(idx1, i));
            // Update the distance matrix in the range (idx2, N).
            for (i = active_nodes.succ[idx2]; i < N; i = active_nodes.succ[i])
                f_complete(&D_(idx2, i), D_(idx1, i));
        }
        else if constexpr (method == method_codes::METHOD_METR_AVERAGE)  {
            /*
            Average linkage.
            Shorter and longer distances can occur.
            */
            // Update the distance matrix in the range [start, idx1).
            const double  s = size1 / (size1 + size2);
            const double  t = size2 / (size1 + size2);

            for (i = active_nodes.start; i < idx1; i = active_nodes.succ[i])
                f_average(&D_(i, idx2), D_(i, idx1), s, t);
            // Update the distance matrix in the range (idx1, idx2).
            for (; i < idx2; i = active_nodes.succ[i])
                f_average(&D_(i, idx2), D_(idx1, i), s, t);
            // Update the distance matrix in the range (idx2, N).
            for (i = active_nodes.succ[idx2]; i < N; i = active_nodes.succ[i])
                f_average(&D_(idx2, i), D_(idx1, i), s, t);
        }
        else if constexpr (method == method_codes::METHOD_METR_WEIGHTED)  {
            /*
            Weighted linkage.
            Shorter and longer distances can occur.
            */
            // Update the distance matrix in the range [start, idx1).
            for (i = active_nodes.start; i < idx1; i = active_nodes.succ[i])
                f_weighted(&D_(i, idx2), D_(i, idx1));
            // Update the distance matrix in the range (idx1, idx2).
            for (; i < idx2; i = active_nodes.succ[i])
                f_weighted(&D_(i, idx2), D_(idx1, i));
            // Update the distance matrix in the range (idx2, N).
            for (i = active_nodes.succ[idx2]; i < N; i = active_nodes.succ[i])
                f_weighted(&D_(idx2, i), D_(idx1, i));
        }
        else if constexpr (method == method_codes::METHOD_METR_WEIGHTED)  {
            /*
            Ward linkage.
            Shorter and longer distances can occur, not smaller than min(d1, d2)
            but maybe bigger than max(d1, d2).
            */
            // Update the distance matrix in the range [start, idx1).
            // double v = static_cast< double>(members[i]);
            for (i = active_nodes.start; i < idx1; i = active_nodes.succ[i])
                f_ward(&D_(i, idx2), D_(i, idx1), min,
                       size1, size2, static_cast<double>(members[i]));
            // Update the distance matrix in the range (idx1, idx2).
            for (; i < idx2; i = active_nodes.succ[i])
                f_ward(&D_(i, idx2), D_(idx1, i), min,
                       size1, size2, static_cast<double>(members[i]));
            // Update the distance matrix in the range (idx2, N).
            for (i = active_nodes.succ[idx2]; i < N; i = active_nodes.succ[i])
                f_ward(&D_(idx2, i), D_(idx1, i), min,
                       size1, size2, static_cast<double>(members[i]));
        }
        else  {
            throw std::runtime_error("NN_chain_core(): Invalid method.");
        }
    }
}

// ----------------------------------------------------------------------------

class   BinaryMinHeap {
  /*
  Class for a binary min-heap. The data resides in an array A. The elements of
  A are not changed but two lists I and R of indices are generated which point
  to elements of A and backwards.

  The heap tree structure is

  H[2 * i + 1]   H[2 * i + 2]
         \            /
          \          /
           ≤        ≤
            \      /
             \    /
              H[i]

  where the children must be less or equal than their parent. Thus, H[0]
  contains the minimum. The lists I and R are made such that H[i] = A[I[i]]
  and R[I[i]] = i.

  This implementation is not designed to handle NaN values.
  */

private:

    double *const               A;
    std::size_t                 size;
    std::vector<std::size_t>    I;
    std::vector<std::size_t>    R;

public:

    BinaryMinHeap() = delete;
    BinaryMinHeap(BinaryMinHeap const &) = delete;
    BinaryMinHeap & operator =(BinaryMinHeap const &) = delete;

    // Allocate memory and initialize the lists I and R to the identity. This
    // does not make it a heap. Call heapify afterwards!
    BinaryMinHeap(double *const A_, std::size_t size_)
        : A(A_), size(size_), I(size), R(size)  {

        for (std::size_t i = 0; i < size; ++i)
            R[i] = I[i] = i;
    }

    // Allocate memory and initialize the lists I and R to the identity. This
    // does not make it a heap. Call heapify afterwards!
    BinaryMinHeap(double *const A_,
                  std::size_t size1,
                  std::size_t size2,
                  std::size_t start)
        : A(A_), size(size1), I(size1), R(size2)  {

        for (std::size_t i = 0; i < size; ++i) {
            R[i + start] = i;
            I[i] = i + start;
        }
    }

    ~BinaryMinHeap() {  }

    // Arrange the indices I and R so that H[i] = A[I[i]] satisfies the heap
    // condition H[i] < H[2 * i + 1] and H[i] < H[2 * i + 2] for each i.
    //
    // Complexity: Θ(size)
    // Reference: Cormen, Leiserson, Rivest, Stein, Introduction to Algorithms,
    // 3rd ed., 2009, Section 6.3 “Building a heap”
    void heapify() {

        std::size_t idx;

        for (idx = (size >> 1); idx > 0;) {
            --idx;
            update_geq_(idx);
        }
    }

    inline std::size_t argmin() const {

        // Return the minimal element.
        return I[0];
    }

    void heap_pop() {

        // Remove the minimal element from the heap.
        --size;
        I[0] = I[size];
        R[I[0]] = 0;
        update_geq_(0);
    }

    void remove(std::size_t idx) {

        // Remove an element from the heap.
        --size;
        R[I[size]] = R[idx];
        I[R[idx]] = I[size];
        if (H(size) <= A[idx])
            update_leq_(R[idx]);
        else
            update_geq_(R[idx]);
    }

    void replace (std::size_t idxold, std::size_t idxnew, double val) {

        R[idxnew] = R[idxold];
        I[R[idxnew]] = idxnew;
        if (val <= A[idxold])
            update_leq(idxnew, val);
        else
            update_geq(idxnew, val);
    }

    // Update the element A[i] with val and re-arrange the indices to preserve
    // the heap condition.
    void update (std::size_t idx, double val)  {

        if (val <= A[idx])
            update_leq(idx, val);
        else
            update_geq(idx, val);
    }

    void update_leq (std::size_t idx, double val)  {

        // Use this when the new value is not more than the old value.
        A[idx] = val;
        update_leq_(R[idx]);
    }

    void update_geq (std::size_t idx, double val)  {

        // Use this when the new value is not less than the old value.
        A[idx] = val;
        update_geq_(R[idx]);
    }

private:

    void update_leq_ (std::size_t i)  {

        std::size_t j;

        for (; (i > 0) && (H(i) < H(j = (i - 1) >> 1)); i = j)
            heap_swap(i, j);
    }

    void update_geq_ (std::size_t i)  {

        std::size_t j;

        for (; (j = 2 * i + 1) < size; i = j) {
            if (H(j) >= H(i)) {
                ++j;
                if (j >= size || H(j) >= H(i))  break;
            }
            else if (((j + 1) < size) && (H(j + 1) < H(j)))  ++j;
            heap_swap(i, j);
        }
    }

    void heap_swap(std::size_t i, std::size_t j)  {

        // Swap two indices.
        const std::size_t tmp = I[i];

        I[i] = I[j];
        I[j] = tmp;
        R[I[i]] = i;
        R[I[j]] = j;
    }

    inline double H(std::size_t i) const { return A[I[i]]; }
};

// ----------------------------------------------------------------------------

template <method_codes method>
static void generic_linkage(std::size_t N,
                            double *const D,
                            std::size_t *const members,
                            ClusterResult &Z2) {

  /*
    N: integer, number of data points
    D: condensed distance matrix N * (N - 1) / 2
    Z2: output data structure
  */

    const std::size_t           N_1 = N - 1;
    std::size_t                 i, j; // loop variables
    std::size_t                 idx1, idx2; // row and column indices
    std::vector<std::size_t>    n_nghbr(N_1); // array of nearest neighbors
    // distances to the nearest neighbors
    std::vector<double>         mindist(N_1);
    std::vector<std::size_t>    row_repr(N); // row_repr[i]: node number
    DLinkedList                 active_nodes(N);
    // minimum heap structure for the distance to the nearest neighbor of
    // each point
    BinaryMinHeap               nn_distances(mindist.data(), N_1);
    std::size_t                 node1, node2; // node numbers in the output
    double                      size1, size2; // and their cardinalities
    // minimum and row index for nearest-neighbor search
    double                      min;
    std::size_t                 idx;

    for (i = 0; i < N; ++i)
        // Build a list of row ↔ node label assignments.
        // Initially i ↦ i
        row_repr[i] = i;

    // Initialize the minimal distances:
    // Find the nearest neighbor of each point.
    // n_nghbr[i] = argmin_{j > i} D(i, j) for i in range(N-1)
    const double    *DD = D;

    for (i = 0; i < N_1; ++i) {
        min = std::numeric_limits<double>::infinity();
        for (idx = j = i + 1; j < N; ++j, ++DD) {
            if (*DD < min) {
                min = *DD;
                idx = j;
            }
        }
        mindist[i] = min;
        n_nghbr[i] = idx;
    }

    // Put the minimal distances into a heap structure to make the repeated
    // global minimum searches fast.
    nn_distances.heapify();

    // Main loop: We have N - 1 merging steps.
    /*
    Here is a special feature that allows fast bookkeeping and updates of the
    minimal distances.

    mindist[i] stores a lower bound on the minimum distance of the point i to
    all points of higher index:

        mindist[i] ≥ min_{j > i} D(i, j)

    Normally, we have equality. However, this minimum may become invalid due
    to the updates in the distance matrix. The rules are:

    1) If mindist[i] is equal to D(i, n_nghbr[i]), this is the correct
       minimum and n_nghbr[i] is a nearest neighbor.

    2) If mindist[i] is smaller than D(i, n_nghbr[i]), this might not be the
       correct minimum. The minimum needs to be recomputed.

    3) mindist[i] is never bigger than the true minimum. Hence, we never
       miss the true minimum if we take the smallest mindist entry,
       re-compute the value if necessary (thus maybe increasing it) and
       looking for the now smallest mindist entry until a valid minimal
       entry is found. This step is done in the lines below.

    The update process for D below takes care that these rules are
    fulfilled. This makes sure that the minima in the rows D(i, i+1:)of D are
    re-calculated when necessary but re-calculation is avoided whenever
    possible.

    The re-calculation of the minima makes the worst-case runtime of this
    algorithm cubic in N. We avoid this whenever possible, and in most cases
    the runtime appears to be quadratic.
    */
    for (i = 0; i < N_1; ++i) {
        idx1 = nn_distances.argmin();
        if constexpr (method != method_codes::METHOD_METR_SINGLE) {
            while (mindist[idx1] < D_(idx1, n_nghbr[idx1])) {
                // Recompute the minimum mindist[idx1] and n_nghbr[idx1].
                // exists, maximally N - 1
                n_nghbr[idx1] = j = active_nodes.succ[idx1];
                min = D_(idx1, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    if (D_(idx1, j) < min) {
                        min = D_(idx1, j);
                        n_nghbr[idx1] = j;
                    }
                }
                /* Update the heap with the new true minimum and search for the
                   (possibly different) minimal entry. */
                nn_distances.update_geq(idx1, min);
                idx1 = nn_distances.argmin();
            }
        }

        nn_distances.heap_pop(); // Remove the current minimum from the heap.
        idx2 = n_nghbr[idx1];

        // Write the newly found minimal pair of nodes to the output array.
        node1 = row_repr[idx1];
        node2 = row_repr[idx2];

        if constexpr (method == method_codes::METHOD_METR_AVERAGE ||
                      method == method_codes::METHOD_METR_WARD ||
                      method == method_codes::METHOD_METR_CENTROID) {
            size1 = static_cast<double>(members[idx1]);
            size2 = static_cast<double>(members[idx2]);
            members[idx2] += members[idx1];
        }
        Z2.append(node1, node2, mindist[idx1]);

        // Remove idx1 from the list of active indices (active_nodes).
        active_nodes.remove(idx1);
        // Index idx2 now represents the new (merged) node with label N+i.
        row_repr[idx2] = N + i;

        // Update the distance matrix
        if constexpr (method == method_codes::METHOD_METR_SINGLE)  {
            /*
            Single linkage.
            Characteristic: new distances are never longer than the
            old distances.
            */
            // Update the distance matrix in the range [start, idx1).
            for (j = active_nodes.start; j < idx1; j = active_nodes.succ[j]) {
                f_single(&D_(j, idx2), D_(j, idx1));
                if (n_nghbr[j] == idx1)
                    n_nghbr[j] = idx2;
            }
            // Update the distance matrix in the range (idx1, idx2).
            for (; j < idx2; j = active_nodes.succ[j]) {
                f_single(&D_(j, idx2), D_(idx1, j));
                // If the new value is below the old minimum in a row, update
                // the mindist and n_nghbr arrays.
                if (D_(j, idx2) < mindist[j]) {
                    nn_distances.update_leq(j, D_(j, idx2));
                    n_nghbr[j] = idx2;
                }
            }
            // Update the distance matrix in the range (idx2, N).
            // Recompute the minimum mindist[idx2] and n_nghbr[idx2].
            if (idx2 < N_1) {
                min = mindist[idx2];
                for (j = active_nodes.succ[idx2]; j < N;
                     j = active_nodes.succ[j]) {
                    f_single(&D_(idx2, j), D_(idx1, j));
                    if (D_(idx2, j) < min) {
                        n_nghbr[idx2] = j;
                        min = D_(idx2, j);
                    }
                }
                nn_distances.update_leq(idx2, min);
            }
        }
        else if constexpr (method == method_codes::METHOD_METR_COMPLETE)  {
            /*
            Complete linkage.
            Characteristic: new distances are never shorter than
            the old distances.
            */
            // Update the distance matrix in the range [start, idx1).
            for (j = active_nodes.start; j < idx1; j = active_nodes.succ[j]) {
                f_complete(&D_(j, idx2), D_(j, idx1));
                if (n_nghbr[j] == idx1)
                    n_nghbr[j] = idx2;
            }
            // Update the distance matrix in the range (idx1, idx2).
            for (; j < idx2; j = active_nodes.succ[j])
                f_complete(&D_(j, idx2), D_(idx1, j));
            // Update the distance matrix in the range (idx2, N).
            for (j = active_nodes.succ[idx2]; j < N; j = active_nodes.succ[j])
                f_complete(&D_(idx2, j), D_(idx1, j));
        }
        else if constexpr (method == method_codes::METHOD_METR_AVERAGE)  {
            /*
            Average linkage.
            Shorter and longer distances can occur.
            */
            // Update the distance matrix in the range [start, idx1).
            const double    s = size1 / (size1 + size2);
            const double    t = size2 / (size1 + size2);

            for (j = active_nodes.start; j < idx1; j = active_nodes.succ[j]) {
                f_average(&D_(j, idx2), D_(j, idx1), s, t);
                if (n_nghbr[j] == idx1)
                    n_nghbr[j] = idx2;
            }
            // Update the distance matrix in the range (idx1, idx2).
            for (; j < idx2; j = active_nodes.succ[j]) {
                f_average(&D_(j, idx2), D_(idx1, j), s, t);
                if (D_(j, idx2) < mindist[j]) {
                    nn_distances.update_leq(j, D_(j, idx2));
                    n_nghbr[j] = idx2;
                }
            }
            // Update the distance matrix in the range (idx2, N).
            if (idx2 < N_1) {
                // exists, maximally N-1
                n_nghbr[idx2] = j = active_nodes.succ[idx2];
                f_average(&D_(idx2, j), D_(idx1, j), s, t);
                min = D_(idx2, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    f_average(&D_(idx2, j), D_(idx1, j), s, t);
                    if (D_(idx2, j) < min) {
                        min = D_(idx2, j);
                        n_nghbr[idx2] = j;
                    }
                }
                nn_distances.update(idx2, min);
            }
        }
        else if constexpr (method == method_codes::METHOD_METR_WEIGHTED)  {
            /*
            Weighted linkage.
            Shorter and longer distances can occur.
            */
            // Update the distance matrix in the range [start, idx1).
            for (j = active_nodes.start; j < idx1; j = active_nodes.succ[j]) {
                f_weighted(&D_(j, idx2), D_(j, idx1));
                if (n_nghbr[j] == idx1)
                    n_nghbr[j] = idx2;
            }
            // Update the distance matrix in the range (idx1, idx2).
            for (; j < idx2; j = active_nodes.succ[j]) {
                f_weighted(&D_(j, idx2), D_(idx1, j));
                if (D_(j, idx2) < mindist[j]) {
                    nn_distances.update_leq(j, D_(j, idx2));
                    n_nghbr[j] = idx2;
                }
            }
            // Update the distance matrix in the range (idx2, N).
            if (idx2 < N_1) {
                // exists, maximally N - 1
                n_nghbr[idx2] = j = active_nodes.succ[idx2];
                f_weighted(&D_(idx2, j), D_(idx1, j));
                min = D_(idx2, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    f_weighted(&D_(idx2, j), D_(idx1, j));
                    if (D_(idx2, j) < min) {
                        min = D_(idx2, j);
                        n_nghbr[idx2] = j;
                    }
                }
                nn_distances.update(idx2, min);
            }
        }
        else if constexpr (method == method_codes::METHOD_METR_WARD)  {
            /*
              Ward linkage.
              Shorter and longer distances can occur, not smaller
              than min(d1, d2) but maybe bigger than max(d1, d2).
            */
            // Update the distance matrix in the range [start, idx1).
            for (j = active_nodes.start; j < idx1; j = active_nodes.succ[j]) {
                f_ward(&D_(j, idx2), D_(j, idx1), mindist[idx1],
                       size1, size2, static_cast<double>(members[j]));
                if (n_nghbr[j] == idx1)
                    n_nghbr[j] = idx2;
            }
            // Update the distance matrix in the range (idx1, idx2).
            for (; j < idx2; j = active_nodes.succ[j]) {
                f_ward(&D_(j, idx2), D_(idx1, j), mindist[idx1], size1, size2,
                           static_cast<double>(members[j]));
                if (D_(j, idx2) < mindist[j]) {
                    nn_distances.update_leq(j, D_(j, idx2));
                    n_nghbr[j] = idx2;
                }
            }
            // Update the distance matrix in the range (idx2, N).
            if (idx2 < N_1) {
                // exists, maximally N-1
                n_nghbr[idx2] = j = active_nodes.succ[idx2];
                f_ward(&D_(idx2, j), D_(idx1, j), mindist[idx1],
                           size1, size2, static_cast<double>(members[j]));
                min = D_(idx2, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    f_ward(&D_(idx2, j), D_(idx1, j), mindist[idx1],
                           size1, size2, static_cast<double>(members[j]));
                    if (D_(idx2, j) < min) {
                        min = D_(idx2, j);
                        n_nghbr[idx2] = j;
                    }
                }
                nn_distances.update(idx2, min);
            }
        }
        else if constexpr (method == method_codes::METHOD_METR_CENTROID)  {
            /*
            Centroid linkage.
            Shorter and longer distances can occur, not bigger than max(d1, d2)
            but maybe smaller than min(d1, d2).
            */
            // Update the distance matrix in the range [start, idx1).
            double  s = size1 / (size1 + size2);
            double  t = size2 / (size1 + size2);
            double  stc = s * t * mindist[idx1];

            for (j = active_nodes.start; j < idx1; j = active_nodes.succ[j]) {
                f_centroid(&D_(j, idx2), D_(j, idx1), stc, s, t);
                if (D_(j, idx2) < mindist[j]) {
                    nn_distances.update_leq(j, D_(j, idx2));
                    n_nghbr[j] = idx2;
                }
                else if (n_nghbr[j] == idx1)
                    n_nghbr[j] = idx2;
            }
            // Update the distance matrix in the range (idx1, idx2).
            for (; j < idx2; j = active_nodes.succ[j]) {
                f_centroid(&D_(j, idx2), D_(idx1, j), stc, s, t);
                if (D_(j, idx2) < mindist[j]) {
                    nn_distances.update_leq(j, D_(j, idx2));
                    n_nghbr[j] = idx2;
                }
            }
            // Update the distance matrix in the range (idx2, N).
            if (idx2 < N_1) {
                // exists, maximally N-1
                n_nghbr[idx2] = j = active_nodes.succ[idx2];
                f_centroid(&D_(idx2, j), D_(idx1, j), stc, s, t);
                min = D_(idx2, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    f_centroid(&D_(idx2, j), D_(idx1, j), stc, s, t);
                    if (D_(idx2, j) < min) {
                        min = D_(idx2, j);
                        n_nghbr[idx2] = j;
                    }
                }
                nn_distances.update(idx2, min);
            }
        }
        else if constexpr (method == method_codes::METHOD_METR_MEDIAN)  {
            /*
            Median linkage.
            Shorter and longer distances can occur, not bigger than max(d1, d2)
            but maybe smaller than min(d1, d2).
            */
            // Update the distance matrix in the range [start, idx1).
            const double    c_4 = mindist[idx1] * 0.25;

            for (j = active_nodes.start; j < idx1; j = active_nodes.succ[j]) {
                f_median(&D_(j, idx2), D_(j, idx1), c_4);
                if (D_(j, idx2) < mindist[j]) {
                    nn_distances.update_leq(j, D_(j, idx2));
                    n_nghbr[j] = idx2;
                }
                else if (n_nghbr[j] == idx1)
                    n_nghbr[j] = idx2;
            }
            // Update the distance matrix in the range (idx1, idx2).
            for (; j < idx2; j = active_nodes.succ[j]) {
                f_median(&D_(j, idx2), D_(idx1, j), c_4);
                if (D_(j, idx2) < mindist[j]) {
                    nn_distances.update_leq(j, D_(j, idx2));
                    n_nghbr[j] = idx2;
                }
            }
            // Update the distance matrix in the range (idx2, N).
            if (idx2 < N_1) {
                // exists, maximally N - 1
                n_nghbr[idx2] = j = active_nodes.succ[idx2];
                f_median(&D_(idx2, j), D_(idx1, j), c_4);
                min = D_(idx2, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    f_median(&D_(idx2, j), D_(idx1, j), c_4);
                    if (D_(idx2, j) < min) {
                        min = D_(idx2, j);
                        n_nghbr[idx2] = j;
                    }
                }
                nn_distances.update(idx2, min);
            }
        }
        else  {
            throw std::runtime_error("generic_linkage(): Invalid method.");
        }
    }
}

// ----------------------------------------------------------------------------

/*
  Clustering methods for vector data
*/

inline static void
MST_linkage_core_vector(std::size_t N,
                        Dissimilarity &dist,
                        ClusterResult &Z2) {

/*
    N: integer, number of data points
    dist: function pointer to the metric
    Z2: output data structure

    The basis of this algorithm is an algorithm by Rohlf:

    F. James Rohlf, Hierarchical clustering using the minimum spanning tree,
    The Computer Journal, vol. 16, 1973, p. 93–95.
*/

    std::size_t         i;
    std::size_t         idx2;
    DLinkedList         active_nodes(N);
    std::vector<double> d(N);
    std::size_t         prev_node;
    double              min;

    // first iteration
    idx2 = 1;
    min = std::numeric_limits<double>::infinity();
    for (i = 1; i < N; ++i) {
        d[i] = dist(0, i);
        if (d[i] < min) {
            min = d[i];
            idx2 = i;
        }
    }

    Z2.append(0, idx2, min);

    for (std::size_t j = 1; j < N - 1; ++j) {
        prev_node = idx2;
        active_nodes.remove(prev_node);

        idx2 = active_nodes.succ[0];
        min = d[idx2];

        for (i = idx2; i < N; i = active_nodes.succ[i]) {
            double  tmp = dist(i, prev_node);

            if (d[i] > tmp)
                d[i] = tmp;
            if (d[i] < min) {
                min = d[i];
                idx2 = i;
            }
        }
        Z2.append(prev_node, idx2, min);
    }
}

// ----------------------------------------------------------------------------

template <method_codes_vector method>
static void
generic_linkage_vector(std::size_t N, Dissimilarity &dist, ClusterResult &Z2) {

    /*
    N: integer, number of data points
    dist: function pointer to the metric
    Z2: output data structure

    This algorithm is valid for the distance update methods
    "Ward", "centroid" and "median" only!
    */

    const std::size_t           N_1 = N - 1;
    std::size_t                 i, j; // loop variables
    std::size_t                 idx1, idx2; // row and column indices
    std::vector<std::size_t>    n_nghbr(N_1); // array of nearest neighbors
    // distances to the nearest neighbors
    std::vector<double>         mindist(N_1);
    std::vector<std::size_t>    row_repr(N); // row_repr[i]: node number
    DLinkedList                 active_nodes(N);
    // minimum heap structure for the distance to the nearest neighbor
    // of each point
    BinaryMinHeap               nn_distances(mindist.data(), N_1);
    std::size_t                 node1, node2;     // node numbers in the output
    // minimum and row index for nearest-neighbor search
    double                      min;

    for (i = 0; i < N; ++i)
        // Build a list of row ↔ node label assignments.
        // Initially i ↦ i
        row_repr[i] = i;

    // Initialize the minimal distances:
    // Find the nearest neighbor of each point.
    // n_nghbr[i] = argmin_{j > i} D(i, j) for i in range(N-1)
    for (i = 0; i < N_1; ++i) {
        std::size_t idx;

        min = std::numeric_limits<double>::infinity();
        for (idx = j = i + 1; j < N; ++j) {
            double  tmp;

            if constexpr (method == method_codes_vector::METHOD_VECTOR_WARD)  {
                tmp = dist.ward_initial(i, j);
            }
            else  {
                tmp = dist.sqeuclidean(i, j);
            }
            if (tmp < min) {
                min = tmp;
                idx = j;
            }
        }
        if constexpr (method == method_codes_vector::METHOD_VECTOR_WARD)  {
            mindist[i] = Dissimilarity::ward_initial_conversion(min);
        }
        else  {
            mindist[i] = min;
        }
        n_nghbr[i] = idx;
    }

    // Put the minimal distances into a heap structure to make the repeated
    // global minimum searches fast.
    nn_distances.heapify();

    // Main loop: We have N - 1 merging steps.
    for (i = 0; i < N_1; ++i) {
        idx1 = nn_distances.argmin();

        while (active_nodes.is_inactive(n_nghbr[idx1])) {
            // Recompute the minimum mindist[idx1] and n_nghbr[idx1].
            // exists, maximally N-1
            n_nghbr[idx1] = j = active_nodes.succ[idx1];
            if constexpr (method == method_codes_vector::METHOD_VECTOR_WARD)  {
                min = dist.ward(idx1, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    const double    tmp = dist.ward(idx1, j);

                    if (tmp < min) {
                        min = tmp;
                        n_nghbr[idx1] = j;
                    }
                }
            }
            else  {
                min = dist.sqeuclidean(idx1, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    const double    tmp = dist.sqeuclidean(idx1, j);

                    if (tmp < min) {
                        min = tmp;
                        n_nghbr[idx1] = j;
                    }
                }
            }
            /* Update the heap with the new true minimum and search
               for the (possibly different) minimal entry. */
            nn_distances.update_geq(idx1, min);
            idx1 = nn_distances.argmin();
        }

        nn_distances.heap_pop(); // Remove the current minimum from the heap.
        idx2 = n_nghbr[idx1];

        // Write the newly found minimal pair of nodes to the output array.
        node1 = row_repr[idx1];
        node2 = row_repr[idx2];

        Z2.append(node1, node2, mindist[idx1]);

        if constexpr (method == method_codes_vector::METHOD_VECTOR_WARD ||
                      method == method_codes_vector::METHOD_VECTOR_CENTROID)  {
            dist.merge_inplace(idx1, idx2);
        }
        else if constexpr (method ==
                               method_codes_vector::METHOD_VECTOR_MEDIAN)  {
            dist.merge_inplace_weighted(idx1, idx2);
        }
        else  {
            throw std::runtime_error("generic_linkage_vector(): "
                                     "Invalid method.");
        }

        // Index idx2 now represents the new (merged) node with label N+i.
        row_repr[idx2] = N + i;
        // Remove idx1 from the list of active indices (active_nodes).
        active_nodes.remove(idx1);  // TBD later!!!

        // Update the distance matrix
        if constexpr (method == method_codes_vector::METHOD_VECTOR_WARD)  {
            /*
              Ward linkage.
              Shorter and longer distances can occur, not smaller
              than min(d1, d2) but maybe bigger than max(d1, d2).
            */
            // Update the distance matrix in the range [start, idx1).
            for (j = active_nodes.start; j < idx1; j = active_nodes.succ[j]) {
                if (n_nghbr[j] == idx2) {
                    n_nghbr[j] = idx1; // invalidate
                }
            }
            // Update the distance matrix in the range (idx1, idx2).
            for (; j < idx2; j = active_nodes.succ[j]) {
                const double    tmp = dist.ward(j, idx2);

                if (tmp < mindist[j]) {
                    nn_distances.update_leq(j, tmp);
                    n_nghbr[j] = idx2;
                }
                else if (n_nghbr[j] == idx2) {
                    n_nghbr[j] = idx1; // invalidate
                }
            }
            // Find the nearest neighbor for idx2.
            if (idx2 < N_1) {
                // exists, maximally N - 1
                n_nghbr[idx2] = j = active_nodes.succ[idx2];
                min = dist.ward(idx2, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    const double    tmp = dist.ward(idx2, j);

                    if (tmp < min) {
                        min = tmp;
                        n_nghbr[idx2] = j;
                    }
                }
                nn_distances.update(idx2, min);
            }
        }
        else  {
            /*
              Centroid and median linkage.
              Shorter and longer distances can occur, not bigger
              than max(d1, d2) but maybe smaller than min(d1, d2).
            */
            for (j = active_nodes.start; j < idx2; j = active_nodes.succ[j]) {
                const double    tmp = dist.sqeuclidean(j, idx2);

                if (tmp < mindist[j]) {
                    nn_distances.update_leq(j, tmp);
                    n_nghbr[j] = idx2;
                }
                else if (n_nghbr[j] == idx2)
                    n_nghbr[j] = idx1; // invalidate
            }
            // Find the nearest neighbor for idx2.
            if (idx2 < N_1) {
                // exists, maximally N-1
                n_nghbr[idx2] = j = active_nodes.succ[idx2];
                min = dist.sqeuclidean(idx2, j);
                for (j = active_nodes.succ[j]; j < N;
                     j = active_nodes.succ[j]) {
                    const double    tmp = dist.sqeuclidean(idx2, j);

                    if (tmp < min) {
                        min = tmp;
                        n_nghbr[idx2] = j;
                    }
                }
                nn_distances.update(idx2, min);
            }
        }
    }
}

// ----------------------------------------------------------------------------

template <method_codes_vector method>
static void generic_linkage_vector_alternative(std::size_t N,
                                               Dissimilarity &dist,
                                               ClusterResult &Z2) {

  /*
    N: integer, number of data points
    dist: function pointer to the metric
    Z2: output data structure

    This algorithm is valid for the distance update methods
    "Ward", "centroid" and "median" only!
  */

    const std::size_t           N_1 = N - 1;
    std::size_t                 i, j = 0; // loop variables
    std::size_t                 idx1, idx2; // row and column indices
    std::vector<std::size_t>    n_nghbr(2 * N - 2); // nearest neighbors
    // distances to the nearest neighbors
    std::vector<double>         mindist(2 * N - 2);
    DLinkedList                 active_nodes(N + N_1);
    // minimum heap structure for the distance to the nearest neighbor of
    // each point
    BinaryMinHeap               nn_distances(mindist.data(), N_1, 2 * N - 2, 1);
    double                      min; // minimum for nearest-neighbor searches

    // Initialize the minimal distances:
    // Find the nearest neighbor of each point.
    // n_nghbr[i] = argmin_{j > i} D(i, j) for i in range(N-1)
    for (i = 1; i < N; ++i) {
        std::size_t idx;

        min = std::numeric_limits<double>::infinity();
        for (idx = j = 0; j < i; ++j) {
            const double   tmp =
                method == method_codes_vector::METHOD_VECTOR_WARD
                    ? dist.ward_initial(i, j) : dist.sqeuclidean(i, j);

            if (tmp < min) {
                min = tmp;
                idx = j;
            }
        }

        mindist[i] = method == method_codes_vector::METHOD_VECTOR_WARD
            ? Dissimilarity::ward_initial_conversion(min) : min;
        n_nghbr[i] = idx;
    }

    // Put the minimal distances into a heap structure to make the repeated
    // global minimum searches fast.
    nn_distances.heapify();

    // Main loop: We have N - 1 merging steps.
    /*
      The bookkeeping is different from the "stored matrix approach" algorithm
      generic_linkage.

      mindist[i] stores a lower bound on the minimum distance of the point i to
      all points of *lower* index:

          mindist[i] ≥ min_{j < i} D(i, j)

      Moreover, new nodes do not re-use one of the old indices, but they are
      given a new, unique index (SciPy convention: initial nodes are 0,…, N−1,
      new nodes are N, ... , 2N − 2).

      Invalid nearest neighbors are not recognized by the fact that the stored
      distance is smaller than the actual distance, but the list active_nodes
      maintains a flag whether a node is inactive. If n_nghbr[i] points to an
      active node, the entries nn_distances[i] and n_nghbr[i] are valid,
      otherwise they must be recomputed.
    */
    for (i = N; i < N + N_1; ++i) {
        idx1 = nn_distances.argmin();
        while (active_nodes.is_inactive(n_nghbr[idx1])) {
            // Recompute the minimum mindist[idx1] and n_nghbr[idx1].
            n_nghbr[idx1] = j = active_nodes.start;

            if constexpr (method == method_codes_vector::METHOD_VECTOR_WARD)  {
                min = dist.ward_extended(idx1, j);
                for (j = active_nodes.succ[j]; j < idx1;
                     j = active_nodes.succ[j]) {
                    const double    tmp = dist.ward_extended(idx1, j);

                    if (tmp < min) {
                        min = tmp;
                        n_nghbr[idx1] = j;
                    }
                }
            }
            else  {
                min = dist.sqeuclidean_extended(idx1, j);
                for (j = active_nodes.succ[j]; j < idx1;
                     j = active_nodes.succ[j]) {
                    const double    tmp = dist.sqeuclidean_extended(idx1, j);

                    if (tmp < min) {
                        min = tmp;
                        n_nghbr[idx1] = j;
                    }
                }
            }
            /* Update the heap with the new true minimum and search
               for the (possibly different) minimal entry. */
            nn_distances.update_geq(idx1, min);
            idx1 = nn_distances.argmin();
        }

        idx2 = n_nghbr[idx1];
        active_nodes.remove(idx1);
        active_nodes.remove(idx2);

        Z2.append(idx1, idx2, mindist[idx1]);

        if (i < (2 * N_1)) {
            if constexpr (method == method_codes_vector::METHOD_VECTOR_WARD ||
                          method ==
                              method_codes_vector::METHOD_VECTOR_CENTROID)  {
                dist.merge(idx1, idx2, i);
            }
            else if constexpr (method ==
                                   method_codes_vector::METHOD_VECTOR_MEDIAN)  {
                dist.merge_weighted(idx1, idx2, i);
            }
            else  {
                throw std::runtime_error(
                    "generic_linkage_vector_alternative(): Invalid method.");
            }

            n_nghbr[i] = active_nodes.start;
            if constexpr (method == method_codes_vector::METHOD_VECTOR_WARD) {
                /*
                  Ward linkage.
                  Shorter and longer distances can occur, not smaller
                  than min(d1, d2) but maybe bigger than max(d1, d2).
                */
                min = dist.ward_extended(active_nodes.start, i);
                for (j = active_nodes.succ[active_nodes.start]; j < i;
                     j = active_nodes.succ[j]) {
                    const double    tmp = dist.ward_extended(j, i);

                    if (tmp < min) {
                        min = tmp;
                        n_nghbr[i] = j;
                    }
                }
            }
            else {
                /*
                  Centroid and median linkage.
                  Shorter and longer distances can occur, not
                  bigger than max(d1, d2) but maybe smaller than min(d1, d2).
                */
                min = dist.sqeuclidean_extended(active_nodes.start, i);
                for (j = active_nodes.succ[active_nodes.start]; j < i;
                     j = active_nodes.succ[j]) {
                    const double    tmp = dist.sqeuclidean_extended(j, i);

                    if (tmp < min) {
                        min = tmp;
                        n_nghbr[i] = j;
                    }
                }
            }

            if (idx2 < active_nodes.start)
                nn_distances.remove(active_nodes.start);
            else
                nn_distances.remove(idx2);
            nn_distances.replace(idx1, i, min);
        }
    }
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
