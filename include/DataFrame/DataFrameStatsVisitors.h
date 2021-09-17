// Hossein Moein
// September 22, 2017
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

#include <DataFrame/DataFrameTypes.h>
#include <DataFrame/Internals/DataFrame_standalone.tcc>
#include <DataFrame/Utils/FixedSizePriorityQueue.h>
#include <DataFrame/Utils/ThreadGranularity.h>
#include <DataFrame/Utils/Utils.h>

#include <algorithm>
#include <cassert>

#include <cmath>
#ifndef M_PI
#  define M_PI 3.14159265358979323846264338327950288
#endif

#include <cstddef>
#include <functional>
#include <future>
#include <iterator>
#include <map>
#include <numeric>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define DEFINE_VISIT_BASIC_TYPES \
    using value_type = T; \
    using index_type = I; \
    using size_type = std::size_t;

#define DEFINE_VISIT_BASIC_TYPES_2 \
    DEFINE_VISIT_BASIC_TYPES \
    using result_type = T;

#define DEFINE_VISIT_BASIC_TYPES_3 \
    DEFINE_VISIT_BASIC_TYPES \
    using result_type = std::vector<T>;

#define DEFINE_PRE_POST \
    inline void pre ()  { result_.clear(); } \
    inline void post ()  {  }

#define DEFINE_RESULT \
    inline const result_type &get_result () const  { return (result_); } \
    inline result_type &get_result ()  { return (result_); }

#define PASS_DATA_ONE_BY_ONE \
    template <typename K, typename H> \
    inline void \
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  { \
\
        while (column_begin < column_end) \
            (*this)(*idx_begin, *column_begin++); \
    }

#define PASS_DATA_ONE_BY_ONE_2 \
    template <typename K, typename H> \
    inline void \
    operator() (K idx_begin, K idx_end, \
                H column_begin1, H column_end1, \
                H column_begin2, H column_end2)  { \
\
        while (column_begin1 < column_end1 && column_begin2 < column_end2) \
            (*this)(*idx_begin, *column_begin1++, *column_begin2++); \
    }

#define DECL_CTOR(name) \
    explicit \
    name(bool skipnan = true) : skip_nan_(skipnan)  {   }

#define SKIP_NAN if (skip_nan_ && is_nan__(val))  { return; }
#define SKIP_NAN_BASE if (BaseClass::skip_nan_ && is_nan__(val))  { return; }

#define GET_COL_SIZE \
    const std::size_t   col_s = \
        std::min(std::distance(idx_begin, idx_end), \
                 std::distance(column_begin, column_end));

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct LastVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void
    operator() (const index_type &, const value_type &val)  {

        if (! skip_nan_ || ! is_nan__(val))  result_ = val;
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { result_ = result_type { }; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    DECL_CTOR(LastVisitor)

private:

    result_type result_ {  };
    const bool  skip_nan_;
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct FirstVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void
    operator() (const index_type &, const value_type &val)  {

        if (! started_)  {
            if (! skip_nan_ || ! is_nan__(val))  {
                result_ = val;
                started_ = true;
            }
        }
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { result_ = result_type { }; started_ = false; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    DECL_CTOR(FirstVisitor)

private:

    result_type result_ {  };
    bool        started_ { false };
    const bool  skip_nan_;
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct CountVisitor {

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::size_t;

    inline void operator() (const index_type &, const value_type &val)  {

        if (! skip_nan_ || ! is_nan__(val))  result_ += 1;
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    DECL_CTOR(CountVisitor)

private:

    result_type result_ { 0 };
    const bool  skip_nan_;
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct SumVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &, const value_type &val)  {

        SKIP_NAN

        result_ += val;
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { result_ = value_type { }; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    DECL_CTOR(SumVisitor)

private:

    value_type  result_ { 0 };
    const bool  skip_nan_;
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MeanBase {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void pre ()  { sum_.pre(); mean_ = 0; cnt_ = 0; }
    inline size_type get_count () const  { return (cnt_); }
    inline value_type get_sum () const  { return (sum_.get_result()); }
    inline result_type get_result () const  { return (mean_); }

    DECL_CTOR(MeanBase)

protected:

    const bool          skip_nan_;
    value_type          mean_ { 0 };
    size_type           cnt_ { 0 };
    SumVisitor<T, I>    sum_ { skip_nan_ };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MeanVisitor : public MeanBase<T, I>  {

    using BaseClass = MeanBase<T, I>;

    inline void operator() (const I &idx, const T &val)  {

        SKIP_NAN_BASE

        BaseClass::cnt_ += 1;
        BaseClass::sum_(idx, val);
    }
    PASS_DATA_ONE_BY_ONE

    inline void post ()  {

        BaseClass::sum_.post();
        BaseClass::mean_ = BaseClass::sum_.get_result() / T(BaseClass::cnt_);
    }

    MeanVisitor(bool skipnan = true) : BaseClass(skipnan)  {   }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct WeightedMeanVisitor : public MeanBase<T, I>  {

    using BaseClass = MeanBase<T, I>;

    inline void operator() (const I &idx, const T &val)  {

        SKIP_NAN_BASE

        BaseClass::cnt_ += 1;
        BaseClass::sum_(idx, val * T(BaseClass::cnt_));
    }
    PASS_DATA_ONE_BY_ONE

    inline void
    post() {

        BaseClass::sum_.post();
        BaseClass::mean_ =
            BaseClass::sum_.get_result() /
            (T((BaseClass::cnt_ * (BaseClass::cnt_ + 1))) / T(2));
    }

    WeightedMeanVisitor(bool skipnan = true) : BaseClass(skipnan)  {   }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct GeometricMeanVisitor : public MeanBase<T, I>  {

    using BaseClass = MeanBase<T, I>;

    inline void operator() (const I &idx, const T &val)  {

        SKIP_NAN_BASE

        BaseClass::cnt_ += 1;
        BaseClass::sum_(idx, std::log(val));
    }
    PASS_DATA_ONE_BY_ONE

    inline void post ()  {

        BaseClass::sum_.post();
        BaseClass::mean_ =
            std::exp(BaseClass::sum_.get_result() / T(BaseClass::cnt_));
    }

    GeometricMeanVisitor(bool skipnan = true) : BaseClass(skipnan)  {   }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct HarmonicMeanVisitor : public MeanBase<T, I>  {

    using BaseClass = MeanBase<T, I>;

    inline void operator() (const I &idx, const T &val)  {

        SKIP_NAN_BASE

        BaseClass::cnt_ += 1;
        BaseClass::sum_(idx, one_ / val);
    }
    PASS_DATA_ONE_BY_ONE

    inline void post ()  {

        BaseClass::sum_.post();
        BaseClass::mean_ = T(BaseClass::cnt_) / BaseClass::sum_.get_result();
    }

    HarmonicMeanVisitor(bool skipnan = true) : BaseClass(skipnan)  {   }

private:

    static constexpr T  one_ { 1 };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct QuadraticMeanVisitor : public MeanBase<T, I>  {

    using BaseClass = MeanBase<T, I>;

    inline void operator() (const I &idx, const T &val)  {

        SKIP_NAN_BASE

        BaseClass::cnt_ += 1;
        BaseClass::sum_(idx, val * val);
    }
    PASS_DATA_ONE_BY_ONE

    inline void post() {

        BaseClass::sum_.post();
        BaseClass::mean_ =
            std::sqrt(BaseClass::sum_.get_result() / T(BaseClass::cnt_));
    }

    QuadraticMeanVisitor(bool skipnan = true) : BaseClass(skipnan)  {   }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct ProdVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &, const value_type &val)  {

        SKIP_NAN

        result_ *= val;
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { result_ = 1; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    DECL_CTOR(ProdVisitor)

private:

    value_type  result_ { 1 };
    const bool  skip_nan_;
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, typename Cmp = std::less<T>>
struct ExtremumVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    using compare_type = Cmp;

    inline void operator() (const index_type &idx, const value_type &val)  {

        counter_ += 1;
        if (is_nan__(val))  {
            if (skip_nan_)  return;
            else  {
                extremum_ = std::numeric_limits<value_type>::quiet_NaN();
                is_first = false;
            }
        }

        if (cmp_(extremum_, val) || is_first) {
            extremum_ = val;
            index_ = idx;
            pos_ = counter_;
            is_first = false;
        }
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { is_first = true; pos_ = 0; counter_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (extremum_); }
    inline index_type get_index () const  { return (index_); }
    inline size_type get_position () const  { return (pos_); }

    DECL_CTOR(ExtremumVisitor)

private:

    value_type      extremum_ { };
    index_type      index_ { };
    bool            is_first { true };
    size_type       pos_ { 0 };
    size_type       counter_ { 0 };
    compare_type    cmp_ {  };
    const bool      skip_nan_;
};

template<typename T, typename I = unsigned long>
using MaxVisitor = ExtremumVisitor<T, I, std::less<T>>;
template<typename T, typename I = unsigned long>
using MinVisitor = ExtremumVisitor<T, I, std::greater<T>>;

// ----------------------------------------------------------------------------

// This visitor takes, at most (when sequance is already sorted), O(N*M)
// time, where N is the number of largest values and M is the total number
// of all values. The assumption is that N should be relatively small, so the
// complexity is not bad.
// I could have used a priority queue, but that requires a std::vector
// instaed of std::array. I think the advantage of using std::array is bigger
// than O(MlogM) vs. O(N*M) for majority of usage.
// By default, this is a NLargestVisitor
//
template<std::size_t N, typename T, typename I = unsigned long,
         typename Cmp = std::less<T>>
struct  NExtremumVisitor {

    DEFINE_VISIT_BASIC_TYPES

    struct  DataItem  {
        value_type  value { };
        index_type  index { };
    };

    using compare_type = Cmp;
    using result_type = std::array<DataItem, N>;

    inline void operator() (const index_type &idx, const value_type &val)  {

        SKIP_NAN

        if (counter_ < N)  {
            result_[counter_] = { val, idx };
            if (extremum_index_ < 0 ||
                cmp_(val, result_[extremum_index_].value))
                extremum_index_ = static_cast<int>(counter_);
        }
        else if (cmp_(result_[extremum_index_].value, val))  {
            result_[extremum_index_] = { val, idx };
            extremum_index_ = 0;
            for (size_type i = 1; i < N; ++i)
                if (cmp_(result_[i].value, result_[extremum_index_].value))
                    extremum_index_ = static_cast<int>(i);
        }

        counter_ += 1;
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { counter_ = 0; extremum_index_ = -1; }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

    inline void sort_by_index()  {

        std::sort(result_.begin(), result_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.index < rhs.index);
                  });
    }
    inline void sort_by_value()  {

        std::sort(result_.begin(), result_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.value < rhs.value);
                  });
    }

    DECL_CTOR(NExtremumVisitor)

private:

    result_type     result_ { };
    size_type       counter_ { 0 };
    int             extremum_index_ { -1 };
    compare_type    cmp_ {  };
    const bool      skip_nan_;
};

template<std::size_t N, typename T, typename I = unsigned long>
using NLargestVisitor = NExtremumVisitor<N, T, I, std::less<T>>;
template<std::size_t N, typename T, typename I = unsigned long>
using NSmallestVisitor = NExtremumVisitor<N, T, I, std::greater<T>>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CovVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &,
                            const value_type &val1, const value_type &val2)  {

        if (skip_nan_ && (is_nan__(val1) || is_nan__(val2)))  return;

        total1_ += val1;
        total2_ += val2;
        dot_prod_ += (val1 * val2);
        dot_prod1_ += (val1 * val1);
        dot_prod2_ += (val2 * val2);
        cnt_ += 1;
    }
    PASS_DATA_ONE_BY_ONE_2

    inline void pre ()  {

        total1_ = total2_ = dot_prod_ = dot_prod1_ = dot_prod2_ = result_ = 0;
        cnt_ = 0;
    }
    inline void post ()  {

        result_ = (dot_prod_ - (total1_ * total2_) / value_type(cnt_)) /
                  (value_type(cnt_) - b_);
    }

    inline result_type get_result () const  { return (result_); }
    inline value_type get_var1 () const  {

        return ((dot_prod1_ - (total1_ * total1_) / value_type(cnt_)) /
                (value_type(cnt_) - b_));
    }
    inline value_type get_var2 () const  {

        return ((dot_prod2_ - (total2_ * total2_) / value_type(cnt_)) /
                (value_type(cnt_) - b_));
    }
    inline size_type get_count() const  { return (cnt_); }

    explicit CovVisitor (bool biased = false, bool skipnan = true)
        : b_ (biased ? 0 : 1), skip_nan_(skipnan) {  }

private:

    value_type          total1_ { 0 };
    value_type          total2_ { 0 };
    value_type          dot_prod_ { 0 };
    value_type          dot_prod1_ { 0 };
    value_type          dot_prod2_ { 0 };
    result_type         result_ { 0 };
    size_type           cnt_ { 0 };
    const value_type    b_;
    const bool          skip_nan_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct VarVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx, const value_type &val)  {

        cov_ (idx, val, val);
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline result_type get_result () const  { return (cov_.get_result()); }
    inline size_type get_count() const  { return (cov_.get_count()); }

    explicit VarVisitor (bool biased = false) : cov_ (biased)  {   }

private:

    CovVisitor<value_type, index_type>  cov_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct BetaVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx,
                            const value_type &val, const value_type &bmark)  {

        cov_ (idx, val, bmark);
    }
    PASS_DATA_ONE_BY_ONE_2

    inline void pre ()  { cov_.pre(); result_ = 0; }
    inline void post ()  {

        cov_.post();

        const value_type    v = cov_.get_var2();

        result_ = v != 0.0
                      ? cov_.get_result() / v
                      : std::numeric_limits<value_type>::quiet_NaN();
    }
    inline result_type get_result () const  { return (result_); }
    inline size_type get_count() const  { return (cov_.get_count()); }

    explicit BetaVisitor (bool biased = false) : cov_ (biased)  {   }

private:

    CovVisitor<value_type, index_type>  cov_;
    result_type                         result_ { 0 };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StdVisitor   {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx, const value_type &val)  {

        var_ (idx, val);
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { var_.pre(); result_ = 0; }
    inline void post ()  { var_.post(); result_ = ::sqrt(var_.get_result()); }
    inline result_type get_result () const  { return (result_); }
    inline size_type get_count() const  { return (var_.get_count()); }

    explicit StdVisitor (bool biased = false) : var_ (biased)  {   }

private:

    VarVisitor<value_type, index_type>  var_;
    result_type                         result_ { 0 };
};

// ----------------------------------------------------------------------------

// Standard Error of the Mean
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SEMVisitor   {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx, const value_type &val)  {

        std_ (idx, val);
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  { std_.pre(); result_ = 0; }
    inline void post ()  {

        std_.post();
        result_ = std_.get_result() / ::sqrt(get_count());
    }
    inline result_type get_result () const  { return (result_); }
    inline size_type get_count() const  { return (std_.get_count()); }

    explicit SEMVisitor (bool biased = false) : std_ (biased)  {   }

private:

    StdVisitor<value_type, index_type>  std_;
    result_type                         result_ { 0 };
};

template<typename T, typename I = unsigned long>
using sem_v = SEMVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct TrackingErrorVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx,
                            const value_type &val1, const value_type &val2)  {

        std_ (idx, val1 - val2);
    }
    PASS_DATA_ONE_BY_ONE_2

    inline void pre ()  { std_.pre(); }
    inline void post ()  { std_.post();  }
    inline result_type get_result () const  { return (std_.get_result()); }

    explicit TrackingErrorVisitor (bool biased = false) : std_ (biased) {  }

private:

    StdVisitor<value_type, index_type>  std_;
};

template<typename T, typename I = unsigned long>
using te_v = TrackingErrorVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CorrVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx,
                            const value_type &val1, const value_type &val2)  {

        cov_ (idx, val1, val2);
    }
    PASS_DATA_ONE_BY_ONE_2

    inline void pre ()  { cov_.pre(); result_ = 0; }
    inline void post ()  {

        cov_.post();
        result_ = cov_.get_result() /
                  (::sqrt(cov_.get_var1()) * ::sqrt(cov_.get_var2()));
    }
    inline result_type get_result () const  { return (result_); }

    explicit CorrVisitor (bool biased = false) : cov_ (biased)  {   }

private:

    CovVisitor<value_type, index_type>  cov_;
    result_type                         result_ { 0 };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DotProdVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &,
                            const value_type &val1, const value_type &val2)  {

        result_ += (val1 * val2);
    }
    PASS_DATA_ONE_BY_ONE_2

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

private:

    result_type result_ { 0 };  // Dot product
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, typename Cmp = std::less<T>>
struct ExtremumSubArrayVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    using compare_type = Cmp;

    inline void operator() (const index_type &, const value_type &val)  {

        if (val >= min_to_consider_ && val <= max_to_consider_)  {
            const value_type    current_plus_val = current_sum_ + val;

            if (cmp_(current_plus_val, val))  {
                // Start a new sequence at the current element
                current_begin_idx_ = current_end_idx_;
                current_sum_ = val;
            }
            else // Extend the existing sequence with the current element
                current_sum_ = current_plus_val;

            if (cmp_(best_sum_, current_sum_))  {
                best_sum_ = current_sum_;
                best_begin_idx_ = current_begin_idx_;
                best_end_idx_ = current_end_idx_ + 1; // Make end_idx exclusive
            }
        }
        current_end_idx_ += 1;
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  {

        best_sum_ = cmp_(-std::numeric_limits<value_type>::max(),
                         std::numeric_limits<value_type>::max())
                        ? -std::numeric_limits<value_type>::max()
                        : std::numeric_limits<value_type>::max();
        best_begin_idx_ = 0;
        best_end_idx_ = 0;
        current_begin_idx_ = 0;
        current_end_idx_ = 0;
        current_sum_ = cmp_(-std::numeric_limits<value_type>::max(),
                            std::numeric_limits<value_type>::max())
                           ? -std::numeric_limits<value_type>::max()
                           : std::numeric_limits<value_type>::max();
    }
    inline void post ()  {  }
    inline result_type get_result () const  { return (best_sum_); }
    inline size_type get_begin_idx () const  { return (best_begin_idx_); }
    inline size_type get_end_idx () const  { return (best_end_idx_); }

    explicit ExtremumSubArrayVisitor(
        value_type min_to_consider = -std::numeric_limits<value_type>::max(),
        value_type max_to_consider = std::numeric_limits<value_type>::max())
        : min_to_consider_(min_to_consider),
          max_to_consider_(max_to_consider)  {   }

private:

    const value_type    min_to_consider_;
    const value_type    max_to_consider_;
    compare_type        cmp_ {  };
    result_type         best_sum_ {
        cmp_(-std::numeric_limits<value_type>::max(),
             std::numeric_limits<value_type>::max())
            ? -std::numeric_limits<value_type>::max()
            : std::numeric_limits<value_type>::max()
    };
    size_type           best_begin_idx_ { 0 };
    size_type           best_end_idx_ { 0 };
    size_type           current_begin_idx_ { 0 };
    size_type           current_end_idx_ { 0 };
    result_type         current_sum_ {
        cmp_(-std::numeric_limits<value_type>::max(),
             std::numeric_limits<value_type>::max())
            ? -std::numeric_limits<value_type>::max()
            : std::numeric_limits<value_type>::max()
    };
};

template<typename T, typename I = unsigned long>
using MaxSubArrayVisitor = ExtremumSubArrayVisitor<T, I, std::less<T>>;
template<typename T, typename I = unsigned long>
using MinSubArrayVisitor = ExtremumSubArrayVisitor<T, I, std::greater<T>>;

// ----------------------------------------------------------------------------

template<std::size_t N, typename T, typename I = unsigned long,
         typename Cmp = std::less<T>>
struct NExtremumSubArrayVisitor  {

    DEFINE_VISIT_BASIC_TYPES

    struct  SubArrayInfo  {

        value_type  sum { };
        size_type   begin_index { 0 };
        size_type   end_index { 0 };

        friend inline bool
        operator < (const SubArrayInfo &lhs, const SubArrayInfo &rhs)  {

            return (lhs.sum < rhs.sum);
        }
        friend inline bool
        operator > (const SubArrayInfo &lhs, const SubArrayInfo &rhs)  {

            return (lhs.sum > rhs.sum);
        }
    };

    using result_type = std::vector<SubArrayInfo>;
    using compare_type = Cmp;

    inline void operator() (const index_type &idx, const value_type &val)  {

        const value_type    prev_sum = extremum_sub_array_.get_result();

        extremum_sub_array_(idx, val);
        if (cmp_(prev_sum, extremum_sub_array_.get_result()))
            q_.push(SubArrayInfo { extremum_sub_array_.get_result(),
                                   extremum_sub_array_.get_begin_idx(),
                                   extremum_sub_array_.get_end_idx() });
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  {

        extremum_sub_array_.pre();
        q_.clear();
        result_.clear();
    }
    inline void post ()  {

        extremum_sub_array_.post();
        result_ = std::move(q_.data());
    }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

    explicit NExtremumSubArrayVisitor(
        value_type min_to_consider = -std::numeric_limits<value_type>::max(),
        value_type max_to_consider = std::numeric_limits<value_type>::max())
        : extremum_sub_array_(min_to_consider, max_to_consider)  {   }

private:

    ExtremumSubArrayVisitor<T, I, Cmp>                      extremum_sub_array_;
    FixedSizePriorityQueue<
        SubArrayInfo, N,
        typename template_switch<SubArrayInfo, Cmp>::type>  q_ {  };
    result_type                                             result_ {  };
    compare_type                                            cmp_ {  };
};

template<std::size_t N, typename T, typename I = unsigned long>
using NMaxSubArrayVisitor = NExtremumSubArrayVisitor<N, T, I, std::less<T>>;
template<std::size_t N, typename T, typename I = unsigned long>
using NMinSubArrayVisitor = NExtremumSubArrayVisitor<N, T, I, std::greater<T>>;

// ----------------------------------------------------------------------------

// Simple rolling adoptor for visitors
//
template<typename F, typename T, typename I = unsigned long>
struct  SimpleRollAdopter  {

private:

    using visitor_type = F;
    using f_result_type = typename visitor_type::result_type;

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<f_result_type>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        assert(roll_count_ != 0);

        GET_COL_SIZE

        result_.reserve(col_s);

        for (size_type i = 0; i < roll_count_ - 1 && i < col_s; ++i)
            result_.push_back(std::numeric_limits<f_result_type>::quiet_NaN());
        for (size_type i = 0; i < col_s; ++i)  {
            if (i + roll_count_ <= col_s)  {
                visitor_.pre();
                visitor_(idx_begin + i, idx_begin + (i + roll_count_),
                         column_begin + i, column_begin + (i + roll_count_));
                visitor_.post();
                result_.push_back(visitor_.get_result());
            }
            else  break;
        }
    }

    inline void pre ()  { visitor_.pre(); result_.clear(); }
    inline void post ()  { visitor_.post(); }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

    SimpleRollAdopter(F &&functor, size_type r_count)
        : visitor_(std::move(functor)), roll_count_(r_count)  {   }

private:

    visitor_type                visitor_ { };
    const size_type             roll_count_ { 0 };
    std::vector<f_result_type>  result_ { };
};

// ----------------------------------------------------------------------------

// This can only work with visitors that accept one data item at a time
//
template<typename F, typename T, typename I = unsigned long>
struct  StepRollAdopter  {

private:

    using visitor_type = F;

    visitor_type        visitor_ {  };
    const std::size_t   period_;

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = typename visitor_type::result_type;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (period_ == 0)  return;

        GET_COL_SIZE

        for (size_type i = 0; i < col_s; ++i)  {
            const size_type idx = i * period_;

            if (idx < col_s)  visitor_(idx_begin[idx], column_begin[idx]);
            else  break;
        }
    }

    inline void pre ()  { visitor_.pre(); }
    inline void post ()  { visitor_.post(); }
    inline const result_type &
    get_result () const  { return (visitor_.get_result()); }
    inline result_type get_result ()  { return (visitor_.get_result()); }

    StepRollAdopter(F &&functor, size_type period)
        : visitor_(std::move(functor)), period_(period)  {   }
};

// ----------------------------------------------------------------------------

// Expanding rolling adoptor for visitors
//
template<typename F, typename T, typename I = unsigned long>
struct  ExpandingRollAdopter  {

private:

    using visitor_type = F;
    using f_result_type = typename visitor_type::result_type;

    visitor_type                visitor_ { };
    const std::size_t           init_roll_count_ { 0 };
    const std::size_t           increment_count_ { 0 };
    std::vector<f_result_type>  result_ { };

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<f_result_type>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        assert(init_roll_count_ != 0);

        GET_COL_SIZE

        result_.reserve(col_s);

        std::size_t rc = init_roll_count_;

        for (std::size_t i = 0; i < rc - 1 && i < col_s; ++i)
            result_.push_back(std::numeric_limits<f_result_type>::quiet_NaN());

        for (std::size_t i = 0; i < col_s; ++i, rc += increment_count_)  {
            std::size_t r = 0;

            visitor_.pre();
            for (std::size_t j = i; r < rc && j < col_s; ++j, ++r)
                visitor_(*(idx_begin + j), *(column_begin + j));
            visitor_.post();
            if (r == rc)
                result_.push_back(visitor_.get_result());
            else  break;
        }
    }

    inline void pre ()  { visitor_.pre(); result_.clear(); }
    inline void post ()  { visitor_.post(); }
    DEFINE_RESULT

    ExpandingRollAdopter(F &&functor,
                         std::size_t r_count,
                         std::size_t i_count = 1)
        : visitor_(std::move(functor)),
          init_roll_count_(r_count),
          increment_count_(i_count)  {   }
};

// ----------------------------------------------------------------------------

// Exponential rolling adoptor for visitors
// (decay * Xt) + ((1 − decay) * AVGt-1)
//
template<typename F, typename T, typename I = unsigned long>
struct  ExponentialRollAdopter  {

private:

    using visitor_type = F;
    using f_result_type = typename visitor_type::result_type;

    std::vector<f_result_type>  result_ { };
    visitor_type                visitor_ { };
    const std::size_t           roll_count_;
    const std::size_t           repeat_count_;
    const T                     decay_;
    const bool                  skip_nan_;

public:

    DEFINE_VISIT_BASIC_TYPES

    using result_type = std::vector<f_result_type>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        assert(roll_count_ != 0 && roll_count_ < col_s);

        result_.resize(col_s, std::numeric_limits<f_result_type>::quiet_NaN());

        std::size_t i = 0;

        visitor_.pre();
        for (; i < roll_count_; ++i)
            visitor_(*(idx_begin + i), *(column_begin + i));
        visitor_.post();
        result_[i - 1] = visitor_.get_result();

        for (; i < col_s; ++i)  {
            if (skip_nan_ && is_nan__(*(column_begin + i)))  continue;
            result_[i] = calc_value_(*(column_begin + i), result_[i - 1]);
        }
        for (i = 1; i < repeat_count_; ++i)
            for (std::size_t j = roll_count_; j < col_s; ++j)  {
                if (skip_nan_ && is_nan__(result_[i]))  continue;
                result_[j] = calc_value_(result_[j], result_[j - 1]);
            }
    }

    inline void pre ()  { visitor_.pre(); result_.clear(); }
    inline void post ()  { visitor_.post(); }
    DEFINE_RESULT

    ExponentialRollAdopter(F &&functor,
                           std::size_t r_count,
                           exponential_decay_spec eds,
                           double value,
                           std::size_t repeat_count = 1,
                           bool skip_nan = true)
        : visitor_(std::move(functor)),
          roll_count_(r_count),
          repeat_count_(repeat_count),
          decay_(eds == exponential_decay_spec::center_of_gravity
                     ? 1.0 / (1.0 + value)
                     : eds == exponential_decay_spec::span
                         ? 2.0 / (1.0 + value)
                         : eds == exponential_decay_spec::halflife
                             ? 1.0 - std::exp(std::log(0.5) / value)
                             : value),
          skip_nan_(skip_nan)  {   }

private:

    inline value_type calc_value_(value_type i_value, value_type i_1_value)  {

        return (decay_ * i_value + (T(1) - decay_) * i_1_value);
    }
};

// ----------------------------------------------------------------------------

// One-pass stats calculation.
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StatsVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &, const value_type &val)  {

        SKIP_NAN

        value_type  delta, delta_n, delta_n2, term1;
        size_type   n1 = n_;

        n_ += 1;
        delta = val - m1_;
        delta_n = delta / value_type(n_);
        delta_n2 = delta_n * delta_n;
        term1 = delta * delta_n * value_type(n1);
        m1_ += delta_n;
        m4_ = m4_ + static_cast<value_type>
                     (term1 * delta_n2 * value_type(n_ * n_ - 3 * n_ + 3) +
                   6.0 * delta_n2 * m2_ - 4.0 * delta_n * m3_);
        m3_ = m3_ + static_cast<value_type>
            (term1 * delta_n * value_type(n_ - 2) - 3.0 * delta_n * m2_);
        m2_ += term1;
    }
    PASS_DATA_ONE_BY_ONE

    inline void pre ()  {

        n_ = 0;
        m1_ = m2_ = m3_ = m4_ = 0;
    }
    inline void post ()  {  }

    inline size_type get_count () const { return (n_); }
    inline result_type get_mean () const  { return (m1_); }
    inline result_type get_variance () const  {

        return static_cast<result_type>(m2_ / (value_type(n_) - 1.0));
    }
    inline result_type get_std () const  {

        return static_cast<result_type>(::sqrt(get_variance()));
    }
    inline result_type get_skew () const  {

        return static_cast<result_type>(::sqrt(n_) * m3_ / ::pow(m2_, 1.5));
    }
    inline result_type get_kurtosis () const  {

        return static_cast<result_type>
            (value_type(n_) * m4_ / (m2_ * m2_) - 3.0);
    }

    DECL_CTOR(StatsVisitor)

private:

    size_type   n_ { 0 };
    value_type  m1_ { 0 };
    value_type  m2_ { 0 };
    value_type  m3_ { 0 };
    value_type  m4_ { 0 };
    const bool  skip_nan_;
};

// ----------------------------------------------------------------------------

// One pass simple linear regression
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SLRegressionVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx,
                            const value_type &x, const value_type &y)  {

        if (skip_nan_ && (is_nan__(x) || is_nan__(y)))  return;

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

    inline size_type get_count () const { return (n_); }
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

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct TTestVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx,
                            const value_type &x, const value_type &y)  {

        if (skip_nan_ && (is_nan__(x) || is_nan__(y)))  return;

        if (! related_ts_)  {
            m_x_(idx, x);
            m_y_(idx, y);
            v_x_(idx, x);
            v_y_(idx, y);
            deg_freedom_ += 2;
        }
        else  {
            m_x_(idx, x - y);
            v_x_(idx, x - y);
            deg_freedom_ += 1;
        }
    }
    PASS_DATA_ONE_BY_ONE_2

    inline void pre ()  {

        m_x_.pre();
        m_y_.pre();
        v_x_.pre();
        v_y_.pre();
        result_ = 0;
        deg_freedom_ = 0;
    }
    inline void post ()  {

        m_x_.post();
        m_y_.post();
        v_x_.post();
        v_y_.post();
        if (! related_ts_)  {
            result_ =
                (m_x_.get_result() - m_y_.get_result()) /
                std::sqrt(v_x_.get_result() / value_type(m_x_.get_count()) +
                          v_y_.get_result() / value_type(m_y_.get_count()));
            deg_freedom_ -= 2;
        }
        else  {
            result_ =
                m_x_.get_result() /
                (std::sqrt(v_x_.get_result()) /
                 std::sqrt(value_type(m_x_.get_count())));
            deg_freedom_ -= 1;
        }
    }

    inline result_type get_result () const  { return (result_); }
    inline size_type get_deg_freedom () const  { return (deg_freedom_); }

    explicit TTestVisitor(bool is_related_ts, bool skipnan = true)
        : related_ts_(is_related_ts), skip_nan_(skipnan)  {  }

private:

    MeanVisitor<T, I>   m_x_ {  };
    MeanVisitor<T, I>   m_y_ {  };
    VarVisitor<T, I>    v_x_ {  };
    VarVisitor<T, I>    v_y_ {  };
    result_type         result_ { 0 };
    size_type           deg_freedom_ { 0 };
    const bool          related_ts_;
    const bool          skip_nan_;
};

// ----------------------------------------------------------------------------

//
// Single Action Visitors
//

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CumSumVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        value_type      running_sum = 0;
        GET_COL_SIZE

        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (! skip_nan_ || ! is_nan__(value))  {
                running_sum += value;
                result.push_back(running_sum);
            }
            else
                result.push_back(value);
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    DECL_CTOR(CumSumVisitor)

private:

    result_type result_ {  };
    const bool  skip_nan_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CumProdVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        value_type      running_prod = 1;
        GET_COL_SIZE

        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (! skip_nan_ || ! is_nan__(value))  {
                running_prod *= value;
                result.push_back(running_prod);
            }
            else
                result.push_back(value);
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    DECL_CTOR(CumProdVisitor)

private:

    std::vector<value_type> result_ {  };
    const bool              skip_nan_;
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long, typename Cmp = std::less<T>>
struct CumExtremumVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    using compare_type = Cmp;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        if (col_s == 0)  return;

        value_type  running_extremum = *column_begin;
        result_type result;

        result.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (! skip_nan_ || ! is_nan__(value))  {
                if (cmp_(running_extremum, value))
                    running_extremum = value;
                result.push_back(running_extremum);
            }
            else
                result.push_back(value);
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    DECL_CTOR(CumExtremumVisitor)

private:

    result_type     result_ {  };  // Extremum
    compare_type    cmp_ {  };
    const bool      skip_nan_;
};

template<typename T, typename I = unsigned long>
using CumMaxVisitor = CumExtremumVisitor<T, I, std::less<T>>;
template<typename T, typename I = unsigned long>
using CumMinVisitor = CumExtremumVisitor<T, I, std::greater<T>>;

// ----------------------------------------------------------------------------

// This categorizes the values in the column with integer values starting
// from 0
//
template<typename T, typename I = unsigned long>
struct CategoryVisitor  {

private:

    struct t_less_  {
        inline bool
        operator() (const T *lhs, const T *rhs) const { return (*lhs < *rhs); }
    };

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<size_type>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        result_type result;

        result.reserve(col_s);

        size_type   cat = nan_ != 0 ? 0 : 1;

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (is_nan__(value))  {
                result.push_back(nan_);
                continue;
            }

            const typename map_type::const_iterator citer =
                cat_map_.find(&value);

            if (citer == cat_map_.end())  {
                cat_map_.insert({ &value, cat });
                result.push_back(cat);
                cat += 1;
                if (cat == nan_)  cat += 1;
            }
            else
                result.push_back(citer->second);
        }
        result_.swap(result);
    }

    inline void pre ()  { result_.clear(); cat_map_.clear(); }
    inline void post ()  {  }
    DEFINE_RESULT

    explicit
    CategoryVisitor(size_type nan_val = size_type(-1)) : nan_(nan_val)  {   }

private:

    using map_type = std::map<const T *, size_type, t_less_>;

    map_type        cat_map_ { };
    result_type     result_ { };
    const size_type nan_;
};

// ----------------------------------------------------------------------------

// This ranks the values in the column based on rank policy starting from 0.
//
template<typename T, typename I = unsigned long>
struct RankVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<double>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE
        std::vector<size_type>  rank_vec(col_s);

        std::iota(rank_vec.begin(), rank_vec.end(), 0);
        std::stable_sort(
            rank_vec.begin(), rank_vec.end(),
            [&column_begin](size_type lhs, size_type rhs) -> bool {
                return *(column_begin + lhs) < *(column_begin + rhs);
            });

        result_type         result(col_s);
        const value_type    *prev_value = &*(column_begin + rank_vec[0]);

        for (size_type i = 0; i < col_s; ++i)  {
            double      avg_val = static_cast<double>(i);
            double      first_val = static_cast<double>(i);
            double      last_val = static_cast<double>(i);
            size_type   j = i + 1;

            for ( ; j < col_s && *prev_value == *(column_begin + rank_vec[j]);
                 ++j)  {
                last_val = static_cast<double>(j);
                avg_val += static_cast<double>(j);
            }
            avg_val /= double(j - i);

            switch(policy_)  {
                case rank_policy::average:
                {
                    for (; i < col_s && i < j; ++i)
                        result[rank_vec[i]] = avg_val;
                    break;
                }
                case rank_policy::first:
                {
                    for (; i < col_s && i < j; ++i)
                        result[rank_vec[i]] = first_val;
                    break;
                }
                case rank_policy::last:
                {
                    for (; i < col_s && i < j; ++i)
                        result[rank_vec[i]] = last_val;
                    break;
                }
                case rank_policy::actual:
                {
                    for (; i < col_s && i < j; ++i)
                        result[rank_vec[i]] = static_cast<double>(i);
                    break;
                }
            }
            if (i < col_s)
                prev_value = &*(column_begin + rank_vec[i]);
            i -= 1;  // Because the outer loop does ++i
        }
        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    RankVisitor(rank_policy p = rank_policy::actual) : policy_(p)  {   }

private:

    const rank_policy   policy_;
    result_type         result_ { };
};

// ----------------------------------------------------------------------------

// It factorizes the given column into a vector of Booleans based on the
// result of the given function.
//
template<typename T, typename I = unsigned long>
struct FactorizeVisitor  {

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<bool>;
    using factor_func = std::function<bool(const value_type &val)>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        result_type result;

        result.reserve(std::distance(column_begin, column_end));

        for (auto citer = column_begin; citer < column_end; ++citer)
            result.push_back(ffunc_(*citer));

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit FactorizeVisitor(factor_func f) : ffunc_(f)  {   }

private:

    result_type result_ { };
    factor_func ffunc_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct AutoCorrVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES_3
    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        if (col_s <= 4)  return;

        std::vector<value_type>               tmp_result(col_s - 4);
        size_type                             lag = 1;
        const size_type                       thread_level =
            ThreadGranularity::get_sensible_thread_level();
        std::vector<std::future<CorrResult>>  futures(thread_level);
        size_type                             thread_count = 0;

        tmp_result[0] = 1.0;
        while (lag < col_s - 4)  {
            if (thread_count >= thread_level)  {
                const auto  result = get_auto_corr_(col_s, lag, column_begin);

                tmp_result[result.first] = result.second;
            }
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &AutoCorrVisitor::get_auto_corr_<H>,
                               this,
                               col_s,
                               lag,
                               std::cref(column_begin));
                thread_count += 1;
            }
            lag += 1;
        }

        for (size_type i = 0; i < thread_count; ++i)  {
            const auto  &result = futures[i].get();

            tmp_result[result.first] = result.second;
        }
        tmp_result.swap(result_);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    AutoCorrVisitor () = default;

private:

    result_type result_ {  };

    using CorrResult = std::pair<size_type, value_type>;

    template<typename H>
    inline CorrResult
    get_auto_corr_(size_type col_s,
                   size_type lag,
                   const H &column_begin) const  {

        CorrVisitor<value_type, index_type> corr {  };
        constexpr I                         dummy = I();

        corr.pre();
        for (size_type i = 0; i < col_s - lag; ++i)
            corr (dummy, *(column_begin + i), *(column_begin + (i + lag)));
        corr.post();

        return (CorrResult(lag, corr.get_result()));
    }
};

// ----------------------------------------------------------------------------

// Exponential rolling adoptor for visitors
// decay * Xt + (1 − decay) * Xt-1
// or
//    Xt + (1 - decay)Xt-1 + (1 - decay)^2 Xt-2 + ... + (1 - decay)^t X0
// ------------------------------------------------------------------------
//          1 + (1 - decay) + (1 - decay)^2 + ... + (1 - decay)^t
//
template<typename T, typename I = unsigned long>
struct  ExponentiallyWeightedMeanVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE
        assert(col_s > 3);

        result_type         result (col_s);
        const value_type    decay_comp = T(1) - decay_;

        result[0] = *column_begin;
        if (! finite_adjust_)  {
            for (size_type i = 1; i < col_s; ++i)
                result[i] =
                    decay_ * *(column_begin + i) + decay_comp * result[i - 1];
        }
        else  {  // Adjust for the fact that this is not an infinite data set
            value_type  denominator = 1;
            value_type  dc_p = 1;
            value_type  numerator = result[0];

            for (size_type i = 1; i < col_s; ++i)  {
                dc_p *= decay_comp;
                denominator += dc_p;
                numerator = numerator * decay_comp + *(column_begin + i);
                result[i] = numerator / denominator;
            }

            /*
            for (size_type i = 1; i < col_s; ++i)  {
                value_type  denominator = 0;
                value_type  dc_p = 1;
                value_type  numerator = 0;

                for (long j = static_cast<long>(i); j >= 0; --j)  {
                    numerator += dc_p * *(column_begin + j);
                    denominator += dc_p;
                    dc_p *= decay_comp;
                }
                result[i] = numerator / denominator;
            }
            */
        }

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    ExponentiallyWeightedMeanVisitor(exponential_decay_spec eds,
                                     value_type value,
                                     bool finite_adjust = false)
        : decay_(eds == exponential_decay_spec::center_of_gravity
                 ? T(1) / (T(1) + value)
                     : eds == exponential_decay_spec::span
                         ? T(2) / (T(1) + value)
                         : eds == exponential_decay_spec::halflife
                             ? T(1) - std::exp(std::log(T(0.5)) / value)
                             : value),
          finite_adjust_(finite_adjust)  {   }

private:

    const value_type    decay_;
    const bool          finite_adjust_;
    result_type         result_ {  };
};

template<typename T, typename I = unsigned long>
using ewm_v = ExponentiallyWeightedMeanVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct KthValueVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &values_begin, const H &values_end)  {

        result_ = find_kth_element_(values_begin, values_end, kth_element_);
    }

    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

    explicit KthValueVisitor (size_type ke, bool skipnan = true)
        : kth_element_(ke), skip_nan_(skipnan)  {   }

private:

    result_type     result_ {  };
    const size_type kth_element_;
    const bool      skip_nan_;

    template<typename It>
    inline value_type
    find_kth_element_ (It begin, It end, size_type k) const  {

        const size_type vec_size = std::distance(begin, end);

        if (k > vec_size || k <= 0)  {
            char    err[512];

            sprintf (err,
#ifdef _MSC_VER
                     "find_kth_element_(): vector length = %zu and k = %zu.",
#else
                     "find_kth_element_(): vector length = %lu and k = %lu.",
#endif // _MSC_VER
                     vec_size, k);
            throw NotFeasible (err);
        }

        std::vector<value_type> tmp_vec (vec_size - 1);
        value_type              kth_value =
            *(begin + static_cast<long>(vec_size / 2));
        size_type               less_count = 0;
        size_type               great_count = vec_size - 2;

        for (auto citer = begin; citer < end; ++citer)  {
            if (skip_nan_ && is_nan__(*citer))  continue;

            if (*citer < kth_value)
                tmp_vec [less_count++] = *citer;
            else if (*citer > kth_value)
                tmp_vec [great_count--] = *citer;
        }

        if (less_count > k - 1)
            return (find_kth_element_ (tmp_vec.begin (),
                                       tmp_vec.begin () + less_count,
                                       k));
        else if (less_count < k - 1)
            return (find_kth_element_ (tmp_vec.begin () + less_count,
                                       tmp_vec.end (),
                                       k - less_count - 1));
        else
            return (kth_value);
    }
};

template<typename T, typename I = unsigned long>
using kthv_v = KthValueVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MedianVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE
        KthValueVisitor<value_type, index_type> kv_visitor (col_s >> 1);


        kv_visitor.pre();
        kv_visitor(idx_begin, idx_end, column_begin, column_end);
        kv_visitor.post();
        result_ = kv_visitor.get_result();
        if (! (col_s & 0x0001))  { // even
            KthValueVisitor<value_type, I>   kv_visitor2 ((col_s >> 1) + 1);

            kv_visitor2.pre();
            kv_visitor2(idx_begin, idx_end, column_begin, column_end);
            kv_visitor2.post();
            result_ = (result_ + kv_visitor2.get_result()) / value_type(2);
        }
    }

    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

    MedianVisitor () = default;

private:

    result_type result_ {  };
};

template<typename T, typename I = unsigned long>
using med_v = MedianVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct QuantileVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

        if (qt_ < 0.0 || qt_ > 1.0 || col_s == 0)  {
            char buffer [512];

            sprintf (buffer,
                     "QuantileVisitor{}: unable to do quantile: "
#ifdef _MSC_VER
                     "qt: %f, Column Len: %zu",
#else
                     "qt: %f, Column Len: %lu",
#endif // _MSC_VER
                     qt_, col_s);
            throw NotFeasible(buffer);
        }

        const double    vec_len_frac = qt_ * col_s;
        const size_type int_idx =
            static_cast<size_type>(std::round(vec_len_frac));
        const bool      need_two =
            ! (col_s & 0x01) || double(int_idx) < vec_len_frac;

        if (qt_ == 0.0 || qt_ == 1.0)  {
            KthValueVisitor<T, I>   kth_value((qt_ == 0.0) ? 1 : col_s);

            kth_value.pre();
            kth_value(idx_begin, idx_end, column_begin, column_end);
            kth_value.post();
            result_ = kth_value.get_result();
        }
        else if (policy_ == quantile_policy::mid_point ||
                 policy_ == quantile_policy::linear)  {
            KthValueVisitor<T, I>   kth_value1(int_idx);

            kth_value1.pre();
            kth_value1(idx_begin, idx_end, column_begin, column_end);
            kth_value1.post();
            result_ = kth_value1.get_result();
            if (need_two && int_idx + 1 < col_s)  {
                KthValueVisitor<T, I>   kth_value2(int_idx + 1);

                kth_value2.pre();
                kth_value2(idx_begin, idx_end, column_begin, column_end);
                kth_value2.post();
                if (policy_ == quantile_policy::mid_point)
                    result_ = (result_ + kth_value2.get_result()) / 2.0;
                else // linear
                    result_ = result_ + (kth_value2.get_result() - result_) *
                              (1.0 - qt_);
            }
        }
        else if (policy_ == quantile_policy::lower_value ||
                 policy_ == quantile_policy::higher_value)  {
            KthValueVisitor<T, I>   kth_value(
                policy_ == quantile_policy::lower_value ? int_idx
                : (int_idx + 1 < col_s && need_two ? int_idx + 1 : int_idx));

            kth_value.pre();
            kth_value(idx_begin, idx_end, column_begin, column_end);
            kth_value.post();
            result_ = kth_value.get_result();
        }
    }

    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

    explicit
    QuantileVisitor (value_type quantile = 0.5,
                     quantile_policy q_policy = quantile_policy::mid_point)
        : qt_(quantile), policy_(q_policy)  {   }

private:

    result_type             result_ {  };
    const double            qt_;
    const quantile_policy   policy_;
};

template<typename T, typename I = unsigned long>
using qt_v = QuantileVisitor<T, I>;

// ----------------------------------------------------------------------------

// Mode of a vector is a value that appears most often in the vector.
// This visitor extracts the top N repeated values in the column with the
// associated indices.
// The T type must be hashable
// Because of the information this has to return, it is not a cheap operation
//
template<std::size_t N, typename T, typename I = unsigned long>
struct  ModeVisitor {

    DEFINE_VISIT_BASIC_TYPES

    struct  DataItem  {
        // Value of the column item
        //
        const value_type                *value { nullptr };
        // List of indices where value occurred
        //
        VectorConstPtrView<index_type>  indices { };

        // Number of times value occurred
        //
        inline size_type repeat_count() const  { return (indices.size()); }

        // List of column indices where value occurred
        //
        std::vector<size_type>  value_indices_in_col {  };

        DataItem() = default;
        inline DataItem(const value_type &v) : value(&v)  {
            indices.reserve(4);
            value_indices_in_col.reserve(4);
        }

        const value_type &get_value() const noexcept  { return (*value); }
    };

private:

    struct my_hash_  {
        using argument_type = const value_type *;
        using result_type = std::size_t;

        inline result_type operator() (const argument_type &a) const noexcept {

            return (std::hash<value_type>()(*a));
        }
    };

    struct my_equal_to_  {
        using first_argument_type = const value_type *;
        using second_argument_type = const value_type *;
        using result_type = bool;

        inline result_type
        operator() (const first_argument_type &f,
                    const second_argument_type &s) const noexcept {

            return (std::equal_to<value_type>()(*f, *s));
        }
    };

    using map_type = std::unordered_map<const value_type *,
                                        DataItem,
                                        my_hash_,
                                        my_equal_to_>;

public:

    using result_type = std::array<DataItem, N>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        DataItem        nan_item;
        GET_COL_SIZE
        map_type        val_map;

        val_map.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            if (is_nan__(*(column_begin + i)))  {
                nan_item.value = &*(column_begin + i);
                nan_item.indices.push_back(&*(idx_begin + i));
                nan_item.value_indices_in_col.push_back(i);
            }
            else  {
                auto    ret =
                    val_map.emplace(
                        std::pair<const value_type *, DataItem>(
                            &*(column_begin + i),
                            DataItem(*(column_begin + i))));

                ret.first->second.indices.push_back(&*(idx_begin + i));
                ret.first->second.value_indices_in_col.push_back(i);
            }
        }

        std::vector<DataItem> val_vec;

        val_vec.reserve(val_map.size() + 1);
        if (nan_item.value != nullptr)
            val_vec.push_back(nan_item);
        std::for_each(val_map.begin(),
                      val_map.end(),
                      [&val_vec](const auto &map_pair) -> void {
                          val_vec.push_back(map_pair.second);
                      });

        std::sort(val_vec.begin(), val_vec.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.repeat_count() > rhs.repeat_count()); // dec
                  });
        for (size_type i = 0; i < N && i < val_vec.size(); ++i)
            result_[i] = val_vec[i];
    }

    inline void pre ()  { result_type x; result_.swap (x); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }
    inline result_type &get_result ()  { return (result_); }

    inline void sort_by_repeat_count()  {

        std::sort(result_.begin(), result_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.repeat_count() < rhs.repeat_count());
                  });
    }
    inline void sort_by_value()  {

        std::sort(result_.begin(), result_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (*(lhs.value) < *(rhs.value));
                  });
    }

private:

    result_type result_ { };
};

template<std::size_t N, typename T, typename I = unsigned long>
using mode_v = ModeVisitor<N, T, I>;

// ----------------------------------------------------------------------------

// This calculates 4 different form of Mean Absolute Deviation based on the
// requested type input. Please the mad_type enum type
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct MADVisitor  {

private:

    const mad_type  mad_type_;
    const bool      skip_nan_;

    template <typename K, typename H>
    inline void
    calc_mean_abs_dev_around_mean_(const K &idx_begin,
                                   const K &idx_end,
                                   const H &column_begin,
                                   const H &column_end)  {

        GET_COL_SIZE
        MeanVisitor<T, I>   mean_visitor(skip_nan_);

        mean_visitor.pre();
        for (std::size_t i = 0; i < col_s; ++i)
            mean_visitor(*(idx_begin + i), *(column_begin + i));
        mean_visitor.post();

        MeanVisitor<T, I>   mean_mean_visitor(skip_nan_);

        mean_mean_visitor.pre();
        for (std::size_t i = 0; i < col_s; ++i)  {
            if (skip_nan_ && is_nan__(*(column_begin + i)))  continue;
            mean_mean_visitor(*(idx_begin + i),
                              std::fabs(*(column_begin + i) -
                                        mean_visitor.get_result()));
        }
        mean_mean_visitor.post();

        result_ = mean_mean_visitor.get_result();
    }

    template <typename K, typename H>
    inline void
    calc_mean_abs_dev_around_median_(const K &idx_begin,
                                     const K &idx_end,
                                     const H &column_begin,
                                     const H &column_end)  {

        MedianVisitor<T, I> median_visitor;

        median_visitor.pre();
        median_visitor(idx_begin, idx_end, column_begin, column_end);
        median_visitor.post();

        GET_COL_SIZE
        MeanVisitor<T, I>   mean_median_visitor(skip_nan_);

        mean_median_visitor.pre();
        for (std::size_t i = 0; i < col_s; ++i)  {
            if (skip_nan_ && is_nan__(*(column_begin + i)))  continue;
            mean_median_visitor(
                *(idx_begin + i),
                std::fabs(*(column_begin + i) - median_visitor.get_result()));
        }
        mean_median_visitor.post();

        result_ = mean_median_visitor.get_result();
    }

    template <typename K, typename H>
    inline void
    calc_median_abs_dev_around_mean_(const K &idx_begin,
                                     const K &idx_end,
                                     const H &column_begin,
                                     const H &column_end)  {

        MeanVisitor<T, I>   mean_visitor(skip_nan_);
        GET_COL_SIZE

        mean_visitor.pre();
        for (std::size_t i = 0; i < col_s; ++i)
            mean_visitor(*(idx_begin + i), *(column_begin + i));
        mean_visitor.post();

        MedianVisitor<T, I> median_mean_visitor;
        std::vector<T>      mean_dists;

        mean_dists.reserve(col_s);
        for (std::size_t i = 0; i < col_s; ++i)
            mean_dists.push_back(
                std::fabs(*(column_begin + i) - mean_visitor.get_result()));
        median_mean_visitor.pre();
        median_mean_visitor(idx_begin, idx_end,
                            mean_dists.begin(), mean_dists.end());
        median_mean_visitor.post();

        result_ = median_mean_visitor.get_result();
    }

    template <typename K, typename H>
    inline void
    calc_median_abs_dev_around_median_(const K &idx_begin,
                                       const K &idx_end,
                                       const H &column_begin,
                                       const H &column_end)  {

        MedianVisitor<T, I> median_visitor;

        median_visitor.pre();
        median_visitor(idx_begin, idx_end, column_begin, column_end);
        median_visitor.post();

        GET_COL_SIZE
        MedianVisitor<T, I> median_median_visitor;
        std::vector<T>      median_dists;

        median_dists.reserve(col_s);
        for (std::size_t i = 0; i < col_s; ++i)
            median_dists.push_back(
                std::fabs(*(column_begin + i) - median_visitor.get_result()));
        median_median_visitor.pre();
        median_median_visitor(idx_begin, idx_end,
                              median_dists.begin(), median_dists.end());
        median_median_visitor.post();

        result_ = median_median_visitor.get_result();
    }

public:

    DEFINE_VISIT_BASIC_TYPES_2

    MADVisitor (mad_type mt, bool skip_nan = true)
        : mad_type_(mt), skip_nan_(skip_nan)  {   }
    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        switch (mad_type_)  {
            case mad_type::mean_abs_dev_around_mean:
                calc_mean_abs_dev_around_mean_(idx_begin, idx_end,
                                               column_begin, column_end);
                break;
            case mad_type::mean_abs_dev_around_median:
                calc_mean_abs_dev_around_median_(idx_begin, idx_end,
                                                 column_begin, column_end);
                break;
            case mad_type::median_abs_dev_around_mean:
                calc_median_abs_dev_around_mean_(idx_begin, idx_end,
                                                 column_begin, column_end);
                break;
            case mad_type::median_abs_dev_around_median:
                calc_median_abs_dev_around_median_(idx_begin, idx_end,
                                                   column_begin, column_end);
                break;
            default:
                break;
        }
    }

    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

private:

    result_type result_ {  };
};

template<typename T, typename I = unsigned long>
using mad_v = MADVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DiffVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        GET_COL_SIZE

			assert(col_s > 0 && size_type(std::abs(periods_)) < (col_s - 1));

        bool        there_is_zero = false;
        result_type result;

        result.reserve(col_s);
        if (periods_ >= 0)  {
            if (! skip_nan_)  {
                for (long i = 0;
                     i < periods_ && static_cast<size_type>(i) < col_s; ++i)
                    result.push_back(
                        std::numeric_limits<value_type>::quiet_NaN());
            }

            auto    i = column_begin + periods_;

            for (auto j = column_begin; i < column_end; ++i, ++j) {
                if (skip_nan_ && (is_nan__(*i) || is_nan__(*j)))  continue;

                const value_type    val = *i - *j;

                result.push_back(val);
                if (val == 0)  there_is_zero = true;
            }
        }
        else {
            H   i = column_end - (1 + std::abs(periods_));
            H   j = column_end - 1;

            for ( ; i > column_begin; --i, --j) {
                if (skip_nan_ && (is_nan__(*i) || is_nan__(*j)))  continue;

                const value_type    val = *i - *j;

                result.push_back(val);
                if (val == 0)  there_is_zero = true;
            }
            if (i == column_begin)  {
                if (! (skip_nan_ && (is_nan__(*i) || is_nan__(*j))))  {
                    const value_type    val = *i - *j;

                    result.push_back(val);
                    if (val == 0)  there_is_zero = true;
                }
            }
            std::reverse(result.begin(), result.end());

            if (! skip_nan_)
                for (size_type i = 0;
                     i < static_cast<size_type>(std::abs(periods_)) &&
                         i < col_s;
                     ++i)
                    result.push_back(
                        std::numeric_limits<value_type>::quiet_NaN());
        }

        if (non_zero_ && there_is_zero)
            std::for_each(result.begin(), result.end(),
                          [](value_type &v) {
                              v += std::numeric_limits<value_type>::epsilon();
                          });

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    DiffVisitor(long periods = 1, bool skipnan = true, bool non_zero = false)
        : periods_(periods), skip_nan_(skipnan), non_zero_(non_zero) {  }

private:

    result_type result_ { };
    const long  periods_;
    const bool  skip_nan_;
    const bool  non_zero_;
};

template<typename T, typename I = unsigned long>
using diff_v = DiffVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct ZScoreVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        MeanVisitor<T, I>   mvisit;
        StdVisitor<T, I>    svisit;
        GET_COL_SIZE

        mvisit.pre();
        svisit.pre();
        for (size_type i = 0; i < col_s; ++i)  {
            if (! skip_nan_ || ! is_nan__(*(column_begin + i)))  {
                mvisit(*(idx_begin + i), *(column_begin + i));
                svisit(*(idx_begin + i), *(column_begin + i));
            }
        }
        mvisit.post();
        svisit.post();

        const value_type    m = mvisit.get_result();
        const value_type    s = svisit.get_result();
        result_type         result;

        result.reserve(col_s);
        for (auto citer = column_begin; citer < column_end; ++citer)
            result.push_back((*citer - m) / s);

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    DECL_CTOR(ZScoreVisitor)

private:

    result_type result_ {  };  // Z Score
    const bool  skip_nan_;
};

template<typename T, typename I = unsigned long>
using zs_v = ZScoreVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SampleZScoreVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &population_begin, const H &population_end,
                const H &sample_begin, const H &sample_end)  {

        MeanVisitor<T, I>   p_mvisit;
        StdVisitor<T, I>    p_svisit;
        MeanVisitor<T, I>   s_mvisit;
        const size_type     p_col_s =
            std::distance(population_begin, population_end);
        const size_type     s_col_s = std::distance(sample_begin, sample_end);
        const size_type     max_s = std::max(p_col_s, s_col_s);

        p_mvisit.pre();
        p_svisit.pre();
        s_mvisit.pre();
        for (size_type i = 0; i < max_s; ++i)  {
            if (i < p_col_s)  {
                if (! skip_nan_ || ! is_nan__(*(population_begin + i)))  {
                    p_mvisit(*(idx_begin + i), *(population_begin + i));
                    p_svisit(*(idx_begin + i), *(population_begin + i));
                }
            }
            if (i < s_col_s)  {
                if (! skip_nan_ || ! is_nan__(*(sample_begin + i)))  {
                    s_mvisit(*(idx_begin + i), *(sample_begin + i));
                }
            }
        }
        p_mvisit.post();
        p_svisit.post();
        s_mvisit.post();

        result_ = (s_mvisit.get_result() - p_mvisit.get_result()) /
                  (p_svisit.get_result() / ::sqrt(s_col_s));
    }

    inline void pre ()  { result_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (result_); }

    DECL_CTOR(SampleZScoreVisitor)

private:

    value_type  result_ {  };  // Z Score
    const bool  skip_nan_;
};

template<typename T, typename I = unsigned long>
using szs_v = SampleZScoreVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SigmoidVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

private:

    template <typename H>
    inline void logistic_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            result_.push_back(1.0 / (1.0 + std::exp(-(*citer))));
    }
    template <typename H>
    inline void algebraic_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            result_.push_back(1.0 / std::sqrt(1.0 + std::pow(*citer, 2.0)));
    }
    template <typename H>
    inline void hyperbolic_tan_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            result_.push_back(std::tanh(*citer));
    }
    template <typename H>
    inline void arc_tan_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            result_.push_back(std::atan(*citer));
    }
    template <typename H>
    inline void error_function_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            result_.push_back(std::erf(*citer));
    }
    template <typename H>
    inline void gudermannian_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            result_.push_back(std::atan(std::sinh(*citer)));
    }
    template <typename H>
    inline void smoothstep_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)  {
            if (*citer <= 0.0)
                result_.push_back(0.0);
            else if (*citer >= 1.0)
                result_.push_back(1.0);
            else
                result_.push_back(*citer * *citer * (3.0 - 2.0 * *citer));
        }
    }

public:

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        result_.reserve(std::distance(column_begin, column_end));
        if (sigmoid_type_ == sigmoid_type::logistic)
            logistic_(column_begin, column_end);
        else if (sigmoid_type_ == sigmoid_type::algebraic)
            algebraic_(column_begin, column_end);
        else if (sigmoid_type_ == sigmoid_type::hyperbolic_tan)
            hyperbolic_tan_(column_begin, column_end);
        else if (sigmoid_type_ == sigmoid_type::arc_tan)
            arc_tan_(column_begin, column_end);
        else if (sigmoid_type_ == sigmoid_type::error_function)
            error_function_(column_begin, column_end);
        else if (sigmoid_type_ == sigmoid_type::gudermannian)
            gudermannian_(column_begin, column_end);
        else if (sigmoid_type_ == sigmoid_type::smoothstep)
            smoothstep_(column_begin, column_end);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit SigmoidVisitor(sigmoid_type st) : sigmoid_type_(st)  {   }

private:

    result_type         result_ {  }; // Sigmoids
    const sigmoid_type  sigmoid_type_;
};

template<typename T, typename I = unsigned long>
using sigm_v = SigmoidVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct BoxCoxVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

private:

    template<typename H>
    inline void modulus_(const H &column_begin, const H &column_end)  {

        H   citer = column_begin;

        if (lambda_ != 0)  {
            while (citer < column_end)  {
                const value_type    sign = std::signbit(*citer) ? -1 : 1;
                const value_type    v =
                    (std::pow(std::fabs(*citer++) + one_, lambda_) - one_) /
                    lambda_;

                result_.push_back(sign * v);
            }
        }
        else  {
            while (citer < column_end)  {
                const value_type    sign = std::signbit(*citer) ? -1 : 1;

                result_.push_back(sign * std::log(std::fabs(*citer++) + one_));
            }
        }
    }

    template<typename H>
    inline void exponential_(const H &column_begin, const H &column_end)  {

        H   citer = column_begin;

        if (lambda_ != 0)  {
            while (citer < column_end)
                result_.push_back(
                    (std::exp(lambda_ * *citer++) - one_) / lambda_);
        }
        else  {
            while (citer < column_end)
                result_.push_back(*citer++);
        }
    }

    template<typename H>
    inline void original_(const H &column_begin,
                          const H &column_end,
                          value_type shift)  {

        H   citer = column_begin;

        if (lambda_ != 0)  {
            while (citer < column_end)
                result_.push_back(
                    (std::pow(*citer++ + shift, lambda_) -  one_) / lambda_);
        }
        else  {
            while (citer < column_end)
                result_.push_back(std::log(*citer++ + shift));
        }
    }

    template<typename K, typename H>
    inline void geometric_mean_(const K &dummy,
                                const H &column_begin,
                                const H &column_end,
                                value_type shift)  {

        H   citer = column_begin;

        if (lambda_ != 0)  {
            GeometricMeanVisitor<T, I>  reg_gm;

            reg_gm.pre();
            while (citer < column_end)
                reg_gm(dummy, *citer++ + shift);
            reg_gm.post();

            citer = column_begin;
            while (citer < column_end)  {
                const value_type    raw_v = *citer++ + shift;
                const value_type    v =
                    (std::pow(raw_v, lambda_) -  one_) /
                    (lambda_ * std::pow(reg_gm.get_result(), lambda_ - one_));

                result_.push_back(v);
            }
        }
        else  {
            GeometricMeanVisitor<T, I>  log_gm;

            log_gm.pre();
            while (citer < column_end)
                log_gm(dummy, std::log(*citer++ + shift));
            log_gm.post();

            citer = column_begin;
            while (citer < column_end)  {
                const value_type    raw_v = *citer++ + shift;

                result_.push_back(raw_v * log_gm.get_result());
            }
        }
    }

public:

    template<typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        value_type  shift = 0;

        if (! is_all_positive_ &&
            (box_cox_type_ == box_cox_type::original ||
             box_cox_type_ == box_cox_type::geometric_mean))  {
            MinVisitor<T, I>    mv;

            mv.pre();
            mv(idx_begin, idx_end, column_begin, column_end);
            mv.post();

            shift = std::fabs(mv.get_result()) + value_type(0.0000001);
        }

        result_.reserve(std::distance(column_begin, column_end));
        if (box_cox_type_ == box_cox_type::original)
            original_(column_begin, column_end, shift);
        else if (box_cox_type_ == box_cox_type::geometric_mean)
            geometric_mean_(*idx_begin, column_begin, column_end, shift);
        else if (box_cox_type_ == box_cox_type::modulus)
            modulus_(column_begin, column_end);
        else if (box_cox_type_ == box_cox_type::exponential)
            exponential_(column_begin, column_end);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    BoxCoxVisitor(box_cox_type bct, value_type l, bool is_all_pos)
        : box_cox_type_(bct),
          lambda_(l),
          is_all_positive_(is_all_pos)  {   }

private:

    result_type                 result_ {  }; // Transformed
    const box_cox_type          box_cox_type_;
    const value_type            lambda_;
    const bool                  is_all_positive_;
    static constexpr value_type one_ { 1 };
};

template<typename T, typename I = unsigned long>
using bcox_v = BoxCoxVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct NormalizeVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template<typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        MinVisitor<T, I>    minv;
        MaxVisitor<T, I>    maxv;

        minv.pre();
        maxv.pre();
        minv(idx_begin, idx_end, column_begin, column_end);
        maxv(idx_begin, idx_end, column_begin, column_end);
        minv.post();
        maxv.post();

        const value_type    diff = maxv.get_result() - minv.get_result();
        H                   citer = column_begin;
        result_type         result;

        result.reserve(std::distance(column_begin, column_end));
        while (citer < column_end)
            result.push_back((*citer++ - minv.get_result()) / diff);

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

private:

    result_type result_ {  };  // Normalized
};

template<typename T, typename I = unsigned long>
using norm_v = NormalizeVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StandardizeVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template<typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        MeanVisitor<T, I>   mv;
        StdVisitor<T, I>    sv;

        mv.pre();
        sv.pre();
        mv(idx_begin, idx_end, column_begin, column_end);
        sv(idx_begin, idx_end, column_begin, column_end);
        mv.post();
        sv.post();

        H           citer = column_begin;
        result_type result;

        result.reserve(std::distance(column_begin, column_end));
        while (citer < column_end)
            result.push_back((*citer++ - mv.get_result()) / sv.get_result());

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

private:

    result_type result_ {  }; // Standardized
};

template<typename T, typename I = unsigned long>
using stand_v = StandardizeVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct PolyFitVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

private:

    static inline size_type
    index_(size_type row, size_type col, size_type num_rows)  {

        return (col * num_rows + row);
    }

public:

    using weight_func =
        std::function<value_type(const index_type &idx, size_type val_index)>;

    template<typename K, typename Hx, typename Hy>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const Hx &x_begin, const Hx &x_end,
                const Hy &y_begin, const Hy &y_end)  {

        const size_type col_s = std::distance(x_begin, x_end);

        assert((col_s == size_type(std::distance(y_begin, y_end))));

        // degree needs to change to contain the slope (0-degree)
        //
        size_type       deg = degree_;
        const size_type nrows = deg + 1;

        // Array that will store the values of
        // sigma(xi), sigma(xi^2), sigma(xi^3) ... sigma(xi^2n)
        //
        std::vector<value_type> sigma_x (2 * nrows, 0);

        for (size_type i = 0; i < sigma_x.size(); ++i) {
            // consecutive positions of the array will store
            // col_s, sigma(xi), sigma(xi^2), sigma(xi^3) ... sigma(xi^2n)
            //
            for (size_type j = 0; j < col_s; ++j)  {
                const value_type    w = weights_(*(idx_begin + j), j);

                sigma_x[i] += std::pow(*(x_begin + j), i) * w;
            }
        }

        // eq_mat is the Normal matrix (augmented) that will store the
        // equations. The extra column is the y column.
        //
        std::vector<value_type> eq_mat (nrows * (deg + 2), 0);

        for (size_type i = 0; i <= deg; ++i)  {
            // Build the Normal matrix by storing the corresponding
            // coefficients at the right positions except the last column
            // of the matrix
            //
            for (size_type j = 0; j <= deg; ++j)
                eq_mat[index_(i, j, nrows)] = sigma_x[i + j];
        }

        // Array to store the values of
        // sigma(yi), sigma(xi * yi), sigma(xi^2 * yi) ... sigma(xi^n * yi)
        //
        std::vector<value_type> sigma_y (nrows, 0);

        for (size_type i = 0; i < sigma_y.size(); ++i) {
            // consecutive positions will store
            // sigma(yi), sigma(xi * yi), sigma(xi^2 * yi) ... sigma(xi^n * yi)
            //
            for (size_type j = 0; j < col_s; ++j)  {
                const value_type    w = weights_(*(idx_begin + j), j);

                sigma_y[i] += std::pow(*(x_begin + j), i) * *(y_begin + j) * w;
            }
        }

        // load the values of sigma_y as the last column of eq_mat
        // (Normal Matrix but augmented)
        //
        for (size_type i = 0; i <= deg; ++i)
            eq_mat[index_(i, nrows, nrows)] = sigma_y[i];

        // deg is made deg + 1 because the Gaussian elimination part
        // below was for deg equations, but here deg is the deg of
        // polynomial and for deg we get deg + 1 equations
        //
        deg += 1;

        // From now Gaussian elimination starts (can be ignored) to solve the
        // set of linear equations (Pivotisation)
        //
        for (size_type i = 0; i < deg; ++i)  {
            for (size_type k = i + 1; k < deg; ++k)
                if (eq_mat[index_(i, i, nrows)] < eq_mat[index_(k, i, nrows)])
                    for (size_type j = 0; j <= deg; ++j)
                        std::swap(eq_mat[index_(i, j, nrows)],
                                  eq_mat[index_(k, j, nrows)]);
        }

        // loop to perform the Gauss elimination
        //
        for (size_type i = 0; i < deg - 1; ++i)  {
            for (size_type k = i + 1; k < deg; ++k) {
                const value_type    t =
                    eq_mat[index_(k, i, nrows)] / eq_mat[index_(i, i, nrows)];

                // make the elements below the pivot elements equal to zero
                // or elimnate the variables
                //
                for (size_type j = 0; j <= deg; ++j)
                    eq_mat[index_(k, j, nrows)] =
                        eq_mat[index_(k, j, nrows)] -
                        eq_mat[index_(i, j, nrows)] * t;
            }
        }

        coeffs_.resize(deg, 0);

        // back-substitution
        // coeffs_ is a vector whose values correspond to the values
        // of x, y, z ...
        //
        for (int i = int(deg) - 1; i >= 0; --i) {
            // make the variable to be calculated equal to the rhs of the last
            // equation
            //
            coeffs_[i] = eq_mat[index_(i, deg, nrows)];
            for (int j = 0; j < int(deg); ++j)  {
                // then subtract all the lhs values except the coefficient of
                // the variable whose value is being calculated
                //
                if (j != i)
                    coeffs_[i] =
                        coeffs_[i] - eq_mat[index_(i, j, nrows)] * coeffs_[j];
            }

            // now finally divide the rhs by the coefficient of the
            // variable to be calculated
            //
            coeffs_[i] = coeffs_[i] / eq_mat[index_(i, i, nrows)];
        }

        for (size_type i = 0; i < col_s; ++i)  {
            value_type  pred = 0;

            for (size_type j = 0; j < deg; ++j)
                pred += coeffs_[j] * std::pow(*(x_begin + i), j);

            const value_type    w = weights_(*(idx_begin + i), i);

            residual_ += ((*(y_begin + i) - pred) * w) *
                         ((*(y_begin + i) - pred) * w);
        }
    }

    inline void pre ()  { coeffs_.clear(); residual_ = 0; }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (coeffs_); }
    inline result_type &get_result ()  { return (coeffs_); }
    inline value_type get_slope () const  { return (coeffs_[0]); }
    inline value_type get_residual () const  { return (residual_); }

    explicit
    PolyFitVisitor(size_type d,
                   weight_func w_func =
                       [](const I &, std::size_t) -> T  { return (1); })
        : degree_(d), weights_(w_func)  {   }

private:

    result_type     coeffs_ {  };
    value_type      residual_ { 0 };
    const size_type degree_;
    weight_func     weights_;
};

template<typename T, typename I = unsigned long>
using pfit_v = PolyFitVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct LogFitVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    using weight_func =
        std::function<value_type(const index_type &idx, size_type val_index)>;

    template<typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &x_begin, const H &x_end,
                const H &y_begin, const H &y_end)  {

        std::vector<value_type> logx (x_begin, x_end);

        std::transform(logx.begin(), logx.end(), logx.begin(),
                       (value_type(*)(value_type)) std::log);
        poly_fit_(idx_begin, idx_end, logx.begin(), logx.end(), y_begin, y_end);

        const size_type col_s = std::distance(x_begin, x_end);

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    pred =
                poly_fit_.get_result()[0] +
                poly_fit_.get_result()[1] * std::log(*(x_begin + i));
            const value_type    w = weights_(*(idx_begin + i), i);

            residual_ += ((*(y_begin + i) - pred) * w) *
                         ((*(y_begin + i) - pred) * w);
        }
    }

    inline void pre ()  { poly_fit_.pre(); residual_ = 0; }
    inline void post ()  { poly_fit_.post(); }
    inline const result_type &
    get_result () const  { return (poly_fit_.get_result()); }
    inline result_type &get_result ()  { return (poly_fit_.get_result()); }
    inline value_type get_slope () const  { return (poly_fit_.get_slope()); }
    inline value_type get_residual () const  { return (residual_); }

    explicit
    LogFitVisitor(weight_func w_func =
                      [](const I &, std::size_t) -> T  { return (1); })
        : poly_fit_(1, w_func), weights_(w_func)  {   }

private:

    PolyFitVisitor<T, I>    poly_fit_ {  };
    weight_func             weights_;
    value_type              residual_ { 0 };
};

template<typename T, typename I = unsigned long>
using lfit_v = LogFitVisitor<T, I>;

// ----------------------------------------------------------------------------

// LOcally WEighted Scatterplot Smoothing
// A LOWESS function outputs smoothed estimates of dependent var (y) at the
// given independent var (x) values.
//
// This lowess function implements the algorithm given in the reference below
// using local linear estimates.
// Suppose the input data has N points. The algorithm works by estimating the
// `smooth` y_i by taking the frac * N closest points to (x_i, y_i) based on
// their x values and estimating y_i using a weighted linear regression. The
// weight for (x_j, y_j) is tricube function applied to |x_i - x_j|.
// If n_loop > 1, then further weighted local linear regressions are performed,
// where the weights are the same as above times the _lowess_bisquare function
// of the residuals. Each iteration takes approximately the same amount of time
// as the original fit, so these iterations are expensive. They are most useful
// when the noise has extremely heavy tails, such as Cauchy noise. Noise with
// less heavy-tails, such as t-distributions with df > 2, are less problematic.
// The weights downgrade the influence of points with large residuals. In the
// extreme case, points whose residuals are larger than 6 times the median
// absolute residual are given weight 0. Delta can be used to save
// computations. For each x_i, regressions are skipped for points closer than
// delta. The next regression is fit for the farthest point within delta of
// x_i and all points in between are estimated by linearly interpolating
// between the two regression fits. Judicious choice of delta can cut
// computation time considerably for large data (N > 5000). A good choice is
// delta = 0.01 * range(independ_var). Some experimentation is likely required
// to find a good choice of frac and iter for a particular dataset.
// References
// ----------
// Cleveland, W.S. (1979) "Robust Locally Weighted Regression
// and Smoothing Scatterplots". Journal of the American Statistical
// Association 74 (368): 829-836.
//
template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct LowessVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

private:

    // The bi-square function (1 - x^2)^2. Used to weight the residuals in the
    // robustifying iterations. Called by the calculate_residual_weights
    // function.
    //
    template<typename X>
    inline static void bi_square_(X x_begin, X x_end)  {

        while (x_begin != x_end)  {
            const value_type    val = one_ - *x_begin * *x_begin;

            *x_begin++ = val * val;
        }
    }

    // The tri-cubic function (1 - x^3)^3. Used to weight neighboring points
    // along the x-axis based on their distance to the current point.
    //
    template<typename X>
    inline static void tri_cube_(X x_begin, X x_end)  {

        while (x_begin != x_end)  {
            const value_type    val = one_ - *x_begin * *x_begin * *x_begin;

            *x_begin++ = val * val * val;
        }
    }

    // Calculate residual weights for the next robustifying iteration.
    //
    template<typename IDX, typename Y, typename K>
    inline void
    calc_residual_weights_(const IDX &idx_begin, const IDX &idx_end,
                           const Y &y_begin, const Y &y_end,
                           const K &y_fits_begin, const K &y_fits_end,
                           size_type col_s)  {

        for (size_type i = 0; i < col_s; ++i)
            resid_weights_[i] =
                std::fabs(*(y_begin + i) - *(y_fits_begin + i));

        MedianVisitor<T, I> median_v;

        median_v.pre();
        median_v(idx_begin, idx_end,
                 resid_weights_.begin(), resid_weights_.end());
        median_v.post();

        if (median_v.get_result() == 0)  {
            std::replace_if(resid_weights_.begin(), resid_weights_.end(),
                            std::bind(std::greater<value_type>(),
                                      std::placeholders::_1, z_),
                            one_);
        }
        else  {
            const value_type    val = six_ * median_v.get_result();

            std::transform(resid_weights_.begin(), resid_weights_.end(),
                           resid_weights_.begin(),
                           [val](auto c) -> value_type { return (c / val); });
        }

        // Some trimming of outlier residuals.
        //
        std::replace_if(resid_weights_.begin(), resid_weights_.end(),
                        std::bind(std::greater<value_type>(),
                                  std::placeholders::_1, one_),
                        one_);

        // std::replace_if(resid_weights_.begin(), resid_weights_.end(),
        //                 std::bind(std::greater_equal<value_type>(),
        //                           std::placeholders::_1, value_type(0.999)),
        //                 one_);
        // std::replace_if(resid_weights_.begin(), resid_weights_.end(),
        //                 std::bind(std::less_equal<value_type>(),
        //                           std::placeholders::_1, value_type(0.001)),
        //                 z_);

        bi_square_(resid_weights_.begin(), resid_weights_.end());
    }

    // Update the counters of the local regression.
    // For most points within delta of the current point, we skip the weighted
    // linear regression (which save much computation of weights and fitted
    // points). Instead, we'll jump to the last point within delta, fit the
    // weighted regression at that point, and linearly interpolate in between.
    //
    template<typename X, typename K>
    inline static void
    update_indices_(const X &x_begin, const X &x_end,
                    const K &y_fits_begin, const K &y_fits_end,
                    value_type delta,
                    long &curr_idx, long &last_fit_idx,
                    size_type col_s)  {

        last_fit_idx = curr_idx;

        // This loop increments until we fall just outside of delta distance,
        // copying the results for any repeated x's along the way.
        //
        const value_type    cutoff = *(x_begin + last_fit_idx) + delta;
        long                k = last_fit_idx + 1;
        bool                looped = false;

        for ( ; size_type(k) < col_s; ++k)  {
            looped = true;
            if (*(x_begin + k) > cutoff)  break;
            if (*(x_begin + k) == *(x_begin + last_fit_idx))  {
                // if tied with previous x-value, just use the already fitted
                // y, and update the last-fit counter.
                //
                *(y_fits_begin + k) = *(y_fits_begin + last_fit_idx);
                last_fit_idx = k;
            }
        }

        // curr_idx, which indicates the next point to fit the regression at,
        // is either one prior to k (since k should be the first point outside
        // of delta) or is just incremented + 1 if k = curr_idx + 1.
        // This insures we always step forward.
        //
        curr_idx = std::max(k - (looped ? 1 : 2), last_fit_idx + 1);
    }

    // Calculate smoothed/fitted y by linear interpolation between the current
    // and previous y fitted by weighted regression.
    // Called only if delta > 0.
    //
    template<typename X, typename K>
    inline void
    interpolate_skipped_fits_(const X x_begin, const X x_end,
                              K y_fits_begin, K y_fits_end,
                              long curr_idx, long last_fit_idx)  {

        auxiliary_vec_.clear();
        auxiliary_vec_.reserve(curr_idx - last_fit_idx);

        const value_type    last_fit_xval = *(x_begin + last_fit_idx);

        for (long i = last_fit_idx + 1; i < curr_idx; ++i)
            auxiliary_vec_.push_back(*(x_begin + i) - last_fit_xval);

        const value_type    x_diff = *(x_begin + curr_idx) - last_fit_xval;

        for (auto iter : auxiliary_vec_)
            iter /= x_diff;

        const value_type    last_fit_yval = *(y_fits_begin + last_fit_idx);
        const value_type    curr_idx_yval = *(y_fits_begin + curr_idx);

        for (long i = last_fit_idx + 1; i < curr_idx; ++i)
            *(y_fits_begin + i) =
                auxiliary_vec_[i] * curr_idx_yval +
                (one_ - auxiliary_vec_[i]) * last_fit_yval;
    }

    // Calculate smoothed/fitted y-value by weighted regression.
    // No regression function (e.g. lstsq) is called. Instead "projection
    // vector" p_idx_j is calculated,
    // and y_fit[i] = sum(p_idx_j * y[j]) = y_fit[i]
    // for j s.t. x[j] is in the neighborhood of xval. p_idx_j is a function of
    // the weights, xval, and its neighbors.
    //
    template<typename X, typename K, typename Y, typename W>
    inline static void
    calculate_y_fits_(const X x_begin, const X x_end,
                      const K y_begin, const K y_end,
                      const W w_begin, const W w_end,
                      Y y_fits_begin, Y y_fits_end,
                      long curr_idx,
                      value_type xval,
                      size_type left_end, size_type right_end,
                      bool reg_ok, bool fill_with_nans)  {

        if (! reg_ok)  {
            // Fill a bad regression (weights all zeros) with nans
            // or
            // Fill a bad regression with the original value only possible
            // when not using xvals distinct from x
            //
            *(y_fits_begin + curr_idx) =
                fill_with_nans
                    ? std::numeric_limits<value_type>::quiet_NaN()
                    : *(y_begin + curr_idx);
        }
        else  {
            value_type  sum_weighted_x = 0;

            for (size_type j = left_end; j < right_end; ++j)
                sum_weighted_x += *(w_begin + j) * *(x_begin + j);

            value_type  weighted_sqdev_x = 0;

            for (size_type j = left_end; j < right_end; ++j)  {
                const value_type    val = *(x_begin + j) - sum_weighted_x;

                weighted_sqdev_x += *(w_begin + j) * val * val;
            }

            for (size_type j = left_end; j < right_end; ++j)  {
                const value_type    p_idx_j =
                    *(w_begin + j) *
                    (one_ + (xval - sum_weighted_x) *
                     (*(x_begin + j) - sum_weighted_x) / weighted_sqdev_x);

                *(y_fits_begin + curr_idx) += p_idx_j * *(y_begin + j);
            }
        }
    }

    // If it returns True, at least some points have positive weight, and the
    // regression will be run. If False, the regression is skipped and
    // y_fit[i] is set to equal y[i].
    //
    template<typename X, typename K>
    inline bool
    calculate_weights_(const X &x_begin, const X &x_end,
                       const K &w_begin, const K &w_end, // Regression weights
                       // The x-value of the point currently being fit
                       //
                       value_type xval,
                       size_type left_end, size_type right_end,
                       // The radius of the current neighborhood. The larger
                       // of distances between x[i] and its left-most or
                       // right-most neighbor.
                       //
                       value_type radius)  {

        x_j_.clear();
        dist_i_j_.clear();
        x_j_.reserve(right_end - left_end);
        dist_i_j_.reserve(right_end - left_end);

        for (size_type j = left_end; j < right_end; ++j)  {
            x_j_.push_back(*(x_begin + j));
            dist_i_j_.push_back(std::fabs(*(x_begin + j) - xval) / radius);

            // Assign the distance measure to the weights, then apply the
            // tricube function to change in-place.
            //
            *(w_begin + j) = dist_i_j_.back();
        }

        tri_cube_(w_begin + left_end, w_begin + right_end);
        for (size_type j = left_end; j < right_end; ++j)
            *(w_begin + j) *= resid_weights_[j];

        value_type    sum_weights = 0;
        size_type     non_zero_cnt = 0;

        for (size_type j = left_end; j < right_end; ++j)  {
            const value_type    val = *(w_begin + j);

            if (val != 0)  {
                sum_weights += val;
                non_zero_cnt += 1;
            }
        }

        bool    reg_ok = true;

        // 2nd condition checks if only 1 local weight is non-zero, which
        // will give a divisor of zero in calculate_y_fit
        //
        if (sum_weights <= 0 || non_zero_cnt == 1)
            reg_ok = false;
        else
            for (size_type j = left_end; j < right_end; ++j)
                *(w_begin + j) /= sum_weights;

        return (reg_ok);
    }

    // Find the indices bounding the k-nearest-neighbors of the current point.
    // It returns the radius of the current neighborhood. The larger of
    // distances between xval and its left-most or right-most neighbor.
    //
    template<typename X>
    inline static value_type
    update_neighborhood_(const X &x_begin, const X &x_end,
                         value_type xval,
                         size_type curr_idx,
                         size_type &left_end, size_type &right_end)  {

        // A subtle loop. Start from the current neighborhood range:
        // [left_end, right_end). Shift both ends rightwards by one (so that
        // the neighborhood still contains k points), until the current point
        // is in the center (or just to the left of the center) of the
        // neighborhood. This neighborhood will contain the k-nearest
        // neighbors of xval.
        //
        // Once the right end hits the end of the data, hold the neighborhood
        // the same for the remaining xvals.
        //
        while (true)  {
            if (right_end < curr_idx)  {
                if (xval > ((*(x_begin + left_end) +
                             *(x_begin + right_end)) / two_))  {
                    left_end += 1;
                    right_end += 1;
                }
                else  break;
            }
            else  break;
        }

        return (std::max(xval - *(x_begin + left_end),
                         *(x_begin + (right_end - 1)) - xval));
    }

    template<typename K, typename Y, typename X>
    inline void
    lowess_(const K &idx_begin, const K &idx_end,
            const Y &y_begin, const Y &y_end,  // dependent variable
            const X &x_begin, const X &x_end)  {  // independent variable

        const size_type col_s = std::distance(x_begin, x_end);

        // The number of neighbors in each regression. round up if close
        // to integer
        //
        size_type   k =
            size_type (frac_ * value_type(col_s) + value_type(1e-10));

        // frac_ should be set, so that 2 <= k <= n. Conform them instead of
        // throwing error.
        //
        if (k < 2)  k = 2;
        if (k > col_s)  k = col_s;

        result_type weights (col_s, 0);

        y_fits_.resize(col_s, 0);
        resid_weights_.resize(col_s, one_);
        for (size_type l = 0; l < loop_n_; ++l)  {
            long        curr_idx = 0;
            long        last_fit_idx = -1;
            size_type   left_end = 0;
            size_type   right_end = k;

            // Fit y[i]'s until the end of the regression
            //
            std::fill(y_fits_.begin(), y_fits_.end(), 0);
            while (true)  {
                // The x value at which we will fit this time
                //
                const value_type    xval = *(x_begin + curr_idx);
                // Describe the neighborhood around the current xval.
                //
                const value_type    radius =
                    update_neighborhood_(x_begin, x_end,
                                         xval,
                                         col_s,
                                         left_end, right_end);

                // Re-initialize the weights for each point xval.
                //
                std::fill(weights.begin(), weights.end(), 0);

                // Calculate the weights for the regression in this
                // neighborhood. Determine if at least some weights are
                // positive, so a regression is ok.
                //
                const bool  reg_ok =
                    calculate_weights_(x_begin, x_end,
                                       weights.begin(), weights.end(),
                                       xval,
                                       left_end, right_end,
                                       radius);

                // If ok, run the regression
                //
                calculate_y_fits_(x_begin, x_end,
                                  y_begin, y_end,
                                  weights.begin(), weights.end(),
                                  y_fits_.begin(), y_fits_.end(),
                                  curr_idx,
                                  xval,
                                  left_end, right_end,
                                  reg_ok, false);

                // If we skipped some points, because of how delta_ was set,
                // go back and fit them by linear interpolation.
                //
                if (last_fit_idx < (curr_idx - 1))
                    interpolate_skipped_fits_(x_begin, x_end,
                                              y_fits_.begin(), y_fits_.end(),
                                              curr_idx, last_fit_idx);

                // Update the last fit counter to indicate we've now fit this
                // point. Find the next i for which we'll run a regression.
                //
                update_indices_(x_begin, x_end,
                                y_fits_.begin(), y_fits_.end(),
                                delta_,
                                curr_idx, last_fit_idx,
                                col_s);

                if (last_fit_idx >= (long(col_s) - 1L))  break;
            }

            calc_residual_weights_(idx_begin, idx_end,
                                   y_begin, y_end,
                                   y_fits_.begin(), y_fits_.end(),
                                   col_s);
        }
    }

public:

    template<typename K, typename Y, typename X>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const Y &y_begin, const Y &y_end,  // dependent variable
                const X &x_begin, const X &x_end)  {  // independent variable

        assert(frac_ >= 0 && frac_ <= 1);
        assert(loop_n_ > 2);

        if (sorted_)
            lowess_(idx_begin, idx_end, y_begin, y_end, x_begin, x_end);
        else  {  // Sort x and y in ascending order of x
            const size_type         col_s = std::distance(x_begin, x_end);
            std::vector<value_type> xvals (x_begin, x_end);
            std::vector<value_type> yvals (y_begin, y_end);
            std::vector<size_type>  sorting_idxs (col_s);

            std::iota(sorting_idxs.begin(), sorting_idxs.end(), 0);
            std::sort(sorting_idxs.begin(), sorting_idxs.end(),
                      [&xvals] (auto lhs, auto rhs) -> bool  {
                          return (xvals[lhs] < xvals[rhs]);
                      });
            std::sort(xvals.begin(), xvals.end(),
                      [] (auto lhs, auto rhs) -> bool  {
                          return (lhs < rhs);
                      });
            _sort_by_sorted_index_(yvals, sorting_idxs, col_s);
            lowess_(idx_begin, idx_end,
                    yvals.begin(), yvals.end(),
                    xvals.begin(), xvals.end());
        }
    }

    inline void pre ()  {

        y_fits_.clear();
        resid_weights_.clear();
        auxiliary_vec_.clear();
        x_j_.clear();
        dist_i_j_.clear();
    }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (y_fits_); }
    inline result_type &get_result ()  { return (y_fits_); }
    inline const result_type &
    get_residual_weights () const  { return (resid_weights_); }
    inline result_type &get_residual_weights ()  { return (resid_weights_); }

    explicit
    LowessVisitor (size_type loop_n = 3,
                   value_type frac = two_ / three_,
                   value_type delta = 0,
                   bool sorted = false)
        : frac_(frac),
          loop_n_(loop_n + 1),
          delta_(delta),
          sorted_(sorted)  {   }

private:

    // Between 0 and 1. The fraction of the data used when estimating
    // each y-value.
    //
    const value_type            frac_;
    // The number of residual-based reweightings to perform.
    //
    const size_type             loop_n_;
    // Distance within which to use linear-interpolation instead of weighted
    // regression.
    //
    const value_type            delta_;
    // Are x and y vectors sorted in the ascending order of values in x vector
    //
    const bool                  sorted_;

    result_type                 y_fits_ {  };
    result_type                 resid_weights_ {  };

    result_type                 auxiliary_vec_ {  };
    result_type                 x_j_ {  };
    result_type                 dist_i_j_ {  };
    static constexpr value_type z_ { 0 };
    static constexpr value_type one_ { 1 };
    static constexpr value_type two_ { 2 };
    static constexpr value_type three_ { 3 };
    static constexpr value_type six_ { 6 };
};

template<typename T, typename I = unsigned long>
using lowess_v = LowessVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DecomposeVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

private:

    template<typename K, typename H>
    inline void
    do_trend_(const K &idx_begin, const K &idx_end,
              const H &y_begin, const H &y_end,
              size_type col_s,
              std::vector<value_type> &xvals)  {

        std::iota(xvals.begin(), xvals.end(), 0);

        LowessVisitor<T, I> l_v (3, frac_, delta_ * value_type(col_s), true);

        // Calculate trend
        //
        l_v.pre();
        l_v (idx_begin, idx_end, y_begin, y_end, xvals.begin(), xvals.end());
        l_v.post();
        trend_ = std::move(l_v.get_result());
    }

    template<typename MEAN, typename K>
    inline void
    do_seasonal_(size_type col_s, const K &idx_begin, const K &idx_end,
                 const std::vector<value_type> &detrended)  {

        StepRollAdopter<MEAN, value_type, I>    sr_mean (MEAN(), s_period_);

        // Calculate one-period seasonality
        //
        seasonal_.resize(col_s, 0);
        for (size_type i = 0; i < s_period_; ++i)  {
            sr_mean.pre();
            sr_mean (idx_begin + i, idx_end,
                     detrended.begin() + i, detrended.end());
            sr_mean.post();
            seasonal_[i] = sr_mean.get_result();
        }

        // [01]-center the period means depending on the type
        //
        MEAN    m_v;

        m_v.pre();
        m_v (idx_begin, idx_begin + s_period_,
             seasonal_.begin(), seasonal_.begin() + s_period_);
        m_v.post();

        const value_type    result = m_v.get_result();

        if (type_ == decompose_type::additive)  {
            for (size_type i = 0; i < s_period_; ++i)
                seasonal_[i] -= result;
        }
        else  {
            for (size_type i = 0; i < s_period_; ++i)
                seasonal_[i] /= result;
        }

        // Tile the one-time seasone over the seasonal_ vector
        //
        for (size_type i = s_period_; i < col_s; ++i)
            seasonal_[i] = seasonal_[i % s_period_];
    }

    inline void
    do_residual_(const std::vector<value_type> &detrended, size_type col_s)  {

        // What is left is residual
        //
        residual_.resize(col_s, 0);
        if (type_ == decompose_type::additive)
            std::transform(detrended.begin(), detrended.end(),
                           seasonal_.begin(),
                           residual_.begin(),
                           std::minus<value_type>());
        else
            std::transform(detrended.begin(), detrended.end(),
                           seasonal_.begin(),
                           residual_.begin(),
                           std::divides<value_type>());
    }

public:

    template<typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &y_begin, const H &y_end)  {

        const size_type col_s = std::distance(y_begin, y_end);

        assert(s_period_ <= col_s / 2);

        std::vector<value_type> xvals (col_s);

        do_trend_(idx_begin, idx_end, y_begin, y_end, col_s, xvals);

        // We want to reuse the vector, so just rename it.
        // This way nobody gets confused
        //
        std::vector<value_type> &detrended = xvals;

        // Remove trend from observations in y
        //
        if (type_ == decompose_type::additive)
            std::transform(y_begin, y_end,
                           trend_.begin(),
                           detrended.begin(),
                           std::minus<value_type>());
        else
            std::transform(y_begin, y_end,
                           trend_.begin(),
                           detrended.begin(),
                           std::divides<value_type>());

        if (type_ == decompose_type::additive)
            do_seasonal_<MeanVisitor<T, I>>
                (col_s, idx_begin, idx_end, detrended);
        else
            do_seasonal_<GeometricMeanVisitor<T, I>>
                (col_s, idx_begin, idx_end, detrended);

        do_residual_(detrended, col_s);
    }

    inline void pre ()  {

        trend_.clear();
        seasonal_.clear();
        residual_.clear();
    }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (trend_); }
    inline result_type &get_result ()  { return (trend_); }
    inline const result_type &get_trend () const  { return (trend_); }
    inline result_type &get_trend ()  { return (trend_); }
    inline const result_type &get_seasonal () const  { return (seasonal_); }
    inline result_type &get_seasonal ()  { return (seasonal_); }
    inline const result_type &get_residual () const  { return (residual_); }
    inline result_type &get_residual ()  { return (residual_); }

    DecomposeVisitor (size_type s_period,
                      value_type frac,
                      value_type delta,
                      decompose_type t = decompose_type::additive)
        : frac_(frac), s_period_(s_period), delta_(delta), type_(t)  {   }

private:

    // Between 0 and 1. The fraction of the data used when estimating
    // each y-value.
    //
    const value_type        frac_;
    // Seasonal period in unit of one observation. There must be at least
    // two seasons in the data
    //
    const size_type         s_period_;
    // Distance within which to use linear-interpolation instead of weighted
    // regression. 0 or small values cause longer/more accurate processing
    //
    const value_type        delta_;
    const decompose_type    type_;

    result_type             trend_ {  };
    result_type             seasonal_ {  };
    result_type             residual_ {  };
};

template<typename T, typename I = unsigned long>
using decom_v = DecomposeVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct  EntropyVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin, const K &idx_end,
                const H &column_begin, const H &column_end)  {

        if (roll_count_ == 0)  return;

        GET_COL_SIZE

        SimpleRollAdopter<SumVisitor<T, I>, T, I>   sum_v(SumVisitor<T, I>(),
                                                          roll_count_);

        sum_v.pre();
        sum_v (idx_begin, idx_end, column_begin, column_end);
        sum_v.post();

        result_type result = std::move(sum_v.get_result());

        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    val = *(column_begin + i) / result[i];

            result[i] = (-val * std::log(val) / std::log(log_base_));
        }

        sum_v.pre();
        sum_v (idx_begin + (roll_count_ - 1), idx_end,
               result.begin() + (roll_count_ - 1), result.end());
        sum_v.post();

        for (size_type i = 0; i < roll_count_ - 1; ++i)
            result[i] = get_nan<value_type>();
        for (size_type i = 0; i < sum_v.get_result().size(); ++i)
            result[i + roll_count_ - 1] = sum_v.get_result()[i];

        result_.swap(result);
    }

    DEFINE_PRE_POST
    DEFINE_RESULT

    explicit
    EntropyVisitor(size_type roll_count, value_type log_base = 2)
        : roll_count_(roll_count), log_base_(log_base)  {   }

private:

    const size_type     roll_count_;
    const value_type    log_base_;
    result_type         result_ { };
};

template<typename T, typename I = unsigned long>
using ent_v = EntropyVisitor<T, I>;

// ----------------------------------------------------------------------------

template<typename V>
inline bool
is_normal(const V &column, double epsl, bool check_for_standard)  {

    using value_type = typename V::value_type;

    const int                       dummy_idx { int() };
    StatsVisitor<value_type, int>   svisit;

    svisit.pre();
    for (auto citer : column)
        svisit(dummy_idx, citer);
    svisit.post();

    const value_type    mean = static_cast<value_type>(svisit.get_mean());
    const value_type    std = static_cast<value_type>(svisit.get_std());
    const value_type    high_band_1 = static_cast<value_type>(mean + std);
    const value_type    low_band_1 = static_cast<value_type>(mean - std);
    double              count_1 = 0.0;
    const value_type    high_band_2 = static_cast<value_type>(mean + std * 2.0);
    const value_type    low_band_2 = static_cast<value_type>(mean - std * 2.0);
    double              count_2 = 0.0;
    const value_type    high_band_3 = static_cast<value_type>(mean + std * 3.0);
    const value_type    low_band_3 = static_cast<value_type>(mean - std * 3.0);
    double              count_3 = 0.0;

    for (auto citer : column)  {
        if (citer >= low_band_1 && citer < high_band_1)  {
            count_3 += 1;
            count_2 += 1;
            count_1 += 1;
        }
        else if (citer >= low_band_2 && citer < high_band_2)  {
            count_3 += 1;
            count_2 += 1;
        }
        else if (citer >= low_band_3 && citer < high_band_3)  {
            count_3 += 1;
        }
    }

    const double    col_s = static_cast<double>(column.size());

    if (std::fabs((count_1 / col_s) - 0.68) <= epsl &&
        std::fabs((count_2 / col_s) - 0.95) <= epsl &&
        std::fabs((count_3 / col_s) - 0.997) <= epsl)  {
        if (check_for_standard)
            return (std::fabs(mean - 0) <= epsl &&
                    std::fabs(std - 1.0) <= epsl);
        return (true);
    }
    return (false);
}

// ----------------------------------------------------------------------------

template<typename V>
inline bool
is_lognormal(const V &column, double epsl)  {

    using value_type = typename V::value_type;

    const int                       dummy_idx { int() };
    StatsVisitor<value_type, int>   svisit;
    StatsVisitor<value_type, int>   log_visit;

    svisit.pre();
    for (auto citer : column)  {
        svisit(dummy_idx, static_cast<value_type>(std::log(citer)));
        log_visit(dummy_idx, citer);
    }
    svisit.post();

    const value_type    mean = static_cast<value_type>(svisit.get_mean());
    const value_type    std = static_cast<value_type>(svisit.get_std());
    const value_type    high_band_1 = static_cast<value_type>(mean + std);
    const value_type    low_band_1 = static_cast<value_type>(mean - std);
    double              count_1 = 0.0;
    const value_type    high_band_2 = static_cast<value_type>(mean + std * 2.0);
    const value_type    low_band_2 = static_cast<value_type>(mean - std * 2.0);
    double              count_2 = 0.0;
    const value_type    high_band_3 = static_cast<value_type>(mean + std * 3.0);
    const value_type    low_band_3 = static_cast<value_type>(mean - std * 3.0);
    double              count_3 = 0.0;

    for (auto citer : column)  {
        const auto  log_val = std::log(citer);

        if (log_val >= low_band_1 && log_val < high_band_1)  {
            count_3 += 1;
            count_2 += 1;
            count_1 += 1;
        }
        else if (log_val >= low_band_2 && log_val < high_band_2)  {
            count_3 += 1;
            count_2 += 1;
        }
        else if (log_val >= low_band_3 && log_val < high_band_3)  {
            count_3 += 1;
        }
    }

    const double    col_s = static_cast<double>(column.size());

    if (std::fabs((count_1 / col_s) - 0.68) <= epsl &&
        std::fabs((count_2 / col_s) - 0.95) <= epsl &&
        std::fabs((count_3 / col_s) - 0.997) <= epsl &&
        log_visit.get_skew() > 10.0 * svisit.get_skew() &&
        log_visit.get_kurtosis() > 10.0 * svisit.get_kurtosis())
        return (true);
    return (false);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
