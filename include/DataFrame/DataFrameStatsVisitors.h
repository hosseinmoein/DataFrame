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

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <functional>
#include <iterator>
#include <map>
#include <numeric>

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

template<typename T, typename I = unsigned long>
struct MeanVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        mean_ += val;
        cnt_ += 1;
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { mean_ = 0; cnt_ = 0; }
    inline void post ()  {  }
    inline size_type get_count () const  { return (cnt_); }
    inline value_type get_sum () const  { return (mean_); }
    inline result_type get_result () const  {

        return (mean_ / value_type(cnt_));
    }

    explicit MeanVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  mean_ { 0 };
    size_type   cnt_ { 0 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct GeometricMeanVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        mean_ *= val;
        cnt_ += 1;
    }
    inline void pre ()  { mean_ = value_type(1); cnt_ = 0; }
    inline void post ()  {  }
    inline size_type get_count () const  { return (cnt_); }
    inline value_type get_sum () const  { return (mean_); }
    inline result_type get_result () const  {

        return (std::pow(mean_, value_type(1) / value_type(cnt_)));
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    explicit GeometricMeanVisitor(bool skipnan = true)
            : skip_nan_(skipnan)  {   }

private:

    value_type  mean_ { 1 };
    size_type   cnt_ { 0 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct HarmonicMeanVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        mean_ += value_type(1) / val;
        cnt_ += 1;
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { mean_ = 0; cnt_ = 0; }
    inline void post ()  {  }
    inline size_type get_count () const  { return (cnt_); }
    inline value_type get_sum () const  { return (mean_); }
    inline result_type get_result () const  {

        return (value_type(cnt_) / mean_);
    }

    explicit HarmonicMeanVisitor(bool skipnan = true)
        : skip_nan_(skipnan)  {   }

private:

    value_type  mean_ { 0 };
    size_type   cnt_ { 0 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct SumVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        sum_ += val;
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { sum_ = value_type { }; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (sum_); }

    explicit SumVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  sum_ { 0 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct ProdVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        prod_ *= val;
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { prod_ = 1; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (prod_); }

    explicit ProdVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  prod_ { 1 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MaxVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (is_nan__(val))  {
            if (skip_nan_)  return;
            else  {
                max_ = std::numeric_limits<value_type>::quiet_NaN();
                is_first = false;
            }
        }

        if (val > max_ || is_first) {
            max_ = val;
            index_ = idx;
            is_first = false;
        }
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { is_first = true; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (max_); }
    inline index_type get_index () const  { return (index_); }

    explicit MaxVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  max_ { };
    index_type  index_ { };
    bool        is_first { true };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MinVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (is_nan__(val))  {
            if (skip_nan_)  return;
            else  {
                min_ = std::numeric_limits<value_type>::quiet_NaN();
                is_first = false;
            }
        }

        if (val < min_ || is_first) {
            min_ = val;
            index_ = idx;
            is_first = false;
        }
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { is_first = true; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (min_); }
    inline index_type get_index () const  { return (index_); }

    explicit MinVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  min_ { };
    index_type  index_ { };
    bool        is_first { true };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

// This visitor takes, at most (when sequance is already sorted), O(N*M)
// time, where N is the number of largest values and M is the total number
// of all values. The assumption is that N should be relatively small, so the
// complexity is not bad.
// I could have used a priority queue, but that requires a std::vector
// instaed of std::array. I think the advantage of using std::array is bigger
// than O(MlogM) vs. O(N*M) for majority of usage.
//
template<std::size_t N, typename T, typename I = unsigned long>
struct  NLargestVisitor {

    DEFINE_VISIT_BASIC_TYPES

    struct  DataItem  {
        value_type  value { };
        index_type  index { };
    };

    using result_type = std::array<DataItem, N>;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        if (counter_ < N)  {
            items_[counter_] = { val, idx };
            if (min_index_ < 0 || val < items_[min_index_].value)
                min_index_ = static_cast<int>(counter_);
        }
        else if (val > items_[min_index_].value)  {
            items_[min_index_] = { val, idx };
            min_index_ = 0;
            for (int i = 1; i < N; ++i)
                if (items_[i].value < items_[min_index_].value)
                    min_index_ = i;
        }

        counter_ += 1;
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { counter_ = 0; min_index_ = -1; }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (items_); }

    inline void sort_by_index()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.index < rhs.index);
                  });
    }
    inline void sort_by_value()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.value < rhs.value);
                  });
    }

    explicit NLargestVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    result_type items_ { };
    size_type   counter_ { 0 };
    int         min_index_ { -1 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

// This visitor takes, at most (when the sequance is already sorted), O(N*M)
// time, where N is the number of largest values and M is the total number
// of all values. The assumption is that N should be relatively small, so the
// complexity is not bad.
// I could have used a priority queue, but that requires a std::vector
// instaed of std::array. I think the advantage of using std::array is bigger
// than O(MlogM) vs. O(N*M) for majority of usage.
//
template<std::size_t N, typename T, typename I = unsigned long>
struct  NSmallestVisitor {

    DEFINE_VISIT_BASIC_TYPES

    struct  DataItem  {
        value_type  value { };
        index_type  index { };
    };

    using result_type = std::array<DataItem, N>;

    inline void operator() (const index_type &idx, const value_type &val)  {

        if (skip_nan_ && is_nan__(val))  return;

        if (counter_ < N)  {
            items_[counter_] = { val, idx };
            if (max_index_ < 0 || val > items_[max_index_].value)
                max_index_ = static_cast<int>(counter_);
        }
        else if (val < items_[max_index_].value)  {
            items_[max_index_] = { val, idx };
            max_index_ = 0;
            for (int i = 1; i < N; ++i)
                if (items_[i].value > items_[max_index_].value)
                    max_index_ = i;
        }

        counter_ += 1;
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { counter_ = 0; max_index_ = -1; }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (items_); }

    inline void sort_by_index()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.index < rhs.index);
                  });
    }
    inline void sort_by_value()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.value < rhs.value);
                  });
    }

    explicit NSmallestVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    result_type items_ { };
    size_type   counter_ { 0 };
    int         max_index_ { -1 };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CovVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    explicit CovVisitor (bool biased = false, bool skipnan = true)
        : b_ (biased), skip_nan_(skipnan) {  }
    inline void operator() (const index_type &,
                            const value_type &val1,
                            const value_type &val2)  {

        if (skip_nan_ && (is_nan__(val1) || is_nan__(val2)))  return;

        total1_ += val1;
        total2_ += val2;
        dot_prod_ += (val1 * val2);
        dot_prod1_ += (val1 * val1);
        dot_prod2_ += (val2 * val2);
        cnt_ += 1;
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end,
                H column_begin1, H column_end1,
                H column_begin2, H column_end2)  {

        while (column_begin1 < column_end1 && column_begin2 < column_end2)
            (*this)(*idx_begin, *column_begin1++, *column_begin2++);
    }

    inline void pre ()  {

        total1_ = total2_ = dot_prod_ = dot_prod1_ = dot_prod2_ = 0;
        cnt_ = 0;
    }
    inline void post ()  {  }
    inline result_type get_result () const  {

        const value_type    b = b_ ? 0 : 1;

        return ((dot_prod_ - (total1_ * total2_) / value_type(cnt_)) /
                (value_type(cnt_) - b));
    }

    inline value_type get_var1 () const  {

        const value_type    b = b_ ? 0 : 1;

        return ((dot_prod1_ - (total1_ * total1_) / value_type(cnt_)) /
                (value_type(cnt_) - b));
    }
    inline value_type get_var2 () const  {

        const value_type    b = b_ ? 0 : 1;

        return ((dot_prod2_ - (total2_ * total2_) / value_type(cnt_)) /
                (value_type(cnt_) - b));
    }

    inline size_type get_count() const  { return (cnt_); }

private:

    value_type  total1_ { 0 };
    value_type  total2_ { 0 };
    value_type  dot_prod_ { 0 };
    value_type  dot_prod1_ { 0 };
    value_type  dot_prod2_ { 0 };
    size_type   cnt_ { 0 };
    const bool  b_;
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct VarVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    explicit VarVisitor (bool biased = false) : cov_ (biased)  {   }
    inline void operator() (const index_type &idx, const value_type &val)  {

        cov_ (idx, val, val);
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline result_type get_result () const  { return (cov_.get_result()); }
    inline size_type get_count() const  { return (cov_.get_count()); }

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

    explicit BetaVisitor (bool biased = false) : cov_ (biased)  {   }
    inline void operator() (const index_type &idx,
                            const value_type &val1,
                            const value_type &benchmark)  {

        cov_ (idx, val1, benchmark);
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end,
                H column_begin1, H column_end1,
                H benchmark_begin, H benchmark_end)  {

        while (column_begin1 < column_end1 && benchmark_begin < benchmark_end)
            (*this)(*idx_begin, *column_begin1++, *benchmark_begin++);
    }

    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline result_type get_result () const  {

        return (cov_.get_var2() != 0.0
                    ? cov_.get_result() / cov_.get_var2()
                    : std::numeric_limits<value_type>::quiet_NaN());
    }
    inline size_type get_count() const  { return (cov_.get_count()); }

private:

    CovVisitor<value_type, index_type>  cov_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct StdVisitor   {

    DEFINE_VISIT_BASIC_TYPES_2

    explicit StdVisitor (bool biased = false) : var_ (biased)  {   }
    inline void operator() (const index_type &idx, const value_type &val)  {

        var_ (idx, val);
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { var_.pre(); }
    inline void post ()  { var_.post(); }
    inline result_type get_result () const  {

        return (::sqrt(var_.get_result()));
    }
    inline size_type get_count() const  { return (var_.get_count()); }

private:

    VarVisitor<value_type, index_type>  var_;
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

    explicit SEMVisitor (bool biased = false) : std_ (biased)  {   }
    inline void operator() (const index_type &idx, const value_type &val)  {

        std_ (idx, val);
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  { std_.pre(); }
    inline void post ()  { std_.post(); }
    inline result_type get_result () const  {

        return (std_.get_result() / ::sqrt(get_count()));
    }
    inline size_type get_count() const  { return (std_.get_count()); }

private:

    StdVisitor<value_type, index_type>  std_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct TrackingErrorVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    explicit TrackingErrorVisitor (bool biased = false) : std_ (biased) {  }
    inline void operator() (const index_type &idx,
                            const value_type &val1,
                            const value_type &val2)  {

        std_ (idx, val1 - val2);
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end,
                H column_begin1, H column_end1,
                H column_begin2, H column_end2)  {

        while (column_begin1 < column_end1 && column_begin2 < column_end2)
            (*this)(*idx_begin, *column_begin1++, *column_begin2++);
    }

    inline void pre ()  { std_.pre(); }
    inline void post ()  { std_.post();  }
    inline result_type get_result () const  { return (std_.get_result()); }

private:

    StdVisitor<value_type, index_type>  std_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct CorrVisitor  {

public:

    DEFINE_VISIT_BASIC_TYPES_2

    explicit CorrVisitor (bool biased = false) : cov_ (biased)  {   }
    inline void operator() (const index_type &idx,
                            const value_type &val1,
                            const value_type &val2)  {

        cov_ (idx, val1, val2);
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end,
                H column_begin1, H column_end1,
                H column_begin2, H column_end2)  {

        while (column_begin1 < column_end1 && column_begin2 < column_end2)
            (*this)(*idx_begin, *column_begin1++, *column_begin2++);
    }

    inline void pre ()  { cov_.pre(); }
    inline void post ()  { cov_.post(); }
    inline result_type get_result () const  {

        return (cov_.get_result() /
                (::sqrt(cov_.get_var1()) * ::sqrt(cov_.get_var2())));
    }

private:

    CovVisitor<value_type, index_type>  cov_;
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DotProdVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const index_type &,
                            const value_type &val1,
                            const value_type &val2)  {

        dot_prod_ += (val1 * val2);
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end,
                H column_begin1, H column_end1,
                H column_begin2, H column_end2)  {

        while (column_begin1 < column_end1 && column_begin2 < column_end2)
            (*this)(*idx_begin, *column_begin1++, *column_begin2++);
    }

    inline void pre ()  { dot_prod_ = value_type(0); }
    inline void post ()  {  }
    inline result_type get_result () const  { return (dot_prod_); }

private:

    result_type dot_prod_ { 0 };
};

// ----------------------------------------------------------------------------

// Simple rolling adoptor for visitors
//
template<typename F, typename T, typename I = unsigned long>
struct  SimpleRollAdopter  {

private:

    using visitor_type = F;
    using f_result_type = typename visitor_type::result_type;

    visitor_type                visitor_ { };
    const size_t                roll_count_ { 0 };
    std::vector<f_result_type>  result_ { };

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<f_result_type>;

    inline SimpleRollAdopter(F &&functor, size_type r_count)
        : visitor_(std::move(functor)), roll_count_(r_count)  {   }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        if (roll_count_ == 0)  return;

        const size_type idx_size =
            static_cast<size_type>(std::distance(idx_begin, idx_end));
        const size_type col_size = std::distance(column_begin, column_end);
        const size_type col_s = std::min(idx_size, col_size);

        result_.reserve(col_s);

        for (size_type i = 0; i < roll_count_ - 1 && i < col_s; ++i)
            result_.push_back(std::numeric_limits<f_result_type>::quiet_NaN());
        for (size_type i = 0; i < col_s; ++i)  {
            size_type   r = 0;

            visitor_.pre();
            for (size_type j = i; r < roll_count_ && j < col_s; ++j, ++r)
                visitor_(*(idx_begin + j), *(column_begin + j));
            visitor_.post();
            if (r == roll_count_)
                result_.push_back(visitor_.get_result());
        }
    }

    inline void pre ()  { visitor_.pre(); result_.clear(); }
    inline void post ()  { visitor_.post(); }
    inline const result_type &get_result () const  { return (result_); }
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
    const size_t                init_roll_count_ { 0 };
    const size_t                increment_count_ { 0 };
    std::vector<f_result_type>  result_ { };

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<f_result_type>;

    inline ExpandingRollAdopter(F &&functor,
                                size_t r_count,
                                size_t i_count = 1)
        : visitor_(std::move(functor)),
          init_roll_count_(r_count),
          increment_count_(i_count)  {   }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        if (init_roll_count_ == 0)  return;

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);
        const size_t    col_s = std::min(idx_size, col_size);

        result_.reserve(col_s);

        size_t  rc = init_roll_count_;

        for (size_t i = 0; i < rc - 1 && i < col_s; ++i)
            result_.push_back(std::numeric_limits<f_result_type>::quiet_NaN());

        for (size_t i = 0; i < col_s; ++i, rc += increment_count_)  {
            size_t  r = 0;

            visitor_.pre();
            for (size_t j = i; r < rc && j < col_s; ++j, ++r)
                visitor_(*(idx_begin + j), *(column_begin + j));
            visitor_.post();
            if (r == rc)
                result_.push_back(visitor_.get_result());
            else  break;
        }
    }

    inline void pre ()  { visitor_.pre(); result_.clear(); }
    inline void post ()  { visitor_.post(); }
    inline const result_type &get_result () const  { return (result_); }
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
    const size_t                roll_count_;
    const double                decay_;
    const bool                  skip_nan_;

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<f_result_type>;

    inline ExponentialRollAdopter(F &&functor,
                                  size_t r_count,
                                  exponential_decay_spec eds,
                                  double value,
                                  bool skip_nan = true)
        : visitor_(std::move(functor)),
          roll_count_(r_count),
          decay_(eds == exponential_decay_spec::center_of_gravity
                     ? 1.0 / (1.0 + value)
                     : eds == exponential_decay_spec::span
                         ? 2.0 / (1.0 + value)
                         : eds == exponential_decay_spec::halflife
                             ? 1.0 - std::exp(std::log(0.5) / value)
                             : value),
          skip_nan_(skip_nan)  {   }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);
        const size_t    col_s = std::min(idx_size, col_size);

        if (roll_count_ == 0 || roll_count_ >= col_s)  return;

        result_.resize(col_s, std::numeric_limits<f_result_type>::quiet_NaN());

        size_t  i = 0;

        visitor_.pre();
        for (; i < roll_count_; ++i)
            visitor_(*(idx_begin + i), *(column_begin + i));
        visitor_.post();
        result_[--i] = visitor_.get_result();
        i += 1;

        for (; i < col_s; ++i)  {
            if (skip_nan_ && is_nan__(*(column_begin + i)))  continue;
            result_[i] =
                decay_ * *(column_begin + i) +
                ((1.0 - decay_) * result_[i - 1]);
        }
    }

    inline void pre ()  { visitor_.pre(); result_.clear(); }
    inline void post ()  { visitor_.post(); }
    inline const result_type &get_result () const  { return (result_); }
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

        if (skip_nan_ && is_nan__(val))  return;

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
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end, H column_begin, H column_end)  {

        while (column_begin < column_end)
            (*this)(*idx_begin, *column_begin++);
    }

    inline void pre ()  {

        n_ = 0;
        m1_ = m2_ = m3_ = m4_ = value_type(0);
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

    explicit StatsVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    size_type   n_ { 0 };
    value_type  m1_ { 0 };
    value_type  m2_ { 0 };
    value_type  m3_ { 0 };
    value_type  m4_ { 0 };
    const bool  skip_nan_ { };
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
                            const value_type &x,
                            const value_type &y)  {

        if (skip_nan_ && (is_nan__(x) || is_nan__(y)))  return;

        s_xy_ += (x_stats_.get_mean() - x) *
                 (y_stats_.get_mean() - y) *
                 value_type(n_) / value_type(n_ + 1);

        x_stats_(idx, x);
        y_stats_(idx, y);
        n_ += 1;
    }
    template <typename K, typename H>
    inline void
    operator() (K idx_begin, K idx_end,
                H x_begin, H x_end,
                H y_begin, H y_end)  {

        while (x_begin < x_end && y_begin < y_end)
            (*this)(*idx_begin, *x_begin++, *y_begin++);
    }

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
    value_type                              s_xy_ { 0 };
    StatsVisitor<value_type, index_type>    x_stats_ {  };
    StatsVisitor<value_type, index_type>    y_stats_ {  };
    const bool                              skip_nan_ { };
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
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        value_type      running_sum = 0;
        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);
        const size_type col_s = std::min(idx_size, col_size);

        sum_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (! skip_nan_ || ! is_nan__(value))  {
                running_sum += value;
                sum_.push_back(running_sum);
            }
            else
                sum_.push_back(value);
        }
    }
    inline void pre ()  { sum_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (sum_); }

    explicit CumSumVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    std::vector<value_type> sum_ {  };
    const bool              skip_nan_ { };
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
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);
        value_type      running_prod = 1;
        const size_type col_s = std::min(idx_size, col_size);

        prod_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (! skip_nan_ || ! is_nan__(value))  {
                running_prod *= value;
                prod_.push_back(running_prod);
            }
            else
                prod_.push_back(value);
        }
    }
    inline void pre ()  { prod_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (prod_); }

    explicit CumProdVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    std::vector<value_type> prod_ {  };
    const bool              skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct CumMaxVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);

        if (col_size == 0)  return;

        value_type      running_max = *column_begin;
        const size_type col_s = std::min(idx_size, col_size);

        max_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (! skip_nan_ || ! is_nan__(value))  {
                if (value > running_max)
                    running_max = value;
                max_.push_back(running_max);
            }
            else
                max_.push_back(value);
        }
    }
    inline void pre ()  { max_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (max_); }

    explicit CumMaxVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    std::vector<value_type> max_ {  };
    const bool              skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct CumMinVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);

        if (col_size == 0)  return;

        value_type      running_min = *column_begin;
        const size_type col_s = std::min(idx_size, col_size);

        min_.reserve(col_s);
        for (size_type i = 0; i < col_s; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (! skip_nan_ || ! is_nan__(value))  {
                if (value < running_min)
                    running_min = value;
                min_.push_back(running_min);
            }
            else
                min_.push_back(value);
        }
    }
    inline void pre ()  { min_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (min_); }

    explicit CumMinVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    std::vector<value_type> min_ {  };
    const bool              skip_nan_ { };
};

// ----------------------------------------------------------------------------

// This categorizes the values in the column with integer values starting
// from 0
//
template<typename T, typename I = unsigned long>
struct CategoryVisitor  {

private:

    struct t_less_  {
        inline bool
        operator() (const T *lhs, const T *rhs) const  { return (*lhs < *rhs); }
    };

public:

    DEFINE_VISIT_BASIC_TYPES
    using result_type = std::vector<size_type>;

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type c_len = std::distance(column_begin, column_end);

        result_.reserve(c_len);

        size_type   cat = nan_ != 0 ? 0 : 1;

        for (size_type i = 0; i < c_len; ++i)  {
            const value_type    &value = *(column_begin + i);

            if (is_nan__(value))  {
                result_.push_back(nan_);
                continue;
            }

            const typename map_type::const_iterator citer =
                cat_map_.find(&value);

            if (citer == cat_map_.end())  {
                cat_map_.insert({ &value, cat });
                result_.push_back(cat);
                cat += 1;
                if (cat == nan_)  cat += 1;
            }
            else
                result_.push_back(citer->second);
        }
    }

    explicit
    CategoryVisitor(size_type nan_val = size_type(-1)) : nan_(nan_val)  {   }

    inline void pre ()  { result_.clear(); cat_map_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }

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
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type         c_len = std::distance(column_begin, column_end);
        std::vector<size_type>  rank_vec(c_len);

        std::iota(rank_vec.begin(), rank_vec.end(), 0);
        std::stable_sort(
            rank_vec.begin(), rank_vec.end(),
            [&column_begin](size_type lhs, size_type rhs) -> bool {
                return *(column_begin + lhs) < *(column_begin + rhs);
            });
        result_.resize(c_len);

        const value_type    *prev_value = &*(column_begin + rank_vec[0]);

        for (size_type i = 0; i < c_len; ++i)  {
            double      avg_val = static_cast<double>(i);
            double      first_val = static_cast<double>(i);
            double      last_val = static_cast<double>(i);
            size_type   j = i + 1;

            for ( ; j < c_len && *prev_value == *(column_begin + rank_vec[j]);
                 ++j)  {
                last_val = static_cast<double>(j);
                avg_val += static_cast<double>(j);
            }
            avg_val /= double(j - i);

            switch(policy_)  {
                case rank_policy::average:
                {
                    for (; i < c_len && i < j; ++i)
                        result_[rank_vec[i]] = avg_val;
                    break;
                }
                case rank_policy::first:
                {
                    for (; i < c_len && i < j; ++i)
                        result_[rank_vec[i]] = first_val;
                    break;
                }
                case rank_policy::last:
                {
                    for (; i < c_len && i < j; ++i)
                        result_[rank_vec[i]] = last_val;
                    break;
                }
                case rank_policy::actual:
                {
                    for (; i < c_len && i < j; ++i)
                        result_[rank_vec[i]] = static_cast<double>(i);
                    break;
                }
            }
            if (i < c_len)
                prev_value = &*(column_begin + rank_vec[i]);
            i -= 1;  // Because the outer loop does ++i
        }
    }

    explicit
    RankVisitor(rank_policy p = rank_policy::actual) : policy_(p)  {   }

    inline void pre ()  { result_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }

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
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        result_.reserve(std::distance(column_begin, column_end));

        for (auto citer = column_begin; citer < column_end; ++citer)
            result_.push_back(ffunc_(*citer));
    }

    explicit FactorizeVisitor(factor_func f) : ffunc_(f)  {   }

    inline void pre ()  { result_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }

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

    AutoCorrVisitor () = default;
    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);
        const size_type col_len = std::min(idx_size, col_size);

        if (col_len <= 4)  return;

        std::vector<value_type>               tmp_result(col_len - 4);
        size_type                             lag = 1;
        const size_type                       thread_level =
            ThreadGranularity::get_sensible_thread_level();
        std::vector<std::future<CorrResult>>  futures(thread_level);
        size_type                             thread_count = 0;

        tmp_result[0] = 1.0;
        while (lag < col_len - 4)  {
            if (thread_count >= thread_level)  {
                const auto  result = get_auto_corr_(col_len, lag, column_begin);

                tmp_result[result.first] = result.second;
            }
            else  {
                futures[thread_count] =
                    std::async(std::launch::async,
                               &AutoCorrVisitor::get_auto_corr_<H>,
                               this,
                               col_len,
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
    inline void pre ()  { result_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (result_); }

private:

    result_type result_ {  };

    using CorrResult = std::pair<size_type, value_type>;

    template<typename H>
    inline CorrResult
    get_auto_corr_(size_type col_len,
                   size_type lag,
                   const H &column_begin) const  {

        CorrVisitor<value_type, index_type> corr {  };
        constexpr I                         dummy = I();

        corr.pre();
        for (size_type i = 0; i < col_len - lag; ++i)
            corr (dummy, *(column_begin + i), *(column_begin + i + lag));
        corr.post();

        return (CorrResult(lag, corr.get_result()));
    }
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct KthValueVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    explicit KthValueVisitor (size_type ke, bool skipnan = true)
        : kth_element_(ke), skip_nan_(skipnan)  {   }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &values_begin,
                const H &values_end)  {

        result_ = find_kth_element_(values_begin, values_end, kth_element_);
    }
    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

private:

    result_type     result_ {  };
    const size_type kth_element_;
    const bool      skip_nan_ { };

    template<typename It>
    inline value_type
    find_kth_element_ (It begin, It end, size_type k) const  {

        const size_type vec_size = std::distance(begin, end);

        if (k > vec_size || k <= 0)  {
            char    err[512];

            sprintf (err,
#ifdef _WIN32
                     "find_kth_element_(): vector length = %zu and k = %zu.",
#else
                     "find_kth_element_(): vector length = %lu and k = %lu.",
#endif // _WIN32
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

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct MedianVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    MedianVisitor () = default;
    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &values_begin,
                const H &values_end)  {

        const size_type                         vec_size =
            std::distance(values_begin, values_end);
        KthValueVisitor<value_type, index_type> kv_visitor (vec_size >> 1);


        kv_visitor.pre();
        kv_visitor(idx_begin, idx_end, values_begin, values_end);
        kv_visitor.post();
        result_ = kv_visitor.get_result();
        if (! (vec_size & 0x0001))  { // even
            KthValueVisitor<value_type, I>   kv_visitor2 ((vec_size >> 1) + 1);

            kv_visitor2.pre();
            kv_visitor2(idx_begin, idx_end, values_begin, values_end);
            kv_visitor2.post();
            result_ = (result_ + kv_visitor2.get_result()) / value_type(2);
        }
    }
    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

private:

    result_type result_ {  };
};

// ----------------------------------------------------------------------------

template<typename T, typename I = unsigned long>
struct QuantileVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    QuantileVisitor () = default;
    QuantileVisitor (double quantile, quantile_policy q_policy)
        : qt_(quantile), policy_(q_policy)  {   }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type vec_len = std::distance(column_begin, column_end);

        if (qt_ < 0.0 || qt_ > 1.0 || vec_len == 0)  {
            char buffer [512];

            sprintf (buffer,
                     "QuantileVisitor{}: unable to do quantile: "
#ifdef _WIN32
                     "qt: %f, Column Len: %zu",
#else
                     "qt: %f, Column Len: %lu",
#endif // _WIN32
                     qt_, vec_len);
            throw NotFeasible(buffer);
        }

        const double    vec_len_frac = qt_ * vec_len;
        const size_type int_idx =
            static_cast<size_type>(std::round(vec_len_frac));
        const bool      need_two =
            ! (vec_len & 0x01) || double(int_idx) < vec_len_frac;

        if (qt_ == 0.0 || qt_ == 1.0)  {
            KthValueVisitor<T, I>   kth_value((qt_ == 0.0 ) ? 1 : vec_len);

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
            if (need_two && int_idx + 1 < vec_len)  {
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
                : (int_idx + 1 < vec_len && need_two ? int_idx + 1 : int_idx));

            kth_value.pre();
            kth_value(idx_begin, idx_end, column_begin, column_end);
            kth_value.post();
            result_ = kth_value.get_result();
        }
    }

    inline void pre ()  { result_ = value_type(); }
    inline void post ()  {   }
    inline result_type get_result () const  { return (result_); }

private:

    result_type             result_ {  };
    const double            qt_ { 0.5  };
    const quantile_policy   policy_ { quantile_policy::mid_point };
};

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
        const value_type                *value { nullptr };
        // List of indices where value occurred
        VectorConstPtrView<index_type>  indices { };

        // Number of times value occurred
        inline size_type repeat_count() const  { return (indices.size()); }

        // List of column indices where value occurred
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
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        DataItem        nan_item;
        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type column_size = std::distance(column_begin, column_end);
        const size_type col_size = std::min(idx_size, column_size);
        map_type        val_map;

        val_map.reserve(col_size);
        for (size_type i = 0; i < col_size; ++i)  {
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
            items_[i] = val_vec[i];
    }
    inline void pre ()  { result_type x; items_.swap (x); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (items_); }

    inline void sort_by_repeat_count()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (lhs.repeat_count() < rhs.repeat_count());
                  });
    }
    inline void sort_by_value()  {

        std::sort(items_.begin(), items_.end(),
                  [](const DataItem &lhs, const DataItem &rhs) -> bool  {
                      return (*(lhs.value) < *(rhs.value));
                  });
    }

private:

    result_type items_ { };
};

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

        const size_type     idx_size = std::distance(idx_begin, idx_end);
        const size_type     col_size = std::distance(column_begin, column_end);
        const std::size_t   col_s = std::min(idx_size, col_size);
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

        const size_type     idx_size = std::distance(idx_begin, idx_end);
        const size_type     col_size = std::distance(column_begin, column_end);
        const std::size_t   col_s = std::min(idx_size, col_size);
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
        const size_type     idx_size = std::distance(idx_begin, idx_end);
        const size_type     col_size = std::distance(column_begin, column_end);
        const std::size_t   col_s = std::min(idx_size, col_size);

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

        const size_type     idx_size = std::distance(idx_begin, idx_end);
        const size_type     col_size = std::distance(column_begin, column_end);
        const std::size_t   col_s = std::min(idx_size, col_size);
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
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

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

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct DiffVisitor  {

    DEFINE_VISIT_BASIC_TYPES_3

    explicit DiffVisitor(long periods = 1, bool skipnan = true)
        : periods_(periods), skip_nan_(skipnan) {  }

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        const size_type idx_size = std::distance(idx_begin, idx_end);
        const size_type col_size = std::distance(column_begin, column_end);
        const size_type col_len = std::min(idx_size, col_size);

        result_.reserve(col_len);
        if (periods_ >= 0)  {
            if (! skip_nan_)  {
                for (long i = 0;
                     i < periods_ && static_cast<size_type>(i) < col_len; ++i)
                    result_.push_back(
                        std::numeric_limits<value_type>::quiet_NaN());
            }

            auto    i = column_begin;

            i += periods_;
            for (auto j = column_begin; i < column_end; ++i, ++j) {
                if (skip_nan_ && (is_nan__(*i) || is_nan__(*j)))  continue;
                result_.push_back(*i - *j);
            }
        }
        else {
            auto    i = column_end;
            auto    j = column_end;

            i -= (1 + std::abs(periods_));
            j -= 1;
            for ( ; i >= column_begin; --i, --j) {
                if (skip_nan_ && (is_nan__(*i) || is_nan__(*j)))  continue;
                result_.push_back(*i - *j);
            }
            std::reverse(result_.begin(), result_.end());

            if (skip_nan_ )  return;
            for (size_type i = 0;
                 i < static_cast<size_type>(std::abs(periods_)) && i < col_len;
                 ++i)
                result_.push_back(
                    std::numeric_limits<value_type>::quiet_NaN());
        }
    }

    inline void pre ()  { result_.clear(); }
    inline void post ()  {   }
    inline const result_type &get_result () const  { return (result_); }

private:

    result_type result_ { };
    const long  periods_ { };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct ZScoreVisitor {

    DEFINE_VISIT_BASIC_TYPES_3

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        MeanVisitor<value_type> mvisit;
        StdVisitor<value_type>  svisit;
        const size_type         idx_size = std::distance(idx_begin, idx_end);
        const size_type         col_size =
            std::distance(column_begin, column_end);
        const size_type         col_s = std::min(idx_size, col_size);

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

        zscore_.reserve(col_s);
        for (auto citer = column_begin; citer < column_end; ++citer)
            zscore_.push_back((*citer - m) / s);
    }
    inline void pre ()  { zscore_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (zscore_); }
    inline result_type &get_result ()  { return (zscore_); }

    explicit ZScoreVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    result_type zscore_ {  };
    const bool  skip_nan_ { };
};

// ----------------------------------------------------------------------------

template<typename T,
         typename I = unsigned long,
         typename =
             typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
struct SampleZScoreVisitor {

    DEFINE_VISIT_BASIC_TYPES_2

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &population_begin,
                const H &population_end,
                const H &sample_begin,
                const H &sample_end)  {

        MeanVisitor<value_type> p_mvisit;
        StdVisitor<value_type>  p_svisit;
        MeanVisitor<value_type> s_mvisit;
        const size_type         p_col_s =
            std::distance(population_begin, population_end);
        const size_type         s_col_s =
            std::distance(sample_begin, sample_end);
        const size_type         max_s = std::max(p_col_s, s_col_s);

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

        zscore_ = (s_mvisit.get_result() - p_mvisit.get_result()) /
                  (p_svisit.get_result() / ::sqrt(s_col_s));
    }
    inline void pre ()  { zscore_ = 0; }
    inline void post ()  {  }
    inline result_type get_result () const  { return (zscore_); }

    explicit
    SampleZScoreVisitor(bool skipnan = true) : skip_nan_(skipnan)  {   }

private:

    value_type  zscore_ {  };
    const bool  skip_nan_ { };
};

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
            sigmoids_.push_back(1.0 / (1.0 + std::exp(-(*citer))));
    }
    template <typename H>
    inline void algebraic_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            sigmoids_.push_back(1.0 / std::sqrt(1.0 + std::pow(*citer, 2.0)));
    }
    template <typename H>
    inline void hyperbolic_tan_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            sigmoids_.push_back(std::tanh(*citer));
    }
    template <typename H>
    inline void arc_tan_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            sigmoids_.push_back(std::atan(*citer));
    }
    template <typename H>
    inline void error_function_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            sigmoids_.push_back(std::erf(*citer));
    }
    template <typename H>
    inline void gudermannian_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)
            sigmoids_.push_back(std::atan(std::sinh(*citer)));
    }
    template <typename H>
    inline void smoothstep_(const H &column_begin, const H &column_end)  {

        for (auto citer = column_begin; citer < column_end; ++citer)  {
            if (*citer <= 0.0)
                sigmoids_.push_back(0.0);
            else if (*citer >= 1.0)
                sigmoids_.push_back(1.0);
            else
                sigmoids_.push_back(*citer * *citer * (3.0 - 2.0 * *citer));
        }
    }

public:

    template <typename K, typename H>
    inline void
    operator() (const K &idx_begin,
                const K &idx_end,
                const H &column_begin,
                const H &column_end)  {

        sigmoids_.reserve(std::distance(column_begin, column_end));
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
    inline void pre ()  { sigmoids_.clear(); }
    inline void post ()  {  }
    inline const result_type &get_result () const  { return (sigmoids_); }
    inline result_type &get_result ()  { return (sigmoids_); }

    explicit SigmoidVisitor(sigmoid_type st) : sigmoid_type_(st)  {   }

private:

    result_type         sigmoids_ {  };
    const sigmoid_type  sigmoid_type_;
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
