// Hossein Moein
// September 12, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>

#include <tuple>

// ----------------------------------------------------------------------------

namespace hmdf
{
#if defined(WIN32) || defined (_WIN32)
#undef min
#undef max
#endif // defined(WIN32) || defined (_WIN32)

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
join_by_index (const RHS_T &rhs, join_policy mp) const  {

    static_assert(std::is_base_of<StdDataFrame<IndexType>, RHS_T>::value ||
                      std::is_base_of<DataFrameView<IndexType>, RHS_T>::value,
                  "The rhs argument to join_by_index() can only be "
                  "StdDataFrame<IndexType> or DataFrameView<IndexType>");

    switch(mp)  {
        case join_policy::inner_join:
            return (index_inner_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case join_policy::left_join:
            return (index_left_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case join_policy::right_join:
            return (index_right_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
        case join_policy::left_right_join:
        default:
            return (index_left_right_join_
                        <decltype(*this), decltype(rhs), types ...>
                            (*this, rhs));
            break;
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
join_helper_(const LHS_T &lhs,
             const RHS_T &rhs,
             const IndexIdxVector &joined_index_idx)  {

    StdDataFrame<IndexType>    result;
    std::vector<IndexType>     result_index;

    // Load the index
    result_index.reserve(joined_index_idx.size());
    for (const auto &citer : joined_index_idx)  {
        const size_type left_i = std::get<0>(citer);

        result_index.push_back(
            left_i != std::numeric_limits<size_type>::max()
                ? lhs.indices_[left_i] : rhs.indices_[std::get<1>(citer)]);
    }
    result.load_index(std::move(result_index));

    // Load the common and lhs columns
    for (auto &iter : lhs.column_tb_)  {
        auto    rhs_citer = rhs.column_tb_.find(iter.first);

        // Common column between two frames
        if (rhs_citer != rhs.column_tb_.end())  {
            index_join_functor_common_<types ...> functor (iter.first.c_str(),
                                                           rhs,
                                                           joined_index_idx,
                                                           result);

            lhs.data_[iter.second].change(functor);

        }
        else  {  // lhs only column
            // 0 = Left
            index_join_functor_oneside_<0, types ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            lhs.data_[iter.second].change(functor);
        }
    }

    // Load the rhs columns
    for (auto &iter : rhs.column_tb_)  {
        auto    lhs_citer = lhs.column_tb_.find(iter.first);

        if (lhs_citer == lhs.column_tb_.end())  {  // rhs only column
            // 1 = Right
            index_join_functor_oneside_<1, types ...> functor (
                iter.first.c_str(),
                joined_index_idx,
                result);

            rhs.data_[iter.second].change(functor);
        }
    }

    return(result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_inner_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(std::min(lhs_end, rhs_end));
    while (lhs_current != lhs_end && rhs_current != rhs_end) {
        if (lhs.indices_[lhs_current] < rhs.indices_[rhs_current])
            lhs_current += 1;
        else  {
            if (lhs.indices_[lhs_current] == rhs.indices_[rhs_current])
                joined_index_idx.emplace_back(lhs_current++, rhs_current);
            rhs_current += 1;
        }
    }

    return (join_helper_<LHS_T, RHS_T, types ...>(lhs, rhs, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_left_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(lhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (lhs_current >= lhs_end)  break;
        if (rhs_current >= rhs_end)  {
            joined_index_idx.emplace_back(
                lhs_current++,
                std::numeric_limits<size_type>::max());
            continue;
        }

        if (lhs.indices_[lhs_current] < rhs.indices_[rhs_current])
            joined_index_idx.emplace_back(
                lhs_current++,
                std::numeric_limits<size_type>::max());
        else  {
            if (lhs.indices_[lhs_current] == rhs.indices_[rhs_current])
                joined_index_idx.emplace_back(lhs_current++, rhs_current);
            rhs_current += 1;
        }
    }

    return (join_helper_<LHS_T, RHS_T, types ...>(lhs, rhs, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_right_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(rhs_end);
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (rhs_current >= rhs_end)  break;
        if (lhs_current >= lhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                rhs_current++);
            continue;
        }

        if (lhs.indices_[lhs_current] < rhs.indices_[rhs_current])
            lhs_current += 1;
        else  {
            if (lhs.indices_[lhs_current] == rhs.indices_[rhs_current])
                joined_index_idx.emplace_back(lhs_current++, rhs_current);
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    rhs_current);
            rhs_current += 1;
        }
    }

    return (join_helper_<LHS_T, RHS_T, types ...>(lhs, rhs, joined_index_idx));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename LHS_T, typename RHS_T, typename ... types>
StdDataFrame<I> DataFrame<I, H>::
index_left_right_join_(const LHS_T &lhs, const RHS_T &rhs)  {

    size_type       lhs_current = 0;
    const size_type lhs_end = lhs.indices_.size();
    size_type       rhs_current = 0;
    const size_type rhs_end = rhs.indices_.size();

    IndexIdxVector  joined_index_idx;

    joined_index_idx.reserve(std::max(lhs_end, rhs_end));
    while (lhs_current != lhs_end || rhs_current != rhs_end) {
        if (lhs_current >= lhs_end && rhs_current < rhs_end)  {
            joined_index_idx.emplace_back(
                std::numeric_limits<size_type>::max(),
                rhs_current++);
            continue;
        }
        if (rhs_current >= rhs_end && lhs_current < lhs_end)  {
            joined_index_idx.emplace_back(
                lhs_current++,
                std::numeric_limits<size_type>::max());
            continue;
        }

        if (lhs.indices_[lhs_current] < rhs.indices_[rhs_current])  {
            joined_index_idx.emplace_back(
                lhs_current++,
                std::numeric_limits<size_type>::max());
        }
        else  {
            if (lhs.indices_[lhs_current] == rhs.indices_[rhs_current])
                joined_index_idx.emplace_back(lhs_current++, rhs_current);
            else
                joined_index_idx.emplace_back(
                    std::numeric_limits<size_type>::max(),
                    rhs_current);
            rhs_current += 1;
        }
    }

    return (join_helper_<LHS_T, RHS_T, types ...>(lhs, rhs, joined_index_idx));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
