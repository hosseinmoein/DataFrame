// Hossein Moein
// October 5, 2017
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

#include <DataFrame/DataFrame.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<typename T, typename ...Ts>
void DataFrame<I, H>::
sort(const char *name, sort_spec dir, bool ignore_index)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call sort()");

    make_consistent<Ts ...>();

    ColumnVecType<T>    *vec { nullptr};
    const SpinGuard     guard (lock_);

    if (! ::strcmp(name, DF_INDEX_COL_NAME))  {
        vec = reinterpret_cast<ColumnVecType<T> *>(&indices_);
        ignore_index = true;
    }
    else
        vec = &(get_column<T>(name, false));

    auto    a = [](const auto &lhs, const auto &rhs) -> bool {
                    return (std::get<0>(lhs) < std::get<0>(rhs));
                };
    auto    d = [](const auto &lhs, const auto &rhs) -> bool {
                    return (std::get<0>(lhs) > std::get<0>(rhs));
                };
    auto    aa = [](const auto &lhs, const auto &rhs) -> bool {
                    return (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)));
                 };
    auto    ad = [](const auto &lhs, const auto &rhs) -> bool {
                    return (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)));
                 };

    const size_type         idx_s = indices_.size();
    StlVecType<size_type>   sorting_idxs(idx_s);

    std::iota(sorting_idxs.begin(), sorting_idxs.end(), 0);

    auto        zip = std::ranges::views::zip(*vec, sorting_idxs);
    auto        zip_idx = std::ranges::views::zip(*vec, indices_, sorting_idxs);
    const auto  thread_level =
        (idx_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (dir == sort_spec::ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), a);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), a);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, a);
            else
                std::ranges::sort(zip, a);
        }
    }
    else if (dir == sort_spec::desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), d);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), d);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, d);
            else
                std::ranges::sort(zip, d);
        }
    }
    else if (dir == sort_spec::abs_ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), aa);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), aa);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, aa);
            else
                std::ranges::sort(zip, aa);
        }
    }
    else if (dir == sort_spec::abs_desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), ad);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), ad);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, ad);
            else
                std::ranges::sort(zip, ad);
        }
    }

    if (((column_list_.size() - 1) > 1) && get_thread_level() > 2)  {
        auto    lbd = [name,
                       &sorting_idxs = std::as_const(sorting_idxs),
                       idx_s, this]
                      (const auto &begin, const auto &end) -> void  {
            sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

            for (auto citer = begin; citer < end; ++citer)
                if (citer->first != name)
                    this->data_[citer->second].change(functor);
        };
        auto    futures =
            thr_pool_.parallel_loop(column_list_.begin(), column_list_.end(),
                                    std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

        for (const auto &citer : column_list_) [[likely]]
            if (citer.first != name)
                data_[citer.second].change(functor);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename ... Ts>
void DataFrame<I, H>::
sort(const char *name1, sort_spec dir1,
     const char *name2, sort_spec dir2,
     bool ignore_index)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call sort()");

    make_consistent<Ts ...>();

    ColumnVecType<T1>   *vec1 { nullptr};
    ColumnVecType<T2>   *vec2 { nullptr};
    const SpinGuard     guard (lock_);

    if (! ::strcmp(name1, DF_INDEX_COL_NAME))  {
        vec1 = reinterpret_cast<ColumnVecType<T1> *>(&indices_);
        ignore_index = true;
    }
    else
        vec1 = &(get_column<T1>(name1, false));

    if (! ::strcmp(name2, DF_INDEX_COL_NAME))  {
        vec2 = reinterpret_cast<ColumnVecType<T2> *>(&indices_);
        ignore_index = true;
    }
    else
        vec2 = &(get_column<T2>(name2, false));

    auto    a_a =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (std::get<0>(lhs) < std::get<0>(rhs))
                return (true);
            else if (std::get<0>(lhs) > std::get<0>(rhs))
                return (false);
            return (std::get<1>(lhs) < std::get<1>(rhs));
        };
    auto    d_d =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (std::get<0>(lhs) > std::get<0>(rhs))
                return (true);
            else if (std::get<0>(lhs) < std::get<0>(rhs))
                return (false);
            return (std::get<1>(lhs) > std::get<1>(rhs));
        };
    auto    a_d =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (std::get<0>(lhs) < std::get<0>(rhs))
                return (true);
            else if (std::get<0>(lhs) > std::get<0>(rhs))
                return (false);
            return (std::get<1>(lhs) > std::get<1>(rhs));
        };
    auto    d_a =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (std::get<0>(lhs) > std::get<0>(rhs))
                return (true);
            else if (std::get<0>(lhs) < std::get<0>(rhs))
                return (false);
            return (std::get<1>(lhs) < std::get<1>(rhs));
        };
    auto    aa_aa =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                return (true);
            else if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                return (false);
            return (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)));
        };
    auto    ad_ad =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                return (true);
            else if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                return (false);
            return (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)));
        };
    auto    aa_ad =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                return (true);
            else if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                return (false);
            return (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)));
        };
    auto    ad_aa =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                return (true);
            else if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                return (false);
            return (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)));
        };
    auto    a_aa =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (std::get<0>(lhs) < std::get<0>(rhs))
                return (true);
            else if (std::get<0>(lhs) > std::get<0>(rhs))
                return (false);
            return (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)));
        };
    auto    a_ad =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (std::get<0>(lhs) < std::get<0>(rhs))
                return (true);
            else if (std::get<0>(lhs) > std::get<0>(rhs))
                return (false);
            return (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)));
        };
    auto    d_aa =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (std::get<0>(lhs) > std::get<0>(rhs))
                return (true);
            else if (std::get<0>(lhs) < std::get<0>(rhs))
                return (false);
            return (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)));
        };
    auto    d_ad =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (std::get<0>(lhs) > std::get<0>(rhs))
                return (true);
            else if (std::get<0>(lhs) < std::get<0>(rhs))
                return (false);
            return (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)));
        };
    auto    aa_a =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                return (true);
            else if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                return (false);
            return (std::get<1>(lhs) < std::get<1>(rhs));
        };
    auto    ad_a =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                return (true);
            else if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                return (false);
            return (std::get<1>(lhs) < std::get<1>(rhs));
        };
    auto    aa_d =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                return (true);
            else if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                return (false);
            return (std::get<1>(lhs) > std::get<1>(rhs));
        };
    auto    ad_d =
        [](const auto &lhs, const auto &rhs) -> bool {
            if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                return (true);
            else if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                return (false);
            return (std::get<1>(lhs) > std::get<1>(rhs));
        };

    const size_type         idx_s = indices_.size();
    StlVecType<size_type>   sorting_idxs(idx_s);

    std::iota(sorting_idxs.begin(), sorting_idxs.end(), 0);

    auto        zip = std::ranges::views::zip(*vec1, *vec2, sorting_idxs);
    auto        zip_idx =
        std::ranges::views::zip(*vec1, *vec2, indices_, sorting_idxs);
    const auto  thread_level =
        (idx_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (dir1 == sort_spec::ascen && dir2 == sort_spec::ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), a_a);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), a_a);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, a_a);
            else
                std::ranges::sort(zip, a_a);
        }
    }
    else if (dir1 == sort_spec::desce && dir2 == sort_spec::desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), d_d);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), d_d);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, d_d);
            else
                std::ranges::sort(zip, d_d);
        }
    }
    else if (dir1 == sort_spec::ascen && dir2 == sort_spec::desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), a_d);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), a_d);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, a_d);
            else
                std::ranges::sort(zip, a_d);
        }
    }
    else if (dir1 == sort_spec::desce && dir2 == sort_spec::ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), d_a);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), d_a);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, d_a);
            else
                std::ranges::sort(zip, d_a);
        }
    }
    else if (dir1 == sort_spec::abs_ascen && dir2 == sort_spec::abs_ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), aa_aa);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), aa_aa);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, aa_aa);
            else
                std::ranges::sort(zip, aa_aa);
        }
    }
    else if (dir1 == sort_spec::abs_desce && dir2 == sort_spec::abs_desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), ad_ad);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), ad_ad);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, ad_ad);
            else
                std::ranges::sort(zip, ad_ad);
        }
    }
    else if (dir1 == sort_spec::abs_ascen && dir2 == sort_spec::abs_desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), aa_ad);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), aa_ad);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, aa_ad);
            else
                std::ranges::sort(zip, aa_ad);
        }
    }
    else if (dir1 == sort_spec::abs_desce && dir2 == sort_spec::abs_ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), ad_aa);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), ad_aa);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, ad_aa);
            else
                std::ranges::sort(zip, ad_aa);
        }
    }
    else if (dir1 == sort_spec::ascen && dir2 == sort_spec::abs_ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), a_aa);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), a_aa);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, a_aa);
            else
                std::ranges::sort(zip, a_aa);
        }
    }
    else if (dir1 == sort_spec::ascen && dir2 == sort_spec::abs_desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), a_ad);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), a_ad);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, a_ad);
            else
                std::ranges::sort(zip, a_ad);
        }
    }
    else if (dir1 == sort_spec::desce && dir2 == sort_spec::abs_ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), d_aa);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), d_aa);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, d_aa);
            else
                std::ranges::sort(zip, d_aa);
        }
    }
    else if (dir1 == sort_spec::desce && dir2 == sort_spec::abs_desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), d_ad);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), d_ad);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, d_ad);
            else
                std::ranges::sort(zip, d_ad);
        }
    }
    else if (dir1 == sort_spec::abs_ascen && dir2 == sort_spec::ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), aa_a);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), aa_a);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, aa_a);
            else
                std::ranges::sort(zip, aa_a);
        }
    }
    else if (dir1 == sort_spec::abs_desce && dir2 == sort_spec::ascen)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), ad_a);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), ad_a);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, ad_a);
            else
                std::ranges::sort(zip, ad_a);
        }
    }
    else if (dir1 == sort_spec::abs_ascen && dir2 == sort_spec::desce)  {
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), aa_d);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), aa_d);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, aa_d);
            else
                std::ranges::sort(zip, aa_d);
        }
    }
    else  {   // dir1 == sort_spec::abs_desce && dir2 == sort_spec::desce
        if (thread_level > 2)  {
            if (! ignore_index)
                thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), ad_d);
            else
                thr_pool_.parallel_sort(zip.begin(), zip.end(), ad_d);
        }
        else  {
            if (! ignore_index)
                std::ranges::sort(zip_idx, ad_d);
            else
                std::ranges::sort(zip, ad_d);
        }
    }

    if (((column_list_.size() - 2) > 1) && get_thread_level() > 2)  {
        auto    lbd = [name1, name2,
                       &sorting_idxs = std::as_const(sorting_idxs),
                       idx_s, this]
                      (const auto &begin, const auto &end) -> void  {
            sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

            for (auto citer = begin; citer < end; ++citer)
                if (citer->first != name1 && citer->first != name2)
                    this->data_[citer->second].change(functor);
        };
        auto    futures =
            thr_pool_.parallel_loop(column_list_.begin(), column_list_.end(),
                                    std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

        for (const auto &citer : column_list_) [[likely]]
            if (citer.first != name1 && citer.first != name2)
                data_[citer.second].change(functor);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename ... Ts>
void DataFrame<I, H>::
sort(const char *name1, sort_spec dir1,
     const char *name2, sort_spec dir2,
     const char *name3, sort_spec dir3,
     bool ignore_index)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call sort()");

    make_consistent<Ts ...>();

    ColumnVecType<T1>   *vec1 { nullptr};
    ColumnVecType<T2>   *vec2 { nullptr};
    ColumnVecType<T3>   *vec3 { nullptr};
    const SpinGuard     guard (lock_);

    if (! ::strcmp(name1, DF_INDEX_COL_NAME))  {
        vec1 = reinterpret_cast<ColumnVecType<T1> *>(&indices_);
        ignore_index = true;
    }
    else
        vec1 = &(get_column<T1>(name1, false));

    if (! ::strcmp(name2, DF_INDEX_COL_NAME))  {
        vec2 = reinterpret_cast<ColumnVecType<T2> *>(&indices_);
        ignore_index = true;
    }
    else
        vec2 = &(get_column<T2>(name2, false));

    if (! ::strcmp(name3, DF_INDEX_COL_NAME))  {
        vec3 = reinterpret_cast<ColumnVecType<T3> *>(&indices_);
        ignore_index = true;
    }
    else
        vec3 = &(get_column<T3>(name3, false));

    auto    cf =
        [dir1, dir2, dir3](const auto &lhs, const auto &rhs) -> bool {
            if (dir1 == sort_spec::ascen)  {
                if (std::get<0>(lhs) < std::get<0>(rhs))
                    return (true);
                else if (std::get<0>(lhs) > std::get<0>(rhs))
                    return (false);
            }
            else if (dir1 == sort_spec::desce)  {
                if (std::get<0>(lhs) > std::get<0>(rhs))
                    return (true);
                else if (std::get<0>(lhs) < std::get<0>(rhs))
                    return (false);
            }
            else if (dir1 == sort_spec::abs_ascen)  {
                if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                    return (true);
                else if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                    return (true);
                else if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                    return (false);
            }

            if (dir2 == sort_spec::ascen)  {
                if (std::get<1>(lhs) < std::get<1>(rhs))
                    return (true);
                else if (std::get<1>(lhs) > std::get<1>(rhs))
                    return (false);
            }
            else if (dir2 == sort_spec::desce)  {
                if (std::get<1>(lhs) > std::get<1>(rhs))
                    return (true);
                else if (std::get<1>(lhs) < std::get<1>(rhs))
                    return (false);
            }
            else if (dir2 == sort_spec::abs_ascen)  {
                if (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)))
                    return (true);
                else if (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)))
                    return (true);
                else if (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)))
                    return (false);
            }

            if (dir3 == sort_spec::ascen)
                return (std::get<2>(lhs) < std::get<2>(rhs));
            else if (dir3 == sort_spec::desce)
                return (std::get<2>(lhs) > std::get<2>(rhs));
            else if (dir3 == sort_spec::abs_ascen)
                return (abs__(std::get<2>(lhs)) < abs__(std::get<2>(rhs)));
            else  // sort_spec::abs_desce
                return (abs__(std::get<2>(lhs)) > abs__(std::get<2>(rhs)));
        };

    const size_type         idx_s = indices_.size();
    StlVecType<size_type>   sorting_idxs(idx_s);

    std::iota(sorting_idxs.begin(), sorting_idxs.end(), 0);

    auto        zip =
        std::ranges::views::zip(*vec1, *vec2, *vec3, sorting_idxs);
    auto        zip_idx =
        std::ranges::views::zip(*vec1, *vec2, *vec3, indices_, sorting_idxs);
    const auto  thread_level =
        (idx_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (thread_level > 2)  {
        if (! ignore_index)
            thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), cf);
        else
            thr_pool_.parallel_sort(zip.begin(), zip.end(), cf);
    }
    else  {
        if (! ignore_index)
            std::ranges::sort(zip_idx, cf);
        else
            std::ranges::sort(zip, cf);
    }

    if (((column_list_.size() - 3) > 1) && get_thread_level() > 2)  {
        auto    lbd = [name1, name2, name3,
                       &sorting_idxs = std::as_const(sorting_idxs),
                       idx_s, this]
                      (const auto &begin, const auto &end) -> void  {
            sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

            for (auto citer = begin; citer < end; ++citer)
                if (citer->first != name1 &&
                    citer->first != name2 &&
                    citer->first != name3)
                    this->data_[citer->second].change(functor);
        };
        auto    futures =
            thr_pool_.parallel_loop(column_list_.begin(), column_list_.end(),
                                    std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

        for (const auto &citer : column_list_) [[likely]]
            if (citer.first != name1 &&
                citer.first != name2 &&
                citer.first != name3)
                data_[citer.second].change(functor);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, typename ... Ts>
void DataFrame<I, H>::
sort(const char *name1, sort_spec dir1,
     const char *name2, sort_spec dir2,
     const char *name3, sort_spec dir3,
     const char *name4, sort_spec dir4,
     bool ignore_index)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call sort()");

    make_consistent<Ts ...>();

    const ColumnVecType<T1> *vec1 { nullptr};
    const ColumnVecType<T2> *vec2 { nullptr};
    const ColumnVecType<T3> *vec3 { nullptr};
    const ColumnVecType<T4> *vec4 { nullptr};
    const SpinGuard         guard (lock_);

    if (! ::strcmp(name1, DF_INDEX_COL_NAME))  {
        vec1 = reinterpret_cast<ColumnVecType<T1> *>(&indices_);
        ignore_index = true;
    }
    else
        vec1 = &(get_column<T1>(name1, false));

    if (! ::strcmp(name2, DF_INDEX_COL_NAME))  {
        vec2 = reinterpret_cast<ColumnVecType<T2> *>(&indices_);
        ignore_index = true;
    }
    else
        vec2 = &(get_column<T2>(name2, false));

    if (! ::strcmp(name3, DF_INDEX_COL_NAME))  {
        vec3 = reinterpret_cast<ColumnVecType<T3> *>(&indices_);
        ignore_index = true;
    }
    else
        vec3 = &(get_column<T3>(name3, false));

    if (! ::strcmp(name4, DF_INDEX_COL_NAME))  {
        vec4 = reinterpret_cast<ColumnVecType<T4> *>(&indices_);
        ignore_index = true;
    }
    else
        vec4 = &(get_column<T4>(name4, false));

    auto    cf =
        [dir1, dir2, dir3, dir4](const auto &lhs, const auto &rhs) -> bool {
            if (dir1 == sort_spec::ascen)  {
                if (std::get<0>(lhs) < std::get<0>(rhs))
                    return (true);
                else if (std::get<0>(lhs) > std::get<0>(rhs))
                    return (false);
            }
            else if (dir1 == sort_spec::desce)  {
                if (std::get<0>(lhs) > std::get<0>(rhs))
                    return (true);
                else if (std::get<0>(lhs) < std::get<0>(rhs))
                    return (false);
            }
            else if (dir1 == sort_spec::abs_ascen)  {
                if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                    return (true);
                else if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                    return (true);
                else if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                    return (false);
            }

            if (dir2 == sort_spec::ascen)  {
                if (std::get<1>(lhs) < std::get<1>(rhs))
                    return (true);
                else if (std::get<1>(lhs) > std::get<1>(rhs))
                    return (false);
            }
            else if (dir2 == sort_spec::desce)  {
                if (std::get<1>(lhs) > std::get<1>(rhs))
                    return (true);
                else if (std::get<1>(lhs) < std::get<1>(rhs))
                    return (false);
            }
            else if (dir2 == sort_spec::abs_ascen)  {
                if (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)))
                    return (true);
                else if (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)))
                    return (true);
                else if (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)))
                    return (false);
            }

            if (dir3 == sort_spec::ascen)  {
                if (std::get<2>(lhs) < std::get<2>(rhs))
                    return (true);
                else if (std::get<2>(lhs) > std::get<2>(rhs))
                    return (false);
            }
            else if (dir3 == sort_spec::desce)  {
                if (std::get<2>(lhs) > std::get<2>(rhs))
                    return (true);
                else if (std::get<2>(lhs) < std::get<2>(rhs))
                    return (false);
            }
            else if (dir3 == sort_spec::abs_ascen)  {
                if (abs__(std::get<2>(lhs)) < abs__(std::get<2>(rhs)))
                    return (true);
                else if (abs__(std::get<2>(lhs)) > abs__(std::get<2>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<2>(lhs)) > abs__(std::get<2>(rhs)))
                    return (true);
                else if (abs__(std::get<2>(lhs)) < abs__(std::get<2>(rhs)))
                    return (false);
            }

            if (dir4 == sort_spec::ascen)
                return (std::get<3>(lhs) < std::get<3>(rhs));
            else if (dir4 == sort_spec::desce)
                return (std::get<3>(lhs) > std::get<3>(rhs));
            else if (dir4 == sort_spec::abs_ascen)
                return (abs__(std::get<3>(lhs)) < abs__(std::get<3>(rhs)));
            else  // sort_spec::abs_desce
                return (abs__(std::get<3>(lhs)) > abs__(std::get<3>(rhs)));
        };

    const size_type         idx_s = indices_.size();
    StlVecType<size_type>   sorting_idxs(idx_s);

    std::iota(sorting_idxs.begin(), sorting_idxs.end(), 0);

    auto         zip =
        std::ranges::views::zip(*vec1, *vec2, *vec3, *vec4, sorting_idxs);
    auto         zip_idx =
        std::ranges::views::zip(*vec1, *vec2, *vec3, *vec4,
                                indices_, sorting_idxs);
    const auto  thread_level =
        (idx_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (thread_level > 2)  {
        if (! ignore_index)
            thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), cf);
        else
            thr_pool_.parallel_sort(zip.begin(), zip.end(), cf);
    }
    else  {
        if (! ignore_index)
            std::ranges::sort(zip_idx, cf);
        else
            std::ranges::sort(zip, cf);
    }

    if (((column_list_.size() - 4) > 1) && get_thread_level() > 2)  {
        auto    lbd = [name1, name2, name3, name4,
                       &sorting_idxs = std::as_const(sorting_idxs),
                       idx_s, this]
                      (const auto &begin, const auto &end) -> void  {
            sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

            for (auto citer = begin; citer < end; ++citer)
                if (citer->first != name1 &&
                    citer->first != name2 &&
                    citer->first != name3 &&
                    citer->first != name4)
                    this->data_[citer->second].change(functor);
        };
        auto    futures =
            thr_pool_.parallel_loop(column_list_.begin(), column_list_.end(),
                                    std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

        for (const auto &citer : column_list_) [[likely]]
            if (citer.first != name1 &&
                citer.first != name2 &&
                citer.first != name3 &&
                citer.first != name4)
                data_[citer.second].change(functor);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename ... Ts>
void DataFrame<I, H>::
sort(const char *name1, sort_spec dir1,
     const char *name2, sort_spec dir2,
     const char *name3, sort_spec dir3,
     const char *name4, sort_spec dir4,
     const char *name5, sort_spec dir5,
     bool ignore_index)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, H>::value,
                  "Only a StdDataFrame can call sort()");

    make_consistent<Ts ...>();

    const ColumnVecType<T1> *vec1 { nullptr};
    const ColumnVecType<T2> *vec2 { nullptr};
    const ColumnVecType<T3> *vec3 { nullptr};
    const ColumnVecType<T4> *vec4 { nullptr};
    const ColumnVecType<T5> *vec5 { nullptr};
    const SpinGuard         guard (lock_);

    if (! ::strcmp(name1, DF_INDEX_COL_NAME))  {
        vec1 = reinterpret_cast<ColumnVecType<T1> *>(&indices_);
        ignore_index = true;
    }
    else
        vec1 = &(get_column<T1>(name1, false));

    if (! ::strcmp(name2, DF_INDEX_COL_NAME))  {
        vec2 = reinterpret_cast<ColumnVecType<T2> *>(&indices_);
        ignore_index = true;
    }
    else
        vec2 = &(get_column<T2>(name2, false));

    if (! ::strcmp(name3, DF_INDEX_COL_NAME))  {
        vec3 = reinterpret_cast<ColumnVecType<T3> *>(&indices_);
        ignore_index = true;
    }
    else
        vec3 = &(get_column<T3>(name3, false));

    if (! ::strcmp(name4, DF_INDEX_COL_NAME))  {
        vec4 = reinterpret_cast<ColumnVecType<T4> *>(&indices_);
        ignore_index = true;
    }
    else
        vec4 = &(get_column<T4>(name4, false));

    if (! ::strcmp(name4, DF_INDEX_COL_NAME))  {
        vec5 = reinterpret_cast<ColumnVecType<T5> *>(&indices_);
        ignore_index = true;
    }
    else
        vec5 = &(get_column<T5>(name5, false));

    auto    cf =
        [dir1, dir2, dir3, dir4, dir5]
        (const auto &lhs, const auto &rhs) -> bool {
            if (dir1 == sort_spec::ascen)  {
                if (std::get<0>(lhs) < std::get<0>(rhs))
                    return (true);
                else if (std::get<0>(lhs) > std::get<0>(rhs))
                    return (false);
            }
            else if (dir1 == sort_spec::desce)  {
                if (std::get<0>(lhs) > std::get<0>(rhs))
                    return (true);
                else if (std::get<0>(lhs) < std::get<0>(rhs))
                    return (false);
            }
            else if (dir1 == sort_spec::abs_ascen)  {
                if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                    return (true);
                else if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<0>(lhs)) > abs__(std::get<0>(rhs)))
                    return (true);
                else if (abs__(std::get<0>(lhs)) < abs__(std::get<0>(rhs)))
                    return (false);
            }

            if (dir2 == sort_spec::ascen)  {
                if (std::get<1>(lhs) < std::get<1>(rhs))
                    return (true);
                else if (std::get<1>(lhs) > std::get<1>(rhs))
                    return (false);
            }
            else if (dir2 == sort_spec::desce)  {
                if (std::get<1>(lhs) > std::get<1>(rhs))
                    return (true);
                else if (std::get<1>(lhs) < std::get<1>(rhs))
                    return (false);
            }
            else if (dir2 == sort_spec::abs_ascen)  {
                if (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)))
                    return (true);
                else if (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<1>(lhs)) > abs__(std::get<1>(rhs)))
                    return (true);
                else if (abs__(std::get<1>(lhs)) < abs__(std::get<1>(rhs)))
                    return (false);
            }

            if (dir3 == sort_spec::ascen)  {
                if (std::get<2>(lhs) < std::get<2>(rhs))
                    return (true);
                else if (std::get<2>(lhs) > std::get<2>(rhs))
                    return (false);
            }
            else if (dir3 == sort_spec::desce)  {
                if (std::get<2>(lhs) > std::get<2>(rhs))
                    return (true);
                else if (std::get<2>(lhs) < std::get<2>(rhs))
                    return (false);
            }
            else if (dir3 == sort_spec::abs_ascen)  {
                if (abs__(std::get<2>(lhs)) < abs__(std::get<2>(rhs)))
                    return (true);
                else if (abs__(std::get<2>(lhs)) > abs__(std::get<2>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<2>(lhs)) > abs__(std::get<2>(rhs)))
                    return (true);
                else if (abs__(std::get<2>(lhs)) < abs__(std::get<2>(rhs)))
                    return (false);
            }

            if (dir4 == sort_spec::ascen)  {
                if (std::get<3>(lhs) < std::get<3>(rhs))
                    return (true);
                else if (std::get<3>(lhs) > std::get<3>(rhs))
                    return (false);
            }
            else if (dir4 == sort_spec::desce)  {
                if (std::get<3>(lhs) > std::get<3>(rhs))
                    return (true);
                else if (std::get<3>(lhs) < std::get<3>(rhs))
                    return (false);
            }
            else if (dir4 == sort_spec::abs_ascen)  {
                if (abs__(std::get<3>(lhs)) < abs__(std::get<3>(rhs)))
                    return (true);
                else if (abs__(std::get<3>(lhs)) > abs__(std::get<3>(rhs)))
                    return (false);
            }
            else  {   // sort_spec::abs_desce
                if (abs__(std::get<3>(lhs)) > abs__(std::get<3>(rhs)))
                    return (true);
                else if (abs__(std::get<3>(lhs)) < abs__(std::get<3>(rhs)))
                    return (false);
            }

            if (dir5 == sort_spec::ascen)
                return (std::get<4>(lhs) < std::get<4>(rhs));
            else if (dir5 == sort_spec::desce)
                return (std::get<4>(lhs) > std::get<4>(rhs));
            else if (dir5 == sort_spec::abs_ascen)
                return (abs__(std::get<4>(lhs)) < abs__(std::get<4>(rhs)));
            else  // sort_spec::abs_desce
                return (abs__(std::get<4>(lhs)) > abs__(std::get<4>(rhs)));
        };

    const size_type         idx_s = indices_.size();
    StlVecType<size_type>   sorting_idxs(idx_s);

    std::iota(sorting_idxs.begin(), sorting_idxs.end(), 0);

    auto        zip =
        std::ranges::views::zip(*vec1, *vec2, *vec3, *vec4, *vec5,
                                sorting_idxs);
    auto        zip_idx =
        std::ranges::views::zip(*vec1, *vec2, *vec3, *vec4, *vec5,
                                indices_, sorting_idxs);
    const auto  thread_level =
        (idx_s < ThreadPool::MUL_THR_THHOLD) ? 0L : get_thread_level();

    if (thread_level > 2)  {
        if (! ignore_index)
            thr_pool_.parallel_sort(zip_idx.begin(), zip_idx.end(), cf);
        else
            thr_pool_.parallel_sort(zip.begin(), zip.end(), cf);
    }
    else  {
        if (! ignore_index)
            std::ranges::sort(zip_idx, cf);
        else
            std::ranges::sort(zip, cf);
    }

    if (((column_list_.size() - 5) > 1) && get_thread_level() > 2)  {
        auto    lbd = [name1, name2, name3, name4, name5,
                       &sorting_idxs = std::as_const(sorting_idxs),
                       idx_s, this]
                      (const auto &begin, const auto &end) -> void  {
            sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

            for (auto citer = begin; citer < end; ++citer)
                if (citer->first != name1 &&
                    citer->first != name2 &&
                    citer->first != name3 &&
                    citer->first != name4 &&
                    citer->first != name5)
                    this->data_[citer->second].change(functor);
        };
        auto    futures =
            thr_pool_.parallel_loop(column_list_.begin(), column_list_.end(),
                                    std::move(lbd));

        for (auto &fut : futures)  fut.get();
    }
    else  {
        sort_functor_<Ts ...>   functor (sorting_idxs, idx_s);

        for (const auto &citer : column_list_) [[likely]]
            if (citer.first != name1 &&
                citer.first != name2 &&
                citer.first != name3 &&
                citer.first != name4 &&
                citer.first != name5)
                data_[citer.second].change(functor);
    }
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T, typename ...Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name, sort_spec dir,
           bool ignore_index)  {

    return (thr_pool_.dispatch(
                       true,
                       [name, dir, ignore_index, this] () -> void {
                           this->sort<T, Ts ...>(name, dir, ignore_index);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename ... Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name1, sort_spec dir1,
           const char *name2, sort_spec dir2,
           bool ignore_index)  {

    return (thr_pool_.dispatch(
                       true,
                       [name1, dir1, name2, dir2,
                        ignore_index, this] () -> void {
                           this->sort<T1, T2, Ts ...>(name1, dir1,
                                                      name2, dir2,
                                                      ignore_index);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename ... Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name1, sort_spec dir1,
           const char *name2, sort_spec dir2,
           const char *name3, sort_spec dir3,
           bool ignore_index)  {

    return (thr_pool_.dispatch(
                       true,
                       [name1, dir1, name2, dir2, name3, dir3,
                        ignore_index, this] () -> void {
                           this->sort<T1, T2, T3, Ts ...>(name1, dir1,
                                                          name2, dir2,
                                                          name3, dir3,
                                                          ignore_index);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, typename ... Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name1, sort_spec dir1,
           const char *name2, sort_spec dir2,
           const char *name3, sort_spec dir3,
           const char *name4, sort_spec dir4,
           bool ignore_index)  {

    return (thr_pool_.dispatch(
                       true,
                       [name1, dir1, name2, dir2, name3, dir3, name4, dir4,
                        ignore_index, this] () -> void {
                           this->sort<T1, T2, T3, T4, Ts ...>(name1, dir1,
                                                              name2, dir2,
                                                              name3, dir3,
                                                              name4, dir4,
                                                              ignore_index);
                       }));
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T1, typename T2, typename T3, typename T4, typename T5,
         typename ... Ts>
std::future<void>
DataFrame<I, H>::
sort_async(const char *name1, sort_spec dir1,
           const char *name2, sort_spec dir2,
           const char *name3, sort_spec dir3,
           const char *name4, sort_spec dir4,
           const char *name5, sort_spec dir5,
           bool ignore_index)  {

    return (thr_pool_.dispatch(
                       true,
                       [name1, dir1, name2, dir2, name3, dir3, name4, dir4,
                        name5, dir5, ignore_index, this] () -> void {
                           this->sort<T1, T2, T3, T4, T5, Ts ...>(name1, dir1,
                                                                  name2, dir2,
                                                                  name3, dir3,
                                                                  name4, dir4,
                                                                  name5, dir5,
                                                                  ignore_index);
                       }));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
