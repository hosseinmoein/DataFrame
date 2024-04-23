// Hossein Moein
// December 27, 2018
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
template<typename ... Ts>
void DataFrame<I, H>::self_shift(size_type periods, shift_policy sp)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call self_shift()");

    if (periods > 0) [[likely]] {
        if (sp == shift_policy::down || sp == shift_policy::up) [[likely]]  {
            vertical_shift_functor_<Ts ...> functor(periods, sp);
            const size_type                 num_cols = data_.size();
            const auto                      thread_level =
                (indices_.size() < ThreadPool::MUL_THR_THHOLD)
                    ? 0L : get_thread_level();
            const SpinGuard                 guard(lock_);

            if (thread_level > 2)  {
                auto    lbd =
                    [&functor, this](auto begin, auto end) -> void  {
                        for (size_type idx = begin; idx < end; ++idx)
                            this->data_[idx].change(functor);
                    };
                auto    futuers =
                    thr_pool_.parallel_loop(size_type(0), num_cols,
                                            std::move(lbd));

                for (auto &fut : futuers)  fut.get();
            }
            else  {
                for (size_type idx = 0; idx < num_cols; ++idx) [[likely]]
                    data_[idx].change(functor);
            }
        }
        else  {
            while (periods-- > 0)  {
                const char                      *col_name =
                    (sp == shift_policy::left)
                        ? column_list_.front().first.c_str()
                        : column_list_.back().first.c_str();
                remove_column_functor_<Ts ...>  functor (col_name, *this);

                for (const auto &[name, idx] : column_list_) [[likely]]  {
                    if (name == col_name)  {
                        data_[idx].change(functor);
                        break;
                    }
                }
            }
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
shift(size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call shift()");

    DataFrame   slug = *this;

    slug.template self_shift<Ts ...>(periods, sp);
    return (slug);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
typename DataFrame<I, H>::template StlVecType<T> DataFrame<I, H>::
shift(const char *col_name, size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call shift()");

    ColumnVecType<T>            result = get_column<T>(col_name);
    vertical_shift_functor_<T>  functor(periods, sp);

    functor (result);
    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::self_rotate(size_type periods, shift_policy sp)  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call self_rotate()");

    if (periods > 0)  {
        if (sp == shift_policy::down || sp == shift_policy::up) [[likely]]  {
            rotate_functor_<Ts ...> functor(periods, sp);
            const size_type         num_cols = data_.size();

            {
                const SpinGuard guard(lock_);

                for (size_type idx = 0; idx < num_cols; ++idx) [[likely]]
                    data_[idx].change(functor);
            }
        }
        else if (sp == shift_policy::left)  {
            std::rotate(column_list_.begin(),
                        column_list_.begin() + periods,
                        column_list_.end());
        }
        else if (sp == shift_policy::right)  {
            std::rotate(column_list_.rbegin(),
                        column_list_.rbegin() + periods,
                        column_list_.rend());
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
DataFrame<I, H> DataFrame<I, H>::
rotate(size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector<align_value>, DataVec>::value,
                  "Only a StdDataFrame can call rotate()");

    DataFrame<IndexType, H> slug = *this;

    slug.template self_rotate<Ts ...>(periods, sp);
    return (slug);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
