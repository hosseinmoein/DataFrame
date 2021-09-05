// Hossein Moein
// December 27, 2018
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

#include <DataFrame/DataFrame.h>

#include <algorithm>
#include <iterator>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::self_shift(size_type periods, shift_policy sp)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call self_shift()");

    if (periods > 0)  {
        if (sp == shift_policy::down || sp == shift_policy::up)  {
            vertical_shift_functor_<Ts ...> functor(periods, sp);
            std::vector<std::future<void>>  futures(get_thread_level());
            size_type                       thread_count = 0;
            const size_type                 data_size = data_.size();

            for (size_type idx = 0; idx < data_size; ++idx)  {
                const SpinGuard guard(lock_);

                if (thread_count >= get_thread_level())
                    data_[idx].change(functor);
                else  {
                    auto    to_be_called =
                        static_cast
                        <void(DataVec::*)(vertical_shift_functor_<Ts ...> &&)>
                            (&DataVec::template
                                 change<vertical_shift_functor_<Ts ...>>);

                    futures[thread_count] =
                        std::async(std::launch::async,
                                   to_be_called,
                                   &(data_[idx]),
                                   std::move(functor));
                    thread_count += 1;
                }
            }
            for (size_type idx = 0; idx < thread_count; ++idx)
                futures[idx].get();
        }
        else if (sp == shift_policy::left)  {
            while (periods-- > 0)
                remove_column(column_list_.front().first.c_str());
        }
        else if (sp == shift_policy::right)  {
            while (periods-- > 0)
                remove_column(column_list_.back().first.c_str());
        }
    }
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
StdDataFrame<I> DataFrame<I, H>::
shift(size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call shift()");

    StdDataFrame<IndexType> slug = *this;

    slug.template self_shift<Ts ...>(periods, sp);
    return (slug);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename T>
std::vector<T> DataFrame<I, H>::
shift(const char *col_name, size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call shift()");

    std::vector<T>              result = get_column<T>(col_name);
    vertical_shift_functor_<T>  functor(periods, sp);

    functor (result);
    return (result);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... Ts>
void DataFrame<I, H>::self_rotate(size_type periods, shift_policy sp)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call self_rotate()");

    if (periods > 0)  {
        if (sp == shift_policy::down || sp == shift_policy::up)  {
            rotate_functor_<Ts ...>         functor(periods, sp);
            std::vector<std::future<void>>  futures(get_thread_level());
            size_type                       thread_count = 0;
            const size_type                 data_size = data_.size();

            for (size_type idx = 0; idx < data_size; ++idx)  {
                const SpinGuard guard(lock_);

                if (thread_count >= get_thread_level())
                    data_[idx].change(functor);
                else  {
                    auto    to_be_called =
                        static_cast
                            <void(H::*)(rotate_functor_<Ts ...> &&)>
                                (&H::template change<rotate_functor_<Ts ...>>);

                    futures[thread_count] =
                        std::async(std::launch::async,
                                   to_be_called,
                                   &(data_[idx]),
                                   std::move(functor));
                    thread_count += 1;
                }
            }
            for (size_type idx = 0; idx < thread_count; ++idx)
                futures[idx].get();
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
StdDataFrame<I> DataFrame<I, H>::
rotate(size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call rotate()");

    StdDataFrame<IndexType> slug = *this;

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
