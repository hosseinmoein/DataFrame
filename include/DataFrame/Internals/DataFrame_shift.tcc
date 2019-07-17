// Hossein Moein
// December 27, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>

#include <algorithm>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename I, typename H>
template<typename ... types>
void DataFrame<I, H>::self_shift(size_type periods, shift_policy sp)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call self_shift()");

    if (periods > 0)  {
        vertical_shift_functor_<types ...> functor(periods, sp);
        std::vector<std::future<void>>     futures(get_thread_level());
        size_type                          thread_count = 0;
        const size_type                    data_size = data_.size();

        for (size_type idx = 0; idx < data_size; ++idx)  {
            if (thread_count >= get_thread_level())
                data_[idx].change(functor);
            else  {
                auto    to_be_called =
                    static_cast
                    <void(DataVec::*)(vertical_shift_functor_<types ...> &&)>
                        (&DataVec::template
                             change<vertical_shift_functor_<types ...>>);

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
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... types>
StdDataFrame<I> DataFrame<I, H>::
shift(size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call shift()");

    StdDataFrame<IndexType> slug = *this;

    slug.template self_shift<types ...>(periods, sp);
    return (slug);
}

// ----------------------------------------------------------------------------

template<typename I, typename DataVec>
template<typename ... types>
void DataFrame<I, DataVec>::self_rotate(size_type periods, shift_policy sp)  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call self_rotate()");

    if (periods > 0)  {
        rotate_functor_<types ...>      functor(periods, sp);
        std::vector<std::future<void>>  futures(get_thread_level());
        size_type                       thread_count = 0;
        const size_type                 data_size = data_.size();

        for (size_type idx = 0; idx < data_size; ++idx)  {
            if (thread_count >= get_thread_level())
                data_[idx].change(functor);
            else  {
                auto    to_be_called =
                    static_cast
                        <void(DataVec::*)(rotate_functor_<types ...> &&)>
                            (&DataVec::template
                                 change<rotate_functor_<types ...>>);

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
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename ... types>
StdDataFrame<I> DataFrame<I, H>::
rotate(size_type periods, shift_policy sp) const  {

    static_assert(std::is_base_of<HeteroVector, DataVec>::value,
                  "Only a StdDataFrame can call rotate()");

    StdDataFrame<IndexType> slug = *this;

    slug.template self_rotate<types ...>(periods, sp);
    return (slug);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename V>
void DataFrame<I, H>::shift_right_(V &vec, size_type n)  {

    using value_type =
        typename std::remove_reference<decltype(vec)>::type::value_type;

    const auto  vec_rend = vec.rend();

    for (auto riter = vec.rbegin(); riter != vec_rend; ++riter)
        if (riter + n < vec_rend)
            *riter = std::move(*(riter + n));
        else
            *riter = std::move(_get_nan<value_type>());

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename V>
void DataFrame<I, H>::shift_left_(V &vec, size_type n)  {

    using value_type =
        typename std::remove_reference<decltype(vec)>::type::value_type;

    const auto  vec_end = vec.end();

    for (auto iter = vec.begin(); iter != vec_end; ++iter)
        if (iter + n < vec_end)
            *iter = std::move(*(iter + n));
        else
            *iter = std::move(_get_nan<value_type>());

    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename V>
void DataFrame<I, H>::rotate_right_(V &vec, size_type n)  {

    // There is no checking the value of n
    std::rotate(vec.rbegin(), vec.rbegin() + n, vec.rend());
    return;
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename V>
void DataFrame<I, H>::rotate_left_(V &vec, size_type n)  {

    // There is no checking the value of n
    std::rotate(vec.begin(), vec.begin() + n, vec.end());
    return;
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
