// Hossein Moein
// November 3, 2022
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

#include <DataFrame/Vectors/HeteroVector.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<std::size_t A>
template<typename T>
HeteroConstView<A>::HeteroConstView(const T *begin_ptr, const T *end_ptr)
    : clear_function_([](HeteroConstView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroConstView &from, HeteroConstView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroConstView &from, HeteroConstView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, VectorConstView<T, A>(begin_ptr, end_ptr));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
void HeteroConstView<A>::set_begin_end_special(const T *bp, const T *ep_1)  {

    clear_function_ = [](HeteroConstView &hv) { views_<T>.erase(&hv); };
    copy_function_  = [](const HeteroConstView &from, HeteroConstView &to)  {
                          views_<T>[&to] = views_<T>[&from];
                      };
    move_function_ = [](HeteroConstView &from, HeteroConstView &to)  {
                         views_<T>[&to] = std::move(views_<T>[&from]);
                     };

    VectorConstView<T, A>   vcv;

    vcv.set_begin_end_special(bp, ep_1);
    views_<T>.emplace(this, vcv);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
VectorConstView<T, A> &HeteroConstView<A>::get_vector()  {

    auto    iter = views_<T>.find (this);

    if (iter == views_<T>.end()) [[unlikely]]
        throw std::runtime_error("HeteroConstView::get_vector(): ERROR: "
                                 "Cannot find view");

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const VectorConstView<T, A> &HeteroConstView<A>::get_vector() const  {

    const auto  citer = views_<T>.find (this);

    if (citer == views_<T>.end()) [[unlikely]]
        throw std::runtime_error("HeteroConstView::get_vector(): ERROR: "
                                 "Cannot find view");

    return (citer->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroConstView<A>::visit_impl_help_ (T &visitor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end()) [[likely]]
        for (auto &&element : citer->second)
            visitor(element);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroConstView<A>::change_impl_help_ (T &functor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end()) [[likely]]
        functor(citer->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroConstView<A>::visit_impl_ (T &&visitor, TLIST<TYPES...>) const  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroConstView<A>::change_impl_ (T &&functor, TLIST<TYPES...>) const  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
bool HeteroConstView<A>::empty() const noexcept  {

    return (get_vector<T>().empty ());
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroConstView<A>::at(size_type idx) const  {

    return (get_vector<T>()[idx]);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroConstView<A>::back() const  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroConstView<A>::
front() const  { return (get_vector<T>().front ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroConstView<A>::template const_iterator<T>
HeteroConstView<A>::begin() const  { return (get_vector<T>().begin ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroConstView<A>::template const_iterator<T>
HeteroConstView<A>::end() const  { return (get_vector<T>().end ()); }


// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroConstView<A>::template const_reverse_iterator<T>
HeteroConstView<A>::rbegin() const  { return (get_vector<T>().rbegin ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroConstView<A>::template const_reverse_iterator<T>
HeteroConstView<A>::rend() const  { return (get_vector<T>().rend ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroConstView<A>::HeteroConstView() = default;

template<std::size_t A>
HeteroConstView<A>::
HeteroConstView(const HeteroConstView &that) { *this = that; }
template<std::size_t A>
HeteroConstView<A>::HeteroConstView(HeteroConstView &&that)  { *this = that; }

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroConstView<A> &HeteroConstView<A>::
operator= (const HeteroConstView &rhs)  {

    if (&rhs != this) [[likely]]  {
        clear();
        clear_function_ = rhs.clear_function_;
        copy_function_ = rhs.copy_function_;
        move_function_ = rhs.move_function_;

        copy_function_(rhs, *this);
    }

    return (*this);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroConstView<A> &HeteroConstView<A>::operator= (HeteroConstView &&rhs)  {

    if (&rhs != this) [[likely]]  {
        clear();
        clear_function_ = std::move(rhs.clear_function_);
        copy_function_ = std::move(rhs.copy_function_);
        move_function_ = std::move(rhs.move_function_);

        move_function_(rhs, *this);
    }

    return (*this);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
void HeteroConstView<A>::clear()  {

    clear_function_(*this);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
