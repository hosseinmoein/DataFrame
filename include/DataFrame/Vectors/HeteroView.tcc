// Hossein Moein
// October 24, 2018
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
HeteroView<A>::HeteroView(T *begin_ptr, T *end_ptr)
    : clear_function_([](HeteroView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroView &from, HeteroView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroView &from, HeteroView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, VectorView<T, A>(begin_ptr, end_ptr));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
void HeteroView<A>::set_begin_end_special(T *bp, T *ep_1)  {

    clear_function_ = [](HeteroView &hv) { views_<T>.erase(&hv); };
    copy_function_  = [](const HeteroView &from, HeteroView &to)  {
                          views_<T>[&to] = views_<T>[&from];
                      };
    move_function_ = [](HeteroView &from, HeteroView &to)  {
                         views_<T>[&to] = std::move(views_<T>[&from]);
                     };

    VectorView<T, A>    vv;

    vv.set_begin_end_special(bp, ep_1);
    views_<T>.emplace(this, vv);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
VectorView<T, A> &HeteroView<A>::get_vector()  {

    auto    iter = views_<T>.find (this);

    if (iter == views_<T>.end()) [[unlikely]]
        throw std::runtime_error("HeteroView::get_vector(): ERROR: "
                                 "Cannot find view");

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const VectorView<T, A> &HeteroView<A>::get_vector() const  {

    return (const_cast<HeteroView *>(this)->get_vector<T>());
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroView<A>::visit_impl_help_ (T &visitor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end()) [[likely]]
        for (auto &&element : iter->second)
            visitor(element);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroView<A>::visit_impl_help_ (T &visitor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end()) [[likely]]
        for (auto &&element : citer->second)
            visitor(element);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroView<A>::sort_impl_help_ (T &functor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end()) [[likely]]
        std::sort (iter->second.begin(), iter->second.end(), functor);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroView<A>::change_impl_help_ (T &functor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end()) [[likely]]
        functor(iter->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroView<A>::change_impl_help_ (T &functor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end()) [[likely]]
        functor(citer->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroView<A>::visit_impl_ (T &&visitor, TLIST<TYPES...>)  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroView<A>::visit_impl_ (T &&visitor, TLIST<TYPES...>) const  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroView<A>::sort_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (sort_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroView<A>::change_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroView<A>::change_impl_ (T &&functor, TLIST<TYPES...>) const  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
bool HeteroView<A>::empty() const noexcept  {

    return (get_vector<T>().empty ());
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroView<A>::at(size_type idx)  {

    return (get_vector<T>()[idx]);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroView<A>::at(size_type idx) const  {

    return (get_vector<T>()[idx]);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroView<A>::back()  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroView<A>::back() const  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroView<A>::front()  { return (get_vector<T>().front ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroView<A>::front() const  { return (get_vector<T>().front ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroView<A>::template iterator<T>
HeteroView<A>::begin()  { return (get_vector<T>().begin ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroView<A>::template const_iterator<T>
HeteroView<A>::begin() const  { return (get_vector<T>().begin ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroView<A>::template iterator<T>
HeteroView<A>::end()  { return (get_vector<T>().end ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroView<A>::template const_iterator<T>
HeteroView<A>::end() const  { return (get_vector<T>().end ()); }


// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroView<A>::template reverse_iterator<T>
HeteroView<A>::rbegin()  { return (get_vector<T>().rbegin ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroView<A>::template const_reverse_iterator<T>
HeteroView<A>::rbegin() const  { return (get_vector<T>().rbegin ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroView<A>::template reverse_iterator<T>
HeteroView<A>::rend()  { return (get_vector<T>().rend ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
typename HeteroView<A>::template const_reverse_iterator<T>
HeteroView<A>::rend() const  { return (get_vector<T>().rend ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroView<A>::HeteroView() = default;

template<std::size_t A>
HeteroView<A>::HeteroView (const HeteroView &that)  { *this = that; }
template<std::size_t A>
HeteroView<A>::HeteroView (HeteroView &&that)  { *this = that; }

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroView<A> &HeteroView<A>::operator= (const HeteroView &rhs)  {

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
HeteroView<A> &HeteroView<A>::operator= (HeteroView &&rhs)  {

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
void HeteroView<A>::clear()  {

    clear_function_(*this);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
