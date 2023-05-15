// Hossein Moein
// June 24, 2019
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

#include <algorithm>
#include <stdexcept>
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<std::size_t A>
template<typename T>
HeteroPtrView<A>::HeteroPtrView(T *begin_ptr, T *end_ptr)
    : clear_function_([](HeteroPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, VectorPtrView<T, A>(begin_ptr, end_ptr));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
void HeteroPtrView<A>::set_begin_end_special(T *bp, T *ep_1)  {

    clear_function_ = [](HeteroPtrView &hv) { views_<T>.erase(&hv); };
    copy_function_  = [](const HeteroPtrView &from, HeteroPtrView &to)  {
                          views_<T>[&to] = views_<T>[&from];
                      };
    move_function_ = [](HeteroPtrView &from, HeteroPtrView &to)  {
                         views_<T>[&to] = std::move(views_<T>[&from]);
                     };

    VectorPtrView<T, A> vv;

    vv.set_begin_end_special(bp, ep_1);
    views_<T>.emplace(this, vv);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
HeteroPtrView<A>::HeteroPtrView(VectorPtrView<T, A> &vec)
    : clear_function_([](HeteroPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, vec);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
HeteroPtrView<A>::HeteroPtrView(VectorPtrView<T, A> &&vec)
    : clear_function_([](HeteroPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, std::move(vec));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
VectorPtrView<T, A> &HeteroPtrView<A>::get_vector()  {

    auto    iter = views_<T>.find (this);

    if (iter == views_<T>.end()) [[unlikely]]
        throw std::runtime_error("HeteroPtrView::get_vector(): ERROR: "
                                 "Cannot find view");

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const VectorPtrView<T, A> &HeteroPtrView<A>::get_vector() const  {

    return (const_cast<HeteroPtrView *>(this)->get_vector<T>());
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroPtrView<A>::visit_impl_help_ (T &visitor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end()) [[likely]]  {
#ifndef _MSC_VER
        for (auto &&element : iter->second)
            visitor(element);
#else
        const size_type vec_size = iter->second.size();

        for (size_type i = 0; i < vec_size; ++i)
            visitor(iter->second[i]);
#endif // !_MSC_VER
    }
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroPtrView<A>::visit_impl_help_ (T &visitor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end()) [[likely]]  {
#ifndef _MSC_VER
        for (auto &&element : citer->second)
            visitor(element);
#else
        const size_type vec_size = citer->second.size();

        for (size_type i = 0; i < vec_size; ++i)
            visitor(citer->second[i]);
#endif // !_MSC_VER
    }
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroPtrView<A>::sort_impl_help_ (T &functor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end()) [[likely]]
        std::sort (iter->second.begin(), iter->second.end(), functor);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroPtrView<A>::change_impl_help_ (T &functor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end()) [[likely]]
        functor(iter->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroPtrView<A>::change_impl_help_ (T &functor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end()) [[likely]]
        functor(citer->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView<A>::visit_impl_ (T &&visitor, TLIST<TYPES...>)  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView<A>::visit_impl_ (T &&visitor, TLIST<TYPES...>) const  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView<A>::sort_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (sort_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView<A>::change_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView<A>::change_impl_ (T &&functor, TLIST<TYPES...>) const  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
bool HeteroPtrView<A>::empty() const noexcept  {

    return (get_vector<T>().empty ());
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroPtrView<A>::at(size_type idx)  {

    return (get_vector<T>()[idx]);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroPtrView<A>::at(size_type idx) const  {

    return (get_vector<T>()[idx]);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroPtrView<A>::back()  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroPtrView<A>::back() const  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroPtrView<A>::front()  { return (get_vector<T>().front ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroPtrView<A>::front() const  { return (get_vector<T>().front ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroPtrView<A>::HeteroPtrView() = default;

template<std::size_t A>
HeteroPtrView<A>::HeteroPtrView (const HeteroPtrView &that)  { *this = that; }
template<std::size_t A>
HeteroPtrView<A>::HeteroPtrView (HeteroPtrView &&that)  { *this = that; }

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroPtrView<A> &HeteroPtrView<A>::operator= (const HeteroPtrView &rhs)  {

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
HeteroPtrView<A> &HeteroPtrView<A>::operator= (HeteroPtrView &&rhs)  {

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
void HeteroPtrView<A>::clear()  {

    clear_function_(*this);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
