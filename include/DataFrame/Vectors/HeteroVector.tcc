// Hossein Moein
// September 12, 2017
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
#include <utility>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<std::size_t A>
template<typename T>
std::vector<T, typename allocator_declare<T, A>::type> &HeteroVector<A>::
get_vector()  {

    auto    iter = vectors_<T>.find (this);

    // don't have it yet, so create functions for copying and destroying
    if (iter == vectors_<T>.end()) [[likely]]  {
        clear_functions_.emplace_back (
            [](HeteroVector &hv) { vectors_<T>.erase(&hv); });

        // if someone copies me, they need to call each
        // copy_function and pass themself
        copy_functions_.emplace_back (
            [](const HeteroVector &from, HeteroVector &to)  {
                vectors_<T>[&to] = vectors_<T>[&from];
            });

        move_functions_.emplace_back (
            [](HeteroVector &from, HeteroVector &to)  {
                vectors_<T>[&to] = std::move(vectors_<T>[&from]);
            });

        iter = vectors_<T>.emplace (
                   this,
                   std::vector<
                       T,
                       typename allocator_declare<T, A>::type>()).first;
    }

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
HeteroView<A> HeteroVector<A>::get_view(size_type begin, size_type end)  {

    std::vector<T, typename allocator_declare<T, A>::type>  &vec =
        get_vector<T>();

    return (HeteroView<A>(
        &(vec[begin]), end == size_t(-1) ? &(vec.back()) : &(vec[end])));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
HeteroConstView<A> HeteroVector<A>::
get_view(size_type begin, size_type end) const  {

    const std::vector<T, typename allocator_declare<T, A>::type> &vec =
        get_vector<T>();

    return (HeteroConstView<A>(
        &(vec[begin]), end == size_t(-1) ? &(vec.back()) : &(vec[end])));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
HeteroPtrView<A> HeteroVector<A>::
get_ptr_view(size_type begin, size_type end)  {

    std::vector<T, typename allocator_declare<T, A>::type>  &vec =
        get_vector<T>();

    return (HeteroPtrView<A>(
        &(*(vec.begin() + begin)),
        end == size_type(-1) ? &(vec.back()) + 1 : &(*(vec.begin() + end))));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
HeteroConstPtrView<A> HeteroVector<A>::
get_ptr_view(size_type begin, size_type end) const  {

    const std::vector<T, typename allocator_declare<T, A>::type> &vec =
        get_vector<T>();

    return (HeteroConstPtrView<A>(
        &(*(vec.begin() + begin)),
        end == size_type(-1) ? &(vec.back()) + 1 : &(*(vec.begin() + end))));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const std::vector<T, typename allocator_declare<T, A>::type> &
HeteroVector<A>::get_vector() const  {

    return (const_cast<HeteroVector *>(this)->get_vector<T>());
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
void HeteroVector<A>::push_back(const T &v)  { get_vector<T>().push_back (v); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, class... Args>
void HeteroVector<A>::emplace_back (Args &&... args)  {

    get_vector<T>().emplace_back (std::forward<Args>(args)...);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename ITR, class... Args>
void HeteroVector<A>::emplace (ITR pos, Args &&... args)  {

    get_vector<T>().emplace (pos, std::forward<Args>(args)...);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroVector<A>::visit_impl_help_ (T &visitor)  {

    auto    iter = vectors_<U>.find (this);

    if (iter != vectors_<U>.end()) [[likely]]
        for (auto &&element : iter->second)
            visitor(element);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroVector<A>::visit_impl_help_ (T &visitor) const  {

    const auto  citer = vectors_<U>.find (this);

    if (citer != vectors_<U>.end()) [[likely]]
        for (auto &&element : citer->second)
            visitor(element);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroVector<A>::sort_impl_help_ (T &functor)  {

    auto    iter = vectors_<U>.find (this);

    if (iter != vectors_<U>.end()) [[likely]]
        std::sort (iter->second.begin(), iter->second.end(), functor);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroVector<A>::change_impl_help_ (T &functor)  {

    auto    iter = vectors_<U>.find (this);

    if (iter != vectors_<U>.end())
        functor(iter->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T, typename U>
void HeteroVector<A>::change_impl_help_ (T &functor) const  {

    const auto  citer = vectors_<U>.find (this);

    if (citer != vectors_<U>.end()) [[likely]]
        functor(citer->second);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector<A>::visit_impl_ (T &&visitor, TLIST<TYPES...>)  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector<A>::visit_impl_ (T &&visitor, TLIST<TYPES...>) const  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector<A>::sort_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (sort_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector<A>::change_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector<A>::change_impl_ (T &&functor, TLIST<TYPES...>) const  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
void HeteroVector<A>::erase(size_type pos)  {

    auto    &vec = get_vector<T>();

    vec.erase (vec.begin() + pos);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
void HeteroVector<A>::resize(size_type count)  {

    get_vector<T>().resize (count);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
void HeteroVector<A>::resize(size_type count, const T &v)  {

    get_vector<T>().resize (count, v);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
void HeteroVector<A>::pop_back()  { get_vector<T>().pop_back (); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
bool HeteroVector<A>::empty() const noexcept  {

    return (get_vector<T>().empty ());
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroVector<A>::at(size_type idx)  {

    return (get_vector<T>().at (idx));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroVector<A>::at(size_type idx) const  {

    return (get_vector<T>().at (idx));
}

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroVector<A>::back()  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroVector<A>::back() const  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
T &HeteroVector<A>::front()  { return (get_vector<T>().front ()); }

// ----------------------------------------------------------------------------

template<std::size_t A>
template<typename T>
const T &HeteroVector<A>::front() const  { return (get_vector<T>().front ()); }


// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroVector<A>::HeteroVector ()  {

    clear_functions_.reserve(2);
    copy_functions_.reserve(2);
    move_functions_.reserve(2);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroVector<A>::HeteroVector (const HeteroVector &that)  { *this = that; }
template<std::size_t A>
HeteroVector<A>::HeteroVector (HeteroVector &&that)  { *this = that; }

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroVector<A> &HeteroVector<A>::operator= (const HeteroVector &rhs)  {

    if (&rhs != this) [[likely]]  {
        clear();
        clear_functions_ = rhs.clear_functions_;
        copy_functions_ = rhs.copy_functions_;
        move_functions_ = rhs.move_functions_;

        for (auto &&copy_function : copy_functions_)
            copy_function(rhs, *this);
    }

    return (*this);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
HeteroVector<A> &HeteroVector<A>::operator= (HeteroVector &&rhs)  {

    if (&rhs != this) [[likely]]  {
        clear();
        clear_functions_ = std::move(rhs.clear_functions_);
        copy_functions_ = std::move(rhs.copy_functions_);
        move_functions_ = std::move(rhs.move_functions_);

        for (auto &&move_function : move_functions_)
            move_function(rhs, *this);
    }

    return (*this);
}

// ----------------------------------------------------------------------------

template<std::size_t A>
void HeteroVector<A>::clear()  {

    for (auto &&clear_func : clear_functions_)
        clear_func (*this);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
