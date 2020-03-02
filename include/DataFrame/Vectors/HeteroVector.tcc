// Hossein Moein
// September 12, 2017
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

#include <DataFrame/Vectors/HeteroVector.h>

#include <algorithm>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
std::vector<T> &HeteroVector::get_vector()  {

    auto    iter = vectors_<T>.find (this);

    // don't have it yet, so create functions for copying and destroying
    if (iter == vectors_<T>.end())  {
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

        iter = vectors_<T>.emplace (this, std::vector<T>()).first;
    }

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<typename T>
HeteroView HeteroVector::get_view(size_type begin, size_type end)  {

    std::vector<T>  &vec = get_vector<T>();

    return (HeteroView(
        &(vec[begin]), end == size_t(-1) ? &(vec.back()) : &(vec[end])));
}

// ----------------------------------------------------------------------------

template<typename T>
HeteroPtrView HeteroVector::get_ptr_view(size_type begin, size_type end)  {

    std::vector<T>  &vec = get_vector<T>();

    return (HeteroPtrView(
        &(*(vec.begin() + begin)),
        end == size_type(-1) ? &(*(vec.end())) : &(*(vec.begin() + end))));
}

// ----------------------------------------------------------------------------

template<typename T>
const std::vector<T> &HeteroVector::get_vector() const  {

    return (const_cast<HeteroVector *>(this)->get_vector<T>());
}

// ----------------------------------------------------------------------------

template<typename T>
void HeteroVector::push_back(const T &v)  { get_vector<T>().push_back (v); }

// ----------------------------------------------------------------------------

template<typename T, class... Args>
void HeteroVector::emplace_back (Args &&... args)  {

    get_vector<T>().emplace_back (std::forward<Args>(args)...);
}

// ----------------------------------------------------------------------------

template<typename T, typename ITR, class... Args>
void HeteroVector::emplace (ITR pos, Args &&... args)  {

    get_vector<T>().emplace (pos, std::forward<Args>(args)...);
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroVector::visit_impl_help_ (T &visitor)  {

    auto    iter = vectors_<U>.find (this);

    if (iter != vectors_<U>.end())
        for (auto &&element : iter->second)
            visitor(element);
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroVector::visit_impl_help_ (T &visitor) const  {

    const auto  citer = vectors_<U>.find (this);

    if (citer != vectors_<U>.end())
        for (auto &&element : citer->second)
            visitor(element);
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroVector::sort_impl_help_ (T &functor)  {

    auto    iter = vectors_<U>.find (this);

    if (iter != vectors_<U>.end())
        std::sort (iter->second.begin(), iter->second.end(), functor);
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroVector::change_impl_help_ (T &functor)  {

    auto    iter = vectors_<U>.find (this);

    if (iter != vectors_<U>.end())
        functor(iter->second);
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroVector::change_impl_help_ (T &functor) const  {

    const auto  citer = vectors_<U>.find (this);

    if (citer != vectors_<U>.end())
        functor(citer->second);
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector::visit_impl_ (T &&visitor, TLIST<TYPES...>)  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector::visit_impl_ (T &&visitor, TLIST<TYPES...>) const  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector::sort_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (sort_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector::change_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroVector::change_impl_ (T &&functor, TLIST<TYPES...>) const  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<typename T>
void HeteroVector::erase(size_type pos)  {

    auto    &vec = get_vector<T>();

    vec.erase (vec.begin() + pos);
}

// ----------------------------------------------------------------------------

template<typename T>
void HeteroVector::resize(size_type count)  {

    get_vector<T>().resize (count);
}

// ----------------------------------------------------------------------------

template<typename T>
void HeteroVector::resize(size_type count, const T &v)  {

    get_vector<T>().resize (count, v);
}

// ----------------------------------------------------------------------------

template<typename T>
void HeteroVector::pop_back()  { get_vector<T>().pop_back (); }

// ----------------------------------------------------------------------------

template<typename T>
bool HeteroVector::empty() const noexcept  {

    return (get_vector<T>().empty ());
}

// ----------------------------------------------------------------------------

template<typename T>
T &HeteroVector::at(size_type idx)  {

    return (get_vector<T>().at (idx));
}

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroVector::at(size_type idx) const  {

    return (get_vector<T>().at (idx));
}

// ----------------------------------------------------------------------------

template<typename T>
T &HeteroVector::back()  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroVector::back() const  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<typename T>
T &HeteroVector::front()  { return (get_vector<T>().front ()); }

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroVector::front() const  { return (get_vector<T>().front ()); }

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
