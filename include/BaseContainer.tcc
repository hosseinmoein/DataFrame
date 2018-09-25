// Hossein Moein
// September 12, 2017
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include "BaseContainer.h"
#include <type_traits>
#include <limits>

// ----------------------------------------------------------------------------

namespace hmdf
{

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
template<typename T, typename V>
std::unordered_map<const HeteroVector *, V> HeteroVector::vectors_;
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)

#ifdef _WIN32
template<typename T, typename V>
std::unordered_map<const HeteroVector *, V> HeteroVector::vectors_{};
#endif // _WIN32

// ----------------------------------------------------------------------------

template<typename T, typename V>
V &HeteroVector::get_vec()  {

    auto    iter = vectors_<T>.find (this);

    // don't have it yet, so create functions for copying and destroying
    if (iter == vectors_<T>.end())  {
        clear_functions_.reserve(4);
        copy_functions_.reserve(4);
        move_functions_.reserve(4);

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

        iter = vectors_<T>.emplace (this, V()).first;
    }

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
const V &HeteroVector::get_vec() const  {

    return (const_cast<HeteroVector *>(this)->get_vec<T, V>());
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
void HeteroVector::push_back(const T &v)  { get_vec<T, V>().push_back (v); }

// ----------------------------------------------------------------------------

template<typename T, typename V, class... Args>
void HeteroVector::emplace_back (Args &&... args)  {

    get_vec<T, V>().emplace_back (std::forward<Args>(args)...);
}

// ----------------------------------------------------------------------------

template<typename T, typename V, class... Args>
void HeteroVector::emplace (typename V::const_iterator pos, Args &&... args)  {

    get_vec<T, V>().emplace (pos, std::forward<Args>(args)...);
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

template<typename T, typename V>
void HeteroVector::resize(typename V::size_type count)  {

    get_vec<T, V>().resize (count);
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
void HeteroVector::resize(typename V::size_type count, const T &v)  {

    get_vec<T, V>().resize (count, v);
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
void HeteroVector::pop_back()  { get_vec<T, V>().pop_back (); }

// ----------------------------------------------------------------------------

template<typename T, typename V>
bool HeteroVector::empty() const noexcept  {

    return (get_vec<T, V>().empty ());
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
T &HeteroVector::at(typename V::size_type idx)  {

    return (get_vec<T, V>().at (idx));
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
const T &HeteroVector::at(typename V::size_type idx) const  {

    return (get_vec<T, V>().at (idx));
}

// ----------------------------------------------------------------------------

template<typename T, typename V>
T &HeteroVector::back()  { return (get_vec<T, V>().back ()); }

// ----------------------------------------------------------------------------

template<typename T, typename V>
const T &HeteroVector::back() const  { return (get_vec<T, V>().back ()); }

// ----------------------------------------------------------------------------

template<typename T, typename V>
T &HeteroVector::front()  { return (get_vec<T, V>().front ()); }

// ----------------------------------------------------------------------------

template<typename T, typename V>
const T &HeteroVector::front() const  { return (get_vec<T, V>().front ()); }

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
