// Hossein Moein
// June 24, 2019
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <algorithm>

// ----------------------------------------------------------------------------

namespace hmdf
{

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
template<typename T>
std::unordered_map<const HeteroPtrView *, VectorPtrView<T>>
    HeteroPtrView::views_;
#endif // defined(__linux__) || defined(__unix__) || defined(__APPLE__)

#ifdef _WIN32
template<typename T>
std::unordered_map<const HeteroPtrView *, VectorPtrView<T>>
    HeteroPtrView::views_{  };
#endif // _WIN32

// ----------------------------------------------------------------------------

template<typename T>
HeteroPtrView::HeteroPtrView(T *begin_ptr, T *end_ptr)
    : clear_function_([](HeteroPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, VectorPtrView<T>(begin_ptr, end_ptr));
}

// ----------------------------------------------------------------------------

template<typename T>
HeteroPtrView::HeteroPtrView(VectorPtrView<T> &vec)
    : clear_function_([](HeteroPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.push_back(this, vec);
}

// ----------------------------------------------------------------------------

template<typename T>
HeteroPtrView::HeteroPtrView(VectorPtrView<T> &&vec)
    : clear_function_([](HeteroPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroPtrView &from, HeteroPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, std::move(vec));
}

// ----------------------------------------------------------------------------

template<typename T>
VectorPtrView<T> &HeteroPtrView::get_vector()  {

    auto    iter = views_<T>.find (this);

    if (iter == views_<T>.end())
        throw std::runtime_error("HeteroPtrView::get_vector(): ERROR: "
                                 "Cannot find view");

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<typename T>
const VectorPtrView<T> &HeteroPtrView::get_vector() const  {

    return (const_cast<HeteroPtrView *>(this)->get_vector<T>());
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroPtrView::visit_impl_help_ (T &visitor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end())  {
#ifndef _WIN32
        for (auto &&element : iter->second)
            visitor(element);
#else
        const size_type vec_size = iter->second.size();

        for (size_type i = 0; i < vec_size; ++i)
            visitor(iter->second[i]);
#endif // _WIN32
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroPtrView::visit_impl_help_ (T &visitor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end())  {
#ifndef _WIN32
        for (auto &&element : citer->second)
            visitor(element);
#else
        const size_type vec_size = citer->second.size();

        for (size_type i = 0; i < vec_size; ++i)
            visitor(citer->second[i]);
#endif // _WIN32
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroPtrView::sort_impl_help_ (T &functor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end())
        std::sort (iter->second.begin(), iter->second.end(), functor);
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroPtrView::change_impl_help_ (T &functor)  {

    auto    iter = views_<U>.find (this);

    if (iter != views_<U>.end())
        functor(iter->second);
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroPtrView::change_impl_help_ (T &functor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end())
        functor(citer->second);
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView::visit_impl_ (T &&visitor, TLIST<TYPES...>)  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView::visit_impl_ (T &&visitor, TLIST<TYPES...>) const  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView::sort_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (sort_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView::change_impl_ (T &&functor, TLIST<TYPES...>)  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroPtrView::change_impl_ (T &&functor, TLIST<TYPES...>) const  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<typename T>
bool HeteroPtrView::empty() const noexcept  {

    return (get_vector<T>().empty ());
}

// ----------------------------------------------------------------------------

template<typename T>
T &HeteroPtrView::at(size_type idx)  {

    return (get_vector<T>()[idx]);
}

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroPtrView::at(size_type idx) const  {

    return (get_vector<T>()[idx]);
}

// ----------------------------------------------------------------------------

template<typename T>
T &HeteroPtrView::back()  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroPtrView::back() const  { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<typename T>
T &HeteroPtrView::front()  { return (get_vector<T>().front ()); }

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroPtrView::front() const  { return (get_vector<T>().front ()); }

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
