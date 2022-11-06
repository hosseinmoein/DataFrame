// Hossein Moein
// November 4, 2022
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

template<typename T>
HeteroConstPtrView::HeteroConstPtrView(const T *begin_ptr, const T *end_ptr)
    : clear_function_([](HeteroConstPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroConstPtrView &from,
                        HeteroConstPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroConstPtrView &from, HeteroConstPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, VectorConstPtrView<T>(begin_ptr, end_ptr));
}

// ----------------------------------------------------------------------------

template<typename T>
void HeteroConstPtrView::set_begin_end_special(const T *bp, const T *ep_1)  {

    clear_function_ = [](HeteroConstPtrView &hv) { views_<T>.erase(&hv); };
    copy_function_  = [](const HeteroConstPtrView &from,
                        HeteroConstPtrView &to)  {
                          views_<T>[&to] = views_<T>[&from];
                      };
    move_function_ = [](HeteroConstPtrView &from, HeteroConstPtrView &to)  {
                         views_<T>[&to] = std::move(views_<T>[&from]);
                     };

    VectorConstPtrView<T>   vv;

    vv.set_begin_end_special(bp, ep_1);
    views_<T>.emplace(this, vv);
}

// ----------------------------------------------------------------------------

template<typename T>
HeteroConstPtrView::HeteroConstPtrView(VectorConstPtrView<T> &vec)
    : clear_function_([](HeteroConstPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroConstPtrView &from,
                        HeteroConstPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroConstPtrView &from, HeteroConstPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, vec);
}

// ----------------------------------------------------------------------------

template<typename T>
HeteroConstPtrView::HeteroConstPtrView(VectorConstPtrView<T> &&vec)
    : clear_function_([](HeteroConstPtrView &hv) { views_<T>.erase(&hv); }),
      copy_function_([](const HeteroConstPtrView &from,
                        HeteroConstPtrView &to)  {
              views_<T>[&to] = views_<T>[&from]; }),
      move_function_([](HeteroConstPtrView &from, HeteroConstPtrView &to)  {
              views_<T>[&to] = std::move(views_<T>[&from]); })  {

    views_<T>.emplace(this, std::move(vec));
}

// ----------------------------------------------------------------------------

template<typename T>
VectorConstPtrView<T> &HeteroConstPtrView::get_vector()  {

    auto    iter = views_<T>.find (this);

    if (iter == views_<T>.end())
        throw std::runtime_error("HeteroConstPtrView::get_vector(): ERROR: "
                                 "Cannot find view");

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<typename T>
const VectorConstPtrView<T> &HeteroConstPtrView::get_vector() const  {

    const auto  iter = views_<T>.find (this);

    if (iter == views_<T>.end())
        throw std::runtime_error("HeteroConstPtrView::get_vector(): ERROR: "
                                 "Cannot find view");

    return (iter->second);
}

// ----------------------------------------------------------------------------

template<typename T, typename U>
void HeteroConstPtrView::visit_impl_help_ (T &visitor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end())  {
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

template<typename T, typename U>
void HeteroConstPtrView::change_impl_help_ (T &functor) const  {

    const auto  citer = views_<U>.find (this);

    if (citer != views_<U>.end())
        functor(citer->second);
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroConstPtrView::visit_impl_ (T &&visitor, TLIST<TYPES...>) const  {

    // (..., visit_impl_help_<std::decay_t<T>, TYPES>(visitor)); // C++17
    using expander = int[];
    (void) expander { 0, (visit_impl_help_<T, TYPES>(visitor), 0) ... };
}

// ----------------------------------------------------------------------------

template<class T, template<class...> class TLIST, class... TYPES>
void HeteroConstPtrView::change_impl_ (T &&functor, TLIST<TYPES...>) const  {

    using expander = int[];
    (void) expander { 0, (change_impl_help_<T, TYPES>(functor), 0) ... };
}

// ----------------------------------------------------------------------------

template<typename T>
bool HeteroConstPtrView::empty() const noexcept  {

    return (get_vector<T>().empty ());
}

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroConstPtrView::at(size_type idx) const  {

    return (get_vector<T>()[idx]);
}

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroConstPtrView::back() const { return (get_vector<T>().back ()); }

// ----------------------------------------------------------------------------

template<typename T>
const T &HeteroConstPtrView::front() const  {

    return (get_vector<T>().front ());
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
