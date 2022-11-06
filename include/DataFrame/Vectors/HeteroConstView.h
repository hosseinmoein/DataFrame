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

#pragma once

#include <DataFrame/Vectors/VectorView.h>
#include <DataFrame/DataFrameExports.h>

#include <functional>
#include <type_traits>
#include <unordered_map>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct HeteroConstView  {

    using size_type = size_t;

    HMDF_API HeteroConstView();
    template<typename T>
    HeteroConstView(const T *begin_ptr, const T *end_ptr);

    // The purpose of this method is for the user be able to conform to STL
    // standards.
    // To create a VectorConstView over an entire std::vector
    // you have to do this:
    //        VectorConstView(&(*v.begin()), &(*v.end()));
    // The above second parameter is against standards and it is caught
    // if you set the STL boundary check flag. So instead, you can do:
    //        VectorConstView vv;
    //        vv.set_begin_end_special(&(*v.begin()), &(v.back()));
    //
    template<typename T>
    void set_begin_end_special(const T *bp, const T *ep_1);

    HMDF_API HeteroConstView(const HeteroConstView &that);
    HMDF_API HeteroConstView(HeteroConstView &&that);

    ~HeteroConstView() { clear(); }

    HMDF_API HeteroConstView &operator= (const HeteroConstView &rhs);
    HMDF_API HeteroConstView &operator= (HeteroConstView &&rhs);

    template<typename T>
    VectorConstView<T> &get_vector();
    template<typename T>
    const VectorConstView<T> &get_vector() const;

    template<typename T>
    typename VectorConstView<T>::size_type
    size () const { return (get_vector<T>().size()); }

    HMDF_API void clear();

    template<typename T>
    bool empty() const noexcept;

    template<typename T>
    const T &at(size_type idx) const;

    template<typename T>
    const T &back() const;

    template<typename T>
    const T &front() const;

    template<typename T>
    using const_iterator = typename VectorConstView<T>::const_iterator;
    template<typename T>
    using const_reverse_iterator =
        typename VectorConstView<T>::const_reverse_iterator;

    template<typename T>
    const_iterator<T> begin() const;
    template<typename T>
    const_iterator<T> end() const;
    template<typename T>
    const_reverse_iterator<T> rbegin() const;
    template<typename T>
    const_reverse_iterator<T> rend() const;

    template<typename... Ts>
    struct type_list  {   };

    template<typename... Ts>
    struct visitor_base  { using types = type_list<Ts ...>; };

    template<typename T>
    void visit (T &&visitor) const  {

        visit_impl_ (visitor, typename std::decay_t<T>::types { });
    }
    template<typename T>
    void change (T &&functor) const  {

        change_impl_ (functor, typename std::decay_t<T>::types { });
    }

private:

    template<typename T>
    inline static
    std::unordered_map<const HeteroConstView *, VectorConstView<T>>
	    views_ {  };

    std::function<void(HeteroConstView &)>
	    clear_function_ {
            [](HeteroConstView &) { return; }
        };
    std::function<void(const HeteroConstView &, HeteroConstView &)>
        copy_function_ {
            [](const HeteroConstView &, HeteroConstView &)  { return; }
        };
    std::function<void(HeteroConstView &, HeteroConstView &)>
        move_function_  {
            [](HeteroConstView &, HeteroConstView &)  { return; }
        };

    // Visitor stuff
    //
    template<typename T, typename U>
    void visit_impl_help_ (T &visitor) const;

    template<typename T, typename U>
    void change_impl_help_ (T &functor) const;

    // Specific visit implementations
    //
    template<class T, template<class...> class TLIST, class... TYPES>
    void visit_impl_ (T &&visitor, TLIST<TYPES...>) const;

    template<class T, template<class...> class TLIST, class... TYPES>
    void change_impl_ (T &&functor, TLIST<TYPES...>) const;
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#  ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#    include <DataFrame/Vectors/HeteroConstView.tcc>
#  endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
