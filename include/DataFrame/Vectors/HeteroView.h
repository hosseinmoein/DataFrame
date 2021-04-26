// Hossein Moein
// October 24, 2018
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

#pragma once

#include <DataFrame/Vectors/VectorView.h>

#include <functional>
#include <unordered_map>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct  HeteroView  {

    using size_type = size_t;

    HeteroView() = default;
    template<typename T>
    HeteroView(T *begin_ptr, T *end_ptr);

    // The purpose of this method is for the user be able to conform to STL
    // standards.
    // To create a VectorView over an entire std::vector you have to do this:
    //        VectorView(&(*v.begin()), &(*v.end()));
    // The above second parameter is against standards and it is caught
    // if you set the STL boundary check flag. So instead, you can do:
    //        VectorView vv;
    //        vv.set_begin_end_special(&(*v.begin()), &(v.back()));
    //
    template<typename T>
    void set_begin_end_special(T *bp, T *ep_1);

    HeteroView(const HeteroView &that);
    HeteroView(HeteroView &&that);

    ~HeteroView() { clear(); }

    HeteroView &operator= (const HeteroView &rhs);
    HeteroView &operator= (HeteroView &&rhs);

    template<typename T>
    VectorView<T> &get_vector();
    template<typename T>
    const VectorView<T> &get_vector() const;

    template<typename T>
    typename VectorView<T>::
    size_type size () const { return (get_vector<T>().size()); }

    void clear();

    template<typename T>
    bool empty() const noexcept;

    template<typename T>
    T &at(size_type idx);
    template<typename T>
    const T &at(size_type idx) const;

    template<typename T>
    T &back();
    template<typename T>
    const T &back() const;

    template<typename T>
    T &front();
    template<typename T>
    const T &front() const;

    template<typename T>
    using iterator = typename VectorView<T>::iterator;
    template<typename T>
    using const_iterator = typename VectorView<T>::const_iterator;
    template<typename T>
    using reverse_iterator = typename VectorView<T>::reverse_iterator;
    template<typename T>
    using const_reverse_iterator =
        typename VectorView<T>::const_reverse_iterator;

    template<typename T>
    iterator<T> begin();
    template<typename T>
    const_iterator<T> begin() const;

    template<typename T>
    iterator<T> end();
    template<typename T>
    const_iterator<T> end() const;

    template<typename T>
    reverse_iterator<T> rbegin();
    template<typename T>
    const_reverse_iterator<T> rbegin() const;

    template<typename T>
    reverse_iterator<T> rend();
    template<typename T>
    const_reverse_iterator<T> rend() const;

    template<typename... Ts>
    struct type_list  {   };

    template<typename... Ts>
    struct visitor_base  { using types = type_list<Ts ...>; };

    template<typename T>
    void visit (T &&visitor)  {

        visit_impl_ (visitor, typename std::decay_t<T>::types { });
    }
    template<typename T>
    void visit (T &&visitor) const  {

        visit_impl_ (visitor, typename std::decay_t<T>::types { });
    }
    template<typename T>
    void sort (T &&functor)  {

        sort_impl_ (functor, typename std::decay_t<T>::types { });
    }
    template<typename T>
    void change (T &&functor)  {

        change_impl_ (functor, typename std::decay_t<T>::types { });
    }
    template<typename T>
    void change (T &&functor) const  {

        change_impl_ (functor, typename std::decay_t<T>::types { });
    }

private:

    template<typename T>
    inline static
    std::unordered_map<const HeteroView *, VectorView<T>>   views_ {  };

    std::function<void(HeteroView &)>   clear_function_ {
        [](HeteroView &) { return; }
    };
    std::function<void(const HeteroView &, HeteroView &)>   copy_function_ {
        [](const HeteroView &, HeteroView &)  { return; }
    };
    std::function<void(HeteroView &, HeteroView &)> move_function_  {
        [](HeteroView &, HeteroView &)  { return; }
    };

    // Visitor stuff
    //
    template<typename T, typename U>
    void visit_impl_help_ (T &visitor);
    template<typename T, typename U>
    void visit_impl_help_ (T &visitor) const;

    template<typename T, typename U>
    void sort_impl_help_ (T &functor);

    template<typename T, typename U>
    void change_impl_help_ (T &functor);
    template<typename T, typename U>
    void change_impl_help_ (T &functor) const;

    // Specific visit implementations
    //
    template<class T, template<class...> class TLIST, class... TYPES>
    void visit_impl_ (T &&visitor, TLIST<TYPES...>);
    template<class T, template<class...> class TLIST, class... TYPES>
    void visit_impl_ (T &&visitor, TLIST<TYPES...>) const;

    template<class T, template<class...> class TLIST, class... TYPES>
    void sort_impl_ (T &&functor, TLIST<TYPES...>);

    template<class T, template<class...> class TLIST, class... TYPES>
    void change_impl_ (T &&functor, TLIST<TYPES...>);
    template<class T, template<class...> class TLIST, class... TYPES>
    void change_impl_ (T &&functor, TLIST<TYPES...>) const;
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#  ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#    include <DataFrame/Vectors/HeteroView.tcc>
#  endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
