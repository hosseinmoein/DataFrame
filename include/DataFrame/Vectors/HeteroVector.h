// Hossein Moein
// September 11, 2017
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

#include <DataFrame/Vectors/HeteroPtrView.h>
#include <DataFrame/Vectors/HeteroView.h>

#include <unordered_map>

// ----------------------------------------------------------------------------

namespace hmdf
{

// This class implements a heterogeneous vector. Its design and implementation
// are partly inspired by Andy G's Blog at:
// https://gieseanw.wordpress.com/2017/05/03/a-true-heterogeneous-container/
//
struct  HeteroVector  {

    using size_type = size_t;

    HeteroVector();
    HeteroVector(const HeteroVector &that);
    HeteroVector(HeteroVector &&that);

    ~HeteroVector() { clear(); }

    HeteroVector &operator= (const HeteroVector &rhs);
    HeteroVector &operator= (HeteroVector &&rhs);

    template<typename T>
    std::vector<T> &get_vector();
    template<typename T>
    const std::vector<T> &get_vector() const;

    // It returns a view of the underlying vector.
    // NOTE: One can modify the vector through the view. But the vector
    //       cannot be extended or shrunk through the view.
    // There is no const version of this method
    //
    template<typename T>
    HeteroView get_view(size_type begin = 0, size_type end = -1);

    template<typename T>
    HeteroPtrView get_ptr_view(size_type begin = 0, size_type end = -1);

    template<typename T>
    void push_back(const T &v);
    template<typename T, class... Args>
    void emplace_back (Args &&... args);
    template<typename T, typename ITR, class... Args>
    void emplace (ITR pos, Args &&... args);

    template<typename T>
    void reserve (size_type r)  { get_vector<T>().reserve (r); }
    template<typename T>
    void shrink_to_fit () { get_vector<T>().shrink_to_fit (); }

    template<typename T>
    size_type size () const { return (get_vector<T>().size()); }

    void clear();

    template<typename T>
    void erase(size_type pos);

    template<typename T>
    void resize(size_type count);
    template<typename T>
    void resize(size_type count, const T &v);

    template<typename T>
    void pop_back();

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
    using iterator = typename std::vector<T>::iterator;
    template<typename T>
    using const_iterator = typename std::vector<T>::const_iterator;
    template<typename T>
    using reverse_iterator = typename std::vector<T>::reverse_iterator;
    template<typename T>
    using const_reverse_iterator =
        typename std::vector<T>::const_reverse_iterator;

    template<typename T>
    inline iterator<T>
    begin() noexcept { return (get_vector<T>().begin()); }

    template<typename T>
    inline iterator<T>
    end() noexcept { return (get_vector<T>().end()); }

    template<typename T>
    inline const_iterator<T>
    begin () const noexcept { return (get_vector<T>().begin()); }

    template<typename T>
    inline const_iterator<T>
    end () const noexcept { return (get_vector<T>().end()); }

    template<typename T>
    inline reverse_iterator<T>
    rbegin() noexcept { return (get_vector<T>().rbegin()); }

    template<typename T>
    inline reverse_iterator<T>
    rend() noexcept { return (get_vector<T>().rend()); }

    template<typename T>
    inline const_reverse_iterator<T>
    rbegin () const noexcept { return (get_vector<T>().rbegin()); }

    template<typename T>
    inline const_reverse_iterator<T>
    rend () const noexcept { return (get_vector<T>().rend()); }

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
    inline static std::unordered_map<const HeteroVector *, std::vector<T>>
        vectors_ {  };

    std::vector<std::function<void(HeteroVector &)>>    clear_functions_;
    std::vector<std::function<void(const HeteroVector &,
                                   HeteroVector &)>>    copy_functions_;
    std::vector<std::function<void(HeteroVector &,
                                   HeteroVector &)>>    move_functions_;

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
#    include <DataFrame/Vectors/HeteroVector.tcc>
#  endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
