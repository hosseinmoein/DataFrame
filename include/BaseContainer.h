// Hossein Moein
// September 11, 2017
// To the extent possible under law, the author(s) have dedicated all
// copyright and related and neighboring rights to this software to
// the public domain worldwide. This software is distributed without
// any warranty.

#pragma once

#include <vector>
#include <functional>
#include <unordered_map>
#include <utility>
#include <algorithm>

// #include <DMSob_ObjectBase.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

// This class implements a heterogeneous vector. Its design and implementation
// are partly inspired by Andy G's Blog at:
// https://gieseanw.wordpress.com/2017/05/03/a-true-heterogeneous-container/
//
struct  HeteroVector  {

public:

    HeteroVector();
    HeteroVector(const HeteroVector &that);
    HeteroVector(HeteroVector &&that);

    ~HeteroVector() { clear(); }

    HeteroVector &operator= (const HeteroVector &rhs);
    HeteroVector &operator= (HeteroVector &&rhs);

    template<typename T, typename V = std::vector<T>>
    V &get_vec();
    template<typename T, typename V = std::vector<T>>
    const V &get_vec() const;

    template<typename T, typename V = std::vector<T>>
    void push_back(const T &v);
    template<typename T, typename V = std::vector<T>, class... Args>
    void emplace_back (Args &&... args);
    template<typename T, typename V = std::vector<T>, class... Args>
    void emplace (typename V::const_iterator pos, Args &&... args);

    template<typename T, typename V = std::vector<T>>
    void reserve (typename V::size_type r)  { get_vec<T, V>().reserve (r); }
    template<typename T, typename V = std::vector<T>>
    void shrink_to_fit () { get_vec<T, V>().shrink_to_fit (); }

    template<typename T, typename V = std::vector<T>>
    typename V::size_type size () const { return (get_vec<T, V>().size()); }

    void clear();

    template<typename T, typename V = std::vector<T>>
    void resize(typename V::size_type count);
    template<typename T, typename V = std::vector<T>>
    void resize(typename V::size_type count, const T &v);

    template<typename T, typename V = std::vector<T>>
    void pop_back();

    template<typename T, typename V = std::vector<T>>
    bool empty() const noexcept;

    template<typename T, typename V = std::vector<T>>
    T &at(typename V::size_type idx);
    template<typename T, typename V = std::vector<T>>
    const T &at(typename V::size_type idx) const;

    template<typename T, typename V = std::vector<T>>
    T &back();
    template<typename T, typename V = std::vector<T>>
    const T &back() const;

    template<typename T, typename V = std::vector<T>>
    T &front();
    template<typename T, typename V = std::vector<T>>
    const T &front() const;

private:

    template<typename T, typename V = std::vector<T>>
    static std::unordered_map<const HeteroVector *, V>  items_;

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

public:

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
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#  ifdef DMS_INCLUDE_SOURCE
#    include <BaseContainer.tcc>
#  endif // DMS_INCLUDE_SOURCE

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
