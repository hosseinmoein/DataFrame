// Hossein Moein
// September 11, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

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

public:

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

private:

    template<typename T>
    static std::unordered_map<const HeteroVector *, std::vector<T>> vectors_;

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

#  ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#    include <DataFrame/Vectors/HeteroVector.tcc>
#  endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
