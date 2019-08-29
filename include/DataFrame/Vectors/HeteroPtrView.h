// Hossein Moein
// June 24, 2019
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <DataFrame/Vectors/VectorPtrView.h>

#include <functional>
#include <unordered_map>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct  HeteroPtrView  {

public:

    using size_type = size_t;

    HeteroPtrView() = default;
    template<typename T>
    HeteroPtrView(T *begin_ptr, T *end_ptr);
    template<typename T>
    HeteroPtrView(VectorPtrView<T> &vec);
    template<typename T>
    HeteroPtrView(VectorPtrView<T> &&vec);
    HeteroPtrView(const HeteroPtrView &that);
    HeteroPtrView(HeteroPtrView &&that);

    ~HeteroPtrView() { clear(); }

    HeteroPtrView &operator= (const HeteroPtrView &rhs);
    HeteroPtrView &operator= (HeteroPtrView &&rhs);

    template<typename T>
    VectorPtrView<T> &get_vector();
    template<typename T>
    const VectorPtrView<T> &get_vector() const;

    template<typename T>
    void reserve (size_type r)  { get_vector<T>().reserve (r); }
    template<typename T>
    void shrink_to_fit () { get_vector<T>().shrink_to_fit (); }

    template<typename T>
    typename VectorPtrView<T>::
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

private:

    template<typename T>
    static std::unordered_map<const HeteroPtrView *, VectorPtrView<T>> views_;

    std::function<void(HeteroPtrView &)>
        clear_function_ {
            [](HeteroPtrView &) { return; }
        };
    std::function<void(const HeteroPtrView &, HeteroPtrView &)>
        copy_function_ {
            [](const HeteroPtrView &, HeteroPtrView &)  { return; }
        };
    std::function<void(HeteroPtrView &, HeteroPtrView &)>
        move_function_  {
            [](HeteroPtrView &, HeteroPtrView &)  { return; }
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
#    include <DataFrame/Vectors/HeteroPtrView.tcc>
#  endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
