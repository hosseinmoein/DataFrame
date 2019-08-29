// Hossein Moein
// October 24, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <DataFrame/Vectors/VectorView.h>

#include <functional>
#include <unordered_map>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct  HeteroView  {

public:

    using size_type = size_t;

    HeteroView() = default;
    template<typename T>
    HeteroView(T *begin_ptr, T *end_ptr);
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

private:

    template<typename T>
    static std::unordered_map<const HeteroView *, VectorView<T>>    views_;

    std::function<void(HeteroView &)>   clear_function_ {
        [](HeteroView &) { return; } };
    std::function<void(const HeteroView &, HeteroView &)>   copy_function_ {
        [](const HeteroView &, HeteroView &)  { return; } };
    std::function<void(HeteroView &, HeteroView &)> move_function_  {
        [](HeteroView &, HeteroView &)  { return; } };

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
#    include <DataFrame/Vectors/HeteroView.tcc>
#  endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
