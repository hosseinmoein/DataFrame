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

#pragma once

#include <DataFrame/Vectors/VectorPtrView.h>

#include <functional>
#include <new>
#include <type_traits>
#include <unordered_map>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<std::size_t A = 0>
struct  HeteroConstPtrView {

    static constexpr std::align_val_t   align_value { A };

    using size_type = std::size_t;

    HeteroConstPtrView();
    template<typename T>
    HeteroConstPtrView(const T *begin_ptr, const T *end_ptr);

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
    void set_begin_end_special(const T *bp, const T *ep_1);
    template<typename T>
    void set_empty_vec();

    template<typename T>
    HeteroConstPtrView(VectorConstPtrView<T, A> &vec);
    template<typename T>
    HeteroConstPtrView(VectorConstPtrView<T, A> &&vec);
    HeteroConstPtrView(const HeteroConstPtrView &that);
    HeteroConstPtrView(HeteroConstPtrView &&that);

    ~HeteroConstPtrView() { clear(); }

    HeteroConstPtrView &operator= (const HeteroConstPtrView &rhs);
    HeteroConstPtrView &operator= (HeteroConstPtrView &&rhs);

    template<typename T>
    [[nodiscard]] VectorConstPtrView<T, A> &get_vector();
    template<typename T>
    [[nodiscard]] const VectorConstPtrView<T, A> &get_vector() const;

    template<typename T>
    [[nodiscard]] size_type size () const { return (get_vector<T>().size()); }

    void clear();

    template<typename T>
    [[nodiscard]] bool empty() const noexcept;

    template<typename T>
    [[nodiscard]] const T &at(size_type idx) const;

    template<typename T>
    [[nodiscard]] const T &back() const;

    template<typename T>
    [[nodiscard]] const T &front() const;

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
    std::unordered_map<
        const HeteroConstPtrView *, VectorConstPtrView<T, A>,
        std::hash<const HeteroConstPtrView *>,
        std::equal_to<const HeteroConstPtrView *>,
        typename allocator_declare<
            std::pair<const HeteroConstPtrView *const,
                      VectorConstPtrView<T, A>>, A>::type>
        views_ {  };


    std::function<void(HeteroConstPtrView &)>
        clear_function_ {
            [](HeteroConstPtrView &) { return; }
        };
    std::function<void(const HeteroConstPtrView &, HeteroConstPtrView &)>
        copy_function_ {
            [](const HeteroConstPtrView &, HeteroConstPtrView &)  { return; }
        };
    std::function<void(HeteroConstPtrView &, HeteroConstPtrView &)>
        move_function_  {
            [](HeteroConstPtrView &, HeteroConstPtrView &)  { return; }
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
#    include <DataFrame/Vectors/HeteroConstPtrView.tcc>
#  endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
