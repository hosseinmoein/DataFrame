// Hossein Moein
// December 8 2022
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

#include <limits>
#include <memory>
#include <new>
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, std::size_t AS>
struct  AlignedValue  {

    static constexpr std::align_val_t   align_value { AS };
};

template<typename T>
struct  AlignedValue<T, 0>  {

    static constexpr std::align_val_t   align_value { alignof(T) };
};

// ----------------------------------------------------------------------------

// By default, the alignment is system defined alignment for the type
//
template<typename T, std::size_t AS = 0>
class   AlignedAllocator : public AlignedValue<T, AS>  {

    static_assert(AS == 0 || AS >= alignof(T),
                  "ERROR: The requested alignment must be bigger or equal "
                  "to Type alignment");

public:

    // std::allocator_traits stuff
    //
    using value_type = T;
    using pointer = T *;
    using const_pointer = const T *;
    using void_pointer = void *;
    using const_void_pointer = const void *;
    using reference = T &;
    using const_reference = const T &;
    using size_type = std::size_t;
    using difference_type = ptrdiff_t;
    using propagate_on_container_copy_assignment = std::false_type;
    using propagate_on_container_move_assignment = std::false_type;
    using propagate_on_container_swap = std::false_type;
    using is_always_equal = std::true_type;

    // This is only necessary because AlignedAllocator has a second template
    // argument for the alignment that will make the default
    // std::allocator_traits implementation fail during compilation.
    //
    template<typename U>
    struct  rebind  { using other = AlignedAllocator<U, AS>; };

    [[nodiscard]] inline constexpr pointer
    address(reference r) const  { return (std::addressof(r)); }
    [[nodiscard]] inline constexpr const_pointer
    address(const_reference cr) const  { return (std::addressof(cr)); }

    [[nodiscard]] constexpr size_type max_size() const  {

        return (std::numeric_limits<size_type>::max() / sizeof(value_type));
    }

public:

    AlignedAllocator() noexcept = default;
    AlignedAllocator(const AlignedAllocator &) noexcept = default;
    AlignedAllocator(AlignedAllocator &&) noexcept = default;
    ~AlignedAllocator() noexcept = default;

    AlignedAllocator &operator=(AlignedAllocator &&) noexcept = default;
    AlignedAllocator &operator=(const AlignedAllocator &) = delete;

    template<typename U>
    inline AlignedAllocator(AlignedAllocator<U, AS> const &) noexcept  {  }

    // Always return true for stateless allocators.
    //
    [[nodiscard]] constexpr bool
    operator == (const AlignedAllocator &) const  { return (true); }
    [[nodiscard]] constexpr bool
    operator != (const AlignedAllocator &) const  { return (false); }

public:

    inline void construct(pointer p, const_reference val) const  {

        new (static_cast<void *>(p)) value_type(val);
    }
    inline void construct(pointer p) const  {

        new (static_cast<void *>(p)) value_type();
    }

    template<typename U, typename ... Ts >
    inline void construct(U *p, Ts && ... args) const  {

        new (static_cast<void *>(p)) U(std::forward<Ts>(args) ...);
    }

    inline void destroy(pointer p) const  { p->~value_type(); }

    template<typename U>
    inline void destroy(U *p) const  { p->~U(); }

    [[nodiscard]] inline pointer allocate(size_type n_items) const  {

        if (n_items == 0)  return (nullptr);
        if (n_items > max_size())
            throw std::bad_array_new_length();

        return(reinterpret_cast<pointer>(
                   ::operator new[](n_items * sizeof(value_type),
                                    AlignedAllocator::align_value)));
    }

    // This is the same for all allocators that ignore hints.
    //
    [[nodiscard]] inline pointer
    allocate(size_type n_items, [[maybe_unused]] const void *hint) const  {

        return (allocate(n_items));
    }

    inline constexpr void
    deallocate(pointer ptr, [[maybe_unused]] size_type n_items) const  {

        // According to the C++20 the delete operator must be called with the
        // same alignment argument as the new expression.
        // The size argument can be omitted but if present must also be equal
        // to the one used in new
        //
        ::operator delete[](ptr, AlignedAllocator::align_value);
    }
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t A>
struct  allocator_declare  {
    using type = AlignedAllocator<T, A>;
};

template<typename T>
struct  allocator_declare<T, 0>  {
    using type = std::allocator<T>;
};

} // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
