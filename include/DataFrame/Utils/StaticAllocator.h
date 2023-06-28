// Hossein Moein
// June 26 2023
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

#include <algorithm>
#include <cstring>
#include <functional>
#include <iterator>
#include <new>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct  AllocatorFlags  {
    inline static constexpr unsigned char   FREE_ { 0 };
    inline static constexpr unsigned char   USED_ { 1 };
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE>
struct  StaticAllocStorage : public AllocatorFlags  {

    using value_type = T;
    using Base = AllocatorFlags;

    StaticAllocStorage()  {

        // This is guaranteed to execute only once even in multithreading
        //
        [[maybe_unused]] static auto    slug = std::invoke([]() -> int  {
            std::memset(in_use_, Base::FREE_, MAX_SIZE);
            return (0);
        });
    }
    StaticAllocStorage(const StaticAllocStorage &) = default;
    StaticAllocStorage(StaticAllocStorage &&) = delete;
    ~StaticAllocStorage() = default;

    StaticAllocStorage &operator =(const StaticAllocStorage &) = delete;
    StaticAllocStorage &operator =(StaticAllocStorage &&) = delete;

    // Main allocation space
    //
    alignas(value_type[])
    inline static unsigned char buffer_[MAX_SIZE * sizeof(value_type)];

    // The bitmap to indicate which slots are in use.
    //
    alignas(64)
    inline static unsigned char in_use_[MAX_SIZE];

    // Pointer to the first free slot.
    //
    alignas(value_type *)
    inline static unsigned char *first_free_ptr_ { in_use_ };
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE>
struct  StackAllocStorage : public AllocatorFlags  {

    using value_type = T;
    using Base = AllocatorFlags;

    StackAllocStorage()  { std::memset(in_use_, Base::FREE_, MAX_SIZE); }
    StackAllocStorage(const StackAllocStorage &)  {

        std::memset(in_use_, Base::FREE_, MAX_SIZE);
    }
    StackAllocStorage(StackAllocStorage &&) = delete;
    ~StackAllocStorage() = default;

    StackAllocStorage &operator =(const StackAllocStorage &) = delete;
    StackAllocStorage &operator =(StackAllocStorage &&) = delete;

    // Main allocation space
    //
    alignas(value_type[])
    unsigned char buffer_[MAX_SIZE * sizeof(value_type)];

    // The bitmap to indicate which slots are in use.
    //
    alignas(64)
    unsigned char in_use_[MAX_SIZE];

    // Pointer to the first free slot.
    //
    alignas(value_type *)
    unsigned char *first_free_ptr_ { in_use_ };
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE,
         template<typename, std::size_t> typename STORE>
class   FixedAllocator : private STORE<T, MAX_SIZE>  {

    using Base = STORE<T, MAX_SIZE>;

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

    // This is only necessary because allocator has a second and third template
    // arguments for the alignment that will make the default
    // std::allocator_traits implementation fail during compilation.
    //
    template<typename U>
    struct  rebind  { using other = FixedAllocator<U, MAX_SIZE, STORE>; };

    [[nodiscard]] pointer
    address(reference r) const  { return (std::addressof(r)); }
    [[nodiscard]] const_pointer
    address(const_reference cr) const  { return (std::addressof(cr)); }

    [[nodiscard]] constexpr size_type max_size() const  { return (MAX_SIZE); }

public:

    FixedAllocator() : Base()  {   }
    FixedAllocator(const FixedAllocator &that) : Base(that)  {   }
    FixedAllocator(FixedAllocator &&) = delete;
    ~FixedAllocator() = default;

    FixedAllocator &operator =(FixedAllocator &&) = delete;
    FixedAllocator &operator =(const FixedAllocator &) = delete;

    template<typename U>
    FixedAllocator(const FixedAllocator<U, MAX_SIZE, STORE> &)  {   }

    // Always return true for stateless allocators.
    //
    [[nodiscard]] inline bool
    operator == (const FixedAllocator &) const  { return (true); }
    [[nodiscard]] inline bool
    operator != (const FixedAllocator &) const  { return (false); }

public:

    inline void construct(pointer p, const_reference val) const  {

        new (static_cast<void *>(p)) value_type(val);
    }
    inline void construct(pointer p) const  {

        new (static_cast<void *>(p)) value_type();
    }

    template<typename U, typename ... Ts>
    inline void construct(U *p, Ts && ... args) const  {

        new (static_cast<void *>(p)) U(std::forward<Ts>(args) ...);
    }

    inline void destroy(pointer p) const  { p->~value_type(); }

    template<typename U>
    inline void destroy(U *p) const  { p->~U(); }

    [[nodiscard]] inline pointer
    allocate(size_type n_items, [[maybe_unused]] const_pointer cp)  {

        // Pointers to the "in use" bitmap.
        //
        unsigned char           *first_ptr = Base::first_free_ptr_;
        unsigned char *const    end_ptr = &(Base::in_use_[MAX_SIZE]);

        // Find first fit allocation algorithm, starting from the first
        // free slot.

        // Search for a big enough range of free slots.
        //
        first_ptr = std::search_n(first_ptr, end_ptr, n_items, Base::FREE_);

        // Not enough space found?
        //
        if (first_ptr == end_ptr)
            throw std::bad_alloc();

        // Mark the range as used
        //
        std::memset(first_ptr, Base::USED_, n_items);

        // Update the "first free" pointer if necessary.
        //
        if (first_ptr == Base::first_free_ptr_) // Find the next free slot
            Base::first_free_ptr_ =
                std::find(first_ptr + n_items, end_ptr, Base::FREE_);

        // Return the memory allocation.
        //
        const size_type offset =
            std::distance(Base::in_use_, first_ptr) * sizeof(value_type);

        return (reinterpret_cast<pointer>(&(Base::buffer_[offset])));
    }

    [[nodiscard]] inline pointer allocate(size_type n_items)  {

        return (allocate(n_items, nullptr));
    }

    void deallocate(pointer p, size_type n_items)  {

        // Find the start of the range.
        //
        const size_type index =
            std::distance(Base::buffer_, reinterpret_cast<unsigned char *>(p)) /
            sizeof(value_type);

        unsigned char   *first_ptr = &(Base::in_use_[index]);

        // Mark the range as free.
        //
        std::memset(first_ptr, Base::FREE_, n_items);

        // Update the "first free" pointer if necessary.
        //
        if (first_ptr < Base::first_free_ptr_)
            Base::first_free_ptr_ = first_ptr;
    }
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE>
using StaticAllocator = FixedAllocator<T, MAX_SIZE, StaticAllocStorage>;

template<typename T, std::size_t MAX_SIZE>
using StackAllocator = FixedAllocator<T, MAX_SIZE, StackAllocStorage>;

} // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
