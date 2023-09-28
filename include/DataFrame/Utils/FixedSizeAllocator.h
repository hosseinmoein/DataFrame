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

#include <iostream>
#include <algorithm>
#include <cstring>
#include <functional>
#include <iterator>
#include <new>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, std::size_t MAX_SIZE>
struct  StaticStorage  {

    using value_type = T;
    using size_type = std::size_t;

    inline static constexpr size_type   max_size = MAX_SIZE;
    inline static constexpr bool        is_static = true;

    StaticStorage() = default;
    StaticStorage(const StaticStorage &) = delete;
    StaticStorage(StaticStorage &&) = delete;
    ~StaticStorage() = default;

    StaticStorage &operator =(const StaticStorage &) = delete;
    StaticStorage &operator =(StaticStorage &&) = delete;

    // Main allocation space
    //
    alignas(value_type[])
    inline static unsigned char buffer_[MAX_SIZE * sizeof(value_type)];
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE>
struct  StackStorage  {

    using value_type = T;
    using size_type = std::size_t;

    inline static constexpr size_type   max_size = MAX_SIZE;
    inline static constexpr bool        is_static = false;

    StackStorage() = default;
    StackStorage(const StackStorage &) = delete;
    StackStorage(StackStorage &&) = delete;
    ~StackStorage() = default;

    StackStorage &operator =(const StackStorage &) = delete;
    StackStorage &operator =(StackStorage &&) = delete;

    // Main allocation space
    //
    alignas(value_type[])
    unsigned char buffer_[MAX_SIZE * sizeof(value_type)];
};

// ----------------------------------------------------------------------------

struct  BestFitMemoryBlock  {

    using value_type = unsigned char *;
    using size_type = std::size_t;

    value_type  address { nullptr };
    size_type   size { 0 };

    value_type get_end() const  { return (address + size); }
    value_type get_start() const  { return (address - size); }

    // Hash function
    //
    size_type operator() (const BestFitMemoryBlock &mb) const  {

        return (std::hash<value_type>{ }(mb.address));
    }

    inline friend bool
    operator < (const BestFitMemoryBlock &lhs, const BestFitMemoryBlock &rhs)  {

        return (lhs.size < rhs.size);
    }
    inline friend bool
    operator > (const BestFitMemoryBlock &lhs, const BestFitMemoryBlock &rhs)  {

        return (lhs.size > rhs.size);
    }
    inline friend bool
    operator == (const BestFitMemoryBlock &lhs, const BestFitMemoryBlock &rhs)  {

        return (lhs.address == rhs.address);
    }
};

// ----------------------------------------------------------------------------

template<typename S>
struct  BestFitAlgo : public S  {

    using Base = S;
    using size_type = Base::size_type;
    using pointer = unsigned char *;

    BestFitAlgo() : Base()  {

        free_blocks_start_.insert({ Base::buffer_, Base::max_size });
        free_blocks_end_.insert(std::make_pair(Base::buffer_ + Base::max_size,
                                               Base::max_size));
    }
    ~BestFitAlgo() = default;

    // Like malloc
    //
    pointer get_space (size_type requested_size)  {

        for (auto iter = free_blocks_start_.begin();
             iter != free_blocks_start_.end();
             ++iter)  {
            if (iter->size >= requested_size)  {
                auto    found_end = iter->get_end();

                if (iter->size > requested_size)  {
                    auto    remaining = iter->size - requested_size;
                    auto    new_address = iter->address + requested_size;

                    free_blocks_start_.insert({ new_address, remaining });
                    free_blocks_end_[found_end] = remaining;
                }
                else  // Exact size match
                    free_blocks_end_.erase(found_end);

                auto    ret = iter->address;

                free_blocks_start_.erase(iter);
                used_blocks_.insert({ ret, requested_size });
                return (ret);
            }
        }
        throw std::bad_alloc();
    }

    // Like free
    //
    void put_space (pointer to_be_freed, size_type)  {

        auto    used_iter = used_blocks_.find({ to_be_freed, 0 });

        if (used_iter != used_blocks_.end())  {
            const pointer   tail_ptr = to_be_freed + used_iter->size;
            bool            found_tail = false;

            // Try to find a free block that starts where to_be_freed block ends.
            // If there is such a free block,  join it with to_be_freed block
            //
            for (auto tail_block = free_blocks_start_.begin();
                 tail_block != free_blocks_start_.end();
                 ++tail_block)  {
                if (tail_block->address == tail_ptr)  {
                    const size_type     new_len =
                        used_iter->size + tail_block->size;
                    const BestFitMemoryBlock to_insert { to_be_freed, new_len };

                    free_blocks_start_.erase(tail_block);
                    free_blocks_start_.insert(to_insert);
                    free_blocks_end_[to_insert.get_end()] = new_len;
                    found_tail = true;
                    break;
                }
            }

            // Try to find a free block that ends where to_be_freed block starts.
            // If there is such a free block,  join it with to_be_freed block
            //
            const auto  end_iter = free_blocks_end_.find(to_be_freed);
            bool        found_head = false;

            if (end_iter != free_blocks_end_.end())  {
                const pointer   head_address =
                    end_iter->first - end_iter->second;

                for (auto head_block = free_blocks_start_.begin();
                     head_block != free_blocks_start_.end();
                     ++head_block)  {
                    if (head_block->address == head_address)  {
                        const size_type new_len =
                            used_iter->size + head_block->size;
                        const auto      new_head = head_block->address;
                        const auto      new_end =
                            end_iter->first + used_iter->size;

                        free_blocks_start_.erase(head_block);
                        free_blocks_start_.insert({ new_head, new_len });
                        free_blocks_end_.erase(end_iter);
                        free_blocks_end_[new_end] = new_len;
                        found_head = true;
                        break;
                    }
                }
            }

            // If we could not join with any other adjacent free blocks,
            // process it as stand alone
            //
            if (! (found_tail || found_head))  {
                const pointer   end_address =
                    used_iter->address + used_iter->size;

                free_blocks_start_.insert(
                    { used_iter->address, used_iter->size });
                free_blocks_end_[end_address] = used_iter->size;
            }

            // Finally remove the block from used blocks map
            //
            used_blocks_.erase(used_iter);
        }
        else  // This is undefined behavior in delete operator
            throw std::invalid_argument("BestFitAlgo::put_space()");
    }

private:

    using blk_set = std::multiset<BestFitMemoryBlock>;
    using blk_uoset = std::unordered_set<BestFitMemoryBlock, BestFitMemoryBlock>;
    using blk_uomap = std::unordered_map<pointer, std::size_t>;

    blk_set     free_blocks_start_ { };  // Pointres to free block beginnings.
    blk_uomap   free_blocks_end_ { };    // Pointres to free block ends.
    blk_uoset   used_blocks_ { };        // Used blocks
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE>
struct  FirstFitStaticBase : public StaticStorage<T, MAX_SIZE>  {

    using Base = StaticStorage<T, MAX_SIZE>;
    using value_type = Base::value_type;
    using size_type = Base::size_type;

    inline static constexpr unsigned char   FREE_ { 0 };
    inline static constexpr unsigned char   USED_ { 1 };
    inline static constexpr std::size_t     max_size = MAX_SIZE;

    // The bitmap to indicate which slots are in use.
    //
    alignas(64)
    inline static unsigned char in_use_[MAX_SIZE];

    // Pointer to the first free slot.
    //
    alignas(value_type *)
    inline static unsigned char *first_free_ptr_ { in_use_ };

    FirstFitStaticBase() : Base()  {

        // This is guaranteed to execute only once even in multithreading
        //
        [[maybe_unused]] static auto    slug = std::invoke([]() -> int  {
            std::memset(in_use_, FREE_, MAX_SIZE);
            return (0);
        });
    }
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE>
struct  FirstFitStackBase : public StackStorage<T, MAX_SIZE>  {

    using Base = StackStorage<T, MAX_SIZE>;
    using value_type = Base::value_type;
    using size_type = Base::size_type;

    inline static constexpr unsigned char   FREE_ { 0 };
    inline static constexpr unsigned char   USED_ { 1 };
    inline static constexpr std::size_t     max_size = MAX_SIZE;

    // The bitmap to indicate which slots are in use.
    //
    alignas(64)
    unsigned char in_use_[MAX_SIZE];

    // Pointer to the first free slot.
    //
    alignas(value_type *)
    unsigned char *first_free_ptr_ { in_use_ };

    FirstFitStackBase() : Base()  { std::memset(in_use_, FREE_, MAX_SIZE); }
};

// ----------------------------------------------------------------------------

template<typename S>
struct  FirstFitAlgo : public S  {

    using Base = S;
    using value_type = Base::value_type;
    using size_type = Base::size_type;
    using pointer = unsigned char *;

    FirstFitAlgo() : Base()  {  }
    ~FirstFitAlgo() = default;

    // Like malloc
    //
    pointer get_space (size_type requested_size)  {

        // Pointers to the "in use" bitmap.
        //
        unsigned char           *first_ptr = Base::first_free_ptr_;
        unsigned char *const    end_ptr = &(Base::in_use_[Base::max_size]);
        const size_type         n_items = requested_size / sizeof(value_type);

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

        return (&(Base::buffer_[offset]));
    }

    // Like free
    //
    void put_space (pointer to_be_freed, size_type space_size)  {

        // Find the start of the range.
        //
        const size_type index =  // Find the start of the range.
            std::distance(Base::buffer_, to_be_freed) / sizeof(value_type);
        unsigned char   *first_ptr = &(Base::in_use_[index]);

        // Mark the range as free.
        //
        std::memset(first_ptr, Base::FREE_, space_size / sizeof(value_type));

        // Update the "first free" pointer if necessary.
        //
        if (first_ptr < Base::first_free_ptr_)
            Base::first_free_ptr_ = first_ptr;
    }
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE,
         template<typename, std::size_t> typename STORAGE,
         template<typename> typename ALGO>
class   FixedSizeAllocator : ALGO<STORAGE<T, MAX_SIZE>>  {

    using AlgoBase = ALGO<STORAGE<T, MAX_SIZE>>;

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
    using size_type = AlgoBase::size_type;
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
    struct  rebind  {
        using other = FixedSizeAllocator<U, MAX_SIZE, STORAGE, ALGO>;
    };

    [[nodiscard]] pointer
    address(reference r) const  { return (std::addressof(r)); }
    [[nodiscard]] const_pointer
    address(const_reference cr) const  { return (std::addressof(cr)); }

    [[nodiscard]] constexpr size_type
    max_size() const  { return (MAX_SIZE); }

public:

    FixedSizeAllocator() : AlgoBase()  {  }
    FixedSizeAllocator(const FixedSizeAllocator &that) = delete;
    FixedSizeAllocator(FixedSizeAllocator &&) = delete;
    ~FixedSizeAllocator() = default;

    FixedSizeAllocator &
    operator =(FixedSizeAllocator &&) = delete;
    FixedSizeAllocator &
    operator =(const FixedSizeAllocator &) = delete;

    template<typename U>
    FixedSizeAllocator(const FixedSizeAllocator<U, MAX_SIZE, STORAGE, ALGO> &) {
    }

    // Always return true for stateless allocators.
    //
    [[nodiscard]] inline bool
    operator == (const FixedSizeAllocator &) const  { return (true); }
    [[nodiscard]] inline bool
    operator != (const FixedSizeAllocator &) const  { return (false); }

public:

    inline void
    construct(pointer p, const_reference val) const  {

        new (static_cast<void *>(p)) value_type(val);
    }
    inline void
    construct(pointer p) const  { new (static_cast<void *>(p)) value_type(); }

    template<typename U, typename ... As>
    inline void
    construct(U *p, As && ... args) const  {

        new (static_cast<void *>(p)) U(std::forward<As>(args) ...);
    }

    inline void
    destroy(pointer p) const  { p->~value_type(); }

    template<typename U>
    inline void
    destroy(U *p) const  { p->~U(); }

    [[nodiscard]] inline pointer
    allocate(size_type n_items, [[maybe_unused]] const_pointer cp)  {

        auto    memory_ptr = this->get_space(n_items * sizeof(value_type));

        return (reinterpret_cast<pointer>(memory_ptr));
    }

    [[nodiscard]] inline pointer
    allocate(size_type n_items)  { return (allocate(n_items, nullptr)); }

    void
    deallocate(pointer p, size_type n_items)  {

        this->put_space(reinterpret_cast<AlgoBase::pointer>(p),
                        n_items * sizeof(value_type));
    }
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE>
using StaticBestFitAllocator =
    FixedSizeAllocator<T, MAX_SIZE, StaticStorage, BestFitAlgo>;

template<typename T, std::size_t MAX_SIZE>
using StackBestFitAllocator =
    FixedSizeAllocator<T, MAX_SIZE, StackStorage, BestFitAlgo>;

template<typename T, std::size_t MAX_SIZE>
using StaticFirstFitAllocator =
    FixedSizeAllocator<T, MAX_SIZE, FirstFitStaticBase, FirstFitAlgo>;

template<typename T, std::size_t MAX_SIZE>
using StackFirstFitAllocator =
    FixedSizeAllocator<T, MAX_SIZE, FirstFitStackBase, FirstFitAlgo>;

} // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
