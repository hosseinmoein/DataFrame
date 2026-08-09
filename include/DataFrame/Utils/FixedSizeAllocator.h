// Hossein Moein
// September 6 2024
/*
Copyright (c) 2023-2028, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the C++ DataFrame nor the
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
#include <memory_resource>
#include <new>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, std::size_t MAX_SIZE>
struct  StackStorage  {

    using mem_type = unsigned char;
    using value_type = T;
    using size_type = std::size_t;

    inline static constexpr size_type   memory_size {
        MAX_SIZE * sizeof(value_type)
    };

    // Main allocation space
    //
    alignas(value_type[])
    mem_type    buffer_[memory_size];

    StackStorage() = default;
    StackStorage(const StackStorage &that)  {

        std::memcpy(buffer_, that.buffer_, memory_size);
    }
    StackStorage(StackStorage &&that)  {

        std::memcpy(buffer_, that.buffer_, memory_size);
    }
    ~StackStorage() = default;
};

// ----------------------------------------------------------------------------

struct  BestFitBlock  {

    using mem_type = unsigned char;
    using value_type = mem_type *;
    using size_type = std::size_t;

    value_type  address { nullptr };
    size_type   size { 0 };

    inline value_type
    get_end() const  { return (address + size); }
    inline value_type
    get_start() const  { return (address); }

    // Hash function
    //
    inline size_type
    operator()(const BestFitBlock &mb) const  {

        return (std::hash<value_type>{ }(mb.address));
    }

    inline friend bool
    operator <(const BestFitBlock &lhs, const BestFitBlock &rhs)  {

        return (lhs.size < rhs.size);
    }
    inline friend bool
    operator >(const BestFitBlock &lhs, const BestFitBlock &rhs)  {

        return (lhs.size > rhs.size);
    }
    inline friend bool
    operator ==(const BestFitBlock &lhs, const BestFitBlock &rhs)  {

        return (lhs.address == rhs.address);
    }
};

// ----------------------------------------------------------------------------

template<typename S>  // Storage class
struct  BestFitAlgo : public S  {

    using Base = S;
    using size_type = Base::size_type;
    using pointer = Base::mem_type *;

    BestFitAlgo() : Base()  {

        free_blocks_start_.insert({ Base::buffer_, Base::memory_size });
        free_blocks_assist_.insert(
            std::make_pair(Base::buffer_, free_blocks_start_.begin()));
        free_blocks_end_.insert(
            std::make_pair(Base::buffer_ + Base::memory_size,
                           Base::memory_size));
    }

    // A copy/move can't sensibly carry over this object's outstanding
    // allocations. The new object's buffer_ is a distinct block of memory
    // at a distinct address (see StackStorage's own copy/move ctors), so any
    // pointers recorded in the bookkeeping below would still reference the
    // *source* object's memory rather than the copy's own. Any pointer this
    // allocator has already handed out to a caller remains valid only as
    // long as the original object stays alive at its original address.
    // That's a usage constraint (don't relocate a container built on this
    // allocator once it holds live allocations), not something a copy/move
    // ctor can paper over. So both constructors just delegate to the default
    // one: the new instance starts as its own fresh, empty arena, which is
    // the only state that's ever actually valid for it.
    //
    BestFitAlgo(const BestFitAlgo &) : BestFitAlgo()  {  }
    BestFitAlgo(BestFitAlgo &&) : BestFitAlgo()  {  }

    ~BestFitAlgo() = default;

    // Like malloc
    //
    [[nodiscard]] pointer
    get_space(size_type requested_size)  {

        auto    free_iter {
            free_blocks_start_.lower_bound({ nullptr, requested_size })
        };

        if (free_iter != free_blocks_start_.end())  {
            auto    found_end { free_iter->get_end() };

            if (free_iter->size > requested_size)  {
                auto        remaining { free_iter->size - requested_size };
                auto        new_address { free_iter->address + requested_size };
                const auto  insert_ret {
                    free_blocks_start_.insert({ new_address, remaining })
                };

                free_blocks_assist_.insert(
                    std::make_pair(new_address, insert_ret));
                free_blocks_end_[found_end] = remaining;
            }
            else  {  // Exact size match
                free_blocks_end_.erase(found_end);
            }

            auto    ret { free_iter->address };

            free_blocks_assist_.erase(free_iter->address);
            free_blocks_start_.erase(free_iter);
            used_blocks_.insert({ ret, requested_size });
            return (ret);
        }
        throw std::bad_alloc();
    }

    // Like free
    //
    void
    put_space(pointer to_be_freed, size_type)  {

        auto    used_iter { used_blocks_.find({ to_be_freed, 0 }) };

        if (used_iter != used_blocks_.end())  {
            pointer merge_start { used_iter->address };
            pointer merge_end { used_iter->address + used_iter->size };

            // Left neighbor: a free block ending exactly where we start
            //
            const auto  end_iter { free_blocks_end_.find(merge_start) };

            if (end_iter != free_blocks_end_.end())  {
                const pointer   head_ptr { end_iter->first - end_iter->second };
                const auto      head_block {
                    free_blocks_assist_.find(head_ptr)
                };

                if (head_block != free_blocks_assist_.end())  {
                    merge_start = head_block->second->address;
                    free_blocks_start_.erase(head_block->second);
                    free_blocks_assist_.erase(head_block);
                    free_blocks_end_.erase(end_iter);
                }
            }

            // Right neighbor: a free block starting exactly where we end
            //
            const auto  tail_block { free_blocks_assist_.find(merge_end) };

            if (tail_block != free_blocks_assist_.end())  {
                merge_end = tail_block->second->get_end();
                free_blocks_end_.erase(tail_block->second->get_end());
                free_blocks_start_.erase(tail_block->second);
                free_blocks_assist_.erase(tail_block);
            }

            const size_type     merged_size {
                static_cast<size_type>(merge_end - merge_start)
            };
            const BestFitBlock  to_insert { merge_start, merged_size };
            const auto          insert_ret {
                free_blocks_start_.insert(to_insert)
            };

            free_blocks_assist_.insert(std::make_pair(merge_start, insert_ret));
            free_blocks_end_[merge_end] = merged_size;
            used_blocks_.erase(used_iter);
        }
    }

private:

    // It is based on size, so it must be multi-set
    //
    using blk_set = std::pmr::multiset<BestFitBlock>;
    using blk_uoset = std::pmr::unordered_set<BestFitBlock, BestFitBlock>;
    using blk_uomap = std::pmr::unordered_map<pointer, std::size_t>;
    using blk_assist =
        std::pmr::unordered_map<pointer, blk_set::const_iterator>;

    // The whole point of a stack allocator is to avoid touching the heap.
    // So, the bookkeeping used to manage that stack arena shouldn't quietly
    // heap-allocate a tree/hash-table node on every insert either. The four
    // containers below draw from a fixed, inline byte pool instead of the
    // default heap-backed allocator.
    //
    // Sizing: at most memory_size / sizeof(value_type) (== MAX_SIZE) blocks
    // can exist simultaneously (the smallest possible allocation unit), and
    // a block is a "free" or used record in only one place at a time, so
    // 2 * MAX_SIZE bounds the worst-case total live-node count across all
    // four containers. node_budget_ is a per-node byte estimate with real
    // headroom over what libstdc++'s tree/hash nodes actually cost.
    //
    inline static constexpr std::size_t node_budget_ { 128 };
    inline static constexpr std::size_t pool_bytes_ {
        (Base::memory_size / sizeof(typename Base::value_type) + 1) *
        2 * node_budget_
    };

    // Cap how large a single chunk request the pool resource is allowed to
    // make -- left at its default, unsynchronized_pool_resource asks its
    // upstream for chunks sized independently of how much we actually need,
    // which can exceed pool_bytes_ in one shot for small MAX_SIZE and defeat
    // the whole point. Capping max_blocks_per_chunk keeps each chunk request
    // modest and proportional to node_budget_ instead.
    //
    inline static std::pmr::pool_options    pool_opts_ {
        /* max_blocks_per_chunk = */ 64,
        /* largest_required_pool_block = */ node_budget_ };

    alignas(std::max_align_t)
    std::byte                               pool_buffer_[pool_bytes_];
    std::pmr::monotonic_buffer_resource     upstream_resource_ {
        pool_buffer_, pool_bytes_, std::pmr::new_delete_resource()
    };
    std::pmr::unsynchronized_pool_resource  pool_resource_ {
        pool_opts_, &upstream_resource_
    };

    // Set of free blocks, keyed by size of the block. There could be multiple
    // blocks with the same size.
    //
    blk_set     free_blocks_start_ { &pool_resource_ };

    // Map of free blocks to size, keyed by the pointer to the end of the block.
    //
    blk_uomap   free_blocks_end_ { &pool_resource_ };

    // Hash set of used blocks, keyed by the pointer to the beginning of
    // the block.
    //
    blk_uoset   used_blocks_ { &pool_resource_ };

    // Map of free blocks to iterators of free_blocks_start_, keyed pointers
    // to the beginning of free blocks
    //
    blk_assist  free_blocks_assist_ { &pool_resource_ };
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t MAX_SIZE>
struct  FirstFitStack : public StackStorage<T, MAX_SIZE>  {

    using Base = StackStorage<T, MAX_SIZE>;
    using value_type = Base::value_type;
    using size_type = Base::size_type;

    inline static constexpr unsigned char   FREE_ { 0 };
    inline static constexpr unsigned char   USED_ { 1 };
    inline static constexpr std::size_t     memory_size { MAX_SIZE };

    // The bitmap to indicate which slots are in use.
    //
    alignas(unsigned char[])
    unsigned char in_use_[memory_size];

    // Pointer to the first free slot.
    //
    alignas(unsigned char *)
    unsigned char *first_free_ptr_ { in_use_ };

    FirstFitStack() : Base()  { std::memset(in_use_, FREE_, MAX_SIZE); }
    FirstFitStack(const FirstFitStack &that) : Base(that)  {

        std::memset(in_use_, FREE_, MAX_SIZE);
    }
    FirstFitStack(FirstFitStack &&that) : Base(that)  {

        std::memset(in_use_, FREE_, MAX_SIZE);
    }
    ~FirstFitStack() = default;
};

// ----------------------------------------------------------------------------

template<typename S>  // Storage class
struct  FirstFitAlgo : public S  {

    using Base = S;
    using value_type = Base::value_type;
    using size_type = Base::size_type;
    using pointer = Base::mem_type *;

    FirstFitAlgo() = default;
    ~FirstFitAlgo() = default;

    // Like malloc
    //
    [[nodiscard]] pointer
    get_space(size_type requested_size)  {

        // Pointers to the "in use" bitmap.
        //
        unsigned char           *first_ptr { Base::first_free_ptr_ };
        unsigned char *const    end_ptr { &(Base::in_use_[Base::memory_size]) };
        const size_type         n_items { requested_size / sizeof(value_type) };

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
        const size_type offset {
            std::distance(Base::in_use_, first_ptr) * sizeof(value_type)
        };

        return (&(Base::buffer_[offset]));
    }

    // Like free
    //
    void
    put_space(pointer to_be_freed, size_type space_size)  {

        // Find the start of the range.
        //
        const size_type index {  // Find the start of the range.
            std::distance(Base::buffer_, to_be_freed) / sizeof(value_type)
        };
        unsigned char   *first_ptr { &(Base::in_use_[index]) };

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
class   FixedSizeAllocator : private ALGO<STORAGE<T, MAX_SIZE>>  {

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

    // This allocator is genuinely stateful. Each instance owns its own
    // space plus its own free/used-block bookkeeping, so memory allocated
    // through one instance is meaningless to a different instance's
    // deallocate(). Instances are not universally interchangeable.
    //
    using is_always_equal = std::false_type;

    // This is only necessary because allocator has a second, third and fourth
    // template arguments that will make the default std::allocator_traits
    // implementation fail during compilation.
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

    FixedSizeAllocator() = default;
    FixedSizeAllocator(const FixedSizeAllocator &) = default;
    FixedSizeAllocator(FixedSizeAllocator &&) = default;
    ~FixedSizeAllocator() = default;

    template<typename U>
    FixedSizeAllocator(const FixedSizeAllocator<U, MAX_SIZE, STORAGE, ALGO> &) {
    }

    // Two instances are only equal (i.e. interchangeable for
    // allocate/deallocate purposes) if they are literally the same object.
    // Each instance owns its own distinct arena, so no two distinct
    // instances ever manage the same memory.
    //
    [[nodiscard]] inline bool
    operator ==(const FixedSizeAllocator &that) const noexcept  {

        return (this == &that);
    }
    [[nodiscard]] inline bool
    operator !=(const FixedSizeAllocator &that) const noexcept  {

        return (this != &that);
    }

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

        if (n_items == 0)  return (nullptr);
        if (n_items > max_size())  throw std::bad_array_new_length();

        auto    memory_ptr { this->get_space(n_items * sizeof(value_type)) };

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


// This is slower than first-fit, but it causes a lot less fragmentations.
//
template<typename T, std::size_t MAX_SIZE>
using StackBestFitAllocator =
    FixedSizeAllocator<T, MAX_SIZE, StackStorage, BestFitAlgo>;

// This is faster than best-fit, but it causes more fragmentations.
//
template<typename T, std::size_t MAX_SIZE>
using StackFirstFitAllocator =
    FixedSizeAllocator<T, MAX_SIZE, FirstFitStack, FirstFitAlgo>;

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
