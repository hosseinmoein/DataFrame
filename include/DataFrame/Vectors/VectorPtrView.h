// Hossein Moein
// April 28, 2019
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

#include <DataFrame/Utils/AlignedAllocator.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct  VPVSentinel  {

    VPVSentinel() = default;
    VPVSentinel(const VPVSentinel &) = default;
    VPVSentinel(VPVSentinel &&) = default;
    VPVSentinel &operator = (const VPVSentinel &) = default;
    VPVSentinel &operator = (VPVSentinel &&) = default;
    ~VPVSentinel() = default;
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t A = 0>
class VectorPtrView {

    using vector_type =
        std::vector<T *, typename allocator_declare<T *, A>::type>;

public:

    static constexpr std::align_val_t   align_value { A };

    using value_type = T;
    using pointer = T *;
    using const_pointer = const T *;
    using const_pointer_const = const T *const;
    using reference = T &;
    using const_reference = const T &;
    using size_type = typename vector_type::size_type;
    using difference_type = typename vector_type::difference_type;

    static const size_type  value_size = sizeof(T);

    [[nodiscard]] inline size_type
    size() const noexcept { return (vector_.size ()); }
    [[nodiscard]] inline size_type
    max_size() const noexcept { return (vector_.max_size()); }
    [[nodiscard]] inline size_type
    capacity() const noexcept { return (vector_.capacity()); }

    [[nodiscard]] inline bool
    empty() const noexcept { return (vector_.empty()); }
    inline void shrink_to_fit()  { vector_.shrink_to_fit(); }

    [[nodiscard]] inline reference at(size_type n)  { return (*(vector_[n])); }
    [[nodiscard]] inline pointer &ptr_at(size_type n)  { return (vector_[n]); }
    [[nodiscard]] inline const_reference
    at(size_type n) const { return (*(vector_[n])); }
    [[nodiscard]] inline reference operator[](size_type n)  { return (at(n)); }
    [[nodiscard]] inline const_reference
    operator[](size_type n) const { return (at(n)); }

    inline void reserve (size_type n) noexcept { vector_.reserve (n); }

    [[nodiscard]] inline reference
    front () noexcept  { return (*(vector_.front ())); }
    [[nodiscard]] inline const_reference
    front() const noexcept { return (*(vector_.front())); }
    [[nodiscard]] inline reference
    back () noexcept { return (*(vector_.back ())); }
    [[nodiscard]] inline const_reference
    back () const noexcept { return (*(vector_.back ())); }

    inline void push_back(value_type *v)  { vector_.push_back(v); }
    inline void emplace_back(value_type *v)  { vector_.emplace_back(v); }
    inline void pop_back()  { vector_.pop_back(); }

    inline void
    swap (VectorPtrView &rhs) noexcept  { vector_.swap (rhs.vector_); }

    [[nodiscard]] inline friend bool
    operator == (const VectorPtrView &lhs, const VectorPtrView &rhs) {

        return (lhs.vector_ == rhs.vector_);
    }
    [[nodiscard]] inline friend bool
    operator != (const VectorPtrView &lhs, const VectorPtrView &rhs) {

        return (lhs.vector_ != rhs.vector_);
    }
    [[nodiscard]] inline friend bool
    operator < (const VectorPtrView &lhs, const VectorPtrView &rhs) {

        return (lhs.vector_ < rhs.vector_);
    }

    inline VectorPtrView () = default;
    inline VectorPtrView (const VectorPtrView &) = default;
    inline VectorPtrView (VectorPtrView &&) = default;
    inline VectorPtrView(const std::vector<T> &rhs)  { *this = rhs; }
    inline VectorPtrView(std::vector<T> &rhs)  { *this = rhs; }
    template<typename ITR>
    VectorPtrView(ITR first, ITR last)  {

        reserve(std::distance(first, last));
        for (auto iter = first; iter < last; ++iter)
            push_back(&(*iter));
    }
    ~VectorPtrView () = default;

    // The purpose of this method is for the user be able to conform to STL
    // standards.
    // To create a VectorView over an entire std::vector you have to do this:
    //        VectorView(&(*v.begin()), &(*v.end()));
    // The above second parameter is against standards and it is caught
    // if you set the STL boundary check flag. So instead, you can do:
    //        VectorView vv;
    //        vv.set_begin_end_special(&(*v.begin()), &(*v.back()));
    //
    template<typename ITR>
    inline void set_begin_end_special(ITR first, ITR last_1)  {

        ITR last = last_1;

        last += 1;
        reserve(std::distance(first, last));
        for (auto iter = first; iter < last; ++iter)
            push_back(&(*iter));
    }

    inline VectorPtrView &operator = (const VectorPtrView &) = default;
    inline VectorPtrView &operator = (VectorPtrView &&) = default;
    VectorPtrView &operator = (const std::vector<T> &rhs) = delete;
    inline VectorPtrView &operator = (std::vector<T> &rhs)  {

        VectorPtrView   tmp_vec;
        const size_type vec_size = rhs.size();

        reserve(vec_size);
        for (size_type idx = 0; idx < vec_size; ++idx)
            tmp_vec.push_back(&(rhs[idx]));
        swap(tmp_vec);
        return (*this);
    }

    inline void clear () throw ()  { vector_.clear(); }
    inline void resize (size_type n) throw ()  { vector_.resize(n); }

    inline void insert (size_type pos, value_type *v)  {

        vector_.insert (vector_.begin() + pos, v);
    }
    template<typename ITR>
    inline void insert(size_type pos, ITR first, ITR last) {

        std::vector<T *>    tmp_vec;

        tmp_vec.reserve(std::distance(first, last));
        for (auto iter = first; iter < last; ++iter)
            tmp_vec.push_back(&(*iter));
        vector_.reserve(vector_.size() + tmp_vec.size());
        vector_.insert(vector_.begin() + pos, tmp_vec.begin(), tmp_vec.end());
    }

    inline void
    sort(std::function<bool(const value_type *, const value_type *)> comp =
             [](const value_type *l, const value_type *r) -> bool {
                 return *l < *r;
             })  {

        std::sort(vector_.begin(), vector_.end(), comp);
    }

public:

    class   const_iterator;
    class   iterator : public std::random_access_iterator_tag  {

    private:

        using iter_type = typename vector_type::iterator;

    public:

        using iterator_category = std::random_access_iterator_tag;
        using value_type = T;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using reference = value_type &;
        using const_reference = const value_type &;
        using difference_type = typename vector_type::difference_type;

    public:

        inline iterator (iter_type node) noexcept : node_ (node)  {  }
        inline iterator (pointer *node) noexcept : node_ (node)  {  }

        iterator () = default;
        ~iterator () = default;
        iterator (const iterator &) = default;
        iterator (iterator &&) = default;
        iterator &operator = (const iterator &) = default;
        iterator &operator = (iterator &&) = default;

        inline iterator &operator = (iter_type rhs) noexcept  {

            node_ = rhs;
            return (*this);
        }
        inline iterator &operator = (pointer *rhs) noexcept  {

            node_ = iter_type(rhs);
            return (*this);
        }

        inline bool operator == (const iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool operator != (const iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }
        inline bool operator > (const iterator &rhs) const noexcept  {

            return (node_ > rhs.node_);
        }
        inline bool operator >= (const iterator &rhs) const noexcept  {

            return (node_ >= rhs.node_);
        }
        inline bool operator < (const iterator &rhs) const noexcept  {

            return (node_ < rhs.node_);
        }
        inline bool operator <= (const iterator &rhs) const noexcept  {

            return (node_ <= rhs.node_);
        }

        friend bool
        operator == (const iterator &rhs, VPVSentinel) noexcept  {

            return (rhs.node_ == rhs.vector_.end());
        }
        friend bool
        operator != (const iterator &rhs, VPVSentinel) noexcept  {

            return (rhs.node_ != rhs.vector_.end());
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline pointer operator -> () noexcept  { return (*node_); }
        inline reference operator * () noexcept  { return (**node_); }
        inline const_pointer
        operator -> () const noexcept  { return (*node_); }
        inline const_reference
        operator * () const noexcept  { return (**node_); }
        inline operator pointer() noexcept  { return (*node_); }
        inline operator const_pointer() const noexcept  { return (*node_); }

       // We are following STL style iterator interface.
       //
        inline iterator &operator ++ () noexcept  {    // ++Prefix

            node_ += 1;
            return (*this);
        }
        inline iterator operator ++ (int) noexcept  {  // Postfix++

            iter_type   ret_node = node_;

            node_ += 1;
            return (iterator (ret_node));
        }

        inline iterator &operator += (difference_type step) noexcept  {

            node_ += step;
            return (*this);
        }

        inline iterator &operator -- () noexcept  {    // --Prefix

            node_ -= 1;
            return (*this);
        }
        inline iterator operator -- (int) noexcept  {  // Postfix--

            iter_type   ret_node = node_;

            node_ -= 1;
            return (iterator (ret_node));
        }

        inline iterator &operator -= (int step) noexcept  {

            node_ -= step;
            return (*this);
        }

        template<typename I>
        inline iterator operator + (I step) const noexcept  {

            auto    ret_node = node_;

            ret_node += static_cast<difference_type>(step);
            return (iterator { ret_node });
        }
        template<>
        inline iterator operator + (const iterator &rhs) const noexcept {

            return (iterator (node_ + rhs.node_));
        }

        friend difference_type
        operator - (iterator lhs, iterator rhs) noexcept  {

            return (lhs.node_ - rhs.node_);
        }

        template<std::semiregular I>
        inline iterator operator - (I step) const noexcept  {

            auto    ret_node = node_;

            ret_node -= static_cast<difference_type>(step);
            return (iterator { ret_node });
        }

        inline const_reference &
        operator [](difference_type step) const noexcept  {

            return (**(node_ + step));
        }
        inline reference &
        operator [](difference_type step) noexcept  {

            return (**(node_ + step));
        }

    private:

        iter_type   node_ {  };

        friend class    const_iterator;
    };

    class   const_iterator : public std::random_access_iterator_tag  {

    private:

        using iter_type = typename vector_type::const_iterator;

    public:

        using iterator_category = std::random_access_iterator_tag;
        using value_type = T;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using reference = value_type &;
        using const_reference = const value_type &;
        using difference_type = typename vector_type::difference_type;

    public:

        inline const_iterator (iter_type node) noexcept : node_ (node)  {   }
        inline const_iterator (pointer const *node) noexcept
            : node_ (node)  {   }
        inline const_iterator (const iterator &itr) noexcept  { *this = itr; }

        const_iterator () = default;
        ~const_iterator () = default;
        const_iterator (const const_iterator &) = default;
        const_iterator (const_iterator &&) = default;
        const_iterator &operator = (const const_iterator &) = default;
        const_iterator &operator = (const_iterator &&) = default;

        inline const_iterator &operator = (iter_type rhs) noexcept  {

            node_ = rhs;
            return (*this);
        }
        inline const_iterator &operator = (pointer const *rhs) noexcept  {

            node_ = iter_type(rhs);
            return (*this);
        }
        inline const_iterator &operator = (const iterator &rhs) noexcept {

            node_ = rhs.node_;
            return (*this);
        }

        inline bool operator == (const const_iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool operator != (const const_iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }
        inline bool operator > (const const_iterator &rhs) const noexcept  {

            return (node_ > rhs.node_);
        }
        inline bool operator >= (const const_iterator &rhs) const noexcept  {

            return (node_ >= rhs.node_);
        }
        inline bool operator < (const const_iterator &rhs) const noexcept  {

            return (node_ < rhs.node_);
        }
        inline bool operator <= (const const_iterator &rhs) const noexcept  {

            return (node_ <= rhs.node_);
        }

        friend bool
        operator == (const const_iterator &rhs, VPVSentinel) noexcept  {

            return (rhs.node_ == rhs.vector_.end());
        }
        friend bool
        operator != (const const_iterator &rhs, VPVSentinel) noexcept  {

            return (rhs.node_ != rhs.vector_.end());
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline const_pointer operator -> () const noexcept { return(*node_); }
        inline const_reference operator * () const noexcept  {

            return (**node_);
        }
        inline operator const_pointer () const noexcept  { return (*node_); }

       // ++Prefix
       //
        inline const_iterator &operator ++ () noexcept  {

            node_ += 1;
            return (*this);
        }

       // Postfix++
       //
        inline const_iterator operator ++ (int) noexcept  {

            iter_type   ret_node = node_;

            node_ += 1;
            return (const_iterator (ret_node));
        }

        inline const_iterator &operator += (int step) noexcept  {

            node_ += step;
            return (*this);
        }

       // --Prefix
       //
        inline const_iterator &operator -- () noexcept  {

            node_ -= 1;
            return (*this);
        }

       // Postfix--
       //
        inline const_iterator operator -- (int) noexcept  {

            iter_type   ret_node = node_;

            node_ -= 1;
            return (const_iterator (ret_node));
        }

        inline const_iterator &operator -= (int step) noexcept  {

            node_ -= step;
            return (*this);
        }

        template<typename I>
        inline const_iterator operator + (I step) const noexcept  {

            auto const  ret_node = node_;

            ret_node += static_cast<difference_type>(step);
            return (const_iterator { ret_node });
        }
        template<>
        inline const_iterator
        operator + (const const_iterator &rhs) const noexcept {

            return (const_iterator (node_ + rhs.node_));
        }

        friend difference_type
        operator - (const_iterator lhs, const_iterator rhs) noexcept  {

            return (lhs.node_ - rhs.node_);
        }

        template<std::semiregular I>
        inline const_iterator operator - (I step) const noexcept  {

            auto const  ret_node = node_;

            ret_node -= static_cast<difference_type>(step);
            return (const_iterator { ret_node });
        }

        inline const_reference &
        operator [](difference_type step) const noexcept  {

            return (**(node_ + step));
        }

    private:

        iter_type   node_ {  };
    };

    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    friend class    iterator;
    friend class    const_iterator;

    [[nodiscard]] inline iterator
    begin () noexcept  { return (iterator(vector_.begin())); }
    [[nodiscard]] inline iterator
    end () noexcept { return (iterator(vector_.end())); }
    [[nodiscard]] inline const_iterator
    begin() const noexcept { return (const_iterator(vector_.begin())); }
    [[nodiscard]] inline const_iterator
    end () const noexcept  { return (const_iterator(vector_.end())); }

    [[nodiscard]] inline std::reverse_iterator<iterator>
    rbegin() noexcept { return (std::make_reverse_iterator(end())); }
    [[nodiscard]] inline std::reverse_iterator<iterator>
    rend() noexcept { return (std::make_reverse_iterator(begin())); }
    [[nodiscard]] inline std::reverse_iterator<const_iterator>
    rbegin() const noexcept { return (std::make_reverse_iterator(end())); }
    [[nodiscard]] inline std::reverse_iterator<const_iterator>
    rend() const noexcept { return (std::make_reverse_iterator(begin())); }

    [[nodiscard]] const_iterator
    cbegin() const { return (const_iterator (vector_.cbegin())); }
    [[nodiscard]] const_iterator
    cend() const { return (const_iterator (vector_.cend())); }
    [[nodiscard]] const_reverse_iterator
    crbegin() const { return (std::make_reverse_iterator(end())); }
    [[nodiscard]] const_reverse_iterator
    crend() const { return (std::make_reverse_iterator(begin())); }

    inline void
    erase (size_type pos)  { vector_.erase (vector_.begin() + pos); }
    inline void erase (size_type first, size_type last)  {

        vector_.erase (vector_.begin() + first, vector_.begin() + last);
    }

private:

    vector_type vector_ {  };
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t A = 0>
class VectorConstPtrView  {

    using vector_type =
        std::vector<const T *, typename allocator_declare<const T *, A>::type>;

public:

    static constexpr std::align_val_t   align_value { A };

    using value_type = T;
    using pointer = const T *;
    using const_pointer = const T *;
    using const_pointer_const = const T *const;
    using reference = const T &;
    using const_reference = const T &;
    using size_type = typename vector_type::size_type;
    using difference_type = typename vector_type::difference_type;

    static const size_type  value_size = sizeof(T);

    [[nodiscard]] inline size_type
    size() const noexcept { return (vector_.size ()); }
    [[nodiscard]] inline size_type
    max_size() const noexcept { return (vector_.max_size()); }
    [[nodiscard]] inline size_type
        capacity() const noexcept { return (vector_.capacity()); }

    [[nodiscard]] inline bool
    empty() const noexcept { return (vector_.empty()); }
    inline void shrink_to_fit()  { vector_.shrink_to_fit(); }

    [[nodiscard]] inline const_pointer
    &ptr_at(size_type n) const  { return (vector_[n]); }
    [[nodiscard]] inline const_reference
    at(size_type n) const { return (*(vector_[n])); }
    [[nodiscard]] inline const_reference
    operator[](size_type n) const { return (at(n)); }

    inline void reserve (size_type n) noexcept { vector_.reserve (n); }

    [[nodiscard]] inline const_reference
    front() const noexcept { return(*(vector_.front())); }
    [[nodiscard]] inline const_reference
    back () const noexcept { return (*(vector_.back ())); }

    inline void push_back(const value_type *v)  { vector_.push_back(v); }
    inline void emplace_back(const value_type *v)  { vector_.emplace_back(v); }
    inline void pop_back()  { vector_.pop_back(); }

    inline void
    swap (VectorConstPtrView &rhs) noexcept  { vector_.swap (rhs.vector_); }

    [[nodiscard]] inline friend bool
    operator == (const VectorConstPtrView &lhs,
                 const VectorConstPtrView &rhs) {

        return (lhs.vector_ == rhs.vector_);
    }
    [[nodiscard]] inline friend bool
    operator < (const VectorConstPtrView &lhs, const VectorConstPtrView &rhs) {

        return (lhs.vector_ < rhs.vector_);
    }

    inline VectorConstPtrView () = default;
    inline VectorConstPtrView (const VectorConstPtrView &) = default;
    inline VectorConstPtrView (VectorConstPtrView &&) = default;
    inline VectorConstPtrView(const std::vector<T> &rhs)  { *this = rhs; }
    inline VectorConstPtrView(std::vector<T> &rhs)  { *this = rhs; }
    template<typename ITR>
    VectorConstPtrView(ITR first, ITR last)  {

        reserve(std::distance(first, last));
        for (auto iter = first; iter < last; ++iter)
            push_back(&(*iter));
    }
    ~VectorConstPtrView () = default;

    // The purpose of this method is for the user be able to conform to STL
    // standards.
    // To create a VectorView over an entire std::vector you have to do this:
    //        VectorView(&(*v.begin()), &(*v.end()));
    // The above second parameter is against standards and it is caught
    // if you set the STL boundary check flag. So instead, you can do:
    //        VectorView vv;
    //        vv.set_begin_end_special(&(*v.begin()), &(*v.back()));
    //
    template<typename ITR>
    inline void set_begin_end_special(ITR first, ITR last_1)  {

        ITR last = last_1;

        last += 1;
        reserve(std::distance(first, last));
        for (auto iter = first; iter < last; ++iter)
            push_back(&(*iter));
    }

    inline VectorConstPtrView &
    operator = (const VectorConstPtrView &) = default;
    inline VectorConstPtrView &operator = (VectorConstPtrView &&) = default;
    inline VectorConstPtrView &operator = (const std::vector<T> &rhs)  {

        VectorConstPtrView   tmp_vec;
        const size_type      vec_size = rhs.size();

        reserve(vec_size);
        for (size_type idx = 0; idx < vec_size; ++idx)
            tmp_vec.push_back(&(rhs[idx]));
        swap(tmp_vec);
        return (*this);
    }
    inline VectorConstPtrView &operator = (std::vector<T> &rhs)  {

        VectorConstPtrView   tmp_vec;
        const size_type      vec_size = rhs.size();

        reserve(vec_size);
        for (size_type idx = 0; idx < vec_size; ++idx)
            tmp_vec.push_back(&(rhs[idx]));
        swap(tmp_vec);
        return (*this);
    }

    inline void clear () throw ()  { vector_.clear(); }
    inline void resize (size_type n) throw ()  { vector_.resize(n); }

    inline void insert (size_type pos, value_type *v)  {

        vector_.insert (vector_.begin() + pos, v);
    }
    template<typename ITR>
    inline void insert(size_type pos, ITR first, ITR last) {

        std::vector<T *>    tmp_vec;

        tmp_vec.reserve(std::distance(first, last));
        for (auto iter = first; iter < last; ++iter)
            tmp_vec.push_back(&(*iter));
        vector_.reserve(vector_.size() + tmp_vec.size());
        vector_.insert(vector_.begin() + pos, tmp_vec.begin(), tmp_vec.end());
    }

    inline void
    sort(std::function<bool(const value_type *, const value_type *)> comp =
             [](const value_type *l, const value_type *r) -> bool {
                 return *l < *r;
             })  {

        std::sort(vector_.begin(), vector_.end(), comp);
    }

public:

    class   const_iterator : public std::random_access_iterator_tag  {

    private:

        using iter_type = typename vector_type::const_iterator;

    public:

        using iterator_category = std::random_access_iterator_tag;
        using value_type = T;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using reference = value_type &;
        using const_reference = const value_type &;
        using difference_type = typename vector_type::difference_type;

    public:

        inline const_iterator (iter_type node) noexcept : node_ (node)  {   }
        inline const_iterator (pointer const *node) noexcept
            : node_ (node)  {   }

        const_iterator () = default;
        ~const_iterator () = default;
        const_iterator (const const_iterator &) = default;
        const_iterator (const_iterator &&) = default;
        const_iterator &operator = (const const_iterator &) = default;
        const_iterator &operator = (const_iterator &&) = default;

        inline const_iterator &operator = (iter_type rhs) noexcept  {

            node_ = rhs;
            return (*this);
        }
        inline const_iterator &operator = (pointer const *rhs) noexcept  {

            node_ = iter_type(rhs);
            return (*this);
        }

        inline bool operator == (const const_iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool operator != (const const_iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }
        inline bool operator > (const const_iterator &rhs) const noexcept  {

            return (node_ > rhs.node_);
        }
        inline bool operator >= (const const_iterator &rhs) const noexcept  {

            return (node_ >= rhs.node_);
        }
        inline bool operator < (const const_iterator &rhs) const noexcept  {

            return (node_ < rhs.node_);
        }
        inline bool operator <= (const const_iterator &rhs) const noexcept  {

            return (node_ <= rhs.node_);
        }

        friend bool
        operator == (const const_iterator &rhs, VPVSentinel) noexcept  {

            return (rhs.node_ == rhs.vector_.end());
        }
        friend bool
        operator != (const const_iterator &rhs, VPVSentinel) noexcept  {

            return (rhs.node_ != rhs.vector_.end());
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline const_pointer operator -> () const noexcept { return(*node_); }
        inline const_reference operator * () const noexcept  {

            return (**node_);
        }
        inline operator const_pointer () const noexcept  { return (*node_); }

       // ++Prefix
       //
        inline const_iterator &operator ++ () noexcept  {

            node_ += 1;
            return (*this);
        }

       // Postfix++
       //
        inline const_iterator operator ++ (int) noexcept  {

            iter_type   ret_node = node_;

            node_ += 1;
            return (const_iterator (ret_node));
        }

        inline const_iterator &operator += (int step) noexcept  {

            node_ += step;
            return (*this);
        }

       // --Prefix
       //
        inline const_iterator &operator -- () noexcept  {

            node_ -= 1;
            return (*this);
        }

       // Postfix--
       //
        inline const_iterator operator -- (int) noexcept  {

            iter_type   ret_node = node_;

            node_ -= 1;
            return (const_iterator (ret_node));
        }

        inline const_iterator &operator -= (int step) noexcept  {

            node_ -= step;
            return (*this);
        }

        inline const_iterator operator + (int step) const noexcept  {

            value_type const  **ret_node = node_;

            ret_node += step;
            return (const_iterator (ret_node));
        }

        inline const_iterator operator - (int step) const noexcept  {

            value_type const  **ret_node = node_;

            ret_node -= step;
            return (const_iterator (ret_node));
        }

        inline const_iterator
        operator + (difference_type step) const noexcept  {

            value_type const  **ret_node = node_;

            ret_node += step;
            return (const_iterator (ret_node));
        }

        inline const_iterator
        operator - (difference_type step) const noexcept  {

            value_type const  **ret_node = node_;

            ret_node -= step;
            return (const_iterator (ret_node));
        }
        inline const_reference &
        operator [](difference_type step) const noexcept  {

            return (**(node_ + step));
        }

    private:

        iter_type   node_ {  };
    };

    using reverse_iterator = std::reverse_iterator<const_iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    friend class    const_iterator;

    [[nodiscard]] inline const_iterator
    begin() const noexcept { return (const_iterator(vector_.begin())); }
    [[nodiscard]] inline const_iterator
    end () const noexcept  { return (const_iterator(vector_.end())); }

    [[nodiscard]] inline std::reverse_iterator<const_iterator>
    rbegin() const noexcept { return (std::make_reverse_iterator(end())); }
    [[nodiscard]] inline std::reverse_iterator<const_iterator>
    rend() const noexcept { return (std::make_reverse_iterator(begin())); }

    [[nodiscard]] const_iterator
    cbegin() const { return (const_iterator (vector_.cbegin())); }
    [[nodiscard]] const_iterator
    cend() const { return (const_iterator (vector_.cend())); }
    [[nodiscard]] const_reverse_iterator
    crbegin() const { return (std::make_reverse_iterator(end())); }
    [[nodiscard]] const_reverse_iterator
    crend() const { return (std::make_reverse_iterator(begin())); }

    inline void
    erase (size_type pos)  { vector_.erase (vector_.begin() + pos); }
    inline void erase (size_type first, size_type last)  {

        vector_.erase (vector_.begin() + first, vector_.begin() + last);
    }

private:

    vector_type vector_ {  };
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
