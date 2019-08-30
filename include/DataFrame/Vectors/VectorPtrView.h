// Hossein Moein
// April 28, 2019
// Copyright (C) 2019-2021 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
class   VectorPtrView  {

private:

    using vector_type = std::vector<T *>;

    vector_type vector_ {  };

public:

    using value_type = T;
    using pointer = T *;
    using const_pointer = const T *;
    using const_pointer_const = const T *const;
    using reference = T &;
    using const_reference = const T &;
    using size_type = typename vector_type::size_type;
    using difference_type = typename vector_type::difference_type;

    static const size_type  value_size = sizeof(T);

public:

    inline size_type size() const noexcept { return (vector_.size ()); }
    inline size_type max_size() const noexcept { return (vector_.max_size()); }
    inline size_type capacity() const noexcept { return (vector_.capacity()); }

    inline bool empty() const noexcept { return (vector_.empty()); }
    inline void shrink_to_fit()  { vector_.shrink_to_fit(); }

    inline reference at(size_type n)  { return (*(vector_[n])); }
    inline const_reference at(size_type n) const { return (*(vector_[n])); }
    inline reference operator[](size_type n)  { return (at(n)); }
    inline const_reference operator[](size_type n) const { return (at(n)); }

    inline void reserve (size_type n) noexcept { vector_.reserve (n); }

    inline reference front () noexcept  { return (*(vector_.front ())); }
    inline const_reference
    front() const noexcept { return(*(vector_.front())); }
    inline reference back () noexcept { return (*(vector_.back ())); }
    inline const_reference
    back () const noexcept { return (*(vector_.back ())); }

    inline void push_back(value_type *v)  { vector_.push_back(v); }
    inline void emplace_back(value_type *v)  { vector_.emplace_back(v); }
    inline void pop_back()  { vector_.pop_back(); }

    inline void
    swap (VectorPtrView &rhs) noexcept  { vector_.swap (rhs.vector_); }

    inline friend bool
    operator == (const VectorPtrView &lhs, const VectorPtrView &rhs) {

        return (lhs.vector_ == rhs.vector_);
    }
    inline friend bool
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

    inline VectorPtrView &operator = (const VectorPtrView &) = default;
    inline VectorPtrView &operator = (VectorPtrView &&) = default;
    inline VectorPtrView &operator = (const std::vector<T> &rhs)  {

        VectorPtrView   tmp_vec;
        const size_type vec_size = rhs.size();

        reserve(vec_size);
        for (size_type idx = 0; idx < vec_size; ++idx)
            tmp_vec.push_back(&(rhs[idx]));
        swap(tmp_vec);
        return (*this);
    }
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

    inline void
    erase (size_type pos)  { vector_.erase (vector_.begin() + pos); }
    inline void erase (size_type first, size_type last)  {

        vector_.erase (vector_.begin() + first, vector_.begin() + last);
    }

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

   // This iterator contains only one pointer. Like STL iterators,
   // it is cheap to create and copy around.
   //
    class   const_iterator;
    class   iterator
        : public std::iterator<std::random_access_iterator_tag,
                               value_type *, long>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline iterator () = default;
        inline iterator (const iterator &) = default;
        inline iterator (iterator &&) = default;
        inline iterator &operator = (const iterator &) = default;
        inline iterator &operator = (iterator &&) = default;

        inline iterator (value_type **node) noexcept : node_ (node)  {  }

        inline iterator &operator = (value_type **rhs) noexcept  {

            node_ = rhs;
            return (*this);
        }

        inline bool operator == (const iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool operator != (const iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline pointer operator -> () const noexcept  { return (*node_); }
        inline reference operator * () const noexcept  { return (**node_); }
        inline operator pointer () const noexcept  { return (*node_); }

       // We are following STL style iterator interface.
       //
        inline iterator &operator ++ () noexcept  {    // ++Prefix

            node_ += 1;
            return (*this);
        }
        inline iterator operator ++ (int) noexcept  {  // Postfix++

            value_type   **ret_node = node_;

            node_ += 1;
            return (iterator (ret_node));
        }

        inline iterator &operator += (long step) noexcept  {

            node_ += step;
            return (*this);
        }

        inline iterator &operator -- () noexcept  {    // --Prefix

            node_ -= 1;
            return (*this);
        }
        inline iterator operator -- (int) noexcept  {  // Postfix--

            value_type   **ret_node = node_;

            node_ -= 1;
            return (iterator (ret_node));
        }

        inline iterator &operator -= (int step) noexcept  {

            node_ -= step;
            return (*this);
        }

        inline iterator operator + (int step) noexcept  {

            value_type   **ret_node = node_;

            ret_node += step;
            return (iterator (ret_node));
        }

        inline iterator operator - (int step) noexcept  {

            value_type   **ret_node = node_;

            ret_node -= step;
            return (iterator (ret_node));
        }

        inline iterator operator + (long step) noexcept  {

            value_type   **ret_node = node_;

            ret_node += step;
            return (iterator (ret_node));
        }

        inline iterator operator - (long step) noexcept  {

            value_type   **ret_node = node_;

            ret_node -= step;
            return (iterator (ret_node));
        }

    private:

        pointer *node_ { nullptr };

        friend class    const_iterator;
    };

    class   const_iterator
        : public std::iterator<std::random_access_iterator_tag,
                               const value_type *, long>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline const_iterator () = default;
        inline const_iterator (const const_iterator &) = default;
        inline const_iterator (const_iterator &&) = default;
        inline const_iterator &operator = (const const_iterator &) = default;
        inline const_iterator &operator = (const_iterator &&) = default;

        inline const_iterator (value_type *const *node) noexcept
            : node_ (node)  {   }
        inline const_iterator (const iterator &itr) noexcept  { *this = itr; }

        inline const_iterator &operator = (value_type *const *rhs) noexcept  {

            node_ = rhs;
            return (*this);
        }
        inline const_iterator &operator = (const iterator &rhs) noexcept {

            node_ = rhs.node_;
            return (*this);
        }

        template<typename ITR>
        inline bool operator == (const ITR &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        template<typename ITR>
        inline bool operator != (const ITR &rhs) const noexcept  {

            return (node_ != rhs.node_);
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

            value_type *const  *ret_node = node_;

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

            value_type *const  *ret_node = node_;

            node_ -= 1;
            return (const_iterator (ret_node));
        }

        inline const_iterator &operator -= (int step) noexcept  {

            node_ -= step;
            return (*this);
        }

        inline const_iterator operator + (int step) noexcept  {

            value_type *const  *ret_node = node_;

            ret_node += step;
            return (const_iterator (ret_node));
        }

        inline const_iterator operator - (int step) noexcept  {

            value_type *const  *ret_node = node_;

            ret_node -= step;
            return (const_iterator (ret_node));
        }

        inline const_iterator operator + (long step) noexcept  {

            value_type *const  *ret_node = node_;

            ret_node += step;
            return (const_iterator (ret_node));
        }

        inline const_iterator operator - (long step) noexcept  {

            value_type *const  *ret_node = node_;

            ret_node -= step;
            return (const_iterator (ret_node));
        }

    private:

        T *const    *node_ { nullptr };
    };

    class   const_reverse_iterator
        : public std::iterator<std::random_access_iterator_tag,
                               const value_type *, long>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline const_reverse_iterator () = default;
        inline const_reverse_iterator(const const_reverse_iterator &) = default;
        inline const_reverse_iterator(const_reverse_iterator &&) = default;
        inline const_reverse_iterator &
        operator = (const const_reverse_iterator &) = default;
        inline const_reverse_iterator &
        operator = (const_reverse_iterator &&) = default;

        inline const_reverse_iterator (value_type *const *node) noexcept
            : node_ (node)  {   }
        inline const_reverse_iterator (const const_iterator &itr) noexcept
            { *this = itr; }

        inline const_reverse_iterator (const iterator &itr) noexcept
            { *this = itr; }

        inline const_reverse_iterator &
        operator = (value_type *const *rhs) noexcept  {

            node_ = rhs;
            return (*this);
        }
        inline const_reverse_iterator &
        operator = (const const_iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }
        inline const_reverse_iterator &
        operator = (const iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }

        template<typename ITR>
        inline bool operator == (const ITR &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        template<typename ITR>
        inline bool operator != (const ITR &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline const_pointer operator -> () const noexcept  {

            return (*node_);
        }
        inline const_reference operator *() const noexcept  {

            return (**node_);
        }
        inline operator pointer () const noexcept  { return (*node_); }

       // ++Prefix
       //
        inline const_reverse_iterator &operator ++ () noexcept  {

            node_ -= 1;
            return (*this);
        }

       // Postfix++
       //
        inline const_reverse_iterator operator ++ (int) noexcept  {

            value_type *const  *ret_node = node_;

            node_ -= 1;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator &operator += (int step) noexcept  {

            node_ -= step;
            return (*this);
        }

       // --Prefix
       //
        inline const_reverse_iterator &operator -- () noexcept  {

            node_ += 1;
            return (*this);
        }

       // Postfix--
       //
        inline const_reverse_iterator operator -- (int) noexcept  {

            value_type *const  *ret_node = node_;

            node_ += 1;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator &operator -= (int step) noexcept  {

            node_ += step;
            return (*this);
        }

        inline const_reverse_iterator operator + (int step) noexcept  {

            value_type *const  *ret_node = node_;

            ret_node -= step;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator operator - (int step) noexcept  {

            value_type *const  *ret_node = node_;

            ret_node += step;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator operator + (long step) noexcept  {

            value_type *const  *ret_node = node_;

            ret_node -= step;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator operator - (long step) noexcept  {

            value_type   *const  *ret_node = node_;

            ret_node += step;
            return (const_reverse_iterator (ret_node));
        }

    private:

        T *const    *node_ { nullptr };

        friend class    const_iterator;
    };

public:

    inline iterator
    begin () noexcept  { return (iterator (&(*(vector_.begin())))); }
    inline iterator
    end () noexcept { return (iterator(&(*(vector_.end())))); }
    inline const_iterator
    begin() const noexcept { return (const_iterator (&(*(vector_.begin())))); }
    inline const_iterator
    end () const noexcept  { return (const_iterator(&(*(vector_.end())))); }

    inline const_reverse_iterator
    rbegin () const noexcept  { 
        return (const_reverse_iterator (&(*(vector_.rbegin()))));
    }
    inline const_reverse_iterator
    rend () const noexcept  {

        return (const_reverse_iterator (&(*(vector_.rend()))));
    }
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
