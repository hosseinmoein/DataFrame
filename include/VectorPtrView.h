// Hossein Moein
// April 28, 2019
// Copyright (C) 2019-2021 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

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
    using reference = T &;
    using const_reference = const T &;
    using size_type = typename vector_type::size_type;
    using difference_type = typename vector_type::difference_type;

    inline size_type size() const noexcept { return (vector_.size ()); }
    inline size_type max_size() const noexcept { return (vector_.max_size()); }
    inline size_type capacity() const noexcept { return (vector_.capacity()); }

    inline bool empty() const noexcept { return (vector_.empty()); }

    inline reference operator[](size_type n)  { return (*(vector_[n])); }
    inline const_reference
    operator[](size_type n) const { return (*(vector_[n])); }

    inline void reserve (size_type n) noexcept { vector_.reserve (n); }

    inline reference front () noexcept  { return (*(vector_.front ())); }
    inline const_reference
    front() const noexcept { return(*(vector_.front())); }
    inline reference back () noexcept { return (*(vector_.back ())); }
    inline const_reference
    back () const noexcept { return (*(vector_.back ())); }
    inline void swap (VectorPtrView &rhs) noexcept  { vector_.swap (rhs); }

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
    VectorPtrView(std::vector<T> &rhs)  {

        const size_type vec_size = rhs.size();

        vector_.reserve(vec_size);
        for (size_type idx = 0; idx < vec_size; ++idx)
            vector_.push_back(&(rhs[idx]));
    }
    template<typename ITR>
    VectorPtrView(ITR first, ITR last)  {

        vector_.reserve(std::distance(first, last));
        for (auto iter = first; iter < last; ++iter)
            vector_.push_back(&(*iter));
    }
    ~VectorPtrView () = default;

    inline VectorPtrView &operator = (const VectorPtrView &) = default;
    inline VectorPtrView &operator = (VectorPtrView &&) = default;

    inline void clear () throw ()  { vector_.clear(); }
    inline void resize (size_type n) throw ()  { vector_.resize(n); }

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

        inline iterator (value_type **node) noexcept : node_ (node)  {  }

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

        friend class    VectorPtrView::const_iterator;
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

        inline const_iterator (const value_type **node) noexcept
            : node_ (node)  {   }

        inline const_iterator (const iterator &itr) noexcept { *this = itr; }

        template<typename ITR>
        inline const_iterator &operator = (const ITR &rhs) noexcept  {

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
                               value_type const *, long>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline const_reverse_iterator () = default;

        inline const_reverse_iterator (value_type const **node)
            noexcept : node_ (node)  {   }

        inline const_reverse_iterator (const const_iterator &itr) noexcept
            : node_ (nullptr)  { *this = itr; }

        inline const_reverse_iterator &
        operator = (const const_iterator &rhs) noexcept  {

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

        friend class    VectorPtrView::const_iterator;
    };

public:

    iterator erase (iterator pos)  { return (vector_.erase (pos)); }
    iterator erase (iterator first, iterator last)  {

        return (vector_.erase (first, last));
    }

    inline iterator begin () noexcept  {

        return (iterator (&(vector_.front())));
    }
    inline iterator end () noexcept { return(iterator(&(vector_.back()) + 1)); }
    inline const_iterator begin () const noexcept  {

        return (const_iterator (&(vector_.front())));
    }
    inline const_iterator end () const noexcept  {

        return (const_iterator (&(vector_.back()) + 1));
    }

    inline const_reverse_iterator rbegin () const noexcept  {

        return (const_reverse_iterator (&(vector_.back())));
    }
    inline const_reverse_iterator rend () const noexcept  {

        return (const_reverse_iterator (&(vector_.front()) - 1));
    }

    VectorPtrView &operator= (std::vector<T> &rhs)  {

        VectorPtrView   tmp_vec;
        const size_type vec_size = rhs.size();

        tmp_vec.reserve(vec_size);
        for (size_type idx = 0; idx < vec_size; ++idx)
            tmp_vec.push_back(&(rhs[idx]));
        swap(tmp_vec);
        return (*this);
    }
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
