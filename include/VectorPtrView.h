// Hossein Moein
// April 28, 2019
// Copyright (C) 2019-2021 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <cstdlib>
#include <iterator>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
class   VectorPtrView  {

private:

    using vector_type = std::vector<T *>;

    vector_type vector_;

public:

    using value_type = T;
    using pointer = T *;
    using const_pointer = const T *;
    using reference = T &;
    using const_reference = const T &;
    using size_type = typename vector_type::size_type;
    using difference_type = typename vector_type::difference_type;

    inline size_type size() const { return (vector_.size ()); }
    inline size_type max_size() const { return (vector_.max_size()); }
    inline size_type capacity() const { return (vector_.capacity()); }

    inline bool empty() const { return (vector_.empty()); }

    inline reference operator[](size_type n)  { return (*(vector_[n])); }
    inline const_reference
    operator[](size_type n) const { return (*(vector_[n])); }

    inline void reserve (size_type n) { vector_.reserve (n); }

    inline reference front ()  { return (*(vector_.front ())); }
    inline const_reference front () const { return (*(vector_.front ())); }
    inline reference back () { return (*(vector_.back ())); }
    inline const_reference back () const { return (*(vector_.back ())); }
    inline void swap (VectorPtrView &rhs)  { vector_.swap (rhs); }

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
    ~VectorPtrView () = default;

    inline VectorPtrView &operator = (const VectorPtrView &) = default;
    inline VectorPtrView &operator = (VectorPtrView &&) = default;

    inline void clear () throw ()  { vector_.clear(); }
    inline void resize (size_type n) throw ()  { vector_.resize(n); }

public:

    class   const_iterator;
    class   const_reverse_iterator
        : public std::iterator<std::random_access_iterator_tag,
                               value_type const, long>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline const_reverse_iterator () = default;

        inline const_reverse_iterator (value_type const **node)
            noexcept : node_ (*node)  {   }

        inline const_reverse_iterator (const const_iterator &itr) noexcept
            : node_ (nullptr)  { *this = itr; }

        inline const_reverse_iterator &
        operator = (const const_iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }

        inline bool
        operator == (const const_reverse_iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool
        operator != (const const_reverse_iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline const_pointer operator -> () const noexcept  {

            return (node_);
        }
        inline const_reference operator *() const noexcept  {

            return (*node_);
        }
        inline operator pointer () const noexcept  { return (node_); }

       // ++Prefix
       //
        inline const_reverse_iterator &operator ++ () noexcept  {

            node_ -= 1;
            return (*this);
        }

       // Postfix++
       //
        inline const_reverse_iterator operator ++ (int) noexcept  {

            value_type   const  *ret_node = node_;

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

            value_type   const  *ret_node = node_;

            node_ += 1;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator &operator -= (int step) noexcept  {

            node_ += step;
            return (*this);
        }

        inline const_reverse_iterator operator + (int step) noexcept  {

            value_type   const  *ret_node = node_;

            ret_node -= step;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator operator - (int step) noexcept  {

            value_type   const  *ret_node = node_;

            ret_node += step;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator operator + (long step) noexcept  {

            value_type   const  *ret_node = node_;

            ret_node -= step;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator operator - (long step) noexcept  {

            value_type   const  *ret_node = node_;

            ret_node += step;
            return (const_reverse_iterator (ret_node));
        }

    private:

        const_pointer   node_ {nullptr};
    };

    class   iterator;
    class   const_iterator
        : public std::iterator<std::random_access_iterator_tag,
                               value_type const, long>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline const_iterator () = default;

        inline const_iterator (value_type const **node) noexcept
            : node_ (*node)  {   }

        inline const_iterator (const iterator &itr) noexcept
            : node_ (nullptr)  { *this = itr; }

        inline const_iterator &operator = (const iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }

        inline bool operator == (const const_iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool operator != (const const_iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline const_pointer operator -> () const noexcept  { return (node_); }
        inline const_reference operator * () const noexcept  {

            return (*node_);
        }
        inline operator const_pointer () const noexcept  { return (node_); }

       // ++Prefix
       //
        inline const_iterator &operator ++ () noexcept  {

            node_ += 1;
            return (*this);
        }

       // Postfix++
       //
        inline const_iterator operator ++ (int) noexcept  {

            value_type   const  *ret_node = node_;

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

            value_type  const  *ret_node = node_;

            node_ -= 1;
            return (const_iterator (ret_node));
        }

        inline const_iterator &operator -= (int step) noexcept  {

            node_ -= step;
            return (*this);
        }

        inline const_iterator operator + (int step) noexcept  {

            value_type  const  *ret_node = node_;

            ret_node += step;
            return (const_iterator (ret_node));
        }

        inline const_iterator operator - (int step) noexcept  {

            value_type  const  *ret_node = node_;

            ret_node -= step;
            return (const_iterator (ret_node));
        }

        inline const_iterator operator + (long step) noexcept  {

            value_type  const  *ret_node = node_;

            ret_node += step;
            return (const_iterator (ret_node));
        }

        inline const_iterator operator - (long step) noexcept  {

            value_type  const  *ret_node = node_;

            ret_node -= step;
            return (const_iterator (ret_node));
        }

    private:

        const_pointer   node_ {nullptr};

        friend class    VectorPtrView::const_reverse_iterator;
    };

   // This iterator contains only one pointer. Like STL iterators,
   // it is cheap to create and copy around.
   //
    class   iterator
        : public std::iterator<std::random_access_iterator_tag,
                               value_type, long>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline iterator () = default;

        inline iterator (value_type **node) noexcept : node_ (*node)  {  }

        inline bool operator == (const iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool operator != (const iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline pointer operator -> () const noexcept  { return (node_); }
        inline reference operator * () const noexcept  { return (*node_); }
        inline operator pointer () const noexcept  { return (node_); }

       // We are following STL style iterator interface.
       //
        inline iterator &operator ++ () noexcept  {    // ++Prefix

            node_ += 1;
            return (*this);
        }
        inline iterator operator ++ (int) noexcept  {  // Postfix++

            value_type   *ret_node = node_;

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

            value_type   *ret_node = node_;

            node_ -= 1;
            return (iterator (ret_node));
        }

        inline iterator &operator -= (int step) noexcept  {

            node_ -= step;
            return (*this);
        }

        inline iterator operator + (int step) noexcept  {

            value_type   *ret_node = node_;

            ret_node += step;
            return (iterator (ret_node));
        }

        inline iterator operator - (int step) noexcept  {

            value_type   *ret_node = node_;

            ret_node -= step;
            return (iterator (ret_node));
        }

        inline iterator operator + (long step) noexcept  {

            value_type   *ret_node = node_;

            ret_node += step;
            return (iterator (ret_node));
        }

        inline iterator operator - (long step) noexcept  {

            value_type   *ret_node = node_;

            ret_node -= step;
            return (iterator (ret_node));
        }

    private:

        pointer node_ {nullptr};

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
    inline iterator end () noexcept  { return(iterator(&(vector_.back()) + 1)); }
    inline const_iterator begin () const noexcept  {

        return (const_iterator (&(vector_.front())));
    }
    inline const_iterator end () const noexcept  {

        return (iterator (&(vector_.back()) + 1));
    }

    inline const_reverse_iterator rbegin () const noexcept  {

        return (const_iterator (&(vector_.back())));
    }
    inline const_reverse_iterator rend () const noexcept  {

        return (const_iterator (&(vector_.front()) - 1));
    }

    VectorPtrView &operator= (std::vector<T> &rhs)  {

        VectorPtrView   tmp_vec;
        const size_type vec_size = rhs.size();

        tmp_vec.reserve(vec_size);
        for (size_type idx = 0; idx < vec_size; ++idx)
            tmp_vec.psuh_back(&(rhs[idx]));
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
