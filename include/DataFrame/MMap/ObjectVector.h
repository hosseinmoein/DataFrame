// Hossein Moein
// June 3, 2019
// Copyright (C) 2019-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#ifndef _WIN32

#include <DataFrame/MMap/MMapFile.h>
#include <DataFrame/MMap/MMapSharedMem.h>

#include <iterator>
#include <time.h>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

enum class ACCESS_MODE : unsigned char  {
    normal = 0, // No special treatment, the default

    // Pages in the given range can be aggressively read ahead, and may be
    // freed soon after they are accessed
    //
    sequential = 2,

    // Read ahead may be less useful than normally
    //
    random = 4,

    // It might be a good idea to read some pages ahead
    //
    need_now = 8,

    // Do not expect access in the near future. (For the  time  being, the
    // application is finished with the given range, so the kernel can free
    // resources associated with it.)  Subsequent accesses  of pages  in this
    // range will succeed, but will result either in re-loading of the memory
    // contents from the underlying  mapped  file (see  mmap) or
    // zero-fill-on-demand pages for mappings without an underlying file.
    //
    dont_need = 16
};

// ----------------------------------------------------------------------------

// T elements are memcpy()'ed in and out of the
// Object vector. Therefore a T cannot have any virtual
// method or any dynamically allocated member or anything that will break as
// a result of memcpy().
//
template<typename T, typename B>
class   ObjectVector : protected B  {

public:

    using BaseClass = B;
    using size_type = typename BaseClass::size_type;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type *;
    using const_pointer = const value_type *;
    using const_pointer_const = const value_type *const;
    using reference = value_type &;
    using const_reference = const value_type &;

    ObjectVector () = delete;
    ObjectVector (const ObjectVector &that);
    ObjectVector &operator = (const ObjectVector &rhs);
    explicit ObjectVector (const char *name,
                           ACCESS_MODE access_mode = ACCESS_MODE::random,
                           size_type buffer_size = 1024 * sizeof(value_type));
    ObjectVector (const char *name,
                  size_type n,
                  const T &value = T(),
                  ACCESS_MODE access_mode = ACCESS_MODE::random,
                  size_type buffer_size = 1024 * sizeof(value_type));
    template<typename ITER>
    ObjectVector (const char *name,
                  ITER first,
                  ITER last,
                  ACCESS_MODE access_mode = ACCESS_MODE::random,
                  size_type buffer_size = 1024 * sizeof(value_type));

    virtual ~ObjectVector ();

    ObjectVector (const char *name,
                  const std::vector<T> &vec,
                  ACCESS_MODE access_mode = ACCESS_MODE::random,
                  size_type buffer_size = 1024 * sizeof(value_type));
    ObjectVector &operator = (const std::vector<T> &rhs);

    inline reference operator [] (size_type);
    inline const_reference operator [] (size_type) const;
    inline reference at (size_type i)  { return ((*this)[i]); }
    inline const_reference at (size_type i) const  { return ((*this)[i]); }

    inline reference front () noexcept  { return ((*this) [0]); }
    inline const_reference front () const noexcept  { return ((*this) [0]); }

    inline reference back () noexcept  { return ((*this) [size () - 1]); }
    inline const_reference
    back () const noexcept  { return ((*this) [size () - 1]); }

    inline size_type size () const noexcept { return (cached_object_count_); }
    inline bool empty () const noexcept  { return (size () == 0); }

    inline void reserve (size_type s);
    inline void shrink_to_fit ();

    inline void push_back (const value_type &d)  { write_ (&d, 1); }
    inline void clear ()  { erase (begin (), end ()); }
    inline void pop_back ()  { erase(end() - 1); }

    friend bool
    operator == (const ObjectVector &lhs, const ObjectVector &rhs)  {

        const size_type lhs_size = lhs.size();

        if (lhs_size != rhs.size())
            return (false);
        for (size_type i = 0; i < lhs_size; ++i)
            if (lhs[i] != rhs[i])  return (false);
        return (true);
    }
    friend bool
    operator != (const ObjectVector &lhs, const ObjectVector &rhs)  {

        return (! (lhs == rhs));
    }

    friend bool
    operator == (const std::vector<T> &lhs, const ObjectVector &rhs)  {

        const size_type lhs_size = lhs.size();

        if (lhs_size != rhs.size())
            return (false);
        for (size_type i = 0; i < lhs_size; ++i)
            if (lhs[i] != rhs[i])  return (false);
        return (true);
    }
    friend bool
    operator != (const std::vector<T> &lhs, const ObjectVector &rhs)  {

        return (! (lhs == rhs));
    }
    friend bool
    operator == (const ObjectVector &lhs, const std::vector<T> &rhs)  {

        return (rhs == lhs);
    }
    friend bool
    operator != (const ObjectVector &lhs, const std::vector<T> &rhs)  {

        return (rhs != lhs);
    }

    inline void set_access_mode (ACCESS_MODE am) const;
    inline void refresh () const noexcept;
    inline time_t creation_time () const noexcept;

protected:

    class   MetaData  {

    public:

        inline MetaData() = default;
        inline MetaData (size_type oc, size_type ct) noexcept
            : object_count (oc), creation_time (ct)  {   }

        size_type   object_count { 0 };
        time_t      creation_time { 0 };
    };

private:

    mutable size_type   cached_object_count_ { 0 };

    inline void setup_ ();
    inline size_type tell_ () const noexcept;
    inline void unlink_ ()  { BaseClass::unlink (); }
    inline int write_ (const value_type *data_ele, size_type count);
    inline bool seek_ (size_type obj_num) const noexcept;

public:

    //
    // The iterators:
    // These iterators contain only one pointer. Like STL iterators,
    // they are cheap to create and copy around.
    //

    class   iterator
        : public std::iterator<std::random_access_iterator_tag,
                               value_type, size_type>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

        using pointer = value_type *;
        using reference = value_type &;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline iterator () = default;
        inline iterator (value_type *node) noexcept : node_ (node)  {   }

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
        inline operator pointer () noexcept  { return (node_); }

       // We are following STL style iterator interface.
       //
        inline iterator &operator ++ () noexcept  {    // ++Prefix

            node_ += 1;
            return (*this);
        }
        inline iterator operator ++ (int) noexcept  {  // Postfix++

            value_type   *the_node = node_;

            node_ += 1;
            return (iterator (the_node));
        }

        inline iterator &operator += (size_type step) noexcept  {

            node_ += step;
            return (*this);
        }

        inline iterator &operator -- () noexcept  {    // --Prefix

            node_ -= 1;
            return (*this);
        }
        inline iterator operator -- (int) noexcept  {  // Postfix--

            value_type   *the_node = node_;

            node_ -= 1;
            return (iterator (the_node));
        }

        inline iterator &operator -= (size_type step) noexcept  {

            node_ -= step;
            return (*this);
        }

        inline iterator operator + (size_type step) noexcept  {

            return (iterator (node_ + step));
        }

        inline iterator operator - (size_type step) noexcept  {

            return (iterator (node_ - step));
        }

        inline iterator operator + (int step) noexcept  {

            return (iterator (node_ + step));
        }

        inline iterator operator - (int step) noexcept  {

            return (iterator (node_ - step));
        }

    private:

        pointer node_ { nullptr };

        friend class    ObjectVector::const_iterator;
        friend class    ObjectVector::reverse_iterator;
    };

    class   reverse_iterator
        : public std::iterator<std::random_access_iterator_tag,
                               value_type, size_type>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

        using pointer = value_type *;
        using reference = value_type &;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline reverse_iterator() = default;
        inline reverse_iterator(value_type *node) noexcept : node_ (node) {   }

        inline
        reverse_iterator(const iterator &itr) noexcept : node_(nullptr)  {

            *this = itr;
        }

        inline reverse_iterator &
        operator = (const iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }

        inline bool
        operator == (const reverse_iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool
        operator != (const reverse_iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline pointer operator -> () const noexcept  { return (node_); }
        inline reference operator * () const noexcept  { return (*node_); }
        inline operator pointer () const noexcept  { return (node_); }

       // ++Prefix
       //
        inline reverse_iterator &operator ++ () noexcept  {

            node_ -= 1;
            return (*this);
        }

       // Postfix++
       //
        inline reverse_iterator operator ++ (int) noexcept  {

            value_type const *ret_node = node_;

            node_ -= 1;
            return (reverse_iterator (ret_node));
        }

        inline reverse_iterator &operator += (size_type step) noexcept {

            node_ -= step;
            return (*this);
        }

       // --Prefix
       //
        inline reverse_iterator &operator -- () noexcept  {

            node_ += 1;
            return (*this);
        }

       // Postfix--
       //
        inline reverse_iterator operator -- (int) noexcept  {

            value_type const *ret_node = node_;

            node_ += 1;
            return (reverse_iterator (ret_node));
        }

        inline reverse_iterator &operator -= (size_type step) noexcept {

            node_ += step;
            return (*this);
        }

        inline reverse_iterator operator + (size_type step) noexcept  {

            return (reverse_iterator (node_ - step));
        }

        inline reverse_iterator operator - (size_type step) noexcept  {

            return (reverse_iterator (node_ + step));
        }

        inline reverse_iterator operator + (int step) noexcept  {

            return (reverse_iterator (node_ - step));
        }

        inline reverse_iterator operator - (int step) noexcept  {

            return (reverse_iterator (node_ + step));
        }

    private:

        pointer node_ { nullptr };

        friend class    ObjectVector::const_reverse_iterator;
        friend class    ObjectVector::const_iterator;
    };

    class   const_iterator
        : public std::iterator<std::random_access_iterator_tag,
                               value_type const, size_type>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

        using pointer = const value_type *;
        using reference = const value_type &;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline const_iterator () = default;

        inline const_iterator (const value_type *node) noexcept
            : node_ (node)  {   }

        inline const_iterator(const iterator &itr) noexcept
            : node_ (nullptr)  { *this = itr; }

        inline const_iterator(const reverse_iterator &itr) noexcept
            : node_(nullptr)  { *this = itr; }

        inline const_iterator &
        operator = (const iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }
        inline const_iterator &
        operator = (const reverse_iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }

        inline bool
        operator == (const const_iterator &rhs) const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool
        operator != (const const_iterator &rhs) const noexcept  {

            return (node_ != rhs.node_);
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline pointer operator -> () const noexcept  { return (node_); }
        inline reference operator * () const noexcept  { return (*node_); }
        inline operator pointer () const noexcept  { return (node_); }

       // ++Prefix
       //
        inline const_iterator &operator ++ () noexcept  {

            node_ += 1;
            return (*this);
        }

       // Postfix++
       //
        inline const_iterator operator ++ (int) noexcept  {

            value_type const *ret_node = node_;

            node_ += 1;
            return (const_iterator (ret_node));
        }

        inline const_iterator &operator += (size_type step) noexcept  {

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

            value_type const *ret_node = node_;

            node_ -= 1;
            return (const_iterator (ret_node));
        }

        inline const_iterator &operator -= (size_type step) noexcept  {

            node_ -= step;
            return (*this);
        }

        inline const_iterator operator + (size_type step) noexcept  {

            return (const_iterator (node_ + step));
        }

        inline const_iterator operator - (size_type step) noexcept  {

            return (const_iterator (node_ - step));
        }

        inline const_iterator operator + (int step) noexcept  {

            return (const_iterator (node_ + step));
        }

        inline const_iterator operator - (int step) noexcept  {

            return (const_iterator (node_ - step));
        }

    private:

        pointer node_ { nullptr };

        friend class    ObjectVector::const_reverse_iterator;
    };

    class   const_reverse_iterator
        : public std::iterator<std::random_access_iterator_tag,
                               value_type const, size_type>  {

    public:

        using iterator_category = std::random_access_iterator_tag;

        using pointer = const value_type *;
        using reference = const value_type &;

    public:

       // NOTE: The constructor with no argument initializes
       //       the iterator to be the "end" iterator
       //
        inline const_reverse_iterator () = default;

        inline const_reverse_iterator (const value_type *node) noexcept
            : node_ (node)  {   }

        inline const_reverse_iterator (const const_iterator &itr)
            noexcept : node_ (nullptr)  { *this = itr; }

        inline const_reverse_iterator (const reverse_iterator &itr)
            noexcept : node_ (nullptr)  { *this = itr; }

        inline const_reverse_iterator &
        operator = (const const_iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }
        inline const_reverse_iterator &
        operator = (const reverse_iterator &rhs) noexcept  {

            node_ = rhs.node_;
            return (*this);
        }

        inline bool operator == (const const_reverse_iterator &rhs)
            const noexcept  {

            return (node_ == rhs.node_);
        }
        inline bool operator != (const const_reverse_iterator &rhs)
            const noexcept  {

            return (node_ != rhs.node_);
        }

       // Following STL style, this iterator appears as a pointer
       // to value_type.
       //
        inline pointer operator -> () const noexcept  { return (node_); }
        inline reference operator * () const noexcept  { return (*node_); }
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

            value_type const *ret_node = node_;

            node_ -= 1;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator &
        operator += (size_type step) noexcept  {

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

            value_type const *ret_node = node_;

            node_ += 1;
            return (const_reverse_iterator (ret_node));
        }

        inline const_reverse_iterator &
        operator -= (size_type step) noexcept  {

            node_ += step;
            return (*this);
        }

        inline const_reverse_iterator
        operator + (size_type step) noexcept  {

            return (const_reverse_iterator (node_ - step));
        }

        inline const_reverse_iterator
        operator - (size_type step) noexcept  {

            return (const_reverse_iterator (node_ + step));
        }

        inline const_reverse_iterator operator + (int step) noexcept  {

            return (const_reverse_iterator (node_ - step));
        }

        inline const_reverse_iterator operator - (int step) noexcept  {

            return (const_reverse_iterator (node_ + step));
        }

    private:

        pointer node_ { nullptr };
    };

public:

    inline iterator begin() noexcept  { return (iterator(&((*this)[0]))); }
    inline iterator end() noexcept  { return (iterator(&((*this)[size()]))); }

    inline const_iterator
    begin () const noexcept  { return (const_iterator (&((*this)[0]))); }
    inline const_iterator
    end () const noexcept  { return (const_iterator (&((*this)[size()]))); }

    inline reverse_iterator
    rbegin() noexcept  { return (reverse_iterator (&((*this)[size() - 1]))); }
    inline reverse_iterator
    rend() noexcept  { return (reverse_iterator (&((*this)[0]) - 1)); }

    inline const_reverse_iterator
    rbegin () const noexcept  {

        return (const_reverse_iterator (&((*this)[size() - 1])));
    }
    inline const_reverse_iterator
    rend () const noexcept  {

        return (const_reverse_iterator (&((*this)[0]) - 1));
    }

   // Erases the range [first, last)
   //
    iterator erase (iterator first, iterator last);
    inline iterator erase (iterator pos) { return (erase (pos, pos + 1)); }

   // Inserts the range [first, last) before pos
   //
   // NOTE: first and last are assumed to be iterators to
   //       _continues_ memory
   //
    template<typename I>
    void insert (iterator pos, I first, I last);
    inline void insert (iterator pos, const_reference value)  {

        return (insert (pos, &value, &value + 1));
    }
};

// ----------------------------------------------------------------------------

template<typename T>
using MMapVector = ObjectVector<T, MMapFile>;

template<typename T>
using SharedMemVector = ObjectVector<T, MMapSharedMem>;

} // namespace hmdf

#endif // _WIN32

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/MMap/ObjectVector.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
