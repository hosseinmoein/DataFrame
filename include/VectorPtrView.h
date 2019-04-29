// Hossein Moein
// April 28, 2019
// Copyright (C) 2019-2021 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <cstdlib>
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
    using reference = T &;
    using const_reference = const T &;
    using size_type = typename vector_type::size_type;
    using difference_type = typename vector_type::difference_type;
    using underlying_vector_type = std::vector<T>;

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

    iterator erase (iterator pos)  { return (vector_.erase (pos)); }
    iterator erase (iterator fisrt, iterator last)  {

        return (vector_.erase (first, last));
	}
    inline void clear () throw ()  { vector_.clear(); }

    inline void resize (size_type n) throw ()  { vector_.resize(n); }
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
