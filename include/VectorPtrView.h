// Hossein Moein
// July 17 2009

#pragma once

#include <cstdlib>
#include <vector>

// ----------------------------------------------------------------------------

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

    // typedef typename vector_type::iterator          iterator;
    // typedef typename vector_type::const_iterator    const_iterator;
    // typedef typename vector_type::reverse_iterator  reverse_iterator;
    // typedef typename vector_type::const_reverse_iterator
                                                    const_reverse_iterator;

    inline iterator begin ()  { return (vector_.begin ()); }
    inline iterator end ()  { return (vector_.end ()); }
    inline const_iterator begin() const { return (vector_.begin ()); }
    inline const_iterator end () const { return (vector_.end ()); }
    inline reverse_iterator rbegin ()  { return (vector_.rbegin ()); }
    inline reverse_iterator rend () throw ()  { return (vector_.rend ()); }
    inline const_reverse_iterator rbegin() const { return (vector_.rbegin()); }
    inline const_reverse_iterator rend() const { return (vector_.rend ()); }

    inline size_type size() const { return (vector_.size ()); }
    inline size_type max_size() const { return (vector_.max_size()); }
    inline size_type capacity() const { return (vector_.capacity()); }

    inline bool empty() const { return (vector_.empty()); }

    inline reference operator[](size_type n)  { return (*vector_[n]); }
    inline const_reference
	operator[](size_type n) const { return (*vector_[n]); }

    inline void reserve (size_type n) { vector_.reserve (n); }

    inline reference front ()  { return (*vector_.front ()); }
    inline const_reference front () const { return (*vector_.front ()); }
    inline reference back () { return (*vector_.back ()); }
    inline const_reference back () const { return (*vector_.back ()); }

    inline friend bool operator == (const VectorPtrView &lhs,
                                    const VectorPtrView &rhs) {

        return (lhs.vector_ == rhs.vector_);
    }
    inline friend bool operator < (const VectorPtrView &lhs,
                                   const VectorPtrView &rhs) {

        return (lhs.vector_ < rhs.vector_);
    }

    inline VectorPtrView () = default;
    inline VectorPtrView (const VectorPtrView &) = default;
    ~VectorPtrView () = default;

    inline VectorPtrView &operator = (const VectorPtrView &) = default;

    iterator erase (iterator pos) throw ();
    iterator erase (iterator fisrt, iterator last) throw ();
    inline void clear () throw ()  { erase (begin (), end ()); }

    inline void resize (size_type n) throw ()  {

        const   size_type   current_size = vector_.size ();

        if (n < current_size)
            for (size_type idx = n; idx < current_size; ++idx)
                delete vector_ [idx];

        vector_.resize (n, NULL);
    }

   // The followings are some convenient methods that are not a part
   // of STL.
   //
    bool erase (const value_type &x) throw ();
    iterator find (const data_type &x) throw ();
    const_iterator find (const data_type &x) const throw ();
};

// ----------------------------------------------------------------------------

#ifdef DMS_INCLUDE_SOURCE
#  include <VectorPtrView.tcc>
#endif // DMS_INCLUDE_SOURCE

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
