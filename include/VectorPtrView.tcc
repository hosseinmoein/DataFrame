// Hossein Moein
// July 17 2009

#include <VectorPtrView.h>

// ----------------------------------------------------------------------------

template <class T, class cu_COPIER>
VectorPtrView<T, cu_COPIER>::~VectorPtrView () throw ()  {

    for (iterator itr = begin (); itr != end (); ++itr)
        delete *itr;
}

// ----------------------------------------------------------------------------

template <class T, class cu_COPIER>
typename VectorPtrView<T, cu_COPIER>::iterator
VectorPtrView<T, cu_COPIER>::
erase (iterator pos) throw ()  {

    delete *pos;
    return (vector_.erase (pos));
}

// ----------------------------------------------------------------------------

template <class T, class cu_COPIER>
typename VectorPtrView<T, cu_COPIER>::iterator
VectorPtrView<T, cu_COPIER>::
erase (iterator first, iterator last) throw ()  {

    for (iterator itr = first; itr < last; ++itr)
        delete *itr;

    return (vector_.erase (first, last));
}

// ----------------------------------------------------------------------------

template <class T, class cu_COPIER>
bool VectorPtrView<T, cu_COPIER>::
erase (const value_type &x) throw ()  {

    for (iterator itr = begin (); itr != end (); ++itr)
        if (*itr == x)  {
            erase (itr);
            return (true);
        }

    return (false);
}

// ----------------------------------------------------------------------------

template <class T, class cu_COPIER>
typename VectorPtrView<T, cu_COPIER>::iterator
VectorPtrView<T, cu_COPIER>::
find (const data_type &x) throw ()  {

    for (iterator itr = begin (); itr != end (); ++itr)
        if (**itr == x)
            return (itr);

    return (end ());
}

// ----------------------------------------------------------------------------

template <class T, class cu_COPIER>
typename VectorPtrView<T, cu_COPIER>::const_iterator
VectorPtrView<T, cu_COPIER>::
find (const data_type &x) const throw ()  {

    for (const_iterator itr = begin (); itr != end (); ++itr)
        if (**itr == x)
            return (itr);

    return (end ());
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
