// Hossein Moein
// August 23, 2007

#include <sys/types.h>
#include <sys/mman.h>

#include <DMScu_Exception.h>
#include <DMScu_FixedSizeString.h>

#include <DMSob_ObjectBase.h>

//
// There must be a nice concise language inside C++ trying to get out
//

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
DMSob_ObjectBase (const char *name,
                  OPEN_MODE open_mode,
                  ACCESS_MODE access_mode,
                  const header_type &header,
                  size_type buffer_size)
    : BaseClass (name,
                 static_cast<typename BaseClass::OPEN_MODE>(open_mode),
                 buffer_size),
      cached_object_count_ (0)  {

    const   bool    just_created = BaseClass::get_file_size () == 0;

    if (just_created)  {
        if (static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()) == _read_)  {
            DMScu_FixedSizeString<2047> err;

            err.printf ("DMSob_ObjectBase::DMSob_ObjectBase(): "
                        "Object '%s' doesn't exist for reading.",
                        name);
            throw DMScu_Exception (err.c_str ());
        }

       // Create the header record
       //
        if (! BaseClass::write (&header, HEADER_SIZE, 1))
            throw DMScu_Exception ("DMSob_ObjectBase::DMSob_ObjectBase<<(): "
                                   "Cannot write(). header record");

        const   _internal_header_type   meta_data (0, time (NULL));

       // Create the meta data record
       //
        if (! BaseClass::write (&meta_data, _INTERNAL_HEADER_SIZE, 1))
            throw DMScu_Exception ("DMSob_ObjectBase::DMSob_ObjectBase<<(): "
                                   "Cannot write(). internal header record");

        flush ();
    }
    else  {
        if (BaseClass::get_file_size () < _DATA_START_POINT)  {
            DMScu_FixedSizeString<2047> err;

            err.printf ("DMSob_ObjectBase::DMSob_ObjectBase(): "
                        "ObjectBase seems to be in an inconsistent "
                        "state (%d).",
                        BaseClass::get_file_size ());
            throw DMScu_Exception (err.c_str ());
        }
    }

   // Extract the meta data record
   //
    _internal_header_type   *mdata_ptr =
        reinterpret_cast<_internal_header_type *>
            (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
             HEADER_SIZE);

    cached_object_count_ = mdata_ptr->object_count;
    switch (static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()))  {
        case _read_:
            seek (0);
            break;
        case _write_:
            seek (0);
            if (! just_created)
                mdata_ptr->object_count = cached_object_count_ = 0;
            break;
        case _append_:
            seek (cached_object_count_);
            break;
    }
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
typename DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::size_type
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::tell () const throw ()  {

    const   size_type   pos = BaseClass::tell ();

    return ((pos - _DATA_START_POINT) / DATA_SIZE);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::~DMSob_ObjectBase ()  {

    if (BaseClass::get_device_type () == BaseClass::_shared_memory_ ||
        BaseClass::get_device_type () == BaseClass::_mmap_file_)
        flush ();
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
inline
typename DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::data_type &
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
operator [] (size_type index)  {

    if (! (static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()) & _write_ ||
           static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()) & _append_))
        throw DMScu_Exception ("DMSob_ObjectBase::operator[](): "
                               "ObejctBase is not open to write.");

    data_type   &this_item =
        *(reinterpret_cast<data_type *>
              (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
               _DATA_START_POINT) +
          index);

    return (this_item);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
inline
const typename DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::data_type &
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
operator [] (size_type index) const throw ()  {

    const   data_type   &this_item =
        *(reinterpret_cast<const data_type *>
              (reinterpret_cast<const char *>(BaseClass::_get_base_ptr ()) +
               _DATA_START_POINT) +
          index);

    return (this_item);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
typename DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::header_type &
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::get_header_rec ()  {

    if (! (static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()) & _write_ ||
           static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()) & _append_))
        throw DMScu_Exception ("DMSob_ObjectBase::get_header_rec(): "
                               "ObejctBase is not open to write.");

    header_type &header_rec =
        *(reinterpret_cast<header_type *>(BaseClass::_get_base_ptr ()));

    return (header_rec);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
const typename DMSob_ObjectBase<ob_HEADER, ob_DATA,
                                ob_BASE_CLASS>::header_type &
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
get_header_rec () const throw ()  {

    const   header_type &header_rec =
        *(reinterpret_cast<const header_type *>(BaseClass::_get_base_ptr ()));

    return (header_rec);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
time_t
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
creation_time () const throw ()  {

    const   _internal_header_type   &meta_data =
        *(reinterpret_cast<const _internal_header_type *>
              (reinterpret_cast<const char *>(BaseClass::_get_base_ptr ()) +
               HEADER_SIZE));

    return (meta_data.creation_time);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
void
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
set_access_mode (ACCESS_MODE am) const  {

    if (empty ())
        return;

    const   int rc =
        ::posix_madvise (BaseClass::_get_base_ptr (),
                         BaseClass::get_mmap_size (),
                         (am == _normal_) ? POSIX_MADV_NORMAL
                             : (am == _need_now_) ? POSIX_MADV_WILLNEED
                             : (am == _random_) ? POSIX_MADV_RANDOM
                             : (am == _sequential_) ? POSIX_MADV_SEQUENTIAL
                             : (am == _dont_need_) ? POSIX_MADV_DONTNEED
                             : -1);

    if (rc)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMSob_ObjectBase::set_access_mode(): ::posix_madvise(): "
                    "(%d) %s",
                    errno, strerror (errno));
        throw DMScu_Exception (err.c_str ());
    }

    return;
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
bool
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
seek (size_type obj_num) const throw ()  {

    BaseClass   *nc_ptr =
        const_cast<BaseClass *>(static_cast<const BaseClass *>(this));

    return (nc_ptr->seek (_DATA_START_POINT + (obj_num * DATA_SIZE),
                          BaseClass::_seek_set_) == 0 ? true : false);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
int
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
write (const data_type *data_ele, size_type count)  {

    const   int rc = BaseClass::write (data_ele, DATA_SIZE, count);

    if (rc != count)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMSob_ObjectBase::write(): Cannot write %llu elements. "
                    "Instead wrote %d elements.",
                    count, rc);
        throw DMScu_Exception (err.c_str ());
    }

    _internal_header_type   &meta_data =
        *(reinterpret_cast<_internal_header_type *>
              (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
               HEADER_SIZE));

    meta_data.object_count += count;
    cached_object_count_ += count;
    return (rc);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
inline int
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
read (data_type *data_ele, size_type count) throw ()  {

    return (BaseClass::read (data_ele, DATA_SIZE, count));
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
typename DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::iterator
DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
erase (iterator first, iterator last)  {

    if (! (static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()) & _write_ ||
           static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()) & _append_))
        throw DMScu_Exception ("DMSob_ObjectBase::erase(): "
                               "ObejctBase is not open to write.");

    const   size_type   lnum = &(*last) - &(*begin ());

    ::memmove (&(*first), &(*last),
               (&(*end ()) - &(*last)) * sizeof (data_type));

    const   size_type   s = &(*last) - &(*first);

    BaseClass::truncate (BaseClass::_file_size - s * sizeof (data_type));

    _internal_header_type   &meta_data =
        *(reinterpret_cast<_internal_header_type *>
              (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
               HEADER_SIZE));

    meta_data.object_count -= s;
    cached_object_count_ -= s;
    seek (cached_object_count_);

    return (iterator_at (lnum - s));
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
template<class ob_ITER>
void DMSob_ObjectBase<ob_HEADER, ob_DATA, ob_BASE_CLASS>::
insert (iterator pos, ob_ITER first, ob_ITER last)  {

    const   long    int to_add = &(*last) - &(*first);
    const   size_type   pos_index = &(*pos) - &(*begin ());

    BaseClass::truncate (BaseClass::_file_size + to_add * sizeof (data_type));

    const   iterator    new_pos = iterator_at (pos_index);

    ::memmove (&(*(new_pos + to_add)), &(*new_pos),
               (&(*end ()) - &(*new_pos)) * sizeof (data_type));
    ::memcpy (&(*new_pos), &(*first), to_add * sizeof (data_type));

    _internal_header_type   &meta_data =
        *(reinterpret_cast<_internal_header_type *>
              (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
               HEADER_SIZE));

    meta_data.object_count += to_add;
    cached_object_count_ += to_add;
    seek (cached_object_count_);

    return;
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
