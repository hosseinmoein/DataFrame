// Hossein Moein
// Juine 4, 2019
// Copyright (C) 2019-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/FixedSizeString.h>
#include <DataFrame/MMap/ObjectVector.h>

#ifndef _WIN32

#include <exception>
#include <sys/types.h>
#include <sys/mman.h>

//
// There must be a nice concise language inside C++ trying to get out
//

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, typename B>
ObjectVector<T, B>::
ObjectVector(const char *name, ACCESS_MODE access_mode, size_type buffer_size)
    : BaseClass (name, BaseClass::_bappend_, buffer_size)  {

    const bool  just_created = BaseClass::get_file_size () == 0;

    if (just_created)  {
        const MetaData  meta_data (0, time (nullptr));

       // Create the meta data record
       //
        if (! BaseClass::write (&meta_data, sizeof(MetaData), 1))
            throw std::runtime_error ("ObjectVector::ObjectVector<<(): Cannot"
                                      " write(). internal header record");

        BaseClass::flush ();
    }
    else  {
        if (BaseClass::get_file_size () < sizeof(MetaData))  {
            String1K    err;

            err.printf ("ObjectVector::ObjectVector(): "
                        "ObjectVector seems to be in an inconsistent "
                        "state (%d).",
                        BaseClass::get_file_size ());
            throw std::runtime_error (err.c_str ());
        }
    }

    const MetaData  *mdata_ptr =
        reinterpret_cast<const MetaData *>
            (reinterpret_cast<const char *>(BaseClass::_get_base_ptr ()));

    cached_object_count_ = mdata_ptr->object_count;
    seek_ (cached_object_count_);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
typename ObjectVector<T, B>::size_type
ObjectVector<T, B>::tell_ () const noexcept  {

    const size_type pos = BaseClass::tell ();

    return ((pos - sizeof(MetaData)) / sizeof(value_type));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
ObjectVector<T, B>::~ObjectVector ()  {

    if (BaseClass::get_device_type () == BaseClass::_shared_memory_ ||
        BaseClass::get_device_type () == BaseClass::_mmap_file_)
        BaseClass::flush ();
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::value_type &
ObjectVector<T, B>::operator [] (size_type index)  {

    value_type  &this_item =
        *(reinterpret_cast<value_type *>
              (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
               sizeof(MetaData)) +
          index);

    return (this_item);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline const typename ObjectVector<T, B>::value_type &
ObjectVector<T, B>::operator [] (size_type index) const noexcept  {

    const value_type    &this_item =
        *(reinterpret_cast<const value_type *>
              (reinterpret_cast<const char *>(BaseClass::_get_base_ptr ()) +
               sizeof(MetaData)) +
          index);

    return (this_item);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
time_t ObjectVector<T, B>::creation_time () const noexcept  {

    const MetaData  &meta_data =
        *reinterpret_cast<const MetaData *>
            (reinterpret_cast<const char *>(BaseClass::_get_base_ptr ()));

    return (meta_data.creation_time);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
void ObjectVector<T, B>::set_access_mode (ACCESS_MODE am) const  {

    const int   rc =
        ::posix_madvise (BaseClass::_get_base_ptr (),
                         BaseClass::get_mmap_size (),
                         (am == _normal_) ? POSIX_MADV_NORMAL
                             : (am == _need_now_) ? POSIX_MADV_WILLNEED
                             : (am == _random_) ? POSIX_MADV_RANDOM
                             : (am == _sequential_) ? POSIX_MADV_SEQUENTIAL
                             : (am == _dont_need_) ? POSIX_MADV_DONTNEED
                             : -1);

    if (rc)  {
        String1K    err;

        err.printf ("ObjectVector::set_access_mode(): ::posix_madvise(): "
                    "(%d) %s",
                    errno, strerror (errno));
        throw std::runtime_error (err.c_str ());
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
bool ObjectVector<T, B>::seek_ (size_type obj_num) const noexcept  {

    BaseClass   *nc_ptr =
        const_cast<BaseClass *>(static_cast<const BaseClass *>(this));

    return (nc_ptr->seek (sizeof(MetaData) + (obj_num * sizeof(value_type)),
                          BaseClass::_seek_set_) == 0 ? true : false);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
int ObjectVector<T, B>::write_ (const value_type *data_ele, size_type count)  {

    const int   rc = BaseClass::write (data_ele, sizeof(value_type), count);

    if (rc != count)  {
        String1K    err;

        err.printf ("ObjectVector::write(): Cannot write %llu elements. "
                    "Instead wrote %d elements.",
                    count, rc);
        throw std::runtime_error (err.c_str ());
    }

    MetaData    &meta_data =
        *reinterpret_cast<MetaData *>
            (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()));

    meta_data.object_count += count;
    cached_object_count_ += count;
    return (rc);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
typename ObjectVector<T, B>::iterator
ObjectVector<T, B>::erase (iterator first, iterator last)  {

    const size_type lnum = &(*last) - &(*begin ());

    ::memmove (&(*first), &(*last),
               (&(*end ()) - &(*last)) * sizeof (value_type));

    const size_type s = &(*last) - &(*first);

    BaseClass::truncate (BaseClass::_file_size - s * sizeof (value_type));

    MetaData    &meta_data =
        *reinterpret_cast<MetaData *>
            (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()));

    meta_data.object_count -= s;
    cached_object_count_ -= s;
    seek (cached_object_count_);

    return (iterator_at (lnum - s));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
template<typename I>
void ObjectVector<T, B>::insert (iterator pos, I first, I last)  {

    const long      int to_add = &(*last) - &(*first);
    const size_type pos_index = &(*pos) - &(*begin ());

    BaseClass::truncate (BaseClass::_file_size + to_add * sizeof (value_type));

    const iterator  new_pos = iterator_at (pos_index);

    ::memmove (&(*(new_pos + to_add)), &(*new_pos),
               (&(*end ()) - &(*new_pos)) * sizeof (value_type));
    ::memcpy (&(*new_pos), &(*first), to_add * sizeof (value_type));

    MetaData    &meta_data =
        *reinterpret_cast<MetaData *>
            (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()));

    meta_data.object_count += to_add;
    cached_object_count_ += to_add;
    seek (cached_object_count_);

    return;
}

} // namespace hmdf

#endif // _WIN32

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
