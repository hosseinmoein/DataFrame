// Hossein Moein
// June 4, 2019
/*
Copyright (c) 2019-2022, Hossein Moein
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

#include <DataFrame/MMap/ObjectVector.h>
#include <DataFrame/Utils/FixedSizeString.h>

#ifndef _WIN32

#include <exception>
#include <string>
#include <sys/mman.h>
#include <sys/types.h>

//
// There must be a nice concise language inside C++ trying to get out
//

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T, typename B>
inline typename ObjectVector<T, B>::MetaData &
ObjectVector<T, B>::_get_meta_data() noexcept  {

    return (*reinterpret_cast<MetaData *>
                (reinterpret_cast<char *>(BaseClass::_get_base_ptr ())));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline const typename ObjectVector<T, B>::MetaData &
ObjectVector<T, B>::_get_meta_data() const noexcept  {

    return (*reinterpret_cast<const MetaData *>
                (reinterpret_cast<const char *>(BaseClass::_get_base_ptr ())));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
void ObjectVector<T, B>::setup_()  {

    const size_type file_size = BaseClass::get_file_size ();

    if (file_size == 0)  {  // File was just created
        const MetaData  meta_data (0, time (nullptr));

       // Create the meta data record
       //
        if (! BaseClass::write (&meta_data, sizeof(MetaData), 1))
            throw std::runtime_error ("ObjectVector::setup_(): Cannot"
                                      " write(). internal header record");
        BaseClass::flush ();
    }
    else  {  // An existing file
        if (file_size < sizeof(MetaData))  {
            String1K    err;

            err.printf ("ObjectVector::setup_(): "
                        "ObjectVector seems to be in an inconsistent "
                        "state (%d).",
                        file_size);
            throw std::runtime_error (err.c_str ());
        }
    }
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
ObjectVector<T, B>::
ObjectVector(const char *name, ACCESS_MODE access_mode, size_type buffer_size)
    : BaseClass (name, BaseClass::_bappend_, buffer_size)  {

    setup_();

    seek_ (_get_meta_data().object_count);
    set_access_mode (access_mode);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
ObjectVector<T, B>::ObjectVector(const ObjectVector &that)
    : BaseClass (that.get_file_name(),
                 BaseClass::_bappend_,
                 that.get_buffer_size())  {

    setup_();

    seek_ (_get_meta_data().object_count);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
ObjectVector<T, B>::
ObjectVector(const char *name,
             size_type n,
             const T &value,
             ACCESS_MODE access_mode,
             size_type buffer_size)
    : BaseClass (name, BaseClass::_bappend_, buffer_size)  {

    setup_();

    clear();
    reserve(n);
    for (size_type i = 0; i < n; ++i)  push_back(value);
    set_access_mode (access_mode);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
template<typename ITER>
ObjectVector<T, B>::
ObjectVector(const char *name,
             ITER first,
             ITER last,
             ACCESS_MODE access_mode,
             size_type buffer_size)
    : BaseClass (name, BaseClass::_bappend_, buffer_size)  {

    setup_();

    clear();
    reserve(std::distance(first, last));
    for (ITER citer = first; citer != last; ++citer)  push_back(*citer);
    set_access_mode (access_mode);
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
ObjectVector<T, B> &ObjectVector<T, B>::operator = (const ObjectVector &rhs) {

    // This class/file is uniquely identified by name
    if (strcmp(BaseClass::get_file_name(), rhs.get_file_name()))  {
        if (BaseClass::get_device_type () == BaseClass::_shared_memory_ ||
            BaseClass::get_device_type () == BaseClass::_mmap_file_)
            BaseClass::flush ();
        // This is really bad design. But it is the cost of having this vector
        BaseClass::close();
        _set_file_name(rhs.get_file_name());
        _set_open_mode(BaseClass::_bappend_);
        set_buffer(rhs.get_buffer_size());
        _set_file_open_mode(rhs.get_file_open_mode());
        BaseClass::_translate_open_mode ();
        BaseClass::open ();
    }
    return (*this);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
ObjectVector<T, B>::
ObjectVector(const char *name,
             const std::vector<T> &vec,
             ACCESS_MODE access_mode,
             size_type buffer_size)
    : BaseClass (name, BaseClass::_bappend_, buffer_size)  {

    setup_();

    *this = vec;
    seek_ (_get_meta_data().object_count);
    set_access_mode (access_mode);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
ObjectVector<T, B> &ObjectVector<T, B>::
operator = (const std::vector<T> &rhs) {

    clear();
    reserve(rhs.size());
    write_(rhs.data(), rhs.size());
    return (*this);
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
inline typename ObjectVector<T, B>::reference
ObjectVector<T, B>::operator [] (size_type index)  {

    return(*(reinterpret_cast<value_type *>
                 (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
                  sizeof(MetaData)) +
             index));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::const_reference
ObjectVector<T, B>::operator [] (size_type index) const  {

    return(*(reinterpret_cast<const value_type *>
                 (reinterpret_cast<const char *>(BaseClass::_get_base_ptr ()) +
                  sizeof(MetaData)) +
             index));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::reference
ObjectVector<T, B>::at (size_type index)  {

    return ((*this)[index]);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::const_reference
ObjectVector<T, B>::at (size_type index) const  {

    return ((*this)[index]);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::reference
ObjectVector<T, B>::front ()  {

    return ((*this)[0]);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::const_reference
ObjectVector<T, B>::front () const  {

    return ((*this)[0]);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::reference
ObjectVector<T, B>::back ()  {

    return ((*this)[size() - 1]);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::const_reference
ObjectVector<T, B>::back () const  {

    return ((*this)[size() - 1]);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline void ObjectVector<T, B>::push_back (const value_type &d)  {

    write_ (&d, 1);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::size_type ObjectVector<T, B>::
size () const noexcept {

    return (_get_meta_data().object_count);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline bool ObjectVector<T, B>::empty () const noexcept {

    return (size() == 0);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
template<typename ... Ts>
inline void ObjectVector<T, B>::emplace_back (Ts && ... args)  {

    const value_type    item (std::forward<Ts>(args) ...);

    write_ (&item, 1);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
time_t ObjectVector<T, B>::creation_time () const noexcept  {

    return (_get_meta_data().creation_time);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
void ObjectVector<T, B>::set_access_mode (ACCESS_MODE am) const  {

    const int   rc =
        ::posix_madvise (
            BaseClass::_get_base_ptr (),
            BaseClass::get_mmap_size (),
            (am == ACCESS_MODE::normal) ? POSIX_MADV_NORMAL
                 : (am == ACCESS_MODE::need_now) ? POSIX_MADV_WILLNEED
                 : (am == ACCESS_MODE::random) ? POSIX_MADV_RANDOM
                 : (am == ACCESS_MODE::sequential) ? POSIX_MADV_SEQUENTIAL
                 : (am == ACCESS_MODE::dont_need) ? POSIX_MADV_DONTNEED
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

    auto    &obj_count = _get_meta_data().object_count;

    obj_count += count;
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

    MetaData    &meta_data = _get_meta_data();

    meta_data.object_count -= s;
    seek_ (meta_data.object_count);

    return (begin() + lnum - s);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::iterator ObjectVector<T, B>::
begin() noexcept  {

    return (iterator(&((*this)[0])));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::iterator ObjectVector<T, B>::
end() noexcept  {

    return (iterator(&((*this)[size()])));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::const_iterator ObjectVector<T, B>::
begin () const noexcept  {

    return (const_iterator (&((*this)[0])));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::const_iterator ObjectVector<T, B>::
end () const noexcept  {

    return (const_iterator (&((*this)[size()])));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::reverse_iterator ObjectVector<T, B>::
rbegin() noexcept  {

    return (reverse_iterator (&((*this)[size() - 1])));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::reverse_iterator ObjectVector<T, B>::
rend() noexcept  {

    return (reverse_iterator (&((*this)[0]) - 1));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::const_reverse_iterator ObjectVector<T, B>::
rbegin () const noexcept  {

    return (const_reverse_iterator (&((*this)[size() - 1])));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::const_reverse_iterator ObjectVector<T, B>::
rend () const noexcept  {

    return (const_reverse_iterator (&((*this)[0]) - 1));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
typename ObjectVector<T, B>::iterator
ObjectVector<T, B>::erase (iterator pos)  {

    return (erase (pos, pos + 1));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
template<typename I>
void ObjectVector<T, B>::insert (iterator pos, I first, I last)  {

    const size_type to_add = &(*last) - &(*first);
    const size_type pos_index = &(*pos) - &(*begin ());

    BaseClass::truncate(BaseClass::_file_size + to_add * sizeof (value_type));

    const iterator  new_pos = begin() + pos_index;

    ::memmove (&(*(new_pos + to_add)), &(*new_pos),
               (&(*end ()) - &(*new_pos)) * sizeof (value_type));
    ::memcpy (&(*new_pos), &(*first), to_add * sizeof (value_type));

    MetaData    &meta_data = _get_meta_data();

    meta_data.object_count += to_add;
    seek_ (meta_data.object_count);

    return;
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
void ObjectVector<T, B>::insert (iterator pos, const_reference value)  {

    return (insert (pos, &value, &value + 1));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline void ObjectVector<T, B>::reserve (size_type s)  {

    const size_type trun_size = s * sizeof(value_type) + sizeof(MetaData);

    if (trun_size > BaseClass::get_file_size ())
        BaseClass::truncate (trun_size);

    return;
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline void ObjectVector<T, B>::clear ()  {

    erase (begin (), end ());
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline void ObjectVector<T, B>::pop_back ()  {

    erase (end () - 1);
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline typename ObjectVector<T, B>::size_type ObjectVector<T, B>::
capacity () const noexcept  {

    const size_type trun_size = BaseClass::get_mmap_size() - sizeof(MetaData);

    return (trun_size / sizeof(value_type));
}

// ----------------------------------------------------------------------------

template<typename T, typename B>
inline void ObjectVector<T, B>::shrink_to_fit ()  {

    const size_type trun_size = size() * sizeof(value_type) + sizeof(MetaData);

    if (trun_size < BaseClass::get_file_size ())
        BaseClass::truncate (trun_size);

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
