// Hossein Moein
// September 23, 2007

#include <sys/types.h>
#include <fcntl.h>

#include <DMScu_Exception.h>
#include <DMScu_FixedSizeString.h>

#include <DMSob_FileObject.h>

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
DMSob_FileObject<ob_HEADER, ob_DATA>::
DMSob_FileObject (const char *name,
                  OPEN_MODE open_mode,
                  ACCESS_MODE access_mode,
                  const header_type &header,
                  size_type buffer_size)
    : BaseClass (name,
                 static_cast<BaseClass::OPEN_MODE>(open_mode),
                 buffer_size)  {

    const   bool    just_created = BaseClass::get_file_size () == 0;

    if (just_created)  {
        if (static_cast<OPEN_MODE>(_get_open_mode ()) == _read_)  {
            DMScu_FixedSizeString<2047> err;

            err.printf ("DMSob_FileObject::DMSob_FileObject(): "
                        "Object '%s' doesn't exist for reading.",
                        name);
            throw DMScu_Exception (err.c_str ());
        }

       // Create the header record
       //
        if (! BaseClass::write (&header, HEADER_SIZE, 1))
            throw DMScu_Exception ("DMSob_FileObject::DMSob_FileObject<<(): "
                                   "Cannot write(). header record");

        const   _internal_header_type   meta_data (0, time (NULL));

       // Create the meta data record
       //
        if (! BaseClass::write (&meta_data, _INTERNAL_HEADER_SIZE, 1))
            throw DMScu_Exception ("DMSob_FileObject::DMSob_FileObject<<(): "
                                   "Cannot write(). internal header record");

        BaseClass::flush ();
    }
    else  {
        if (BaseClass::get_file_size () < _DATA_START_POINT)  {
            DMScu_FixedSizeString<2047> err;

            err.printf ("DMSob_FileObject::DMSob_FileObject(): "
                        "FileObject seems to be in an inconsistent "
                        "state (%d).",
                        BaseClass::get_file_size ());
            throw DMScu_Exception (err.c_str ());
        }
    }

    switch (static_cast<OPEN_MODE>(_get_open_mode ()))  {
        case _read_:
            seek (0);
            break;
        case _write_:
            seek (0);
            break;
        case _append_:
            seek (object_count ());
            break;
    }

    set_access_mode (access_mode);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
typename DMSob_FileObject<ob_HEADER, ob_DATA>::size_type
DMSob_FileObject<ob_HEADER, ob_DATA>::tell () const throw ()  {

    const   size_type   pos = BaseClass::tell ();

    return ((pos - _DATA_START_POINT) / DATA_SIZE);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
bool DMSob_FileObject<ob_HEADER, ob_DATA>::attach ()  {

    BaseClass::open ();

    switch (static_cast<OPEN_MODE>(_get_open_mode ()))  {
        case _read_:
        case _write_:
            seek (0);
            break;
        case _append_:
            seek (object_count ());
            break;
    }

    return (true);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
bool DMSob_FileObject<ob_HEADER, ob_DATA>::dettach ()  {

    flush ();
    BaseClass::close ();
    return (true);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
typename DMSob_FileObject<ob_HEADER, ob_DATA>::size_type
DMSob_FileObject<ob_HEADER, ob_DATA>::object_count () const throw ()  {

    return ((get_file_size () - _DATA_START_POINT) / DATA_SIZE);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
int DMSob_FileObject<ob_HEADER, ob_DATA>::flush ()  {

    if (BaseClass::flush () != EOF)  {

        const   size_type   pos = BaseClass::tell ();

        _internal_header_type   node;

        BaseClass::seek (HEADER_SIZE, BaseClass::_seek_set_);
        BaseClass::read (&node, _INTERNAL_HEADER_SIZE, 1);

        node.object_count = object_count ();

        BaseClass::seek (HEADER_SIZE, BaseClass::_seek_set_);
        BaseClass::write (&node, _INTERNAL_HEADER_SIZE, 1);

        BaseClass::flush ();
        BaseClass::seek (pos, BaseClass::_seek_set_);
    }

    return (0);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
DMSob_FileObject<ob_HEADER, ob_DATA>::~DMSob_FileObject ()  {

    if (BaseClass::is_open ())
        dettach ();
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
inline typename DMSob_FileObject<ob_HEADER, ob_DATA>::data_type
DMSob_FileObject<ob_HEADER, ob_DATA>::operator [] (size_type index) throw ()  {

    const   size_type   pos = BaseClass::tell ();
    data_type           node;

    BaseClass::seek (_DATA_START_POINT + index * DATA_SIZE,
                     BaseClass::_seek_set_);
    BaseClass::read (&node, DATA_SIZE, 1);
    BaseClass::seek (pos, BaseClass::_seek_set_);

    return (node);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
typename DMSob_FileObject<ob_HEADER, ob_DATA>::header_type
DMSob_FileObject<ob_HEADER, ob_DATA>::get_header_rec () throw ()  {

    const   size_type   pos = BaseClass::tell ();
    header_type         node;

    BaseClass::seek (0, BaseClass::_seek_set_);
    BaseClass::read (&node, HEADER_SIZE, 1);
    BaseClass::seek (pos, BaseClass::_seek_set_);

    return (node);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
time_t DMSob_FileObject<ob_HEADER, ob_DATA>::creation_time () throw ()  {

    const   size_type       pos = BaseClass::tell ();
    _internal_header_type   node;

    BaseClass::seek (HEADER_SIZE, BaseClass::_seek_set_);
    BaseClass::read (&node, _INTERNAL_HEADER_SIZE, 1);
    BaseClass::seek (pos, BaseClass::_seek_set_);

    return (static_cast<time_t>(node.creation_time));
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
void DMSob_FileObject<ob_HEADER, ob_DATA>::
set_access_mode (ACCESS_MODE am) const  {

    const   int rc =
        ::posix_fadvise (_get_file_desc (),
                         _DATA_START_POINT,
                         0,
                         (am == _normal_) ? POSIX_FADV_NORMAL
                             : (am == _need_now_) ? POSIX_FADV_WILLNEED
                             : (am == _random_) ? POSIX_FADV_RANDOM
                             : (am == _sequential_) ? POSIX_FADV_SEQUENTIAL
                             : (am == _no_reuse_) ? POSIX_FADV_NOREUSE
                             : (am == _dont_need_) ? POSIX_FADV_DONTNEED
                             : -1);

    if (rc)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMSob_FileObject::set_access_mode(): ::posix_fadvise(): "
                    "(%d) %s",
                    errno, strerror (errno));
        throw DMScu_Exception (err.c_str ());
    }

    return;
}

//-----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
bool DMSob_FileObject<ob_HEADER, ob_DATA>::seek (size_type obj_num) throw ()  {

    BaseClass::seek (_DATA_START_POINT + obj_num * DATA_SIZE,
                     BaseClass::_seek_set_);
    return (true);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
inline int DMSob_FileObject<ob_HEADER, ob_DATA>::
write (const data_type *data_ele, size_type count)  {

    const   size_type   rc = BaseClass::write (data_ele, DATA_SIZE, count);

    if (rc != count)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMSob_FileObject::write(): Cannot write %llu elements. "
                    "Instead wrote %llu elements.",
                    count, rc);
        throw DMScu_Exception (err.c_str ());
    }

    return (rc);
}

// ----------------------------------------------------------------------------

template<class ob_HEADER, class ob_DATA>
inline int DMSob_FileObject<ob_HEADER, ob_DATA>::
read (data_type *data_ele, size_type count) throw ()  {

    const   size_type   rc = BaseClass::read (data_ele, DATA_SIZE, count);

    if (rc != count)  {
        // DMScu_FixedSizeString<2047> err;

        // err.printf ("DMSob_FileObject::read(): Cannot read %llu elements. "
        //             "Instead read %llu elements.",
        //             count, rc);
        // throw DMScu_Exception (err.c_str ());
        return (-1);
    }

    return (rc);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
