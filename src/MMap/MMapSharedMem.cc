// Hossein Moein
// May 28, 2019
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _WIN32

#include <DataFrame/MMap/MMapSharedMem.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

bool MMapSharedMem::open ()  {

    if (is_open ())
        throw std::runtime_error (
            "MMapSharedMem::open(): The device is already open");

    _file_desc = ::shm_open(get_file_name(), _file_open_flags, _file_open_mode);
    if (_file_desc > 0)  {
        struct stat stat_data;

        if (! ::fstat (_file_desc, &stat_data))
            _file_size = stat_data.st_size;
        else  {
            String2K    err;

            err.printf ("MMapSharedMem::shm_open(): ::fstat(): (%d) %s -- %s",
                        errno, ::strerror (errno), get_file_name ());

            ::close (_file_desc);
            _file_desc = 0;
            throw std::runtime_error (err.c_str ());
        }
    }
    else  {
        String2K    err;

        err.printf ("MMapSharedMem::open(): ::shm_open(): (%d) %s -- %s",
                    errno, ::strerror (errno), get_file_name ());

        _file_desc = 0;
        throw std::runtime_error (err.c_str ());
    }

    if (initial_map_done_)
        return (true);
    else
        return (_initial_map(_file_size, _mmap_prot, _mmap_flags, _file_desc));
}

// ----------------------------------------------------------------------------

bool MMapSharedMem::_initial_map_posthook ()  {

    const flag_type flag = _s_bwrite_ | _s_bappend_ | _s_write_ | _s_append_;

    if (! (_file_flags & flag))
        close ();
    _file_flags |= _in_use_;

    return (true);
}

// ----------------------------------------------------------------------------

int MMapSharedMem::close (CLOSE_MODE close_mode)  {

    if (is_open ())  {
        if (::close (_file_desc) != 0)  {
            String2K    err;

            err.printf ("MMapSharedMem::close(): ::close(): (%d) %s -- %s",
                        errno, ::strerror (errno), get_file_name ());
            throw std::runtime_error (err.c_str ());
        }
        _file_desc = 0;
    }

    return (0);
}

// ----------------------------------------------------------------------------

void MMapSharedMem::unlink ()  {

    if (is_open ())
        close ();

    MMapBase::close ();
    if (::shm_unlink (get_file_name ()) < 0)  {
        String2K    err;

        err.printf ("MMapSharedMem::unlink(): ::shm_unlink(%s): (%d) %s",
                    get_file_name (), errno, ::strerror (errno));
        throw std::runtime_error (err.c_str ());
    }

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
