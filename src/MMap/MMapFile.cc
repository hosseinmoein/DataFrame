// Hossein Moein
// May 28, 2019
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _WIN32

#include <DataFrame/MMap/MMapFile.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

bool MMapFile::open ()  {

    if (is_open ())
        throw std::runtime_error (
            "MMapFile::open(): The device is already open");

    _file_desc = ::open (get_file_name (), _file_open_flags, _file_open_mode);
    if (_file_desc > 0)  {
        struct stat stat_data;

        if (! ::fstat (_file_desc, &stat_data))
            _file_size = stat_data.st_size;
        else  {
            String2K    err;

            err.printf ("MMapFile::open(): ::fstat(): (%d) %s -- %s",
                        errno, strerror (errno), get_file_name ());

            ::close (_file_desc);
            _file_desc = 0;
            throw std::runtime_error (err.c_str ());
        }
    }
    else  {
        String2K    err;

        err.printf ("MMapFile::open(): ::open(): (%d) %s -- %s",
                    errno, ::strerror (errno), get_file_name ());

        _file_desc = 0;
        throw std::runtime_error (err.c_str ());
    }

    return (_initial_map (_file_size, _mmap_prot, _mmap_flags, _file_desc));
}

// ----------------------------------------------------------------------------

void MMapFile::unlink ()  {

    if (is_open ())
        close ();

    if (::unlink (get_file_name ()) < 0)  {
        String2K    err;

        err.printf ("MMapFile::unlink(): ::unlink(): (%d) %s -- %s",
                    errno, ::strerror (errno), get_file_name ());
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
