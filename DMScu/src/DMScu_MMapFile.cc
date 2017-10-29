// Hossein Moein
// August 21, 2007
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <DMScu_MMapFile.h>

// ----------------------------------------------------------------------------

bool DMScu_MMapFile::open ()  {

    if (is_open ())
        throw DMScu_Exception ("DMScu_MMapFile::open(): "
                               "The device is already open");

    if ((_file_desc = ::open (get_file_name (), _file_open_flags,
                              _file_open_mode)) > 0)  {
        struct  stat    stat_data;

        if (! ::fstat (_file_desc, &stat_data))
            _file_size = stat_data.st_size;
        else  {
            DMScu_FixedSizeString<4095> err;

            err.printf ("DMScu_MMapFile::open(): ::fstat(): (%d) %s --- %s",
                        errno, strerror (errno), get_file_name ());

            ::close (_file_desc);
            _file_desc = 0;
            throw DMScu_Exception (err.c_str ());
        }
    }
    else  {
        DMScu_FixedSizeString<4095> err;

        err.printf ("DMScu_MMapFile::open(): ::open(): (%d) %s --- %s",
                    errno, ::strerror (errno), get_file_name ());

        _file_desc = 0;
        throw DMScu_Exception (err.c_str ());
    }

    return (_initial_map (_file_size, _mmap_prot, _mmap_flags, _file_desc));
}

// ----------------------------------------------------------------------------

void DMScu_MMapFile::unlink ()  {

    if (is_open ())
        close ();

    if (::unlink (get_file_name ()) < 0)  {
        DMScu_FixedSizeString<4095> err;

        err.printf ("DMScu_MMapFile::unlink(): ::unlink(): (%d) %s --- %s",
                    errno, ::strerror (errno), get_file_name ());
        throw DMScu_Exception (err.c_str ());
    }

    return;
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
