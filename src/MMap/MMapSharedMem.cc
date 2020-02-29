// Hossein Moein
// May 28, 2019
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
