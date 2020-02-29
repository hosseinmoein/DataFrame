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
