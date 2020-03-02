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

#pragma once

#include <DataFrame/MMap/MMapBase.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

class   MMapSharedMem : public MMapBase  {

public:

    inline MMapSharedMem (const char *file_name,
                          OPEN_MODE open_mode,
                          size_type init_file_size,
                          mode_t file_open_mode = 0660)
        : MMapBase (file_name,
                    open_mode,
                    _shared_memory_,
                    init_file_size,
                    file_open_mode)  {

        _translate_open_mode ();
        open ();
        initial_map_done_ = true;
    }

    virtual bool open ();
    virtual void unlink ();
    inline virtual bool clobber ()  {

        initial_map_done_ = false;
        return (! MMapBase::close ());
    }

protected:

   // this is public in the base class.
   //
    virtual int close (CLOSE_MODE close_mode = _normal_);

    virtual bool _initial_map_posthook ();

private:

    bool    initial_map_done_ { false };
};

} // namespace hmdf

#endif // _WIN32

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
