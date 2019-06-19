// Hossein Moein
// May 28, 2019
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _WIN32

#pragma once

#include <DataFrame/MMap/MMapBase.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

class   MMapFile : public MMapBase  {

public:

    inline MMapFile (const char *file_name,
                     OPEN_MODE open_mode,
                     size_type buffer_size = 0LL,
                     mode_t file_open_mode = S_IRUSR | S_IWUSR | S_IRGRP)
        : MMapBase (file_name,
                    open_mode,
                    _mmap_file_,
                    buffer_size,
                    file_open_mode)  {

        _translate_open_mode ();
        open ();
    }

    virtual bool open ();
    virtual void unlink ();

protected:

    virtual bool _initial_map_posthook ()  {

        _file_flags |= _in_use_;
        return (true);
    }
};

} // namespace hmdf

#endif // _WIN32

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
