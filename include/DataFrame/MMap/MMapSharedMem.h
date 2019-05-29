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
