// Hossein Moein
// August 21, 2007
// To the extent possible under law, the author(s) have dedicated all
// copyright and related and neighboring rights to this software to
// the public domain worldwide. This software is distributed without
// any warranty.

#ifndef _INCLUDED_DMScu_MMapFile_h
#define _INCLUDED_DMScu_MMapFile_h 0

// ----------------------------------------------------------------------------

#include <DMScu_MMapBase.h>

// ----------------------------------------------------------------------------

class   DMScu_MMapFile : public DMScu_MMapBase  {


    public:

        inline DMScu_MMapFile (const char *file_name, OPEN_MODE open_mode,
                               size_type buffer_size = 0LL,
                               mode_t file_open_mode =
                                   S_IRUSR | S_IWUSR | S_IRGRP)
            : DMScu_MMapBase (file_name, open_mode, _mmap_file_,
                              buffer_size, file_open_mode)  {

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

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMScu_MMapFile_h
#define _INCLUDED_DMScu_MMapFile_h 1
#endif  // _INCLUDED_DMScu_MMapFile_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
