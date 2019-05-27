// Hossein Moein
// August 21, 2007

#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <DMScu_MMapSharedMem.h>

// ----------------------------------------------------------------------------

bool DMScu_MMapSharedMem::open ()  {

    if (is_open ())
        throw DMScu_Exception ("DMScu_MMapSharedMem::open(): "
                               "The device is already open");

    if ((_file_desc = ::shm_open (get_file_name (), _file_open_flags,
                                  _file_open_mode)) > 0)  {
        struct  stat    stat_data;

        if (! ::fstat (_file_desc, &stat_data))
            _file_size = stat_data.st_size;
        else  {
            DMScu_FixedSizeString<4095> err;

            err.printf ("DMScu_MMapSharedMem::shm_open(): ::fstat(): (%d) %s"
                        " --- %s",
                        errno, ::strerror (errno), get_file_name ());

            ::close (_file_desc);
            _file_desc = 0;
            throw DMScu_Exception (err.c_str ());
        }
    }
    else  {
        DMScu_FixedSizeString<4095> err;

        err.printf ("DMScu_MMapSharedMem::open(): ::shm_open(): (%d) %s"
                    " --- %s",
                    errno, ::strerror (errno), get_file_name ());

        _file_desc = 0;
        throw DMScu_Exception (err.c_str ());
    }

    if (initial_map_done_)
        return (true);
    else
        return (_initial_map(_file_size, _mmap_prot, _mmap_flags, _file_desc));
}

// ----------------------------------------------------------------------------

bool DMScu_MMapSharedMem::_initial_map_posthook ()  {

    const   flag_type   flag =
        _s_bwrite_ | _s_bappend_ | _s_write_ | _s_append_;

    if (! (_file_flags & flag))
        close ();
    _file_flags |= _in_use_;

    return (true);
}

// ----------------------------------------------------------------------------

int DMScu_MMapSharedMem::close (CLOSE_MODE close_mode)  {

    if (is_open ())  {
        const   int status = ::close (_file_desc);

        if (status != 0)  {
            DMScu_FixedSizeString<4095> err;

            err.printf ("DMScu_MMapSharedMem::close(): ::close(): (%d) %s"
                        " --- %s",
                        errno, ::strerror (errno), get_file_name ());
            throw DMScu_Exception (err.c_str ());
        }
        _file_desc = 0;
    }

    return (0);
}

// ----------------------------------------------------------------------------

void DMScu_MMapSharedMem::unlink ()  {

    if (is_open ())
        close ();

    DMScu_MMapBase::close ();
    if (::shm_unlink (get_file_name ()) < 0)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_MMapSharedMem::unlink(): ::shm_unlink(%s): (%d) %s",
                    get_file_name (), errno, ::strerror (errno));
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
