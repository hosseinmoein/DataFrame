// Hossein Moein
// May 28, 2019
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _WIN32

#pragma once

#include <DataFrame/MMap/FileDef.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <fstream>
#include <errno.h>
#include <iostream>
#include <string>
#include <strings.h>
#include <sys/fcntl.h>
#include <sys/mman.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

class   MMapBase : public FileDef  {

public:

    static const size_type  SYSTEM_PAGE_SIZE;

    MMapBase () = delete;
    inline MMapBase (const char *file_name,
                     OPEN_MODE open_mode,
                     DEVICE_TYPE device_type,
                     size_type buffer_size = 0UL,
                     mode_t file_open_mode =
                           S_IRUSR | S_IWUSR | S_IRGRP) noexcept
        : FileDef (file_name, open_mode, device_type),
          buffer_size_ (buffer_size),
          _file_open_mode (file_open_mode)  {   }

    virtual ~MMapBase ()  { if (is_open ()) close (); }

    inline bool is_open () const noexcept  { return (_file_desc > 0); }

    virtual bool open () = 0;
    virtual int close (CLOSE_MODE close_mode = _normal_);
    size_type read (void *, size_type, size_type) noexcept;
    size_type write (const void *, size_type, size_type);

    inline int flush () noexcept  {

        const flag_type flag =
            _in_use_ | _s_bwrite_ | _s_write_ | _s_append_ | _s_bappend_;

        if (_file_flags & flag)  {
            ::msync (_mmap_ptr, _file_size, MS_SYNC | MS_INVALIDATE);
            return (0);
        }

        return (EOF);
    }

    int get_char () noexcept;
    inline flag_type get_line (char *buffer) noexcept  {

        return (get_token ('\n', buffer));
    }
    std::string get_string (const char *search_str) noexcept;
    std::string get_token (const char *delimit) noexcept;
    size_type get_token (char delimit, char *buffer) noexcept;
    int printf (const char *, ...) noexcept;
    int put_char (int);
    int put_string (const char *) noexcept;
    int put_back ();

   // Here you can remap the file, on the fly, from a different offset.
   // Be sure you know what you are doing. This call is useful to get
   // rid of excessive memory that you don't need anymore.
   //
   // NOTE: if offset is non-zero, the actual offset will be a multiple
   //       of SYSTEM_PAGE_SIZE. Therefore there is no guarantee that the
   //       actual offset will be offset. Any offset less than
   //       SYSTEM_PAGE_SIZE will be zero.
   //
    int remap (size_type offset = 0, size_type map_size = 0);

    int seek (size_type, SEEK_TYPE seek_type) noexcept;
    inline void rewind () noexcept  { seek (0L, _seek_set_); }
    size_type go_to_line (size_type);
    inline void set_buffer (size_type bs) noexcept  { buffer_size_ = bs; }
    int set_flag (int, flag_type);

    int truncate (size_type);

    inline MMapBase &operator << (char rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (char), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%c", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(char): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (unsigned char rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned char), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%c", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(unsigned char): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (short rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (short), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%hd", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(short): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (unsigned short rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned short), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%hu", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(unsigned short): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%d", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (unsigned int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%u", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(unsigned int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (long int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (long int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%ld", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(long int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (long long int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (long long int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%lld", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(long long int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (unsigned long int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned long int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%lu", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(unsigned long int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (unsigned long long int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned long long int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%llu", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(unsigned long long int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (float rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (float), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%.*f", _precision, rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(const float): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (double rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (double), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%.*lf", _precision, rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(const double): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (const char *rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (rhs, sizeof (char), ::strlen (rhs));
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%s", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(const char *): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &
    operator << (const std::string &rhs)  { return (*this << rhs.c_str ()); }
    inline MMapBase &
    operator << (const VirtualString &rhs)  { return (*this << rhs.c_str ()); }
    inline MMapBase &operator << (const void *rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (void *), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
            printf ("%p", rhs);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(const void *): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator << (const MMapBase &rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_) ||
            (_file_flags & _s_write_) || (_file_flags & _s_append_))
            write (rhs._get_base_ptr (), rhs.get_mmap_size (), 1);
        else  {
            String1K    err;

            err.printf ("MMapBase::<<(const MMapBase &): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    MMapBase &operator << (const FILE &);
    MMapBase &operator << (std::ifstream &);

    inline MMapBase &operator >> (char *rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (rhs, sizeof (char), _width);
        else  {
            strncpy (rhs, get_token (" ").c_str (), _width);
            rhs [_width] = 0;
        }

        return (*this);
    }
    inline MMapBase &operator >> (std::ostream &rhs)  {

        if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
            _file_flags & _s_append_)
            rhs << get_token (" ").data ();
        else  {
            String1K    err;

            err.printf ("MMapBase::>>(std::ostream &) "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator >> (std::string &rhs)  {

        if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
            _file_flags & _s_append_)
            rhs += get_token (" ");
        else  {
            String1K    err;

            err.printf ("MMapBase::>>(std::string &): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator >> (VirtualString &rhs)  {

        if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
            _file_flags & _s_append_)
            rhs += get_token (" ").c_str ();
        else  {
            String1K    err;

            err.printf ("MMapBase::>>(VirtualString &): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline MMapBase &operator >> (char &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (char), 1);
        else
            rhs = static_cast<char>(get_char ());

        return (*this);
    }
    inline MMapBase &operator >> (unsigned char &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (unsigned char), 1);
        else
            rhs = static_cast<unsigned char>(get_char ());

        return (*this);
    }
    inline MMapBase &operator >> (short &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (short), 1);
        else
            rhs = static_cast<short>
                (strtol (get_string ("0123456789").c_str (), nullptr, 0));

        return (*this);
    }
    inline MMapBase &operator >> (int &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (int), 1);
        else
            rhs = static_cast<int>
                (strtol (get_string ("0123456789").c_str (), nullptr, 0));

        return (*this);
    }
    inline MMapBase &operator >> (long int &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (long int), 1);
        else
            rhs =  strtol (get_string ("0123456789").c_str (), nullptr, 0);

        return (*this);
    }
    inline MMapBase &operator >> (long long int &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (long long int), 1);
        else
            rhs =  strtoll (get_string ("0123456789").c_str (), nullptr, 0);

        return (*this);
    }
    inline MMapBase &operator >> (unsigned short &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (unsigned short), 1);
        else
            rhs = static_cast<unsigned short>
                (strtol (get_string ("0123456789").c_str (), nullptr, 0));

        return (*this);
    }
    inline MMapBase &operator >> (unsigned int &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (unsigned int), 1);
        else
            rhs = static_cast<unsigned int>
                (strtol (get_string ("0123456789").c_str (), nullptr, 0));

        return (*this);
    }
    inline MMapBase &operator >> (unsigned long int &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (unsigned long int), 1);
        else
            rhs = static_cast<unsigned long int>
                (strtol (get_string ("0123456789").c_str (), nullptr, 0));

        return (*this);
    }
    inline MMapBase &operator >> (unsigned long long int &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (unsigned long long int), 1);
        else
            rhs = static_cast<unsigned long long int>
                (strtol (get_string ("0123456789").c_str (), nullptr, 0));

        return (*this);
    }
    inline MMapBase &operator >> (float &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (float), 1);
        else
            rhs = static_cast<float>
                (strtod (get_string ("0123456789.").c_str (), nullptr));

        return (*this);
    }
    inline MMapBase &operator >> (double &rhs) noexcept  {

        if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
            _file_flags & _s_bappend_)
            read (&rhs, sizeof (double), 1);
        else
            rhs = strtod (get_string ("0123456789.").c_str (), nullptr);

        return (*this);
    }

private:

    size_type   buffer_size_ { 0 };

    inline bool check_space_4_printf_ () noexcept;

protected:

    class   AutoFileDesc  {

    public:

        inline AutoFileDesc (MMapBase &mmap_file) noexcept
            : mmap_file_ (mmap_file), should_close_ (false)  {

            if (! mmap_file_.is_open ())  {
                mmap_file_.open ();
                should_close_ = true;
            }
        }

        inline ~AutoFileDesc () noexcept  {

            if (should_close_)  mmap_file_.close ();
        }

    private:

        bool        should_close_ { false };
        MMapBase    &mmap_file_;

        AutoFileDesc ();
    };

    int         _file_desc { 0 };
    flag_type   _file_open_flags { 0 };
    mode_t      _file_open_mode { 0 };
    int         _mmap_mode { 0 };
    int         _mmap_prot { 0 };
    void        *_mmap_ptr { nullptr };
    size_type   _mmap_size { 0 };
    flag_type   _mmap_flags { MAP_SHARED };

    virtual bool _translate_open_mode () noexcept;
    virtual bool _initial_map (size_type file_size,
                               int mmap_prot,
                               flag_type mmap_flags,
                               int file_desc,
                               off_t  offset = 0,
                               void *start = nullptr) noexcept;
    virtual bool _initial_map_posthook () = 0;

    inline void *_get_base_ptr () const noexcept  { return (_mmap_ptr); }
    inline int _get_file_desc () const noexcept  { return (_file_desc); }
    inline void _set_file_open_mode(mode_t m) noexcept { _file_open_mode = m; }

public:

    inline flag_type
    get_file_open_flags () const noexcept  { return (_file_open_flags); }
    inline mode_t
    get_file_open_mode () const noexcept  { return (_file_open_mode); }
    inline size_type
    get_buffer_size () const noexcept  { return (buffer_size_); }
    inline size_type
    get_mmap_size () const noexcept  { return (_mmap_size); }

    virtual void unlink ()  {

        throw std::runtime_error ("MMapBase::unlink() is not implemented.");
    }
    virtual bool clobber ()  { return (! close ()); }
};

} // namespace hmdf

#endif // _WIN32

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
