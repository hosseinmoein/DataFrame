// Hossein Moein
// May 28, 2019
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _WIN32

#pragma once

#include <DataFrame/MMap/FileDef.h>
#include <DataFrame/FixedSizeString.h>

#include <iostream>
#include <string>
#include <stdarg.h>
#include <sys/fcntl.h>
#include <errno.h>
#include <strings.h>
#include <stdexcept>

// ----------------------------------------------------------------------------

namespace hmdf
{
	
class   FileBase : public FileDef  {

public:

    FileBase() = delete;
    inline FileBase (const char *file_name,
                     OPEN_MODE open_mode,
                     size_type buffer_size = 0UL)
        : FileDef (file_name, open_mode, _stream_file_),
          buffer_size_ (buffer_size)  {

        _translate_open_mode ();
        open ();
    }

    virtual ~FileBase ()  { if (is_open ()) close (); }

    inline bool is_open () const noexcept  { return (stream_ != nullptr); }

    bool open ();
    int close ();
    size_type read (void *, size_type, size_type) noexcept;
    size_type write (const void *, size_type, size_type);

    inline int flush () noexcept  {

        const flag_type flag =
            _in_use_ | _s_bwrite_ | _s_write_ | _s_append_ | _s_bappend_;

        if (_file_flags & flag)  {
            ::fflush (stream_);
            return (0);
        }

        return (EOF);
    }

    int get_char (void) noexcept;
    inline flag_type get_line (char *buffer)  {

        return (get_token ('\n', buffer));
    }
    std::string get_string (const char *search_str) noexcept;
    std::string get_token (const char *delimit) noexcept;
    size_type get_token (char delimit, char *buffer) noexcept;
    int printf (const char *, ...) noexcept;
    int put_char (int);
    int put_string (const char *) noexcept;
    int put_back (unsigned char c);

    inline void rewind (void) noexcept  { seek (0L, _seek_set_); }
    int seek (size_type, SEEK_TYPE seek_type) noexcept;
    size_type go_to_line (size_type);

    int truncate (size_type);

    inline FileBase &operator << (char rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (char), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%c", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (char): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (unsigned char rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof(unsigned char), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%c", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (const unsigned char): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (short rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (short), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%hd", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (const short): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (unsigned short rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned short), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%hu", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (const unsigned short): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%d", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (unsigned int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%u", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (unsigned int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (long int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (long int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%ld", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (long int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (long long int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (long long int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%lld", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (long long int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (unsigned long int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned long int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%lu", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (unsigned long int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (unsigned long long int rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (unsigned long long int), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%llu", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (unsigned long long int): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (float rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (float), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset +=
                ::fprintf (stream_, "%.*f", _precision, rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (const float): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (double rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (double), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset +=
                ::fprintf (stream_, "%.*lf", _precision, rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (const double): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &operator << (const char *rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (char), ::strlen (rhs));
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%s", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (const char *): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }
    inline FileBase &
    operator << (const std::string &rhs)  { return (*this << rhs.c_str ()); }
    inline FileBase &
    operator << (const VirtualString &rhs)  { return (*this << rhs.c_str ()); }
    inline FileBase &operator << (const void *rhs)  {

        if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
            write (&rhs, sizeof (void *), 1);
        else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
            _current_offset += ::fprintf (stream_, "%p", rhs);
            _file_size = _current_offset > _file_size ? _current_offset
                                                      : _file_size;
        }
        else  {
            String1K    err;

            err.printf ("FileBase::<< (const void *): "
                        "Bad file permission for the action requested.");
            throw std::runtime_error (err.c_str ());
        }

        return (*this);
    }

    inline FileBase &operator >> (char *rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (rhs, sizeof (char), _width);
            else  {
                ::fscanf (stream_, "%s", rhs);
                _current_offset += ::strlen (rhs);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (std::ostream &rhs)   {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
                _file_flags & _s_append_)
                rhs << get_token (" ").data ();
            else
                _good_flag = false;
        }

        return (*this);
    }
    inline FileBase &operator >> (std::string &rhs)   {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
                _file_flags & _s_append_)
                rhs += get_token (" ").c_str ();
            else
                _good_flag = false;
        }

        return (*this);
    }
    inline FileBase &operator >> (VirtualString &rhs)  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
                _file_flags & _s_append_)
                rhs += get_token (" ").c_str ();
            else
                _good_flag = false;
        }

        return (*this);
    }
    inline FileBase &operator >> (char &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (char), 1);
            else  {
                ::fscanf (stream_, "%c", &rhs);
                _current_offset += 1;
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (unsigned char &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (unsigned char), 1);
            else  {
                ::fscanf (stream_, "%hhi", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (short &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (short), 1);
            else  {
                ::fscanf (stream_, "%hi", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (int &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (int), 1);
            else  {
                ::fscanf (stream_, "%i", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (long int &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (long int), 1);
            else  {
                ::fscanf (stream_, "%ld", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (long long int &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (long long int), 1);
            else  {
                ::fscanf (stream_, "%lld", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (unsigned short &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (unsigned short), 1);
            else  {
                ::fscanf (stream_, "%hu", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (unsigned int &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (unsigned int), 1);
            else  {
                ::fscanf (stream_, "%u", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (unsigned long int &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (unsigned long int), 1);
            else  {
                ::fscanf (stream_, "%lu", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (unsigned long long int &rhs)
        noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (unsigned long long int), 1);
            else  {
                ::fscanf (stream_, "%llu", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (float &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (float), 1);
            else  {
                ::fscanf (stream_, "%f", &rhs);
                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }
    inline FileBase &operator >> (double &rhs) noexcept  {

        if (_current_offset > _file_size)  {
            _good_flag = false;
        }
        else  {
            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_)
                read (&rhs, sizeof (double), 1);
            else  {
                int res = ::fscanf (stream_, "%lf", &rhs);

                _current_offset = ::ftell (stream_);
            }
        }

        return (*this);
    }

private:

    FILE        *stream_ { nullptr };
    char        *file_buffer_ { nullptr };
    size_type   buffer_size_ { 0 };

protected:

    bool _translate_open_mode () noexcept;
    void _set_buffer (size_type bs);

    inline int _get_file_desc () const noexcept  {

        return (is_open () ? ::fileno (stream_) : -1);
    }

public:

    inline size_type get_buffer_size() const noexcept { return(buffer_size_); }

    void unlink ();
};

} // namespace hmdf

#endif // _WIN32

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
