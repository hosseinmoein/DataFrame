// Hossein Moein
// September 21, 2007
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <DMScu_FileBase.h>

// ----------------------------------------------------------------------------

bool DMScu_FileBase::_translate_open_mode () throw ()  {

    switch (_get_open_mode ())  {

        case _read_:
            _file_flags = _s_read_ | _in_use_;
            break;
        case _bread_:
            _file_flags = _s_bread_ | _in_use_;
            break;
        case _write_:
            _file_flags = _s_write_ | _s_read_ | _in_use_;
            break;
        case _bwrite_:
            _file_flags = _s_bwrite_ | _s_bread_ | _in_use_;
            break;
        case _append_:
            _file_flags = _s_append_ | _s_read_ | _in_use_;
            break;
        case _bappend_:
            _file_flags = _s_bappend_ | _s_bread_ | _in_use_;
            break;
    }

    return (true);
}

// ----------------------------------------------------------------------------

bool DMScu_FileBase::open ()  {

    if (is_open ())
        throw DMScu_Exception ("DMScu_FileBase::open(): "
                               "The device is already open");

    const   char    *om =
        (_file_flags & _s_read_ && ! (_file_flags & _s_write_ ||
                                      _file_flags & _s_append_) ? "r"
            : (_file_flags & _s_bread_ && ! (_file_flags & _s_bwrite_ ||
                                             _file_flags & _s_bappend_) ? "rb"
                   : (_file_flags & _s_write_ ? "w+"
                         : (_file_flags & _s_bwrite_ ? "wb+"
                               : (_file_flags & _s_append_ ? "a+"
                                     : (_file_flags & _s_bappend_ ? "ab+"
                                           : "r"))))));

    if (_file_flags & _already_opened_)
        if (_file_flags & _s_write_)
            om = "r+";
        else if (_file_flags & _s_bwrite_)
            om = "rb+";

    if ((stream_ = ::fopen (get_file_name (), om)) != NULL)  {
        struct  stat    stat_data;

        if (! ::stat (get_file_name (), &stat_data))
            _file_size = stat_data.st_size;
        else  {
            DMScu_FixedSizeString<4095> err;

            err.printf ("DMScu_FileBase::open(): ::stat(): (%d) %s --- %s",
                        errno, strerror (errno), get_file_name ());

            close ();
            stream_ = NULL;
            throw DMScu_Exception (err.c_str ());
        }
    }
    else  {
        DMScu_FixedSizeString<4095> err;

        err.printf ("DMScu_FileBase::open(): ::fopen(): (%d) %s --- %s (%s)",
                    errno, strerror (errno), get_file_name (), om);
        throw DMScu_Exception (err.c_str ());
    }

    _current_offset = 0;
    _set_buffer (buffer_size_);

    if (_file_flags & _s_append_ || _file_flags & _s_bappend_)  {
        if (::fseek (stream_, 0, SEEK_END) < 0 )  {
            DMScu_FixedSizeString<2047> err;

            err.printf ("DMScu_FileBase::open(): ::fseek(): (%d) %s",
                        errno, strerror (errno));
            throw DMScu_Exception (err.c_str ());
        }
        _current_offset = _file_size;
    }

    _file_flags |= _in_use_;
    _file_flags |= _already_opened_;
    _good_flag = true;
    return (true);
}

// ----------------------------------------------------------------------------

void DMScu_FileBase::_set_buffer (size_type bs)  {

    buffer_size_ = bs;

    if (file_buffer_)  {
        delete[] file_buffer_;
        file_buffer_ = NULL;
    }
    file_buffer_ = (buffer_size_ == 0) ? NULL : new char [buffer_size_];
    // if (file_buffer_)
    //     ::memset (file_buffer_, 0, buffer_size_);

    const   int type = (buffer_size_ == 0) ? _IONBF : _IOFBF;

    if (::setvbuf (stream_, file_buffer_, type, buffer_size_) != 0)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_FileBase::_set_buffer(): ::setvbuf(): (%d) %s",
                    errno, strerror (errno));
        throw DMScu_Exception (err.c_str ());
    }

    return;
}

// ----------------------------------------------------------------------------

DMScu_FileBase::size_type DMScu_FileBase::
read (void *data_ptr, size_type element_size, size_type element_count)
    throw ()  {

    size_type   read_size = element_count;

    if (_current_offset + element_size * read_size < _file_size)
        _current_offset += element_size * read_size;
    else  {
        read_size = (_file_size - _current_offset) / element_size;
        _current_offset = _file_size;
    }

    if (::fread (data_ptr, element_size, read_size, stream_) != read_size)  {
        // DMScu_FixedSizeString<2047> err;

        // err.printf ("DMScu_FileBase::read(): ::fread(): (%d) %s",
        //             errno, strerror (errno));
        // throw DMScu_Exception (err.c_str ());
        _good_flag = false;
        return (NOVAL);
    }

    _file_flags |= _touched_;
    return (read_size);
}

// ----------------------------------------------------------------------------

DMScu_FileBase::size_type DMScu_FileBase::
write (const void *data_ptr, size_type element_size, size_type element_count) {

    const   flag_type   flag =
        _s_bwrite_ | _s_bappend_ | _s_write_ | _s_append_;

    if (! (_file_flags & flag))  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_FileBase::write(): "
                    "Bad file permission for the action requested.");
        throw DMScu_Exception (err.c_str ());
    }

    if (::fwrite (data_ptr,
                  element_size,
                  element_count,
                  stream_) != element_count)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_FileBase::write(): ::fwrite(): (%d) %s",
                    errno, strerror (errno));
        throw DMScu_Exception (err.c_str ());
    }

    const   size_type   byte_count = element_size * element_count;
    const   size_type   growth = _current_offset + byte_count;

    if (growth > _file_size)
        _file_size += growth - _file_size;
    _current_offset += byte_count;
    _file_flags |= _written_;

    return (element_count);
}

// ----------------------------------------------------------------------------

int DMScu_FileBase::close ()  {

    if (::fclose (stream_) != 0)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_FileBase::close(): ::fclose(): (%d) %s",
                    errno, strerror (errno));
        throw DMScu_Exception (err.c_str ());
    }

    stream_ = NULL;
    _file_flags &= ~_in_use_;
    _current_offset = 0;
    _file_size = 0;

    delete[] file_buffer_;
    file_buffer_ = NULL;

    return (0);
}

// ----------------------------------------------------------------------------

void DMScu_FileBase::unlink ()  {

    if (is_open ())
        close ();

    if (::unlink (get_file_name ()) < 0)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_FileBase::unlink(): ::unlink(): (%d) %s",
                    errno, strerror (errno));
        throw DMScu_Exception (err.c_str ());
    }

    return;
}

// ----------------------------------------------------------------------------

int DMScu_FileBase::put_back (unsigned char c)  {

    if (_current_offset == 0)  {
        DMScu_FixedSizeString<511> err;

        err.printf ("DMScu_FileBase::put_back(): "
                    "Trying to pass the edge of the file. Under flow");
        throw DMScu_Exception (err.c_str ());
    }

    _current_offset -= 1;
    return (::ungetc (c, stream_));
}

// ----------------------------------------------------------------------------

int DMScu_FileBase::get_char () throw ()  {

    const   int rc = ::fgetc (stream_);

    _current_offset += (rc == EOF) ? 0 : 1;
    return (rc);
}

// ----------------------------------------------------------------------------

std::string DMScu_FileBase::get_string (const char *search_str) throw ()  {

    std::string slug;

    if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
        _file_flags & _s_bappend_)  {
        return (slug);
    }

   // _current_offset is incremented in get_char()
   //
    while (_current_offset < _file_size)  {
        const   char    c = static_cast<const char>(get_char ());

        if (c == EOF)
            break;

        if (! _is_in_list (c , search_str))  {
            put_back (c);
            break;
        }
        slug += c;
    }

    return (slug);
}

// ----------------------------------------------------------------------------

std::string DMScu_FileBase::get_token (const char *delimit_str) throw ()  {

    std::string slug;

    if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
        _file_flags & _s_bappend_)  {
        return (slug);
    }

   // _current_offset is incremented in get_char()
   //
    while (_current_offset < _file_size)  {
        const   char    c = static_cast<const char>(get_char ());

        if (c == EOF)
            break;

        if (_is_in_list (c , delimit_str))
            break;
        slug += c;
    }

    return (slug);
}

// ----------------------------------------------------------------------------

DMScu_FileBase::size_type
DMScu_FileBase::get_token (char delimit, char *buffer) throw ()  {

    if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
        _file_flags & _s_bappend_)  {
        return (NOVAL);
    }

    size_type   counter = 0;

   // _current_offset is incremented in get_char()
   //
    while (_current_offset < _file_size)  {
        const   char    c = static_cast<const char>(get_char ());

        if (c == EOF)
            break;

        if (c == delimit)
            break;

        buffer [counter] = c;
        counter += 1;
    }
    buffer [counter] = 0;

    return (counter);
}

// ----------------------------------------------------------------------------

int DMScu_FileBase::printf (const char *format_str, ...) throw ()  {

    va_list argument_ptr;

    va_start (argument_ptr, format_str);

    const   int rc = ::vfprintf (stream_, format_str, argument_ptr);

    if (rc < 0)  {
        _good_flag = false;
        return (-1);
    }

    va_end (argument_ptr);

    if (rc > 0)  {
        _file_flags |= _written_;
        _current_offset += rc;
        _file_size =
            _current_offset > _file_size ? _current_offset : _file_size;
    }

    return (rc);
}

// ----------------------------------------------------------------------------

int DMScu_FileBase::put_char (int the_char)  {

    const   char    tmp_char = the_char;

    if (write (&tmp_char, sizeof (char), 1) == 1)
        return (the_char);

    return (EOF);
}

// ----------------------------------------------------------------------------

int DMScu_FileBase::put_string (const char *the_str) throw ()  {

    return (printf ("%s", the_str));
}

// ----------------------------------------------------------------------------

// Go to the 0-based line
//
DMScu_FileBase::size_type DMScu_FileBase::go_to_line (size_type line)  {

   // It does not make sense to go to a particular line
   // in a binary file
   //
    if (_file_flags & _s_bwrite_ || _file_flags & _s_bappend_ ||
        _file_flags & _s_bread_)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_FileBase::go_to_line(): "
                    "Bad file permission for the action requested.");
        throw DMScu_Exception (err.c_str ());
    }

    size_type   counter = 0;

    seek (0, _seek_set_);
    while (_current_offset < _file_size && counter < line)  {
        const   char    c = static_cast<const char>(get_char ());

        if (c == EOF)
            break;
        else if (c == '\n')
            counter += 1;
    }

    return (_current_offset);
}

// ----------------------------------------------------------------------------

int DMScu_FileBase::seek (size_type the_offset, SEEK_TYPE seek_type)
    throw ()  { // Just for maximum efficiency

    switch (seek_type)  {

        case _seek_set_:
        {
            if (the_offset > _file_size)  {
                _good_flag = false;
                return (-1);
            }
            break;
        }
        case _seek_cur_:
        {
            the_offset += _current_offset;
            if (the_offset > _file_size)  {
                _good_flag = false;
                return (-1);
            }
            break;
        }
        case _seek_end_:
        {
            if (the_offset > _file_size)  {
                _good_flag = false;
                return (-1);
            }
            else
                the_offset = _file_size - the_offset;
            break;
        }
    }

    if (::fseek (stream_, the_offset, SEEK_SET) < 0 )  {
        _good_flag = false;
        return (-1);
    }
    _current_offset = the_offset;

    return (0);
}

// ----------------------------------------------------------------------------

int DMScu_FileBase::truncate (size_type truncate_size)  {

    const   flag_type   flag =
        _s_bwrite_ | _s_bappend_ | _s_write_ | _s_append_;

    if (! is_open () || ! (_file_flags & flag))  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_FileBase::truncate(): "
                    "Bad file permission for the action requested.");
        throw DMScu_Exception (err.c_str ());
    }

    if (::ftruncate (_get_file_desc (), truncate_size) < 0)  {
        DMScu_FixedSizeString<2047> err;

        err.printf ("DMScu_FileBase::truncate(): ::ftruncate(): (%d) %s",
                    errno, strerror (errno));
        throw DMScu_Exception (err.c_str ());
    }

    _file_size = truncate_size;
    seek (_current_offset > truncate_size ? truncate_size : _current_offset,
          _seek_set_);

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
