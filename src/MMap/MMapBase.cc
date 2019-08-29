// Hossein Moein
// May 28, 2019
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _WIN32

#include <DataFrame/MMap/MMapBase.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

const MMapBase::size_type   MMapBase::SYSTEM_PAGE_SIZE =
    ::sysconf (_SC_PAGESIZE);

static const MMapBase::size_type    _buff_size_ = 64 * 1024;

// ----------------------------------------------------------------------------

bool MMapBase::_translate_open_mode () noexcept  {

    switch (_get_open_mode ())  {
        case _read_:
        {
            _file_flags = _s_read_ | _in_use_;
            _mmap_mode = PROT_READ;
            _mmap_prot = PROT_READ;
            _file_open_flags = O_RDONLY | O_CREAT | O_SYNC;
            break;
        }
        case _bread_:
        {
            _file_flags = _s_bread_ | _in_use_;
            _mmap_mode = PROT_READ;
            _mmap_prot = PROT_READ;
            _file_open_flags = O_RDONLY | O_CREAT | O_SYNC;
            break;
        }
        case _write_:
        {
            _file_flags = _s_write_ | _s_read_ | _in_use_;
            _mmap_mode = PROT_WRITE | PROT_READ;
            _mmap_prot = PROT_WRITE | PROT_READ;
            _file_open_flags = O_RDWR | O_CREAT | O_SYNC;
            break;
        }
        case _bwrite_:
        {
            _file_flags = _s_bwrite_ | _s_bread_ | _in_use_;
            _mmap_mode = PROT_WRITE | PROT_READ;
            _mmap_prot = PROT_WRITE | PROT_READ;
            _file_open_flags = O_RDWR | O_CREAT | O_SYNC;
            break;
        }
        case _append_:
        {
            _file_flags = _s_append_ | _s_read_ | _in_use_;
            _mmap_mode = PROT_WRITE | PROT_READ;
            _mmap_prot = PROT_WRITE | PROT_READ;
            _file_open_flags = O_RDWR | O_APPEND | O_CREAT | O_SYNC;
            break;
        }
        case _bappend_:
        {
            _file_flags = _s_bappend_ | _s_bread_ | _in_use_;
            _mmap_mode = PROT_WRITE | PROT_READ;
            _mmap_prot = PROT_WRITE | PROT_READ;
            _file_open_flags = O_RDWR | O_APPEND | O_CREAT | O_SYNC;
            break;
        }
    }

    return (true);
}

// ----------------------------------------------------------------------------

bool MMapBase::_initial_map (size_type file_size,
                             int mmap_prot,
                             flag_type mmap_flags,
                             int file_desc,
                             off_t offset,
                             void *start) noexcept  {

    if (file_size)  {
        _mmap_ptr =
            ::mmap(start, file_size, mmap_prot, mmap_flags, file_desc, offset);

        if (_mmap_ptr != MAP_FAILED)  {
            _mmap_size = file_size;
            _file_desc = file_desc;
            if (_get_open_mode () == _append_ || _get_open_mode() == _bappend_)
                _current_offset = _mmap_size;
        }
        else  {
            ::close (file_desc);
            _file_desc = 0;

            return (_good_flag = false);
        }
    }

    return (_good_flag = _initial_map_posthook ());
}

// ----------------------------------------------------------------------------

MMapBase::size_type MMapBase::
read (void *data_ptr, size_type data_size, size_type data_count) noexcept  {

    size_type       read_size = data_size * data_count;
    const void      *cpy_from =
         reinterpret_cast<char *>(_mmap_ptr) + _current_offset;

    if (_current_offset + read_size < _file_size)
        _current_offset += read_size;
    else  {
        data_count = (_file_size - _current_offset) / data_size;
        read_size = data_size * data_count;
        _current_offset = _file_size;
    }

    ::memcpy (data_ptr, cpy_from, read_size);
    _file_flags |= _touched_;
    return (data_count);
}

// ----------------------------------------------------------------------------

MMapBase::size_type MMapBase::
write (const void *data_ptr, size_type data_size, size_type data_count)  {

    const flag_type flag = _s_bwrite_ | _s_bappend_ | _s_write_ | _s_append_;

    if (! (_file_flags & flag))  {
        String2K    err;

        err.printf ("MMapBase::write(): "
                    "Bad file permission for the action requested.");
        throw std::runtime_error (err.c_str ());
    }

    const size_type byte_count = data_size * data_count;
    const size_type growth = _current_offset + byte_count;

    if (growth > _mmap_size)  {
        const size_type tp_add = growth - _mmap_size;
        const size_type new_size =
            _mmap_size +
                ((! get_buffer_size ())
                    ? tp_add
                    : (((tp_add / get_buffer_size()) * get_buffer_size()) +
                       (tp_add % get_buffer_size() ? get_buffer_size() : 0)));

        const AutoFileDesc  desc_guard (*this);
        void *const         tmp_mmap_ptr =
            ::mmap(nullptr, new_size, _mmap_prot, _mmap_flags, _file_desc, 0);

        if (tmp_mmap_ptr == MAP_FAILED)  {
            String2K    err;

            err.printf ("MMapBase::write(): ::mmap(): (%d) %s",
                        errno, ::strerror (errno));
            throw std::runtime_error (err.c_str ());
        }

        if (_mmap_ptr)
            ::munmap (_mmap_ptr, _mmap_size);

        if (::ftruncate (_file_desc, new_size) < 0)  {
            String2K    err;

            err.printf ("MMapBase::write(): ::ftruncate(): (%d) %s",
                        errno, ::strerror (errno));

            throw std::runtime_error (err.c_str ());
        }

        _mmap_size = new_size;
        _mmap_ptr = tmp_mmap_ptr;
    }

    ::memcpy (reinterpret_cast<char *>(_mmap_ptr) + _current_offset,
              data_ptr,
              byte_count);

    _file_size += growth > _file_size ? growth - _file_size : 0;
    _file_flags |= _written_;
    _current_offset += byte_count;

    return (data_count);
}

// ----------------------------------------------------------------------------

int MMapBase::close (CLOSE_MODE close_mode)  {

    if (_file_flags & _in_use_)  {
        const flag_type wflag =
            _s_bwrite_ | _s_write_ | _s_append_ | _s_bappend_;
        const size_type length =
            close_mode == _normal_ ? _file_size : _current_offset;

        if (_mmap_ptr && _file_size && _file_flags & wflag)
            ::msync (_mmap_ptr, length, MS_SYNC);

        if (_mmap_ptr && ::munmap (_mmap_ptr, _mmap_size) != 0)  {
            String2K    err;

            err.printf ("MMapBase::close(): ::munmap(): (%d) %s",
                        errno, ::strerror (errno));
            throw std::runtime_error (err.c_str ());
        }

        if (is_open ())  {
            if (_file_flags & wflag)  {
                if (::ftruncate (_file_desc, length) < 0)  {
                    String2K    err;

                    err.printf ("MMapBase::close(): ftruncatie() failed ");
                    throw std::runtime_error (err.c_str ());
                }
                ::fsync (_file_desc);
            }

            if (::close (_file_desc) != 0)  {
                String2K    err;

                err.printf ("MMapBase::close(): ::close(): (%d) %s",
                            errno, ::strerror (errno));
                throw std::runtime_error (err.c_str ());
            }

            _file_desc = 0;
        }

        _file_flags &= ~_in_use_;
        _current_offset = 0;
        _file_size = 0;
        _mmap_ptr = nullptr;
    }
    else
        return (EOF);

    return (0);
}

// ----------------------------------------------------------------------------

int MMapBase::put_back ()  {

    if (_current_offset == 0)  {
        String2K    err;

        err.printf ("MMapBase::put_back(): "
                    "Trying to pass the edge of the file. Under flow");
        throw std::runtime_error (err.c_str ());
    }

    return (*(reinterpret_cast<char *>(_mmap_ptr) + --_current_offset));
}

// ----------------------------------------------------------------------------

int MMapBase::get_char () noexcept  {

    if (_current_offset < _file_size)
        return (*(reinterpret_cast<char *>(_mmap_ptr) + _current_offset++));
    else
        return (-1);
}

// ----------------------------------------------------------------------------

std::string MMapBase::get_string (const char *search_str) noexcept  {

    std::string s;

    if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
        _file_flags & _s_bappend_)  {
        return (s);
    }

    while (_current_offset < _file_size)  {
        if (! _is_in_list (
                  *(reinterpret_cast<char *>(_mmap_ptr) + _current_offset),
                  search_str))  {
            _current_offset += 1;
            break;
        }
        s.append (reinterpret_cast<char *>(_mmap_ptr) + _current_offset++, 1);
    }

    return (s);
}

// ----------------------------------------------------------------------------

std::string MMapBase::get_token (const char *delimit_str) noexcept  {

    std::string s;

    if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
        _file_flags & _s_bappend_)
        return (s);

    while (_current_offset < _file_size)  {
        if (_is_in_list (
                *(reinterpret_cast<char *>(_mmap_ptr) + _current_offset),
                delimit_str))  {
            _current_offset += 1;
            break;
        }
        s.append (reinterpret_cast<char *>(_mmap_ptr) + _current_offset++, 1);
    }

    return (s);
}

// ----------------------------------------------------------------------------

MMapBase::size_type
MMapBase::get_token (char delimit, char *buffer) noexcept  {

    if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
        _file_flags & _s_bappend_)
        return (NOVAL);

    flag_type   counter = 0;

    while (_current_offset < _file_size)  {
        if (*(reinterpret_cast<char *>(_mmap_ptr) + _current_offset) ==
                delimit)  {
            _current_offset += 1;
            break;
        }
        buffer [counter++] =
            *(reinterpret_cast<char *>(_mmap_ptr) + _current_offset++);
    }
    buffer [counter] = 0;

    return (counter);
}

// ----------------------------------------------------------------------------

inline bool MMapBase::check_space_4_printf_ () noexcept  {

    const flag_type flag = _s_write_ | _s_append_ | _in_use_;

    if (_file_flags & flag)  {
        const size_type buffer_size =
            get_buffer_size() > BUFFER_SIZE ? get_buffer_size () : BUFFER_SIZE;
        const size_type new_size = _mmap_size - _current_offset;

        if (new_size < MIN_BUFFER_SIZE)  {
            truncate (_mmap_size + buffer_size);
            _file_size = _current_offset;
        }
        return (true);
    }

    return (false);
}

// ----------------------------------------------------------------------------

int MMapBase::printf (const char *format_str, ...) noexcept  {

    int     char_count = 0;
    va_list argument_ptr;

    if (check_space_4_printf_ ())  {
        va_start (argument_ptr, format_str);

        char_count =
            ::vsprintf(reinterpret_cast<char *>(_mmap_ptr) + _current_offset,
                       format_str,
                       argument_ptr);

        va_end (argument_ptr);
        if (char_count > 0)  {
            _file_flags |= _written_;
            _current_offset += char_count;
            _file_size =
                _current_offset > _file_size ? _current_offset : _file_size;
        }
    }

    return (char_count);
}

// ----------------------------------------------------------------------------

int MMapBase::put_char (int the_char)  {

    const char  tmp_char = the_char;

    write (&tmp_char, sizeof (char), 1);
    return (the_char);
}

// ----------------------------------------------------------------------------

int MMapBase::put_string (const char *the_str) noexcept  {

    return (printf ("%s", the_str));
}

// ----------------------------------------------------------------------------

int MMapBase::remap (size_type offset, size_type map_size)  {

    const AutoFileDesc  desc_guard (*this);
    struct stat         stat_data;

    if (::fstat (_file_desc, &stat_data) < 0)  {
        String2K    err;

        err.printf ("MMapBase::remap(): ::fstat(): (%d) %s",
                    errno, ::strerror (errno));
        throw std::runtime_error (err.c_str ());
    }
    if (offset + map_size >= stat_data.st_size)  {
        String2K    err;

        err.printf ("MMapBase::remap(): offset %llu + map size %llu "
                    ">= file size %llu",
                    offset, map_size,
                    static_cast<size_type>(stat_data.st_size));
        throw std::runtime_error (err.c_str ());
    }

    void    *old_mmap_ptr = _mmap_ptr;

    if (_mmap_ptr)
        munmap (old_mmap_ptr, _mmap_size);

    _mmap_size = 0;
    _mmap_ptr = nullptr;

    const size_type useable_offset =
        (offset / SYSTEM_PAGE_SIZE) * SYSTEM_PAGE_SIZE;
    const size_type size_to_do =
        (map_size > 0) ? map_size : stat_data.st_size - useable_offset;
    void            *tmp_mmap_ptr = ::mmap (old_mmap_ptr,
                                            size_to_do,
                                            _mmap_prot,
                                            _mmap_flags,
                                            _file_desc,
                                            useable_offset);

    if (tmp_mmap_ptr != MAP_FAILED)  {
        _mmap_size = size_to_do;
        _mmap_ptr = tmp_mmap_ptr;
        _current_offset =
            (_current_offset < useable_offset)
                 ? 0 : (_current_offset > _mmap_size)
                            ? _mmap_size : _current_offset;
    }
    else  {
        String2K    err;

        err.printf ("MMapBase::remap(): ::mmap(): (%d) %s",
                    errno, ::strerror (errno));
        throw std::runtime_error (err.c_str ());
    }

    return (0);
}

// ----------------------------------------------------------------------------

// Go to the 0-based line
//
MMapBase::size_type MMapBase::go_to_line (size_type line)  {

   // It does not make sense to go to a particular line
   // in a binary file
   //
    if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_) ||
         (_file_flags & _s_bread_))  {
        String2K    err;

        err.printf ("MMapBase::go_to_line(): "
                    "Bad file permission for the action requested.");
        throw std::runtime_error (err.c_str ());
    }

    const char  *str = reinterpret_cast<char *>(_mmap_ptr);
    size_type   curr_line = 0;
    size_type   offset;

    for (offset = 0; offset < _file_size; ++offset)  {
        if (curr_line == line)
            break;
        if (str [offset] == '\n')
            curr_line += 1;
    }

    seek (offset, _seek_set_);
    return (_current_offset);
}

// ----------------------------------------------------------------------------

int MMapBase::
seek (size_type the_offset, SEEK_TYPE seek_type) noexcept  {

    switch (seek_type)  {
        case _seek_set_:
        {
            if (the_offset <= _file_size)
                _current_offset = the_offset;
            else  {
                _good_flag = false;
                return (-1);
            }
            break;
        }
        case _seek_cur_:
        {
            the_offset += _current_offset;
            if (the_offset <= _file_size)
                _current_offset = the_offset;
            else  {
                _good_flag = false;
                return (-1);
            }
            break;
        }
        case _seek_end_:
        {
            if (the_offset <= _file_size)
                _current_offset = _file_size - the_offset;
            else  {
                _good_flag = false;
                return (-1);
            }
            break;
        }
    }

    return (0);
}

// ----------------------------------------------------------------------------

int MMapBase::set_flag (int mmap_prot, flag_type mmap_flags)  {

    if ((_mmap_prot != mmap_prot) || (_mmap_flags != mmap_flags))  {
        _mmap_prot  = mmap_prot;
        _mmap_flags = mmap_flags;
        if (_mmap_ptr)  {
            munmap (_mmap_ptr, _mmap_size);
            _mmap_ptr = nullptr;

            const AutoFileDesc  desc_guard (*this);

            _mmap_ptr = ::mmap (nullptr,
                                _mmap_size,
                                _mmap_prot,
                                _mmap_flags,
                                _file_desc,
                                0);
            if (_mmap_ptr == MAP_FAILED)  {
                String2K    err;

                err.printf ("MMapBase::set_flag(): ::mmap(): (%d) %s",
                            errno, ::strerror (errno));

                throw std::runtime_error (err.c_str ());
            }
        }
    }

    return (0);
}

// ----------------------------------------------------------------------------

int MMapBase::truncate (size_type truncate_size)  {

    const flag_type flag = _s_bwrite_ | _s_bappend_ | _s_write_ | _s_append_;

    if (! (_file_flags & flag))  {
        String2K    err;

        err.printf ("MMapBase::truncate(): "
                    "Bad file permission for the action requested.");
        throw std::runtime_error (err.c_str ());
    }

    if (truncate_size == _file_size)
        return (0);

    void    *old_mmap_ptr = nullptr;

    if (_mmap_ptr)  {
        old_mmap_ptr  = _mmap_ptr;
        ::munmap (old_mmap_ptr, _mmap_size);
    }

    _mmap_size = 0;
    _mmap_ptr = nullptr;

    const AutoFileDesc  desc_guard (*this);
    void                *tmp_mmap_ptr = nullptr;

    if (truncate_size != 0)  {
        tmp_mmap_ptr = ::mmap (old_mmap_ptr,
                               truncate_size,
                               _mmap_prot,
                               _mmap_flags,
                               _file_desc,
                               0);

        if (tmp_mmap_ptr == MAP_FAILED)  {
            String2K    err;

            err.printf ("MMapBase::truncate(): ::mmap(): (%d) %s",
                        errno, ::strerror (errno));
            throw std::runtime_error (err.c_str ());
        }
    }

    if (::ftruncate (_file_desc, truncate_size) < 0)  {
        String2K    err;

        err.printf ("MMapBase::truncate(): ::ftruncate(): (%d) %s",
                    errno, ::strerror (errno));
        throw std::runtime_error (err.c_str ());
    }

    _file_size = truncate_size;
    _mmap_size = truncate_size;
    _mmap_ptr  = tmp_mmap_ptr;
    _current_offset =
        _current_offset > truncate_size ? truncate_size : _current_offset;

    return (0);
}

// ----------------------------------------------------------------------------

MMapBase &MMapBase::operator << (std::ifstream &ifs)  {

    char    buffer [_buff_size_];

    if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))  {
        while (! ifs.eof ())  {
            ifs.read (buffer, _buff_size_);
            write (buffer, ifs.gcount (), 1);
        }
    }
    else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))  {
        while (! ifs.eof ())  {
            ifs.read (buffer, _buff_size_ - 1);
            buffer [ifs.gcount ()] = 0;
            printf ("%s", buffer);
        }
    }
    else  {
        String2K    err;

        err.printf ("MMapBase::<< (std::ifstream &): "
                    "Bad file permission for the action requested.");
        throw std::runtime_error (err.c_str ());
   }

    return (*this);
}

// ----------------------------------------------------------------------------

MMapBase &MMapBase::operator << (const FILE &fref)  {

    char    buffer[_buff_size_];

    if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))  {
        while (! feof (const_cast<FILE *>(&fref)))  {
            const int   sread =
                fread (buffer, _buff_size_, 1, const_cast<FILE *>(&fref));

            write (buffer, sread, 1);
        }
    }
    else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))  {
        while (! feof (const_cast<FILE *>(&fref)))  {
            const int   sread =
                fread (buffer, _buff_size_ - 1, 1, const_cast<FILE *>(&fref));

            buffer [sread] = 0;
            printf ("%s", buffer);
        }
    }
    else  {
        String2K    err;

        err.printf ("MMapBase::<< (const FILE &): "
                    "Bad file permission for the action requested.");
        throw std::runtime_error (err.c_str ());
    }

    return (*this);
}

} // namespace hmdf

#endif // _WIN32

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
