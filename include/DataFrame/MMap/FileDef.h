// Hossein Moein
// May 28, 2019
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _WIN32

#pragma once

#include <DataFrame/Utils/FixedSizeString.h>

#include <cstdio>
#include <cstdlib>

// ----------------------------------------------------------------------------

namespace hmdf
{

class   FileDef  {

public:

    enum    OPEN_MODE { _read_ = 1, _write_ = 2, _append_ = 4,
                        _bread_ = 8, _bwrite_ = 16, _bappend_ = 32 };
    enum    SEEK_TYPE { _seek_set_ = 1, _seek_cur_ = 2, _seek_end_ = 4 };
    enum    CLOSE_MODE { _normal_, _to_offset_ };
    enum    DEVICE_TYPE { _shared_memory_, _mmap_file_, _stream_file_ };

    typedef unsigned long long int  size_type;
    typedef unsigned int            flag_type;

    static const size_type  NOVAL = static_cast<size_type>(-1);

protected:

    static const size_type  MIN_BUFFER_SIZE = 65536LL;
    static const size_type  BUFFER_SIZE = MIN_BUFFER_SIZE * 16LL;

public:

    inline void
    set_width (flag_type the_width) noexcept  { _width = the_width; }
    inline void set_precision (short pre) noexcept  { _precision = pre; }
    inline bool is_ok () const noexcept  { return (_good_flag); }
    inline bool
    is_eof () const noexcept  { return (_current_offset >= _file_size); }
    inline short get_precision () const noexcept  { return (_precision); }
    inline size_type
    get_file_size () const noexcept  { return (_file_size); }
    inline DEVICE_TYPE
    get_device_type () const noexcept  { return (device_type_); }
    inline size_type tell () const noexcept  {

        return (_file_flags & _in_use_
                    ? _current_offset : static_cast<size_type>(-1));
    }
    inline const char *
    get_file_name () const noexcept  { return (file_name_.c_str ()); }

    FileDef () = delete;
    FileDef(const char *file_name, OPEN_MODE om, DEVICE_TYPE dt) noexcept
        : open_mode_ (om), device_type_ (dt), file_name_ (file_name)  {   }

protected:

    enum    STATE { _written_ = 64, _touched_ = 128, _in_use_ = 256,
                    _s_read_ = 1, _s_write_ = 2, _s_append_ = 4,
                    _s_bread_ = 8, _s_bwrite_ = 16, _s_bappend_ = 32,
                    _not_in_use_ = 0, _already_opened_ = 512 };

    flag_type   _width { ~0U };
    flag_type   _file_flags { _not_in_use_ };
    short       _precision { 6 };
    bool        _good_flag { false };
    size_type   _current_offset { 0 };
    size_type   _file_size { 0 };

    inline static bool
    _is_in_list (const char this_char, const char *char_list) noexcept  {

        const char  *str = char_list;

        while (*str)
            if (this_char ^ *str)
                str += 1;
            else
                return (true);

        return (false);
    }

    inline OPEN_MODE _get_open_mode() const noexcept  { return (open_mode_); }
    inline void _set_file_name(const char *n) noexcept  { file_name_ = n; }
    inline void _set_open_mode (OPEN_MODE o)  noexcept  { open_mode_ = o; }

private:

    OPEN_MODE           open_mode_;
    const DEVICE_TYPE   device_type_;
    String2K            file_name_;
};

} // namespace hmdf

#endif // _WIN32

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
