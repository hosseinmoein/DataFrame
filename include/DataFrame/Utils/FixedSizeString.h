// Hossein Moein
// July 17 2009
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

// This abstract base class makes it possible to pass different template
// instances around as one type and to be able to assign them interchangeably.
// The only penalty paid for having this base class is to carry around one
// additional (pointer size) member. There shouldn't be any performace
// penalty, since everything is still stack based and there is no virtuality.
//
// NOTE: VirtualString MAKES NO BOUNDARY CHECKS. IT IS THE RESPONSIBILITY
//       OF THE PROGRAMMER TO TAKE CARE OF THAT.
//
class   VirtualString  {

public:

    using size_type = size_t;
    using value_type = char;
    using pointer = value_type *;
    using const_pointer = const value_type *;
    using reference = value_type &;
    using const_reference = const value_type &;

    using iterator = pointer;
    using const_iterator = const_pointer;

    static const size_type  npos = static_cast<size_type>(-1);

    inline iterator begin () noexcept  { return (string_); }
    inline const_iterator begin () const noexcept  { return (string_); }

    // Unfortunately, the following two methods are not as cheap as they are
    // supposed to be.
    //
    inline iterator end () noexcept  { return (string_ + size ()); }
    inline const_iterator end () const noexcept  {

        return (string_ + size ());
    }

protected:

    inline VirtualString (pointer str) noexcept : string_ (str)  {  }

public:

    VirtualString () = delete;
    VirtualString (const VirtualString &) = delete;
    VirtualString (VirtualString &&) = delete;
    VirtualString &operator = (VirtualString &&) = delete;

    // Assignment methods.
    //
    inline VirtualString &operator = (const_pointer rhs) noexcept  {

        ::strcpy (string_, rhs);
        return (*this);
    }
    inline VirtualString &operator = (const VirtualString &rhs) noexcept  {

        return (*this = rhs.c_str ());
    }
    inline VirtualString &ncopy (const_pointer rhs, size_type len) noexcept  {

        ::strncpy (string_, rhs, len);
        string_ [len] = 0;
        return (*this);
    }

    //
    // Appending methods.
    //

    inline VirtualString &append (const_pointer rhs) noexcept  {

        ::strcat (string_, rhs);
        return (*this);
    }
    inline VirtualString &append (const VirtualString &rhs) noexcept  {

        return (append (rhs.c_str ()));
    }
    inline VirtualString &operator += (const_pointer rhs) noexcept  {

        return (append (rhs));
    }
    inline VirtualString &operator += (const VirtualString &rhs) noexcept  {

        return (append (rhs.c_str ()));
    }

    inline size_type
    find (const_reference token, size_type pos = 0) const noexcept  {

        size_type   counter = 0;

        for (const_pointer itr = &(string_ [pos]); *itr; ++itr, ++counter)
            if (string_ [pos + counter] == token)
                return (pos + counter);

        return (npos);
    }
    inline size_type
    find (const_pointer token, size_type pos = 0) const noexcept  {

        const size_type token_len = ::strlen (token);
        const size_type self_len = size ();

        if ((token_len + pos) > self_len)
            return (npos);

        size_type   counter = 0;

        for (const_pointer itr = &(string_ [pos]);
             itr + token_len - begin () <= static_cast<int>(self_len);
             ++itr, ++counter)
            if (! ::strncmp (token, itr, token_len))
                return (pos + counter);

        return (npos);
    }
    inline size_type
    find (const VirtualString &token, size_type pos = 0) const noexcept  {

        const size_type len = size ();

        return (find (token.c_str (), pos));
    }

    // Replaces the substring statring at pos with length n with s
    //
    inline VirtualString &
    replace (size_type pos, size_type n, const_pointer s)  {

        if (*s == 0)  {
            size_type   i = pos;

            for (; string_ [i]; ++i)
                string_ [i] = string_ [i + 1];
            string_ [i] = string_ [i + 1];
        }
        else  {
            bool        overwrote_null = false;
            size_type   i = 0;

            while (s [i])  {
                if (string_ [i + pos] == 0)
                    overwrote_null = 0;
                if (i >= n)
                    string_ [i + pos + 1] = string_ [i + pos];
                string_ [i + pos] = s [i];
                ++i;
            }
            if (overwrote_null)
                string_ [i + pos] = 0;
        }

        return (*this);
    }

    inline int printf (const char *format_str, ...) noexcept  {

        va_list     argument_ptr;

        va_start (argument_ptr, format_str);

        const   int ret = ::vsprintf (string_, format_str, argument_ptr);

        va_end (argument_ptr);
        return (ret);
    }

    inline int append_printf (const char *format_str, ...) noexcept  {

        va_list     argument_ptr;

        va_start (argument_ptr, format_str);

        const   int ret =
            ::vsprintf (string_ + size (), format_str, argument_ptr);

        va_end (argument_ptr);
        return (ret);
    }

    // Comparison methods.
    //
    inline int compare (const_pointer rhs) const noexcept  {

        return (::strcmp (string_, rhs));
    }
    inline int compare (const VirtualString &rhs) const noexcept  {

        return (compare (rhs.c_str ()));
    }

    inline bool operator == (const_pointer rhs) const noexcept  {

        return (compare (rhs) == 0);
    }
    inline bool operator == (const VirtualString &rhs) const noexcept  {

        return (*this == rhs.c_str ());
    }
    inline bool operator != (const_pointer rhs) const noexcept  {

        return (compare (rhs) != 0);
    }
    inline bool operator != (const VirtualString &rhs) const noexcept  {

        return (*this != rhs.c_str ());
    }
    inline bool operator > (const_pointer rhs) const noexcept  {

        return (compare (rhs) > 0);
    }
    inline bool operator > (const VirtualString &rhs) const noexcept  {

        return (*this > rhs.c_str ());
    }
    inline bool operator < (const_pointer rhs) const noexcept  {

        return (compare (rhs) < 0);
    }
    inline bool operator < (const VirtualString &rhs) const noexcept  {

        return (*this < rhs.c_str ());
    }

    // char based access methods.
    //
    inline const_reference operator [] (size_type index) const noexcept  {

        return (string_ [index]);
    }
    inline reference operator [] (size_type index) noexcept  {

        return (string_ [index]);
    }

    inline void clear () noexcept  { *string_ = 0; }

    // const utility methods.
    //
    inline const_pointer c_str () const noexcept  { return (string_); }
    inline const_pointer sub_c_str (size_type offset) const noexcept  {

        return (offset != npos ? string_ + offset : nullptr);
    }
    inline size_type size () const noexcept { return (::strlen(string_)); }
    inline bool empty () const noexcept  { return (*string_ == 0); }

private:

    pointer string_;
};

// ----------------------------------------------------------------------------

// This is a fixed size NTBS. Its sole purpose is performance. Most often
// programmers use utility strings with known upper limit size.
// FixedSizeString makes no dynamic allocation/deallocation and is
// strictly stack based. This will improve the performance of multi-threaded
// applications that use a lot of utiltiy strings.
//
// NOTE: FixedSizeString makes no boundary checks. It is the
//       responsibility of the programmer to take care of that.
//
template <unsigned int S>
class   FixedSizeString : public VirtualString  {

public:

    inline FixedSizeString () noexcept
        : VirtualString (buffer_) { *buffer_ = 0; }
    inline FixedSizeString (const_pointer str) noexcept
        : VirtualString (buffer_)  { *this = str; }
    inline FixedSizeString (const FixedSizeString &that) noexcept
        : VirtualString (buffer_)  { *this = that; }
    inline FixedSizeString (const VirtualString &that) noexcept
        : VirtualString (buffer_)  { *this = that; }

    // Assignment methods which cannot be inherited or virtual.
    //
    inline FixedSizeString &operator = (const FixedSizeString &rhs) noexcept  {

        ::strcpy (buffer_, rhs.buffer_);
        return (*this);
    }
    inline FixedSizeString &operator = (const_pointer rhs) noexcept {

        ::strncpy (buffer_, rhs, S);
        buffer_ [S] = 0;
        return (*this);
    }
    inline FixedSizeString &operator = (const VirtualString &rhs) noexcept  {

        *this = rhs.c_str ();
        return (*this);
    }

    static inline size_type capacity () noexcept  { return (S); }

private:

    value_type  buffer_ [S + 1];
};

// ----------------------------------------------------------------------------

template<typename S>
inline S &operator << (S &lhs, const VirtualString &rhs)  {

    return (lhs << rhs.c_str ());
}

// ----------------------------------------------------------------------------

using String32 = FixedSizeString<31>;
using String64 = FixedSizeString<63>;
using String128 = FixedSizeString<127>;
using String512 = FixedSizeString<511>;
using String1K = FixedSizeString<1023>;
using String2K = FixedSizeString<2047>;

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
