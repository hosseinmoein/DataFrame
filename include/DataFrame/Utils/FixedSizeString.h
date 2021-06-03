// Hossein Moein
// July 17 2009
/*
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#pragma once

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <string.h>

#if defined(_WIN32) || defined(_WIN64)
#  ifdef _MSC_VER
#    ifdef LIBRARY_EXPORTS
#      define LIBRARY_API __declspec(dllexport)
#    else
#      define LIBRARY_API __declspec(dllimport)
#    endif // LIBRARY_EXPORTS
#  else
#    define LIBRARY_API
#  endif // _MSC_VER
#endif // _WIN32 || _WIN64

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

class LIBRARY_API   VirtualString {

public:

    using size_type = std::size_t;
    using value_type = char;
    using pointer = value_type *;
    using const_pointer = const value_type *;
    using reference = value_type &;
    using const_reference = const value_type &;
    using iterator = pointer;
    using const_iterator = const_pointer;

    inline static const size_type  npos = static_cast<size_type>(-1);

    inline iterator begin () noexcept  { return (string_); }
    inline const_iterator begin () const noexcept  { return (string_); }

    // Unfortunately, the following two methods are not as cheap as they are
    // supposed to be.
    //
    inline iterator end() noexcept  { return (string_ + size ()); }
    inline const_iterator end() const noexcept  { return (string_ + size ()); }

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

        ::snprintf(string_, len, "%s", rhs);
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

        va_list argument_ptr;

        va_start (argument_ptr, format_str);

        const int   ret = ::vsprintf (string_, format_str, argument_ptr);

        va_end (argument_ptr);
        return (ret);
    }

    inline int append_printf (const char *format_str, ...) noexcept  {

        va_list argument_ptr;

        va_start (argument_ptr, format_str);

        const int   ret =
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

    // These two make it compatible with std::string
    //
    inline void resize (size_type) noexcept  {  }
    inline void resize (size_type, value_type) noexcept  {  }

    // const utility methods.
    //
    inline const_pointer c_str () const noexcept  { return (string_); }
    inline const_pointer sub_c_str (size_type offset) const noexcept  {

        return (offset != npos ? string_ + offset : nullptr);
    }
    inline size_type size () const noexcept { return (::strlen(string_)); }
    inline bool empty () const noexcept  { return (*string_ == 0); }

    // Fowler–Noll–Vo (FNV-1a) hash function
    // This is for 64-bit systems
    //
    inline size_type hash () const noexcept {

        size_type       h = 14695981039346656037UL; // offset basis
        const_pointer   s = string_;

        while (*(s++)) { h = (h ^ *s) * 1099511628211UL; } // 64bit prime
        return (h);
    }

protected:

    inline VirtualString (pointer str) noexcept : string_ (str)  {  }

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

        ::snprintf(buffer_, S, "%s", rhs);
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

namespace std  {
template<>
struct  hash<typename hmdf::VirtualString>  {

    inline size_t operator()(const hmdf::VirtualString &key) const noexcept {

        return (key.hash());
    }
};

} // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
