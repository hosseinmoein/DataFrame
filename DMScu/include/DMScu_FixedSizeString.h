// Hossein Moein
// July 17 2009
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _INCLUDED_DMScu_FixedSizeString_h
#define _INCLUDED_DMScu_FixedSizeString_h 0

#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <cstdarg>
#include <algorithm>

#include <string.h>

// ----------------------------------------------------------------------------

// This abstract base class makes it possible to pass different template
// instances around as one type and to be able to assign them interchangeably.
// The only penalty paid for having this base class is to carry around one
// additional (pointer size) member. There shouldn't be any performace
// penalty, since everything is still stack based and there is no virtuality.
//
// NOTE: DMScu_VirtualString MAKES NO BOUNDARY CHECKS. IT IS THE RESPONSIBILITY
//       OF THE PROGRAMMER TO TAKE CARE OF THAT.
//
class   DMScu_VirtualString  {

    public:

        typedef unsigned int        size_type;
        typedef char                value_type;
        typedef value_type *        pointer;
        typedef const value_type *  const_pointer;
        typedef value_type &        reference;
        typedef const value_type &  const_reference;

        typedef pointer             iterator;
        typedef const_pointer       const_iterator;

        static  const   size_type   npos = static_cast<size_type>(-1);

        inline iterator begin () throw ()  { return (string_); }
        inline const_iterator begin () const throw ()  { return (string_); }

       // Unfortunately, the following two methods are not as cheap as they are
       // supposed to be.
       //
        inline iterator end () throw ()  { return (string_ + size ()); }
        inline const_iterator end () const throw ()  {

            return (string_ + size ());
        }

    protected:

        inline DMScu_VirtualString (pointer str) throw () : string_ (str)  {  }

    public:

       // Assignment methods.
       //
        inline DMScu_VirtualString &operator = (const_pointer rhs) throw ()  {

            ::strcpy (string_, rhs);
            return (*this);
        }
        inline DMScu_VirtualString &
        operator = (const DMScu_VirtualString &rhs) throw ()  {

            return (*this = rhs.c_str ());
        }
        inline DMScu_VirtualString &
        ncopy (const_pointer rhs, size_type len) throw ()  {

            ::strncpy (string_, rhs, len);
            string_ [len] = 0;
            return (*this);
        }

        //
        // Appending methods.
        //

        inline DMScu_VirtualString &append (const_pointer rhs) throw ()  {

            ::strcat (string_, rhs);
            return (*this);
        }
        inline DMScu_VirtualString &
        append (const DMScu_VirtualString &rhs) throw ()  {

            return (append (rhs.c_str ()));
        }
        inline DMScu_VirtualString &operator += (const_pointer rhs) throw ()  {

            return (append (rhs));
        }
        inline DMScu_VirtualString &
        operator += (const DMScu_VirtualString &rhs) throw ()  {

            return (append (rhs.c_str ()));
        }

        inline size_type
        find (const_reference token, size_type pos = 0) const throw ()  {

            size_type   counter = 0;

            for (const_pointer itr = &(string_ [pos]); *itr; ++itr, ++counter)
                if (string_ [pos + counter] == token)
                    return (pos + counter);

            return (npos);
        }
        inline size_type
        find (const_pointer token, size_type pos = 0) const throw ()  {

            const   size_type   token_len = ::strlen (token);
            const   size_type   self_len = size ();

            if ((token_len + pos) > self_len)
                return (npos);

            size_type   counter = 0;

            for (const_pointer itr = &(string_ [pos]);
                 itr + token_len - begin () <= self_len; ++itr, ++counter)
                if (! ::strncmp (token, itr, token_len))
                    return (pos + counter);

            return (npos);
        }
        inline size_type find (const DMScu_VirtualString &token,
                               size_type pos = 0) const throw ()  {

            const   size_type   len = size ();

            return (find (token.c_str (), pos));
        }

       // Replaces the substring statring at pos with length n with s
       //
        inline DMScu_VirtualString &
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

        inline int printf (const char *format_str, ...) throw ()  {

            va_list     argument_ptr;

            va_start (argument_ptr, format_str);

            const   int ret = ::vsprintf (string_, format_str, argument_ptr);

            va_end (argument_ptr);
            return (ret);
        }

        inline int append_printf (const char *format_str, ...) throw ()  {

            va_list     argument_ptr;

            va_start (argument_ptr, format_str);

            const   int ret =
                ::vsprintf (string_ + size (), format_str, argument_ptr);

            va_end (argument_ptr);
            return (ret);
        }

       // Comparison methods.
       //
        inline int compare (const_pointer rhs) const throw ()  {

            return (::strcmp (string_, rhs));
        }
        inline int compare (const DMScu_VirtualString &rhs) const throw ()  {

            return (compare (rhs.c_str ()));
        }

        inline bool operator == (const_pointer rhs) const throw ()  {

            return (compare (rhs) == 0);
        }
        inline bool
        operator == (const DMScu_VirtualString &rhs) const throw ()  {

            return (*this == rhs.c_str ());
        }
        inline bool operator != (const_pointer rhs) const throw ()  {

            return (compare (rhs) != 0);
        }
        inline bool
        operator != (const DMScu_VirtualString &rhs) const throw ()  {

            return (*this != rhs.c_str ());
        }
        inline bool operator > (const_pointer rhs) const throw ()  {

            return (compare (rhs) > 0);
        }
        inline bool
        operator > (const DMScu_VirtualString &rhs) const throw ()  {

            return (*this > rhs.c_str ());
        }
        inline bool operator < (const_pointer rhs) const throw ()  {

            return (compare (rhs) < 0);
        }
        inline bool
        operator < (const DMScu_VirtualString &rhs) const throw ()  {

            return (*this < rhs.c_str ());
        }

       // char based access methods.
       //
        inline const_reference operator [] (size_type index) const throw ()  {

            return (string_ [index]);
        }
        inline reference operator [] (size_type index) throw ()  {

            return (string_ [index]);
        }

        inline void clear () throw ()  { *string_ = 0; }

       // const utility methods.
       //
        inline const_pointer c_str () const throw ()  { return (string_); }
        inline const_pointer sub_c_str (size_type offset) const throw ()  {

            return (offset != npos ? string_ + offset : NULL);
        }
        inline size_type size () const throw () { return (::strlen(string_)); }
        inline bool empty () const throw ()  { return (*string_ == 0); }

    private:

        pointer string_;

       // The semantics of this class does not allow the following two
       // methods, therefore they are prohibited.
       //
        DMScu_VirtualString ();
        DMScu_VirtualString (const DMScu_VirtualString &);
};

// ----------------------------------------------------------------------------

// This is a fixed size NTBS. Its sole purpose is performance. Most often
// programmers use utility strings with known upper limit size.
// DMScu_FixedSizeString makes no dynamic allocation/deallocation and is
// strictly stack based. This will improve the performance of multi-threaded
// applications that use a lot of utiltiy strings.
//
// NOTE: DMScu_FixedSizeString makes no boundary checks. It is the
//       responsibility of the programmer to take care of that.
//
template <unsigned int cu_SIZE>
class   DMScu_FixedSizeString : public DMScu_VirtualString  {

    public:

        inline DMScu_FixedSizeString () throw ()
            : DMScu_VirtualString (buffer_) { *buffer_ = 0; }
        inline DMScu_FixedSizeString (const_pointer str) throw ()
            : DMScu_VirtualString (buffer_)  { *this = str; }
        inline DMScu_FixedSizeString (const DMScu_FixedSizeString &that)
            throw ()
            : DMScu_VirtualString (buffer_)  { *this = that; }
        inline DMScu_FixedSizeString (const DMScu_VirtualString &that) throw ()
            : DMScu_VirtualString (buffer_)  { *this = that; }

       // Assignment methods which cannot be inherited or virtual.
       //
        inline DMScu_FixedSizeString &
        operator = (const DMScu_FixedSizeString &rhs) throw ()  {

            ::strcpy (buffer_, rhs.buffer_);
            return (*this);
        }
        inline DMScu_FixedSizeString &operator = (const_pointer rhs) throw () {

            ::strncpy (buffer_, rhs, cu_SIZE);
            buffer_ [cu_SIZE] = 0;
            return (*this);
        }
        inline DMScu_FixedSizeString &
        operator = (const DMScu_VirtualString &rhs) throw ()  {

            *this = rhs.c_str ();
            return (*this);
        }

        static inline size_type capacity () throw ()  { return (cu_SIZE); }

    private:

        value_type  buffer_ [cu_SIZE + 1];
};

// ----------------------------------------------------------------------------

inline std::ostream &
operator << (std::ostream &lhs, const DMScu_VirtualString &rhs)  {

    return (lhs << rhs.c_str ());
}

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMScu_FixedSizeString_h
#define _INCLUDED_DMScu_FixedSizeString_h 1
#endif  // _INCLUDED_DMScu_FixedSizeString_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
