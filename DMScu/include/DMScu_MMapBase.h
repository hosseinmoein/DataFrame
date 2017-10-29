// Hossein Moein
// August 21, 2007
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _INCLUDED_DMScu_MMapBase_h
#define _INCLUDED_DMScu_MMapBase_h 0

// ----------------------------------------------------------------------------

#include <fstream>
#include <iostream>
#include <string>

#include <sys/mman.h>
#include <sys/fcntl.h>
#include <errno.h>
#include <strings.h>

#include <DMScu_Exception.h>
#include <DMScu_FileDef.h>

// ----------------------------------------------------------------------------

class   DMScu_MMapBase : public DMScu_FileDef  {

    public:

        static  const   size_type   SYSTEM_PAGE_SIZE;

        inline DMScu_MMapBase (const char *file_name, OPEN_MODE open_mode,
                               DEVICE_TYPE device_type,
                               size_type buffer_size = 0UL,
                               mode_t file_open_mode =
                                   S_IRUSR | S_IWUSR | S_IRGRP) throw ()
            : DMScu_FileDef (file_name, open_mode, device_type),
              _mmap_ptr (NULL),
              _file_desc (0),
              _mmap_size (0L),
              buffer_size_ (buffer_size),
              _mmap_prot (0),
              _mmap_flags (MAP_SHARED),
              _mmap_mode (0),
              _file_open_flags (0),
              _file_open_mode (file_open_mode)  {   }

        virtual ~DMScu_MMapBase ()  { if (is_open ()) close (); }

        inline bool is_open () const throw ()  { return (_file_desc > 0); }

        virtual bool open () = 0;
        virtual int close (CLOSE_MODE close_mode = _normal_);
        size_type read (void *, size_type, size_type) throw ();
        size_type write (const void *, size_type, size_type);

        inline int flush () throw ()  {

            const   flag_type   flag =
                _in_use_ | _s_bwrite_ | _s_write_ | _s_append_ | _s_bappend_;

            if (_file_flags & flag)  {
                ::msync (_mmap_ptr, _file_size, MS_SYNC | MS_INVALIDATE);
                return (0);
            }

            return (EOF);
        }

        int get_char () throw ();
        inline flag_type get_line (char *buffer) throw ()  {

            return (get_token ('\n', buffer));
        }
        std::string get_string (const char *search_str) throw ();
        std::string get_token (const char *delimit) throw ();
        size_type get_token (char delimit, char *buffer) throw ();
        int printf (const char *, ...) throw ();
        int put_char (int);
        int put_string (const char *) throw ();
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

        int seek (size_type, SEEK_TYPE seek_type) throw ();
        inline void rewind () throw ()  { seek (0L, _seek_set_); }
        size_type go_to_line (size_type);
        inline void set_buffer (size_type bs) throw ()  { buffer_size_ = bs; }
        int set_flag (int, flag_type);

        int truncate (size_type);

        inline DMScu_MMapBase &operator << (char rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (char), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%c", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(char): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (unsigned char rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned char), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%c", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(unsigned char): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (short rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (short), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%hd", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(short): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (unsigned short rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned short), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%hu", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(unsigned short): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%d", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (unsigned int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%u", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(unsigned int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (long int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (long int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%ld", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(long int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (long long int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (long long int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%lld", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(long long int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (unsigned long int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned long int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%lu", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(unsigned long int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (unsigned long long int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned long long int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%llu", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(unsigned long long int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (float rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (float), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%.*f", _precision, rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(const float): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (double rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (double), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%.*lf", _precision, rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(const double): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (const char *rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (rhs, sizeof (char), ::strlen (rhs));
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%s", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(const char *): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (const std::string &rhs)  {

            return (*this << rhs.c_str ());
        }
        inline DMScu_MMapBase &operator << (const DMScu_VirtualString &rhs)  {

            return (*this << rhs.c_str ());
        }
        inline DMScu_MMapBase &operator << (const void *rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (void *), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_))
                printf ("%p", rhs);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(const void *): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator << (const DMScu_MMapBase &rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_) ||
                (_file_flags & _s_write_) || (_file_flags & _s_append_))
                write (rhs._get_base_ptr (), rhs.get_mmap_size (), 1);
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::<<(const DMScu_MMapBase &): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        DMScu_MMapBase &operator << (const FILE &);
        DMScu_MMapBase &operator << (std::ifstream &);

        inline DMScu_MMapBase &operator >> (char *rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (rhs, sizeof (char), _width);
            else  {
                strncpy (rhs, get_token (" ").c_str (), _width);
                rhs [_width] = 0;
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (std::ostream &rhs)  {

            if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
                _file_flags & _s_append_) 
                rhs << get_token (" ").data ();
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::>>(std::ostream &) "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (std::string &rhs)  {

            if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
                _file_flags & _s_append_) 
                rhs += get_token (" ");
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::>>(std::string &): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (DMScu_VirtualString &rhs)  {

            if (_file_flags & _s_read_ || _file_flags & _s_write_ ||
                _file_flags & _s_append_) 
                rhs += get_token (" ").c_str ();
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_MMapBase::>>(DMScu_VirtualString &): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (char &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (char), 1);
            else
                rhs = static_cast<char>(get_char ());

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (unsigned char &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (unsigned char), 1);
            else
                rhs = static_cast<unsigned char>(get_char ());

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (short &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (short), 1);
            else
                rhs = static_cast<short>
                    (strtol (get_string ("0123456789").c_str (), NULL, 0));

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (int &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (int), 1);
            else
                rhs = static_cast<int>
                    (strtol (get_string ("0123456789").c_str (), NULL, 0));

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (long int &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (long int), 1);
            else
                rhs =  strtol (get_string ("0123456789").c_str (), NULL, 0);

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (long long int &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (long long int), 1);
            else
                rhs =  strtoll (get_string ("0123456789").c_str (), NULL, 0);

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (unsigned short &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (unsigned short), 1);
            else
                rhs = static_cast<unsigned short>
                    (strtol (get_string ("0123456789").c_str (), NULL, 0));

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (unsigned int &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (unsigned int), 1);
            else
                rhs = static_cast<unsigned int>
                    (strtol (get_string ("0123456789").c_str (), NULL, 0));

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (unsigned long int &rhs)
            throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (unsigned long int), 1);
            else
                rhs = static_cast<unsigned long int>
                    (strtol (get_string ("0123456789").c_str (), NULL, 0));

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (unsigned long long int &rhs)
            throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (unsigned long long int), 1);
            else
                rhs = static_cast<unsigned long long int>
                    (strtol (get_string ("0123456789").c_str (), NULL, 0));

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (float &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (float), 1);
            else
                rhs = static_cast<float>
                    (strtod (get_string ("0123456789.").c_str (), NULL));

            return (*this);
        }
        inline DMScu_MMapBase &operator >> (double &rhs) throw ()  {

            if (_file_flags & _s_bread_ || _file_flags & _s_bwrite_ ||
                _file_flags & _s_bappend_) 
                read (&rhs, sizeof (double), 1);
            else
                rhs = strtod (get_string ("0123456789.").c_str (), NULL);

            return (*this);
        }

    private:

        size_type   buffer_size_;

        inline bool check_space_4_printf_ () throw ();

       // It's not gonna happen
       //
        DMScu_MMapBase ();

    protected:

        class   cu_AutoFileDesc  {

            public:

                inline cu_AutoFileDesc (DMScu_MMapBase &mmap_file) throw ()
                    : mmap_file_ (mmap_file), should_close_ (false)  {

                    if (! mmap_file_.is_open ())  {
                        mmap_file_.open ();
                        should_close_ = true;
                    }
                }

                inline ~cu_AutoFileDesc () throw ()  {

                    if (should_close_)  mmap_file_.close ();
                }

            private:

                bool            should_close_;
                DMScu_MMapBase  &mmap_file_;

                cu_AutoFileDesc ();
        };

        int         _file_desc;
        flag_type   _file_open_flags;
        mode_t      _file_open_mode;
        int         _mmap_mode;
        int         _mmap_prot;
        void        *_mmap_ptr;
        size_type   _mmap_size;
        flag_type   _mmap_flags;


        virtual bool _translate_open_mode () throw ();
        virtual bool _initial_map (size_type file_size,
                                   int mmap_prot,
                                   flag_type mmap_flags,
                                   int file_desc,
                                   off_t  offset = 0,
                                   void *start = NULL) throw ();
        virtual bool _initial_map_posthook () = 0;

        inline void *_get_base_ptr () const throw ()  { return (_mmap_ptr); }
        inline int _get_file_desc () const throw ()  { return (_file_desc); }

    public:

        inline flag_type get_file_open_flags () const throw ()  {

            return (_file_open_flags);
        }
        inline mode_t get_file_open_mode () const throw ()  {

            return (_file_open_mode);
        }
        inline size_type get_buffer_size () const throw ()  {

            return (buffer_size_);
        }
        inline size_type get_mmap_size () const throw ()  {

            return (_mmap_size);
        }

        virtual void unlink ()  {

            throw DMScu_Exception ("DMScu_MMapBase::unlink() "
                                   "is not implemented.");
        }
        virtual bool clobber ()  { return (! close ()); }

};

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMScu_MMapBase_h
#define _INCLUDED_DMScu_MMapBase_h 1
#endif  // _INCLUDED_DMScu_MMapBase_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
