// Hossein Moein
// September 21, 2007

#ifndef _INCLUDED_DMScu_FileBase_h
#define _INCLUDED_DMScu_FileBase_h 0

// ----------------------------------------------------------------------------

#include <iostream>
#include <string>
#include <stdarg.h>

#include <sys/fcntl.h>
#include <errno.h>
#include <strings.h>

#include <DMScu_Exception.h>
#include <DMScu_FileDef.h>

// ----------------------------------------------------------------------------

class   DMScu_FileBase : public DMScu_FileDef  {

    public:

        inline DMScu_FileBase (const char *file_name,
                               OPEN_MODE open_mode,
                               size_type buffer_size = 0UL)
            : DMScu_FileDef (file_name, open_mode, _stream_file_),
              stream_ (NULL),
              file_buffer_ (NULL),
              buffer_size_ (buffer_size)  {

            _translate_open_mode ();
            open ();
        }

        virtual ~DMScu_FileBase ()  { if (is_open ()) close (); }

        inline bool is_open () const throw ()  { return (stream_ != NULL); }

        bool open ();
        int close ();
        size_type read (void *, size_type, size_type) throw ();
        size_type write (const void *, size_type, size_type);

        inline int flush () throw ()  {

            const   flag_type   flag =
                _in_use_ | _s_bwrite_ | _s_write_ | _s_append_ | _s_bappend_;

            if (_file_flags & flag)  {
                ::fflush (stream_);
                return (0);
            }

            return (EOF);
        }

        int get_char (void) throw ();
        inline flag_type get_line (char *buffer)  {

            return (get_token ('\n', buffer));
        }
        std::string get_string (const char *search_str) throw ();
        std::string get_token (const char *delimit) throw ();
        size_type get_token (char delimit, char *buffer) throw ();
        int printf (const char *, ...) throw ();
        int put_char (int);
        int put_string (const char *) throw ();
        int put_back (unsigned char c);

        inline void rewind (void) throw ()  { seek (0L, _seek_set_); }
        int seek (size_type, SEEK_TYPE seek_type) throw ();
        size_type go_to_line (size_type);

        int truncate (size_type);

        inline DMScu_FileBase &operator << (char rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (char), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%c", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (char): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (unsigned char rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof(unsigned char), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%c", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (const unsigned char): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (short rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (short), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%hd", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (const short): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (unsigned short rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned short), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%hu", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (const unsigned short): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%d", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (unsigned int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%u", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (unsigned int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (long int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (long int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%ld", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (long int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (long long int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (long long int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%lld", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (long long int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (unsigned long int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned long int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%lu", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (unsigned long int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (unsigned long long int rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (unsigned long long int), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%llu", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (unsigned long long int): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (float rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (float), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset +=
                    ::fprintf (stream_, "%.*f", _precision, rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (const float): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (double rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (double), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset +=
                    ::fprintf (stream_, "%.*lf", _precision, rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (const double): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (const char *rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (char), ::strlen (rhs));
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%s", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (const char *): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }
        inline DMScu_FileBase &operator << (const std::string &rhs)  {

            return (*this << rhs.c_str ());
        }
        inline DMScu_FileBase &operator << (const DMScu_VirtualString &rhs)  {

            return (*this << rhs.c_str ());
        }
        inline DMScu_FileBase &operator << (const void *rhs)  {

            if ((_file_flags & _s_bwrite_) || (_file_flags & _s_bappend_))
                write (&rhs, sizeof (void *), 1);
            else if ((_file_flags & _s_write_) || (_file_flags & _s_append_)) {
                _current_offset += ::fprintf (stream_, "%p", rhs);
                _file_size = _current_offset > _file_size ? _current_offset
                                                          : _file_size;
            }
            else  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMScu_FileBase::<< (const void *): "
                            "Bad file permission for the action requested.");
                throw DMScu_Exception (err.c_str ());
            }

            return (*this);
        }

        inline DMScu_FileBase &operator >> (char *rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (std::ostream &rhs)   {

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
        inline DMScu_FileBase &operator >> (std::string &rhs)   {

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
        inline DMScu_FileBase &operator >> (DMScu_VirtualString &rhs)  {

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
        inline DMScu_FileBase &operator >> (char &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (unsigned char &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (short &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (int &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (long int &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (long long int &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (unsigned short &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (unsigned int &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (unsigned long int &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (unsigned long long int &rhs)
            throw ()  {

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
        inline DMScu_FileBase &operator >> (float &rhs) throw ()  {

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
        inline DMScu_FileBase &operator >> (double &rhs) throw ()  {

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

        FILE        *stream_;
        char        *file_buffer_;
        size_type   buffer_size_;

       // It's not gonna happen
       //
        DMScu_FileBase ();

    protected:

        bool _translate_open_mode () throw ();
        void _set_buffer (size_type bs);

        inline int _get_file_desc () const throw ()  {

            return (is_open () ? ::fileno (stream_) : -1);
        }

    public:

        inline size_type get_buffer_size () const throw ()  {

            return (buffer_size_);
        }

        void unlink ();
};

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMScu_FileBase_h
#define _INCLUDED_DMScu_FileBase_h 1
#endif  // _INCLUDED_DMScu_FileBase_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
