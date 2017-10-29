// Hossein Moein
// July 17 2009
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#ifndef _INCLUDED_DMScu_Exception_h
#define _INCLUDED_DMScu_Exception_h 0

#include <iostream>
#include <string>
#include <cstring>
#include <stdexcept>

// ----------------------------------------------------------------------------

class   DMScu_Exception : public std::runtime_error  {

    public:

        typedef unsigned int    size_type;

        inline DMScu_Exception (const char *desc,
                                bool is_fatal = true,
                                const char *filename = "",
                                size_type line_number = 0) throw ()
            : std::runtime_error (desc),
              line_number_ (line_number),
              is_fatal_ (is_fatal)  {

            std::strncpy (filename_, filename, sizeof (filename_) - 1);
            filename_ [sizeof (filename_) - 1] = 0;
        }

        inline bool is_fatal () const throw ()  { return (is_fatal_); }
        inline const char *file_name () const throw ()  { return (filename_); }
        inline size_type line_number () const throw ()  {

            return (line_number_);
        }

        std::ostream &dump (std::ostream &os) const  {

            os << "EXCEPTION THROWN:\n"
               << "    Description : " << what () << "\n"
               << "    File Name   : " << filename_ << "\n"
               << "    Line Number : " << line_number_ << "\n"
               << "    Is Fatal    : " << (is_fatal_ ? "Yes" : "No")
               << std::endl;
            return os;
        }

    private:

        char                filename_ [64];
        const   size_type   line_number_;
        const   bool        is_fatal_;
};

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMScu_Exception_h
#define _INCLUDED_DMScu_Exception_h 1
#endif  // _INCLUDED_DMScu_Exception_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
