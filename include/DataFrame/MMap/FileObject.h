// Hossein Moein
// September 21, 2007

#ifndef _INCLUDED_DMSob_FileObject_h
#define _INCLUDED_DMSob_FileObject_h 0

#include <time.h>
#include <iterator>

#include <DMScu_FileBase.h>

// ----------------------------------------------------------------------------

// ob_HEADER. ob_DATA elements are written in and out of the File Object in
// binary format. Therefore a ob_HEADER or ob_DATA cannot have any virtual
// method or any dynamically allocated member or anything that will break as
// a result of binary copy.
//
template<class ob_HEADER, class ob_DATA>
class   DMSob_FileObject : protected DMScu_FileBase  {

    public:

        typedef DMScu_FileBase          BaseClass;
        typedef BaseClass::size_type    size_type;
        typedef ob_HEADER               header_type;
        typedef ob_DATA                 data_type;

        static  const   size_type   HEADER_SIZE = sizeof (header_type);
        static  const   size_type   DATA_SIZE = sizeof (data_type);

        enum OPEN_MODE { _write_ = 16, _read_ = 8, _append_ = 32 };
        enum ACCESS_MODE { _normal_ = 0, // No special treatment, the default

                          // Pages in the given range can be aggressively
                          // read ahead, and may be freed soon after they
                          // are accessed
                          //
                           _sequential_ = 2,

                          // Read ahead may be less useful than normally
                          //
                           _random_ = 4,

                          // It might be a good idea to read some pages ahead
                          //
                           _need_now_ = 8,

                          // Specifies that the application expects to access
                          // the specified data once and then not reuse it
                          // thereafter.
                          //
                           _no_reuse_ = 16,

                          // Do not expect access in the near future.
                          // (For the  time  being, the  application is
                          // finished with the given range, so the kernel can
                          // free resources associated with it.)  Subsequent
                          // accesses  of pages  in this range will succeed,
                          // but will result either in re-loading of the memory
                          // contents from the underlying  mapped  file
                          // (see  mmap) or zero-fill-on-demand pages for
                          // mappings without an underlying file.
                          //
                           _dont_need_ = 32 };

    public:

        DMSob_FileObject (const char *name,
                          OPEN_MODE open_mode,
                          ACCESS_MODE access_mode,
                          const header_type &header,
                          size_type buffer_size = 1024 * sizeof (data_type));
        virtual ~DMSob_FileObject ();

        inline bool is_ok () const throw ()  { return (BaseClass::is_ok ()); }
        size_type tell () const throw ();
        int flush ();
        inline void unlink ()  { BaseClass::unlink (); }

        bool seek (size_type obj_num) throw ();
        void set_access_mode (ACCESS_MODE am) const;

        inline data_type operator [] (size_type) throw ();

        header_type get_header_rec () throw ();

        inline DMSob_FileObject &operator << (const data_type &data_ele)  {

            write (&data_ele, 1);
            return (*this);
        }
        inline DMSob_FileObject &operator >> (data_type &data_ele) throw ()  {

            read (&data_ele, 1);
            return (*this);
        }

       // Bulk read and write methods
       //
        inline int write (const data_type *data_ele, size_type count);
        inline int read (data_type *data_ele, size_type count) throw ();

        size_type object_count () const throw ();
        inline bool empty () const throw ()  { return (object_count () == 0); }
        time_t creation_time () throw ();

    protected:

        class   MetaData  {

            public:

                inline MetaData () throw ()
                    : object_count (0), creation_time (0)  {   }
                inline MetaData (size_type oc, size_type ct) throw ()
                    : object_count (oc), creation_time (ct)  {   }

                size_type   object_count;
                size_type   creation_time;
        };

        typedef MetaData    _internal_header_type;

        static  const   size_type   _INTERNAL_HEADER_SIZE =
            sizeof (_internal_header_type);
        static  const   size_type   _DATA_START_POINT =
            HEADER_SIZE + _INTERNAL_HEADER_SIZE;

    private:

       // These are prohabited for now
       //
        DMSob_FileObject ();
        DMSob_FileObject (const DMSob_FileObject &that);
        DMSob_FileObject &operator = (const DMSob_FileObject &rhs);

    public:

       // These are somehow equivalent to open()/close().
       //
        bool attach ();
        bool dettach ();
};

// ----------------------------------------------------------------------------

#  ifdef DMSHITS_INCLUDE_SOURCE
#    include <DMSob_FileObject.tcc>
#  endif // DMSHITS_INCLUDE_SOURCE

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMSob_FileObject_h
#define _INCLUDED_DMSob_FileObject_h 1
#endif  // _INCLUDED_DMSob_FileObject_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
