// Hossein Moein
// August 21, 2007

#ifndef _INCLUDED_DMScu_MMapSharedMem_h
#define _INCLUDED_DMScu_MMapSharedMem_h 0

// ----------------------------------------------------------------------------

#include <DMScu_MMapBase.h>

// ----------------------------------------------------------------------------

class   DMScu_MMapSharedMem : public DMScu_MMapBase  {


    public:

        inline DMScu_MMapSharedMem (const char *file_name,
                                    OPEN_MODE open_mode,
                                    size_type init_file_size,
                                    mode_t file_open_mode = 0660)
            : DMScu_MMapBase (file_name, open_mode, _shared_memory_,
                              init_file_size, file_open_mode),
              initial_map_done_ (false)  {

            _translate_open_mode ();
            open ();
            initial_map_done_ = true;
        }

        virtual bool open ();
        virtual void unlink ();
        inline virtual bool clobber ()  {

            initial_map_done_ = false;
            return (! DMScu_MMapBase::close ());
        }

    protected:

       // this is public in the base class.
       //
        virtual int close (CLOSE_MODE close_mode = _normal_);

        virtual bool _initial_map_posthook ();

    private:

        bool    initial_map_done_;
};

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMScu_MMapSharedMem_h
#define _INCLUDED_DMScu_MMapSharedMem_h 1
#endif  // _INCLUDED_DMScu_MMapSharedMem_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
