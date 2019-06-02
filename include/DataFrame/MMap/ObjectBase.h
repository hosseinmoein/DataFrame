// Hossein Moein
// August 23, 2007

#ifndef _INCLUDED_DMSob_ObjectBase_h
#define _INCLUDED_DMSob_ObjectBase_h 0

#include <time.h>
#include <iterator>

#include <DMScu_MMapFile.h>
#include <DMScu_MMapSharedMem.h>

// ----------------------------------------------------------------------------

// ob_HEADER. ob_DATA elements are memcpy()'ed in and out of the
// Object Base. Therefore a ob_HEADER or ob_DATA cannot have any virtual
// method or any dynamically allocated member or anything that will break as
// a result of memcpy().
//
template<class ob_HEADER, class ob_DATA, class ob_BASE_CLASS>
class   DMSob_ObjectBase : protected ob_BASE_CLASS  {

    public:

        typedef ob_BASE_CLASS                   BaseClass;
        typedef typename BaseClass::size_type   size_type;
        typedef ob_HEADER                       header_type;
        typedef ob_DATA                         data_type;

        static  const   size_type   HEADER_SIZE = sizeof (header_type);
        static  const   size_type   DATA_SIZE = sizeof (data_type);

        enum OPEN_MODE { _write_ = 16, _read_ = 8, _append_ = 32 };
        enum ACCESS_MODE { _normal_ = 0, // No special treatment, the default

                          // Pages in the given range can be aggressively
                          // read ahead, and may be freed soon after they
                          // are accessed
                          //
                           _sequential_ = 2,

                          // Read ahead  may be less useful than normally
                          //
                           _random_ = 4,

                          // It might be a good idea to read some pages ahead
                          //
                           _need_now_ = 8,

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
                           _dont_need_ = 16 };

    public:

        DMSob_ObjectBase (const char *name,
                          OPEN_MODE open_mode,
                          ACCESS_MODE access_mode,
                          const header_type &header,
                          size_type buffer_size = 1024 * sizeof (data_type));
        virtual ~DMSob_ObjectBase ();

        inline bool is_ok () const throw ()  { return (BaseClass::is_ok ()); }
        size_type tell () const throw ();
        inline int flush () throw ()  { return (BaseClass::flush ()); }
        inline void unlink ()  { BaseClass::unlink (); }

        void set_access_mode (ACCESS_MODE am) const;

        header_type &get_header_rec ();
        const header_type &get_header_rec () const throw ();

        inline DMSob_ObjectBase &operator << (const data_type &data_ele)  {

            write (&data_ele, 1);
            return (*this);
        }
        inline DMSob_ObjectBase &operator >> (data_type &data_ele) throw ()  {

            read (&data_ele, 1);
            return (*this);
        }
        inline const DMSob_ObjectBase &
        operator >> (data_type &data_ele) const throw ()  {

            read (&data_ele, 1);
            return (*this);
        }

        bool seek (size_type obj_num) const throw ();

       // Bulk read and write methods
       //
        int write (const data_type *data_ele, size_type count);
        inline int read (data_type *data_ele, size_type count) throw ();
        inline int read (data_type *data_ele, size_type count) const throw () {

            return (const_cast<DMSob_ObjectBase *>
                        (this)->read (data_ele, count));
        }

        inline void refresh () const throw ()  {

            const   _internal_header_type   *mdata_ptr =
                reinterpret_cast<const _internal_header_type *>
                    (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
                     HEADER_SIZE);

            cached_object_count_ = mdata_ptr->object_count;
        }

        inline size_type object_count () const throw ()  {

            return (cached_object_count_);
        }
        time_t creation_time () const throw ();

       // NOTE: Be carefull with this. It will over-write the whole database.
       //
        template<class ob_MEMB_BASE_CLASS>
        inline bool overwrite_objectbase (
            const DMSob_ObjectBase<header_type,
                                   data_type,
                                   ob_MEMB_BASE_CLASS> &rhs)  {

            BaseClass::seek (0, BaseClass::_seek_set_);
            (*(static_cast<BaseClass *>(this))) <<
                (*(reinterpret_cast<const ob_MEMB_BASE_CLASS *>(&rhs)));
            seek (0);

            const   _internal_header_type   *mdata_ptr =
                reinterpret_cast<const _internal_header_type *>
                    (reinterpret_cast<char *>(BaseClass::_get_base_ptr ()) +
                     HEADER_SIZE);

            cached_object_count_ = mdata_ptr->object_count;
            return (true);
        }

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

        mutable size_type   cached_object_count_;

       // These are prohabited for now
       //
        DMSob_ObjectBase ();
        DMSob_ObjectBase (const DMSob_ObjectBase &that);
        DMSob_ObjectBase &operator = (const DMSob_ObjectBase &rhs);

    public:

        inline bool attach ()  {

            BaseClass::open ();
            refresh ();
            switch (static_cast<OPEN_MODE>(BaseClass::_get_open_mode ()))  {
                case _read_:
                case _write_:
                    seek (0);
                    break;
                case _append_:
                    seek (cached_object_count_);
                    break;
            }
            return (true);
        }
        inline bool dettach ()  { return (BaseClass::clobber ()); }

        inline const char *get_file_name () const throw ()  {

            return (BaseClass::get_file_name ());
        }

    //
    // The iterators:
    // These iterators contain only one pointer. Like STL iterators,
    // they are cheap to create and copy around.
    //

    public:

        class   iterator
            : public std::iterator<std::random_access_iterator_tag,
                                   data_type, long int>  {

            public:

                typedef std::random_access_iterator_tag iterator_category;

                typedef data_type * pointer;
                typedef data_type & reference;

            public:

               // NOTE: The constructor with no argument initializes
               //       the iterator to be the "end" iterator
               //
                inline iterator () throw () : node_ (NULL)  {   }

                inline iterator (data_type *node) throw ()
                    : node_ (node)  {   }

                inline bool operator == (const iterator &rhs) const throw ()  {

                    return (node_ == rhs.node_);
                }
                inline bool operator != (const iterator &rhs) const throw ()  {

                    return (node_ != rhs.node_);
                }

               // Following STL style, this iterator appears as a pointer
               // to data_type.
               //
                inline pointer operator -> () const throw ()  {

                    return (node_);
                }
                inline reference operator * () const throw ()  {

                    return (*node_);
                }
                inline operator pointer () const throw ()  { return (node_); }
                inline operator pointer () throw ()  { return (node_); }

               // We are following STL style iterator interface.
               //
                inline iterator &operator ++ () throw ()  {    // ++Prefix

                    node_ += 1;
                    return (*this);
                }
                inline iterator operator ++ (int) throw ()  {  // Postfix++

                    data_type   *the_node = node_;

                    node_ += 1;
                    return (iterator (the_node));
                }

                inline iterator &operator += (long int step) throw ()  {

                    node_ += step;
                    return (*this);
                }

                inline iterator &operator -- () throw ()  {    // --Prefix

                    node_ -= 1;
                    return (*this);
                }
                inline iterator operator -- (int) throw ()  {  // Postfix--

                    data_type   *the_node = node_;

                    node_ -= 1;
                    return (iterator (the_node));
                }

                inline iterator &operator -= (long int step) throw ()  {

                    node_ -= step;
                    return (*this);
                }

                inline iterator operator + (long int step) throw ()  {

                    return (iterator (node_ + step));
                }

                inline iterator operator - (long int step) throw ()  {

                    return (iterator (node_ - step));
                }

                inline iterator operator + (int step) throw ()  {

                    return (iterator (node_ + step));
                }

                inline iterator operator - (int step) throw ()  {

                    return (iterator (node_ - step));
                }

            private:

                pointer node_;

                friend  class   DMSob_ObjectBase::const_iterator;
                friend  class   DMSob_ObjectBase::reverse_iterator;
        };

        class   reverse_iterator
            : public std::iterator<std::random_access_iterator_tag,
                                   data_type, long int>  {

            public:

                typedef std::random_access_iterator_tag iterator_category;

                typedef data_type * pointer;
                typedef data_type & reference;

            public:

               // NOTE: The constructor with no argument initializes
               //       the iterator to be the "end" iterator
               //
                inline reverse_iterator () throw () : node_ (NULL)  {   }

                inline reverse_iterator (data_type *node) throw ()
                    : node_ (node)  {   }

                inline reverse_iterator (const iterator &itr) throw ()
                    : node_ (NULL)  {

                    *this = itr;
                }

                inline reverse_iterator &
                operator = (const iterator &rhs) throw ()  {

                    node_ = rhs.node_;
                    return (*this);
                }

                inline bool
                operator == (const reverse_iterator &rhs) const throw ()  {

                    return (node_ == rhs.node_);
                }
                inline bool
                operator != (const reverse_iterator &rhs) const throw ()  {

                    return (node_ != rhs.node_);
                }

               // Following STL style, this iterator appears as a pointer
               // to data_type.
               //
                inline pointer operator -> () const throw ()  {

                    return (node_);
                }
                inline reference operator * () const throw ()  {

                    return (*node_);
                }
                inline operator pointer () const throw ()  { return (node_); }

               // ++Prefix
               //
                inline reverse_iterator &operator ++ () throw ()  {

                    node_ -= 1;
                    return (*this);
                }

               // Postfix++
               //
                inline reverse_iterator operator ++ (int) throw ()  {

                    data_type   const  *ret_node = node_;

                    node_ -= 1;
                    return (reverse_iterator (ret_node));
                }

                inline reverse_iterator &operator += (long int step) throw () {

                    node_ -= step;
                    return (*this);
                }

               // --Prefix
               //
                inline reverse_iterator &operator -- () throw ()  {

                    node_ += 1;
                    return (*this);
                }

               // Postfix--
               //
                inline reverse_iterator operator -- (int) throw ()  {

                    data_type   const  *ret_node = node_;

                    node_ += 1;
                    return (reverse_iterator (ret_node));
                }

                inline reverse_iterator &operator -= (long int step) throw () {

                    node_ += step;
                    return (*this);
                }

                inline reverse_iterator operator + (long int step) throw ()  {

                    return (reverse_iterator (node_ - step));
                }

                inline reverse_iterator operator - (long int step) throw ()  {

                    return (reverse_iterator (node_ + step));
                }

                inline reverse_iterator operator + (int step) throw ()  {

                    return (reverse_iterator (node_ - step));
                }

                inline reverse_iterator operator - (int step) throw ()  {

                    return (reverse_iterator (node_ + step));
                }

            private:

                pointer node_;

                friend  class   DMSob_ObjectBase::const_reverse_iterator;
                friend  class   DMSob_ObjectBase::const_iterator;
        };

        class   const_iterator
            : public std::iterator<std::random_access_iterator_tag,
                                   data_type const, long int>  {

            public:

                typedef std::random_access_iterator_tag iterator_category;

                typedef const data_type *   pointer;
                typedef const data_type &   reference;

            public:

               // NOTE: The constructor with no argument initializes
               //       the iterator to be the "end" iterator
               //
                inline const_iterator () throw () : node_ (NULL)  {   }

                inline const_iterator (const data_type *node) throw ()
                    : node_ (node)  {   }

                inline const_iterator (const iterator &itr) throw ()
                    : node_ (NULL)  {

                    *this = itr;
                }

                inline const_iterator (const reverse_iterator &itr) throw ()
                    : node_ (NULL)  {

                    *this = itr;
                }

                inline const_iterator &
                operator = (const iterator &rhs) throw ()  {

                    node_ = rhs.node_;
                    return (*this);
                }
                inline const_iterator &
                operator = (const reverse_iterator &rhs) throw ()  {

                    node_ = rhs.node_;
                    return (*this);
                }

                inline bool
                operator == (const const_iterator &rhs) const throw ()  {

                    return (node_ == rhs.node_);
                }
                inline bool
                operator != (const const_iterator &rhs) const throw ()  {

                    return (node_ != rhs.node_);
                }

               // Following STL style, this iterator appears as a pointer
               // to data_type.
               //
                inline pointer operator -> () const throw ()  {

                    return (node_);
                }
                inline reference operator * () const throw ()  {

                    return (*node_);
                }
                inline operator pointer () const throw ()  { return (node_); }

               // ++Prefix
               //
                inline const_iterator &operator ++ () throw ()  {

                    node_ += 1;
                    return (*this);
                }

               // Postfix++
               //
                inline const_iterator operator ++ (int) throw ()  {

                    data_type   const  *ret_node = node_;

                    node_ += 1;
                    return (const_iterator (ret_node));
                }

                inline const_iterator &operator += (long int step) throw ()  {

                    node_ += step;
                    return (*this);
                }

               // --Prefix
               //
                inline const_iterator &operator -- () throw ()  {

                    node_ -= 1;
                    return (*this);
                }

               // Postfix--
               //
                inline const_iterator operator -- (int) throw ()  {

                    data_type   const  *ret_node = node_;

                    node_ -= 1;
                    return (const_iterator (ret_node));
                }

                inline const_iterator &operator -= (long int step) throw ()  {

                    node_ -= step;
                    return (*this);
                }

                inline const_iterator operator + (long int step) throw ()  {

                    return (const_iterator (node_ + step));
                }

                inline const_iterator operator - (long int step) throw ()  {

                    return (const_iterator (node_ - step));
                }

                inline const_iterator operator + (int step) throw ()  {

                    return (const_iterator (node_ + step));
                }

                inline const_iterator operator - (int step) throw ()  {

                    return (const_iterator (node_ - step));
                }

            private:

                pointer node_;

                friend  class   DMSob_ObjectBase::const_reverse_iterator;
        };

        class   const_reverse_iterator
            : public std::iterator<std::random_access_iterator_tag,
                                   data_type const, long int>  {

            public:

                typedef std::random_access_iterator_tag iterator_category;

                typedef const data_type *   pointer;
                typedef const data_type &   reference;

            public:

               // NOTE: The constructor with no argument initializes
               //       the iterator to be the "end" iterator
               //
                inline const_reverse_iterator () throw () : node_ (NULL)  {   }

                inline const_reverse_iterator (const data_type *node) throw ()
                    : node_ (node)  {   }

                inline const_reverse_iterator (const const_iterator &itr)
                    throw ()
                    : node_ (NULL)  {

                    *this = itr;
                }

                inline const_reverse_iterator (const reverse_iterator &itr)
                    throw ()
                    : node_ (NULL)  {

                    *this = itr;
                }

                inline const_reverse_iterator &
                operator = (const const_iterator &rhs) throw ()  {

                    node_ = rhs.node_;
                    return (*this);
                }
                inline const_reverse_iterator &
                operator = (const reverse_iterator &rhs) throw ()  {

                    node_ = rhs.node_;
                    return (*this);
                }

                inline bool operator == (const const_reverse_iterator &rhs)
                    const throw ()  {

                    return (node_ == rhs.node_);
                }
                inline bool operator != (const const_reverse_iterator &rhs)
                    const throw ()  {

                    return (node_ != rhs.node_);
                }

               // Following STL style, this iterator appears as a pointer
               // to data_type.
               //
                inline pointer operator -> () const throw ()  {

                    return (node_);
                }
                inline reference operator * () const throw ()  {

                    return (*node_);
                }
                inline operator pointer () const throw ()  { return (node_); }

               // ++Prefix
               //
                inline const_reverse_iterator &operator ++ () throw ()  {

                    node_ -= 1;
                    return (*this);
                }

               // Postfix++
               //
                inline const_reverse_iterator operator ++ (int) throw ()  {

                    data_type   const  *ret_node = node_;

                    node_ -= 1;
                    return (const_reverse_iterator (ret_node));
                }

                inline const_reverse_iterator &
                operator += (long int step) throw ()  {

                    node_ -= step;
                    return (*this);
                }

               // --Prefix
               //
                inline const_reverse_iterator &operator -- () throw ()  {

                    node_ += 1;
                    return (*this);
                }

               // Postfix--
               //
                inline const_reverse_iterator operator -- (int) throw ()  {

                    data_type   const  *ret_node = node_;

                    node_ += 1;
                    return (const_reverse_iterator (ret_node));
                }

                inline const_reverse_iterator &
                operator -= (long int step) throw ()  {

                    node_ += step;
                    return (*this);
                }

                inline const_reverse_iterator
                operator + (long int step) throw ()  {

                    return (const_reverse_iterator (node_ - step));
                }

                inline const_reverse_iterator
                operator - (long int step) throw ()  {

                    return (const_reverse_iterator (node_ + step));
                }

                inline const_reverse_iterator operator + (int step) throw ()  {

                    return (const_reverse_iterator (node_ - step));
                }

                inline const_reverse_iterator operator - (int step) throw ()  {

                    return (const_reverse_iterator (node_ + step));
                }

            private:

                pointer node_;
        };

        inline iterator begin () throw ()  {

            return (iterator (&((*this) [0])));
        }
        inline iterator end () throw ()  {

            return (iterator (&((*this) [object_count ()])));
        }
        inline iterator iterator_at (size_type i) throw ()  {

            return (iterator (&((*this) [i])));
        }

        inline const_iterator begin () const throw ()  {

            return (const_iterator (&((*this) [0])));
        }
        inline const_iterator end () const throw ()  {

            return (const_iterator (&((*this) [object_count ()])));
        }
        inline const_iterator const_iterator_at (size_type i) const throw ()  {

            return (const_iterator (&((*this) [i])));
        }

        inline reverse_iterator rbegin () throw ()  {

            return (reverse_iterator (&((*this) [object_count () - 1])));
        }
        inline reverse_iterator rend () throw ()  {

            return (reverse_iterator (&((*this) [0]) - 1));
        }
        inline reverse_iterator reverse_iterator_at (size_type i) throw ()  {

            return (reverse_iterator (&((*this) [i])));
        }

        inline const_reverse_iterator rbegin () const throw ()  {

            return (const_reverse_iterator (&((*this) [object_count () - 1])));
        }
        inline const_reverse_iterator rend () const throw ()  {

            return (const_reverse_iterator (&((*this) [0]) - 1));
        }
        inline const_reverse_iterator
        const_reverse_iterator_at (size_type i) const throw ()  {

            return (const_reverse_iterator (&((*this) [i])));
        }

        inline data_type &operator [] (size_type);
        inline const data_type &operator [] (size_type) const throw ();

        inline size_type size () const throw ()  { return (object_count ()); }
        inline bool empty () const throw ()  { return (object_count () == 0); }

        inline data_type &front () throw ()  { return ((*this) [0]); }
        inline const data_type &front () const throw ()  {

            return ((*this) [0]);
        }

        inline data_type &back () throw ()  { return ((*this) [size () - 1]); }
        inline const data_type &back () const throw ()  {

            return ((*this) [size () - 1]);
        }

        inline void push_back (const data_type &d)  { write (&d, 1); }

        inline void reserve (size_type s)  {

            const   size_type   trun_size =
                (s * DATA_SIZE) + _DATA_START_POINT;

            if (trun_size > BaseClass::get_file_size ())
                BaseClass::truncate (trun_size);

            return;
        }

       // Erases the range [first, last)
       //
        iterator erase (iterator first, iterator last);
        inline iterator erase (iterator pos) { return (erase (pos, pos + 1)); }
        inline void clear ()  { erase (begin (), end ()); }

       // Inserts the range [first, last) before pos
       //
       // NOTE: first and last are assumed to be iterators to
       //       _continues_ memory
       //
        template<class ob_ITER>
        void insert (iterator pos, ob_ITER first, ob_ITER last);
        inline void insert (iterator pos, const data_type &value)  {

            return (insert (pos, &value, &value + 1));
        }
};

// ----------------------------------------------------------------------------

#  ifdef DMSHITS_INCLUDE_SOURCE
#    include <DMSob_ObjectBase.tcc>
#  endif // DMSHITS_INCLUDE_SOURCE

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMSob_ObjectBase_h
#define _INCLUDED_DMSob_ObjectBase_h 1
#endif  // _INCLUDED_DMSob_ObjectBase_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
