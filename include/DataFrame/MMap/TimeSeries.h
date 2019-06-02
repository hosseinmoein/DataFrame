// Hossein Moein
// August 27, 2007

#ifndef _INCLUDED_DMSob_TimeSeries_h
#define _INCLUDED_DMSob_TimeSeries_h 0

#include <cstdlib>
#include <vector>
#include <utility>
#include <time.h>

#include <DMScu_FixedSizeString.h>
#include <DMScu_VectorRange.h>

#include <DMSob_ObjectBase.h>

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM = DMScu_MMapFile>
class   DMSob_TimeSeries  {

    public:

        typedef ob_DATA                         data_type;
        typedef ob_INDEX                        index_type;
        typedef typename data_type::size_type   size_type;
        typedef typename data_type::time_type   time_type;

    protected:

        typedef DMSob_ObjectBase<time_t, data_type, ob_DATA_MEDIUM>
            DataSegType;
        typedef DMSob_ObjectBase<time_t, index_type, DMScu_MMapSharedMem>
            IndexSegType;
        typedef DMSob_ObjectBase<time_t, index_type, DMScu_MMapFile>
            UpdatableIndexSegType;
        typedef std::pair<const index_type *, const index_type *>
            ConstIndexPair;
        typedef std::pair<const data_type *, const data_type *>
            ConstDataPair;

    public:

        typedef DMScu_VectorRange<const data_type>  DataVector;
        typedef DMScu_VectorRange<const index_type> IndexVector;

    private:

        inline void initialize_ (const char *data_file,
                                 const char *index_file,
                                 time_type genesis);

    public:

       // It needs the _full_ path to the following files
       //
        DMSob_TimeSeries (const char *data_file,
                          const char *index_file,
                          time_type genesis = 0,
                          bool update_mode = false);
        virtual ~DMSob_TimeSeries ();

        template<class ob_FUNCTOR, class ob_PRED>
        inline size_type find_if (const char *symbol,
                                  ob_FUNCTOR &processor,
                                  ob_PRED &predicate,
                                  time_type start_time = 0,
                                  time_type end_time = 0,
                                  bool in_reverse = false) const throw ();

        template<class ob_FUNCTOR>
        inline size_type find (const char *symbol,
                               ob_FUNCTOR &processor,
                               time_type start_time = 0,
                               time_type end_time = 0,
                               bool in_reverse = false) const throw ()  {

            const   TruePred    true_pred;

            return (find_if (symbol,
                             processor,
                             true_pred,
                             start_time,
                             end_time,
                             in_reverse));
        }

        template<class ob_FUNCTOR>
        inline size_type get_symbols (ob_FUNCTOR &processor) const throw ();

       // The following two methods are the non-template versions of the
       // above methods. They return the whole vector for processing by
       // the user.
       //
        inline DataVector find (const char *symbol,
                                time_type start_time = 0,
                                time_type end_time = 0) const throw ();

        inline IndexVector get_symbols () const throw ();

        inline size_type get_symbol_count () const throw ()  {

            return (index_seg_ ? index_seg_->object_count ()
                        : updatable_index_seg
                              ? updatable_index_seg->object_count () : 1);
        }
        inline size_type get_tick_count () const throw ()  {

            return (data_seg_ ? data_seg_->object_count () : 0);
        }
        inline size_type get_tick_count (const char *symbol) const throw ();

       // NOTE: table_scan methods are very expensive. if you are running
       //       inside a server, it can mess up your caching mechanism.
       //       It will touch every record in the database.
       //       Use it only, if it is absolutely necessary.
       //
        template<class ob_FUNCTOR, class ob_PRED>
        inline size_type table_scan_if (ob_FUNCTOR &processor,
                                        ob_PRED &predicate,
                                        bool in_reverse = false) const throw();

        template<class ob_FUNCTOR>
        inline size_type table_scan (ob_FUNCTOR &processor,
                                     bool in_reverse = false) const throw ()  {

            const   TruePred    true_pred;

            return (table_scan_if (processor, true_pred));
        }

        class   TruePred : public std::unary_function<data_type, bool>  {

            public:

                typedef std::unary_function<data_type, bool>    BaseClass;
                typedef typename BaseClass::result_type         result_type;
                typedef typename BaseClass::argument_type       argument_type;

                inline TruePred () throw ()  {   }
                inline result_type
                operator () (const argument_type &) const throw ()  {

                    return (true);
                }
        };

    private:

        DataSegType             *data_seg_;
        IndexSegType            *index_seg_;
        UpdatableIndexSegType   *updatable_index_seg;
        const   ob_DATALESS     data_less_operator_;
        const   ob_INDEXLESS    index_less_operator_;
        const   ob_INDEXEQUAL   index_equal_operator_;
        bool                    is_attached_;
        const   bool            update_mode_;

    protected:

        inline DataSegType *_get_data_seg () throw ()  { return (data_seg_); }
        inline const DataSegType *_get_data_seg () const throw ()  {

            return (data_seg_);
        }
        inline UpdatableIndexSegType *_get_index_seg () throw ()  {

            return (updatable_index_seg);
        }
        inline const UpdatableIndexSegType *_get_index_seg () const throw ()  {

            return (updatable_index_seg);
        }

        inline const ob_INDEXLESS &_get_index_less_opt () const throw ()  {

            return (index_less_operator_);
        }
        inline const ob_INDEXEQUAL &_get_index_equal_opt () const throw ()  {

            return (index_equal_operator_);
        }

    public:

        inline bool attach ()  {

            if (! data_seg_)
                return (false);

            if (! is_attached_)  {
                is_attached_ = data_seg_->attach ();
                data_seg_->set_access_mode (DataSegType::_need_now_);
                return (is_attached_);
            }

            return (false);
        }
        inline bool dettach ()  {

            if (! data_seg_)
                return (false);

            if (is_attached_)  {
                is_attached_ = data_seg_->dettach ();
                is_attached_ = ! is_attached_;
                return (! is_attached_);
            }

            return (false);
        }
};

// ----------------------------------------------------------------------------

#  ifdef DMSHITS_INCLUDE_SOURCE
#    include <DMSob_TimeSeries.tcc>
#  endif // DMSHITS_INCLUDE_SOURCE

// ----------------------------------------------------------------------------

#undef _INCLUDED_DMSob_TimeSeries_h
#define _INCLUDED_DMSob_TimeSeries_h 1
#endif  // _INCLUDED_DMSob_TimeSeries_h

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
