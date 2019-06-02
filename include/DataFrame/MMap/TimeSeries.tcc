// Hossein Moein
// August 27, 2007

#include <sys/types.h>
#include <sys/stat.h>

#include <algorithm>
#include <stdexcept>

#include <DMScu_FixedSizeString.h>
#include <DMScu_StrTokenizer.h>

#include <DMSob_TimeSeries.h>

//
// Mr. Stroustrup, please do something about this syntax.
// It is getting ridicules.
//

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
void
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
initialize_ (const char *data_file,
             const char *index_file,
             time_type genesis)  {

    struct  stat    stat_data;

    if (::stat (data_file, &stat_data) == 0)  {
        data_seg_ = new DataSegType (data_file,
                                     update_mode_
                                         ? DataSegType::_append_
                                         : DataSegType::_read_,
                                     DataSegType::_sequential_,
                                     genesis);
        data_seg_->set_access_mode (DataSegType::_sequential_);
    }
    else
        return;

   // If the index file name is NULL, it is assumed that we are a real-time
   // database which doesn't need an index.  In a real time database, each
   // instrument has its own database.
   //
    if (index_file != NULL)  {
        if (::stat (index_file, &stat_data) != 0)  {
            delete data_seg_;
            data_seg_ = NULL;
            return;
        }

        if (update_mode_)
            updatable_index_seg =
                new UpdatableIndexSegType (index_file,
                                           UpdatableIndexSegType::_append_,
                                           UpdatableIndexSegType::_sequential_,
                                           genesis);
        else  {
            DMScu_StrTokenizer              tokener (index_file);
            DMScu_StrTokenizer::TokenVector token_vec;

            tokener.tokens ("/ ", token_vec);
            if (token_vec.size () < 2)  {
                DMScu_FixedSizeString<1023> err;

                err.printf ("DMSob_TimeSeries::DMSob_TimeSeries(): There is "
                            "something wrong with index file name '%s'",
                            index_file);
                throw std::runtime_error (err.c_str ());
            }

            DMScu_FixedSizeString<1023> index_shared_mem_name ("/");

            index_shared_mem_name += token_vec.back ().c_str ();
            index_shared_mem_name += ".";

           // This should be the date (e.g. 20070827)
           //
            index_shared_mem_name +=
                token_vec [token_vec.size () - 2].c_str ();

            index_seg_ = new IndexSegType (index_shared_mem_name.c_str (),
                                           IndexSegType::_write_,
                                           IndexSegType::_sequential_,
                                           genesis);

            if (index_seg_->object_count () == 0)  {
                UpdatableIndexSegType   tmp_file (
                    index_file,
                    UpdatableIndexSegType::_read_,
                    UpdatableIndexSegType::_sequential_,
                    genesis);

                index_seg_->overwrite_objectbase (tmp_file);
                index_seg_->seek (0);
                index_seg_->set_access_mode (IndexSegType::_random_);
            }
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
DMSob_TimeSeries (const char *data_file,
                  const char *index_file,
                  time_type genesis,
                  bool update_mode)
    : data_seg_ (NULL),
      index_seg_ (NULL),
      updatable_index_seg (NULL),
      data_less_operator_ (),
      index_less_operator_ (),
      index_equal_operator_ (),
      is_attached_ (true),
      update_mode_ (update_mode)  {

    initialize_ (data_file, index_file, genesis);
}

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
~DMSob_TimeSeries ()  {

    if (index_seg_)
        index_seg_->unlink ();

    delete index_seg_;
    delete updatable_index_seg;
    delete data_seg_;
}

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
inline typename DMSob_TimeSeries<ob_DATA,
                                 ob_INDEX,
                                 ob_DATALESS,
                                 ob_INDEXLESS,
                                 ob_INDEXEQUAL,
                                 ob_DATA_MEDIUM>::size_type
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
get_tick_count (const char *symbol) const throw ()  {

    const   IndexVector index_vec = get_symbols ();

    if (! index_vec.empty ())  {
        const   index_type      idx_entry (symbol);
        const   ConstIndexPair  index_pair =
            std::equal_range (index_vec.begin (),
                              index_vec.end (),
                              idx_entry,
                              index_less_operator_);

        if (index_equal_operator_ (*(index_pair.first), idx_entry))
            return (index_pair.first->get_end_offset () -
                    index_pair.first->get_start_offset ());
    }
    else if (data_seg_)
        return (data_seg_->object_count ());

    return (0);
}

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
template<class ob_FUNCTOR, class ob_PRED>
inline typename DMSob_TimeSeries<ob_DATA,
                                 ob_INDEX,
                                 ob_DATALESS,
                                 ob_INDEXLESS,
                                 ob_INDEXEQUAL,
                                 ob_DATA_MEDIUM>::size_type
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
find_if (const char *symbol,
         ob_FUNCTOR &processor,
         ob_PRED &predicate,
         time_type start_time,
         time_type end_time,
         bool in_reverse) const throw ()  {

    const   DataVector  data_vec = find (symbol, start_time, end_time);
    size_type           total_count = 0;

    processor.prolog (data_vec.size ());

    if (in_reverse)
        for (typename DataVector::const_reverse_iterator critr =
                 data_vec.rbegin ();
             critr != data_vec.rend (); ++critr)  {
            if (predicate (*critr))  {
                if (! processor (*critr, total_count))
                    break;
                total_count += 1;
            }
        }
    else
        for (typename DataVector::const_iterator citr = data_vec.begin ();
             citr != data_vec.end (); ++citr)
            if (predicate (*citr))  {
                if (! processor (*citr, total_count))
                    break;
                total_count += 1;
            }

    processor.epilog ();

    return (total_count);
}

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
inline typename DMSob_TimeSeries<ob_DATA,
                                 ob_INDEX,
                                 ob_DATALESS,
                                 ob_INDEXLESS,
                                 ob_INDEXEQUAL,
                                 ob_DATA_MEDIUM>::DataVector
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
find (const char *symbol,
      time_type start_time,
      time_type end_time) const throw ()  {

    const   DataSegType *db = data_seg_;

    if (! db)
        return (DataVector (NULL, NULL));

    const   IndexVector index_vec = get_symbols ();
    size_type           s_pos = 0;
    size_type           e_pos = db->object_count () - 1;

    if (! index_vec.empty ())  {
        const   index_type      idx_entry (symbol);
        const   ConstIndexPair  index_pair =
            std::equal_range (index_vec.begin (),
                              index_vec.end (),
                              idx_entry,
                              index_less_operator_);

        if (index_equal_operator_ (*(index_pair.first), idx_entry))  {
            s_pos = index_pair.first->get_start_offset ();
            e_pos = index_pair.first->get_end_offset () - 1;
        }
        else
            return (DataVector (NULL, NULL));
    }

    if ((start_time == 0 && end_time == 0) ||
        ((*db) [s_pos].get_timestamp () >= start_time &&
         (*db) [e_pos].get_timestamp () <= end_time))
        return (DataVector (&((*db) [s_pos]), &((*db) [e_pos])));
    else  {
        const   DataVector  data_vec (&((*db) [s_pos]), &((*db) [e_pos]));
        data_type           data_entry;

        data_entry.set_timestamp (start_time);

        const   typename DataVector::const_iterator  s_citr =
            std::lower_bound (data_vec.begin (),
                              data_vec.end (),
                              data_entry,
                              data_less_operator_);

        data_entry.set_timestamp (end_time);

        const   typename DataVector::const_iterator  e_citr =
            std::upper_bound (data_vec.begin (),
                              data_vec.end (),
                              data_entry,
                              data_less_operator_);

        if (e_citr > s_citr)
            return (DataVector (s_citr, e_citr - 1));
    }

    return (DataVector (NULL, NULL));
}

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
template<class ob_FUNCTOR>
inline typename DMSob_TimeSeries<ob_DATA,
                                 ob_INDEX,
                                 ob_DATALESS,
                                 ob_INDEXLESS,
                                 ob_INDEXEQUAL,
                                 ob_DATA_MEDIUM>::size_type
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
get_symbols (ob_FUNCTOR &processor) const throw ()  {

    const   IndexVector index_vec = get_symbols ();
    size_type           total_count = 0;

    processor.prolog  (index_vec.size ());
    for (typename IndexVector::const_iterator citr = index_vec.begin ();
         citr != index_vec.end (); ++citr)  {
        if (! processor (*citr, total_count))
            break;

        total_count += 1;
    }
    processor.epilog  ();

    return (total_count);
}

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
inline typename DMSob_TimeSeries<ob_DATA,
                                 ob_INDEX,
                                 ob_DATALESS,
                                 ob_INDEXLESS,
                                 ob_INDEXEQUAL,
                                 ob_DATA_MEDIUM>::IndexVector
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
get_symbols () const throw ()  {

    if (index_seg_)
        return (IndexVector (
                    &((*index_seg_) [0]),
                    &((*index_seg_) [index_seg_->object_count () - 1])));
    else if (updatable_index_seg)
        return (IndexVector (
                    &((*updatable_index_seg) [0]),
                    &((*updatable_index_seg) [
                        updatable_index_seg->object_count () - 1])));

    return (IndexVector (NULL, NULL));
}

// ----------------------------------------------------------------------------

template<class ob_DATA,
         class ob_INDEX,
         class ob_DATALESS,
         class ob_INDEXLESS,
         class ob_INDEXEQUAL,
         class ob_DATA_MEDIUM>
template<class ob_FUNCTOR, class ob_PRED>
inline typename DMSob_TimeSeries<ob_DATA,
                                 ob_INDEX,
                                 ob_DATALESS,
                                 ob_INDEXLESS,
                                 ob_INDEXEQUAL,
                                 ob_DATA_MEDIUM>::size_type
DMSob_TimeSeries<ob_DATA,
                 ob_INDEX,
                 ob_DATALESS,
                 ob_INDEXLESS,
                 ob_INDEXEQUAL,
                 ob_DATA_MEDIUM>::
table_scan_if (ob_FUNCTOR &processor,
               ob_PRED &predicate,
               bool in_reverse) const throw ()  {

    const   DataSegType *db = data_seg_;

    if (! db)
        return (0);

    const   size_type   obj_count = db->object_count ();
    const   DataVector  data_vec (obj_count > 0 ? &((*db) [0]) : NULL,
                                  obj_count > 0 ? &((*db) [obj_count - 1])
                                                : NULL);
    size_type           total_count = 0;

    processor.prolog (data_vec.size ());

    if (in_reverse)
        for (typename DataVector::const_reverse_iterator critr =
                 data_vec.rbegin ();
             critr != data_vec.rend (); ++critr)  {
            if (predicate (*critr))
                if (! processor (*critr, total_count))
                    break;

            total_count += 1;
        }
    else
        for (typename DataVector::const_iterator citr = data_vec.begin ();
             citr != data_vec.end (); ++citr)  {
            if (predicate (*citr))
                if (! processor (*citr, total_count))
                    break;

            total_count += 1;
        }

    processor.epilog ();

    return (total_count);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
