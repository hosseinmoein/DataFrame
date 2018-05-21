// Hossein Moein
// September 11, 2017
// Copyright (C) 2017-2018 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include "BaseContainer.h"
#include <map>
#include <stdexcept>
#include <iostream>
#include <cstdio>
#include <cstring>
#include <future>

// ----------------------------------------------------------------------------

namespace hmdf
{

// DS is a data storage container. It must have an interface partly identical
// to std::vector
//
template<typename TS, template<typename DT, class... types> class DS>
class DataFrame  {

public:

    struct DataFrameError : public std::runtime_error  {

        DataFrameError (const char *name) : std::runtime_error (name)  {   }
    };
    struct ColNotFound : public DataFrameError  {

        ColNotFound (const char *name) : DataFrameError (name)  {   }
    };
    struct InconsistentData : public DataFrameError  {

        InconsistentData (const char *name) : DataFrameError (name)  {   }
    };
    struct NotImplemented : public DataFrameError  {

        NotImplemented (const char *name) : DataFrameError (name)  {   }
    };

private:

    using DataVec = HeteroVector;
    using DataVecVec = DS<DataVec>;

public:

    using size_type = typename DS<DataVec>::size_type;
    using TimeStamp = TS;
    using TSVec = DS<TS>;

    DataFrame() = default;
    DataFrame(const DataFrame &) = default;
    DataFrame(DataFrame &&) = default;
    DataFrame &operator= (const DataFrame &) = default;
    DataFrame &operator= (DataFrame &&) = default;

    ~DataFrame() = default;

private:

    using DataTable = std::unordered_map<std::string, size_type>;

    // Data fields
    //
    DataVecVec  data_ { };        // Vector of Heterogeneous vectors
    TSVec       timestamps_ { };  // Vector
    DataTable   data_tb_ { };     // Hash table of name -> vector index

public:  // Load/append interfaces

    // It creates an empty column named name
    //
    template<typename T>
    DS<T> &create_column (const char *name);

    // This is the most generalized load function. It creates and loads an
    // index and a variable number of columns. The index vector and all
    // column vectors are "moved" to DataFrame.
    //
    // Ts: The list of types for columns in args
    // indices: A vector of indices (timestamps) of type TimeStamp;
    // args: A variable list of arguments consisting of
    //       std::pair(<const char *name, DS<T> &&data>).
    //       Each pair, represents a column data and its name
    //
    template<typename ... Ts>
    size_type load_data (TSVec &&indices, Ts ... args);

    // It copies the data from iterators begin to end into the index column
    //
    // ITR: Type of the iterator
    //
    template<typename ITR>
    size_type load_index(const ITR &begin, const ITR &end);
    // It moves the idx vector into the index column.
    //
    size_type load_index(TSVec &&idx);

    // It copies the data from iterators begin to end to the named column.
    // If column does not exist, it will be created. If the column exist,
    // it will be over written.
    //
    // T: Type of data being copied
    // ITR: Type of the iterator
    // name: Name of the column
    // pad_with_nan: If true, it pads the data column with nan,
    //               if it is shorter than the index column.
    //
    template<typename T, typename ITR>
    size_type load_column(const char *name,
                          const ITR &begin,
                          const ITR &end,
                          bool pad_with_nan = true);
    // It moves the data to the named column in DataFrame.
    // If column does not exist, it will be created. If the column exist,
    // it will be over written.
    //
    // T: Type of data being moved
    // name: Name of the column
    // pad_with_nan: If true, it pads the data column with nan,
    //               if it is shorter than the index column.
    //
    template<typename T>
    size_type
    load_column(const char *name, DS<T> &&data, bool pad_with_nan = true);

    // It appends val to the end of the index column.
    //
    size_type append_index(const TimeStamp &val);
    // It appends val to the end of the named data column.
    // If data column doesn't exist, it throws an exception.
    //
    // T: Type of the named data column
    // name: Name of the column
    // pad_with_nan: If true, it pads the data column with nan,
    //               if it is shorter than the index column.
    //
    template<typename T>
    size_type append_column(const char *name,
                            const T &val,
                            bool pad_with_nan = true);

    // It appends the range begin to end to the end of the index column
    //
    // ITR: Type of the iterator
    //
    template<typename ITR>
    size_type append_index(const ITR &begin, const ITR &end);
    // It appends the range begin to end to the end of the named data column.
    // If data column doesn't exist, it throws an exception.
    //
    // T: Type of the named data column
    // ITR: Type of the iterator
    // name: Name of the column
    // pad_with_nan: If true, it pads the data column with nan,
    //               if it is shorter than the index column.
    //
    template<typename T, typename ITR>
    size_type append_column(const char *name,
                            const ITR &begin,
                            const ITR &end,
                            bool pad_with_nan = true);

public:  // Other public interfaces

    // Make all data columns the same length as the index.
    // If any data column is shorter than the index column, it will be padded
    // by nan.
    // This is also called by sort(), before sorting
    //
    template<typename ... types>
    void make_consistent ();

    // Sort the DataFrame by the named column. By default, it sorts
    // by index (i.e. by_name == nullptr).
    // Sort first calls make_consistent() that may add nan values to
    // data columns.
    // nan values make sorting nondeterministic.
    //
    // T: Type of the by_name column. You always of the specify this type,
    //    even if it is being sorted to the default index
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    //
    template<typename T, typename ... types>
    void sort(const char *by_name = nullptr);
    // Same as sort() above, but executed asynchronously
    //
    template<typename T, typename ... types>
    std::future<void> sort_async (const char *by_name = nullptr);

    // Groupby copies the DataFrame into a temp DataFrame and sorts
    // the temp df by gb_col_name before performing groupby.
    // if gb_col_name is null, it groups by index.
    //
    // F: type functor to be applied to columns to group by
    // T: type of the groupby column. In case if index, it is type of index
    // types: List of the types of all data columns.
    //        A type should be specified in the list only once.
    // func: The functor to do the groupby. Specs for the functor is
    //       in a separate doc.
    // already_sorted: If the DataFrame is already sorted by gb_col_name,
    //                 this will save the expensive sort operation
    //
    template<typename F, typename T, typename ... types>
    DataFrame groupby (F &&func,
                       const char *gb_col_name = nullptr,
                       bool already_sorted = false) const;
    template<typename F, typename T, typename ... types>
    // Same as groupby() above, but executed asynchronously
    //
    std::future<DataFrame>
    groupby_async (F &&func,
                   const char *gb_col_name = nullptr,
                   bool already_sorted = false) const;

    // It bucketizes the data and index into bucket_interval's,
    // based on index values and calls the functor for each bucket.
    // The result of each bucket will be stored in a new DataFrame with
    // same shape and returned.
    // Every data bucket is guaranteed to be as wide as bucket_interval.
    // This mean some data items at the end may not be included in the
    // new bucketized DataFrame.
    // The index of each bucket will be the last index in the original
    // DataFrame that is less than bucket_interval away from the
    // previous bucket
    //
    // NOTE:The DataFrame must already be sorted by index.
    //
    // F: type functor to be applied to columns to bucketize
    // types: List of the types of all data columns.
    //        A type should be specified in the list only once.
    // bucket_interval: Bucket interval is in the index's single value unit.
    //                  For example if index is in minutes, bucket_interval
    //                  will be in the unit of minutes and so on.
    // already_sorted: If the DataFrame is already sorted by index,
    //                 this will save the expensive sort operation
    //
    template<typename F, typename ... types>
    DataFrame bucketize (F &&func, const TimeStamp &bucket_interval) const;
    template<typename F, typename ... types>
    // Same as bucketize() above, but executed asynchronously
    //
    std::future<DataFrame>
    bucketize_async (F &&func, const TimeStamp &bucket_interval) const;

    // This is exactly the same as bucketize() above. The only difference is
    // it stores the result in itself and returns void.
    // So after the return the original data is lost and replaced with
    // bucketized data
    //
    // NOTE:The DataFrame must already be sorted by index.
    //
    template<typename F, typename ... types>
    void self_bucketize (F &&func, const TimeStamp &bucket_interval);

    // It outputs the content of DataFrame into the stream o as text in the
    // following format:
    //     INDEX:<Comma delimited list of values>
    //     <Column1 name>:<Column1 type>:<Comma delimited list of values>
    //     <Column2 name>:<Column2 type>:<Comma delimited list of values>
    //
    // S: Output stream type
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // o: Reference to an streamable object (e.g. cout)
    // values_only: If true, the name and type of each column is not written
    //
    template<typename S, typename ... types>
    bool write (S &o, bool values_only = false) const;
    // Same as write() above, but executed asynchronously
    //
    template<typename S, typename ... Ts>
    std::future<bool> write_async (S &o, bool values_only = false) const;

    // It inputs the contents of a text file into itself (i.e. DataFrame).
    // The format of the file must be:
    //     INDEX:<Comma delimited list of values>
    //     <Column1 name>:<Column1 type>:<Comma delimited list of values>
    //     <Column2 name>:<Column2 type>:<Comma delimited list of values>
    // All empty lines or lines starting with # will be skipped.
    //
    // file_name: Complete path to the file
    //
    bool read (const char *file_name);
    // Same as read() above, but executed asynchronously
    //
    std::future<bool> read_async (const char *file_name);

public: // Read/access interfaces

    // It returns a reference to the container of named data column
    //
    // T: Data type of the named column
    //
    template<typename T>
    DS<T> &get_column (const char *name);
    // It returns a const reference to the container of named data column
    //
    // T: Data type of the named column
    //
    template<typename T>
    const DS<T> &get_column (const char *name) const;

    // It returns a DataFrame (including the index and data columns)
    // containing the data from index begin to index end
    // This function assumes the DataFrame is consistent and sorted by index.
    // The behavior is undefined otherwise.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    //
    template<typename ... types>
    DataFrame get_data_by_idx (TS begin, TS end) const;

    // It returns a DataFrame (including the index and data columns)
    // containing the data from location begin to location end
    // This function assumes the DataFrame is consistent and sorted by index.
    // The behavior is undefined otherwise.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    //
    template<typename ... types>
    DataFrame get_data_by_loc (size_type begin, size_type end) const;

    // It returns a const reference to the index container
    //
    const TSVec &get_index () const  { return (timestamps_); }
    // It returns a reference to the index container
    //
    TSVec &get_index ()  { return (timestamps_); }

    // This is the most generalized visit function. It visits multiple
    // columns with the corresponding function objects sequentially.
    // Each function object is passed every single value of the given
    // column along with its name and the corresponding index value.
    // All functions objects must have this signature
    //     bool (const TimeStamp &i, const char *name, const T &col_value)
    // If the function object returns false, the DataFrame will not go
    // on that column.
    //
    // Ts: The list of types for columns in args
    // args: A variable list of arguments consisting of
    //     std::pair(<const char *name,
    //       &std::function<bool (const TimeStamp &, const char *, const T &)>).
    //      Each pair represents a column name and the functor to run on it.
    // Note: The second member of pair is a _pointer_ to the function or
    //       functor object
    //
    template<typename ... Ts>
    void multi_visit (Ts ... args) const;

    // It passes the values of each index and each named column to the
    // functor visitor sequentially from beginning to end
    //
    // T: Type of the named column
    // V: Type of the visitor functor
    // name: Name of the data column
    //
    template<typename T, typename V>
    V &visit (const char *name, V &visitor) const;

    // It passes the values of each index and the two named columns to the
    // functor visitor sequentially from beginning to end
    //
    // T1: Type of the first named column
    // T2: Type of the second named column
    // V: Type of the visitor functor
    // name1: Name of the first data column
    // name2: Name of the second data column
    //
    template<typename T1, typename T2, typename V>
    V &&visit (const char *name1, const char *name2, V &&visitor) const;

    // It passes the values of each index and the three named columns to the
    // functor visitor sequentially from beginning to end
    //
    // T1: Type of the first named column
    // T2: Type of the second named column
    // T3: Type of the third named column
    // V: Type of the visitor functor
    // name1: Name of the first data column
    // name2: Name of the second data column
    // name3: Name of the third data column
    //
    template<typename T1, typename T2, typename T3, typename V>
    V &&visit (const char *name1,
               const char *name2,
               const char *name3,
               V &&visitor) const;

    // It passes the values of each index and the four named columns to the
    // functor visitor sequentially from beginning to end
    //
    // T1: Type of the first named column
    // T2: Type of the second named column
    // T3: Type of the third named column
    // T4: Type of the forth named column
    // V: Type of the visitor functor
    // name1: Name of the first data column
    // name2: Name of the second data column
    // name3: Name of the third data column
    // name4: Name of the forth data column
    //
    template<typename T1, typename T2, typename T3, typename T4, typename V>
    V &&visit (const char *name1,
               const char *name2,
               const char *name3,
               const char *name4,
               V &&visitor) const;

    // It passes the values of each index and the five named columns to the
    // functor visitor sequentially from beginning to end
    //
    // T1: Type of the first named column
    // T2: Type of the second named column
    // T3: Type of the third named column
    // T4: Type of the forth named column
    // T5: Type of the fifth named column
    // V: Type of the visitor functor
    // name1: Name of the first data column
    // name2: Name of the second data column
    // name3: Name of the third data column
    // name4: Name of the forth data column
    // name5: Name of the fifth data column
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    V &&visit (const char *name1,
               const char *name2,
               const char *name3,
               const char *name4,
               const char *name5,
               V &&visitor) const;

public:  // Operators

    // It compares self with rhs. If both have the sanme indices,
    // same number of columns, same names for each column, and all
    // columns are eual, then it retunrs true. Otherwise it returns false
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    //
    template<typename ... types>
    bool is_equal (const DataFrame &rhs) const;

    // It iterates over all indices in rhs and modifyies all the data
    // columns in self that correspond to the given index value.
    // If not already_sorted, both rhs and self will be sorted by index
    // It returns a reference to self
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // already_sorted: If the self and rhs are already sorted by index,
    //                 this will save the expensive sort operations
    //
    template<typename ... types>
    DataFrame &modify_by_idx (DataFrame &rhs, bool already_sorted = false);

protected:

    template<typename T1, typename T2>
    size_type _load_pair(std::pair<T1, T2> &col_name_data);

    template<typename T>
    static inline constexpr T _get_nan();

private:

    template<typename ... types>
    struct consistent_functor_ : HeteroVector::visitor_base<types ...>  {

        consistent_functor_ (const size_type s) : size(s)  {   }

        const DataFrame::size_type  size;

        template<typename T>
        void operator() (T &vec) const;
    };

    template<typename T, typename ... types>
    struct sort_functor_ : HeteroVector::visitor_base<types ...>  {

        sort_functor_ (const DS<T> &iv) : idx_vec(iv)  {   }

        const DS<T> &idx_vec;

        template<typename T2>
        void operator() (T2 &vec) const;
    };

    template<typename ... types>
    struct load_functor_ : HeteroVector::visitor_base<types ...>  {

        load_functor_ (const char *n,
                       std::size_t b,
                       std::size_t e,
                       DataFrame &d)
            : name (n), begin (b), end (e), df(d)  {   }

        const char          *name;
        const std::size_t   begin;
        const std::size_t   end;
        DataFrame           &df;

        template<typename T>
        void operator() (const T &vec);
    };

    template<typename ... types>
    struct add_col_functor_ : HeteroVector::visitor_base<types ...>  {

        add_col_functor_ (const char *n, DataFrame &d) : name (n), df(d)  {   }

        const char  *name;
        DataFrame   &df;

        template<typename T>
        void operator() (const T &vec);
    };

    template<typename F, typename ... types>
    struct groupby_functor_ : HeteroVector::visitor_base<types ...>  {

        groupby_functor_ (const char *n,
                          std::size_t b,
                          std::size_t e,
                          const TS &ts,
                          F &f,
                          DataFrame &d)
            : name(n), begin(b), end(e), timestamp(ts), functor(f), df(d) {  }

        const char          *name;
        const std::size_t   begin;
        const std::size_t   end;
        const TS            &timestamp;
        F                   &functor;
        DataFrame           &df;

        template<typename T>
        void operator() (const T &vec);
    };

    template<typename F, typename ... types>
    struct bucket_functor_ : HeteroVector::visitor_base<types ...>  {

        bucket_functor_ (const char *n,
                         const TSVec &ts,
                         const TimeStamp &i,
                         F &f,
                         DataFrame &d)
            : name(n), timestamps(ts), interval(i), functor(f), df(d) {  }

        const char      *name;
        const TSVec     &timestamps;
        const TimeStamp &interval;
        F               &functor;
        DataFrame       &df;

        template<typename T>
        void operator() (const T &vec);
    };

    template<typename ... types>
    struct print_functor_ : HeteroVector::visitor_base<types ...>  {

        print_functor_ (const char *n, bool vo, std::ostream &o)
            : name(n), values_only(vo), os(o)  {   }

        const char      *name;
        const bool      values_only;
        std::ostream    &os;

        template<typename T>
        void operator() (const T &vec);
    };

    template<typename ... types>
    struct equal_functor_ : HeteroVector::visitor_base<types ...>  {

        equal_functor_ (const char *n, const DataFrame &d)
            : name(n), df(d)  {  }

        const char      *name;
        const DataFrame &df;
        bool            result { true };

        template<typename T>
        void operator() (const DS<T> &lhs_vec);
    };

    template<typename ... types>
    struct mod_by_idx_functor_ : HeteroVector::visitor_base<types ...>  {

        mod_by_idx_functor_ (const char *n,
                             const DataFrame &d,
                             size_type li,
                             size_type ri)
            : name(n), rhs_df(d), lhs_idx(li), rhs_idx(ri)  {  }

        const char      *name;
        const DataFrame &rhs_df;
        const size_type lhs_idx;
        const size_type rhs_idx;

        template<typename T>
        void operator() (DS<T> &lhs_vec) const;
    };

    template<typename ... Ts, typename F, std::size_t ... Is>
    void for_each_in_tuple_ (const std::tuple<Ts ...> &tu,
                             F func,
                             std::index_sequence<Is ...>) const;

    template<typename ... Ts, typename F, std::size_t ... Is>
    void for_each_in_tuple_ (std::tuple<Ts ...> &tu,
                             F func,
                             std::index_sequence<Is ...>);

    template<typename ... Ts, typename F>
    void for_each_in_tuple_ (const std::tuple<Ts...> &tu, F func) const;

    template<typename ... Ts, typename F>
    void for_each_in_tuple_ (std::tuple<Ts...> &tu, F func);
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#  ifdef DMS_INCLUDE_SOURCE
#    include "DataFrame_misc.tcc"
#    include "DataFrame_set.tcc"
#    include "DataFrame_get.tcc"
#    include "DataFrame_read.tcc"
#    include "DataFrame_opt.tcc"
#    include "DataFrame.tcc"
#  endif // DMS_INCLUDE_SOURCE

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
