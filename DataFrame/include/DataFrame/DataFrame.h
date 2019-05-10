// Hossein Moein
// September 11, 2017
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#ifndef HMDF_DATAFRAME_HPP
#define HMDF_DATAFRAME_HPP

#include "dllexports/DataFrame_lib_exports.h"

#include "HeteroVector.h"
#include "ThreadGranularity.h"

#include <array>
#include <bitset>
#include <limits>
#include <functional>
#include <map>
#include <vector>
#include <stdexcept>
#include <type_traits>
#include <future>
#include <cstring>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct DataFrameError : public std::runtime_error  {

    DataFrameError (const char *desc) : std::runtime_error (desc)  {   }
};
struct ColNotFound : public DataFrameError  {

    ColNotFound (const char *desc) : DataFrameError (desc)  {   }
};
struct InconsistentData : public DataFrameError  {

    InconsistentData (const char *desc) : DataFrameError (desc)  {   }
};
struct BadRange : public DataFrameError  {

    BadRange (const char *desc) : DataFrameError (desc)  {   }
};
struct NotFeasible : public DataFrameError  {

    NotFeasible (const char *desc) : DataFrameError (desc)  {   }
};
struct NotImplemented : public DataFrameError  {

    NotImplemented (const char *desc) : DataFrameError (desc)  {   }
};

// -------------------------------------

enum class nan_policy : bool  {
    pad_with_nans = true,
    dont_pad_with_nans = false
};

// -------------------------------------

enum class sort_state : bool  {
    sorted = true,
    not_sorted = false
};

// -------------------------------------

enum class join_policy : unsigned char  {
    inner_join = 1,
    left_join = 2,
    right_join = 3,
    left_right_join = 4  // This is merge
};

// -------------------------------------

// This policy is relative to a tabular data structure
// There is no right or left shift (like Pandas), because columns in DataFrame
// have no ordering. They can only be accessed by name
enum class shift_policy : unsigned char  {
    down = 1,  // Shift/rotate the content of all columns down,
               // keep index unchanged
    up = 2,    // Shift/rotate the content of all columns up,
               // keep index unchanged
};

// -------------------------------------

enum class fill_policy : unsigned char  {
    value = 1,
    fill_forward = 2,
    fill_backward = 3,

    //           X - X1
    // Y = Y1 + ------- * (Y2 - Y1)
    //          X2 - X1
    linear_interpolate = 4,  // Using the index as X coordinate
    linear_extrapolate = 5   // Using the index as X coordinate
};

// -------------------------------------

enum class drop_policy : unsigned char  {
    all = 1,  // Remove row if all columns are nan
    any = 2,  // Remove row if any column is nan
    threshold = 3  // Remove row if threshold number of columns are nan
};

// -------------------------------------

enum class io_format : unsigned char  {
    csv = 1,
};

// -------------------------------------

// It represents a range with begin and end within a continuous memory space
//
template<typename T>
struct Index2D  {
    T   begin {};
    T   end {};
};

// -------------------------------------

template<typename T, typename U>
struct type_declare;

template<typename U>
struct type_declare<HeteroVector, U>  { using type = std::vector<U>; };

template<typename U>
struct type_declare<HeteroView, U>  { using type = VectorView<U>; };

// -------------------------------------


template<typename TS, typename HETERO>
class DataFrame;

template<typename TS>
using StdDataFrame = DataFrame<TS, HeteroVector>;

template<typename TS>
using DataFrameView = DataFrame<TS, HeteroView>;

// ----------------------------------------------------------------------------

// TS: Index(e.g. Timestamp) type. Although an index column need not necessarily
//     represent time. Basically TS could be any built-in or user-defined type.
// HETERO: See the static assert below. It can only be either
//         a HeteroVector (StdDataFrame) or a HeteroView (DataFrameView)
// A DataFrame can contain columns of any built-in or user-defined types.
//
template<typename TS, typename HETERO>
class DataFrame : public ThreadGranularity  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value ||
                      std::is_base_of<HeteroView, HETERO>::value,
                  "HETERO argument can only be either of "
                  "HeteroVector or HeteroView or their derived types");

    using DataVec = HETERO;
    using DataVecVec = std::vector<DataVec>;

    friend DataFrameView<TS>;
    friend StdDataFrame<TS>;

public:

    using size_type = typename std::vector<DataVec>::size_type;
    using TimeStamp = TS;
    using TSVec = typename type_declare<HETERO, TS>::type;

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
    DataVecVec  data_ { };     // Vector of Heterogeneous vectors
    TSVec       indices_ { };  // Vector
    DataTable   data_tb_ { };  // Hash table of name -> vector index

public:  // Load/append/remove interfaces

    // It creates an empty column named name
    //
    // T: Type of column being added
    //
    template<typename T>
    std::vector<T> &create_column (const char *name);

    // It removes a column named name.
    // The actual data vector is not deleted, but the column is dropped from
    // DataFrame
    //
    void remove_column (const char *name);

    // It renames column named from to to. If column from does not exists,
    // it throws an exception
    //
    void rename_column (const char *from, const char *to);

    // This is the most generalized load function. It creates and loads an
    // index and a variable number of columns. The index vector and all
    // column vectors are "moved" to DataFrame.
    //
    // Ts: The list of types for columns in args
    // indices: A vector of indices of type TimeStamp;
    // args: A variable list of arguments consisting of
    //       std::pair(<const char *name, std::vector<T> &&data>).
    //       Each pair, represents a column data and its name
    //
    template<typename ... Ts>
    size_type load_data (TSVec &&indices, Ts&& ... args);

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
    // range: The begin and end iterators for data
    // padding: If true, it pads the data column with nan,
    //          if it is shorter than the index column.
    //
    template<typename T, typename ITR>
    size_type load_column(const char *name,
                          Index2D<const ITR &> range,
                          nan_policy padding = nan_policy::pad_with_nans);

    // It moves the data to the named column in DataFrame.
    // If column does not exist, it will be created. If the column exist,
    // it will be over written.
    //
    // T: Type of data being moved
    // name: Name of the column
    // padding: If true, it pads the data column with nan,
    //          if it is shorter than the index column.
    //
    template<typename T>
    size_type
    load_column(const char *name,
                std::vector<T> &&data,
                nan_policy padding = nan_policy::pad_with_nans);

    // It appends val to the end of the index column.
    //
    size_type append_index(const TimeStamp &val);

    // It appends val to the end of the named data column.
    // If data column doesn't exist, it throws an exception.
    //
    // T: Type of the named data column
    // name: Name of the column
    // padding: If true, it pads the data column with nan,
    //          if it is shorter than the index column.
    //
    template<typename T>
    size_type append_column(const char *name,
                            const T &val,
                            nan_policy padding = nan_policy::pad_with_nans);

    // It appends the range begin to end to the end of the index column
    //
    // ITR: Type of the iterator
    // range: The begin and end iterators for data
    //
    template<typename ITR>
    size_type append_index(Index2D<const ITR &> range);

    // It appends the range begin to end to the end of the named data column.
    // If data column doesn't exist, it throws an exception.
    //
    // T: Type of the named data column
    // ITR: Type of the iterator
    // name: Name of the column
    // range: The begin and end iterators for data
    // padding: If true, it pads the data column with nan,
    //          if it is shorter than the index column.
    //
    template<typename T, typename ITR>
    size_type append_column(const char *name,
                            Index2D<const ITR &> range,
                            nan_policy padding = nan_policy::pad_with_nans);

    // It removes the data rows from index begin to index end.
    // DataFrame must be sorted by index or behavior is undefined.
    // This function first calls make_consistent() that may add nan values to
    // data columns.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // range: The begin and end iterators for index specified with index values
    //
    template<typename ... types>
    void remove_data_by_idx (Index2D<TS> range);

    // It removes the data rows from location begin to location end
    // within range.
    // This function supports Python-like negative indexing. That is why the
    // range type is int.
    // This function first calls make_consistent() that may add nan values to
    // data columns.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // range: The begin and end iterators for data
    //
    template<typename ... types>
    void remove_data_by_loc (Index2D<int> range);

public:  // Other public interfaces

    // It fills all the "missing values" with the given values, and/or using
    // the given method.
    // Missing is determined by being NaN for types that have NaN. For types
    // without NaN (e.g. string), default value is considered missing value.
    //
    // T: Type of the column(s) in col_names array
    // N: Size of col_names and values array
    // col_names: An array of names specifying the columns to fill.
    // fp:: Specifies the method to use to fill the missing values.
    //      For example; forward fill, values, etc.
    // values: If the policy is "values", use these values to fill the missing
    //         holes. Each value corresponds to the same index in the
    //         col_names array.
    // limit: Specifies how many values to fill. Default is -1 meaning fill
    //        all missing values.
    //
    template<typename T, size_t N>
    void fill_missing(const std::array<const char *, N> col_names,
                      fill_policy policy,
                      const std::array<T, N> values = { },
                      int limit = -1);

    // It removes a row if any or all or some of the columns are NaN, based
    // on drop policy
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // threshold: If drop policy is threshold, it specifies the numbers of
    //            NaN columns before removing the row.
    //
    template<typename ... types>
    void drop_missing(drop_policy policy, size_type threshold = 0);

    // Make all data columns the same length as the index.
    // If any data column is shorter than the index column, it will be padded
    // by nan.
    // This is also called by sort(), before sorting
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    //
    template<typename ... types>
    void make_consistent ();

    // Sort the DataFrame by the named column. By default, it sorts
    // by index (i.e. by_name == nullptr).
    // Sort first calls make_consistent() that may add nan values to
    // data columns.
    // nan values make sorting nondeterministic.
    //
    // T: Type of the by_name column. You always must specify this type,
    //    even if it is being sorted by the default index
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
    // func: The functor to do the groupby. To see an example of func, refer
    //       to GroupbySum in DataFrameVisitors.h file
    // already_sorted: If the DataFrame is already sorted by gb_col_name,
    //                 this will save the expensive sort operation
    //
    template<typename F, typename T, typename ... types>
    DataFrame groupby(F &&func,
                      const char *gb_col_name = nullptr,
                      sort_state already_sorted = sort_state::not_sorted) const;

    // Same as groupby() above, but executed asynchronously
    //
    template<typename F, typename T, typename ... types>
    std::future<DataFrame>
    groupby_async (F &&func,
                   const char *gb_col_name = nullptr,
                   sort_state already_sorted = sort_state::not_sorted) const;

    // It counts the unique values in the named column.
    // It returns a StdDataFrame of following specs:
    // 1) The index is of type T and contains all unique values in
    //    the named column.
    // 2) There is only one column named "counts" of type size_type that
    //    contains the count for each index row.
    // For this method to compile and work, 3 conditions must be met:
    // 1) Type T must be hashable. If this is a user defined type, you
    //    must enable and specialize std::hash.
    // 2) The equality operator (==) must be well defined for type T.
    // 3) Type T must match the actual type of the named column.
    // Of course, if you never call this method in your application,
    // you need not be worried about these conditions.
    //
    // T: Type of the col_name column.
    //
    template<typename T>
    StdDataFrame<T> value_counts (const char *col_name) const;

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
    // func: The functor to do summarization and bucktization.
    // bucket_interval: Bucket interval is in the index's single value unit.
    //                  For example if index is in minutes, bucket_interval
    //                  will be in the unit of minutes and so on.
    //
    template<typename F, typename ... types>
    DataFrame bucketize (F &&func, const TimeStamp &bucket_interval) const;

    // Same as bucketize() above, but executed asynchronously
    //
    template<typename F, typename ... types>
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

    // It transposes the data in the DataFrame.
    // The transpose() is only defined for DataFrame's that have a single
    // data type.
    // NOTE: Since DataFrame columns have no ordering, the user must specify
    //       the order with current_col_order.
    //
    // T: The single type for all data columns
    // V: The type of string vector specifying the new names for new columns
    //    after transpose
    // indices: A vector on indices for the new transposed DataFrame.
    //          Its length must equal the number of rows in this DataFrame.
    //          Otherwise an exception is thrown
    // current_col_order: A vector of strings specifying the order of columns
    //                    in the original DataFrame.
    // new_col_names: A vector of strings, specifying the column names for the
    //                new transposed DataFrame.
    //                Its length must equal the number of rows in this
    //                DataFrame. Otherwise an exception is thrown
    //
    template<typename T, typename V>
    DataFrame transpose(TSVec &&indices,
                        const V &current_col_order,
                        const V &new_col_names) const;

    // It joins the data between self (lhs) and rhs and returns the joined data
    // in a StdDataFrame, based on specification in join_policy.
    // The following conditions must be meet for this method
    // to compile and work properly:
    // 1) TS type must be the same between lhs and rhs.
    // 2) Ordering (< > != ==) must be well defined for type TS
    // 3) Both lhs and rhs must be sorted by index
    // 4) In both lhs and rhs, columns with the same name must have the same
    //    type
    //
    // RHS_T: Type of DataFrame rhs
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // rhs: The rhs DataFrame
    // join_policy: Specifies how to join. For example inner join,
    //              or left join, etc. (See join_policy definition)
    //
    template<typename RHS_T, typename ... types>
    StdDataFrame<TS> join_by_index (const RHS_T &rhs, join_policy jp) const;

    // It shifts all the columns in self up or down based on shift_policy.
    // Values that are shifted will be assigned to NaN. The index column
    // remains unchanged.
    // If user shifts with periods that is larger than the column length,
    // all values in that column become NaN.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // periods: Number of periods to shift
    // shift_policy: Specifies the direction (i.e. up/down) to shift
    //
    template<typename ... types>
    void self_shift (size_type periods, shift_policy sp);

    // It is exactly the same as self_shift, but it leaves self unchanged
    // and returns a new DataFrame with columns shifted.
    //
    template<typename ... types>
    StdDataFrame<TS> shift (size_type periods, shift_policy sp) const;

    // It rotates all the columns in self up or down based on shift_policy.
    // The index column remains unchanged.
    // If user rotates with periods that is larger than the column length,
    // the behavior is undefined.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // periods: Number of periods to rotate
    // shift_policy: Specifies the direction (i.e. up/down) to rotate
    //
    template<typename ... types>
    void self_rotate (size_type periods, shift_policy sp);

    // It is exactly the same as self_rotate, but it leaves self unchanged
    // and returns a new DataFrame with columns rotated.
    //
    template<typename ... types>
    StdDataFrame<TS> rotate (size_type periods, shift_policy sp) const;

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
    // iof: Specifies the I/O format. The default is CSV
    //
    template<typename S, typename ... types>
    bool write (S &o,
                bool values_only = false,
                io_format iof = io_format::csv) const;

    // Same as write() above, but executed asynchronously
    //
    template<typename S, typename ... Ts>
    std::future<bool> write_async (S &o,
                                   bool values_only = false,
                                   io_format iof = io_format::csv) const;

    // It inputs the contents of a text file into itself (i.e. DataFrame).
    // The format of the file must be:
    //     INDEX:<Comma delimited list of values>
    //     <Column1 name>:<Column1 type>:<Comma delimited list of values>
    //     <Column2 name>:<Column2 type>:<Comma delimited list of values>
    // All empty lines or lines starting with # will be skipped.
    //
    // file_name: Complete path to the file
    // iof: Specifies the I/O format. The default is CSV
    //
    bool read (const char *file_name, io_format iof = io_format::csv);

    // Same as read() above, but executed asynchronously
    //
    std::future<bool> read_async (const char *file_name,
                                  io_format iof = io_format::csv);

public: // Read/access interfaces

    // It returns a reference to the container of named data column
    // The return type depends on if we are in standard or view mode
    //
    // T: Data type of the named column
    //
    template<typename T>
    typename type_declare<HETERO, T>::type &
    get_column (const char *name);

    // It returns a const reference to the container of named data column
    // The return type depends on if we are in standard or view mode
    //
    // T: Data type of the named column
    //
    template<typename T>
    const typename type_declare<HETERO, T>::type &
    get_column (const char *name) const;

    // It returns the data in row row_num for columns in col_names.
    // The order of data items in the returned vector is the same as order
    // of columns on col_names.
    // The first item in the returned vector is always the index value
    // corresponding to the row_num
    // It returns a HeteroVector which contains a different type
    // for each column.
    //
    // N: Size of col_names and values array
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // row_num: The row number
    // col_names: Names of columns to get data from. It also specifies the
    //            order of data in the returned vector
    //
    template<size_t N, typename ... types>
    HeteroVector get_row(size_type row_num,
                         const std::array<const char *, N> col_names) const;

    // It returns a vector of unique values in the named column in the same
    // order that exists in the column.
    // For this method to compile and work, 3 conditions must be met:
    // 1) Type T must be hashable. If this is a user defined type, you
    //    must enable and specialize std::hash.
    // 2) The equality operator (==) must be well defined for type T.
    // 3) Type T must match the actual type of the named column.
    // Of course, if you never call this method in your application,
    // you need not be worried about these conditions.
    //
    // T: Data type of the named column
    //
    template<typename T>
    std::vector<T> get_col_unique_values(const char *name) const;

    // It returns a DataFrame (inc     the index and data columns)
    // containing the data from index begin to index end.
    // DataFrame must be sorted by index or behavior is undefined.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // range: The begin and end iterators for index specified with index values
    //
    template<typename ... types>
    DataFrame get_data_by_idx (Index2D<TS> range) const;

    // It behaves like get_data_by_idx(), but it returns a DataFrameView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    // Note: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // range: The begin and end iterators for index specified with index values
    //
    template<typename ... types>
    DataFrameView<TS> get_view_by_idx (Index2D<TS> range);

    // It returns a DataFrame (including the index and data columns)
    // containing the data from location begin to location end within range.
    // This function supports Python-like negative indexing. That is why the
    // range type is int.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // range: The begin and end iterators for data
    //
    template<typename ... types>
    DataFrame get_data_by_loc (Index2D<int> range) const;

    // It behaves like get_data_by_loc(), but it returns a DataFrameView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    // Note: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    //
    // types: List all the types of all data columns.
    //        A type should be specified in the list only once.
    // range: The begin and end iterators for data
    //
    template<typename ... types>
    DataFrameView<TS> get_view_by_loc (Index2D<int> range);

    // It returns a const reference to the index container
    //
    inline const TSVec &get_index () const  { return (indices_); }

    // It returns a reference to the index container
    //
    inline TSVec &get_index ()  { return (indices_); }

    // This is the most generalized visit function. It visits multiple
    // columns with the corresponding function objects sequentially.
    // Each function object is passed every single value of the given
    // column along with its name and the corresponding index value.
    // All functions objects must have this signature
    //     bool (const TimeStamp &i, const char *name, [const] T &col_value)
    // If the function object returns false, the DataFrame will stop iterating
    // at that point on that column.
    // NOTE: This method could be used to implement a pivot table.
    //
    // Ts: The list of types for columns in args
    // args: A variable list of arguments consisting of
    //       std::pair(<const char *name,
    //       &std::function<bool (const TimeStamp &,
    //                            const char *,
    //                            [const] T &)>).
    //       Each pair represents a column name and the functor to run on it.
    // NOTE: The second member of pair is a _pointer_ to the function or
    //       functor object
    //
    template<typename ... Ts>
    void multi_visit (Ts ... args);

    // It passes the values of each index and each named column to the
    // functor visitor sequentially from beginning to end
    // NOTE: This method could be used to implement a pivot table.
    //
    // T: Type of the named column
    // V: Type of the visitor functor
    // name: Name of the data column
    //
    template<typename T, typename V>
    V &visit (const char *name, V &visitor);

    // It passes the values of each index and the two named columns to the
    // functor visitor sequentially from beginning to end
    // NOTE: This method could be used to implement a pivot table.
    //
    // T1: Type of the first named column
    // T2: Type of the second named column
    // V: Type of the visitor functor
    // name1: Name of the first data column
    // name2: Name of the second data column
    //
    template<typename T1, typename T2, typename V>
    V &visit (const char *name1, const char *name2, V &visitor);

    // It passes the values of each index and the three named columns to the
    // functor visitor sequentially from beginning to end
    // NOTE: This method could be used to implement a pivot table.
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
    V &visit (const char *name1,
              const char *name2,
              const char *name3,
              V &visitor);

    // It passes the values of each index and the four named columns to the
    // functor visitor sequentially from beginning to end
    // NOTE: This method could be used to implement a pivot table.
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
    V &visit (const char *name1,
              const char *name2,
              const char *name3,
              const char *name4,
              V &visitor);

    // It passes the values of each index and the five named columns to the
    // functor visitor sequentially from beginning to end
    // NOTE: This method could be used to implement a pivot table.
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
    V &visit (const char *name1,
              const char *name2,
              const char *name3,
              const char *name4,
              const char *name5,
              V &visitor);

    // This is similar to visit(), but it passes a const reference to the index
    // vector and the named column vector at once the functor visitor.
    // This is convenient for calculations that need the whole data vector,
    // for example auto-correlation.
    //
    // T: Type of the named column
    // V: Type of the visitor functor
    // name: Name of the data column
    //
    template<typename T, typename V>
    V &single_act_visit (const char *name, V &visitor) const;

    // This is similar to visit(), but it passes a const reference to the index
    // vector and the two named column vectors at once the functor visitor.
    // This is convenient for calculations that need the whole data vector.
    // NOTE: This method could be used to implement a pivot table.
    //
    // T1: Type of the first named column
    // T2: Type of the second named column
    // V: Type of the visitor functor
    // name1: Name of the first data column
    // name2: Name of the second data column
    //
    template<typename T1, typename T2, typename V>
    V &single_act_visit (const char *name1, const char *name2, V &visitor);

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
    DataFrame &modify_by_idx(
        DataFrame &rhs,
        sort_state already_sorted = sort_state::not_sorted);

private:  // Friend Operators

    template<typename DF, template<typename> class OPT, typename ... types>
    friend DF binary_operation(const DF &lhs, const DF &rhs);

protected:

    template<typename T1, typename T2>
    size_type _load_pair(std::pair<T1, T2> &col_name_data);

    template<typename T>
    static inline constexpr T _get_nan();

    template<typename T>
    static inline constexpr bool _is_nan(const T &val);

private:  // Static helper functions

    template<typename T>
    static void
    fill_missing_value_(std::vector<T> &vec,
                        const T &value,
                        int limit,
                        size_type col_num);

    template<typename T>
    static void
    fill_missing_ffill_(std::vector<T> &vec, int limit, size_type col_num);

    template<typename T>
    static void fill_missing_bfill_(std::vector<T> &vec, int limit);

    template<typename T,
             typename std::enable_if<std::is_arithmetic<T>::value &&
                                     std::is_arithmetic<TS>::value>::type* =
                 nullptr>
    static void
    fill_missing_linter_(std::vector<T> &vec, const TSVec &inedx, int limit);

    // Maps row number -> number of missing column(s)
    using DropRowMap = std::map<size_type, size_type>;

    template<typename T,
             typename std::enable_if<! std::is_arithmetic<T>::value ||
                                     ! std::is_arithmetic<TS>::value>::type* =
                 nullptr>
    static void
    fill_missing_linter_(std::vector<T> &, const TSVec &, int);

    template<typename T>
    static void drop_missing_rows_(T &vec,
                                   const DropRowMap missing_row_map,
                                   drop_policy policy,
                                   size_type threshold,
                                   size_type col_num);

    template<typename T, typename ITR>
    void setup_view_column_(const char *name, Index2D<ITR> range);

    using IndexIdxVector = std::vector<std::tuple<size_type, size_type>>;

    template<typename LHS_T, typename RHS_T, typename ... types>
    static StdDataFrame<TS>
    join_helper_(const LHS_T &lhs,
                 const RHS_T &rhs,
                 const IndexIdxVector &joined_index_idx);

    template<typename LHS_T, typename RHS_T, typename ... types>
    static StdDataFrame<TS>
    index_inner_join_(const LHS_T &lhs, const RHS_T &rhs);

    template<typename LHS_T, typename RHS_T, typename ... types>
    static StdDataFrame<TS>
    index_left_join_(const LHS_T &lhs, const RHS_T &rhs);

    template<typename LHS_T, typename RHS_T, typename ... types>
    static StdDataFrame<TS>
    index_right_join_(const LHS_T &lhs, const RHS_T &rhs);

    template<typename LHS_T, typename RHS_T, typename ... types>
    static StdDataFrame<TS>
    index_left_right_join_(const LHS_T &lhs, const RHS_T &rhs);

    template<typename V>
    static void shift_right_(V &vec, size_type n);

    template<typename V>
    static void shift_left_(V &vec, size_type n);

    template<typename V>
    static void rotate_right_(V &vec, size_type n);

    template<typename V>
    static void rotate_left_(V &vec, size_type n);

    // Visiting functors
#   include "DataFrame_functors.h"

    private:  // Tuple stuff

    template<typename ... Ts, typename F, std::size_t ... Is>
    static void for_each_in_tuple_ (const std::tuple<Ts ...> &tu,
                                    F func,
                                    std::index_sequence<Is ...>);

    template<typename ... Ts, typename F, std::size_t ... Is>
    static void for_each_in_tuple_ (std::tuple<Ts ...> &tu,
                                    F func,
                                    std::index_sequence<Is ...>);

    template<typename ... Ts, typename F>
    static void for_each_in_tuple_ (const std::tuple<Ts...> &tu, F func);

    template<typename ... Ts, typename F>
    static void for_each_in_tuple_ (std::tuple<Ts...> &tu, F func);
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#  ifndef DMS_NOINCLUDE_SOURCE
#    include "DataFrame_misc.tcc"
#    include "DataFrame_set.tcc"
#    include "DataFrame_get.tcc"
#    include "DataFrame_read.tcc"
#    include "DataFrame_opt.tcc"
#    include "DataFrame_join.tcc"
#    include "DataFrame_shift.tcc"
#    include "DataFrame.tcc"
#  endif // DMS_NOINCLUDE_SOURCE

#endif //HMDF_DATAFRAME_HPP

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
