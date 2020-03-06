// Hossein Moein
// September 11, 2017
/*
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#pragma once

#include <DataFrame/DataFrameTypes.h>
#include <DataFrame/Utils/DateTime.h>
#include <DataFrame/Utils/FixedSizeString.h>
#include <DataFrame/Utils/ThreadGranularity.h>

#include <array>
#include <fstream>
#include <future>
#include <map>

// ----------------------------------------------------------------------------

namespace hmdf
{

// I: Index (e.g. Timestamp) type. Although an index column need not necessarily
//    represent time, it could be any built-in or user-defined type.
// H: See the static assert below. It can only be either
//    a HeteroVector (typedef'ed below to StdDataFrame) or
//    a HeteroView (typedef'ed below to DataFrameView) or
//    a HeteroPtrView (typedef'ed below to DataFramePtrView)
//
// A DataFrame may contain one index and any number of columns of any built-in
// or user-defined types.
//
template<typename I, typename H>
class LIBRARY_API DataFrame : public ThreadGranularity {

    static_assert(std::is_base_of<HeteroVector, H>::value ||
                      std::is_base_of<HeteroView, H>::value ||
                      std::is_base_of<HeteroPtrView, H>::value,
                  "H argument can only be either of "
                  "HeteroVector, HeteroView, HeteroPtrView "
                  "or their derived types");

    using DataVec = H;
    using DataVecVec = std::vector<DataVec>;

    template<typename II, typename HH>
    friend class DataFrame;

public:

    using size_type = typename std::vector<DataVec>::size_type;
    using IndexType = I;
    using IndexVecType = typename type_declare<DataVec, IndexType>::type;

    DataFrame() = default;
    DataFrame(const DataFrame &) = default;
    DataFrame(DataFrame &&) = default;
    DataFrame &operator= (const DataFrame &) = default;
    DataFrame &operator= (DataFrame &&) = default;

    ~DataFrame() = default;

private:

    using ColNameType = String64;
    using ColumnTable =
        std::unordered_map<ColNameType, size_type, std::hash<VirtualString>>;

    // Data fields
    //
    DataVecVec      data_ { };       // Vector of Heterogeneous vectors
    IndexVecType    indices_ { };    // Vector
    ColumnTable     column_tb_ { };  // Hash table of name -> vector index

    inline static SpinLock *lock_ { nullptr };    // It is null by default

public:  // Load/append/remove interfaces

    // DataFrame has unprotected static data. If you are using DataFrame in a
    // multi-threaded program, you must provide a SpinLock. DataFrame will use
    // your SpinLock to protect its static data.
    // This is done this way, so by default, there is no locking overhead.
    //
    static void set_lock (SpinLock *sl)  { lock_ = sl; }
    static void remove_lock ()  { lock_ = nullptr; }

    // It creates an empty column named name
    //
    // T:
    //   Type of column being added
    //
    template<typename T>
    std::vector<T> &
    create_column(const char *name);

    // It removes a column named name.
    // The actual data vector is not deleted, but the column is dropped from
    // DataFrame
    //
    void
    remove_column(const char *name);

    // It renames column named from to to. If column from does not exist,
    // it throws an exception
    //
    void
    rename_column(const char *from, const char *to);

    // This is the most generalized load function. It creates and loads an
    // index and a variable number of columns. The index vector and all
    // column vectors are "moved" to DataFrame.
    //
    // Ts:
    //   The list of types for columns in args
    // indices:
    //   A vector of indices of type IndexType;
    // args:
    //   A variable list of arguments consisting of
    //     std::pair(<const char *name, std::vector<T> &&data>).
    //   Each pair, represents a column data and its name
    //
    template<typename ... Ts>
    size_type
    load_data(IndexVecType &&indices, Ts&& ... args);

    // It copies the data from iterators begin to end into the index column
    //
    // ITR:
    //   Type of the iterator
    //
    template<typename ITR>
    size_type
    load_index(const ITR &begin, const ITR &end);

    // It moves the idx vector into the index column.
    //
    size_type
    load_index(IndexVecType &&idx);

    // It copies the data from iterators begin to end to the named column.
    // If column does not exist, it will be created. If the column exists,
    // it will be over written.
    //
    // T:
    //   Type of data being copied
    // ITR:
    //   Type of the iterator
    // name:
    //   Name of the column
    // range:
    //   The begin and end iterators for data
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<typename T, typename ITR>
    size_type
    load_column(const char *name,
                Index2D<const ITR &> range,
                nan_policy padding = nan_policy::pad_with_nans);

    // It moves the data to the named column in DataFrame.
    // If column does not exist, it will be created. If the column exists,
    // it will be over written.
    //
    // T:
    //   Type of data being moved
    // name:
    //   Name of the column
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<typename T>
    size_type
    load_column(const char *name,
                std::vector<T> &&data,
                nan_policy padding = nan_policy::pad_with_nans);

    template<typename T>
    size_type
    load_column(const char *name,
                const std::vector<T> &data,
                nan_policy padding = nan_policy::pad_with_nans);

    // It appends val to the end of the index column.
    //
    size_type
    append_index(const IndexType &val);

    // It appends val to the end of the named data column.
    // If data column doesn't exist, it throws an exception.
    //
    // T:
    //   Type of the named data column
    // name:
    //   Name of the column
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<typename T>
    size_type
    append_column(const char *name,
                  const T &val,
                  nan_policy padding = nan_policy::pad_with_nans);

    // It appends the range begin to end to the end of the index column
    //
    // ITR:
    //   Type of the iterator
    // range:
    //   The begin and end iterators for data
    //
    template<typename ITR>
    size_type
    append_index(Index2D<const ITR &> range);

    // It appends the range begin to end to the end of the named data column.
    // If data column doesn't exist, it throws an exception.
    //
    // T:
    //   Type of the named data column
    // ITR:
    //   Type of the iterator
    // name:
    //   Name of the column
    // range:
    //   The begin and end iterators for data
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<typename T, typename ITR>
    size_type
    append_column(const char *name,
                  Index2D<const ITR &> range,
                  nan_policy padding = nan_policy::pad_with_nans);

    // It removes the data rows from index begin to index end.
    // DataFrame must be sorted by index or behavior is undefined.
    // This function first calls make_consistent() that may add nan values to
    // data columns.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // range:
    //   The begin and end iterators for index specified with index values
    //
    template<typename ... Ts>
    void
    remove_data_by_idx(Index2D<IndexType> range);

    // It removes the data rows from location begin to location end
    // within range.
    // This function supports Python-like negative indexing. That is why the
    // range type is long.
    // This function first calls make_consistent() that may add nan values to
    // data columns.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // range:
    //   The begin and end iterators for data
    //
    template<typename ... Ts>
    void
    remove_data_by_loc(Index2D<long> range);

    // It removes data rows by boolean filtering selection via the sel_functor
    // (e.g. a functor, function, or lambda).
    // Each element of the named column along with its corresponding index
    // is passed to the sel_functor. If sel_functor returns true, that row
    // will be removed.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &, const T &)
    //
    // NOTE: If the selection logic results in empty column(s), the empty
    //       column(s) will _not_ be padded with NaN's. You can always call
    //       make_consistent() afterwards to make all columns into consistent
    //       length
    //
    // T:
    //   Type of the named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name:
    //   Name of the data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T, typename F, typename ... Ts>
    void
    remove_data_by_sel(const char *name, F &sel_functor);

    // This does the same function as above remove_data_by_sel() but operating
    // on two columns.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &, const T1 &, const T2 &)
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename F, typename ... Ts>
    void
    remove_data_by_sel(const char *name1, const char *name2, F &sel_functor);

    // This does the same function as above remove_data_by_sel() but operating
    // on three columns.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &, const T1 &, const T2 &, const T3 &)
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // name3:
    //   Name of the third data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename T3, typename F,
             typename ... Ts>
    void
    remove_data_by_sel(const char *name1,
                       const char *name2,
                       const char *name3,
                       F &sel_functor);

public:  // Data manipulation

    // It randomly shuffles the named column(s) non-deterministically.
    //
    // also_shuffle_index: If true, it shuffles the named column(s) and the
    //                     index column. Otherwise, index is not shuffled.
    // N:
    //   Number of named columns
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<size_t N, typename ... Ts>
    void
    shuffle(const std::array<const char *, N> col_names,
            bool also_shuffle_index);

    // It fills all the "missing values" with the given values, and/or using
    // the given method.
    // Missing is determined by being NaN for types that have NaN. For types
    // without NaN (e.g. string), default value is considered missing value.
    //
    // T:
    //   Type of the column(s) in col_names array
    // N:
    //   Size of col_names and values array
    // col_names:
    //   An array of names specifying the columns to fill.
    // fp:
    //   Specifies the method to use to fill the missing values. For example;
    //   forward fill, values, etc.
    // values:
    //   If the policy is "values", use these values to fill the missing
    //   holes. Each value corresponds to the same index in the col_names array.
    // limit:
    //   Specifies how many values to fill. Default is -1 meaning fill all
    //   missing values.
    //
    template<typename T, size_t N>
    void
    fill_missing(const std::array<const char *, N> col_names,
                 fill_policy policy,
                 const std::array<T, N> values = { },
                 int limit = -1);

    // It removes a row if any or all or some of the columns are NaN, based
    // on drop policy
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // threshold:
    //   If drop policy is threshold, it specifies the numbers of NaN columns
    //   before removing the row.
    //
    template<typename ... Ts>
    void
    drop_missing(drop_policy policy, size_type threshold = 0);

    // It iterates over the column named col_name and replaces all values
    // in old_values with the corresponding values in new_values up to the
    // limit. If limit is omitted, all values will be replaced.
    // It returns number of items replaced.
    //
    // T:
    //   Type on column col_name. If this is index it would be the same as
    //   IndexType.
    // N:
    //   Size of old_values and new_values arrays
    // col_name:
    //   Name of the column
    // old_array:
    //   An array of values to be replaced in col_name column
    // new_array:
    //   An array of values to to replace the old_values in col_name column
    // limit:
    //   Limit of how many items to replace. Default is to replace all.
    //
    template<typename T, size_t N>
    size_type
    replace(const char *col_name,
            const std::array<T, N> old_values,
            const std::array<T, N> new_values,
            int limit = -1);

    // Same as replace() above, but executed asynchronously
    //
    // NOTE: multiple instances of replace_async() maybe executed for
    //       different columns at the same time with no problem.
    //
    template<typename T, size_t N>
    std::future<size_type>
    replace_async(const char *col_name,
                  const std::array<T, N> old_values,
                  const std::array<T, N> new_values,
                  int limit = -1);

    // This is similar to replace() above but it lets a functor replace the
    // values in the named column. The functor is passed every value of the
    // column along with a const reference of the corresponding index value.
    // Unlike the replace version above, this replace can only work on data
    // columns. It will not work on index column.
    // The functor must have the following interface at minimum:
    //     bool operator() (const IndexType &ts, T &value);
    // A false return from the above operator method stops the iteration
    // through named column values.
    //
    // T:
    //   Type on column col_name. If this is index it would be the same as
    //   IndexType.
    // F:
    //   The functor type
    // col_name:
    //   Name of the column
    // functor:
    //   An instance of the functor
    //
    template<typename T, typename F>
    void
    replace(const char *col_name, F &functor);

    // Same as replace() above, but executed asynchronously
    //
    // NOTE: multiple instances of replace_async() maybe executed for
    //       different columns at the same time with no problem.
    //
    template<typename T, typename F>
    std::future<void>
    replace_async(const char *col_name, F &functor);

    // This does the same thing as replace() above for the index column
    //
    // N: Size of old_values and new_values arrays
    // old_array: An array of values to be replaced in col_name column
    // new_array: An array of values to to replace the old_values in col_name
    //            column
    // limit:
    //   Limit of how many items to replace. Default is to replace all.
    //
    template<size_t N>
    size_type
    replace_index(const std::array<IndexType, N> old_values,
                  const std::array<IndexType, N> new_values,
                  int limit = -1);

    // Sort the DataFrame by the named column. If name equals "INDEX" or
    // DF_INDEX_COL_NAME, it sorts by index. Otherwise it sorts by the named
    // column.
    // NOTE: Sort first calls make_consistent() that may add nan values to
    //       data columns.
    //       nan values make sorting nondeterministic.
    //
    // T:
    //   Type of the named column. You always must specify this type,
    //   even if it is being sorted by the index.
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name:
    //   The name of a column or string "INDEX" or DF_INDEX_COL_NAME
    // dir:
    //   Direction of sorting, ascending or descending
    //
    template<typename T, typename ... Ts>
    void
    sort(const char *name, sort_spec dir);

    // This sort function sorts DataFrame based on two columns, also
    // specified by the two directions.
    // name[n] could be "INDEX" (or DF_INDEX_COL_NAME) in whcih case the index
    // column is taken as the sorting column.
    // NOTE: The order of T[n] type specifications must match the order of
    //       name[n] column names. In addition, all column types must be
    //       specified separately. Otherwise, the behavior is undefined.
    // NOTE: Sort first calls make_consistent() that may add nan values to
    //       data columns.
    //       nan values make sorting nondeterministic.
    //
    // T1:
    //   Type of the first named column. You always must specify this type,
    //   even if it is being sorted by the index.
    // T2:
    //   Type of the second named column. You always must specify this type,
    //   even if it is being sorted by the index.
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first column or string "INDEX" or DF_INDEX_COL_NAME
    // name2:
    //   Name of the second column or string "INDEX" or DF_INDEX_COL_NAME
    // dir1:
    //   Direction of sorting for the first column
    // dir2:
    //   Direction of sorting for the second column
    //
    template<typename T1, typename T2, typename ... Ts>
    void
    sort(const char *name1, sort_spec dir1,
         const char *name2, sort_spec dir2);

    // This sort function is similar to above, but it uses 3 columns
    //
    template<typename T1, typename T2, typename T3, typename ... Ts>
    void
    sort(const char *name1, sort_spec dir1,
         const char *name2, sort_spec dir2,
         const char *name3, sort_spec dir3);

    // This sort function is similar to above, but it uses 4 columns
    //
    template<typename T1, typename T2, typename T3, typename T4,
             typename ... Ts>
    void
    sort(const char *name1, sort_spec dir1,
         const char *name2, sort_spec dir2,
         const char *name3, sort_spec dir3,
         const char *name4, sort_spec dir4);

    // This sort function is similar to above, but it uses 5 columns
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename ... Ts>
    void
    sort(const char *name1, sort_spec dir1,
         const char *name2, sort_spec dir2,
         const char *name3, sort_spec dir3,
         const char *name4, sort_spec dir4,
         const char *name5, sort_spec dir5);

    // Same as sort() above, but executed asynchronously
    //
    template<typename T, typename ... Ts>
    std::future<void>
    sort_async(const char *name, sort_spec dir);

    template<typename T1, typename T2, typename ... Ts>
    std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2);

    template<typename T1, typename T2, typename T3, typename ... Ts>
    std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3);

    template<typename T1, typename T2, typename T3, typename T4,
             typename ... Ts>
    std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3,
               const char *name4, sort_spec dir4);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename ... Ts>
    std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3,
               const char *name4, sort_spec dir4,
               const char *name5, sort_spec dir5);

    // Groupby copies the DataFrame into a temp DataFrame and sorts
    // the temp df by gb_col_name before performing groupby.
    // if gb_col_name is null, it groups by index.
    //
    // F:
    //   type functor to be applied to columns to group by
    // T:
    //   type of the groupby column. In case if index, it is type of index
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // func:
    //   The functor to do the groupby. To see an example of func, refer to
    //   GroupbySum in DataFrameStatsVisitors.h file
    // gb_col_name:
    //   Name of the column
    // already_sorted:
    //   If the DataFrame is already sorted by gb_col_name, this will save the
    //   expensive sort operation
    //
    template<typename F, typename T, typename ... Ts>
    DataFrame
    groupby(F &&func,
            const char *gb_col_name = nullptr,
            sort_state already_sorted = sort_state::not_sorted) const;

    // Same as groupby() above, but executed asynchronously
    //
    template<typename F, typename T, typename ... Ts>
    std::future<DataFrame>
    groupby_async(F &&func,
                  const char *gb_col_name = nullptr,
                  sort_state already_sorted = sort_state::not_sorted) const;

    // It counts the unique values in the named column.
    // It returns a StdDataFrame of following specs:
    //   1) The index is of type T and contains all unique values in
    //      the named column.
    //   2) There is only one column named "counts" of type size_type that
    //      contains the count for each index row.
    // For this method to compile and work, 3 conditions must be met:
    //   1) Type T must be hashable. If this is a user defined type, you
    //      must enable and specialize std::hash.
    //   2) The equality operator (==) must be well defined for type T.
    //   3) Type T must match the actual type of the named column.
    // Of course, if you never call this method in your application,
    // you need not be worried about these conditions.
    //
    // T:
    //   Type of the col_name column.
    // col_name:
    //   Name of the column
    //
    template<typename T>
    StdDataFrame<T>
    value_counts(const char *col_name) const;

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
    // F:
    //   type functor to be applied to columns to bucketize
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // func:
    //   The functor to do summarization and bucktization.
    // bucket_interval:
    //   Bucket interval is in the index's single value unit. For example if
    //   index is in minutes, bucket_interval will be in the unit of minutes
    //   and so on.
    //
    template<typename F, typename ... Ts>
    DataFrame
    bucketize(F &&func, const IndexType &bucket_interval) const;

    // Same as bucketize() above, but executed asynchronously
    //
    template<typename F, typename ... Ts>
    std::future<DataFrame>
    bucketize_async(F &&func, const IndexType &bucket_interval) const;

    // This is exactly the same as bucketize() above. The only difference is
    // it stores the result in itself and returns void.
    // So after the return the original data is lost and replaced with
    // bucketized data
    //
    // NOTE:The DataFrame must already be sorted by index.
    //
    template<typename F, typename ... Ts>
    void
    self_bucketize(F &&func, const IndexType &bucket_interval);

    // It transposes the data in the DataFrame.
    // The transpose() is only defined for DataFrame's that have a single
    // data type.
    //
    // NOTE: Since DataFrame columns have no ordering, the user must specify
    //       the order with current_col_order.
    //
    // T:
    //   The single type for all data columns
    // V:
    //   The type of string vector specifying the new names for new columns
    //    after transpose
    // indices:
    //   A vector on indices for the new transposed DataFrame. Its length must
    //   equal the number of rows in this DataFrame. Otherwise an exception is
    //   thrown
    // current_col_order:
    //   A vector of strings specifying the order of columns in the original
    //   DataFrame.
    // new_col_names:
    //   A vector of strings, specifying the column names for the new
    //   transposed DataFrame. Its length must equal the number of rows in this
    //   DataFrame. Otherwise an exception is thrown
    //
    template<typename T, typename V>
    DataFrame
    transpose(IndexVecType &&indices,
              const V &current_col_order,
              const V &new_col_names) const;

    // It joins the data between self (lhs) and rhs and returns the joined data
    // in a StdDataFrame, based on specification in join_policy.
    // The following conditions must be meet for this method
    // to compile and work properly:
    //   1) IndexType type must be the same between lhs and rhs.
    //   2) Ordering (< > != ==) must be well defined for type IndexType
    //   3) In both lhs and rhs, columns with the same name must have the same
    //      type
    //
    // RHS_T:
    //   Type of DataFrame rhs
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // rhs:
    //   The rhs DataFrame
    // join_policy:
    //   Specifies how to join. For example inner join, or left join, etc.
    //   (See join_policy definition)
    //
    template<typename RHS_T, typename ... Ts>
    StdDataFrame<IndexType>
    join_by_index(const RHS_T &rhs, join_policy jp) const;

    // It joins the data between self (lhs) and rhs and returns the joined data
    // in a StdDataFrame, based on specification in join_policy.
    // The returned DataFrame is indexed by a sequence of unsigned integers from
    // 0 to N. The returned DataFrame will at least have two columns names
    // lhs.INDEX and rhs.INDEX containing the lhs and rhs indices based on join
    // policy.
    // The following conditions must be meet for this method
    // to compile and work properly:
    //   1) Ordering (< > != ==) must be well defined for the type of the
    //      named column.
    //   2) Both lhs and rhs must contain the named column
    //   3) In both lhs and rhs, columns with the same name must have the same
    //      type
    //
    // RHS_T:
    //   Type of DataFrame rhs
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // rhs:
    //   The rhs DataFrame
    // name:
    //   Name of the column which the join will be based on
    // join_policy:
    //   Specifies how to join. For example inner join, or left join, etc.
    //   (See join_policy definition)
    //
    template<typename RHS_T, typename T, typename ... Ts>
    StdDataFrame<unsigned int>
    join_by_column(const RHS_T &rhs, const char *name, join_policy jp) const;

    // It concatenates rhs to the end of self and returns the result as
    // another DataFrame.
    // Concatenation is done based on policy
    //
    // RHS_T:
    //   Type of DataFrame rhs
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // rhs:
    //   The rhs DataFrame
    // cp:
    //   Concatenation policy:
    //   all_columns: concatenate all columns. If a column does not exist in
    //                self, create one in the result and prepend with nan
    //   common_columns: only apply concatenation to the common columns
    //   lhs_and_common_columns: the result will have all the columns in self,
    //                           but only common columns and index are
    //                           concatenated
    //
    template<typename RHS_T, typename ... Ts>
    StdDataFrame<IndexType>
    concat(const RHS_T &rhs,
           concat_policy cp = concat_policy::all_columns) const;

    // This is similar to concat() method but it is applied to self. It changes
    // self.
    //
    // RHS_T:
    //   Type of DataFrame rhs
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // rhs:
    //   The rhs DataFrame
    // add_new_columns:
    //   If true, it creates new columns in self and prepend them with nan
    //
    template<typename RHS_T, typename ... Ts>
    void
    self_concat(const RHS_T &rhs, bool add_new_columns = true);

    // It shifts all the columns in self up or down based on shift_policy.
    // Values that are shifted will be assigned to NaN. The index column
    // remains unchanged.
    // If user shifts with periods that is larger than the column length,
    // all values in that column become NaN.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // periods:
    //   Number of periods to shift shift_policy: Specifies the direction
    //   (i.e. up/down) to shift
    //
    template<typename ... Ts>
    void
    self_shift(size_type periods, shift_policy sp);

    // It is exactly the same as self_shift, but it leaves self unchanged
    // and returns a new DataFrame with columns shifted.
    //
    template<typename ... Ts>
    StdDataFrame<IndexType>
    shift(size_type periods, shift_policy sp) const;

    // It rotates all the columns in self up or down based on shift_policy.
    // The index column remains unchanged.
    // If user rotates with periods that is larger than the column length,
    // the behavior is undefined.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // periods:
    //   Number of periods to rotate
    // shift_policy:
    //   Specifies the direction (i.e. up/down) to rotate
    //
    template<typename ... Ts>
    void
    self_rotate(size_type periods, shift_policy sp);

    // It is exactly the same as self_rotate, but it leaves self unchanged
    // and returns a new DataFrame with columns rotated.
    //
    template<typename ... Ts>
    StdDataFrame<IndexType>
    rotate(size_type periods, shift_policy sp) const;

public: // Read/access and slicing interfaces

    // It returns a reference to the container of named data column
    // The return type depends on if we are in standard or view mode
    //
    // T:
    //   Data type of the named column
    //
    template<typename T>
    typename type_declare<DataVec, T>::type &
    get_column(const char *name);

    // It returns a const reference to the container of named data column
    // The return type depends on if we are in standard or view mode
    //
    // T:
    //   Type of the named column.
    // name:
    //   Name of the column
    //
    template<typename T>
    const typename type_declare<DataVec, T>::type &
    get_column(const char *name) const;

    // Returns true if self has the named column, otherwise false
    // NOTE: Even if the column exists, it may not be of the type you expect.
    //
    // name:
    //   Name of the column
    //
    bool
    has_column(const char *name) const;

    // It returns the data in row row_num for columns in col_names.
    // The order of data items in the returned vector is the same as order
    // of columns on col_names.
    // The first item in the returned vector is always the index value
    // corresponding to the row_num
    // It returns a HeteroVector which contains a different type
    // for each column.
    //
    // N:
    //   Size of col_names and values array
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // row_num:
    //   The row number
    // col_names:
    //   Names of columns to get data from. It also specifies the order of data
    //   in the returned vector
    //
    template<size_t N, typename ... Ts>
    HeteroVector
    get_row(size_type row_num,
            const std::array<const char *, N> col_names) const;

    // It returns a vector of unique values in the named column in the same
    // order that exists in the column.
    // For this method to compile and work, 3 conditions must be met:
    //   1) Type T must be hashable. If this is a user defined type, you
    //      must enable and specialize std::hash.
    //   2) The equality operator (==) must be well defined for type T.
    //   3) Type T must match the actual type of the named column.
    // Of course, if you never call this method in your application,
    // you need not be worried about these conditions.
    //
    // T:
    //   Data type of the named column
    //
    template<typename T>
    std::vector<T>
    get_col_unique_values(const char *name) const;

    // It returns a DataFrame (including the index and data columns)
    // containing the data from index begin to index end.
    // DataFrame must be sorted by index or behavior is undefined.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // range:
    //   The begin and end iterators for index specified with index values
    //
    template<typename ... Ts>
    DataFrame
    get_data_by_idx(Index2D<IndexType> range) const;

    // It returns a DataFrame (including the index and data columns)
    // containing the data corresponding to the indices specified in "values"
    // vector.
    // This method runs in O(n), where n is the number of indices, by
    // creating a hash table of values. IndexType must be hashable.
    //
    // NOTE: The returned DataFrame is in the same order as original DataFrame
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // values:
    //   List of indices to copy data from
    //
    template<typename ... Ts>
    DataFrame
    get_data_by_idx(const std::vector<IndexType> &values) const;

    // It behaves like get_data_by_idx(range), but it returns a DataFrameView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // range:
    //   The begin and end iterators for index specified with index values
    //
    template<typename ... Ts>
    DataFrameView<IndexType>
    get_view_by_idx(Index2D<IndexType> range) const;

    // It behaves like get_data_by_idx(values), but it returns a
    // DataFramePtrView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // values:
    //   List of indices to copy data from
    //
    template<typename ... Ts>
    DataFramePtrView<IndexType>
    get_view_by_idx(const std::vector<IndexType> &values) const;

    // It returns a DataFrame (including the index and data columns)
    // containing the data from location begin to location end within range.
    // This function supports Python-like negative indexing. That is why the
    // range type is long.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // range:
    //   The begin and end iterators for data
    //
    template<typename ... Ts>
    DataFrame
    get_data_by_loc(Index2D<long> range) const;

    // It returns a DataFrame (including the index and data columns)
    // containing the data from locations, specified in locations vector.
    // This function supports Python-like negative indexing. That is why the
    // locations vector type is long.
    //
    // NOTE: The negative indixing is relative to the "index" column, which
    //       may not be the size as all other column.
    //       The returned DataFrame is in the same order as locations parameter
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // locations: List of indices into the index column to copy data
    //
    template<typename ... Ts>
    DataFrame
    get_data_by_loc(const std::vector<long> &locations) const;

    // It behaves like get_data_by_loc(range), but it returns a DataFrameView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // range:
    //   The begin and end iterators for data
    //
    template<typename ... Ts>
    DataFrameView<IndexType>
    get_view_by_loc(Index2D<long> range) const;

    // It behaves like get_data_by_loc(locations), but it returns a
    // DataFramePtrView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // locations:
    //   List of indices into the index column to copy data
    //
    template<typename ... Ts>
    DataFramePtrView<IndexType>
    get_view_by_loc(const std::vector<long> &locations) const;

    // This method does boolean filtering selection via the sel_functor
    // (e.g. a functor, function, or lambda). It returns a new DataFrame.
    // Each element of the named column along with its corresponding index
    // is passed to the sel_functor. If sel_functor returns true, that index
    // is selected and all the elements of all column for that index will be
    // included in the returned DataFrame.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &, const T &)
    //
    // NOTE: If the selection logic results in empty column(s), the result
    //       empty columns will _not_ be padded with NaN's. You can always
    //       call make_consistent() on the original or result DataFrame to make
    //       all columns into consistent length
    //
    // T:
    //   Type of the named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name:
    //   Name of the data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T, typename F, typename ... Ts>
    DataFrame
    get_data_by_sel(const char *name, F &sel_functor) const;

    // This is identical with above get_data_by_sel(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // T:
    //   Type of the named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name:
    //   Name of the data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T, typename F, typename ... Ts>
    DataFramePtrView<IndexType>
    get_view_by_sel(const char *name, F &sel_functor) const;

    // This does the same function as above get_data_by_sel() but operating
    // on two columns.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &, const T1 &, const T2 &)
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename F, typename ... Ts>
    DataFrame
    get_data_by_sel(const char *name1,
                    const char *name2,
                    F &sel_functor) const;

    // This is identical with above get_data_by_sel(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename F, typename ... Ts>
    DataFramePtrView<IndexType>
    get_view_by_sel(const char *name1, const char *name2, F &sel_functor) const;

    // This does the same function as above get_data_by_sel() but operating
    // on three columns.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &, const T1 &, const T2 &, const T3 &)
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // name3:
    //   Name of the third data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename T3, typename F,
             typename ... Ts>
    DataFrame
    get_data_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    F &sel_functor) const;

    // This is identical with above get_data_by_sel(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // F:
    //   Type of the selecting functor
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // name3:
    //   Name of the third data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename T3, typename F,
             typename ... Ts>
    DataFramePtrView<IndexType>
    get_view_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    F &sel_functor) const;

    // It returns a DataFrame (including the index and data columns)
    // containing the data from uniform random selection.
    // random_policy determines the behavior of method.
    //
    // NOTE: The actual number of rows returned might be smaller than
    //       requested. That is because the random process might produce
    //       the same number more than once.
    // NOTE: The columns in the result are not padded with NaN.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // random_policy:
    //   Please see random_policy in DataFrameTypes.h. It specifies how this
    //   function should proceed.
    // n:
    //   Depending on the random policy, it is either the number of rows to
    //   sample or a fraction of rows to sample. In case of fraction, for
    //   example 0.4 means 40% of rows.
    // seed:
    //   depending on the random policy, user could specify a seed. The same
    //   seed should always produce the same random selection.
    //
    template<typename ... Ts>
    DataFrame
    get_data_by_rand(random_policy spec, double n, size_type seed = 0) const;

    // It behaves like get_data_by_rand(), but it returns a DataFrameView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: The columns in the result are not padded with NaN.
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // random_policy:
    //   Please see random_policy in DataFrameTypes.h. It specifies how this
    //   function should proceed.
    // n:
    //   Depending on the random policy, it is either the number of rows to
    //   sample or a fraction of rows to sample. In case of fraction, for
    //   example 0.4 means 40% of rows.
    // seed:
    //   depending on the random policy, user could specify a seed. The same
    //   seed should always produce the same random selection.
    //
    template<typename ... Ts>
    DataFramePtrView<IndexType>
    get_view_by_rand(random_policy spec, double n, size_type seed = 0) const;

    // It returns a const reference to the index container
    //
    inline const IndexVecType &get_index() const  { return (indices_); }

    // It returns a reference to the index container
    //
    inline IndexVecType &get_index()  { return (indices_); }

    // It creates and returns a new DataFrame which has the col_to_be_index
    // column as the index. If old_index_name is not null, it will be loaded
    // as a regular column in the result under the name old_index_name.
    //
    // NOTE: If the new index column is shorter than other columns, every
    //       column will be cut to that length.
    // NOTE: Columns will not be padded by nan
    //
    // T:
    //   Type of the "new index" column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_to_be_index:
    //   Name of the column you want as the new index. This name will not be a
    //   column in the result anymore
    // old_index_name:
    //   Name of the current index, if converted into a regular column in the
    //   result. If this is null, the current index will not be loaded into
    //   the result as a column.
    //   
    template<typename T, typename ... Ts>
    StdDataFrame<T>
    get_reindexed(const char *col_to_be_index,
                  const char *old_index_name = nullptr) const;

    // This is similar to get_reindexed(), but it returns a view. Please read
    // above for specs.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Although this is a const method, it returns a view. So, the data
    //       could still be modified through the returned view
    //
    // T:
    //   Type of the "new index" column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_to_be_index:
    //   Name of the column you want as the new index. This name will not be a
    //   column in the result anymore
    // old_index_name:
    //   Name of the current index, if converted into a regular column in the
    //   result. If this is null, the current index will not be loaded into
    //   the result as a column.
    //   
    template<typename T, typename ... Ts>
    DataFrameView<T>
    get_reindexed_view(const char *col_to_be_index,
                       const char *old_index_name = nullptr) const;

public:  // Visitors

    // This is the most generalized visit function. It visits multiple
    // columns with the corresponding function objects sequentially.
    // Each function object is passed every single value of the given
    // column along with its name and the corresponding index value.
    // All functions objects must have this signature
    //     bool (const IndexType &i, const char *name, [const] T &col_value)
    // If the function object returns false, the DataFrame will stop iterating
    // at that point on that column.
    //
    // NOTE: This method could be used to implement a pivot table.
    //
    // Ts:
    //   The list of types for columns in args
    // args:
    //   A variable list of arguments consisting of
    //       std::pair(<const char *name,
    //                  &std::function<bool (const IndexType &,
    //                                       const char *,
    //                                       [const] T &)>)
    //   Each pair represents a column name and the functor to run on it.
    //
    // NOTE: The second member of pair is a _pointer_ to the function or
    //       functor object
    //
    template<typename ... Ts>
    void
    multi_visit(Ts ... args);

    template<typename ... Ts>
    void
    multi_visit(Ts ... args) const  {

        const_cast<DataFrame *>(this)->multi_visit<Ts ...>(args ...);
    }

    // It passes the values of each index and each named column to the
    // functor visitor sequentially from beginning to end
    //
    // NOTE: This method could be used to implement a pivot table.
    //
    // T:
    //   Type of the named column
    // V:
    //   Type of the visitor functor
    // name:
    //   Name of the data column
    //
    template<typename T, typename V>
    V &
    visit(const char *name, V &visitor);

    template<typename T, typename V>
    V &
    visit(const char *name, V &visitor) const  {

        return(const_cast<DataFrame *>(this)->visit<T, V>(name, visitor));
    }

    // It passes the values of each index and the two named columns to the
    // functor visitor sequentially from beginning to end
    //
    // NOTE: This method could be used to implement a pivot table.
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // V:
    //   Type of the visitor functor
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    //
    template<typename T1, typename T2, typename V>
    V &
    visit(const char *name1, const char *name2, V &visitor);

    template<typename T1, typename T2, typename V>
    V &
    visit(const char *name1, const char *name2, V &visitor) const  {

        return(const_cast<DataFrame *>(this)->visit<T1, T2, V>
               (name1, name2, visitor));
    }

    // It passes the values of each index and the three named columns to the
    // functor visitor sequentially from beginning to end
    //
    // NOTE: This method could be used to implement a pivot table.
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // V:
    //   Type of the visitor functor
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // name3:
    //   Name of the third data column
    //
    template<typename T1, typename T2, typename T3, typename V>
    V &
    visit(const char *name1, const char *name2, const char *name3, V &visitor);

    template<typename T1, typename T2, typename T3, typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          V &visitor) const  {

        return(const_cast<DataFrame *>(this)->visit<T1, T2, T3, V>
               (name1, name2, name3, visitor));
    }

    // It passes the values of each index and the four named columns to the
    // functor visitor sequentially from beginning to end
    //
    // NOTE: This method could be used to implement a pivot table.
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // T4:
    //   Type of the fourth named column
    // V:
    //   Type of the visitor functor
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // name3:
    //   Name of the third data column
    // name4:
    //   Name of the fourth data column
    //
    template<typename T1, typename T2, typename T3, typename T4, typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          const char *name4,
          V &visitor);

    template<typename T1, typename T2, typename T3, typename T4, typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          const char *name4,
          V &visitor) const  {

        return(const_cast<DataFrame *>(this)->visit<T1, T2, T3, T4, V>
               (name1, name2, name3, name4, visitor));
    }

    // It passes the values of each index and the five named columns to the
    // functor visitor sequentially from beginning to end
    //
    // NOTE: This method could be used to implement a pivot table.
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // T4:
    //   Type of the fourth named column
    // T5:
    //   Type of the fifth named column
    // V:
    //   Type of the visitor functor
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // name3:
    //   Name of the third data column
    // name4:
    //   Name of the fourth data column
    // name5:
    //   Name of the fifth data column
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          const char *name4,
          const char *name5,
          V &visitor);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          const char *name4,
          const char *name5,
          V &visitor) const  {

        return(const_cast<DataFrame *>(this)->visit<T1, T2, T3, T4, T5, V>
               (name1, name2, name3, name4, name5, visitor));
    }

    // This is similar to visit(), but it passes a const reference to the index
    // vector and the named column vector at once the functor visitor.
    // This is convenient for calculations that need the whole data vector,
    // for example auto-correlation.
    //
    // T:
    //   Type of the named column
    // V:
    //   Type of the visitor functor
    // name:
    //   Name of the data column
    //
    template<typename T, typename V>
    V &
    single_act_visit(const char *name, V &visitor);

    template<typename T, typename V>
    V &
    single_act_visit(const char *name, V &visitor) const  {

        return(const_cast<DataFrame *>(this)->single_act_visit<T, V>
               (name, visitor));
    }

    // This is similar to visit(), but it passes a const reference to the index
    // vector and the two named column vectors at once the functor visitor.
    // This is convenient for calculations that need the whole data vector.
    //
    // NOTE: This method could be used to implement a pivot table.
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // V:
    //   Type of the visitor functor
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    //
    template<typename T1, typename T2, typename V>
    V &
    single_act_visit(const char *name1, const char *name2, V &visitor);

    template<typename T1, typename T2, typename V>
    V &
    single_act_visit(const char *name1, const char *name2, V &visitor) const  {

        return(const_cast<DataFrame *>(this)->single_act_visit<T1, T2, V>
               (name1, name2, visitor));
    }

public:  // Operators

    // It compares self with rhs. If both have the sanme indices,
    // same number of columns, same names for each column, and all
    // columns are eual, then it retunrs true. Otherwise it returns false
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<typename ... Ts>
    bool
    is_equal(const DataFrame &rhs) const;

    // It iterates over all indices in rhs and modifyies all the data
    // columns in self that correspond to the given index value.
    // If not already_sorted, both rhs and self will be sorted by index
    // It returns a reference to self
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // already_sorted:
    //   If the self and rhs are already sorted by index, this will save the
    //   expensive sort operations
    //
    template<typename ... Ts>
    DataFrame &
    modify_by_idx(DataFrame &rhs,
                  sort_state already_sorted = sort_state::not_sorted);

public:  // Utilities and miscellaneous

    // Make all data columns the same length as the index.
    // If any data column is shorter than the index column, it will be padded
    // by nan.
    // This is also called by sort(), before sorting
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<typename ... Ts>
    void
    make_consistent();

    // It returns a pair containing number of rows and columns.
    //
    // NOTE: Number of rows is the number of index rows. Not every column
    //       has the same number of rows, necessarily. But each column has,
    //       at most, this number of rows.
    //
    std::pair<size_type, size_type>
    shape() const;

    // It returns the memory used by the given column and index column.
    // All numbers are in bytes.
    // MemUsage is a structure defined in DataFrameTypes.h file.
    //
    // NOTE: The returned values are only estimates. The actual allocated
    //       memory by OS is unknown to any container object.
    //       In other words, the actual memory used might be and probably is
    //       larger than numbers returned by this call.
    //       Also if a type (T) allocates dynamic memory, it is not included
    //       in the result
    //
    // T:
    //   Type of the col_name column.
    // col_name:
    //   Name of the column
    //
    template<typename T>
    MemUsage
    get_memory_usage(const char *col_name) const;

    // This will reclaim unused/reserve memory from all columns including the
    // index.
    // If your DataFrame has grown organically from different data sources,
    // shrink_to_fit() could potentially reduce your memory footprint
    // significantly.
    // After this call, any iterator or reference you hold to any data point
    // in the DataFrame could be invalidated.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<typename ... Ts>
    void
    shrink_to_fit();

    // This static method generates a date/time-based index vector that could
    // be fed directly to one of the load methods. Depending on the specified
    // frequency, it generates specific timestamps (see below).
    // It returns a vector of IndexType timestamps.
    // Currently IndexType could be any built-in numeric type or DateTime
    //
    // start_datetime, end_datetime:
    //   They are the start/end date/times of requested timestamps.
    //   They must be in the following format: MM/DD/YYYY [HH[:MM[:SS[.MMM]]]]
    // t_freq:
    //   Specifies the timestamp frequency. Depending on the frequency, and
    //   IndexType type specific timestamps are generated as follows:
    //   - IndexType type of DateTime always generates timestamps of DateTime.
    //   - Annual, monthly, weekly, and daily frequencies generates YYYYMMDD
    //     timestamps.
    //   - Hourly, minutely, and secondly frequencies generates epoch
    //     timestamps (64 bit).
    //   - Millisecondly frequency generates nano-second since epoch
    //     timestamps (128 bit).
    // increment:
    //   Increment in the units of the frequency
    // tz:
    //   Time-zone of generated timestamps
    //
    // NOTE: It is the responsibility of the programmer to make sure
    //       IndexType type is big enough to contain the frequency.
    //
    static std::vector<IndexType>
    gen_datetime_index(const char *start_datetime,
                       const char *end_datetime,
                       time_frequency t_freq,
                       long increment = 1,
                       DT_TIME_ZONE tz = DT_TIME_ZONE::LOCAL);

    // This static method generates a vector of sequential values of
    // IndexType that could be fed directly to one of the load methods.
    // The values are incremented by "increment".
    // The index type must be incrementable.
    // If by incrementing "start_value" by increment you would never reach
    // "end_value", the behavior will be undefined.
    // It returns a vector of IndexType values.
    //
    // start_value, end_value:
    //   Starting and ending values of IndexType. Start value is included.
    //   End value is excluded.
    // increment:
    //   Increment by value
    //
    static std::vector<IndexType>
    gen_sequence_index(const IndexType &start_value,
                       const IndexType &end_value,
                       long increment = 1);

    // It outputs the content of DataFrame into the stream o.
    // Currently two formats (i.e. csv, json) are supported specified by
    // the iof parameter.
    // The csv file format must be:
    //     INDEX:<Number data points>:<values>
    //     <Col1>:<Number data points>:<Col1 type>:<values>
    //     <Col2>:<Number of data points>:<Col2 type>:<values>
    //     .
    //     .
    //     .
    // All empty lines or lines starting with # will be skipped.
    //
    // The JSON file format looks like this:
    //     {
    //     "INDEX":{"N":3,"T":"ulong","D":[123450,123451,123452]},
    //     "col_3":{"N":3,"T":"double","D":[15.2,16.34,17.764]},
    //     "col_4":{"N":3,"T":"int","D":[22,23,24]},
    //     "col_str":{"N":3,"T":"string","D":["11","22","33"]},
    //     "col_2":{"N":3,"T":"double","D":[8,9.001,10]},
    //     "col_1":{"N":3,"T":"double","D":[1,2,3.456]}
    //     }
    // Please note DataFrame json does not follow json spec 100%. In json,
    // there is not particular order in dictionary fields. But in DataFrame
    // json:
    //   1) Column “INDEX” must be the first column
    //   2) Fields in column dictionaries must be in N, T, D order
    //
    // S:
    //   Output stream type
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // o:
    //   Reference to an streamable object (e.g. cout)
    // values_only:
    //   If true, the name and type of each column is not written
    // iof:
    //   Specifies the I/O format. The default is CSV
    //
    template<typename S, typename ... Ts>
    bool
    write(S &o,
          bool values_only = false,
          io_format iof = io_format::csv) const;

    // Same as write() above, but executed asynchronously
    //
    template<typename S, typename ... Ts>
    std::future<bool>
    write_async(S &o,
                bool values_only = false,
                io_format iof = io_format::csv) const;

    // It inputs the contents of a text file into itself (i.e. DataFrame).
    // Currently two formats (i.e. csv, json) are supported specified by
    // the iof parameter.
    // The csv file format must be:
    //     INDEX:<Number data points>:<values>
    //     <Col1>:<Number data points>:<Col1 type>:<values>
    //     <Col2>:<Number of data points>:<Col2 type>:<values>
    //     .
    //     .
    //     .
    // All empty lines or lines starting with # will be skipped.
    //
    // The JSON file format looks like this:
    //     {
    //     "INDEX":{"N":3,"T":"ulong","D":[123450,123451,123452]},
    //     "col_3":{"N":3,"T":"double","D":[15.2,16.34,17.764]},
    //     "col_4":{"N":3,"T":"int","D":[22,23,24]},
    //     "col_str":{"N":3,"T":"string","D":["11","22","33"]},
    //     "col_2":{"N":3,"T":"double","D":[8,9.001,10]},
    //     "col_1":{"N":3,"T":"double","D":[1,2,3.456]}
    //     }
    // Please note DataFrame json does not follow json spec 100%. In json,
    // there is not particular order in dictionary fields. But in DataFrame
    // json:
    //   1) Column “INDEX” must be the first column
    //   2) Fields in column dictionaries must be in N, T, D order
    //
    // file_name:
    //   Complete path to the file
    // iof:
    //   Specifies the I/O format. The default is CSV
    //
    bool
    read(const char *file_name, io_format iof = io_format::csv);

    // Same as read() above, but executed asynchronously
    //
    std::future<bool>
    read_async(const char *file_name, io_format iof = io_format::csv);

private:  // Friend Operators

    template<typename DF, template<typename> class OPT, typename ... Ts>
    friend DF
    binary_operation(const DF &lhs, const DF &rhs);

protected:

    template<typename T1, typename T2>
    size_type
    _load_pair(std::pair<T1, T2> &col_name_data);

    template<typename T>
    static inline constexpr
    T _get_nan();

    template<typename T>
    static inline constexpr bool
    _is_nan(const T &val);

private:  // Static helper functions

    void read_json_(std::ifstream &file);

    template<typename CF, typename ... Ts>
    static void
    sort_common_(DataFrame<I, H> &df, CF &&comp_func);

    template<typename T>
    static void
    fill_missing_value_(std::vector<T> &vec,
                        const T &value,
                        int limit,
                        size_type col_num);

    template<typename T>
    static void
    fill_missing_ffill_(std::vector<T> &vec, int limit, size_type col_num);

    template<typename T,
             typename std::enable_if<
                 std::is_arithmetic<T>::value &&
                 std::is_arithmetic<IndexType>::value>::type* = nullptr>
    static void
    fill_missing_midpoint_(std::vector<T> &vec, int limit, size_type col_num);

    template<typename T,
             typename std::enable_if<
                 ! std::is_arithmetic<T>::value ||
                 ! std::is_arithmetic<IndexType>::value>::type* = nullptr>
    static void
    fill_missing_midpoint_(std::vector<T> &vec, int limit, size_type col_num);

    template<typename T>
    static void
    fill_missing_bfill_(std::vector<T> &vec, int limit);

    template<typename T,
             typename std::enable_if<
                 std::is_arithmetic<T>::value &&
                 std::is_arithmetic<IndexType>::value>::type* = nullptr>
    static void
    fill_missing_linter_(std::vector<T> &vec,
                         const IndexVecType &inedx,
                         int limit);

    template<typename T,
             typename std::enable_if<
                 ! std::is_arithmetic<T>::value ||
                 ! std::is_arithmetic<IndexType>::value>::type* = nullptr>
    static void
    fill_missing_linter_(std::vector<T> &, const IndexVecType &, int);

    // Maps row number -> number of missing column(s)
    using DropRowMap = std::map<size_type, size_type>;

    template<typename T>
    static void
    drop_missing_rows_(T &vec,
                       const DropRowMap missing_row_map,
                       drop_policy policy,
                       size_type threshold,
                       size_type col_num);

    template<typename T, typename ITR>
    void
    setup_view_column_(const char *name, Index2D<ITR> range);

    using IndexIdxVector = std::vector<std::tuple<size_type, size_type>>;
    template<typename T>
    using JoinSortingPair = std::pair<const T *, size_type>;

    template<typename LHS_T, typename RHS_T, typename IDX_T, typename ... Ts>
    static void
    join_helper_common_(const LHS_T &lhs,
                        const RHS_T &rhs,
                        const IndexIdxVector &joined_index_idx,
                        StdDataFrame<IDX_T> &result,
                        const char *skip_col_name = nullptr);

    template<typename LHS_T, typename RHS_T, typename ... Ts>
    static StdDataFrame<IndexType>
    index_join_helper_(const LHS_T &lhs,
                       const RHS_T &rhs,
                       const IndexIdxVector &joined_index_idx);

    template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
    static StdDataFrame<unsigned int>
    column_join_helper_(const LHS_T &lhs,
                        const RHS_T &rhs,
                        const char *col_name,
                        const IndexIdxVector &joined_index_idx);

    template<typename T>
    static IndexIdxVector
    get_inner_index_idx_vector_(
        const std::vector<JoinSortingPair<T>> &col_vec_lhs,
        const std::vector<JoinSortingPair<T>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename ... Ts>
    static StdDataFrame<IndexType>
    index_inner_join_(
        const LHS_T &lhs, const RHS_T &rhs,
        const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
        const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
    static StdDataFrame<unsigned int>
    column_inner_join_(const LHS_T &lhs,
                       const RHS_T &rhs,
                       const char *col_name,
                       const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                       const std::vector<JoinSortingPair<T>> &col_vec_rhs);

    template<typename T>
    static IndexIdxVector
    get_left_index_idx_vector_(
        const std::vector<JoinSortingPair<T>> &col_vec_lhs,
        const std::vector<JoinSortingPair<T>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename ... Ts>
    static StdDataFrame<IndexType>
    index_left_join_(
        const LHS_T &lhs, const RHS_T &rhs,
        const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
        const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
    static StdDataFrame<unsigned int>
    column_left_join_(const LHS_T &lhs,
                      const RHS_T &rhs,
                      const char *col_name,
                      const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                      const std::vector<JoinSortingPair<T>> &col_vec_rhs);

    template<typename T>
    static IndexIdxVector
    get_right_index_idx_vector_(
        const std::vector<JoinSortingPair<T>> &col_vec_lhs,
        const std::vector<JoinSortingPair<T>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename ... Ts>
    static StdDataFrame<IndexType>
    index_right_join_(
        const LHS_T &lhs, const RHS_T &rhs,
        const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
        const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
    static StdDataFrame<unsigned int>
    column_right_join_(const LHS_T &lhs,
                       const RHS_T &rhs,
                       const char *col_name,
                       const std::vector<JoinSortingPair<T>> &col_vec_lhs,
                       const std::vector<JoinSortingPair<T>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename ... Ts>
    static void
    concat_helper_(LHS_T &lhs, const RHS_T &rhs, bool add_new_columns);

    template<typename T>
    static IndexIdxVector
    get_left_right_index_idx_vector_(
        const std::vector<JoinSortingPair<T>> &col_vec_lhs,
        const std::vector<JoinSortingPair<T>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename ... Ts>
    static StdDataFrame<IndexType>
    index_left_right_join_(
        const LHS_T &lhs, const RHS_T &rhs,
        const std::vector<JoinSortingPair<IndexType>> &col_vec_lhs,
        const std::vector<JoinSortingPair<IndexType>> &col_vec_rhs);

    template<typename LHS_T, typename RHS_T, typename T, typename ... Ts>
    static StdDataFrame<unsigned int>
    column_left_right_join_(
        const LHS_T &lhs,
        const RHS_T &rhs,
        const char *col_name,
        const std::vector<JoinSortingPair<T>> &col_vec_lhs,
        const std::vector<JoinSortingPair<T>> &col_vec_rhs);

    template<typename V>
    static void
    shift_right_(V &vec, size_type n);

    template<typename V>
    static void
    shift_left_(V &vec, size_type n);

    template<typename V>
    static void
    rotate_right_(V &vec, size_type n);

    template<typename V>
    static void
    rotate_left_(V &vec, size_type n);

    // Visiting functors
#   include <DataFrame/Internals/DataFrame_functors.h>

private:  // Tuple stuff

    template<typename ... Ts, typename F, std::size_t ... Is>
    static void
    for_each_in_tuple_(const std::tuple<Ts ...> &tu,
                       F func,
                       std::index_sequence<Is ...>);

    template<typename ... Ts, typename F, std::size_t ... Is>
    static void
    for_each_in_tuple_(std::tuple<Ts ...> &tu,
                       F func,
                       std::index_sequence<Is ...>);

    template<typename ... Ts, typename F>
    static void
    for_each_in_tuple_(const std::tuple<Ts...> &tu, F func);

    template<typename ... Ts, typename F>
    static void
    for_each_in_tuple_(std::tuple<Ts...> &tu, F func);
};

} // namespace hmdf

// ----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Internals/DataFrame_standalone.tcc>
#  include <DataFrame/Internals/DataFrame.tcc>
#  include <DataFrame/Internals/DataFrame_get.tcc>
#  include <DataFrame/Internals/DataFrame_join.tcc>
#  include <DataFrame/Internals/DataFrame_misc.tcc>
#  include <DataFrame/Internals/DataFrame_opt.tcc>
#  include <DataFrame/Internals/DataFrame_read.tcc>
#  include <DataFrame/Internals/DataFrame_set.tcc>
#  include <DataFrame/Internals/DataFrame_shift.tcc>
#  include <DataFrame/Internals/DataFrame_write.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
