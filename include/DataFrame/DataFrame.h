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

#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/DataFrameTypes.h>
#include <DataFrame/Utils/DateTime.h>
#include <DataFrame/Utils/FixedSizeString.h>
#include <DataFrame/Utils/ThreadGranularity.h>
#include <DataFrame/Utils/Utils.h>

#include <array>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <tuple>
#include <typeindex>

// ----------------------------------------------------------------------------

namespace hmdf
{

// I: Index (e.g. Timestamp) type. Although an index column need not
//    necessarily represent time, it could be any built-in or user-defined type
// H: See the static assert below. It can only be either
//    a HeteroVector (typedef'ed to StdDataFrame) or
//    a HeteroView (typedef'ed to DataFrameView) or
//    a HeteroPtrView (typedef'ed to DataFramePtrView)
//
// A DataFrame may contain one index and any number of columns of any built-in
// or user-defined types
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

 public:  // Construction

    using size_type = typename std::vector<DataVec>::size_type;
    using IndexType = I;
    using IndexVecType = typename type_declare<DataVec, IndexType>::type;
    using ColNameType = String64;

    template<typename T>
    using ColumnVecType = typename type_declare<DataVec, T>::type;

    DataFrame() = default;

    // Because of thread safety, these need tender loving care
    //
    DataFrame(const DataFrame &that);
    DataFrame(DataFrame &&that);
    DataFrame &operator= (const DataFrame &that);
    DataFrame &operator= (DataFrame &&that);
    ~DataFrame();

public:  // Load/append/remove interfaces

    // DataFrame has unprotected static data. If you are using DataFrame in a
    // multi-threaded program, you must provide a SpinLock. DataFrame will use
    // your SpinLock to protect its static data.
    // This is done this way, so by default, there is no locking overhead.
    //
    static void
    set_lock (SpinLock *sl);

    static void
    remove_lock ();

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

    void
    remove_column(size_type index);

    // It renames column named from to to. If column from does not exist,
    // it throws an exception
    //
    void
    rename_column(const char *from, const char *to);

    // It changes the type of the named column. The change happens by
    // calling convert_func on each element of named column.
    // NOTE: This will copy data
    //
    // FROM_T:
    //   Current type of the named column
    // TO_T:
    //   New type to be of the named column
    // name:
    //   Column name
    // convert_func:
    //   A function to change each element of named column from FROM_T to TO_T
    //   type. The default is C-style cast
    //
    template<typename FROM_T, typename TO_T>
    void
    retype_column(const char *name,
                  std::function<TO_T (const FROM_T &)> convert_func =
                      [](const FROM_T &val) -> TO_T  {
                          return ((TO_T) (*(&val)));
                      });

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
    // data:
    //   Data vector
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

    // This method creates a column similar to above, but assumes data is
    // bucket or bar values. That means the data vector contains statistical
    // figure(s) for time buckets and must be aligned with the index column
    // at bucket intervals.
    // For example, index column is in minutes unit. The data vector is the
    // sum of 5-minute buckets of some column, or some set not even present in
    // DataFrame. The values in data vector will be aligned with the index
    // column at every 5 minutes. The in-between values will “null_value”.
    //
    // NOTE: The data vector must contain (index size / interval) number of
    //       values or less, if index has values per interval. Otherwise, data
    //       must contain appropriate number of values.
    // NOTE: The index must be in ascending order
    //
    // T:
    //   Type of data being loaded
    // name:
    //   Name of the column
    // data:
    //   Data vector
    // interval:
    //   Bucket interval measured in index units distance
    // start_from_beginning:
    //   If true, the first data value will be associated with the first index
    //   value. If false, the first data value will be associated with index
    //   value interval away from the first index value
    // null_value:
    //   The value to fill the new column in-between intervals. The default is
    //   T version of NaN. For None NaN'able types, it will be default value
    //   for T
    // diff_func:
    //   Function to calculate distance between two index values
    //
    template<typename T>
    size_type
    load_align_column(
        const char *name,
        std::vector<T> &&data,
        size_type interval,
        bool start_from_beginning,
        const T &null_value = hmdf::get_nan<T>(),
        std::function<typename DataFrame<I, H>::size_type (
            const typename DataFrame<I, H>::IndexType &,
            const typename DataFrame<I, H>::IndexType &)> diff_func =
            [](const typename DataFrame<I, H>::IndexType &t_1,
               const typename DataFrame<I, H>::IndexType &t) ->
                   typename DataFrame<I, H>::size_type  {
                return (static_cast<std::size_t>(t - t_1));
            });

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

    // It removes duplicate rows and returns a new DataFrame. Duplication is
    // determined by the given column. remove_dup_spec determines which
    // of the duplicated rows to keep.
    //
    // NOTE: The given column type must be hash-able and must have
    //       equality (==) operator well defined.
    //
    // T:
    //   Type of the first named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name:
    //   Name of the data column
    // include_index:
    //   If true, it includes the index column to determine uniqueness
    // rds:
    //   Determined which of the duplicated columns to keep
    //
    template<typename T, typename ... Ts>
    [[nodiscard]] DataFrame
    remove_duplicates(const char *name,
                      bool include_index,
                      remove_dup_spec rds) const;

    // It removes duplicate rows and returns a new DataFrame. Duplication is
    // determined by the two given columns. remove_dup_spec determines which
    // of the duplicated rows to keep.
    //
    // NOTE: The two given column types must be hash-able and must have
    //       equality (==) operator well defined.
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // include_index:
    //   If true, it includes the index column to determine uniqueness
    // rds:
    //   Determined which of the duplicated columns to keep
    //
    template<typename T1, typename T2, typename ... Ts>
    [[nodiscard]] DataFrame
    remove_duplicates(const char *name1,
                      const char *name2,
                      bool include_index,
                      remove_dup_spec rds) const;

    // Same as above, but there are 3 columns involved
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // name3:
    //   Name of the third data column
    // include_index:
    //   If true, it includes the index column to determine uniqueness
    // rds:
    //   Determined which of the duplicated columns to keep
    //
    template<typename T1, typename T2, typename T3, typename ... Ts>
    [[nodiscard]] DataFrame
    remove_duplicates(const char *name1,
                      const char *name2,
                      const char *name3,
                      bool include_index,
                      remove_dup_spec rds) const;

    // Same as above, but there are 4 columns involved
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // T4:
    //   Type of the fourth named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // name3:
    //   Name of the third data column
    // name4:
    //   Name of the fourth data column
    // include_index:
    //   If true, it includes the index column to determine uniqueness
    // rds:
    //   Determined which of the duplicated columns to keep
    //
    template<typename T1, typename T2, typename T3, typename T4,
             typename ... Ts>
    [[nodiscard]] DataFrame
    remove_duplicates(const char *name1,
                      const char *name2,
                      const char *name3,
                      const char *name4,
                      bool include_index,
                      remove_dup_spec rds) const;

    // Same as above, but there are 5 columns involved
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
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
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
    // include_index:
    //   If true, it includes the index column to determine uniqueness
    // rds:
    //   Determined which of the duplicated columns to keep
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename ... Ts>
    [[nodiscard]] DataFrame
    remove_duplicates(const char *name1,
                      const char *name2,
                      const char *name3,
                      const char *name4,
                      const char *name5,
                      bool include_index,
                      remove_dup_spec rds) const;

    // Same as above, but there are 6 columns involved
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
    // T6:
    //   Type of the sixth named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
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
    // name6:
    //   Name of the sixth data column
    // include_index:
    //   If true, it includes the index column to determine uniqueness
    // rds:
    //   Determined which of the duplicated columns to keep
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename T6, typename ... Ts>
    [[nodiscard]] DataFrame
    remove_duplicates(const char *name1,
                      const char *name2,
                      const char *name3,
                      const char *name4,
                      const char *name5,
                      const char *name6,
                      bool include_index,
                      remove_dup_spec rds) const;

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

    // It fills the missing values in all columns in self by investigating the
    // rhs DataFrame. It attempts to find columns with the same name and type
    // in rhs. If there are such columns in rhs, it fills the missing values
    // in the corresponding columns in self that also have the same index value.
    //
    // NOTE: This means that self and rhs must be aligned/ordered the same way
    //       for all common columns including index column. Otherwise, the
    //       result is either nonsense (or not applied, if index column is in a
    //       different order).
    // NOTE: Self and rhs must have the same type index. The == operator must
    //       be well defined on the index type
    // NOTE: This method does not extend any of the columns in self. It just
    //       fills the holes, if data is present in rhs.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // rhs:
    //   DataFrame to be used to find the missing values in self
    //
    template<typename DF, typename ... Ts>
    void
    fill_missing(const DF &rhs);

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
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
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
    [[nodiscard]] std::future<size_type>
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
    [[nodiscard]] std::future<void>
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
    [[nodiscard]] std::future<void>
    sort_async(const char *name, sort_spec dir);

    template<typename T1, typename T2, typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2);

    template<typename T1, typename T2, typename T3, typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3);

    template<typename T1, typename T2, typename T3, typename T4,
             typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3,
               const char *name4, sort_spec dir4);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3,
               const char *name4, sort_spec dir4,
               const char *name5, sort_spec dir5);

    // This method groups the DataFrame by the named column of type T.
    // The group-by’ing is done by equality.
    // The comparison and equality operators must be well defined for type T.
    // It returns a new DataFrame that has been group-by’ed.
    // The summarization of columns is specified by a list of 3-member-tuples
    // (triples) of the following format:
    //    1. Current DataFrame column name
    //    2. Column name for the new bucketized DataFrame
    //    3. A visitor to aggregate current column to new column
    //
    // You must also specify how the index column is summarized, by providing
    // a visitor.
    // If the named column is other than index column, then the returned
    // DataFrame also has a column with the same name which has the unique
    // values of the named column.
    // Also see bucketize().
    //
    // T:
    //   Type of groupby column. In case if index, it is type of index
    // I_V:
    //   Type of visitor to be used to summarize the index column
    // Ts:
    //   Types of triples to specify the column summarization
    // col_name:
    //   Name of the grouop-by'ing column
    // idx_visitor:
    //   A visitor to specify the index summarization
    // args:
    //   List of triples to specify the column summarization
    //
    template<typename T, typename I_V, typename ... Ts>
    [[nodiscard]] DataFrame
    groupby1(const char *col_name, I_V &&idx_visitor, Ts&& ... args) const;

    // This is the same as above groupby1() but it groups by two columns
    //
    // T1:
    //   Type of first groupby column. In case if index, it is type of index
    // T2:
    //   Type of second groupby column. In case if index, it is type of index
    // I_V:
    //   Type of visitor to be used to summarize the index column
    // Ts:
    //   Types of triples to specify the column summarization
    // col_name1:
    //   Name of the first grouop-by'ing column
    // col_name2:
    //   Name of the second grouop-by'ing column
    // idx_visitor:
    //   A visitor to specify the index summarization
    // args:
    //   List of triples to specify the column summarization
    //
    template<typename T1, typename T2, typename I_V, typename ... Ts>
    [[nodiscard]] DataFrame
    groupby2(const char *col_name1,
             const char *col_name2,
             I_V &&idx_visitor,
             Ts&& ... args) const;

    // This is the same as above groupby2() but it groups by three columns
    //
    // T1:
    //   Type of first groupby column. In case if index, it is type of index
    // T2:
    //   Type of second groupby column. In case if index, it is type of index
    // T3:
    //   Type of third groupby column. In case if index, it is type of index
    // I_V:
    //   Type of visitor to be used to summarize the index column
    // Ts:
    //   Types of triples to specify the column summarization
    // col_name1:
    //   Name of the first grouop-by'ing column
    // col_name2:
    //   Name of the second grouop-by'ing column
    // col_name3:
    //   Name of the third grouop-by'ing column
    // idx_visitor:
    //   A visitor to specify the index summarization
    // args:
    //   List of triples to specify the column summarization
    //
    template<typename T1, typename T2, typename T3,
             typename I_V, typename ... Ts>
    [[nodiscard]] DataFrame
    groupby3(const char *col_name1,
             const char *col_name2,
             const char *col_name3,
             I_V &&idx_visitor,
             Ts&& ... args) const;

    // Same as groupby1() above, but executed asynchronously
    //
    template<typename T, typename I_V, typename ... Ts>
    [[nodiscard]] std::future<DataFrame>
    groupby1_async(const char *col_name,
                   I_V &&idx_visitor,
                   Ts&& ... args) const;

    // Same as groupby2() above, but executed asynchronously
    //
    template<typename T1, typename T2, typename I_V, typename ... Ts>
    [[nodiscard]] std::future<DataFrame>
    groupby2_async(const char *col_name1,
                   const char *col_name2,
                   I_V &&idx_visitor,
                   Ts&& ... args) const;

    // Same as groupby3() above, but executed asynchronously
    //
    template<typename T1, typename T2, typename T3,
             typename I_V, typename ... Ts>
    [[nodiscard]] std::future<DataFrame>
    groupby3_async(const char *col_name1,
                   const char *col_name2,
                   const char *col_name3,
                   I_V &&idx_visitor,
                   Ts&& ... args) const;

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
    [[nodiscard]] StdDataFrame<T>
    value_counts(const char *col_name) const;

    template<typename T>
    [[nodiscard]] StdDataFrame<T>
    value_counts(size_type index) const;

    // It bucketizes the data and index into intervals, based on index values
    // and bucket_type.
    // You must specify how the index column is bucketized, by providing
    // a visitor.
    // You must specify how each column is bucketized, by providing 3-member
    // tuples (triples). Each triple must have the following members:
    //    1. Current DataFrame column name
    //    2. Column name for the new bucketized DataFrame
    //    3. A visitor to aggregate/bucketize current column to new column
    //
    // The result of each bucket will be stored in a new DataFrame and returned.
    // Some data at the end of source columns may not be included in the result
    // columns, because based on bucket_type they may not fit into the bucket.
    // The index of each bucket will be determined by idx_visitor.
    //
    // V:
    //   Type of value to be uased for bucketizing based on bucket_type
    // I_V:
    //   Type of visitor to be used to bucketize the index column
    // Ts:
    //   Types of triples to specify each column's bucketization
    // bt:
    //   bucket_type to specify bucketization logic
    // value:
    //   The value to be uased to bucketize based on bucket_type. For example,
    //   if bucket_type is by_distance, then value is the distance between two
    //   index values. If bucket_type is by_count, then value is an integer
    //   count.
    // idx_visitor:
    //   A visitor to specify the index bucketization
    // args:
    //   Variable argument list of triples as specified above
    //
    template<typename V, typename I_V, typename ... Ts>
    [[nodiscard]] DataFrame
    bucketize(bucket_type bt,
              const V &value,
              I_V &&idx_visitor,
              Ts&& ... args) const;

    // Same as bucketize() above, but executed asynchronously
    //
    template<typename V, typename I_V, typename ... Ts>
    [[nodiscard]] std::future<DataFrame>
    bucketize_async(bucket_type bt,
                    const V &value,
                    I_V &&idx_visitor,
                    Ts&& ... args) const;

    // It transposes the data in the DataFrame.
    // The transpose() is only defined for DataFrame's that have a single
    // data type.
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
    // new_col_names:
    //   A vector of strings, specifying the column names for the new
    //   transposed DataFrame. Its length must equal the number of rows in this
    //   DataFrame. Otherwise an exception is thrown
    //
    template<typename T, typename V>
    [[nodiscard]] DataFrame
    transpose(IndexVecType &&indices, const V &new_col_names) const;

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
    [[nodiscard]] StdDataFrame<IndexType>
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
    [[nodiscard]] StdDataFrame<unsigned int>
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
    [[nodiscard]] StdDataFrame<IndexType>
    concat(const RHS_T &rhs,
           concat_policy cp = concat_policy::all_columns) const;

    // This behaves just lie concat(), but retunrs a view instead of another
    // DataFrame.
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
    //   common_columns: only apply concatenation to the common columns
    //   lhs_and_common_columns: the result will have all the columns in self,
    //                           but only common columns and index are
    //                           concatenated
    //
    template<typename RHS_T, typename ... Ts>
    [[nodiscard]] DataFramePtrView<IndexType>
    concat_view(const RHS_T &rhs,
                concat_policy cp = concat_policy::common_columns) const;

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

    // It shifts all the columns in self up, down, left or right based on
    // shift_policy.
    // When shifting up or down, values that are shifted out will be assigned
    // to NaN.
    // When shifting left or right, columns that are shifted out will be
    // removed from the DataFrame.
    // The index column remains unchanged.
    // If user shifts with periods that is larger than the column length,
    // all values in that column become NaN.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // periods:
    //   Number of periods to shift
    // shift_policy:
    //   Specifies the direction (i.e. up/down, left/right) to shift
    //
    template<typename ... Ts>
    void
    self_shift(size_type periods, shift_policy sp);

    // It is exactly the same as self_shift, but it leaves self unchanged
    // and returns a new DataFrame with columns shifted.
    //
    template<typename ... Ts>
    [[nodiscard]] StdDataFrame<IndexType>
    shift(size_type periods, shift_policy sp) const;

    // This copies the named column into another vector and shifts it up or down
    // and returns it.
    // It is handy to create columns of shifted data in the dataframe for
    // machine-learning analysis
    //
    // T:
    //   Type of the col_name column.
    // col_name:
    //   Name of the column
    // periods:
    //   Number of periods to shift
    // shift_policy:
    //   Specifies the direction. In this case it is only up or down.
    //
    template<typename T>
    [[nodiscard]] std::vector<T>
    shift(const char *col_name, size_type periods, shift_policy sp) const;

    // It rotates all the columns in self up, down, left, or right based on
    // shift_policy.
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
    //   Specifies the direction (i.e. up/down, left/right) to rotate
    //
    template<typename ... Ts>
    void
    self_rotate(size_type periods, shift_policy sp);

    // It is exactly the same as self_rotate, but it leaves self unchanged
    // and returns a new DataFrame with columns rotated.
    //
    template<typename ... Ts>
    [[nodiscard]] StdDataFrame<IndexType>
    rotate(size_type periods, shift_policy sp) const;

public: // Read/access and slicing interfaces

    // Returns true, if there is no data in DataFrame
    //
    [[nodiscard]] bool
    empty() const noexcept;

    // Returns true, if there is no column created in the DataFrame
    //
    [[nodiscard]] bool
    shapeless() const noexcept;

    // It returns a reference to the container of named data column
    // The return type depends on if we are in standard or view mode
    //
    // T:
    //   Data type of the named column
    //
    template<typename T>
    [[nodiscard]] ColumnVecType<T> &
    get_column(const char *name);

    template<typename T>
    [[nodiscard]] ColumnVecType<T> &
    get_column(size_type index);

    // It returns a const reference to the container of named data column
    // The return type depends on if we are in standard or view mode
    //
    // T:
    //   Type of the named column.
    // name:
    //   Name of the column
    //
    template<typename T>
    [[nodiscard]] const ColumnVecType<T> &
    get_column(const char *name) const;

    template<typename T>
    [[nodiscard]] const ColumnVecType<T> &
    get_column(size_type index) const;

    // Returns true if self has the named column, otherwise false
    // NOTE: Even if the column exists, it may not be of the type you expect.
    //
    // name:
    //   Name of the column
    //
    [[nodiscard]] bool
    has_column(const char *name) const;

    [[nodiscard]] bool
    has_column(size_type index) const;

    // This method returns true if the given column follows the given pattern,
    // otherwise it returns false. Epsilon is used for approximation.
    // List of supported patterns are specified in pattern_spec enum in
    // DataFrameTypes.h file.
    //
    // T:
    //   Type of the named column.
    // col_name:
    //   Name of the column
    // pattern:
    //   The pattern we are looking for specified in DataFrameTypes.h
    // epsilon:
    //   An epsilon value to use to match pattern parameters
    //
    template<typename T>
    [[nodiscard]] bool
    pattern_match(const char *col_name,
                  pattern_spec pattern,
                  double epsilon = 0.0) const;

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
    [[nodiscard]] HeteroVector
    get_row(size_type row_num,
            const std::array<const char *, N> col_names) const;

    // This is same as get_row() above. But it always includes all the columns
    // in the returned row. The order is the column creation order. If you
    // have rotated the columns, the creation order has changed. You can always
    // use column_name_to_idx() and column_idx_to_name() methods.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // row_num:
    //   The row number
    //
    template<typename ... Ts>
    [[nodiscard]] HeteroVector
    get_row(size_type row_num) const;

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
    [[nodiscard]] std::vector<T>
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
    [[nodiscard]] DataFrame
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
    [[nodiscard]] DataFrame
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
    [[nodiscard]] DataFrameView<IndexType>
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
    [[nodiscard]] DataFramePtrView<IndexType>
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
    [[nodiscard]] DataFrame
    get_data_by_loc(Index2D<long> range) const;

    // It returns a DataFrame (including the index and data columns)
    // containing the data from locations, specified in locations vector.
    // This function supports Python-like negative indexing. That is why the
    // locations vector type is long.
    //
    // NOTE: The negative indexing is relative to the "index" column, which
    //       may not be the size as all other column.
    //       The returned DataFrame is in the same order as locations parameter
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // locations: List of indices into the index column to copy data
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame
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
    [[nodiscard]] DataFrameView<IndexType>
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
    [[nodiscard]] DataFramePtrView<IndexType>
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
    [[nodiscard]] DataFrame
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
    [[nodiscard]] DataFramePtrView<IndexType>
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
    [[nodiscard]] DataFrame
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
    [[nodiscard]] DataFramePtrView<IndexType>
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
    [[nodiscard]] DataFrame
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
    [[nodiscard]] DataFramePtrView<IndexType>
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
    [[nodiscard]] DataFrame
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
    [[nodiscard]] DataFramePtrView<IndexType>
    get_view_by_rand(random_policy spec, double n, size_type seed = 0) const;

    // It returns a const reference to the index container
    //
    [[nodiscard]] const IndexVecType &
    get_index() const;

    // It returns a reference to the index container
    //
    [[nodiscard]] IndexVecType &
    get_index();

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
    [[nodiscard]] StdDataFrame<T>
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
    [[nodiscard]] DataFrameView<T>
    get_reindexed_view(const char *col_to_be_index,
                       const char *old_index_name = nullptr) const;

    // This method combines the content of column col_name between self and
    // rhs based on the logic in functor. Both self and rhs must contain
    // col_name column with the same type as T.
    // It returns the result as a vector that could be moved into one of the
    // dataframes consequently. The length of the result vector will be the min
    // length of the columns in self and rhs.
    //
    // NOTE: It is the responsibility of the user to make sure both columns
    //       in self and rhs are properly aligned.
    //
    // T:
    //   Type on column col_name. If this is index it would be the same as
    //   IndexType.
    // DF:
    //   Type of the rhs dataframe
    // F:
    //   The functor type
    // col_name:
    //   Name of the column
    // rhs:
    //   An instance of a dataframe that contains the same col_name with the
    //   same type (T)
    // functor:
    //   An instance of the functor with signature:
    //       T func(const T &self_data, const T &rhs_data)
    //
    template<typename T, typename DF, typename F>
    [[nodiscard]] std::vector<T>
    combine(const char *col_name, const DF &rhs, F &functor) const;

    // Same as the combine() above but it combines 3 columns.
    //
    // T:
    //   Type on column col_name. If this is index it would be the same as
    //   IndexType.
    // DF1:
    //   Type of the df1 dataframe
    // DF2:
    //   Type of the df2 dataframe
    // F:
    //   The functor type
    // col_name:
    //   Name of the column
    // df1:
    //   An instance of a dataframe that contains the same col_name with the
    //   same type (T)
    // df2:
    //   Another instance of a dataframe that contains the same col_name with
    //   the same type (T)
    // functor:
    //   An instance of the functor with signature:
    //       T func(const T &self_data, const T &df1_data, const T &df2_data)
    //
    template<typename T, typename DF1, typename DF2, typename F>
    [[nodiscard]] std::vector<T>
    combine(const char *col_name,
            const DF1 &df1,
            const DF2 &df2,
            F &functor) const;

    // Same as the combine() above but it combines 4 columns.
    //
    // T:
    //   Type on column col_name. If this is index it would be the same as
    //   IndexType.
    // DF1:
    //   Type of the df1 dataframe
    // DF2:
    //   Type of the df2 dataframe
    // DF3:
    //   Type of the df3 dataframe
    // F:
    //   The functor type
    // col_name:
    //   Name of the column
    // df1:
    //   An instance of a dataframe that contains the same col_name with the
    //   same type (T)
    // df2:
    //   Another instance of a dataframe that contains the same col_name with
    //   the same type (T)
    // df3:
    //   Another instance of a dataframe that contains the same col_name with
    //   the same type (T)
    // functor:
    //   An instance of the functor with signature:
    //       T func(const T &self_data,
    //              const T &df1_data,
    //              const T &df2_data,
    //              const T &df3_data)
    //
    template<typename T, typename DF1, typename DF2, typename DF3, typename F>
    [[nodiscard]] std::vector<T>
    combine(const char *col_name,
            const DF1 &df1,
            const DF2 &df2,
            const DF3 &df3,
            F &functor) const;

    // This method feeds old_col_name1 and old_col_name2 of types OLD_T1 and
    // OLD_T2 to functor which returns a std::vector<NEW_T> which will be
    // loaded into self as column new_col_name. Both old columns will be
    // removed, if delete_old_cols is true
    // Functor "functor" should implement the logic of consolidating two
    // columns into one. Functor signature is:
    //     template<typename ITR1, typename ITR2>
    //     std::vector<NEW_T> (IndexVecType::const_iterator idx_begin,
    //                         IndexVecType::const_iterator idx_end,
    //                         ITR1 col1_begin, ITR1 col1_end,
    //                         ITR2 col2_begin, ITR2 col2_end);
    //     Where ITR[12] are iterators for columns 1 and 2. They are iterators
    //     of std::vector.
    //
    // NOTE: This method could not be called from views.
    //
    // OLD_T1:
    //   Type of existing column named old_col_name1
    // OLD_T2:
    //   Type of existing column named old_col_name2
    // NEW_T:
    //   Type of the new column new_col_name which is the consolidation of the
    //   two existing columns
    // F:
    //   Type of the consildating functor
    // old_col_name1:
    //   Name of the first existing column
    // old_col_name2:
    //   Name of the second existing column
    // new_col_name:
    //   Name of the new consolidated column
    // functor:
    //   Consolidating functor
    // delete_old_cols:
    //   If true, old columns will be removed
    //
    template<typename OLD_T1, typename OLD_T2, typename NEW_T, typename F>
    void
    consolidate(const char *old_col_name1,
                const char *old_col_name2,
                const char *new_col_name,
                F &functor,
                bool delete_old_cols = true);

    // This is the same as above consolidate(), but it consolidates 3 columns
    // into one.
    // Functor signature is:
    //     template<typename ITR1, typename ITR2, typename ITR3>
    //     std::vector<NEW_T> (IndexVecType::const_iterator idx_begin,
    //                         IndexVecType::const_iterator idx_end,
    //                         ITR1 col1_begin, ITR1 col1_end,
    //                         ITR2 col2_begin, ITR2 col2_end,
    //                         ITR3 col3_begin, ITR3 col3_end);
    //     Where ITR[123] are iterators for columns 1, 2 and 3. They are
    //     iterators of std::vector.
    //
    // NOTE: This method could not be called from views.
    //
    // OLD_T1:
    //   Type of existing column named old_col_name1
    // OLD_T2:
    //   Type of existing column named old_col_name2
    // OLD_T3:
    //   Type of existing column named old_col_name3
    // NEW_T:
    //   Type of the new column new_col_name which is the consolidation of the
    //   two existing columns
    // F:
    //   Type of the consildating functor
    // old_col_name1:
    //   Name of the first existing column
    // old_col_name2:
    //   Name of the second existing column
    // old_col_name3:
    //   Name of the third existing column
    // new_col_name:
    //   Name of the new consolidated column
    // functor:
    //   Consolidating functor
    // delete_old_cols:
    //   If true, old columns will be removed
    //
    template<typename OLD_T1, typename OLD_T2, typename OLD_T3,
             typename NEW_T, typename F>
    void
    consolidate(const char *old_col_name1,
                const char *old_col_name2,
                const char *old_col_name3,
                const char *new_col_name,
                F &functor,
                bool delete_old_cols = true);

    // This is the same as above consolidate(), but it consolidates 4 columns
    // into one.
    // Functor signature is:
    //     template<typename ITR1, typename ITR2, typename ITR3,
    //              typename ITR4>
    //     std::vector<NEW_T> (IndexVecType::const_iterator idx_begin,
    //                         IndexVecType::const_iterator idx_end,
    //                         ITR1 col1_begin, ITR1 col1_end,
    //                         ITR2 col2_begin, ITR2 col2_end,
    //                         ITR3 col3_begin, ITR3 col3_end,
    //                         ITR4 col4_begin, ITR4 col4_end);
    //     Where ITR[1234] are iterators for columns 1, 2, 3, and 4. They are
    //     iterators of std::vector.
    //
    // NOTE: This method could not be called from views.
    //
    // OLD_T1:
    //   Type of existing column named old_col_name1
    // OLD_T2:
    //   Type of existing column named old_col_name2
    // OLD_T3:
    //   Type of existing column named old_col_name3
    // OLD_T4:
    //   Type of existing column named old_col_name4
    // NEW_T:
    //   Type of the new column new_col_name which is the consolidation of the
    //   two existing columns
    // F:
    //   Type of the consildating functor
    // old_col_name1:
    //   Name of the first existing column
    // old_col_name2:
    //   Name of the second existing column
    // old_col_name3:
    //   Name of the third existing column
    // old_col_name4:
    //   Name of the forth existing column
    // new_col_name:
    //   Name of the new consolidated column
    // functor:
    //   Consolidating functor
    // delete_old_cols:
    //   If true, old columns will be removed
    //
    template<typename OLD_T1, typename OLD_T2, typename OLD_T3,
             typename OLD_T4,
             typename NEW_T, typename F>
    void
    consolidate(const char *old_col_name1,
                const char *old_col_name2,
                const char *old_col_name3,
                const char *old_col_name4,
                const char *new_col_name,
                F &functor,
                bool delete_old_cols = true);

    // This is the same as above consolidate(), but it consolidates 5 columns
    // into one.
    // Functor signature is:
    //     template<typename ITR1, typename ITR2, typename ITR3,
    //              typename ITR4, typename ITR5>
    //     std::vector<NEW_T> (IndexVecType::const_iterator idx_begin,
    //                         IndexVecType::const_iterator idx_end,
    //                         ITR1 col1_begin, ITR1 col1_end,
    //                         ITR2 col2_begin, ITR2 col2_end,
    //                         ITR3 col3_begin, ITR3 col3_end,
    //                         ITR4 col4_begin, ITR4 col4_end,
    //                         ITR5 col5_begin, ITR5 col5_end);
    //     Where ITR[12345] are iterators for columns 1, 2, 3, 4, and 5.
    //     They are iterators of std::vector.
    //
    // NOTE: This method could not be called from views.
    //
    // OLD_T1:
    //   Type of existing column named old_col_name1
    // OLD_T2:
    //   Type of existing column named old_col_name2
    // OLD_T3:
    //   Type of existing column named old_col_name3
    // OLD_T4:
    //   Type of existing column named old_col_name4
    // OLD_T5:
    //   Type of existing column named old_col_name5
    // NEW_T:
    //   Type of the new column new_col_name which is the consolidation of the
    //   two existing columns
    // F:
    //   Type of the consildating functor
    // old_col_name1:
    //   Name of the first existing column
    // old_col_name2:
    //   Name of the second existing column
    // old_col_name3:
    //   Name of the third existing column
    // old_col_name4:
    //   Name of the forth existing column
    // old_col_name5:
    //   Name of the fifth existing column
    // new_col_name:
    //   Name of the new consolidated column
    // functor:
    //   Consolidating functor
    // delete_old_cols:
    //   If true, old columns will be removed
    //
    template<typename OLD_T1, typename OLD_T2, typename OLD_T3,
             typename OLD_T4, typename OLD_T5,
             typename NEW_T, typename F>
    void
    consolidate(const char *old_col_name1,
                const char *old_col_name2,
                const char *old_col_name3,
                const char *old_col_name4,
                const char *old_col_name5,
                const char *new_col_name,
                F &functor,
                bool delete_old_cols = true);

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
    multi_visit(Ts ... args) const;

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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over the column in reverse order
    //
    template<typename T, typename V>
    V &
    visit(const char *name, V &visitor, bool in_reverse = false);

    template<typename T, typename V>
    V &
    visit(const char *name, V &visitor, bool in_reverse = false) const;

    // These are identical to above visit() but could execute asynchronously.
    // NOTE: It should be safe to run multiple visits on different columns
    //       at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only visits on the same
    //       column or different columns at the same time
    //
    template<typename T, typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name, V &visitor, bool in_reverse = false);

    template<typename T, typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name, V &visitor, bool in_reverse = false) const;

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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over columns in reverse order
    //
    template<typename T1, typename T2, typename V>
    V &
    visit(const char *name1, const char *name2, V &visitor,
          bool in_reverse = false);

    template<typename T1, typename T2, typename V>
    V &
    visit(const char *name1, const char *name2, V &visitor,
          bool in_reverse = false) const;

    // These are identical to above visit() but could execute asynchronously.
    // NOTE: It should be safe to run multiple visits on different columns
    //       at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only visits on the same
    //       column or different columns at the same time
    //
    template<typename T1, typename T2, typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name1, const char *name2, V &visitor,
                bool in_reverse = false);

    template<typename T1, typename T2, typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name1, const char *name2, V &visitor,
                bool in_reverse = false) const;

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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over columns in reverse order
    //
    template<typename T1, typename T2, typename T3, typename V>
    V &
    visit(const char *name1, const char *name2, const char *name3, V &visitor,
          bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          V &visitor,
          bool in_reverse = false) const;

    // These are identical to above visit() but could execute asynchronously.
    // NOTE: It should be safe to run multiple visits on different columns
    //       at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only visits on the same
    //       column or different columns at the same time
    //
    template<typename T1, typename T2, typename T3, typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name1,
                const char *name2,
                const char *name3,
                V &visitor,
                bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name1,
                const char *name2,
                const char *name3,
                V &visitor,
                bool in_reverse = false) const;

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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over columns in reverse order
    //
    template<typename T1, typename T2, typename T3, typename T4, typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          const char *name4,
          V &visitor,
          bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename T4, typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          const char *name4,
          V &visitor,
          bool in_reverse = false) const;

    // These are identical to above visit() but could execute asynchronously.
    // NOTE: It should be safe to run multiple visits on different columns
    //       at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only visits on the same
    //       column or different columns at the same time
    //
    template<typename T1, typename T2, typename T3, typename T4, typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name1,
                const char *name2,
                const char *name3,
                const char *name4,
                V &visitor,
                bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename T4, typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name1,
                const char *name2,
                const char *name3,
                const char *name4,
                V &visitor,
                bool in_reverse = false) const;

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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over columns in reverse order
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          const char *name4,
          const char *name5,
          V &visitor,
          bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    V &
    visit(const char *name1,
          const char *name2,
          const char *name3,
          const char *name4,
          const char *name5,
          V &visitor,
          bool in_reverse = false) const;

    // These are identical to above visit() but could execute asynchronously.
    // NOTE: It should be safe to run multiple visits on different columns
    //       at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only visits on the same
    //       column or different columns at the same time
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name1,
                const char *name2,
                const char *name3,
                const char *name4,
                const char *name5,
                V &visitor,
                bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    [[nodiscard]] std::future<V &>
    visit_async(const char *name1,
                const char *name2,
                const char *name3,
                const char *name4,
                const char *name5,
                V &visitor,
                bool in_reverse = false) const;

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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over the column in reverse order
    //
    template<typename T, typename V>
    V &
    single_act_visit(const char *name, V &visitor, bool in_reverse = false);

    template<typename T, typename V>
    V &
    single_act_visit(const char *name,
                     V &visitor,
                     bool in_reverse = false) const;

    // These are identical to above single_act_visit() but could execute
    // asynchronously.
    // NOTE: It should be safe to run multiple single_act_visit on different
    //       columns at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only single_act_visit on
    //       the same column or different columns at the same time
    //
    template<typename T1, typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name,
                           V &visitor,
                           bool in_reverse = false);

    template<typename T1, typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name,
                           V &visitor,
                           bool in_reverse = false) const;

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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over columns in reverse order
    //
    template<typename T1, typename T2, typename V>
    V &
    single_act_visit(const char *name1,
                     const char *name2,
                     V &visitor,
                     bool in_reverse = false);

    template<typename T1, typename T2, typename V>
    V &
    single_act_visit(const char *name1,
                     const char *name2,
                     V &visitor,
                     bool in_reverse = false) const;

    // These are identical to above single_act_visit() but could execute
    // asynchronously.
    // NOTE: It should be safe to run multiple single_act_visit on different
    //       columns at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only single_act_visit on
    //       the same column or different columns at the same time
    //
    template<typename T1, typename T2, typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name1,
                           const char *name2,
                           V &visitor,
                           bool in_reverse = false);

    template<typename T1, typename T2, typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name1,
                           const char *name2,
                           V &visitor,
                           bool in_reverse = false) const;

    // This is similar to visit(), but it passes a const reference to the index
    // vector and the 3 named column vectors at once to the functor visitor.
    // This is convenient for calculations that need the whole data vector.
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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over columns in reverse order
    //
    template<typename T1, typename T2, typename T3, typename V>
    V &
    single_act_visit(const char *name1,
                     const char *name2,
                     const char *name3,
                     V &visitor,
                     bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename V>
    V &
    single_act_visit(const char *name1,
                     const char *name2,
                     const char *name3,
                     V &visitor,
                     bool in_reverse = false) const;

    // These are identical to above single_act_visit() but could execute
    // asynchronously.
    // NOTE: It should be safe to run multiple single_act_visit on different
    //       columns at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only single_act_visit on
    //       the same column or different columns at the same time
    //
    template<typename T1, typename T2, typename T3, typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name1,
                           const char *name2,
                           const char *name3,
                           V &visitor,
                           bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name1,
                           const char *name2,
                           const char *name3,
                           V &visitor,
                           bool in_reverse = false) const;

    // This is similar to visit(), but it passes a const reference to the index
    // vector and the 4 named column vectors at once to the functor visitor.
    // This is convenient for calculations that need the whole data vector.
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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over columns in reverse order
    //
    template<typename T1, typename T2, typename T3, typename T4, typename V>
    V &
    single_act_visit(const char *name1,
                     const char *name2,
                     const char *name3,
                     const char *name4,
                     V &visitor,
                     bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename T4, typename V>
    V &
    single_act_visit(const char *name1,
                     const char *name2,
                     const char *name3,
                     const char *name4,
                     V &visitor,
                     bool in_reverse = false) const;

    // These are identical to above single_act_visit() but could execute
    // asynchronously.
    // NOTE: It should be safe to run multiple single_act_visit on different
    //       columns at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only single_act_visit on
    //       the same column or different columns at the same time
    //
    template<typename T1, typename T2, typename T3, typename T4, typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name1,
                           const char *name2,
                           const char *name3,
                           const char *name4,
                           V &visitor,
                           bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename T4, typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name1,
                           const char *name2,
                           const char *name3,
                           const char *name4,
                           V &visitor,
                           bool in_reverse = false) const;

    // This is similar to visit(), but it passes a const reference to the index
    // vector and the 4 named column vectors at once to the functor visitor.
    // This is convenient for calculations that need the whole data vector.
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
    // visitor:
    //   An instance of the visitor
    // in_reverse:
    //   If true, it will iterate over columns in reverse order
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    V &
    single_act_visit(const char *name1,
                     const char *name2,
                     const char *name3,
                     const char *name4,
                     const char *name5,
                     V &visitor,
                     bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    V &
    single_act_visit(const char *name1,
                     const char *name2,
                     const char *name3,
                     const char *name4,
                     const char *name5,
                     V &visitor,
                     bool in_reverse = false) const;

    // These are identical to above single_act_visit() but could execute
    // asynchronously.
    // NOTE: It should be safe to run multiple single_act_visit on different
    //       columns at the same time (as long as the index column is not being
    //       modified).
    // NOTE: It should be safe to run multiple read-only single_act_visit on
    //       the same column or different columns at the same time
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name1,
                           const char *name2,
                           const char *name3,
                           const char *name4,
                           const char *name5,
                           V &visitor,
                           bool in_reverse = false);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename V>
    [[nodiscard]] std::future<V &>
    single_act_visit_async(const char *name1,
                           const char *name2,
                           const char *name3,
                           const char *name4,
                           const char *name5,
                           V &visitor,
                           bool in_reverse = false) const;

public:  // Operators

    // It compares self with rhs. If both have the sanme indices,
    // same number of columns, same names for each column, and all
    // columns are equal, then it returns true. Otherwise it returns false
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<typename ... Ts>
    [[nodiscard]] bool
    is_equal(const DataFrame &rhs) const;

    // It iterates over all indices in rhs and modifies all the data
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
    [[nodiscard]] std::pair<size_type, size_type>
    shape() const;

    // It returns information about each column. The result is a vector of
    // tuples containing each column name, size, and std::type_index(typeid).
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<typename ... Ts>
    [[nodiscard]] std::vector<std::tuple<ColNameType,
                                         size_type,
                                         std::type_index>>
    get_columns_info() const;

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
    [[nodiscard]] MemUsage
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
    // The index type must be increment-able.
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
    // there is no particular order in dictionary fields. But in DataFrame
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
    // iof:
    //   Specifies the I/O format. The default is CSV
    // columns_only:
    //   If true, it won't write the index column
    //
    template<typename S, typename ... Ts>
    bool
    write(S &o, io_format iof = io_format::csv,
          bool columns_only = false) const;

    template<typename ... Ts>
    bool
    write(const char *file_name, io_format iof = io_format::csv,
          bool columns_only = false) const;

    // Same as write() above, but executed asynchronously
    //
    template<typename S, typename ... Ts>
    [[nodiscard]] std::future<bool>
    write_async(S &o, io_format iof = io_format::csv,
                bool columns_only = false) const;

    template<typename ... Ts>
    [[nodiscard]] std::future<bool>
    write_async(const char *file_name, io_format iof = io_format::csv,
                bool columns_only = false) const;

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
    // there is no particular order in dictionary fields. But in DataFrame
    // json:
    //   1) Column “INDEX” must be the first column
    //   2) Fields in column dictionaries must be in N, T, D order
    //
    // file_name:
    //   Complete path to the file
    // iof:
    //   Specifies the I/O format. The default is CSV
    // columns_only:
    //   If true, it won't read the index column. It assumes an index with
    //   matching granularity already exists.
    //
    template<typename S>
    bool
    read(S &in_s, io_format iof = io_format::csv,
         bool columns_only = false);

    bool
    read(const char *file_name, io_format iof = io_format::csv,
         bool columns_only = false);

    // Same as read() above, but executed asynchronously
    //
    [[nodiscard]] std::future<bool>
    read_async(const char *file_name, io_format iof = io_format::csv,
               bool columns_only = false);

    template<typename S>
    [[nodiscard]] std::future<bool>
    read_async(S &in_s, io_format iof = io_format::csv,
               bool columns_only = false);

private:

    template<typename II, typename HH>
    friend class DataFrame;

    using ColNameDict =
        std::unordered_map<ColNameType, size_type, std::hash<VirtualString>>;
    using ColNameList = std::vector<std::pair<ColNameType, size_type>>;

    // Data fields
    //
    DataVecVec      data_ { };       // Vector of Heterogeneous vectors
    IndexVecType    indices_ { };    // Vector
    ColNameDict     column_tb_ { };  // Hash table of name -> vector index

    // This is necessary to have a deterministic column order across all
    // implementations
    //
    ColNameList     column_list_ { };  // Vector of column names and indices

    inline static SpinLock *lock_ { nullptr };  // No lock safety by default

    // Private methods
    //
#   include <DataFrame/Internals/DataFrame_private_decl.h>

    // Visiting functors
    //
#   include <DataFrame/Internals/DataFrame_functors.h>
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// All the implementations
//
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
#  include <DataFrame/Internals/DataFrame_visit.tcc>
#  include <DataFrame/Internals/DataFrame_write.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
