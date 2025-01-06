// Hossein Moein
// September 11, 2017
/*
Copyright (c) 2019-2026, Hossein Moein
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
#include <DataFrame/Utils/AlignedAllocator.h>
#include <DataFrame/Utils/Concepts.h>
#include <DataFrame/Utils/DateTime.h>
#include <DataFrame/Utils/FixedSizeString.h>
#include <DataFrame/Utils/Matrix.h>
#include <DataFrame/Utils/Threads/ThreadGranularity.h>
#include <DataFrame/Utils/Utils.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <execution>
#include <fstream>
#include <functional>
#include <future>
#include <ios>
#include <iterator>
#include <limits>
#include <map>
#include <random>
#include <ranges>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <typeindex>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

// I: Index type. It could be any built-in or user-defined type
// H: See the static assert below. It can only be either
//    a HeteroVector (typedef'ed to StdDataFrame) or
//    a HeteroView (typedef'ed to View) or
//    a HeteroConstView (typedef'ed to ConstView) or
//    a HeteroPtrView (typedef'ed to PtrView)
//    a HeteroConstPtrView (typedef'ed to ConstPtrView)
//
// A DataFrame may contain one index and any number of columns of any built-in
// or user-defined types
//
template<typename I, typename H>
class   DataFrame : public ThreadGranularity {

    using DataVec = H;

public:  // Construction and public types

    static constexpr std::size_t   align_value { std::size_t(H::align_value) };

    template<typename T>
    using AllocatorType = typename allocator_declare<T, align_value>::type;

    using size_type = std::size_t;
    using IndexType = I;
    using IndexVecType =
        typename type_declare<DataVec, IndexType, align_value>::type;
    using ColNameType = String64;

    using View = DataFrame<I, HeteroView<align_value>>;
    using ConstView = DataFrame<I, HeteroConstView<align_value>>;
    using PtrView = DataFrame<I, HeteroPtrView<align_value>>;
    using ConstPtrView = DataFrame<I, HeteroConstPtrView<align_value>>;

    template<typename T>
    using ColumnVecType = typename type_declare<DataVec, T, align_value>::type;

    template<typename T>
    using StlVecType = std::vector<T, AllocatorType<T>>;

    using seed_t = std::random_device::result_type;

    DataFrame() = default;

    // Because of thread safety, these need tender loving care
    //
    DataFrame(const DataFrame &that);
    DataFrame(DataFrame &&that);
    DataFrame &operator= (const DataFrame &that);
    DataFrame &operator= (DataFrame &&that);
    ~DataFrame();

    // Any version of DataFrame should be assignable to any other version
    //
    template<typename OTHER, typename ... Ts>
    DataFrame &assign(const OTHER &rhs);
    template<typename OTHER, typename ... Ts>
    DataFrame &assign(OTHER &rhs);

public:  // Load/append/remove interfaces

    // DataFrame has unprotected static data. If you are using DataFrame in a
    // multi threaded program, you must provide a SpinLock. DataFrame will use
    // your SpinLock to protect its static data.
    // This is done this way, so by default, there is no locking overhead.
    //
    static void
    set_lock (SpinLock *sl);

    static SpinLock *
    get_lock ();

    static void
    remove_lock ();

    // It creates an empty column named name
    //
    // T:
    //   Type of column being added
    //
    template<typename T>
    ColumnVecType<T> &
    create_column(const char *name, bool do_lock = true);

    // It removes a column named name.
    //
    // T:
    //   Type of the named column
    // name:
    //   Name of the column
    //
    template<typename T>
    void
    remove_column(const char *name);

    // T:
    //   Type of the indexed column
    // index:
    //   Index of the column
    //
    template<typename T>
    void
    remove_column(size_type index);

    // This removes all the index and data columns but doesn't necessarily
    // free memory space of underlying containers. After this call DataFrame
    // will be empty.
    // It is very similar to std::vector clear()
    //
    void
    clear();

    // This swaps all self's index and data columns with the ones in other
    // It is very similar to std::vector swap()
    //
    // other:
    //   Another DataFrme of the same type
    //
    void
    swap(DataFrame &other);

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
    // it will be overwritten.
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
                nan_policy padding = nan_policy::pad_with_nans,
                bool do_lock = true);

    // It moves the data to the named column in DataFrame.
    // If column does not exist, it will be created. If the column exists,
    // it will be overwritten.
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
                StlVecType<T> &&data,
                nan_policy padding = nan_policy::pad_with_nans,
                bool do_lock = true);

    template<typename T>
    size_type
    load_column(const char *name,
                const StlVecType<T> &data,
                nan_policy padding = nan_policy::pad_with_nans,
                bool do_lock = true);

    // This method creates a column similar to above, but assumes data is
    // bucket or bar values. That means the data vector contains statistical
    // figure(s) for time buckets and must be aligned with the index column
    // at bucket intervals.
    // For example, index column is in minutes unit. The data vector is the
    // sum of 5-minute buckets of some column, or some set not even present in
    // DataFrame. The values in data vector will be aligned with the index
    // column at every 5 minutes. The in-between values will be “null_value”.
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
        StlVecType<T> &&data,
        size_type interval,
        bool start_from_beginning,
        const T &null_value = hmdf::get_nan<T>(),
        std::function<std::size_t (
            const typename DataFrame<I, H>::IndexType &,
            const typename DataFrame<I, H>::IndexType &)> diff_func =
            [](const typename DataFrame<I, H>::IndexType &t_1,
               const typename DataFrame<I, H>::IndexType &t) ->
                   typename std::size_t  {
                return (static_cast<std::size_t>(t - t_1));
            });

    // This method feeds an existing column data, along with index data, into
    // the given functor which for each data point creates a new data point
    // for a new column with the given name
    //
    // NT:
    //   Type of the new column
    // ET:
    //   Type of the existing column
    // func:
    //   Functor to create the new column content
    // padding:
    //   If true, it pads the data new column with nan, if it is shorter than
    //   the index column.
    //
    template<typename NT, typename ET>
    size_type
    load_column(const char *new_col_name,
                const char *existing_col_name,
                std::function<NT(const IndexType &, const ET &)> &&func,
                nan_policy padding = nan_policy::pad_with_nans,
                bool do_lock = true);

    // This method loads the result() of a visitor to the named column.
    // For this method to work:
    //     1. The visitor must have a get_result() method
    //     2. The result must be a vector
    //     3. The visitor must define result_type type
    //     4. Caller must run the visitor before this call,
    //        so the result is already populated
    //
    // NOTE: This call moves the result vector to the DataFrame. After the
    //       call the visitor's result vector will be empty
    //
    // V:
    //   Visitor type
    // visitor:
    //   A reference to a visitor instance
    // new_col_name:
    //   Name of the new column to be added
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<has_result V>
    size_type
    load_result_as_column(V &visitor,
                          const char *new_col_name,
                          nan_policy padding = nan_policy::pad_with_nans);

    // This is a shortcut to running an algorithm and loading its vectored
    // result as a column in one shot. It runs and loads the result() of a
    // visitor to the named column.
    // For this method to work:
    //     1. The visitor must have a result() method
    //     2. The result must be a vector
    //     3. The visitor must define result_type type
    //
    // T:
    //   Column type to be passed to the visitor
    // V:
    //   Visitor type
    // col_name:
    //   Name of the column to be passed to the visitor
    // visitor:
    //   A reference to a visitor instance
    // new_col_name:
    //   Name of the new column to be added
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<typename T, has_result V>
    size_type
    load_result_as_column(const char *col_name,
                          V &&visitor,
                          const char *new_col_name,
                          nan_policy padding = nan_policy::pad_with_nans);

    // Same as above but supporting two columns.
    //
    // T1:
    //   First column type to be passed to the visitor
    // T2:
    //   Second column type to be passed to the visitor
    // V:
    //   Visitor type
    // col_name1:
    //   Name of the first column to be passed to the visitor
    // col_name2:
    //   Name of the second column to be passed to the visitor
    // visitor:
    //   A reference to a visitor instance
    // new_col_name:
    //   Name of the new column to be added
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<typename T1, typename T2, has_result V>
    size_type
    load_result_as_column(const char *col_name1,
                          const char *col_name2,
                          V &&visitor,
                          const char *new_col_name,
                          nan_policy padding = nan_policy::pad_with_nans);

    // Same as above but supporting three columns.
    //
    // T1:
    //   First column type to be passed to the visitor
    // T2:
    //   Second column type to be passed to the visitor
    // T3:
    //   Third column type to be passed to the visitor
    // V:
    //   Visitor type
    // col_name1:
    //   Name of the first column to be passed to the visitor
    // col_name2:
    //   Name of the second column to be passed to the visitor
    // col_name3:
    //   Name of the third column to be passed to the visitor
    // visitor:
    //   A reference to a visitor instance
    // new_col_name:
    //   Name of the new column to be added
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<typename T1, typename T2, typename T3, has_result V>
    size_type
    load_result_as_column(const char *col_name1,
                          const char *col_name2,
                          const char *col_name3,
                          V &&visitor,
                          const char *new_col_name,
                          nan_policy padding = nan_policy::pad_with_nans);

    // Same as above but supporting four columns.
    //
    // T1:
    //   First column type to be passed to the visitor
    // T2:
    //   Second column type to be passed to the visitor
    // T3:
    //   Third column type to be passed to the visitor
    // T4:
    //   Fourth column type to be passed to the visitor
    // V:
    //   Visitor type
    // col_name1:
    //   Name of the first column to be passed to the visitor
    // col_name2:
    //   Name of the second column to be passed to the visitor
    // col_name3:
    //   Name of the third column to be passed to the visitor
    // col_name4:
    //   Name of the fourth column to be passed to the visitor
    // visitor:
    //   A reference to a visitor instance
    // new_col_name:
    //   Name of the new column to be added
    // padding:
    //   If true, it pads the data column with nan, if it is shorter than the
    //   index column.
    //
    template<typename T1, typename T2, typename T3, typename T4, has_result V>
    size_type
    load_result_as_column(const char *col_name1,
                          const char *col_name2,
                          const char *col_name3,
                          const char *col_name4,
                          V &&visitor,
                          const char *new_col_name,
                          nan_policy padding = nan_policy::pad_with_nans);

    // Given a categorical (nominal) column, it generates a series of numerical
    // columns (0 or 1) for each category in the given column.
    // These numeric indictors encode categorical information.
    // In machine learning, this is also sometimes referred to as “one-hot”
    // encoding of categorical data
    //
    // NOTE: The values of the categorical column must be converted to string
    //       to generate names for indicator columns
    // NOTE: The values of the categorical column must be hashable
    //
    // T:
    //   Type of the named categorical column
    // IT:
    //   Type of the generated numerical indicator columns
    // cat_col_name:
    //   Name of the categorical column
    // numeric_cols_prefix:
    //   Optional prefix for generated column names
    //
    template<hashable_stringable T, typename IT = int>
    size_type
    load_indicators(const char *cat_col_name,
                    const char *numeric_cols_prefix = nullptr);

    // This does the opposite of the load_indicators(). Given a bunch of
    // one-hot indicator columns, it reconstructs the category column.
    //
    // T:
    //   Type of the indicator columns
    // CT:
    //   Type of the new categorical column
    // ind_col_names:
    //   Names of the indicator columns
    // cat_col_name:
    //   Name of the new categorical column
    // numeric_cols_prefix:
    //   Prefix of indicator column names
    //
    template<typename T = int, typename CT = std::string>
    size_type
    from_indicators(const StlVecType<const char *> &ind_col_names,
                    const char *cat_col_name,
                    const char *numeric_cols_prefix = nullptr);

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
    template<typename ITR>
    size_type
    append_column(const char *name,
                  Index2D<const ITR &> range,
                  nan_policy padding = nan_policy::pad_with_nans);

    // This is a way to append data to multiple columns on a row.
    // It appends the values in the args to the end of given columns.
    //
    // Ts:
    //   The list of types for columns in args
    // index_val:
    //   A pointer to an index value. If this pointer is not null, the index
    //   will alos be appended.
    // args:
    //   A variable list of arguments consisting of
    //     std::pair(<const char *name, &&data>).
    //   Each pair, represents a pair of column name and a single
    //   column value
    //
    // NOTE: This is not the most efficient way of appending values to columns
    //       in a rapid way, for example in a real-time system.
    //       For the most efficient way of appending to columns, maintain a
    //       reference to column vectors – by calling get_column() –
    //       and simply use push_back().
    //
    template<typename ... Ts>
    size_type
    append_row(IndexType *idx_val, Ts&& ... args);

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

    // This removes data rows by basic Glob-like pattern matching (also similar
    // to SQL like clause) to filter data in the named column. Each element of
    // the named column is checked against a Glob-like matching logic
    //
    // Globbing rules:
    //
    //      '*'       Matches any sequence of zero or more characters.
    //
    //      '?'       Matches exactly one character.
    //
    //     [...]      Matches one character from the enclosed list of
    //                characters.
    //
    //     [^...]     Matches one character not in the enclosed list.
    //
    // With the [...] and [^...] matching, a ']' character can be included
    // in the list by making it the first character after '[' or '^'.  A
    // range of characters can be specified using '-'.  Example:
    // "[a-z]" matches any single lower-case letter. To match a '-', make
    // it the last character in the list.
    //
    // Hints: to match '*' or '?', put them in "[]". Like this:
    //        abc[*]xyz matches "abc*xyz" only
    //
    // NOTE: This could be, in some cases, n-squared. But it is pretty fast
    //       with moderately sized strings. I have not tested this with
    //       huge/massive strings.
    //
    // T:
    //   Type of the named column. Based on the concept, it can only be either
    //   of these types: std::string, VirtualString, const char *, char *
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name:
    //   Name of the data column
    // pattern:
    //   Glob like pattern to use for matching strings
    // case_insensitive:
    //   If true, matching logic ignores case
    // esc_char:
    //   Character used for escape
    //
    template<StringOnly T, typename ... Ts>
    void
    remove_data_by_like(const char *name,
                        const char *pattern,
                        bool case_insensitive = false,
                        char esc_char = '\\');

    // This does the same function as above remove_data_by_like() but operating
    // on two columns.
    //
    // T:
    //   Type of the named columns. Based on the concept, it can only be either
    //   of these types: std::string, VirtualString, const char *, char *
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // pattern1:
    //   Glob like pattern to use for matching strings in the first column
    // pattern2:
    //   Glob like pattern to use for matching strings in the second column
    // case_insensitive:
    //   If true, matching logic ignores case
    // esc_char:
    //   Character used for escape
    //
    template<StringOnly T, typename ... Ts>
    void
    remove_data_by_like(const char *name1,
                        const char *name2,
                        const char *pattern1,
                        const char *pattern2,
                        bool case_insensitive = false,
                        char esc_char = '\\');

    // It removes data rows corresponding to n top rows of the named column.
    //
    // NOTE: Comparison operators (<, >, ==) must be well defined for type T.
    //
    // T:
    //   Type of column name
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // n:
    //   Number of top rows
    //
    template<comparable T, typename ... Ts>
    void
    remove_top_n_data(const char *col_name, size_type n);

    // It removes data rows corresponding to n bottom rows of the named column.
    //
    // NOTE: Comparison operators (<, >, ==) must be well defined for type T.
    //
    // T:
    //   Type of column name
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // n:
    //   Number of bottom rows
    //
    template<comparable T, typename ... Ts>
    void
    remove_bottom_n_data(const char *col_name, size_type n);

    // It removes rows greater than the specified quantile of the given column.
    // The row equal to the quantile is not removed.
    //
    // NOTE: Comparison operators (<, >, ==) must be well defined for type T.
    //
    // T:
    //   Type of column name
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // quantile:
    //   quantile specified as a fraction. For example, 0.35 for 35% quantile.
    //
    template<comparable T, typename ... Ts>
    void
    remove_above_quantile_data(const char *col_name, double quantile);

    // It removes rows smaller than the specified quantile of the given column.
    // The row equal to the quantile is not removed.
    //
    // NOTE: Comparison operators (<, >, ==) must be well defined for type T.
    //
    // T:
    //   Type of column name
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // quantile:
    //   quantile specified as a fraction. For example, 0.35 for 35% quantile.
    //
    template<comparable T, typename ... Ts>
    void
    remove_below_quantile_data(const char *col_name, double quantile);

    // This calculates the mean and standard deviation of the named column.
    // All data rows above and below the thresholds will be removed.
    //
    // NOTE: Type T must support arithmetic operations
    //
    // T:
    //   Type of the named column.
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // above_stdev:
    //   Above standard deviation threshold where data will be removed
    // below_stdev:
    //   Below standard deviation threshold where data will be removed
    //
    template<arithmetic T, typename ... Ts>
    void
    remove_data_by_stdev(const char *col_name, T above_stdev, T below_stdev);

    // This uses an Hampel filter to detect and remove outliers in the named
    // column and all rows corresponding to those outliers in the DataFrame.
    //
    // NOTE: Type T must support arithmetic operations
    //
    // T:
    //   Type of the named column.
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // window_size:
    //   Size of the sliding window for MAD calculations
    // htype:
    //   Use either Median or Mean of Absolute Deviation (MAD)
    // num_of_stdev:
    //   Number of stdev used in the filter
    //
    template<arithmetic T, typename ... Ts>
    void
    remove_data_by_hampel(const char *col_name,
                          size_type window_size,
                          hampel_type htype = hampel_type::median,
                          T num_of_stdev = 3);

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
    template<hashable_equal T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    template<hashable_equal T1, hashable_equal T2, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    template<hashable_equal T1, hashable_equal T2, hashable_equal T3,
             typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    template<hashable_equal T1, hashable_equal T2,
             hashable_equal T3, hashable_equal T4,
             typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    template<hashable_equal T1, hashable_equal T2, hashable_equal T3,
             hashable_equal T4, hashable_equal T5,
             typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    template<hashable_equal T1, hashable_equal T2, hashable_equal T3,
             hashable_equal T4, hashable_equal T5, hashable_equal T6,
             typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    remove_duplicates(const char *name1,
                      const char *name2,
                      const char *name3,
                      const char *name4,
                      const char *name5,
                      const char *name6,
                      bool include_index,
                      remove_dup_spec rds) const;

    // This removes data from the index column and all other data columns
    // before the index value "before" and after the index value "after".
    // Before and after values are excluded.
    //
    // NOTE: It is assumed that the index column is sorted
    // NOTE: Index type must have comparison operators (== != < > <= >=)
    //       well defined
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // before:
    //   An index value before which all data must be removed
    // after:
    //   An index value after which all data must be removed
    //
    template<typename ... Ts>
    void
    truncate(IndexType &&before, IndexType &&after);

    // This function assumes the named column is a time-series.
    // It attempts to make the time-series stationary by the specified method.
    // In cases of <I>differencing</I> and <I>smoothing</I> methods, the first
    // datapoint in the column remains unchanged.
    //
    // T:
    //   Type of the named column
    // col_name:
    //   Name of the column
    // method:
    //   The method by which it makes the column staionary
    // params:
    //   Parameters necessary for some of the above methods.
    //   Please see stationary_params.
    //
    template<typename T>
    void
    make_stationary(const char *col_name,
                    stationary_method method,
                    const StationaryParams params = { });

public:  // Data manipulation

    // It randomly shuffles the named column(s) non-deterministically.
    //
    // also_shuffle_index: If true, it shuffles the named column(s) and the
    //                     index column. Otherwise, index is not shuffled.
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<typename ... Ts>
    void
    shuffle(const StlVecType<const char *> &col_names,
            bool also_shuffle_index,
            seed_t seed = seed_t(-1));

    // It fills all the "missing values" with the given values, and/or using
    // the given method.
    // Missing is determined by being NaN for types that have NaN. For types
    // without NaN (e.g. string), default value is considered missing value.
    //
    // T:
    //   Type of the column(s) in col_names vector
    // N:
    //   Size of col_names and values vector
    // col_names:
    //   A vector of names specifying the columns to fill.
    // fp:
    //   Specifies the method to use to fill the missing values. For example;
    //   forward fill, values, etc.
    // values:
    //   If the policy is "values", use these values to fill the missing
    //   holes. Each value corresponds to the same index in the col_names
    //   vector.
    // limit:
    //   Specifies how many values to fill. Default is -1 meaning fill all
    //   missing values.
    //
    template<typename T>
    void
    fill_missing(const StlVecType<const char *> &col_names,
                 fill_policy policy,
                 const StlVecType<T> &values = { },
                 int limit = -1);

    // It fills the missing values in all columns in self by investigating the
    // rhs DataFrame. It attempts to find columns with the same name and type
    // in rhs. If there are such columns in rhs, it fills the missing values
    // in the corresponding columns in self that also have the same index
    // value.
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
    // N:
    //   Size of old_values and new_values vectors
    // col_name:
    //   Name of the column
    // old_values:
    //   A vector of values to be replaced in col_name column
    // new_values:
    //   A vector of values to to replace the old_values in col_name column
    // limit:
    //   Limit of how many items to replace. Default is to replace all.
    //
    template<hashable_equal T>
    size_type
    replace(const char *col_name,
            const StlVecType<T> &old_values,
            const StlVecType<T> &new_values,
            long limit = -1);

    // Same as replace() above, but executed asynchronously
    //
    // NOTE: multiple instances of replace_async() maybe executed for
    //       different columns at the same time with no problem.
    //
    template<hashable_equal T>
    [[nodiscard]] std::future<size_type>
    replace_async(const char *col_name,
                  const StlVecType<T> &old_values,
                  const StlVecType<T> &new_values,
                  long limit = -1);

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
    template<typename T, replace_callable<I, T> F>
    void
    replace(const char *col_name, F &functor);

    // Same as replace() above, but executed asynchronously
    //
    // NOTE: multiple instances of replace_async() maybe executed for
    //       different columns at the same time with no problem.
    //
    template<typename T, replace_callable<I, T> F>
    [[nodiscard]] std::future<void>
    replace_async(const char *col_name, F &functor);

    // This does the same thing as replace() above for the index column
    //
    // N: Size of old_values and new_values vector
    // old_values: A vector of values to be replaced in col_name column
    // new_values: A vector of values to to replace the old_values in col_name
    //            column
    // limit:
    //   Limit of how many items to replace. Default is to replace all.
    //
    size_type
    replace_index(const StlVecType<IndexType> &old_values,
                  const StlVecType<IndexType> &new_values,
                  long limit = -1);

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
    sort(const char *name, sort_spec dir, bool ignore_index = false);

    // This sort function sorts DataFrame based on two columns, also
    // specified by the two directions.
    // name[n] could be "INDEX" (or DF_INDEX_COL_NAME) in which case the index
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
         const char *name2, sort_spec dir2,
         bool ignore_index = false);

    // This sort function is similar to above, but it uses 3 columns
    //
    template<typename T1, typename T2, typename T3, typename ... Ts>
    void
    sort(const char *name1, sort_spec dir1,
         const char *name2, sort_spec dir2,
         const char *name3, sort_spec dir3,
         bool ignore_index = false);

    // This sort function is similar to above, but it uses 4 columns
    //
    template<typename T1, typename T2, typename T3, typename T4,
             typename ... Ts>
    void
    sort(const char *name1, sort_spec dir1,
         const char *name2, sort_spec dir2,
         const char *name3, sort_spec dir3,
         const char *name4, sort_spec dir4,
         bool ignore_index = false);

    // This sort function is similar to above, but it uses 5 columns
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename ... Ts>
    void
    sort(const char *name1, sort_spec dir1,
         const char *name2, sort_spec dir2,
         const char *name3, sort_spec dir3,
         const char *name4, sort_spec dir4,
         const char *name5, sort_spec dir5,
         bool ignore_index = false);

    // Same as sort() above, but executed asynchronously
    //
    template<typename T, typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name, sort_spec dir,
               bool ignore_index = false);

    template<typename T1, typename T2, typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               bool ignore_index = false);

    template<typename T1, typename T2, typename T3, typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3,
               bool ignore_index = false);

    template<typename T1, typename T2, typename T3, typename T4,
             typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3,
               const char *name4, sort_spec dir4,
               bool ignore_index = false);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename ... Ts>
    [[nodiscard]] std::future<void>
    sort_async(const char *name1, sort_spec dir1,
               const char *name2, sort_spec dir2,
               const char *name3, sort_spec dir3,
               const char *name4, sort_spec dir4,
               const char *name5, sort_spec dir5,
               bool ignore_index = false);

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
    template<comparable T, typename I_V, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    template<comparable T1, comparable T2, typename I_V, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    template<comparable T1, comparable T2, comparable T3,
             typename I_V, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    groupby3(const char *col_name1,
             const char *col_name2,
             const char *col_name3,
             I_V &&idx_visitor,
             Ts&& ... args) const;

    // Same as groupby1() above, but executed asynchronously
    //
    template<comparable T, typename I_V, typename ... Ts>
    [[nodiscard]]
    std::future<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
    groupby1_async(const char *col_name,
                   I_V &&idx_visitor,
                   Ts&& ... args) const;

    // Same as groupby2() above, but executed asynchronously
    //
    template<comparable T1, comparable T2, typename I_V, typename ... Ts>
    [[nodiscard]]
    std::future<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
    groupby2_async(const char *col_name1,
                   const char *col_name2,
                   I_V &&idx_visitor,
                   Ts&& ... args) const;

    // Same as groupby3() above, but executed asynchronously
    //
    template<comparable T1, comparable T2, comparable T3,
             typename I_V, typename ... Ts>
    [[nodiscard]]
    std::future<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
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
    template<hashable_equal T>
    [[nodiscard]] DataFrame<T, HeteroVector<std::size_t(H::align_value)>>
    value_counts(const char *col_name) const;

    template<hashable_equal T>
    [[nodiscard]] DataFrame<T, HeteroVector<std::size_t(H::align_value)>>
    value_counts(size_type index) const;

    // This function determines if each item in the named column starts with
    // the given pattern. The column type could be either a string or binary
    // data. More precisely the column type could be any of the following:
    //     std::string
    //     std::basic_string<unsigned char>
    //     std::basic_string<std::byte>
    //     std::vector<char>
    //     std::vector<unsigned char>
    //     std::vector<std::byte>
    //
    // It returns a vector of chars with the same size as the named column.
    // A 0 value means the corresponding element does not start with the
    // pattern. A 1 value means the corresponding element does start with the
    // pattern.
    //
    // T:
    //   Type of the col_name column.
    // col_name:
    //   Name of the column
    // pattern:
    //   A string or binary pattern to match
    //
    template<binary_array T>
    [[nodiscard]]
    typename
    DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
        StlVecType<char>
    starts_with(const char *col_name, const T &pattern) const;

    // This function determines if each item in the named column ends with
    // the given pattern. The column type could be either a string or binary
    // data. More precisely the column type could be any of the following:
    //     std::string
    //     std::basic_string<unsigned char>
    //     std::basic_string<std::byte>
    //     std::vector<char>
    //     std::vector<unsigned char>
    //     std::vector<std::byte>
    //
    // It returns a vector of chars with the same size as the named column.
    // A 0 value means the corresponding element does not end with the
    // pattern. A 1 value means the corresponding element does end with the
    // pattern.
    //
    // T:
    //   Type of the col_name column.
    // col_name:
    //   Name of the column
    // pattern:
    //   A string or binary pattern to match
    //
    template<binary_array T>
    [[nodiscard]]
    typename
    DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
        StlVecType<char>
    ends_with(const char *col_name, const T &pattern) const;

    // This function determines if each item in the named column is between
    // the given lower and upper bounds. Lower bound is inclusive and
    // upper bound is excluded.
    //
    // It returns a vector of chars with the same size as the named column.
    // A 0 value means the corresponding element is not between lower and
    // upper bounds. A 1 value means it is.
    //
    // T:
    //   Type of the col_name column. T must have the comparison operators
    //   (== != > < >= <=) well defined
    // col_name:
    //   Name of the column
    // lower_bound:
    //   A lower bound value
    // upper_bound:
    //   An upper bound value
    //
    template<comparable T>
    [[nodiscard]]
    typename
    DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
        StlVecType<char>
    in_between(const char *col_name,
               const T &lower_bound,
               const T &upper_bound) const;

    // This function determines if each item in the named column is a peak.
    // A peak data point is bigger than data points before and after it.
    //
    // It returns a vector of chars with the same size as the named column.
    // A 0 value means the data point is not a peak. 1 means it is.
    // The first and last n values of the returned vector are always 0;
    //
    // T:
    //   Type of the col_name column. T must have the comparison operators
    //   (== != > < >= <=) well defined
    // col_name:
    //   Name of the column
    // n
    //   Number of periods to account for before and after.
    //   It is defaulted to 1
    //
    template<comparable T>
    [[nodiscard]]
    typename
    DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
        StlVecType<char>
    peaks(const char *col_name, size_type n = 1) const;

    // This function determines if each item in the named column is a valley.
    // A valley data point is smaller than data points before and after it.
    //
    // It returns a vector of chars with the same size as the named column.
    // A 0 value means the data point is not a valley. 1 means it is.
    // The first and last n values of the returned vector are always 0;
    //
    // T:
    //   Type of the col_name column. T must have the comparison operators
    //   (== != > < >= <=) well defined
    // col_name:
    //   Name of the column
    // n
    //   Number of periods to account for before and after.
    //   It is defaulted to 1
    //
    template<comparable T>
    [[nodiscard]]
    typename
    DataFrame<T, HeteroVector<std::size_t(H::align_value)>>::template
        StlVecType<char>
    valleys(const char *col_name, size_type n = 1) const;

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
    // The result of each bucket will be stored in a new DataFrame and
    // returned.
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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    bucketize(bucket_type bt,
              const V &value,
              I_V &&idx_visitor,
              Ts&& ... args) const;

    // Same as bucketize() above, but executed asynchronously
    //
    template<typename V, typename I_V, typename ... Ts>
    [[nodiscard]]
    std::future<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    join_by_index(const RHS_T &rhs, join_policy jp) const;

    // It joins the data between self (lhs) and rhs and returns the joined data
    // in a StdDataFrame, based on specification in join_policy.
    // The returned DataFrame is indexed by a sequence of unsigned integers
    // from 0 to N. The returned DataFrame will at least have two columns names
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
    template<typename RHS_T, comparable T, typename ... Ts>
    [[nodiscard]]
    DataFrame<unsigned long, HeteroVector<std::size_t(H::align_value)>>
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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    concat(const RHS_T &rhs,
           concat_policy cp = concat_policy::all_columns) const;

    // This behaves just like concat(), but returns a view instead of another
    // DataFrame.
    //
    // NOTE: Views could not be const, because you can change original data
    //       through views.
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
    // NOTE: Views could not be const, because you can change original data
    //       through views.
    //
    template<typename RHS_T, typename ... Ts>
    [[nodiscard]] PtrView
    concat_view(RHS_T &rhs,
                concat_policy cp = concat_policy::common_columns);

    template<typename RHS_T, typename ... Ts>
    [[nodiscard]] ConstPtrView
    concat_view(RHS_T &rhs,
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
    [[nodiscard]] DataFrame
    shift(size_type periods, shift_policy sp) const;

    // This copies the named column into another vector and shifts it up
    // or down
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
    [[nodiscard]] StlVecType<T>
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
    [[nodiscard]] DataFrame
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

    // It returns the column index for the given column name. If column name
    // does not exist, a ColNotFound will be thrown.
    // This method uses a linear search.
    //
    [[nodiscard]] size_type
    col_name_to_idx(const char *col_name) const;

    // It returns the column name for the given column index. If column index
    // does not exist, a ColNotFound will be thrown.
    // This method uses a linear search.
    //
    [[nodiscard]] const char *
    col_idx_to_name(size_type col_idx) const;

    // It returns a reference to the container of named data column
    // The return type depends on if we are in standard or view mode
    //
    // T:
    //   Data type of the named column
    //
    template<typename T>
    [[nodiscard]] ColumnVecType<T> &
    get_column(const char *name, bool do_lock = true);

    template<typename T>
    [[nodiscard]] ColumnVecType<typename T::type> &
    get_column();

    template<typename T>
    [[nodiscard]] ColumnVecType<T> &
    get_column(size_type index, bool do_lock = true);

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
    get_column(const char *name, bool do_lock = true) const;

    template<typename T>
    [[nodiscard]] const ColumnVecType<typename T::type> &
    get_column() const;

    template<typename T>
    [[nodiscard]] const ColumnVecType<T> &
    get_column(size_type index, bool do_lock = true) const;

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
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // row_num:
    //   The row number
    // col_names:
    //   Names of columns to get data from. It also specifies the order of data
    //   in the returned vector
    //
    template<typename ... Ts>
    [[nodiscard]] HeteroVector<std::size_t(H::align_value)>
    get_row(size_type row_num,
            const StlVecType<const char *> &col_names) const;

    // This is same as get_row() above. But it always includes all the columns
    // in the returned row. The order is the column creation order. If you
    // have rotated the columns, the creation order has changed. You can always
    // use col_name_to_idx() and col_idx_to_name() methods.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // row_num:
    //   The row number
    //
    template<typename ... Ts>
    [[nodiscard]] HeteroVector<std::size_t(H::align_value)>
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
    template<hashable_equal T>
    [[nodiscard]] StlVecType<T>
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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_idx(const StlVecType<IndexType> &values) const;

    // It behaves like get_data_by_idx(range), but it returns a View.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Views could not be const, because you can change original data
    //       through views.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // range:
    //   The begin and end iterators for index specified with index values
    //
    template<typename ... Ts>
    [[nodiscard]] View
    get_view_by_idx(Index2D<IndexType> range);

    template<typename ... Ts>
    [[nodiscard]] ConstView
    get_view_by_idx(Index2D<IndexType> range) const;

    // It behaves like get_data_by_idx(values), but it returns a PtrView.
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
    [[nodiscard]] PtrView
    get_view_by_idx(const StlVecType<IndexType> &values);

    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_idx(const StlVecType<IndexType> &values) const;

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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_loc(const StlVecType<long> &locations) const;

    // It behaves like get_data_by_loc(range), but it returns a View.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Views could not be const, because you can change original data
    //       through views.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // range:
    //   The begin and end iterators for data
    //
    template<typename ... Ts>
    [[nodiscard]] View
    get_view_by_loc(Index2D<long> range);

    template<typename ... Ts>
    [[nodiscard]] ConstView
    get_view_by_loc(Index2D<long> range) const;

    // It behaves like get_data_by_loc(locations), but it returns a PtrView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Views could not be const, because you can change original data
    //       through views.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // locations:
    //   List of indices into the index column to copy data
    //
    template<typename ... Ts>
    [[nodiscard]] PtrView
    get_view_by_loc(const StlVecType<long> &locations);

    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_loc(const StlVecType<long> &locations) const;

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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_sel(const char *name, F &sel_functor) const;

    // This is identical with above get_data_by_sel(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
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
    [[nodiscard]] PtrView
    get_view_by_sel(const char *name, F &sel_functor);

    template<typename T, typename F, typename ... Ts>
    [[nodiscard]] ConstPtrView
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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_sel(const char *name1,
                    const char *name2,
                    F &sel_functor) const;

    // This is identical with above get_data_by_sel(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    // NOTE: Views could not be const, because you can change original data
    //       through views.
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
    [[nodiscard]] PtrView
    get_view_by_sel(const char *name1, const char *name2, F &sel_functor);

    template<typename T1, typename T2, typename F, typename ... Ts>
    [[nodiscard]] ConstPtrView
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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    F &sel_functor) const;

    // This is identical with above get_data_by_sel(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    // NOTE: Views could not be const, because you can change original data
    //       through views.
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
    [[nodiscard]] PtrView
    get_view_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    F &sel_functor);

    template<typename T1, typename T2, typename T3, typename F,
             typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    F &sel_functor) const;

    // This does the same function as above get_data_by_sel() but operating
    // on four columns.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &,
    //             const T1 &, const T2 &, const T3 &, const T4 &)
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // T4:
    //   Type of the fourth named column
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
    // name4:
    //   Name of the fourth data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename T3, typename T4, typename F,
             typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    F &sel_functor) const;

    // This is identical with above get_data_by_sel(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    // NOTE: Views could not be const, because you can change original data
    //       through views.
    //
    // T1:
    //   Type of the first named column
    // T2:
    //   Type of the second named column
    // T3:
    //   Type of the third named column
    // T4:
    //   Type of the fourth named column
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
    // name4:
    //   Name of the fourth data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename T3, typename T4, typename F,
             typename ... Ts>
    [[nodiscard]] PtrView
    get_view_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    F &sel_functor);

    template<typename T1, typename T2, typename T3, typename T4, typename F,
             typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    F &sel_functor) const;

    // This does the same function as above get_data_by_sel() but operating
    // on five columns.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &,
    //             const T1 &, const T2 &, const T3 &, const T4 &, const T5 &)
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
    // name4:
    //   Name of the fourth data column
    // name5:
    //   Name of the fifth data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename F, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    const char *name5,
                    F &sel_functor) const;

    // This is identical with above get_data_by_sel(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    // NOTE: Views could not be const, because you can change original data
    //       through views.
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
    // name4:
    //   Name of the fourth data column
    // name5:
    //   Name of the fifth data column
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename F, typename ... Ts>
    [[nodiscard]] PtrView
    get_view_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    const char *name5,
                    F &sel_functor);

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename F, typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    const char *name5,
                    F &sel_functor) const;

    // Data by select for larger number of columns
    //
    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename T6, typename T7, typename T8, typename T9, typename T10,
             typename T11,
             typename F, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    const char *name5,
                    const char *name6,
                    const char *name7,
                    const char *name8,
                    const char *name9,
                    const char *name10,
                    const char *name11,
                    F &sel_functor) const;

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename T6, typename T7, typename T8, typename T9, typename T10,
             typename T11, typename T12,
             typename F, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    const char *name5,
                    const char *name6,
                    const char *name7,
                    const char *name8,
                    const char *name9,
                    const char *name10,
                    const char *name11,
                    const char *name12,
                    F &sel_functor) const;

    template<typename T1, typename T2, typename T3, typename T4, typename T5,
             typename T6, typename T7, typename T8, typename T9, typename T10,
             typename T11, typename T12, typename T13,
             typename F, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_sel(const char *name1,
                    const char *name2,
                    const char *name3,
                    const char *name4,
                    const char *name5,
                    const char *name6,
                    const char *name7,
                    const char *name8,
                    const char *name9,
                    const char *name10,
                    const char *name11,
                    const char *name12,
                    const char *name13,
                    F &sel_functor) const;

    // This method does boolean filtering selection via the sel_functor
    // (e.g. a functor, function, or lambda). It returns a new DataFrame.
    // Each element of the named column along with its corresponding index
    // is passed to the sel_functor. If sel_functor returns true, that index
    // is selected and all the elements of all column for that index will be
    // included in the returned DataFrame.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &, const T &, ...)
    //
    // NOTE: If the selection logic results in empty column(s), the result
    //       empty columns will _not_ be padded with NaN's. You can always
    //       call make_consistent() on the original or result DataFrame to make
    //       all columns into consistent length
    //
    // Tuple:
    //   Tuple to collect the columns which will be shown in the return
    //   DataFrame. Use `projection_list` to set columns.
    // F:
    //   Type of the selecting functor
    // Cols:
    //   List all data columns which need to be declared
    //   by DECL_COL defined in DataFrameTypes.h, e.g.:
    //     struct MyDfSchema
    //     {
    //         DECL_COL(Col0, std::string);
    //         DECL_COL(Col1, int32_t);
    //         DECL_COL(Col2, int64_t);
    //         DECL_COL(Col3, double);
    //     };
    //
    // sel_functor:
    //   A reference to the selecting functor
    //
    template<typename Tuple, typename F, typename... FilterCols>
    [[nodiscard]] DataFrame
    get_data_by_sel(F &sel_functor) const;

    // This method does boolean filtering selection via the sel_functor
    // (e.g. a functor, function, or lambda). It returns a new DataFrame.
    // Each element of the named column along with its corresponding index
    // is passed to the sel_functor. If sel_functor returns true, that index
    // is selected and all the elements of all column for that index will be
    // included in the returned DataFrame.
    // The signature of sel_fucntor:
    //     bool ()(const IndexType &, const T &, ...)
    //
    // NOTE: If the selection logic results in empty column(s), the result
    //       empty columns will _not_ be padded with NaN's. You can always
    //       call make_consistent() on the original or result DataFrame to make
    //       all columns into consistent length
    //
    // Tuple:
    //   Tuple to collect types of the named columns which will be shown
    //   in the return DataFrame. Use `projection_type_list` to set columns.
    // F:
    //   Type of the selecting functor
    // FilterCols:
    //   The types of input parameter `filter_cols`
    //
    // sel_functor:
    //   A reference to the selecting functor
    //
    // filter_cols:
    //   These will be as input parameter definition of sel_functor.
    //   The arguments will be defined like:
    //   struct XXXColumn
    //   {
    //       using type = T;
    //       const char* Name() const { return xxx; }
    //   };
    //   So you can just use `struct CommonColumn defined in DataFrameTypes.h
    //
    template<typename Tuple, typename F, typename... FilterCols>
    [[nodiscard]] DataFrame
    get_data_by_sel(F &sel_functor, FilterCols&&... filter_cols) const;

    // This method does a basic Glob-like pattern matching (also similar to
    // SQL like clause) to filter data in the named column.
    // It returns a new DataFrame. Each element of the named column is checked
    // against a Glob-like matching logic
    //
    // Globbing rules:
    //
    //      '*'       Matches any sequence of zero or more characters.
    //
    //      '?'       Matches exactly one character.
    //
    //     [...]      Matches one character from the enclosed list of
    //                characters.
    //
    //     [^...]     Matches one character not in the enclosed list.
    //
    // With the [...] and [^...] matching, a ']' character can be included
    // in the list by making it the first character after '[' or '^'.  A
    // range of characters can be specified using '-'.  Example:
    // "[a-z]" matches any single lower-case letter. To match a '-', make
    // it the last character in the list.
    //
    // Hints: to match '*' or '?', put them in "[]". Like this:
    //        abc[*]xyz matches "abc*xyz" only
    //
    // NOTE: This could be, in some cases, n-squared. But it is pretty fast
    //       with moderately sized strings. I have not tested this with
    //       huge/massive strings.
    //
    // T:
    //   Type of the named column. Based on the concept, it can only be either
    //   of these types: std::string, VirtualString, const char *, char *
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name:
    //   Name of the data column
    // pattern:
    //   Glob like pattern to use for matching strings
    // case_insensitive:
    //   If true, matching logic ignores case
    // esc_char:
    //   Character used for escape
    //
    template<StringOnly T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_like(const char *name,
                     const char *pattern,
                     bool case_insensitive = false,
                     char esc_char = '\\') const;

    // This is identical with above get_data_by_like(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    template<StringOnly T, typename ... Ts>
    [[nodiscard]] PtrView
    get_view_by_like(const char *name,
                     const char *pattern,
                     bool case_insensitive = false,
                     char esc_char = '\\');

    template<StringOnly T, typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_like(const char *name,
                     const char *pattern,
                     bool case_insensitive = false,
                     char esc_char = '\\') const;

    // This does the same function as above get_data_by_like() but operating
    // on two columns.
    //
    // T:
    //   Type of both named columns. Based on the concept, it can only be
    //   either of these types: std::string, VirtualString, const char *,
    //   char *
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // name1:
    //   Name of the first data column
    // name2:
    //   Name of the second data column
    // pattern1:
    //   Glob like pattern to use for matching strings for the first column
    // pattern2:
    //   Glob like pattern to use for matching strings for the second column
    // case_insensitive:
    //   If true, matching logic ignores case
    // esc_char:
    //   Character used for escape
    //
    template<StringOnly T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_like(const char *name1,
                     const char *name2,
                     const char *pattern1,
                     const char *pattern2,
                     bool case_insensitive = false,
                     char esc_char = '\\') const;

    // This is identical with above get_data_by_like(), but:
    //   1) The result is a view
    //   2) Since the result is a view, you cannot call make_consistent() on
    //      the result.
    //
    template<StringOnly T, typename ... Ts>
    [[nodiscard]] PtrView
    get_view_by_like(const char *name1,
                     const char *name2,
                     const char *pattern1,
                     const char *pattern2,
                     bool case_insensitive = false,
                     char esc_char = '\\');

    template<StringOnly T, typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_like(const char *name1,
                     const char *name2,
                     const char *pattern1,
                     const char *pattern2,
                     bool case_insensitive = false,
                     char esc_char = '\\') const;

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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_rand(random_policy spec, double n, seed_t seed = 0) const;

    // It behaves like get_data_by_rand(), but it returns a PtrView.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: The columns in the result are not padded with NaN.
    // NOTE: Views could not be const, because you can change original data
    //       through views.
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
    [[nodiscard]] PtrView
    get_view_by_rand(random_policy spec, double n, seed_t seed = 0);

    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_rand(random_policy spec, double n, seed_t seed = 0) const;

    // This returns a DataFrame with index and col_names copied from the
    // original DataFrame
    //
    // Ts:
    //   List types of col_names
    // col_names:
    //   List of column names
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data(const StlVecType<const char *> &col_names) const;

    // This returns a new DataFrame with the n top rows of the given column.
    // The returned DataFrame rows will be in the same order as self.
    //
    // NOTE: Comparison operators (<, >, ==) must be well defined for type T.
    //
    // T:
    //   Type of column name
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // n:
    //   Number of top rows
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_top_n_data(const char *col_name, size_type n) const;

    // Smae as above but it returns a View with the n top rows of
    // the given column.
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] PtrView
    get_top_n_view(const char *col_name, size_type n);

    // Same as above but it returns a const View with the n top rows of
    // the given column.
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_top_n_view(const char *col_name, size_type n) const;

    // This returns a new DataFrame with the n bottom rows of the given column.
    // The returned DataFrame rows will be in the same order as self.
    //
    // NOTE: Comparison operators (<, >, ==) must be well defined for type T.
    //
    // T:
    //   Type of column name
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // n:
    //   Number of bottom rows
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_bottom_n_data(const char *col_name, size_type n) const;

    // Same as above but it returns a View with the n bottom rows of
    // the given column.
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] PtrView
    get_bottom_n_view(const char *col_name, size_type n);

    // Same as above but it returns a const View with the n bottom rows of
    // the given column.
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_bottom_n_view(const char *col_name, size_type n) const;

    // This returns a new DataFrame with rows greater than the specified
    // quantile of the given column. The row equal to the quantile is also
    // included.
    // The returned DataFrame rows will be in the same order as self.
    //
    // NOTE: Comparison operators (<, >, ==) must be well defined for type T.
    //
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // quantile:
    //   quantile specified as fraction. For example, 0.35 for 35% quantile.
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_above_quantile_data(const char *col_name, double quantile) const;

    // Same as above but it returns a View with above quantile rows
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] PtrView
    get_above_quantile_view(const char *col_name, double quantile);

    // Same as above but it returns a const View with above quantile rows
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_above_quantile_view(const char *col_name, double quantile) const;

    // This returns a new DataFrame with rows less than the specified
    // quantile of the given column. The row equal to the quantile is excluded.
    // The returned DataFrame rows will be in the same order as self.
    //
    // NOTE: Comparison operators (<, >, ==) must be well defined for type T.
    //
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // quantile:
    //   quantile specified as fraction. For example, 0.35 for 35% quantile.
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_below_quantile_data(const char *col_name, double quantile) const;

    // Same as above but it returns a View with above quantile rows
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] PtrView
    get_below_quantile_view(const char *col_name, double quantile);

    // Same as above but it returns a const View with above quantile rows
    //
    template<comparable T, typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_below_quantile_view(const char *col_name, double quantile) const;

    // This calculates the mean and standard deviation of the named column.
    // It returns a new DataFrame that contains all the data where named column
    // data is between high_stdev and low_stdev from the mean.
    // Self is unchanged.
    //
    // NOTE: Type T must support arithmetic operations
    //
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // high_stdev:
    //   All data rows below this will be returned.
    // low_stdev:
    //   All data rows above this will be returned.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_data_by_stdev(const char *col_name, T high_stdev, T low_stdev) const;

    // Same as above but it returns a View.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]] PtrView
    get_view_by_stdev(const char *col_name, T high_stdev, T low_stdev);

    // Same as above but it returns a const View.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_by_stdev(const char *col_name, T high_stdev, T low_stdev) const;

    // This uses k-means clustering algorithm to divide the named column into
    // K clusters. It returns an array of K DataFrame's each containing one of
    // the clusters of data based on the named column.
    // Self in unchanged.
    //
    // NOTE: Type T must support arithmetic operations
    //
    // K:
    //   Number of clusters for k-means clustering algorithm
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // dfunc:
    //   A function to calculate the distance between two data points in the
    //   named column
    // num_of_iter:
    //   Maximum number of iterations for k-means clustering algorithm before
    //   converging
    // seed:
    //   Seed for random number generator to initialize k-means clustering
    //   algorithm. Default is a random number for each call.
    //
    template<std::size_t K, arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::array<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>, K>
    get_data_by_kmeans(const char *col_name,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           },
                       size_type num_of_iter = 1000,
                       seed_t seed = seed_t(-1)) const;

    // Same as above but it returns an array of Views.
    //
    template<std::size_t K, arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::array<PtrView, K>
    get_view_by_kmeans(const char *col_name,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           },
                       size_type num_of_iter = 1000,
                       seed_t seed = seed_t(-1));

    // Same as above but it returns an array of const Views.
    //
    template<std::size_t K, arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::array<ConstPtrView, K>
    get_view_by_kmeans(const char *col_name,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           },
                       size_type num_of_iter = 1000,
                       seed_t seed = seed_t(-1)) const;

    // This uses spectral clustering algorithm to divide the named column into
    // K clusters. It returns an array of K DataFrame's each containing one of
    // the clusters of data based on the named column.
    // Self in unchanged.
    //
    // NOTE: Type T must support arithmetic operations
    //
    // K:
    //   Number of clusters for k-means clustering algorithm
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // sfunc:
    //   A function to calculate the similarity matrix between data points in
    //   the named column
    // num_of_iter:
    //   Maximum number of iterations for k-means clustering algorithm before
    //   converging
    // seed:
    //   Seed for random number generator to initialize k-means clustering
    //   algorithm. Default is a random numbers for each call.
    //
    template<std::size_t K, arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::array<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>, K>
    get_data_by_spectral(const char *col_name,
                         double sigma,
                         seed_t seed = seed_t(-1),
                         std::function<double(const T &x, const T &y,
                                              double sigma)>  &&sfunc =
                             [](const T &x, const T &y,
                                double sigma) -> double  {
                                 return (std::exp(-((x - y) * (x - y)) /
                                                  (2 * sigma * sigma)));
                             },
                         size_type num_of_iter = 1000) const;

    // Same as above but it returns an array of Views.
    //
    template<std::size_t K, arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::array<PtrView, K>
    get_view_by_spectral(const char *col_name,
                         double sigma,
                         seed_t seed = seed_t(-1),
                         std::function<double(const T &x, const T &y,
                                              double sigma)>  &&sfunc =
                             [](const T &x, const T &y,
                                double sigma) -> double  {
                                 return (std::exp(-((x - y) * (x - y)) /
                                                  (2 * sigma * sigma)));
                             },
                         size_type num_of_iter = 1000);

    // Same as above but it returns an array of const Views.
    //
    template<std::size_t K, arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::array<ConstPtrView, K>
    get_view_by_spectral(const char *col_name,
                         double sigma,
                         seed_t seed = seed_t(-1),
                         std::function<double(const T &x, const T &y,
                                              double sigma)>  &&sfunc =
                             [](const T &x, const T &y,
                                double sigma) -> double  {
                                 return (std::exp(-((x - y) * (x - y)) /
                                                  (2 * sigma * sigma)));
                             },
                         size_type num_of_iter = 1000) const;

    // This uses Affinity Propagation algorithm to divide the named column
    // into clusters. It returns an array of DataFrame's each containing one
    // of the clusters of data based on the named column. Unlike K-Means
    // clustering, you do not have to specify the number of clusters.
    // The algorithm determines that.
    // Self in unchanged.
    //
    // NOTE: This is a resource consuming and relatively slow algorithm.
    //       Its time complexity is O(I * n^2) where I is number of
    //       iterations. Its space complexity is O(2 * n^2).
    // NOTE: Type T must support arithmetic operations
    // NOTE: This algorithm might be too slow for large datasets.
    //       Also, see get_[data|view]_by_kmeans().
    // NOTE: If this returns zero centroids (zero DataFrames) it is probably
    //       because number of iterations is too small to converge.
    //
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // dfunc:
    //   A function to calculate the distance between two data points in the
    //   named column
    // num_of_iter:
    //   Maximum number of iterations for AP clustering algorithm to converge
    // damping_factor:
    //   It is used in the algorithm. The default is 0.9. (1 – damping factor)
    //   prevents numerical oscillations.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
    get_data_by_affin(const char *col_name,
                      std::function<double(const T &x, const T &y)> &&dfunc =
                          [](const T &x, const T &y) -> double  {
                              return ((x - y) * (x - y));
                          },
                      size_type num_of_iter = 20,
                      double damping_factor = 0.9) const;

    // Same as above but it returns a vector of Views.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<PtrView>
    get_view_by_affin(const char *col_name,
                      std::function<double(const T &x, const T &y)> &&dfunc =
                          [](const T &x, const T &y) -> double  {
                              return ((x - y) * (x - y));
                          },
                      size_type num_of_iter = 20,
                      double damping_factor = 0.9);

    // Same as above but it returns a vector of const Views.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<ConstPtrView>
    get_view_by_affin(const char *col_name,
                      std::function<double(const T &x, const T &y)> &&dfunc =
                          [](const T &x, const T &y) -> double  {
                              return ((x - y) * (x - y));
                          },
                      size_type num_of_iter = 20,
                      double damping_factor = 0.9) const;

    // This uses DBSCAN algorithm to divide the named column into clusters.
    // It returns an array of DataFrame's each containing one of the clusters
    // of data based on the named column. The last DataFrame in the array
    // contains noisy data. It contains datapoints that could not be placed
    // into any cluster. Ideally, you want the last DataFrame to be empty.
    // Unlike K-Means clustering, you do not have to specify the number of
    // clusters. The algorithm determines that.
    // Self in unchanged.
    //
    // NOTE: Type T must support arithmetic operations
    // NOTE: If this returns zero centroids (zero DataFrames) or there are too
    //       many noisy datapoints, it is probably because number of iterations
    //       is too small to converge.
    //
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // min_members:
    //   Minimum number of datapoints to constitute a cluster
    // max_distance:
    //   Maximum distance between two data points in the same cluster
    // dfunc:
    //   A function to calculate the distance between two data points in the
    //   named column
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
    get_data_by_dbscan(const char *col_name,
                       long min_members,
                       double max_distance,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           }) const;

    // Same as above but it returns a vector of Views.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<PtrView>
    get_view_by_dbscan(const char *col_name,
                       long min_members,
                       double max_distance,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           });

    // Same as above but it returns a vector of const Views.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<ConstPtrView>
    get_view_by_dbscan(const char *col_name,
                       long min_members,
                       double max_distance,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           }) const;

    // This uses Mean-Shift algorithm to divide the named column into clusters.
    // It returns an array of DataFrame's each containing one of the clusters
    // of data based on the named column.  Unlike K-Means clustering, you do
    // not have to specify the number of clusters. The algorithm determines
    // that.
    // Self in unchanged.
    //
    // NOTE: Type T must support arithmetic operations
    //
    // T:
    //   Type of the named column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the given column
    // kernel_bandwidth:
    //   the width or spread of the kernel function used
    // max_distance:
    //   Maximum distance between two data points in the same cluster
    // mean_shift_kernel:
    //   Kernel type used
    // dfunc:
    //   A function to calculate the distance between two data points in the
    //   named column
    // num_of_iter:
    //   Maximum number of iterations for AP clustering algorithm to converge
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<DataFrame<I, HeteroVector<std::size_t(H::align_value)>>>
    get_data_by_mshift(const char *col_name,
                       double kernel_bandwidth,
                       double max_distance,
                       mean_shift_kernel kernel = mean_shift_kernel::gaussian,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           },
                       size_type num_of_iter = 50) const;

    // Same as above but it returns a vector of Views.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<PtrView>
    get_view_by_mshift(const char *col_name,
                       double kernel_bandwidth,
                       double max_distance,
                       mean_shift_kernel kernel = mean_shift_kernel::gaussian,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           },
                       size_type num_of_iter = 50);

    // Same as above but it returns a vector of const Views.
    //
    template<arithmetic T, typename ... Ts>
    [[nodiscard]]
    std::vector<ConstPtrView>
    get_view_by_mshift(const char *col_name,
                       double kernel_bandwidth,
                       double max_distance,
                       mean_shift_kernel kernel = mean_shift_kernel::gaussian,
                       std::function<double(const T &x, const T &y)> &&dfunc =
                           [](const T &x, const T &y) -> double  {
                               return ((x - y) * (x - y));
                           },
                       size_type num_of_iter = 50) const;

    // This returns a new DataFrame with the same index column as self and an
    // integer column with the same name for each column in self.
    // The integer columns in returned DataFrame show a duplication mask for
    // each column in self.
    // For example, if self has a column like:
    //     ----------------------------------------
    //    |  aa | bb | cc | aa | dd | aa | bb | hh |
    //     ----------------------------------------
    // The returned DataFrame has a corresponding integer column with the
    // same name
    // If binary is false:
    //     --------------------------------
    //    |  3 | 2 | 1 | 3 | 1 | 3 | 2 | 1 |
    //     --------------------------------
    // If binary is true:
    //     --------------------------------
    //    |  1 | 1 | 0 | 1 | 0 | 1 | 1 | 0 |
    //     --------------------------------
    //
    // NOTE: All column types must be hash-able and have == operator
    //       well defined
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // include_index:
    //   If true, it includes the index column to determine uniqueness
    // binary:
    //   If false, the returned integer columns contain the count of each item
    //   in the original column. If true, the returned integer columns contain
    //   0’s and 1’s depending on if original items in the column had
    //   duplicates or not.
    //
    template<hashable_equal ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    duplication_mask(bool include_index, bool binary = false) const;

    // It behaves like get_data(), but it returns a View.
    // A view is a DataFrame that is a reference to the original DataFrame.
    // So if you modify anything in the view the original DataFrame will
    // also be modified.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Views could not be const, because you can change original data
    //       through views.
    //
    // Ts:
    //   List types of col_names
    // col_names:
    //   List of column names
    //
    template<typename ... Ts>
    [[nodiscard]] View
    get_view(const StlVecType<const char *> &col_names);

    template<typename ... Ts>
    [[nodiscard]] ConstView
    get_view(const StlVecType<const char *> &col_names) const;

    // It returns a const reference to the index container
    //
    [[nodiscard]] const IndexVecType &
    get_index() const;

    // It returns a reference to the index container
    //
    [[nodiscard]] IndexVecType &
    get_index();

    /*
    // It prints to stdout n rows of all columns either from the beginning
    // or end of the DataFrame. Print will be in csv2 format.
    // If n is positive, n rows from the beginning of DataFrame are printed.
    // If negative, n rows from the end of DataFrame are printed.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // n:
    //   Number of columns to print. It could be positive or negative
    //
    template<typename ... Ts>
    void
    head(long n) const;
    */

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
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    get_reindexed(const char *col_to_be_index,
                  const char *old_index_name = nullptr) const;

    // This is similar to get_reindexed(), but it returns a view. Please read
    // above for specs.
    //
    // NOTE: There are certain operations that you cannot do with a view.
    //       For example, you cannot add/delete columns, etc.
    // NOTE: Views could not be const, because you can change original data
    //       through views.
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
    [[nodiscard]] typename DataFrame<T, H>::View
    get_reindexed_view(const char *col_to_be_index,
                       const char *old_index_name = nullptr);

    template<typename T, typename ... Ts>
    [[nodiscard]] typename DataFrame<T, H>::ConstView
    get_reindexed_view(const char *col_to_be_index,
                       const char *old_index_name = nullptr) const;

    // This returns a new DataFrame with the same index type. But the frequency
    // of the index in the new DataFrame is according to the parameters of this
    // call. If the frequency has  increased the data is forward-filled from
    // self. If the frequency has decreased, the last data point is copied
    // from self.
    //
    // NOTE: The index type must be either a numeric or DateTime or there will
    //       be a compilation assert
    // NOTE: Self must be already sorted by index in ascending order,
    //       or the behavior is undefined.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // new_freq:
    //   The new frequency expressed in a positive integer value. The Unit
    //   depends on the index type and time_unit parameter
    // time_unit:
    //   Time unit based on time_frequency enum.
    //   If index time is not DateTime, time_unit must be not_valid.
    //   If index time is DateTime, time_unit must not be not_valid.
    //   Otherwise, the behavior is undefined.
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    change_freq(size_type new_freq,
                [[maybe_unused]] time_frequency time_unit =
                    time_frequency::not_valid) const;

    // This call takes a string column as input and returns a set of statistics
    // about the strings in that column. You could use these statistics to
    // figure out the characteristics of the strings in the column. Or you
    // could use the statistics to determine if a new string is acceptable
    // for this column.
    //
    // T:
    //   Type of the input column. Based on the concept, it can only be either
    //   of these types: std::string, VirtualString, const char *, char *
    // col_name:
    //   Name of the input column
    //
    template<StringOnly T>
    [[nodiscard]] StringStats
    get_str_col_stats(const char *col_name) const;

    // This returns the number of inversions in the named column. For example,
    // in a column that is already sorted, the number of inversions is zero.
    // In a column that is sorted in reverse, the number of inversions is
    // n(n - 1) / 2.
    //
    // T:
    //   Data type of the named column
    // C:
    //   Type of the comparison functor defaulted to std::less
    // col_name:
    //   Name of the column
    //
    template<typename T, typename C = std::less<T>>
    [[nodiscard]] size_type
    inversion_count(const char *col_name) const;

    // This calculates and returns the variance/covariance matrix of the
    // specified columns, optionally normalizing the columns first.
    //
    // T:
    //   Type of the named columns
    // col_names:
    //   Vector of column names
    // norm_type:
    //   The method to normalize the columns first before calculations.
    //   Default is not normalizing
    //
    template<typename T>
    [[nodiscard]] Matrix<T, matrix_orient::column_major>
    covariance_matrix(
        std::vector<const char *> &&col_names,
        normalization_type norm_type = normalization_type::none) const;

    // This uses Eigenspace evaluation to calculate Principal Component
    // Analysis (PCA).
    // It returns a matrix whose columns are the reduced dimensions with most
    // significant information.
    // PCA is a dimensionality reduction method that is often used to reduce
    // the dimensionality of large data sets, by transforming a large set of
    // variables into a smaller one that still contains most of the information
    // in the large set.
    // Reducing the number of variables of a data set naturally comes at the
    // expense of accuracy, but the trick in dimensionality reduction is to
    // trade a little accuracy for simplicity. Because smaller data sets are
    // easier to explore and visualize, and thus make analyzing data points
    // much easier and faster for machine learning algorithms without
    // extraneous variables to process.
    //
    // T:
    //   Type of the named columns
    // col_names:
    //   Vector of column names
    // params:
    //   Parameters necessary for for this operation
    //
    template<typename T>
    [[nodiscard]] Matrix<T, matrix_orient::column_major>
    pca_by_eigen(std::vector<const char *> &&col_names,
                 const PCAParams params = { }) const;

    // This calculates Singular Value Decomposition (SVD). Optionaly it may
    // normalize the original matrix first.
    // In linear algebra, SVD is a factorization of a real or complex matrix
    // into a rotation, followed by a rescaling followed by another rotation.
    // It generalizes the eigen-decomposition of a square normal matrix with
    // an orthonormal eigenbasis to any ⁠mXn matrix.
    //
    // It returns the 3 metrices U, S, and V inside a std::tuple.
    // U contains the left singular vectors of the original matrix, meaning
    // its columns are orthonormal vectors that span the row space of the
    // matrix.
    // S is a diagonal matrix that contains sqrt of eigenvalues of the original
    // matrix's covariance matrix, arranged in descending order.
    // V contains the right singular vectors of the original matrix,
    // represented as its columns.
    // Original matrix (A) = U * S * VT
    //
    // T:
    //   Type of the named columns
    // norm_type:
    //   Type of normalization applied to raw data first
    //
    template<typename T>
    [[nodiscard]] std::tuple<Matrix<T, matrix_orient::column_major>,  // U
                             Matrix<T, matrix_orient::column_major>,  // S
                             Matrix<T, matrix_orient::column_major>>  // V
    compact_svd(std::vector<const char *> &&col_names,
                normalization_type norm_type =
                    normalization_type::z_score) const;

    // This function returns a DataFrame indexed by std::string that provides
    // a few statistics about the columns of the calling DataFrame.
    // The statistics are:
    //  -- Number of items in the column
    //  -- Number of items missing in the column
    //  -- Mean of the items
    //  -- Standard deviation of the items
    //  -- Minimum item in the column
    //  -- Maximum item in the column
    //  -- 25% quantile item in the column
    //  -- 50% quantile item in the column
    //  -- 75% quantile item in the column
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<std::string, H>
    describe() const;

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
    [[nodiscard]] StlVecType<T>
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
    [[nodiscard]] StlVecType<T>
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
    [[nodiscard]] StlVecType<T>
    combine(const char *col_name,
            const DF1 &df1,
            const DF2 &df2,
            const DF3 &df3,
            F &functor) const;

    // This method feeds old_col_name1 and old_col_name2 of types OLD_T1 and
    // OLD_T2 to functor which returns a StlVecType<NEW_T> which will be
    // loaded into self as column new_col_name. Both old columns will be
    // removed, if delete_old_cols is true
    // Functor "functor" should implement the logic of consolidating two
    // columns into one. Functor signature is:
    //     template<typename ITR1, typename ITR2>
    //     StlVecType<NEW_T> (IndexVecType::const_iterator idx_begin,
    //                         IndexVecType::const_iterator idx_end,
    //                         ITR1 col1_begin, ITR1 col1_end,
    //                         ITR2 col2_begin, ITR2 col2_end);
    //     Where ITR[12] are iterators for columns 1 and 2. They are iterators
    //     of StlVecType.
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
    //     StlVecType<NEW_T> (IndexVecType::const_iterator idx_begin,
    //                         IndexVecType::const_iterator idx_end,
    //                         ITR1 col1_begin, ITR1 col1_end,
    //                         ITR2 col2_begin, ITR2 col2_end,
    //                         ITR3 col3_begin, ITR3 col3_end);
    //     Where ITR[123] are iterators for columns 1, 2 and 3. They are
    //     iterators of StlVecType.
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
    //     StlVecType<NEW_T> (IndexVecType::const_iterator idx_begin,
    //                         IndexVecType::const_iterator idx_end,
    //                         ITR1 col1_begin, ITR1 col1_end,
    //                         ITR2 col2_begin, ITR2 col2_end,
    //                         ITR3 col3_begin, ITR3 col3_end,
    //                         ITR4 col4_begin, ITR4 col4_end);
    //     Where ITR[1234] are iterators for columns 1, 2, 3, and 4. They are
    //     iterators of StlVecType.
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
    //     StlVecType<NEW_T> (IndexVecType::const_iterator idx_begin,
    //                         IndexVecType::const_iterator idx_end,
    //                         ITR1 col1_begin, ITR1 col1_end,
    //                         ITR2 col2_begin, ITR2 col2_end,
    //                         ITR3 col3_begin, ITR3 col3_end,
    //                         ITR4 col4_begin, ITR4 col4_end,
    //                         ITR5 col5_begin, ITR5 col5_end);
    //     Where ITR[12345] are iterators for columns 1, 2, 3, 4, and 5.
    //     They are iterators of StlVecType.
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

    // This transforms a column of containers into column of container’s value
    // types. For each data point in each container, index and data columns
    // data points will be repeated.
    // It returns a new DataFrame. Self will be unchanged.
    //
    // T:
    //   Type of the container column
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // col_name:
    //   Name of the container column
    //
    template<container T, typename ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    explode(const char *col_name) const;

    // This returns a DataFrame that is the difference between self and other.
    // The returned DataFrame has the exact same index column as self.
    // For each column in self and other with the same name, the returned
    // column is the difference between them.
    // For each comparison between pairs of columns, there will be two columns
    // in the returned DataFrame, if there is a difference between them. Their
    // names are prepended with "self_" and "other_". The returned column has
    // the value that is different on spots that there is a difference. In
    // spots that there is no difference there will be a NaN or default value
    // for the type. Columns that are in one DataFrame but not the other are
    // ignored and they are not in the returned DataFrame.
    //
    // NOTE: Index type and all data types must have equality (== !=)
    //       operators well defined.
    // NOTE: All data column types must have default construction well defined.
    // NOTE: Columns in self and other with the same name must have the same
    //       type.
    // NOTE: The index columns in self and other must be identical.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // other:
    //   The other DataFrame to compae with.
    //
    template<equality_default_construct ... Ts>
    [[nodiscard]] DataFrame<I, HeteroVector<std::size_t(H::align_value)>>
    difference(const DataFrame &other) const;

    //
    // DateTime specific functionalities
    //

    // This selects the rows using the index column at specified time. It
    // returns another DataFrame with selected data indexed by DateTime.
    // Self is unchanged.
    //
    // NOTE: The index column type must be DateTime or it won’t compile
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // hr:
    //   Specified hour
    // mn:
    //   Specified minute
    // sc:
    //   Specified second
    // msc:
    //   Specified milli-second
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<DateTime, HeteroVector<std::size_t(H::align_value)>>
    get_data_at_times(DateTime::HourType hr,  // 24 hour notation
                      DateTime::MinuteType mn = 0,
                      DateTime::SecondType sc = 0,
                      DateTime::MillisecondType msc = 0) const;

    // Same as get_data_at_times() above, but it returns a view
    //
    template<typename ... Ts>
    [[nodiscard]] PtrView
    get_view_at_times(DateTime::HourType hr,  // 24 hour notation
                      DateTime::MinuteType mn = 0,
                      DateTime::SecondType sc = 0,
                      DateTime::MillisecondType msc = 0);

    // Same as get_data_at_times() above, but it returns a const view
    //
    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_at_times(DateTime::HourType hr,  // 24 hour notation
                      DateTime::MinuteType mn = 0,
                      DateTime::SecondType sc = 0,
                      DateTime::MillisecondType msc = 0) const;

    // This selects the rows using the index column that happen before the
    // specified time. It returns another DataFrame with selected data indexed
    // by DateTime. The specified times are excluded. Self is unchanged.
    //
    // NOTE: The index column type must be DateTime or it won’t compile
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // hr:
    //   Specified hour
    // mn:
    //   Specified minute
    // sc:
    //   Specified second
    // msc:
    //   Specified milli-second
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<DateTime, HeteroVector<std::size_t(H::align_value)>>
    get_data_before_times(DateTime::HourType hr,  // 24 hour notation
                          DateTime::MinuteType mn = 0,
                          DateTime::SecondType sc = 0,
                          DateTime::MillisecondType msc = 0) const;

    // Same as get_view_before_times() above, but it returns a view
    //
    template<typename ... Ts>
    [[nodiscard]] PtrView
    get_view_before_times(DateTime::HourType hr,  // 24 hour notation
                          DateTime::MinuteType mn = 0,
                          DateTime::SecondType sc = 0,
                          DateTime::MillisecondType msc = 0);

    // Same as get_view_before_times() above, but it returns a const view
    //
    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_before_times(DateTime::HourType hr,  // 24 hour notation
                          DateTime::MinuteType mn = 0,
                          DateTime::SecondType sc = 0,
                          DateTime::MillisecondType msc = 0) const;

    // This selects the rows using the index column that happen after the
    // specified time. It returns another DataFrame with selected data indexed
    // by DateTime. The specified times are excluded. Self is unchanged.
    //
    // NOTE: The index column type must be DateTime or it won’t compile
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // hr:
    //   Specified hour
    // mn:
    //   Specified minute
    // sc:
    //   Specified second
    // msc:
    //   Specified milli-second
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<DateTime, HeteroVector<std::size_t(H::align_value)>>
    get_data_after_times(DateTime::HourType hr,  // 24 hour notation
                         DateTime::MinuteType mn = 0,
                         DateTime::SecondType sc = 0,
                         DateTime::MillisecondType msc = 0) const;

    // Same as get_view_after_times() above, but it returns a view
    //
    template<typename ... Ts>
    [[nodiscard]] PtrView
    get_view_after_times(DateTime::HourType hr,  // 24 hour notation
                         DateTime::MinuteType mn = 0,
                         DateTime::SecondType sc = 0,
                         DateTime::MillisecondType msc = 0);

    // Same as get_view_after_times() above, but it returns a const view
    //
    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_after_times(DateTime::HourType hr,  // 24 hour notation
                         DateTime::MinuteType mn = 0,
                         DateTime::SecondType sc = 0,
                         DateTime::MillisecondType msc = 0) const;

    // This selects the rows using the index column that happen between the
    // specified start and end time. It returns another DataFrame with selected
    // data indexed by DateTime. The specified start/end times are excluded.
    // Self is unchanged.
    //
    // NOTE: The index column type must be DateTime or it won’t compile
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // start_hr:
    //   Specified start hour
    // end_hr:
    //   Specified end hour
    // start_mn:
    //   Specified start minute
    // end_mn:
    //   Specified end minute
    // start_sc:
    //   Specified start second
    // end_sc:
    //   Specified end second
    // start_msc:
    //   Specified start milli-second
    // end_msc:
    //   Specified end milli-second
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<DateTime, HeteroVector<std::size_t(H::align_value)>>
    get_data_between_times(DateTime::HourType start_hr,  // 24 hour notation
                           DateTime::HourType end_hr,  // 24 hour notation
                           DateTime::MinuteType start_mn = 0,
                           DateTime::MinuteType end_mn = 0,
                           DateTime::SecondType start_sc = 0,
                           DateTime::SecondType end_sc = 0,
                           DateTime::MillisecondType start_msc = 0,
                           DateTime::MillisecondType end_msc = 0) const;

    // Same as get_view_after_times() above, but it returns a view
    //
    template<typename ... Ts>
    [[nodiscard]] PtrView
    get_view_between_times(DateTime::HourType start_hr,  // 24 hour notation
                           DateTime::HourType end_hr,  // 24 hour notation
                           DateTime::MinuteType start_mn = 0,
                           DateTime::MinuteType end_mn = 0,
                           DateTime::SecondType start_sc = 0,
                           DateTime::SecondType end_sc = 0,
                           DateTime::MillisecondType start_msc = 0,
                           DateTime::MillisecondType end_msc = 0);

    // Same as get_view_after_times() above, but it returns a const view
    //
    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_between_times(DateTime::HourType start_hr,  // 24 hour notation
                           DateTime::HourType end_hr,  // 24 hour notation
                           DateTime::MinuteType start_mn = 0,
                           DateTime::MinuteType end_mn = 0,
                           DateTime::SecondType start_sc = 0,
                           DateTime::SecondType end_sc = 0,
                           DateTime::MillisecondType start_msc = 0,
                           DateTime::MillisecondType end_msc = 0) const;

    // This selects the rows using the index column that happen on the specified
    // days of the week. It returns another DataFrame with selected data
    // indexed by DateTime. Self is unchanged
    //
    // NOTE: The index column type must be DateTime or it won’t compile
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // days:
    //   List of specified days
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<DateTime, HeteroVector<std::size_t(H::align_value)>>
    get_data_on_days(std::vector<DT_WEEKDAY> &&days) const;

    // Same as get_data_on_days() above, but it returns a view
    //
    template<typename ... Ts>
    [[nodiscard]] PtrView
    get_view_on_days(std::vector<DT_WEEKDAY> &&days);

    // Same as get_view_on_days() above, but it returns a const view
    //
    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_on_days(std::vector<DT_WEEKDAY> &&days) const;

    // This selects the rows using the index column that happen on the specified
    // days of the month. It returns another DataFrame with selected data
    // indexed by DateTime. Self is unchanged
    //
    // NOTE: The index column type must be DateTime or it won’t compile
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // days:
    //   List of specified days
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<DateTime, HeteroVector<std::size_t(H::align_value)>>
    get_data_on_days_in_month(std::vector<DateTime::DatePartType> &&days) const;

    // Same as get_data_on_days() above, but it returns a view
    //
    template<typename ... Ts>
    [[nodiscard]] PtrView
    get_view_on_days_in_month(std::vector<DateTime::DatePartType> &&days);

    // Same as get_view_on_days() above, but it returns a const view
    //
    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_on_days_in_month(std::vector<DateTime::DatePartType> &&days) const;

    // This selects the rows using the index column that happen in the
    // specified months. It returns another DataFrame with selected data
    // indexed by DateTime. Self is unchanged
    //
    // NOTE: The index column type must be DateTime or it won’t compile
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // months:
    //   List of specified months
    //
    template<typename ... Ts>
    [[nodiscard]] DataFrame<DateTime, HeteroVector<std::size_t(H::align_value)>>
    get_data_in_months(std::vector<DT_MONTH> &&months) const;

    // Same as get_data_on_months() above, but it returns a view
    //
    template<typename ... Ts>
    [[nodiscard]] PtrView
    get_view_in_months(std::vector<DT_MONTH> &&months);

    // Same as get_view_on_months() above, but it returns a const view
    //
    template<typename ... Ts>
    [[nodiscard]] ConstPtrView
    get_view_in_months(std::vector<DT_MONTH> &&months) const;

public:  // Visitors

    // apply is a shortcut for a simple visit. It applies the func to every
    // data point in the named column as long as func returns true. As soon as
    // func returns false apply stops.
    // func must receive a const reference to the index data point and a
    // reference to the named column data point.
    //
    // T:
    //   Type of the named column
    // col_name:
    //   Name of the column
    // func:
    //   Function to be applied to the named column with the specified
    //   signature.
    //
    template<typename T>
    void
    apply(const char *col_name,
          std::function<bool(const IndexType &, T &)> &&func);

    // Same as above but applied to two columns
    //
    // T1:
    //   Type of the first column
    // T2:
    //   Type of the second column
    // col_name1:
    //   Name of the first column
    // col_name2:
    //   Name of the second column
    // func:
    //   Function to be applied to the named columns with the specified
    //   signature.
    template<typename T1, typename T2>
    void
    apply(const char *col_name1, const char *col_name2,
          std::function<bool(const IndexType &, T1 &, T2 &)> &&func);

    // Same as above but applied to three columns
    //
    // T1:
    //   Type of the first column
    // T2:
    //   Type of the second column
    // T3:
    //   Type of the third column
    // col_name1:
    //   Name of the first column
    // col_name2:
    //   Name of the second column
    // col_name3:
    //   Name of the third column
    // func:
    //   Function to be applied to the named columns with the specified
    //   signature.
    template<typename T1, typename T2, typename T3>
    void
    apply(const char *col_name1, const char *col_name2, const char *col_name3,
          std::function<bool(const IndexType &, T1 &, T2 &, T3 &)> &&func);

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
    //                  &std::function<bool(const IndexType &,
    //                                      const char *,
    //                                      [const] T &)>)
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
    [[nodiscard]]
    StlVecType<std::tuple<ColNameType, size_type, std::type_index>>
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
    static StlVecType<I>
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
    static StlVecType<I>
    gen_sequence_index(const IndexType &start_value,
                       const IndexType &end_value,
                       long increment = 1);

public:  // Reading and writing

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
    // precision:
    //   Specifies the precision for floating point numbers
    // columns_only:
    //   If true, it won't write the index column
    // max_recs:
    //   Max number of rows to write. If it is positive, it will write max_recs
    //   from the beginning of DataFrame. If it is negative, it will write
    //   max_recs from the end of DataFrame
    //
    template<typename S, typename ... Ts>
    bool
    write(S &o,
          io_format iof = io_format::csv,
          std::streamsize precision = 12,
          bool columns_only = false,
          long max_recs = std::numeric_limits<long>::max()) const;

    template<typename ... Ts>
    bool
    write(const char *file_name,
          io_format iof = io_format::csv,
          std::streamsize precision = 12,
          bool columns_only = false,
          long max_recs = std::numeric_limits<long>::max()) const;

    // Same as write() above, but executed asynchronously
    //
    template<typename S, typename ... Ts>
    [[nodiscard]] std::future<bool>
    write_async(S &o,
                io_format iof = io_format::csv,
                std::streamsize precision = 12,
                bool columns_only = false,
                long max_recs = std::numeric_limits<long>::max()) const;

    template<typename ... Ts>
    [[nodiscard]] std::future<bool>
    write_async(const char *file_name,
                io_format iof = io_format::csv,
                std::streamsize precision = 12,
                bool columns_only = false,
                long max_recs = std::numeric_limits<long>::max()) const;

    // This is a convenient function (simple implementation) to convert a
    // DataFrame into a string that could be restored later by calling
    // from_string(). It utilizes the write() member function of DataFrame.
    // These functions could be used to transmit a DataFrame from one place to
    // another or store a DataFrame in databases, caches, …
    //
    // NOTE: The choice between to_string() and serialize() depends on the
    //       dataset. Some datasets (i.e. US Options market data) mostly
    //       contain small floating-point/integer numbers such as '.5', '.75'
    //       or  '123' and so on. These set of numbers will produce a smaller
    //       buffer size in string form compared with binary form, especially
    //       if there are millions of them. But generally, in most cases
    //       binary form is more efficient.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    // precision:
    //   Specifies the precision for floating point numbers
    //
    template<typename ... Ts>
    [[nodiscard]] std::string
    to_string(std::streamsize precision = 12) const;

    // Same as to_string() above, but executed asynchronously
    //
    template<typename ... Ts>
    [[nodiscard]] std::future<std::string>
    to_string_async(std::streamsize precision = 12) const;

    // This is similar to to_string() to serialize a DataFrame into a binary
    // buffer that could be restored later by calling deserialize(). It
    // utilizes the write() member function of DataFrame.
    // These functions could be used to transmit a DataFrame from one place to
    // another or store a DataFrame in databases, caches, …
    //
    // NOTE: Although this returns a std::string, the string contains binary
    //       data including potentially many null chars. The best way to read
    //       the string is by using .data() and .size() methods on std::string
    //
    // NOTE: The choice between to_string() and serialize() depends on the
    //       dataset. Some datasets (i.e. US Options market data) mostly
    //       contain small floating-point/integer numbers such as '.5', '.75'
    //       or  '123' and so on. These set of numbers will produce a smaller
    //       buffer size in string form compared with binary form, especially
    //       if there are millions of them. But generally, in most cases
    //       binary form is more efficient.
    //
    // Ts:
    //   List all the types of all data columns. A type should be specified in
    //   the list only once.
    //
    template<typename ... Ts>
    [[nodiscard]] std::string
    serialize() const;

    // Same as serialize() above, but executed asynchronously
    //
    template<typename ... Ts>
    [[nodiscard]] std::future<std::string>
    serialize_async() const;

    // This is similar to to_string() to serialize a DataFrame into a binary
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
    // starting_row:
    //   Zero-based number of the row to start reading. starting_row and
    //   num_rows can be used to read large files in chunks.
    // num_rows:
    //   Number of rows to read starting at starting_row
    //
    template<typename S>
    bool
    read(S &in_s,
         io_format iof = io_format::csv,
         bool columns_only = false,
         size_type starting_row = 0,
         size_type num_rows = std::numeric_limits<size_type>::max());

    bool
    read(const char *file_name,
         io_format iof = io_format::csv,
         bool columns_only = false,
         size_type starting_row = 0,
         size_type num_rows = std::numeric_limits<size_type>::max());

    // Same as read() above, but executed asynchronously
    //
    [[nodiscard]] std::future<bool>
    read_async(const char *file_name,
               io_format iof = io_format::csv,
               bool columns_only = false,
               size_type starting_row = 0,
               size_type num_rows = std::numeric_limits<size_type>::max());

    template<typename S>
    [[nodiscard]] std::future<bool>
    read_async(S &in_s,
               io_format iof = io_format::csv,
               bool columns_only = false,
               size_type starting_row = 0,
               size_type num_rows = std::numeric_limits<size_type>::max());

    // This is a convenient function (simple implementation) to restore a
    // DataFrame from a string that was previously generated by calling
    // to_string(). It utilizes the read() member function of DataFrame.
    // These functions could be used to transmit a DataFrame from one place to
    // another or store a DataFrame in databases, caches, …
    //
    // NOTE: The choice between to_string() and serialize() depends on the
    //       dataset. Some datasets (i.e. US Options market data) mostly
    //       contain small floating-point/integer numbers such as '.5', '.75'
    //       or  '123' and so on. These set of numbers will produce a smaller
    //       buffer size in string form compared with binary form, especially
    //       if there are millions of them. But generally, in most cases
    //       binary form is more efficient.
    //
    // data_frame:
    //   A null terminated string that was generated by calling to_string().
    //   It must contain a complete DataFrame
    //
    bool
    from_string(const char *data_frame);

    // Same as from_string() above, but executed asynchronously
    //
    [[nodiscard]] std::future<bool>
    from_string_async(const char *data_frame);

    // This is a convenient function (conceptually similar to from_string())
    // to restore a DataFrame from a binary buffer that was previously
    // generated by calling serialize(). It utilizes the read() member function
    // of DataFrame.
    // These functions could be used to transmit a DataFrame from one place to
    // another or store a DataFrame in databases, caches, …
    //
    // NOTE: The choice between to_string() and serialize() depends on the
    //       dataset. Some datasets (i.e. US Options market data) mostly
    //       contain small floating-point/integer numbers such as '.5', '.75'
    //       or  '123' and so on. These set of numbers will produce a smaller
    //       buffer size in string form compared with binary form, especially
    //       if there are millions of them. But generally, in most cases
    //       binary form is more efficient.
    //
    // data_frame:
    //   A std::string that was generated by calling serialize().
    //   It must contain a complete DataFrame in binary format
    //
    bool
    deserialize(const std::string &data_frame);

    // Same as deserialize() above, but executed asynchronously
    //
    [[nodiscard]] std::future<bool>
    deserialize_async(const std::string &data_frame);

private:

    // Internally used containers aligned with DataFrame alignment
    //
    template<typename K,
             typename HA = std::hash<K>,
             typename E = std::equal_to<K>>
    using DFUnorderedSet = std::unordered_set<K, HA, E, AllocatorType<K>>;

    template<typename K,
             typename T,
             typename HA = std::hash<K>,
             typename E = std::equal_to<K>>
    using DFUnorderedMap =
        std::unordered_map<K, T, HA, E, AllocatorType<std::pair<const K, T>>>;

    template<typename K, typename C = std::less<K>>
    using DFSet = std::set<K, C, AllocatorType<K>>;

    template<typename K, typename T, typename C = std::less<K>>
    using DFMap = std::map<K, T, C, AllocatorType<std::pair<const K, T>>>;

    // All DataFrames types should be able to access private data of other
    // DataFrame types
    //
    template<typename ALT_I, typename ALT_H>
    friend class DataFrame;

    // Maps column names to their vector index
    //
    using ColNameDict =
        DFUnorderedMap<ColNameType, size_type, std::hash<VirtualString>>;

    // List of column names and indices
    //
    using ColNameList = StlVecType<std::pair<ColNameType, size_type>>;

    // Vector of Heterogeneous vectors
    //
    using DataVecVec = std::vector<DataVec, AllocatorType<DataVec>>;

    // Data fields
    //
    DataVecVec      data_ { };       // Vector of Heterogeneous vectors
    IndexVecType    indices_ { };    // Vector
    ColNameDict     column_tb_ { };  // Hash table of name -> vector index

    // This is necessary to have a deterministic column order across all
    // implementations
    //
    ColNameList     column_list_ { };  // Vector of column names and indices

    inline static SpinLock  *lock_ { nullptr };  // No lock safety by default

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
#  include <DataFrame/Internals/DataFrame_slice.tcc>
#  include <DataFrame/Internals/DataFrame_join.tcc>
#  include <DataFrame/Internals/DataFrame_misc.tcc>
#  include <DataFrame/Internals/DataFrame_opt.tcc>
#  include <DataFrame/Internals/DataFrame_read.tcc>
#  include <DataFrame/Internals/DataFrame_set.tcc>
#  include <DataFrame/Internals/DataFrame_remove.tcc>
#  include <DataFrame/Internals/DataFrame_shift.tcc>
#  include <DataFrame/Internals/DataFrame_sort.tcc>
#  include <DataFrame/Internals/DataFrame_visit.tcc>
#  include <DataFrame/Internals/DataFrame_write.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
