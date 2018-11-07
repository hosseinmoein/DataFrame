// Hossein Moein
// September 11, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include "HeteroVector.h"

#include <bitset>
#include <limits>
#include <map>
#include <stdexcept>
#include <future>
#include <cstring>

// ----------------------------------------------------------------------------

namespace hmdf
{

struct DataFrameError : public std::runtime_error  {

    DataFrameError (const char *name) : std::runtime_error (name)  {   }
};
struct ColNotFound : public DataFrameError  {

    ColNotFound (const char *name) : DataFrameError (name)  {   }
};
struct InconsistentData : public DataFrameError  {

    InconsistentData (const char *name) : DataFrameError (name)  {   }
};
struct BadRange : public DataFrameError  {

    BadRange (const char *name) : DataFrameError (name)  {   }
};
struct NotImplemented : public DataFrameError  {

    NotImplemented (const char *name) : DataFrameError (name)  {   }
};

// ----------------------------------------------------------------------------

enum class nan_policy : bool  {
    pad_with_nans = true,
    dont_pad_with_nans = false
};

enum class sort_state : bool  {
    sorted = true,
    not_sorted = false
};

enum class join_policy : unsigned char  {
    inner_join = 1,
    left_join = 2,
    right_join = 3,
    left_right_join = 4  // This is merge
};

// It represents a range with begin and end within a continuous memory space
//
template<typename T>
struct Index2D  {
    T   begin {};
    T   end {};
};

template<typename T, typename U>
struct type_declare;

template<typename U>
struct type_declare<HeteroVector, U>  { using type = std::vector<U>; };

template<typename U>
struct type_declare<HeteroView, U>  { using type = VectorView<U>; };

// ----------------------------------------------------------------------------

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
class DataFrame  {

    static_assert(std::is_base_of<HeteroVector, HETERO>::value or
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
    // data type
    //
    // T: The single type for all data columns
    // V: The type of string vector specifying the new names for new columns
    //    after transpose
    // idx: A vector on indices for the new transposed DataFrame.
    //      Its length must equal the number of rows in this DataFrame.
    //      Otherwise an exception is thrown
    // col_names: A vector of strings, specifying the column names for the
    //            new transposed DataFrame.
    //            Its length must equal the number of rows in this DataFrame.
    //            Otherwise an exception is thrown
    //
    template<typename T, typename V>
    DataFrame transpose(TSVec &&indices, const V &col_names) const;

    // NOT IMPLEMENTED YET
    template<typename RHS_T, typename ... types>
    StdDataFrame<TS> join_by_index (const RHS_T &rhs, join_policy mp) const;

    // NOT IMPLEMENTED YET
    template<typename RHS_T, typename T, typename ... types>
    StdDataFrame<TS> join_by_column (const RHS_T &rhs,
                                     const char *col_name,
                                     join_policy mp) const;

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

    // It returns a DataFrame (including the index and data columns)
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
    DataFrame &modify_by_idx(
        DataFrame &rhs,
        sort_state already_sorted = sort_state::not_sorted);

protected:

    template<typename T1, typename T2>
    size_type _load_pair(std::pair<T1, T2> &col_name_data);

    template<typename T>
    static inline constexpr T _get_nan();

    template<typename T>
    static inline constexpr bool _is_nan(const T &val);

private:  // Visiting functors

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

    template<typename LHS_T, typename RHS_T, typename COL_T, typename ... types>
    static StdDataFrame<TS>
    column_inner_join_(const char *col_name,
                       const LHS_T &lhs,
                       const RHS_T &rhs);

    template<typename LHS_T, typename RHS_T, typename COL_T, typename ... types>
    static StdDataFrame<TS>
    column_left_join_(const char *col_name, const LHS_T &lhs, const RHS_T &rhs);

    template<typename LHS_T, typename RHS_T, typename COL_T, typename ... types>
    static StdDataFrame<TS>
    column_right_join_(const char *col_name,
                       const LHS_T &lhs,
                       const RHS_T &rhs);

    template<typename LHS_T, typename RHS_T, typename COL_T, typename ... types>
    static StdDataFrame<TS>
    column_left_right_join_(const char *col_name,
                            const LHS_T &lhs,
                            const RHS_T &rhs);

#   include "DataFrame_functors.h"

private:  // Tuple stuff

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
#    include "DataFrame_join.tcc"
#    include "DataFrame.tcc"
#  endif // DMS_INCLUDE_SOURCE

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
