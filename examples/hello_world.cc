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

#include <DataFrame/DataFrame.h>  // Main DataFrame header
#include <DataFrame/DataFrameFinancialVisitors.h>  // Financial algorithms
#include <DataFrame/DataFrameMLVisitors.h>  // Machine-learning algorithms
#include <DataFrame/DataFrameStatsVisitors.h>  // Statistical algorithms
#include <DataFrame/Utils/DateTime.h>  // Cool and handy date-time object

#include <iostream>

// -----------------------------------------------------------------------------

// DataFrame library is entirely under hmdf name-space
//
using namespace hmdf;

// A DataFrame with ulong index type
//
using ULDataFrame = StdDataFrame<unsigned long>;

// A DataFrame with string index type
//
using StrDataFrame = StdDataFrame<std::string>;

// A DataFrame with DateTime index type
//
using DTDataFrame = StdDataFrame<DateTime>;

// This is just some arbitrary type to show how any type could be in DataFrame
//
struct  MyData  {
    int         i { 10 };
    double      d { 5.5 };
    std::string s { "Boo" };

    MyData() = default;
};

// -----------------------------------------------------------------------------

// The main purpose of this file is to introduce the basic operations of DataFrame.
// For more advanced operations and a complete list of features with code samples, see documentation at:
// https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/DataFrame.html
//
int main(int, char *[]) {

    std::vector<unsigned long>  idx_col1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<MyData>         mydata_col (10);
    std::vector<int>            int_col1 = { 1, 2, -3, -4, 5, 6, 7, 8, 9, -10 };
    std::vector<double>         dbl_col1 = { 0.01, 0.02, 0.03, 0.03, 0.05, 0.06, 0.03, 0.08, 0.09, 0.03 };

    ULDataFrame ul_df1;

    // One way to load data into the DataFrame is one column at a time.
    // A DataFram column could be at most as long as its index column. So, you must load the index
    // first before loading any column
    //
    // Once you load a column or index, the data is moved to DataFrame. The original vectors are now empty.
    // There are other ways of loading data without the move
    //
    ul_df1.load_index(std::move(idx_col1));
    ul_df1.load_column<double>("dbl_col", std::move(dbl_col1));
    ul_df1.load_column<MyData>("my_data_col", std::move(mydata_col));
    ul_df1.load_column<int>("integers", std::move(int_col1));

    std::vector<unsigned long>  idx_col2 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<std::string>    str_col1 = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J" };
    std::vector<std::string>    str_col2 =
        { "Azadi", "Hello", " World", "!", "Hype", "cubic spline", "Foo", "Silverado", "Arash", "Pardis" };
    std::vector<double>         dbl_col2 = { 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0};
    ULDataFrame                 ul_df2;

    // Also, you can load data into a DataFrame all at once
    //
    ul_df2.load_data(std::move(idx_col2),
                     std::make_pair("string col", str_col1),
                     std::make_pair("Cool Column", str_col2),
                     std::make_pair("numbers", dbl_col2));

    StrDataFrame    ibm_df;

    // Also, you can load data into a DataFrame from a file, suporting a few different formats.
    // If the file cannot be found, an exception will be thrown.
    // If the DataFrame root directory is your current directory when running this, it should work fine.
    //
    ibm_df.read("data/IBM.csv", io_format::csv2);

    // To access a column, you must know its name (or index) and its type
    //
    auto       &str_col_ref = ul_df2.get_column<std::string>("string col");
    const auto &close_const_ref = ibm_df.get_column<double>("IBM_Close");
    const auto &index_vec = ibm_df.get_index();

    // In case of a "standard" DataFrame (not a view), the columns are returned
    // as a reference to a std::vector of type of that column.
    //
    std::cout << ul_df2.get_column<std::string>("Cool Column")[1]
              << ul_df2.get_column<std::string>("Cool Column")[2]
              << ul_df2.get_column<std::string>("Cool Column")[3]
              << std::endl;
    for (auto citer : str_col_ref)
        std::cout << citer << ", ";
    std::cout << std::endl;
    for (std::size_t i = 0; i < str_col_ref.size(); ++i)
        std::cout << str_col_ref[i] << ", ";
    std::cout << std::endl;
    std::cout << "There are " << close_const_ref.size() << " IBM close prices" << std::endl;
    std::cout << "There are " << index_vec.size() << " IBM indices" << std::endl;

    // You can write the data to a file or stdout in a few formats
    // You must specify all the column types, but only once
    // When writing to a file, the file name/path must be create-able.
    //
    ul_df2.write<std::ostream, std::string, double>(std::cout, io_format::csv2);
    ibm_df.write<double, long>("/tmp/test.json", io_format::json);

    // You can convert a DataFrame to a string and from a string back into a DataFrame.
    // This could be used to transmit a DataFrame from one place to another
    // or store a DataFrame in databases, caches, …
    //
    const std::string  str_ibm = ibm_df.to_string<double, long>();
    StrDataFrame       ibm_df_2;

    // Since we convert from native type to string and back, if you have
    // floating point numbers with long precisions, you may run into precision mismatches.
    // to_string() has a precision parameter you can adjust. The defualt is 12
    // which is a relatively high precision.
    //
    ibm_df_2.from_string(str_ibm.c_str());
    // std::cout << str_ibm << std::endl;

    using ul_idx_t = ULDataFrame::IndexType;  // This is just unsigned long

    // You can sort by one or multiple columns You must specify all the column types, but only once
    // Sort first by the index column in ascending order then by "string col" column in descending order
    //
    ul_df2.sort<ul_idx_t, std::string, double, std::string>(DF_INDEX_COL_NAME, sort_spec::ascen,
                                                            "string col", sort_spec::desce);

    // You could get another DataFrame by selecting on one or multiple columns
    // You must specify all the column types, but only once
    //
    auto    functor =
        [](const std::string &, const double &val)-> bool { return (val > 150.0); };
    auto    above_150_df =
        ibm_df.get_data_by_sel<double, decltype(functor), double, long>("IBM_Close", functor);

    // Or, you could choose to get a view. See docs for views
    //
    auto    above_150_view =
        ibm_df.get_view_by_sel<double, decltype(functor), double, long>("IBM_Close", functor);

    // You can get another DataFrame by group-bying on one or multiple columns
    // You must specify only the type(s) of column(s), you are group-bying
    //
    // Group-by column dbl_col, and I am specifying how to summarize the index
    // column and each of the other columns
    //
    auto    gb_df =
        ul_df1.groupby1<double>("dbl_col",
                                LastVisitor<ul_idx_t, ul_idx_t>(),
                                std::make_tuple("integers", "sum_int", SumVisitor<int>()),
                                std::make_tuple("my_data_col", "last_my_data", LastVisitor<MyData>()));

    // You can run statistical, financial, ML, … algorithms on one or multiple
    // columns by using visitors. You must specify the column(s) type(s)
    //
    StdVisitor<double, std::string> ibm_stdev;

    ibm_df.visit<double>("IBM_Close", ibm_stdev);
    std::cout << "Standard deviation of IBM close prices: " << ibm_stdev.get_result()
              << std::endl;

    // Now Let’s declare two DataFrames with index type of DateTime
    // which is a handy object for date/time manipulations.
    //
    DTDataFrame dt_ibm;
    DTDataFrame dt_aapl;

    // Let’s read the AAPL and IBM market data from their files.
    // The data for these two stocks start and end at different dates.
    // But there is overlapping data between them
    //
    dt_ibm.read("data/DT_IBM.csv", io_format::csv2);
    dt_aapl.read("data/DT_AAPL.csv", io_format::csv2);

    // Now we join the AAPL and IBM DataFrames using their indices and applying inner-join policy
    //
    DTDataFrame aapl_ibm = dt_ibm.join_by_index<DTDataFrame, double, long>(dt_aapl, join_policy::inner_join);

    // Now we calculate the Pearson correlation coefficient between AAPL and IBM close prices
    //
    CorrVisitor<double, DateTime>   aapl_ibm_corrl;

    std::cout << "Correlation between AAPL and IBM close prices: "
              << aapl_ibm.visit<double, double>("AAPL_Close", "IBM_Close", aapl_ibm_corrl).get_result()
              << std::endl;

    using dt_idx_t = DTDataFrame::IndexType;  // This is just DateTime

    // Appel data are daily. Let’s create 10-day OHLC (plus mean, std, total volume) for close prices.
    //
    DTDataFrame aapl_ohlc =
        dt_aapl.bucketize(bucket_type::by_count,
                          10,
                          LastVisitor<dt_idx_t, dt_idx_t>(),
                          std::make_tuple("AAPL_Close", "Open", FirstVisitor<double, dt_idx_t>()),
                          std::make_tuple("AAPL_Close", "High", MaxVisitor<double, dt_idx_t>()),
                          std::make_tuple("AAPL_Close", "Low", MinVisitor<double, dt_idx_t>()),
                          std::make_tuple("AAPL_Close", "Close", LastVisitor<double, dt_idx_t>()),
                          std::make_tuple("AAPL_Close", "Mean", MeanVisitor<double, dt_idx_t>()),
                          std::make_tuple("AAPL_Close", "Std", StdVisitor<double, dt_idx_t>()),
                          std::make_tuple("AAPL_Volume", "Volume", SumVisitor<long, dt_idx_t>()));

    // Now, let's get a view of a random sample of appel data.
    // We randomley sample 35% of the data
    //
    auto    random_view =
        dt_aapl.get_view_by_rand<double, long>(random_policy::frac_rows_no_seed, 0.35);

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
