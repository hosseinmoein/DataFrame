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
#include <DataFrame/DataFrame.h>
#include <DataFrame/DataFrameFinancialVisitors.h>
#include <DataFrame/DataFrameMLVisitors.h>
#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/DataFrameTransformVisitors.h>
#include <DataFrame/RandGen.h>

#include <cassert>
#include <iostream>
#include <string>

using namespace hmdf;

// A DataFrame with ulong index type
//
using MyDataFrame = StdDataFrame<unsigned long>;

// -----------------------------------------------------------------------------

static void test_groupby_edge()  {

    std::cout << "\nTesting groupby( ) ..." << std::endl;

    MyDataFrame                df;
    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0,
          3.0, 9.0, 10.0};
    std::vector<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8,
          111.0, 112.0, 113.0, 114.0, 115.0, 116.0};
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };
    dblvec2.resize(1);
    dblvec.resize(1);
    strvec.resize(1);
    intvec.resize(1);
    idxvec.resize(1); // make this greater then one to fix coredump with hack
    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col", std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    FactorizeVisitor<double>    fact([] (const double &f) -> bool {
                                         return (f > 11106.0 && f < 114.0);
                                     });
    df.load_column("bool_col",
                   df.single_act_visit<double>("dbl_col_2", fact).get_result());

    df.write<std::ostream, std::string, double, int, bool>
        (std::cout, io_format::csv2);

    auto bool_df = df.groupby1<bool>(
        "bool_col",
        LastVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
        std::make_tuple("dbl_col_2", "sum_dbl2", SumVisitor<double>()),
        std::make_tuple("dbl_col_2", "cnt_dbl2", CountVisitor<double>()));
    bool_df.write<std::ostream, double, std::size_t, bool>
        (std::cout, io_format::csv2);
}

// -----------------------------------------------------------------------------

static void test_concat_view()  {

    std::cout << "\nTesting concat_view( ) ..." << std::endl;

    MyDataFrame df1;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, 14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0,
          9.0, 10.0 };
    std::vector<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8, 111.0,
          112.0, 113.0, 114.0, 115.0, 116.0 };
    std::vector<int>            intvec =
        { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9, 10, 15, 12, 13 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df1.load_data(std::move(idxvec),
                  std::make_pair("dbl_col", dblvec),
                  std::make_pair("int_col", intvec),
                  std::make_pair("str_col", strvec));

    MyDataFrame df2 = df1;

    df2.load_column("dbl_col_2", std::move(dblvec2));

    auto    result1 =
        df1.concat_view<decltype(df2), double, int, std::string>(df2);

    assert(result1.get_index().size() == 30);
    assert(result1.get_column<double>("dbl_col").size() == 30);
    assert(result1.get_column<std::string>("str_col").size() == 30);
    assert(result1.get_column<int>("int_col").size() == 30);
    assert(result1.get_index()[0] == 1);
    assert(result1.get_index()[14] == 14);
    assert(result1.get_index()[15] == 1);
    assert(result1.get_index()[29] == 14);
    assert(result1.get_column<std::string>("str_col")[0] == "zz");
    assert(result1.get_column<std::string>("str_col")[14] == "oo");
    assert(result1.get_column<std::string>("str_col")[15] == "zz");
    assert(result1.get_column<int>("int_col")[0] == 1);
    assert(result1.get_column<int>("int_col")[14] == 13);
    assert(result1.get_column<int>("int_col")[15] == 1);

    auto    result2 =
        df2.concat_view<decltype(df2), double, int, std::string>
            (df1, concat_policy::lhs_and_common_columns);

    assert(result2.get_index().size() == 30);
    assert(result2.get_column<double>("dbl_col").size() == 30);
    assert(result2.get_column<std::string>("str_col").size() == 30);
    assert(result2.get_column<std::string>("str_col")[0] == "zz");
    assert(result2.get_column<std::string>("str_col")[14] == "oo");
    assert(result2.get_column<std::string>("str_col")[15] == "zz");
    assert(result2.get_column<double>("dbl_col_2").size() == 15);
    assert(result2.get_column<double>("dbl_col_2")[0] == 100.0);
    assert(result2.get_column<double>("dbl_col_2")[5] == 105.0);
    assert(result2.get_column<double>("dbl_col_2")[10] == 112.0);

    auto    result3 =
        df1.concat_view<decltype(df1), double, int, std::string>
            (df2, concat_policy::all_columns);

    assert(result3.get_index().size() == 30);
    assert(result3.get_column<double>("dbl_col").size() == 30);
    assert(result3.get_column<std::string>("str_col").size() == 30);
    assert(result3.get_column<double>("dbl_col_2").size() == 15);
    assert(result3.get_column<std::string>("str_col")[0] == "zz");
    assert(result3.get_column<std::string>("str_col")[14] == "oo");
    assert(result3.get_column<std::string>("str_col")[15] == "zz");
    assert(result3.get_column<double>("dbl_col_2").size() == 15);
    assert(result3.get_column<double>("dbl_col_2")[0] == 100.0);
    assert(result3.get_column<double>("dbl_col_2")[5] == 105.0);
    assert(result3.get_column<double>("dbl_col_2")[10] == 112.0);
}

// -----------------------------------------------------------------------------

/*
static void test_multithreading(int j)  {

    std::cout << "\nTesting test ..." << std::endl;

    MyDataFrame                df;
    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0,
          3.0, 9.0, 10.0};
    std::vector<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8,
          111.0, 112.0, 113.0, 114.0, 115.0, 116.0};
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col", std::move(intvec),
                   nan_policy::dont_pad_with_nans);
    std::cout << "PRINTING FIRST ..." << std::endl;
    df.write<std::ostream, std::string, double, int, bool>
        (std::cout, io_format::json);
    FactorizeVisitor<double>    fact([] (const double &f) -> bool {
                                         return (f > 11106.0 && f < 114.0);
                                     });
    df.load_column("bool_col",
                   df.single_act_visit<double>("dbl_col_2", fact).get_result());

    auto& xvec = df.get_column<std::string>("str_col");
    auto& yvec = df.get_column<double>("dbl_col_2");
    auto m = df;
    std::cout << "PRINTING AFTER FACTOR ..." << std::endl;
    m.write<std::ostream, std::string, double, int, bool>
        (std::cout, io_format::json);

    auto bool_df = m.groupby1<bool>(
        "bool_col",
        LastVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
        std::make_tuple("dbl_col_2", "sum_dbl2", SumVisitor<double>()),
        std::make_tuple("dbl_col_2", "cnt_dbl2", CountVisitor<double>()));
    std::cout << "PRINTING AFTER GROUPBY ..." << std::endl;
    bool_df.write<std::ostream, double, std::size_t, bool>
        (std::cout, io_format::json);
}
*/

// -----------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    test_groupby_edge();
    test_concat_view();

    /*
    hmdf::SpinLock      locker;
    static const int    thread_count = 10;

    MyDataFrame::set_lock(&locker);
    for (int i = 0; i < 100; ++i)  {
        std::thread threads[thread_count];

        for (size_t j = 0; j < thread_count; ++j)
            threads[j] = std::thread(test_multithreading, j);
        for (size_t j = 0; j < thread_count; ++j)
            threads[j].join();
    }
    MyDataFrame::remove_lock();
    */

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
