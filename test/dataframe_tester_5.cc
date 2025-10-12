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

#include <DataFrame/DataFrame.h>
#include <DataFrame/DataFrameFinancialVisitors.h>
#include <DataFrame/DataFrameMLVisitors.h>
#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/DataFrameTransformVisitors.h>
#include <DataFrame/RandGen.h>

#include <cassert>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <ranges>
#include <string>

using namespace hmdf;

// A DataFrame with ulong index type
//
using ULDataFrame = StdDataFrame<unsigned long>;
using MyDataFrame = StdDataFrame<unsigned long>;
using MyStdDataFrame = StdDataFrame<unsigned long>;
using StrDataFrame = StdDataFrame<std::string>;
using DTDataFrame = StdDataFrame256<DateTime>;
using StrDataFrame2 = StdDataFrame256<std::string>;

template<typename T>
using StlVecType = typename MyDataFrame::template StlVecType<T>;

// ----------------------------------------------------------------------------

static void test_permutation_vec()  {

    std::cout << "\nTesting permutation_vec( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  res1 =
        df.permutation_vec<double>("IBM_Close", sort_spec::ascen);
    const auto  &close = df.get_column<double>("IBM_Close");

    assert(close.size() == res1.size());
    for (std::size_t i { 1 }; i < close.size(); ++i)
        assert(close[res1[i - 1]] <= close[res1[i]]);

    const auto  res2 =
        df.permutation_vec<double>("IBM_Close", sort_spec::desce);

    assert(close.size() == res2.size());
    for (std::size_t i { 1 }; i < close.size(); ++i)
        assert(close[res2[i - 1]] >= close[res2[i]]);

    const auto  res3 =
        df.permutation_vec<double, long>("IBM_Close", sort_spec::ascen,
                                         "IBM_Volume", sort_spec::desce);
    const auto  &volume = df.get_column<long>("IBM_Volume");

    assert(res3.size() == std::min(close.size(), volume.size()));
    for (std::size_t i { 1 }; i < std::min(close.size(), volume.size()); ++i) {
        if (close[res3[i - 1]] == close[res3[i]])
            assert(volume[res3[i - 1]] >= volume[res3[i]]);
        else
            assert(close[res3[i - 1]] <= close[res3[i]]);
    }

    const auto  res4 =
        df.permutation_vec<double, long, double>(
            "IBM_Close", sort_spec::ascen,
            "IBM_Volume", sort_spec::desce,
            "IBM_Open", sort_spec::desce);
    const auto  &open = df.get_column<double>("IBM_Open");
    const auto  min_size =
        std::min({ close.size(), volume.size(), open.size() });

    assert(res4.size() == min_size);
    for (std::size_t i { 1 }; i < min_size; ++i) {
        if (close[res4[i - 1]] == close[res4[i]])  {
            if (volume[res4[i - 1]] == volume[res4[i]])
                assert(open[res4[i - 1]] >= open[res4[i]]);
            else
                assert(volume[res4[i - 1]] >= volume[res4[i]]);
        }
        else
            assert(close[res4[i - 1]] <= close[res4[i]]);
    }
}

// ----------------------------------------------------------------------------

static void test_get_data_every_n()  {

    std::cout << "\nTesting get_data_every_n( ) ..." << std::endl;

    MyDataFrame                df;
    StlVecType<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
          19, 20, 21 };
    StlVecType<double>         dblvec =
        { 0, 15, -14, 2, 1, 12, 11, 8, 7, 6, 5, 9, 10, 1, 2, 3, 4, 5 };
    StlVecType<double>         dblvec2 =
        { 100, 101, 102, 103, 104, 105, 106.55, 107.34, 1.8, 111, 112, 113,
          114, 115, 116, 50, 51, 52, 53, 54, 55 };
    StlVecType<int>            intvec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    StlVecType<std::string>    strvec =
        { "zz", "bb", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2));
    df.load_column("int_col", std::move(intvec),
                   nan_policy::dont_pad_with_nans);
    df.load_column("str_col", std::move(strvec),
                   nan_policy::dont_pad_with_nans);

    auto    df2 = df.get_data_every_n<double, std::string, int>(3, 1);

    assert(df2.get_index().size() == 7);
    assert(df2.get_index()[0] == 2);
    assert(df2.get_index()[3] == 11);
    assert(df2.get_index()[6] == 20);

    assert(df2.get_column<double>("dbl_col").size() == 7);
    assert(df2.get_column<double>("dbl_col")[0] == 15.0);
    assert(df2.get_column<double>("dbl_col")[3] == 5.0);
    assert(std::isnan(df2.get_column<double>("dbl_col")[6]));

    assert(df2.get_column<double>("dbl_col_2").size() == 7);
    assert(df2.get_column<double>("dbl_col_2")[0] == 101.0);
    assert(df2.get_column<double>("dbl_col_2")[3] == 112.0);
    assert(df2.get_column<double>("dbl_col_2")[6] == 54.0);

    assert(df2.get_column<int>("int_col").size() == 3);
    assert(df2.get_column<int>("int_col")[0] == 2);
    assert(df2.get_column<int>("int_col")[1] == 5);
    assert(df2.get_column<int>("int_col")[2] == 8);

    assert(df2.get_column<std::string>("str_col").size() == 4);
    assert(df2.get_column<std::string>("str_col")[0] == "bb");
    assert(df2.get_column<std::string>("str_col")[1] == "ff");
    assert(df2.get_column<std::string>("str_col")[3] == "ll");

    const auto  view = df.get_view_every_n<double, std::string, int>(3, 1);

    assert(view.get_index().size() == 7);
    assert(view.get_index()[0] == 2);
    assert(view.get_index()[3] == 11);
    assert(view.get_index()[6] == 20);

    assert(view.get_column<double>("dbl_col").size() == 7);
    assert(view.get_column<double>("dbl_col")[0] == 15.0);
    assert(view.get_column<double>("dbl_col")[3] == 5.0);
    assert(std::isnan(view.get_column<double>("dbl_col")[6]));

    assert(view.get_column<double>("dbl_col_2").size() == 7);
    assert(view.get_column<double>("dbl_col_2")[0] == 101.0);
    assert(view.get_column<double>("dbl_col_2")[3] == 112.0);
    assert(view.get_column<double>("dbl_col_2")[6] == 54.0);

    assert(view.get_column<int>("int_col").size() == 3);
    assert(view.get_column<int>("int_col")[0] == 2);
    assert(view.get_column<int>("int_col")[1] == 5);
    assert(view.get_column<int>("int_col")[2] == 8);

    assert(view.get_column<std::string>("str_col").size() == 4);
    assert(view.get_column<std::string>("str_col")[0] == "bb");
    assert(view.get_column<std::string>("str_col")[1] == "ff");
    assert(view.get_column<std::string>("str_col")[3] == "ll");
}

// ----------------------------------------------------------------------------

static void test_get_n_largest_data()  {

    std::cout << "\nTesting get_n_largest_data( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto        res_df =
        df.get_n_largest_data<double, double, long>("IBM_Close", 5);
    const auto  res_vw =
        df.get_n_largest_view<double, double, long>("IBM_Close", 5);

    assert(res_df.get_index().size() == 5);
    assert(res_vw.get_index().size() == 5);
    assert((res_df.get_index()[0] == "2013-03-14"));
    assert((res_vw.get_index()[0] == "2013-03-14"));
    assert((res_df.get_index()[3] == "2013-04-02"));
    assert((res_vw.get_index()[3] == "2013-04-02"));
    assert(res_df.get_column<double>("IBM_Low").size() == 5);
    assert(res_vw.get_column<double>("IBM_Low").size() == 5);
    assert(res_df.get_column<double>("IBM_Close").size() == 5);
    assert(res_vw.get_column<double>("IBM_Close").size() == 5);
    assert((
        std::fabs(res_df.get_column<double>("IBM_Close")[1] - 215.06) < 0.01));
    assert((
        std::fabs(res_vw.get_column<double>("IBM_Close")[1] - 215.06) < 0.01));
    assert((
        std::fabs(res_df.get_column<double>("IBM_Close")[2] - 214.92) < 0.01));
    assert((
        std::fabs(res_vw.get_column<double>("IBM_Close")[2] - 214.92) < 0.01));
    assert((res_df.get_column<long>("IBM_Volume")[3] == 3116800));
    assert((res_vw.get_column<long>("IBM_Volume")[3] == 3116800));
}

// ----------------------------------------------------------------------------

static void test_get_n_smallest_data()  {

    std::cout << "\nTesting get_n_smallest_data( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto        res_df =
        df.get_n_smallest_data<double, double, long>("IBM_Close", 5);
    const auto  res_vw =
        df.get_n_smallest_view<double, double, long>("IBM_Close", 5);

    assert(res_df.get_index().size() == 5);
    assert(res_vw.get_index().size() == 5);
    assert((res_df.get_index()[0] == "2002-10-09"));
    assert((res_vw.get_index()[0] == "2002-10-09"));
    assert((res_df.get_index()[3] == "2002-10-08"));
    assert((res_vw.get_index()[3] == "2002-10-08"));
    assert(res_df.get_column<double>("IBM_Low").size() == 5);
    assert(res_vw.get_column<double>("IBM_Low").size() == 5);
    assert(res_df.get_column<double>("IBM_Close").size() == 5);
    assert(res_vw.get_column<double>("IBM_Close").size() == 5);
    assert((
        std::fabs(res_df.get_column<double>("IBM_Close")[1] - 56.6) < 0.01));
    assert((
        std::fabs(res_vw.get_column<double>("IBM_Close")[1] - 56.6) < 0.01));
    assert((
        std::fabs(res_df.get_column<double>("IBM_Close")[2] - 56.86) < 0.01));
    assert((
        std::fabs(res_vw.get_column<double>("IBM_Close")[2] - 56.86) < 0.01));
    assert((res_df.get_column<long>("IBM_Volume")[3] == 14744500));
    assert((res_vw.get_column<long>("IBM_Volume")[3] == 14744500));
}

// ----------------------------------------------------------------------------

static void test_is_nan_mask()  {

    std::cout << "\nTesting is_nan_mask( ) ..." << std::endl;

    const double                nval = std::numeric_limits<double>::quiet_NaN();
    ULDataFrame                 df;
    std::vector<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    std::vector<double>         dblvec =
        { 0, 15, -14, 2, 1, 12, 11, 8, 7, 6, 5, 4, 3, 9, 10};
    std::vector<double>         dblvec2 =
        { 100, 101, nval, 103, 104, 103.9, 106.55, 106.34, 1.8, nval, 112,
          111.5, 114, nval, nval};
    std::vector<double>         dblempty { };
    std::vector<double>         allnan =
        { nval, nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
          nval, nval, nval, nval };
    std::vector<std::string>    strvec =
        { "", "bb", "cc", "ww", "", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("Empty Col", dblempty),
                 std::make_pair("All NaN Col", allnan),
                 std::make_pair("str_col", strvec));

    const auto  res1 = df.is_nan_mask<double>("dbl_col");

    assert((res1 ==
            std::vector<char>{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));

    const auto  res2 = df.is_nan_mask<double>("dbl_col", true);

    assert((res2 ==
            std::vector<char>{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }));

    const auto  res3 = df.is_nan_mask<double>("dbl_col_2");

    assert((res3 ==
            std::vector<char>{ 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1 }));

    const auto  res4 = df.is_nan_mask<double>("dbl_col_2", true);

    assert((res4 ==
            std::vector<char>{ 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 0 }));

    const auto  res5 = df.is_nan_mask<double>("All NaN Col", true);

    assert((res5 ==
            std::vector<char>{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));
}

// ----------------------------------------------------------------------------

static void test_is_infinity_mask()  {

    std::cout << "\nTesting is_infinity_mask( ) ..." << std::endl;

    const double                ival = std::numeric_limits<double>::infinity();
    ULDataFrame                 df;
    std::vector<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    std::vector<double>         dblvec =
        { 0, 15, -14, 2, 1, 12, 11, 8, 7, 6, 5, 4, 3, 9, 10};
    std::vector<double>         dblvec2 =
        { 100, 101, ival, 103, 104, 103.9, 106.55, 106.34, 1.8, ival, 112,
          111.5, 114, ival, ival};
    std::vector<double>         dblempty { };
    std::vector<double>         allinfinity =
        { ival, ival, ival, ival, ival, ival, ival, ival, ival, ival, ival,
          ival, ival, ival, ival };
    std::vector<std::string>    strvec =
        { "", "bb", "cc", "ww", "", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("All Infinity Col", allinfinity),
                 std::make_pair("str_col", strvec));
    df.load_column("Empty Col", std::move(dblempty),
                   nan_policy::dont_pad_with_nans);

    const auto  res1 = df.is_infinity_mask<double>("dbl_col");

    assert((res1 ==
            std::vector<char>{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));

    const auto  res2 = df.is_infinity_mask<double>("dbl_col", true);

    assert((res2 ==
            std::vector<char>{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }));

    const auto  res3 = df.is_infinity_mask<double>("dbl_col_2");

    assert((res3 ==
            std::vector<char>{ 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1 }));

    const auto  res4 = df.is_infinity_mask<double>("dbl_col_2", true);

    assert((res4 ==
            std::vector<char>{ 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 0 }));

    const auto  res5 = df.is_infinity_mask<double>("All Infinity Col", true);

    assert((res5 ==
            std::vector<char>{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));

    const auto  res6 = df.is_infinity_mask<double>("Empty Col");

    assert(res6.empty());
}

// ----------------------------------------------------------------------------

static void test_is_default_mask()  {

    std::cout << "\nTesting is_default_mask( ) ..." << std::endl;

    ULDataFrame                 df;
    std::vector<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    std::vector<double>         dblvec =
        { 30, 15, -14, 2, 1, 12, 11, 8, 7, 6, 5, 4, 3, 9, 10};
    std::vector<double>         dblvec2 =
        { 100, 101, 0, 103, 104, 103.9, 106.55, 106.34, 1.8, 0, 112,
          111.5, 114, 0, 0};
    std::vector<double>         dblempty { };
    std::vector<double>         alldefault =
        { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    std::vector<std::string>    strvec =
        { "", "bb", "cc", "ww", "", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("All Default Col", alldefault),
                 std::make_pair("str_col", strvec));
    df.load_column("Empty Col", std::move(dblempty),
                   nan_policy::dont_pad_with_nans);

    const auto  res1 = df.is_default_mask<double>("dbl_col");

    assert((res1 ==
            std::vector<char>{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));

    const auto  res2 = df.is_default_mask<double>("dbl_col", true);

    assert((res2 ==
            std::vector<char>{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }));

    const auto  res3 = df.is_default_mask<double>("dbl_col_2");

    assert((res3 ==
            std::vector<char>{ 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1 }));

    const auto  res4 = df.is_default_mask<double>("dbl_col_2", true);

    assert((res4 ==
            std::vector<char>{ 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 0 }));

    const auto  res5 = df.is_default_mask<double>("All Default Col", true);

    assert((res5 ==
            std::vector<char>{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));

    const auto  res6 = df.is_default_mask<double>("Empty Col");

    assert(res6.empty());

    const auto  res7 = df.is_default_mask<std::string>("str_col");

    assert((res7 ==
            std::vector<char>{ 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }));
}

// ----------------------------------------------------------------------------

static void test_SkewVisitor()  {

    std::cout << "\nTesting SkewVisitor{ } ..." << std::endl;

    ULDataFrame                 df;
    std::vector<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    std::vector<double>         dblvec =
        { 2.2, 4, 5, 15.5, 7, 8, 4.4, 12, 12.6, 11, 6.8, 5.5, 10.1, 9.8, 1.2 };
    std::vector<double>         dblvec2 =
        { 100, 101, 0, 103, 104, 103.9, 106.55, 106.34, 1.8, 0, 112,
          111.5, 114, 1.2, 3.3 };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2));

    SkewVisitor<double, unsigned long>  skew;

    df.visit<double>("dbl_col", skew);
    assert(std::fabs(skew.get_result() - 0.2049) < 0.0001);
    df.visit<double>("dbl_col_2", skew);
    assert(std::fabs(skew.get_result() - -0.6896) < 0.0001);
}

// ----------------------------------------------------------------------------

static void test_KurtosisVisitor()  {

    std::cout << "\nTesting KurtosisVisitor{ } ..." << std::endl;

    ULDataFrame                 df;
    std::vector<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    std::vector<double>         dblvec =
        { 2.2, 4, 5, 15.5, 7, 8, 4.4, 12, 12.6, 11, 6.8, 5.5, 10.1, 9.8, 1.2 };
    std::vector<double>         dblvec2 =
        { 100, 101, 0, 103, 104, 103.9, 106.55, 106.34, 1.8, 0, 112,
          111.5, 114, 1.2, 3.3 };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2));

    KurtosisVisitor<double, unsigned long>  kurt;

    df.visit<double>("dbl_col", kurt);
    assert(std::fabs(kurt.get_result() - -0.8557) < 0.0001);
    df.visit<double>("dbl_col_2", kurt);
    assert(std::fabs(kurt.get_result() - -1.4976) < 0.0001);
}

// ----------------------------------------------------------------------------

static void test_ConfIntervalVisitor()  {

    std::cout << "\nTesting ConfIntervalVisitor{ } ..." << std::endl;

    using StrDataFrame = StdDataFrame<std::string>;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }
    df.load_column("Extra Col",
                   std::vector<double> { 5.1, 4.9, 5.0, 5.3, 5.2, 4.8 },
                   nan_policy::dont_pad_with_nans);

    ConfIntervalVisitor<double, std::string>    ci_v1 { 0.95 };

    df.single_act_visit<double>("Extra Col", ci_v1);
    assert(std::fabs(ci_v1.get_error_margin() - 0.149697) < 0.000001);
    assert(std::fabs(ci_v1.get_result().first - 4.9003) < 0.0001);
    assert(std::fabs(ci_v1.get_result().second - 5.1997) < 0.0001);

    ConfIntervalVisitor<double, std::string>    ci_v2 { 0.96 };

    df.single_act_visit<double>("Extra Col", ci_v2);
    assert(std::fabs(ci_v2.get_error_margin() - 0.159023) < 0.000001);
    assert(std::fabs(ci_v2.get_result().first - 4.89098) < 0.00001);
    assert(std::fabs(ci_v2.get_result().second - 5.20902) < 0.00001);

    coni_v<double, std::string> ci_v3 { 0.99 };

    df.single_act_visit<double>("IBM_Close", ci_v3);
    assert(std::fabs(ci_v3.get_error_margin() - 1.39119) < 0.00001);
    assert(std::fabs(ci_v3.get_result().first - 128.601) < 0.001);
    assert(std::fabs(ci_v3.get_result().second - 131.383) < 0.001);
}

// ----------------------------------------------------------------------------

static void test_CoeffVariationVisitor()  {

    std::cout << "\nTesting ConfIntervalVisitor{ } ..." << std::endl;

    using StrDataFrame = StdDataFrame<std::string>;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }
    df.load_column("Extra Col",
                   std::vector<double> { 5.1, 4.9, 5.0, 5.3, 5.2, 4.8 },
                   nan_policy::dont_pad_with_nans);

    CoeffVariationVisitor<double, std::string>    cv_v1;

    df.single_act_visit<double>("Extra Col", cv_v1);
    assert(std::fabs(cv_v1.get_result() - 0.037046) < 0.000001);

    cffv_v<double, std::string> cv_v2;

    df.single_act_visit<double>("IBM_Close", cv_v2);
    assert(std::fabs(cv_v2.get_result() - 0.294703) < 0.000001);
}

// ----------------------------------------------------------------------------

static void test_gen_join()  {

    std::cout << "\nTesting gen_join( ) ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466 };
    std::vector<double>         d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    std::vector<double>         d2 =
        { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 1.89 };
    std::vector<double>         d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
    std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("col_1", d1),
                 std::make_pair("col_2", d2),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    auto    vw =
        df.get_view<double, int>( { "col_1", "col_2", "col_3", "col_4" });

    std::vector<unsigned long>  idx2 =
        { 123452, 123453, 123455, 123458, 123466, 223450, 223451,
          223454, 223456, 223457, 223459, 223460, 223461, 223462 };
    std::vector<double>         d12 =
        { 11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114 };
    std::vector<double>         d22 =
        { 8, 19, 110, 111, 9, 113, 114, 99, 122, 123, 130, 131, 20, 11.89 };
    std::vector<double>         d32 =
        { 115, 116, 115, 118, 119, 116, 121, 10.34, 11.56, 10.34, 12.3, 10.34,
          119.0 };
    std::vector<int>            i12 = { 122, 123, 124, 125, 199 };
    MyDataFrame                 df2;

    df2.load_data(std::move(idx2),
                  std::make_pair("xcol_1", d12),
                  std::make_pair("col_2", d22),
                  std::make_pair("xcol_3", d32),
                  std::make_pair("col_4", i12));

    auto    vw2 =
        df2.get_view<double, int>( { "xcol_1", "col_2", "xcol_3", "col_4" });

    auto    predicate =
        [](const unsigned long &, const unsigned long &,
           const double &lhs_val, const double &rhs_val) -> gen_join_type  {
            if (lhs_val == rhs_val)
                return (gen_join_type::include_both);
            return (gen_join_type::no_match);
        };

    // df.write<std::ostream, double, int>(
    //      std::cout, io_format::pretty_prt, { .precision = 2 });
    // std::cout << "\n\n\n";

    // df2.write<std::ostream, double, int>(
    //      std::cout, io_format::pretty_prt, { .precision = 2 });
    // std::cout << "\n\n\n";

    auto    inner_result =
        df.gen_join<decltype(df2), double, double, double, int>
            (df2, "col_2", "col_2", predicate);
    auto    inner_result_vw =
        vw.gen_join<decltype(df2), double, double, double, int>
            (df2, "col_2", "col_2", predicate);

    // inner_result.write<std::ostream, double, int, unsigned long>(
    //      std::cout, io_format::pretty_prt, { .precision = 2 });
    // std::cout << "\n\n\n";

    assert(inner_result.get_index().size() == 1);
    assert(inner_result.get_column<double>("xcol_1")[0] == 11.0);
    assert(inner_result.get_column<double>("xcol_3")[0] == 115.0);
    assert(inner_result.get_column<int>("lhs.col_4")[0] == 22);
    assert(inner_result.get_column<unsigned long>("rhs.INDEX")[0] == 123452);
    assert(inner_result.get_column<unsigned long>("lhs.INDEX")[0] == 123450);

    assert(inner_result_vw.get_index().size() == 1);
    assert(inner_result_vw.get_column<double>("col_1")[0] == 1.0);
    assert(inner_result_vw.get_column<int>("lhs.col_4")[0] == 22);
    assert(inner_result_vw.get_column<unsigned long>("rhs.INDEX")[0] == 123452);

    auto    predicate2 =
        [](const unsigned long &, const unsigned long &,
           const double &lhs_val, const double &rhs_val) -> gen_join_type  {
            if (lhs_val == rhs_val)
                return (gen_join_type::include_both);
            return (gen_join_type::include_right);
        };

    auto    result_vw2 =
        vw.gen_join<decltype(df2), double, double, double, int>
            (df2, "col_2", "col_2", predicate2);

    // result_vw2.write<std::ostream, double, int, unsigned long>(
    //      std::cout, io_format::pretty_prt, { .precision = 2 });
    // std::cout << "\n\n\n";

    assert(result_vw2.get_index().size() == 14);
    assert(result_vw2.get_column<double>("xcol_1")[0] == 11.0);
    assert(result_vw2.get_column<double>("xcol_1")[7] == 18.0);
    assert(result_vw2.get_column<double>("xcol_1")[13] == 114.0);
    assert(result_vw2.get_column<double>("xcol_3")[0] == 115.0);
    assert(result_vw2.get_column<double>("xcol_3")[10] == 12.3);
    assert(result_vw2.get_column<int>("lhs.col_4")[0] == 22);
    assert(result_vw2.get_column<int>("lhs.col_4")[6] == 0);
    assert(result_vw2.get_column<int>("lhs.col_4")[12] == 0);
    assert(result_vw2.get_column<int>("rhs.col_4")[0] == 122);
    assert(result_vw2.get_column<int>("rhs.col_4")[6] == 0);
    assert(result_vw2.get_column<int>("rhs.col_4")[12] == 0);
    assert(result_vw2.get_column<unsigned long>("rhs.INDEX")[0] == 123452);
    assert(result_vw2.get_column<unsigned long>("lhs.INDEX")[0] == 123450);
    assert(result_vw2.get_column<unsigned long>("lhs.INDEX")[8] == 0 );

    auto    predicate3 =
        [](const unsigned long &, const unsigned long &,
           const int &col_4, const double &xcol_1) -> gen_join_type  {
            if ((col_4 < 23 && col_4 != 0) || xcol_1 > 112.0)
                return (gen_join_type::include_both);
            return (gen_join_type::no_match);
        };

    auto    result_vw3 =
        vw.gen_join<MyDataFrame, int, double, double, int>
            (df2, "col_4", "xcol_1", predicate3);

    // result_vw3.write<std::ostream, double, int, unsigned long>(
    //      std::cout, io_format::pretty_prt, { .precision = 2 });
    // std::cout << "\n\n\n";

    assert(result_vw3.get_index().size() == 3);
    assert(result_vw3.get_column<double>("xcol_1")[0] == 11.0);
    assert(result_vw3.get_column<double>("xcol_1")[1] == 113.0);
    assert(result_vw3.get_column<double>("xcol_1")[2] == 114.0);
    assert(result_vw3.get_column<unsigned long>("lhs.INDEX")[0] == 123450);
    assert(result_vw3.get_column<unsigned long>("lhs.INDEX")[1] == 123462);
    assert(result_vw3.get_column<unsigned long>("lhs.INDEX")[2] == 123466);
    assert(result_vw3.get_column<unsigned long>("rhs.INDEX")[0] == 123452);
    assert(result_vw3.get_column<unsigned long>("rhs.INDEX")[1] == 223461);
    assert(result_vw3.get_column<unsigned long>("rhs.INDEX")[2] == 223462);
    assert(result_vw3.get_column<int>("lhs.col_4")[0] == 22);
    assert(result_vw3.get_column<int>("lhs.col_4")[1] == 0);
    assert(result_vw3.get_column<int>("lhs.col_4")[2] == 0);
    assert(result_vw3.get_column<int>("rhs.col_4")[0] == 122);
    assert(result_vw3.get_column<int>("rhs.col_4")[1] == 0);
    assert(result_vw3.get_column<int>("rhs.col_4")[2] == 0);
    assert(result_vw3.get_column<double>("lhs.col_2")[0] == 8.0);
    assert(result_vw3.get_column<double>("lhs.col_2")[1] == 32.0);
    assert(result_vw3.get_column<double>("lhs.col_2")[2] == 1.89);
    assert(result_vw3.get_column<double>("rhs.col_2")[0] == 8.0);
    assert(result_vw3.get_column<double>("rhs.col_2")[1] == 20.0);
    assert(result_vw3.get_column<double>("rhs.col_2")[2] == 11.89);
    assert(result_vw3.get_column<double>("col_3")[0] == 15.0);
    assert(result_vw3.get_column<double>("col_3")[1] == 19.0);
    assert(std::isnan(result_vw3.get_column<double>("col_3")[2]));
    assert(result_vw3.get_column<double>("xcol_3")[0] == 115.0);
    assert(result_vw3.get_column<double>("xcol_3")[1] == 119.0);
    assert(std::isnan(result_vw3.get_column<double>("xcol_3")[2]));
}

// -----------------------------------------------------------------------------

int main(int, char *[])  {

    MyDataFrame::set_optimum_thread_level();

    test_permutation_vec();
    test_get_data_every_n();
    test_get_n_largest_data();
    test_get_n_smallest_data();
    test_is_nan_mask();
    test_is_infinity_mask();
    test_is_default_mask();
    test_SkewVisitor();
    test_KurtosisVisitor();
    test_ConfIntervalVisitor();
    test_CoeffVariationVisitor();
    test_gen_join();

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
