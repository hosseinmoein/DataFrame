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

static void test_ChiSquaredTestVisitor()  {

    std::cout << "\nTesting ChiSquaredTestVisitor{ } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454 };
    std::vector<double>         ob1 = { 1, 2, 3, 4, 5 };
    std::vector<double>         ex1 = { 8, 9, 10, 11, 12 };
    std::vector<double>         ob2 = { 5, 18, 42, 27, 8 };
    std::vector<double>         ex2 = { 8, 20, 36, 24, 12 };
    std::vector<double>         ob3 = { 2, 5, 6, 8, 4 };
    std::vector<double>         ex3 = { 5, 5, 5, 5, 5 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("observation 1", ob1),
                 std::make_pair("expected 1", ex1),
                 std::make_pair("observation 2", ob2),
                 std::make_pair("expected 2", ex2),
                 std::make_pair("observation 3", ob3),
                 std::make_pair("expected 3", ex3));

    ChiSquaredTestVisitor<double>   chi;

    df.single_act_visit<double, double>("observation 1", "expected 1", chi);
    assert(std::fabs(chi.get_result() - 25.0073) < 0.0001);
    assert(chi.get_p_value(4) < 0.0000000001);

    df.single_act_visit<double, double>("observation 2", "expected 2", chi);
    assert(std::fabs(chi.get_result() - 4.0333) < 0.0001);
    assert(std::fabs(chi.get_p_value(4) - 0.4953) < 0.0001);

    df.single_act_visit<double, double>("observation 3", "expected 3", chi);
    assert(std::fabs(chi.get_result() - 4.0) < 0.0001);
    assert(std::fabs(chi.get_p_value(4) - 0.5) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_get_matrix()  {

    std::cout << "\nTesting get_matrix( ) ..." << std::endl;

    ULDataFrame                 df;
    std::vector<unsigned long>  idxvec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<double>         dblvec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<double>         dblvec2 =
        { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    std::vector<double>         dblvec3 =
        { 100, 200, 300, 400, 500, 600, 700, 800 };
    std::vector<double>         dblempty { };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<std::string>    strvec =
        { "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("int_col", intvec),
                 std::make_pair("str_col", strvec));
    df.load_column("Empty Col", std::move(dblempty),
                   nan_policy::dont_pad_with_nans);
    df.load_column("dbl_col_3", std::move(dblvec3),
                   nan_policy::dont_pad_with_nans);

    const auto  matrix = df.get_matrix();

    // for (long r = 0; r < matrix.rows(); ++r)  {
    //     for (long c = 0; c < matrix.cols(); ++c)  {
    //         std::cout << matrix(r, c) << ", ";
    //     }
    //     std::cout << '\n';
    // }
    // std::cout << "\n\n";

    assert(matrix.cols() == 4);
    assert(matrix.rows() == 10);

    // dbl_col
    //
    assert(matrix(0, 0) == 1.0);
    assert(matrix(1, 0) == 2.0);
    assert(matrix(2, 0) == 3.0);
    assert(matrix(3, 0) == 4.0);
    assert(matrix(8, 0) == 9.0);
    assert(matrix(9, 0) == 10.0);

    // dbl_col_2
    //
    assert(matrix(0, 1) == 10.0);
    assert(matrix(1, 1) == 20.0);
    assert(matrix(2, 1) == 30.0);
    assert(matrix(3, 1) == 40.0);
    assert(matrix(8, 1) == 90.0);
    assert(matrix(9, 1) == 100.0);

    // Empty Col
    //
    assert(std::isnan(matrix(0, 2)));
    assert(std::isnan(matrix(1, 2)));
    assert(std::isnan(matrix(2, 2)));
    assert(std::isnan(matrix(3, 2)));
    assert(std::isnan(matrix(8, 2)));
    assert(std::isnan(matrix(9, 2)));

    // dbl_col_3
    //
    assert(matrix(0, 3) == 100.0);
    assert(matrix(1, 3) == 200.0);
    assert(matrix(2, 3) == 300.0);
    assert(matrix(3, 3) == 400.0);
    assert(std::isnan(matrix(8, 3)));
    assert(std::isnan(matrix(9, 3)));

    const auto  view = df.get_view<double, int, std::string>(
        { "dbl_col", "dbl_col_2", "int_col", "str_col",
          "Empty Col", "dbl_col_3" });
    const auto  matrix2 = view.get_matrix();


    assert(matrix2.cols() == 4);
    assert(matrix2.rows() == 10);

    // dbl_col
    //
    assert(matrix2(0, 0) == 1.0);
    assert(matrix2(1, 0) == 2.0);
    assert(matrix2(2, 0) == 3.0);
    assert(matrix2(3, 0) == 4.0);
    assert(matrix2(8, 0) == 9.0);
    assert(matrix2(9, 0) == 10.0);

    // dbl_col_2
    //
    assert(matrix2(0, 1) == 10.0);
    assert(matrix2(1, 1) == 20.0);
    assert(matrix2(2, 1) == 30.0);
    assert(matrix2(3, 1) == 40.0);
    assert(matrix2(8, 1) == 90.0);
    assert(matrix2(9, 1) == 100.0);

    // Empty Col
    //
    assert(std::isnan(matrix2(0, 2)));
    assert(std::isnan(matrix2(1, 2)));
    assert(std::isnan(matrix2(2, 2)));
    assert(std::isnan(matrix2(3, 2)));
    assert(std::isnan(matrix2(8, 2)));
    assert(std::isnan(matrix2(9, 2)));

    // dbl_col_3
    //
    assert(matrix2(0, 3) == 100.0);
    assert(matrix2(1, 3) == 200.0);
    assert(matrix2(2, 3) == 300.0);
    assert(matrix2(3, 3) == 400.0);
    assert(std::isnan(matrix2(8, 3)));
    assert(std::isnan(matrix2(9, 3)));
}

// ----------------------------------------------------------------------------

static void test_get_matrix_2()  {

    std::cout << "\nTesting get_matrix_2( ) ..." << std::endl;

    ULDataFrame                 df;
    std::vector<unsigned long>  idxvec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<double>         dblvec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<double>         dblvec2 =
        { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    std::vector<double>         dblvec3 =
        { 100, 200, 300, 400, 500, 600, 700, 800 };
    std::vector<double>         dblempty { };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<std::string>    strvec =
        { "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("int_col", intvec),
                 std::make_pair("str_col", strvec));
    df.load_column("Empty Col", std::move(dblempty),
                   nan_policy::dont_pad_with_nans);
    df.load_column("dbl_col_3", std::move(dblvec3),
                   nan_policy::dont_pad_with_nans);

    const auto  matrix =
        df.get_matrix({ "Empty Col", "dbl_col_3", "dbl_col_2"  });

    // for (long r = 0; r < matrix.rows(); ++r)  {
    //     for (long c = 0; c < matrix.cols(); ++c)  {
    //         std::cout << matrix(r, c) << ", ";
    //     }
    //     std::cout << '\n';
    // }
    // std::cout << "\n\n";

    assert(matrix.cols() == 3);
    assert(matrix.rows() == 10);

    // Empty Col
    //
    assert(std::isnan(matrix(0, 0)));
    assert(std::isnan(matrix(1, 0)));
    assert(std::isnan(matrix(2, 0)));
    assert(std::isnan(matrix(3, 0)));
    assert(std::isnan(matrix(8, 0)));
    assert(std::isnan(matrix(9, 0)));

    // dbl_col_3
    //
    assert(matrix(0, 1) == 100.0);
    assert(matrix(1, 1) == 200.0);
    assert(matrix(2, 1) == 300.0);
    assert(matrix(3, 1) == 400.0);
    assert(std::isnan(matrix(8, 1)));
    assert(std::isnan(matrix(9, 1)));

    // dbl_col_2
    //
    assert(matrix(0, 2) == 10.0);
    assert(matrix(1, 2) == 20.0);
    assert(matrix(2, 2) == 30.0);
    assert(matrix(3, 2) == 40.0);
    assert(matrix(8, 2) == 90.0);
    assert(matrix(9, 2) == 100.0);

    const auto  view = df.get_view<double, int, std::string>(
        { "dbl_col", "dbl_col_2", "int_col", "str_col",
          "Empty Col", "dbl_col_3" });
    const auto  matrix2 =
        view.get_matrix({ "Empty Col", "dbl_col_3", "dbl_col_2"  });


    assert(matrix2.cols() == 3);
    assert(matrix2.rows() == 10);

    // Empty Col
    //
    assert(std::isnan(matrix2(0, 0)));
    assert(std::isnan(matrix2(1, 0)));
    assert(std::isnan(matrix2(2, 0)));
    assert(std::isnan(matrix2(3, 0)));
    assert(std::isnan(matrix2(8, 0)));
    assert(std::isnan(matrix2(9, 0)));

    // dbl_col_3
    //
    assert(matrix2(0, 1) == 100.0);
    assert(matrix2(1, 1) == 200.0);
    assert(matrix2(2, 1) == 300.0);
    assert(matrix2(3, 1) == 400.0);
    assert(std::isnan(matrix2(8, 1)));
    assert(std::isnan(matrix2(9, 1)));

    // dbl_col_2
    //
    assert(matrix2(0, 2) == 10.0);
    assert(matrix2(1, 2) == 20.0);
    assert(matrix2(2, 2) == 30.0);
    assert(matrix2(3, 2) == 40.0);
    assert(matrix2(8, 2) == 90.0);
    assert(matrix2(9, 2) == 100.0);
}

// ----------------------------------------------------------------------------

static void test_ARIMAVisitor()  {

    std::cout << "\nTesting ARIMAVisitor{ } ..." << std::endl;

    std::vector<unsigned long>  idxvec = {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    };
    std::vector<double>         col1 = {
        266.0, 145.9, 183.1, 119.3, 180.3, 168.5, 231.8, 224.5, 192.8, 122.9,
        336.5, 185.9, 194.3
    };
    std::vector<double>         oscil = {
        1.5, 1.8, 1.62, 1.78, 1.5, 1.68, 1.6, 1.8, 1.71, 1.9, 1.78, 1.84, 1.69
    };
    std::vector<double>         constant = {
        10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56,
        10.56, 10.56, 10.56
    };
    std::vector<double>         increasing = {
        10.56, 10.68, 10.78, 10.90, 11.01, 11.45, 11.99, 12.01, 12.21, 12.35,
        12.67, 13.89, 13.01
    };
    std::vector<double>         decreasing = {
        10.56, 10.30, 10.12, 10.01, 9.80, 9.74, 9.41, 9.03, 9.0, 8.20,
        8.01, 7.9, 7.55
    };
    MyDataFrame                 df;

    df.load_data(std::move(idxvec),
                 std::make_pair("col1", col1),
                 std::make_pair("oscil", oscil),
                 std::make_pair("constant", constant),
                 std::make_pair("increasing", increasing),
                 std::make_pair("decreasing", decreasing));

    ARIMAVisitor<double>   ari;

    df.single_act_visit<double>("col1", ari);

    const auto  result1 = ari.get_result();

    assert(result1.size() == 3);
    assert(std::fabs(result1[0] - 247.175) < 0.001);
    assert(std::fabs(result1[1] - 197.294) < 0.001);
    assert(std::fabs(result1[2] - 220.021) < 0.001);

    df.single_act_visit<double>("oscil", ari);

    const auto  result2 = ari.get_result();

    assert(result2.size() == 3);
    assert(std::fabs(result2[0] - 1.77088) < 0.00001);
    assert(std::fabs(result2[1] - 1.67015) < 0.00001);
    assert(std::fabs(result2[2] - 1.74417) < 0.00001);

    try  {
        df.single_act_visit<double>("constant", ari);
    }
    catch (const NotFeasible &ex)  {
        std::cout << ex.what() << std::endl;
    }

    df.single_act_visit<double>("increasing", ari);

    const auto  result3 = ari.get_result();

    assert(result3.size() == 3);
    assert(std::fabs(result3[0] - 14.3335) < 0.0001);
    assert(std::fabs(result3[1] - 13.09) < 0.0001);
    assert(std::fabs(result3[2] - 14.6469) < 0.0001);

    df.single_act_visit<double>("decreasing", ari);

    const auto  result4 = ari.get_result();

    assert(result4.size() == 3);
    assert(std::fabs(result4[0] - 7.42058) < 0.00001);
    assert(std::fabs(result4[1] - 7.21897) < 0.00001);
    assert(std::fabs(result4[2] - 7.11158) < 0.00001);

    // Now some real data
    //
    StrDataFrame    df2;

    try  {
        df2.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    ARIMAVisitor<double, std::string>   ari2 { 4 };

    df2.single_act_visit<double>("IBM_Close", ari2);

    const auto  result5 = ari2.get_result();

    assert(result5.size() == 4);
    assert(std::fabs(result5[0] - 111.63) < 0.001);
    assert(std::fabs(result5[1] - 111.658) < 0.001);
    assert(std::fabs(result5[2] - 111.657) < 0.001);
    assert(std::fabs(result5[3] - 111.658) < 0.001);
}

// ----------------------------------------------------------------------------

static void test_HWESForecastVisitor()  {

    std::cout << "\nTesting HWESForecastVisitor{ } ..." << std::endl;

    std::vector<unsigned long>  idxvec = {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    };
    std::vector<double>         col1 = {
        266.0, 145.9, 183.1, 119.3, 180.3, 168.5, 231.8, 224.5, 192.8, 122.9,
        336.5, 185.9, 194.3
    };
    std::vector<double>         oscil = {
        1.5, 1.8, 1.62, 1.78, 1.5, 1.68, 1.6, 1.8, 1.71, 1.9, 1.78, 1.84, 1.69
    };
    std::vector<double>         constant = {
        10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56,
        10.56, 10.56, 10.56
    };
    std::vector<double>         increasing = {
        10.56, 10.68, 10.78, 10.90, 11.01, 11.45, 11.99, 12.01, 12.21, 12.35,
        12.67, 13.89, 13.01
    };
    std::vector<double>         decreasing = {
        10.56, 10.30, 10.12, 10.01, 9.80, 9.74, 9.41, 9.03, 9.0, 8.20,
        8.01, 7.9, 7.55
    };
    MyDataFrame                 df;

    df.load_data(std::move(idxvec),
                 std::make_pair("col1", col1),
                 std::make_pair("oscil", oscil),
                 std::make_pair("constant", constant),
                 std::make_pair("increasing", increasing),
                 std::make_pair("decreasing", decreasing));

    HWESForecastVisitor<double>   hwes;

    df.single_act_visit<double>("col1", hwes);

    const auto  result1 = hwes.get_result();

    assert(result1.size() == 3);
    assert(std::fabs(result1[0] - 208.351) < 0.001);
    assert(std::fabs(result1[1] - 208.69) < 0.001);
    assert(std::fabs(result1[2] - 209.03) < 0.001);

    HWESForecastVisitor<double>   hwes2 { 3, 2 };

    df.single_act_visit<double>("oscil", hwes2);

    const auto  result2 = hwes2.get_result();

    assert(result2.size() == 3);
    assert(std::fabs(result2[0] - 1.73499) < 0.00001);
    assert(std::fabs(result2[1] - 1.9216) < 0.00001);
    assert(std::fabs(result2[2] - 1.76383) < 0.00001);

    df.single_act_visit<double>("constant", hwes);

    const auto  result3 = hwes.get_result();

    assert(result3.size() == 3);
    assert(std::fabs(result3[0] - 10.56) < 0.00001);
    assert(std::fabs(result3[1] - 10.56) < 0.00001);
    assert(std::fabs(result3[2] - 10.56) < 0.00001);

    df.single_act_visit<double>("increasing", hwes);

    const auto  result4 = hwes.get_result();

    assert(result4.size() == 3);
    assert(std::fabs(result4[0] - 13.4675) < 0.0001);
    assert(std::fabs(result4[1] - 13.6977) < 0.0001);
    assert(std::fabs(result4[2] - 13.928) < 0.0001);

    df.single_act_visit<double>("decreasing", hwes);

    const auto  result5 = hwes.get_result();

    assert(result5.size() == 3);
    assert(std::fabs(result5[0] - 7.41545) < 0.00001);
    assert(std::fabs(result5[1] - 7.16314) < 0.00001);
    assert(std::fabs(result5[2] - 6.91084) < 0.00001);

    // Now some real data
    //
    StrDataFrame    df2;

    try  {
        df2.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    HWESForecastVisitor<double, std::string>    hwes3 { 4 };

    df2.single_act_visit<double>("IBM_Close", hwes3);

    const auto  result6 = hwes3.get_result();

    assert(result6.size() == 4);
    assert(std::fabs(result6[0] - 109.264) < 0.001);
    assert(std::fabs(result6[1] - 108.313) < 0.001);
    assert(std::fabs(result6[2] - 107.361) < 0.001);
    assert(std::fabs(result6[3] - 106.41) < 0.001);
}

// ----------------------------------------------------------------------------

static void test_LSTMForecastVisitor()  {

    std::cout << "\nTesting LSTMForecastVisitor{ } ..." << std::endl;

    std::vector<unsigned long>  idxvec = {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    };
    std::vector<double>         col1 = {
        266.0, 145.9, 183.1, 119.3, 180.3, 168.5, 231.8, 224.5, 192.8, 122.9,
        336.5, 185.9, 194.3
    };
    std::vector<double>         oscil = {
        1.5, 1.8, 1.62, 1.78, 1.5, 1.68, 1.6, 1.8, 1.71, 1.9, 1.78, 1.84, 1.69
    };
    std::vector<double>         constant = {
        10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56, 10.56,
        10.56, 10.56, 10.56
    };
    std::vector<double>         increasing = {
        10.56, 10.68, 10.78, 10.90, 11.01, 11.45, 11.99, 12.01, 12.21, 12.35,
        12.67, 13.89, 14.01
    };
    std::vector<double>         decreasing = {
        10.56, 10.30, 10.12, 10.01, 9.80, 9.74, 9.41, 9.03, 9.0, 8.20,
        8.01, 7.9, 7.55
    };
    MyDataFrame                 df;

    df.load_data(std::move(idxvec),
                 std::make_pair("col1", col1),
                 std::make_pair("oscil", oscil),
                 std::make_pair("constant", constant),
                 std::make_pair("increasing", increasing),
                 std::make_pair("decreasing", decreasing));

    lstm_v<double>  lstm { 1, 40, 4, 1, 100, 0.001, 3, 123 };

    df.single_act_visit<double>("col1", lstm);

    const auto  result1 = lstm.get_result();

    assert(result1.size() == 3);
    assert(std::fabs(result1[0] - 177.075) < 0.001);
    assert(std::fabs(result1[1] - 181.576) < 0.001);
    assert(std::fabs(result1[2] - 185.103) < 0.001);

    lstm_v<double>  lstm2 { 1, 40, 4, 1, 1000, 0.001, 3, 123 };

    df.single_act_visit<double>("oscil", lstm2);

    const auto  result2 = lstm2.get_result();

    assert(result2.size() == 3);
    assert(std::fabs(result2[0] - 1.7698) < 0.00001);
    assert(std::fabs(result2[1] - 1.64903) < 0.00001);
    assert(std::fabs(result2[2] - 1.74927) < 0.00001);

    df.single_act_visit<double>("constant", lstm);

    const auto  result3 = lstm.get_result();

    assert(result3.size() == 3);
    assert(std::fabs(result3[0] - 10.56) < 0.00001);
    assert(std::fabs(result3[1] - 10.56) < 0.00001);
    assert(std::fabs(result3[2] - 10.56) < 0.00001);

    lstm_v<double>  lstm3 { 1, 100, 4, 1, 100, 0.001, 3, 123 };

    df.single_act_visit<double>("increasing", lstm3);

    const auto  result4 = lstm3.get_result();

    // It doesn't see the pattern here
    //
    assert(result4.size() == 3);
    assert(std::fabs(result4[0] - 13.4408) < 0.0001);
    assert(std::fabs(result4[1] - 13.4358) < 0.0001);
    assert(std::fabs(result4[2] - 13.4193) < 0.0001);

    df.single_act_visit<double>("decreasing", lstm3);

    const auto  result5 = lstm3.get_result();

    // Strangely, it sees the pattern here
    //
    assert(result5.size() == 3);
    assert(std::fabs(result5[0] - 7.23448) < 0.00001);
    assert(std::fabs(result5[1] - 7.14929) < 0.00001);
    assert(std::fabs(result5[2] - 7.10167) < 0.00001);

    // Now some real data
    //
    StrDataFrame    df2;

    try  {
        df2.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    lstm_v<double>  lstm4 { 1, 50, 4, 1, 100, 0.001, 4, 123 };

    df2.single_act_visit<double>("IBM_Close", lstm4);

    const auto  result6 = lstm4.get_result();

    assert(result6.size() == 4);
    assert(std::fabs(result6[0] - 175.419) < 0.001);
    assert(std::fabs(result6[1] - 100.078) < 0.001);
    assert(std::fabs(result6[2] - 186.713) < 0.001);
    assert(std::fabs(result6[3] - 99.2631) < 0.0001);
}

// ----------------------------------------------------------------------------

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
    test_ChiSquaredTestVisitor();
    test_get_matrix();
    test_get_matrix_2();
    test_ARIMAVisitor();
    test_HWESForecastVisitor();
    test_LSTMForecastVisitor();

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
