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

#include <array>
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
using StrDataFrame = StdDataFrame<std::string>;
using DTDataFrame = StdDataFrame256<DateTime>;
using StrDataFrame2 = StdDataFrame256<std::string>;

template<typename T>
using StlVecType = typename ULDataFrame::template StlVecType<T>;

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

    ULDataFrame                df;
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

    // Now multidimensional data
    //
    constexpr std::size_t   dim { 3 };

    using ary_col_t = std::array<double, dim>;
    using vec_col_t = std::vector<double>;

    std::vector<ary_col_t>  ary_md_x  {
        { 22.1, 1013.2, 55.4 }, { 21.8, 1012.8, 57.1 }, { 23.4, 1014.5, 53.2 },
        { 22.7, 1013.9, 56.8 }, { 21.5, 1012.1, 58.3 }, { 23.1, 1014.1, 54.6 },
        { 22.9, 1013.7, 55.9 }, { 21.2, 1011.8, 59.1 }, { 23.8, 1015.0, 52.7 },
        { 22.5, 1013.4, 56.2 },
    };
    std::vector<vec_col_t>  vec_md_x  {
        { 22.1, 1013.2, 55.4 }, { 21.8, 1012.8, 57.1 }, { 23.4, 1014.5, 53.2 },
        { 22.7, 1013.9, 56.8 }, { 21.5, 1012.1, 58.3 }, { 23.1, 1014.1, 54.6 },
        { 22.9, 1013.7, 55.9 }, { 21.2, 1011.8, 59.1 }, { 23.8, 1015.0, 52.7 },
        { 22.5, 1013.4, 56.2 },
    };

    df.load_column<ary_col_t>("ARY MD COL", std::move(ary_md_x),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC MD COL", std::move(vec_md_x),
                              nan_policy::dont_pad_with_nans);

    ConfIntervalVisitor<ary_col_t, std::string> ary_ci_v;
    ConfIntervalVisitor<vec_col_t, std::string> vec_ci_v;

    df.single_act_visit<ary_col_t>("ARY MD COL", ary_ci_v);
    df.single_act_visit<vec_col_t>("VEC MD COL", vec_ci_v);

    assert(ary_ci_v.get_error_margin().size() == dim);
    assert(std::fabs(ary_ci_v.get_error_margin()[0] - 0.522667) < 0.000001);
    assert(std::fabs(ary_ci_v.get_error_margin()[2] - 1.27024) < 0.00001);
    assert(vec_ci_v.get_error_margin().size() == dim);
    assert(std::fabs(vec_ci_v.get_error_margin()[0] - 0.522667) < 0.000001);
    assert(std::fabs(vec_ci_v.get_error_margin()[2] - 1.27024) < 0.00001);

    assert(ary_ci_v.get_result().first.size() == dim);
    assert(ary_ci_v.get_result().second.size() == dim);
    assert(std::fabs(ary_ci_v.get_result().first[0] - 21.9773) < 0.0001);
    assert(std::fabs(ary_ci_v.get_result().first[2] - 54.6598) < 0.0001);
    assert(std::fabs(ary_ci_v.get_result().second[0] - 23.0227) < 0.0001);
    assert(std::fabs(ary_ci_v.get_result().second[2] - 57.2002) < 0.0001);
    assert(vec_ci_v.get_result().first.size() == dim);
    assert(vec_ci_v.get_result().second.size() == dim);
    assert(std::fabs(vec_ci_v.get_result().first[0] - 21.9773) < 0.0001);
    assert(std::fabs(vec_ci_v.get_result().first[2] - 54.6598) < 0.0001);
    assert(std::fabs(vec_ci_v.get_result().second[0] - 23.0227) < 0.0001);
    assert(std::fabs(vec_ci_v.get_result().second[2] - 57.2002) < 0.0001);
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

    // Now multidimensional data
    //
    constexpr std::size_t   dim { 3 };

    using ary_col_t = std::array<double, dim>;
    using vec_col_t = std::vector<double>;

    std::vector<ary_col_t>  ary_md_x  {
        { 22.1, 1013.2, 55.4 }, { 21.8, 1012.8, 57.1 }, { 23.4, 1014.5, 53.2 },
        { 22.7, 1013.9, 56.8 }, { 21.5, 1012.1, 58.3 }, { 23.1, 1014.1, 54.6 },
        { 22.9, 1013.7, 55.9 }, { 21.2, 1011.8, 59.1 }, { 23.8, 1015.0, 52.7 },
        { 22.5, 1013.4, 56.2 },
    };
    std::vector<vec_col_t>  vec_md_x  {
        { 22.1, 1013.2, 55.4 }, { 21.8, 1012.8, 57.1 }, { 23.4, 1014.5, 53.2 },
        { 22.7, 1013.9, 56.8 }, { 21.5, 1012.1, 58.3 }, { 23.1, 1014.1, 54.6 },
        { 22.9, 1013.7, 55.9 }, { 21.2, 1011.8, 59.1 }, { 23.8, 1015.0, 52.7 },
        { 22.5, 1013.4, 56.2 },
    };

    df.load_column<ary_col_t>("ARY MD COL", std::move(ary_md_x),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC MD COL", std::move(vec_md_x),
                              nan_policy::dont_pad_with_nans);

    CoeffVariationVisitor<ary_col_t, std::string>   ary_cv_v;
    CoeffVariationVisitor<vec_col_t, std::string>   vec_cv_v;

    df.single_act_visit<ary_col_t>("ARY MD COL", ary_cv_v);
    df.single_act_visit<vec_col_t>("VEC MD COL", vec_cv_v);

    assert(ary_cv_v.get_result().size() == dim);
    assert(std::fabs(ary_cv_v.get_result()[0] - 0.037479) < 0.000001);
    assert(std::fabs(ary_cv_v.get_result()[1] - 0.00099899) < 0.00000001);
    assert(std::fabs(ary_cv_v.get_result()[2] - 0.036643) < 0.000001);

    assert(vec_cv_v.get_result().size() == dim);
    assert(std::fabs(vec_cv_v.get_result()[0] - 0.037479) < 0.000001);
    assert(std::fabs(vec_cv_v.get_result()[1] - 0.00099899) < 0.00000001);
    assert(std::fabs(vec_cv_v.get_result()[2] - 0.036643) < 0.000001);
}

// ----------------------------------------------------------------------------

static void test_gen_join()  {

    std::cout << "\nTesting gen_join( ) ..." << std::endl;

    std::vector<unsigned long>  idx = {
        123450, 123451, 123452, 123453, 123454, 123455, 123456, 123457, 123458,
        123459, 123460, 123461, 123462, 123466
    };
    std::vector<double>         d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    std::vector<double>         d2 =
        { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 1.89 };
    std::vector<double>         d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
    std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
    ULDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("col_1", d1),
                 std::make_pair("col_2", d2),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    auto    vw = df.get_view<double, int>(
                     { "col_1", "col_2", "col_3", "col_4" });

    std::vector<unsigned long>  idx2 = {
        123452, 123453, 123455, 123458, 123454, 223450, 223451, 223454, 223456,
        123459, 223459, 223460, 223461, 123466
    };
    std::vector<double>         d12 =
        { 11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114 };
    std::vector<double>         d22 =
        { 8, 19, 110, 111, 9, 113, 114, 99, 122, 123, 130, 131, 20, 11.89 };
    std::vector<double>         d32 = {
        115, 116, 115, 118, 119, 116, 121, 10.34, 11.56, 10.34, 12.3, 10.34,
        119.0
    };
    std::vector<int>            i12 = { 122, 123, 124, 125, 199 };
    ULDataFrame                 df2;

    df2.load_data(std::move(idx2),
                  std::make_pair("xcol_1", d12),
                  std::make_pair("col_2", d22),
                  std::make_pair("xcol_3", d32),
                  std::make_pair("col_4", i12));

    auto    vw2 = df2.get_view<double, int>(
                      { "xcol_1", "col_2", "xcol_3", "col_4" });

    auto    predicate =
        [](const unsigned long &, const unsigned long &,
           const double &lhs_val, const double &rhs_val) -> gen_join_type  {
            if (lhs_val == rhs_val)
                return (gen_join_type::include_both);
            return (gen_join_type::no_match);
        };

    auto    inner_result =
        df.gen_join<decltype(df2),
                    double,
                    double,
                    decltype(predicate),
                    double,
                    int>(df2, "col_2", "col_2", predicate);
    auto    inner_result_vw =
        vw.gen_join<decltype(df2),
                    double,
                    double,
                    decltype(predicate),
                    double,
                    int>(df2, "col_2", "col_2", predicate);

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
        vw.gen_join<decltype(df2),
                    double,
                    double,
                    decltype(predicate2),
                    double,
                    int>(df2, "col_2", "col_2", predicate2);

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
        vw.gen_join<ULDataFrame,
                    int,
                    double,
                    decltype(predicate3),
                    double,
                    int>(df2, "col_4", "xcol_1", predicate3);

    assert(result_vw3.get_index().size() == 3);
    assert(result_vw3.get_column<double>("xcol_1")[0] == 11.0);
    assert(result_vw3.get_column<double>("xcol_1")[1] == 113.0);
    assert(result_vw3.get_column<double>("xcol_1")[2] == 114.0);
    assert(result_vw3.get_column<unsigned long>("lhs.INDEX")[0] == 123450);
    assert(result_vw3.get_column<unsigned long>("lhs.INDEX")[1] == 123462);
    assert(result_vw3.get_column<unsigned long>("lhs.INDEX")[2] == 123466);
    assert(result_vw3.get_column<unsigned long>("rhs.INDEX")[0] == 123452);
    assert(result_vw3.get_column<unsigned long>("rhs.INDEX")[1] == 223461);
    assert(result_vw3.get_column<unsigned long>("rhs.INDEX")[2] == 123466);
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

    // Now join only by index
    //
    auto    pred_by_idx =
        []
        (const unsigned long &lhs_idx,
         const unsigned long &rhs_idx) -> gen_join_type  {
            if (lhs_idx == rhs_idx)  return (gen_join_type::include_both);
            return (gen_join_type::no_match);
        };
    auto    res_by_idx =
        df.gen_join<ULDataFrame,
                    decltype(pred_by_idx),
                    double,
                    int>(df2, pred_by_idx);

    // res_by_idx.write<std::ostream, unsigned long, double, int>
    //     (std::cout, io_format::pretty_prt, { .precision = 3 });

    assert(res_by_idx.get_index().size() == 3);
    assert(res_by_idx.get_column<double>("xcol_1")[0] == 15.0);
    assert(res_by_idx.get_column<double>("xcol_1")[1] == 110.0);
    assert(res_by_idx.get_column<double>("xcol_1")[2] == 114.0);
    assert(res_by_idx.get_column<double>("xcol_3")[0] == 119.0);
    assert(res_by_idx.get_column<double>("xcol_3")[1] == 10.34);
    assert(std::isnan(res_by_idx.get_column<double>("xcol_3")[2]));
    assert(res_by_idx.get_column<unsigned long>("lhs.INDEX")[0] == 123454);
    assert(res_by_idx.get_column<unsigned long>("lhs.INDEX")[1] == 123459);
    assert(res_by_idx.get_column<unsigned long>("lhs.INDEX")[2] == 123466);
    assert(res_by_idx.get_column<unsigned long>("rhs.INDEX")[0] == 123454);
    assert(res_by_idx.get_column<unsigned long>("rhs.INDEX")[1] == 123459);
    assert(res_by_idx.get_column<unsigned long>("rhs.INDEX")[2] == 123466);
    assert(res_by_idx.get_column<int>("lhs.col_4")[0] == 99);
    assert(res_by_idx.get_column<int>("lhs.col_4")[1] == 0);
    assert(res_by_idx.get_column<int>("lhs.col_4")[2] == 0);
    assert(res_by_idx.get_column<double>("rhs.col_2")[0] == 9.0);
    assert(res_by_idx.get_column<double>("rhs.col_2")[1] == 123.0);
    assert(res_by_idx.get_column<double>("rhs.col_2")[2] == 11.89);
    assert(res_by_idx.get_column<double>("lhs.col_2")[0] == 12.0);
    assert(res_by_idx.get_column<double>("lhs.col_2")[1] == 23.0);
    assert(res_by_idx.get_column<double>("lhs.col_2")[2] == 1.89);
}

// -----------------------------------------------------------------------------

static void test_gen_join2()  {

    std::cout << "\nTesting gen_join( ) two columns ..." << std::endl;

    // Reuse the same frames as test_gen_join() so the two tests
    // are easy to compare side-by-side.
    //
    std::vector<unsigned long>  idx = {
        123450, 123451, 123452, 123453, 123454, 123455, 123456, 123457, 123458,
        123459, 123460, 123461, 123462, 123466
    };
    std::vector<double>         d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    std::vector<double>         d2 =
        { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 1.89 };
    std::vector<double>         d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
    std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
    ULDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("col_1", d1),
                 std::make_pair("col_2", d2),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    std::vector<unsigned long>  idx2 = {
        123452, 123453, 123455, 123458, 123454, 223450, 223451, 223454, 223456,
        123459, 223459, 223460, 223461, 123466
    };
    std::vector<double>         d12 =
        { 11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114 };
    std::vector<double>         d22 =
        { 8, 19, 110, 111, 9, 113, 114, 99, 122, 123, 130, 131, 20, 11.89 };
    std::vector<double>         d32 = {
        115, 116, 115, 118, 119, 116, 121, 10.34, 11.56, 10.34, 12.3, 10.34,
        119.0
    };
    std::vector<int>            i12 = { 122, 123, 124, 125, 199 };
    ULDataFrame                 df2;

    df2.load_data(std::move(idx2),
                  std::make_pair("xcol_1", d12),
                  std::make_pair("col_2", d22),
                  std::make_pair("xcol_3", d32),
                  std::make_pair("col_4", i12));

    // Predicate 1 — pure inner join.
    // Both column pairs must satisfy their condition simultaneously:
    //   col_2 (lhs) == col_2 (rhs)   AND   col_1 (lhs) < 10
    // Only position 0 qualifies (lhs col_2=8 == rhs col_2=8, lhs col_1=1<10).
    //
    auto    pred_1 =
        [](const unsigned long &, const unsigned long &,
           const double &lhs_col2, const double &rhs_col2,
           const double &lhs_col1, const double &) -> gen_join_type  {
            if (lhs_col2 == rhs_col2 && lhs_col1 < 10.0)
                return (gen_join_type::include_both);
            return (gen_join_type::no_match);
        };

    auto    result_1 {
        df.gen_join<decltype(df2),
                    double, double,  // lhs_col2, rhs_col2 types
                    double, double,  // lhs_col1, rhs_col1 types
                    decltype(pred_1),
                    double,
                    int>(df2,
                         "col_2", "col_2",   // first column pair
                         "col_1", "xcol_1",  // second column pair
                         pred_1)
    };

    // col_2 is the same name on both sides -> lhs.col_2 / rhs.col_2
    // col_1 only exists on lhs, xcol_1 only on rhs -> no prefix needed
    //
    assert(result_1.get_index().size() == 1);
    assert(result_1.get_column<unsigned long>("lhs.INDEX")[0] == 123450UL);
    assert(result_1.get_column<unsigned long>("rhs.INDEX")[0] == 123452UL);
    assert(result_1.get_column<double>("col_1")[0] == 1.0);
    assert(result_1.get_column<double>("xcol_1")[0] == 11.0);
    assert(result_1.get_column<double>("lhs.col_2")[0] == 8.0);
    assert(result_1.get_column<double>("rhs.col_2")[0] == 8.0);
    assert(result_1.get_column<double>("xcol_3")[0] == 115.0);
    assert(result_1.get_column<double>("col_3")[0] == 15.0);

    // Predicate 2 — three distinct outcomes.
    // include_both  when lhs col_2 == rhs col_2  (position 0 only)
    // include_left  when lhs col_1 < rhs xcol_1  (positions 1-13, always true
    //               because d1 tops out at 14 and d12 starts at 11 with d12[0]
    //               already > d1[0]; positions 1+ all have d1<d12)
    // include_right never fires here (lhs col_1 is always < rhs xcol_1 when
    //               the col_2 condition doesn't hold), so we get 14 rows:
    //               1 both + 13 left.
    //
    auto    pred_2 =
        [](const unsigned long &, const unsigned long &,
           const double &lhs_col2, const double &rhs_col2,
           const double &lhs_col1, const double &rhs_xcol1) -> gen_join_type  {
            if (lhs_col2 == rhs_col2)
                return (gen_join_type::include_both);
            if (lhs_col1 < rhs_xcol1)
                return (gen_join_type::include_left);
            return (gen_join_type::include_right);
        };

    auto    result_2 {
        df.gen_join<decltype(df2),
                    double, double,  // lhs_col2, rhs_col2 types
                    double, double,  // lhs_col1, rhs_xcol1 types
                    decltype(pred_2),
                    double,
                    int>(df2,
                         "col_2", "col_2",
                         "col_1", "xcol_1",
                         pred_2)
    };

    assert(result_2.get_index().size() == 14);

    // row 0: include_both (lhs col_2[0]=8 == rhs col_2[0]=8)
    //
    assert(result_2.get_column<unsigned long>("lhs.INDEX")[0] == 123450UL);
    assert(result_2.get_column<unsigned long>("rhs.INDEX")[0] == 123452UL);
    assert(result_2.get_column<double>("col_1")[0] == 1.0);
    assert(result_2.get_column<double>("xcol_1")[0] == 11.0);
    assert(result_2.get_column<double>("lhs.col_2")[0] == 8.0);
    assert(result_2.get_column<double>("rhs.col_2")[0] == 8.0);

    // row 1: include_left (lhs col_1[1]=2 < rhs xcol_1[1]=12, col_2 mismatch)
    //
    assert(result_2.get_column<unsigned long>("lhs.INDEX")[1] == 123451UL);
    assert(result_2.get_column<unsigned long>("rhs.INDEX")[1] == 0UL);
    assert(result_2.get_column<double>("col_1")[1] == 2.0);
    assert(std::isnan(result_2.get_column<double>("xcol_1")[1]));
    assert(result_2.get_column<double>("lhs.col_2")[1] == 9.0);
    assert(std::isnan(result_2.get_column<double>("rhs.col_2")[1]));

    // row 7: include_left — verify mid-sequence
    //
    assert(result_2.get_column<unsigned long>("lhs.INDEX")[7] == 123457UL);
    assert(result_2.get_column<unsigned long>("rhs.INDEX")[7] == 0UL);
    assert(result_2.get_column<double>("col_1")[7] == 8.0);
    assert(std::isnan(result_2.get_column<double>("xcol_1")[7]));
    assert(result_2.get_column<double>("lhs.col_2")[7] == 20.0);
    assert(std::isnan(result_2.get_column<double>("rhs.col_2")[7]));

    // row 13: include_left — last row
    //
    assert(result_2.get_column<unsigned long>("lhs.INDEX")[13] == 123466UL);
    assert(result_2.get_column<unsigned long>("rhs.INDEX")[13] == 0UL);
    assert(result_2.get_column<double>("col_1")[13] == 14.0);
    assert(std::isnan(result_2.get_column<double>("xcol_1")[13]));
    assert(result_2.get_column<double>("lhs.col_2")[13] == 1.89);
    assert(std::isnan(result_2.get_column<double>("rhs.col_2")[13]));

    // Columns not involved in the predicate are still carried through
    // by join_helper_common_: col_3 (lhs-only), xcol_3 (rhs-only),
    // and col_4 (present in both -> lhs.col_4 / rhs.col_4).
    //
    assert(result_2.get_column<double>("col_3")[0] == 15.0);
    assert(result_2.get_column<double>("col_3")[1] == 16.0);
    assert(std::isnan(result_2.get_column<double>("xcol_3")[1]));
    assert(result_2.get_column<int>("lhs.col_4")[0] == 22);
    assert(result_2.get_column<int>("lhs.col_4")[1] == 23);
    assert(result_2.get_column<int>("rhs.col_4")[0] == 122);
    assert(result_2.get_column<int>("rhs.col_4")[1] == 0);
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
    ULDataFrame                 df;

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

    // Now multidimensional data
    //
    constexpr std::size_t   dim { 3 };

    using ary_col_t = std::array<double, dim>;
    using vec_col_t = std::vector<double>;

    std::vector<ary_col_t>  ary_observed  {
        { 18.0, 10.0, 30.0 }, { 22.0, 20.0, 20.0 }, { 19.0, 15.0, 33.0 },
        { 21.0, 18.0, 16.0 }, { 20.0, 20.0, 41.0 },
    };
    std::vector<vec_col_t>  vec_observed  {
        { 18.0, 10.0, 30.0 }, { 22.0, 20.0, 20.0 }, { 19.0, 15.0, 33.0 },
        { 21.0, 18.0, 16.0 }, { 20.0, 20.0, 41.0 },
    };
    std::vector<ary_col_t>  ary_expected  {
        { 20.0, 16.6667, 28.0 }, { 20.0, 16.6667, 21.0 },
        { 20.0, 16.6667, 35.0 }, { 20.0, 16.6667, 14.0 },
        { 20.0, 16.6667, 42.0 },
    };
    std::vector<vec_col_t>  vec_expected  {
        { 20.0, 16.6667, 28.0 }, { 20.0, 16.6667, 21.0 },
        { 20.0, 16.6667, 35.0 }, { 20.0, 16.6667, 14.0 },
        { 20.0, 16.6667, 42.0 },
    };

    df.load_column<ary_col_t>("ARY OBSV", std::move(ary_observed),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC OBSV", std::move(vec_observed),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY EXPT", std::move(ary_expected),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC EXPT", std::move(vec_expected),
                              nan_policy::dont_pad_with_nans);

    ChiSquaredTestVisitor<ary_col_t>    ary_chi;
    ChiSquaredTestVisitor<vec_col_t>    vec_chi;

    df.single_act_visit<ary_col_t, ary_col_t>("ARY OBSV", "ARY EXPT", ary_chi);
    df.single_act_visit<vec_col_t, vec_col_t>("VEC OBSV", "VEC EXPT", vec_chi);

    assert(ary_chi.get_result().size() == dim);
    assert(ary_chi.get_p_value(4).size() == dim);
    assert(ary_chi.get_p_value({ 4, 4, 4 }).size() == dim);

    assert(vec_chi.get_result().size() == dim);
    assert(vec_chi.get_p_value(4).size() == dim);
    assert(vec_chi.get_p_value({ 4, 4, 4 }).size() == dim);

    assert(std::fabs(ary_chi.get_result()[0] - 0.5) < 0.01);
    assert(std::fabs(ary_chi.get_result()[2] - 0.614286) < 0.000001);
    assert(std::fabs(ary_chi.get_p_value(4)[0] - 0.892038) < 0.000001);
    assert(std::fabs(ary_chi.get_p_value(4)[1] - 0.461508) < 0.000001);
    assert(std::fabs(ary_chi.get_p_value(4)[2] - 0.884353) < 0.000001);
    assert(std::fabs(ary_chi.get_p_value({4,4,4})[0] - 0.892038) < 0.000001);
    assert(std::fabs(ary_chi.get_p_value({4,4,4})[1] - 0.461508) < 0.000001);
    assert(std::fabs(ary_chi.get_p_value({4,4,4})[2] - 0.884353) < 0.000001);

    assert(std::fabs(vec_chi.get_result()[0] - 0.5) < 0.01);
    assert(std::fabs(vec_chi.get_result()[2] - 0.614286) < 0.000001);
    assert(std::fabs(vec_chi.get_p_value(4)[0] - 0.892038) < 0.000001);
    assert(std::fabs(vec_chi.get_p_value(4)[1] - 0.461508) < 0.000001);
    assert(std::fabs(vec_chi.get_p_value(4)[2] - 0.884353) < 0.000001);
    assert(std::fabs(vec_chi.get_p_value({4,4,4})[0] - 0.892038) < 0.000001);
    assert(std::fabs(vec_chi.get_p_value({4,4,4})[1] - 0.461508) < 0.000001);
    assert(std::fabs(vec_chi.get_p_value({4,4,4})[2] - 0.884353) < 0.000001);
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
    ULDataFrame                 df;

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
    ULDataFrame                 df;

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

    // Now multidimensional data
    //
    constexpr std::size_t   dim { 3 };
    constexpr std::size_t   n { 24 };

    using ary_col_t = std::array<double, dim>;
    using vec_col_t = std::vector<double>;

    std::vector<vec_col_t>  vec_col1(n, vec_col_t(dim));

    for (std::size_t t { 0 }; t < n; ++t)  {
        vec_col1[t][0] = 10.0 + 2.0 * t;   // dim 0: upward
        vec_col1[t][1] = 100.0 - 2.0 * t;  // dim 1: downward
        vec_col1[t][2] = 50.0;             // dim 2: flat
    }

    // Add a repeating additive seasonal pattern of period 12 to all dims
    // so the seasonal path is exercised meaningfully.
    //
    //
    const std::vector<double>   pattern {
        3, -3, 2, -2, 1, -1, 3, -3, 2, -2, 1, -1,
    };

    for (size_t t { 0 }; t < n; ++t)
        for (size_t d { 0 }; d < dim; ++d)
            vec_col1[t][d] += pattern[t % pattern.size()];

    std::vector<ary_col_t>  ary_col1(n);

    for (size_t t { 0 }; t < n; ++t)
        for (size_t d { 0 }; d < dim; ++d)
            ary_col1[t][d] = vec_col1[t][d];

    df2.load_column<vec_col_t>("VEC OBSV", std::move(vec_col1),
                               nan_policy::dont_pad_with_nans);
    df2.load_column<ary_col_t>("ARY OBSV", std::move(ary_col1),
                               nan_policy::dont_pad_with_nans);

    // No seasons
    //
    HWESForecastVisitor<vec_col_t, std::string> vec_hwes1 { 5, 0, 0.3, 0.1 };
    HWESForecastVisitor<ary_col_t, std::string> ary_hwes1 { 5, 0, 0.3, 0.1 };

    df2.single_act_visit<vec_col_t>("VEC OBSV", vec_hwes1);
    df2.single_act_visit<ary_col_t>("ARY OBSV", ary_hwes1);

    assert(vec_hwes1.get_result().size() == 5);
    for (const auto &vec : vec_hwes1.get_result())
        assert(vec.size() == dim);
    assert(ary_hwes1.get_result().size() == 5);
    for (const auto &ary : ary_hwes1.get_result())
        assert(ary.size() == dim);

    // dim 0 is trending up — each forecast step should be increasing
    //
    for (std::size_t i { 1 }; i < vec_hwes1.get_result().size(); ++i)
        assert(vec_hwes1.get_result()[i][0] > vec_hwes1.get_result()[i-1][0]);
    // dim 1 is trending down — each step should be decreasing
    //
    for (std::size_t i { 1 }; i < ary_hwes1.get_result().size(); ++i)
        assert(ary_hwes1.get_result()[i][1] < ary_hwes1.get_result()[i-1][1]);

    // With seasons 12 periods
    //
    HWESForecastVisitor<vec_col_t, std::string> vec_hwes2 {
        6, 12, 0.3, 0.1, 0.2, decompose_type::additive
    };

    df2.single_act_visit<vec_col_t>("VEC OBSV", vec_hwes2);
    assert(vec_hwes2.get_result().size() == 6);
    assert(vec_hwes2.get_fitted().size() == n);
    assert(vec_hwes2.get_seasonal_factors().size() == pattern.size());
    for (const auto &vec : vec_hwes2.get_result())
        assert(vec.size() == dim);
    assert(std::abs(vec_hwes2.get_result()[0][0] - 45.1255) < 0.0001);
    assert(std::abs(vec_hwes2.get_result()[2][2] - 52.0) < 0.0001);
    assert(std::abs(vec_hwes2.get_result()[4][1] - 59.49) < 0.0001);
    assert(std::abs(vec_hwes2.get_result()[5][0] - 53.3878) < 0.0001);
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
    ULDataFrame                 df;

    df.load_data(std::move(idxvec),
                 std::make_pair("col1", col1),
                 std::make_pair("oscil", oscil),
                 std::make_pair("constant", constant),
                 std::make_pair("increasing", increasing),
                 std::make_pair("decreasing", decreasing));

    lstm_v<double>  lstm { 40, 4, 1, 100, 0.001, 3, 123 };

    df.single_act_visit<double>("col1", lstm);

    const auto  result1 = lstm.get_result();

    assert(result1.size() == 3);
    assert(std::fabs(result1[0] - 177.075) < 0.001);
    assert(std::fabs(result1[1] - 181.576) < 0.001);
    assert(std::fabs(result1[2] - 185.103) < 0.001);

    lstm_v<double>  lstm2 { 40, 4, 1, 1000, 0.001, 3, 123 };

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

    lstm_v<double>  lstm3 { 100, 4, 1, 100, 0.001, 3, 123 };

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

    // lstm_v<double>  lstm4 { 50, 4, 1, 100, 0.001, 4, 123 };

    // df2.single_act_visit<double>("IBM_Close", lstm4);

    // const auto  result6 = lstm4.get_result();

    // assert(result6.size() == 4);
    // assert(std::fabs(result6[0] - 118.424) < 0.001);
    // assert(std::fabs(result6[1] - 132.139) < 0.001);
    // assert(std::fabs(result6[2] - 132.311) < 0.001);
    // assert(std::fabs(result6[3] - 132.382) < 0.001);

    // Now multidimensional data
    //
    constexpr std::size_t   dim { 3 };

    using ary_col_t = std::array<double, dim>;
    using vec_col_t = std::vector<double>;

    // 3-dimensional time-series: 60 time steps, 3 features each
    // Feature 0: price-like (trending upward with noise)
    // Feature 1: volume-like (mean-reverting around 1000)
    // Feature 2: volatility-like (small positive values)
    //
    std::vector<ary_col_t>  ary_col  {
        { 100.0,  980.0, 0.012 }, { 101.2,  995.0, 0.015 },
        { 100.8, 1020.0, 0.011 }, { 102.1, 1005.0, 0.018 },
        { 103.4,  990.0, 0.014 }, { 102.9, 1010.0, 0.013 },
        { 104.2, 1030.0, 0.016 }, { 105.0,  970.0, 0.020 },
        { 104.6,  985.0, 0.017 }, { 106.1, 1015.0, 0.014 },
        { 107.3, 1025.0, 0.013 }, { 106.8,  995.0, 0.019 },
        { 108.0, 1040.0, 0.015 }, { 107.5, 1010.0, 0.012 },
        { 109.2,  980.0, 0.016 }, { 110.0, 1000.0, 0.014 },
        { 109.7, 1020.0, 0.018 }, { 111.1,  975.0, 0.021 },
        { 110.5,  990.0, 0.017 }, { 112.0, 1035.0, 0.013 },
        { 111.8, 1015.0, 0.015 }, { 113.2,  985.0, 0.019 },
        { 112.9, 1005.0, 0.016 }, { 114.0, 1025.0, 0.014 },
        { 113.5,  995.0, 0.018 }, { 115.1,  970.0, 0.022 },
        { 114.8, 1010.0, 0.015 }, { 116.0,  980.0, 0.017 },
        { 115.6, 1030.0, 0.013 }, { 117.2, 1000.0, 0.016 },
        { 116.9, 1020.0, 0.019 }, { 118.0,  990.0, 0.014 },
        { 117.5, 1040.0, 0.012 }, { 119.1,  975.0, 0.020 },
        { 118.8, 1005.0, 0.018 }, { 120.0,  995.0, 0.015 },
        { 119.7, 1025.0, 0.017 }, { 121.2,  985.0, 0.021 },
        { 120.9, 1015.0, 0.013 }, { 122.0,  970.0, 0.016 },
        { 121.6, 1000.0, 0.019 }, { 123.1,  990.0, 0.014 },
        { 122.8, 1030.0, 0.018 }, { 124.0, 1010.0, 0.015 },
        { 123.5,  980.0, 0.017 }, { 125.2, 1020.0, 0.012 },
        { 124.9,  995.0, 0.020 }, { 126.0,  975.0, 0.016 },
        { 125.6, 1005.0, 0.019 }, { 127.1, 1035.0, 0.014 },
        { 126.8,  985.0, 0.018 }, { 128.0, 1015.0, 0.015 },
        { 127.5,  990.0, 0.021 }, { 129.2, 1000.0, 0.013 },
        { 128.9, 1025.0, 0.017 }, { 130.0,  970.0, 0.016 },
        { 129.7,  980.0, 0.020 }, { 131.1, 1010.0, 0.014 },
        { 130.8, 1030.0, 0.018 }, { 132.0,  995.0, 0.015 },
    };
    std::vector<vec_col_t>  vec_col  {
        { 100.0,  980.0, 0.012 }, { 101.2,  995.0, 0.015 },
        { 100.8, 1020.0, 0.011 }, { 102.1, 1005.0, 0.018 },
        { 103.4,  990.0, 0.014 }, { 102.9, 1010.0, 0.013 },
        { 104.2, 1030.0, 0.016 }, { 105.0,  970.0, 0.020 },
        { 104.6,  985.0, 0.017 }, { 106.1, 1015.0, 0.014 },
        { 107.3, 1025.0, 0.013 }, { 106.8,  995.0, 0.019 },
        { 108.0, 1040.0, 0.015 }, { 107.5, 1010.0, 0.012 },
        { 109.2,  980.0, 0.016 }, { 110.0, 1000.0, 0.014 },
        { 109.7, 1020.0, 0.018 }, { 111.1,  975.0, 0.021 },
        { 110.5,  990.0, 0.017 }, { 112.0, 1035.0, 0.013 },
        { 111.8, 1015.0, 0.015 }, { 113.2,  985.0, 0.019 },
        { 112.9, 1005.0, 0.016 }, { 114.0, 1025.0, 0.014 },
        { 113.5,  995.0, 0.018 }, { 115.1,  970.0, 0.022 },
        { 114.8, 1010.0, 0.015 }, { 116.0,  980.0, 0.017 },
        { 115.6, 1030.0, 0.013 }, { 117.2, 1000.0, 0.016 },
        { 116.9, 1020.0, 0.019 }, { 118.0,  990.0, 0.014 },
        { 117.5, 1040.0, 0.012 }, { 119.1,  975.0, 0.020 },
        { 118.8, 1005.0, 0.018 }, { 120.0,  995.0, 0.015 },
        { 119.7, 1025.0, 0.017 }, { 121.2,  985.0, 0.021 },
        { 120.9, 1015.0, 0.013 }, { 122.0,  970.0, 0.016 },
        { 121.6, 1000.0, 0.019 }, { 123.1,  990.0, 0.014 },
        { 122.8, 1030.0, 0.018 }, { 124.0, 1010.0, 0.015 },
        { 123.5,  980.0, 0.017 }, { 125.2, 1020.0, 0.012 },
        { 124.9,  995.0, 0.020 }, { 126.0,  975.0, 0.016 },
        { 125.6, 1005.0, 0.019 }, { 127.1, 1035.0, 0.014 },
        { 126.8,  985.0, 0.018 }, { 128.0, 1015.0, 0.015 },
        { 127.5,  990.0, 0.021 }, { 129.2, 1000.0, 0.013 },
        { 128.9, 1025.0, 0.017 }, { 130.0,  970.0, 0.016 },
        { 129.7,  980.0, 0.020 }, { 131.1, 1010.0, 0.014 },
        { 130.8, 1030.0, 0.018 }, { 132.0,  995.0, 0.015 },
    };

    df2.load_column<ary_col_t>("ARY COL", std::move(ary_col),
                               nan_policy::dont_pad_with_nans);
    df2.load_column<vec_col_t>("VEC COL", std::move(vec_col),
                               nan_policy::dont_pad_with_nans);

    lstm_v<ary_col_t, std::string>  ary_lstm { 16, 10, 1, 30, 0.001, 4, 42 };
    lstm_v<vec_col_t, std::string>  vec_lstm { 16, 10, 1, 30, 0.001, 4, 42 };

    df2.single_act_visit<ary_col_t>("ARY COL", ary_lstm);
    df2.single_act_visit<vec_col_t>("VEC COL", vec_lstm);

    assert(ary_lstm.get_result().size() == 4);
    for (const auto &ary : ary_lstm.get_result())
        assert(ary.size() == dim);
    assert(vec_lstm.get_result().size() == 4);
    for (const auto &vec : vec_lstm.get_result())
        assert(vec.size() == dim);

    assert(std::abs(ary_lstm.get_result()[0][0] - 125.733) < 0.001);
    assert(std::abs(ary_lstm.get_result()[0][1] - 1001.01) < 0.01);
    assert(std::abs(ary_lstm.get_result()[2][1] - 1000.86) < 0.01);
    assert(std::abs(ary_lstm.get_result()[3][2] - 0.016641) < 0.000001);
    assert(std::abs(vec_lstm.get_result()[0][0] - 125.733) < 0.001);
    assert(std::abs(vec_lstm.get_result()[0][1] - 1001.01) < 0.01);
    assert(std::abs(vec_lstm.get_result()[2][1] - 1000.86) < 0.01);
    assert(std::abs(vec_lstm.get_result()[3][2] - 0.016641) < 0.000001);
}

// ----------------------------------------------------------------------------

static void test_kshape_groups()  {

    std::cout << "\nTesting kshape_groups( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const std::size_t   data_s = df.get_index().size();

    // Cluster 1: Sine wave pattern
    //
    for (std::size_t i = 0; i < 5; ++i)  {
        std::vector<double> series(data_s);
        const std::string   col_name { "Sine Wave " };

        for (std::size_t j = 0; j < data_s; ++j)
            series[j] =
                std::sin(2 * M_PI * j / 25.0) + (std::rand() % 100) / 500.0;

        df.load_column((col_name + std::to_string(i)).c_str(),
                       std::move(series),
                       nan_policy::dont_pad_with_nans);
    }

    // Cluster 2: Exponential growth pattern
    //
    for (std::size_t i = 0; i < 5; ++i)  {
        std::vector<double> series(data_s);
        const std::string   col_name { "Exponential Inc " };

        for (std::size_t j = 0; j < data_s; ++j)
            series[j] =
                std::exp(j / 25.0) - 1.0 + (std::rand() % 100) / 500.0;

        df.load_column((col_name + std::to_string(i)).c_str(),
                       std::move(series),
                       nan_policy::dont_pad_with_nans);
    }

    // Cluster 3: Linear increasing pattern
    //
    for (std::size_t i = 0; i < 5; ++i) {
        std::vector<double> series(data_s);
        const std::string   col_name { "Linear Inc " };

        for (std::size_t j = 0; j < data_s; ++j)
            series[j] = j / 25.0 + (std::rand() % 100) / 500.0;

        df.load_column((col_name + std::to_string(i)).c_str(),
                       std::move(series),
                       nan_policy::dont_pad_with_nans);
    }

    const auto result =
        df.kshape_groups<double>(
            { "IBM_Open", "IBM_High", "IBM_Low", "IBM_Close",
              "Sine Wave 0", "Sine Wave 1", "Sine Wave 2", "Sine Wave 3",
              "Sine Wave 4",
              "Exponential Inc 0", "Exponential Inc 1", "Exponential Inc 2",
              "Exponential Inc 3", "Exponential Inc 4",
              "Linear Inc 0", "Linear Inc 1", "Linear Inc 2", "Linear Inc 3",
              "Linear Inc 4" },
            4L,
            { .seed = 123 });

    assert(result.size() == 4);
    assert((result[0] ==
            std::vector<std::string> { "Linear Inc 0", "Linear Inc 1",
                                       "Linear Inc 2", "Linear Inc 3",
                                       "Linear Inc 4"
            }));
    assert((result[1] ==
            std::vector<std::string> { "Sine Wave 0", "Sine Wave 1",
                                       "Sine Wave 2", "Sine Wave 3",
                                       "Sine Wave 4"
            }));
    assert((result[2] ==
            std::vector<std::string> { "IBM_Open", "IBM_High", "IBM_Low",
                                       "IBM_Close"
            }));
    assert((result[3] ==
            std::vector<std::string> { "Exponential Inc 0", "Exponential Inc 1",
                                       "Exponential Inc 2", "Exponential Inc 3",
                                       "Exponential Inc 4"
            }));

    // Now multidimensional data
    //
    constexpr std::size_t   dim { 3 };

    using ary_col_t = std::array<double, dim>;
    using vec_col_t = std::vector<double>;

    // Dataset 1 — Two clearly separated clusters (sine vs. linear ramp)
    //
    // Cluster A — sinusoidal pattern across all dims
    //
    std::vector<ary_col_t>  ary_sin_col1  {
        { 0.0, 0.0, 0.0 }, { 0.71, 0.50, 0.35 }, { 1.0, 0.87, 0.64 },
        { 0.71, 0.97, 0.87 }, { 0.0, 0.87, 1.0 }, { -0.71, 0.50, 0.97 },
        { -1.0, 0.0, 0.87 }, { -0.71, -0.50, 0.64 }
    };
    std::vector<ary_col_t>  ary_sin_col2  {
        { 0.05, -0.03, 0.02 }, { 0.68, 0.52, 0.37 }, { 0.97, 0.85, 0.61 },
        { 0.74, 0.99, 0.90 }, { 0.03, 0.85, 0.98 }, { -0.69, 0.53, 0.99 },
        { -0.98, 0.02, 0.85 }, { -0.73, -0.48, 0.62 }
    };
    std::vector<ary_col_t>  ary_sin_col3  {
        { -0.02, 0.01, 0.03 }, { 0.73, 0.48, 0.33 }, { 1.02, 0.89, 0.67 },
        { 0.69, 0.95, 0.85 }, { -0.02, 0.90, 1.02 }, { -0.74, 0.48, 0.95 },
        { -1.02, -0.02, 0.89 }, { -0.69, -0.52, 0.66 }
    };

    std::vector<vec_col_t>  vec_sin_col1  {
        { 0.0, 0.0, 0.0 }, { 0.71, 0.50, 0.35 }, { 1.0, 0.87, 0.64 },
        { 0.71, 0.97, 0.87 }, { 0.0, 0.87, 1.0 }, { -0.71, 0.50, 0.97 },
        { -1.0, 0.0, 0.87 }, { -0.71, -0.50, 0.64 }
    };
    std::vector<vec_col_t>  vec_sin_col2  {
        { 0.05, -0.03, 0.02 }, { 0.68, 0.52, 0.37 }, { 0.97, 0.85, 0.61 },
        { 0.74, 0.99, 0.90 }, { 0.03, 0.85, 0.98 }, { -0.69, 0.53, 0.99 },
        { -0.98, 0.02, 0.85 }, { -0.73, -0.48, 0.62 }
    };
    std::vector<vec_col_t>  vec_sin_col3  {
        { -0.02, 0.01, 0.03 }, { 0.73, 0.48, 0.33 }, { 1.02, 0.89, 0.67 },
        { 0.69, 0.95, 0.85 }, { -0.02, 0.90, 1.02 }, { -0.74, 0.48, 0.95 },
        { -1.02, -0.02, 0.89 }, { -0.69, -0.52, 0.66 }
    };

    df.load_column<ary_col_t>("ARY sin COL 1", std::move(ary_sin_col1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY sin COL 2", std::move(ary_sin_col2),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY sin COL 3", std::move(ary_sin_col3),
                              nan_policy::dont_pad_with_nans);

    df.load_column<vec_col_t>("VEC sin COL 1", std::move(vec_sin_col1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC sin COL 2", std::move(vec_sin_col2),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC sin COL 3", std::move(vec_sin_col3),
                              nan_policy::dont_pad_with_nans);

    // Cluster B — linear ramp pattern across all dims
    //
    std::vector<ary_col_t>  ary_lin_col1  {
        { -1.0, -0.8, -0.5 }, { -0.71, -0.57, -0.36 }, { -0.43, -0.34, -0.21 },
        { -0.14, -0.11, -0.07 }, { 0.14, 0.11, 0.07 }, { 0.43, 0.34, 0.21 },
        { 0.71, 0.57, 0.36 }, { 1.0, 0.8, 0.5 }
    };
    std::vector<ary_col_t>  ary_lin_col2  {
        { -0.98, -0.82, -0.52 }, { -0.73, -0.55, -0.34 },
        { -0.41, -0.36, -0.23 }, { -0.16, -0.09, -0.05 }, { 0.12, 0.13, 0.09 },
        { 0.45, 0.32, 0.19 }, { 0.69, 0.59, 0.38 }, { 1.02, 0.78, 0.48 }
    };
    std::vector<ary_col_t>  ary_lin_col3  {
        { -1.02, -0.78, -0.48 }, { -0.69, -0.59, -0.38 },
        { -0.45, -0.32, -0.19 }, { -0.12, -0.13, -0.09 }, { 0.16, 0.09, 0.05 },
        { 0.41, 0.36, 0.23 }, { 0.73, 0.55, 0.34 }, { 0.98, 0.82, 0.52 }
    };

    std::vector<vec_col_t>  vec_lin_col1  {
        { -1.0, -0.8, -0.5 }, { -0.71, -0.57, -0.36 }, { -0.43, -0.34, -0.21 },
        { -0.14, -0.11, -0.07 }, { 0.14, 0.11, 0.07 }, { 0.43, 0.34, 0.21 },
        { 0.71, 0.57, 0.36 }, { 1.0, 0.8, 0.5 }
    };
    std::vector<vec_col_t>  vec_lin_col2  {
        { -0.98, -0.82, -0.52 }, { -0.73, -0.55, -0.34 },
        { -0.41, -0.36, -0.23 }, { -0.16, -0.09, -0.05 }, { 0.12, 0.13, 0.09 },
        { 0.45, 0.32, 0.19 }, { 0.69, 0.59, 0.38 }, { 1.02, 0.78, 0.48 }
    };
    std::vector<vec_col_t>  vec_lin_col3  {
        { -1.02, -0.78, -0.48 }, { -0.69, -0.59, -0.38 },
        { -0.45, -0.32, -0.19 }, { -0.12, -0.13, -0.09 }, { 0.16, 0.09, 0.05 },
        { 0.41, 0.36, 0.23 }, { 0.73, 0.55, 0.34 }, { 0.98, 0.82, 0.52 }
    };

    df.load_column<ary_col_t>("ARY lin COL 1", std::move(ary_lin_col1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY lin COL 2", std::move(ary_lin_col2),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY lin COL 3", std::move(ary_lin_col3),
                              nan_policy::dont_pad_with_nans);

    df.load_column<vec_col_t>("VEC lin COL 1", std::move(vec_lin_col1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC lin COL 2", std::move(vec_lin_col2),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC lin COL 3", std::move(vec_lin_col3),
                              nan_policy::dont_pad_with_nans);

    const auto  ary_res1 =
        df.kshape_groups<ary_col_t>(
            { "ARY sin COL 1", "ARY sin COL 2", "ARY sin COL 3",
              "ARY lin COL 1", "ARY lin COL 2", "ARY lin COL 3" },
            2L,
            { .seed = 123 });
    const auto  vec_res1 =
        df.kshape_groups<vec_col_t>(
            { "VEC sin COL 1", "VEC sin COL 2", "VEC sin COL 3",
              "VEC lin COL 1", "VEC lin COL 2", "VEC lin COL 3" },
            2L,
            { .seed = 123 });

    assert(ary_res1.size() == 2);
    assert((ary_res1[0] ==
            std::vector<std::string> { "ARY lin COL 1", "ARY lin COL 2",
                                       "ARY lin COL 3" }));
    assert((ary_res1[1] ==
            std::vector<std::string> { "ARY sin COL 1", "ARY sin COL 2",
                                       "ARY sin COL 3" }));

    assert(vec_res1.size() == 2);
    assert((vec_res1[0] ==
            std::vector<std::string> { "VEC lin COL 1", "VEC lin COL 2",
                                       "VEC lin COL 3" }));
    assert((vec_res1[1] ==
            std::vector<std::string> { "VEC sin COL 1", "VEC sin COL 2",
                                       "VEC sin COL 3" }));

    // Dataset 2 — Shifted copies (tests the lag/alignment path)
    //
    // Columns 0–2 are the same shape, each shifted by 1 timestep.
    // The algorithm should still cluster them together since SBD is
    // shift-invariant.
    //
    std::vector<ary_col_t>  ary_base  {
        { 0.0, 0.0, 0.0 }, { 0.5, 0.3, 0.1 }, { 1.0, 0.8, 0.5 },
        { 0.8, 1.0, 0.9 }, { 0.3, 0.7, 1.0 }, { -0.2, 0.2, 0.8 },
        { -0.7, -0.3, 0.4 }, { -1.0, -0.8, 0.0 }
    };
    std::vector<ary_col_t>  ary_shift_0 = ary_base;
    std::vector<ary_col_t>  ary_shift_1  {
        { 0.0, 0.0, 0.0 }, { 0.0, 0.0, 0.0 }, { 0.5, 0.3, 0.1 },
        { 1.0, 0.8, 0.5 }, { 0.8, 1.0, 0.9 }, { 0.3, 0.7, 1.0 },
        { -0.2, 0.2, 0.8 }, { -0.7, -0.3, 0.4 }
    };
    std::vector<ary_col_t>  ary_shift_2  {
        { 0.0, 0.0, 0.0 }, { 0.0, 0.0, 0.0 }, { 0.0, 0.0, 0.0 },
        { 0.5, 0.3, 0.1 }, { 1.0, 0.8, 0.5 }, { 0.8, 1.0, 0.9 },
        { 0.3, 0.7, 1.0 }, { -0.2, 0.2, 0.8 }
    };

    df.load_column<ary_col_t>("ARY BASE", std::move(ary_base),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY SHIFT 0", std::move(ary_shift_0),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY SHIFT 1", std::move(ary_shift_1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY SHIFT 2", std::move(ary_shift_2),
                              nan_policy::dont_pad_with_nans);

    // // Cluster B — flat/constant series, clearly different
    //
    std::vector<ary_col_t>  ary_flat_0  {
        { 0.5, 0.5, 0.5 }, { 0.5, 0.5, 0.5 }, { 0.5, 0.5, 0.5 },
        { 0.5, 0.5, 0.5 }, { 0.5, 0.5, 0.5 }, { 0.5, 0.5, 0.5 },
        { 0.5, 0.5, 0.5 }, { 0.5, 0.5, 0.5 }
    };
    std::vector<ary_col_t>  ary_flat_1  {
        { 0.48, 0.51, 0.50 }, { 0.50, 0.49, 0.51 }, { 0.51, 0.50, 0.49 },
        { 0.49, 0.51, 0.50 }, { 0.50, 0.50, 0.50 }, { 0.51, 0.49, 0.51 },
        { 0.50, 0.50, 0.48 }, { 0.49, 0.51, 0.50 }
    };
    std::vector<ary_col_t>  ary_flat_2  {
        { 0.51, 0.50, 0.49 }, { 0.50, 0.51, 0.50 }, { 0.49, 0.50, 0.51 },
        { 0.51, 0.49, 0.50 }, { 0.50, 0.50, 0.52 }, { 0.49, 0.51, 0.50 },
        { 0.52, 0.50, 0.49 }, { 0.50, 0.49, 0.51 }
    };

    df.load_column<ary_col_t>("ARY FLAT 0", std::move(ary_flat_0),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY FLAT 1", std::move(ary_flat_1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY FLAT 2", std::move(ary_flat_2),
                              nan_policy::dont_pad_with_nans);

    const auto  ary_res2 =
        df.kshape_groups<ary_col_t>(
            { "ARY BASE", "ARY SHIFT 0", "ARY SHIFT 1", "ARY SHIFT 2",
              "ARY FLAT 0", "ARY FLAT 1", "ARY FLAT 1" },
            2L,
            { .seed = 123 });

    assert(ary_res2.size() == 2);
    assert((ary_res2[0] ==
            std::vector<std::string> { "ARY BASE", "ARY SHIFT 0",
                                       "ARY SHIFT 1", "ARY SHIFT 2" }));
    assert((ary_res2[1] ==
            std::vector<std::string> { "ARY FLAT 0", "ARY FLAT 1",
                                       "ARY FLAT 1" }));

    // Dataset 3 — Three clusters, k=3 (stress test)
    //
    // Cluster A — decaying exponential shape
    //
    std::vector<ary_col_t>  ary_de_exp_1  {
        { 1.0, 0.8, 0.6 }, { 0.61, 0.49, 0.37 }, { 0.37, 0.30, 0.22 },
        { 0.22, 0.18, 0.14 }, { 0.14, 0.11, 0.08 }, { 0.08, 0.07, 0.05 },
        { 0.05, 0.04, 0.03 }, { 0.03, 0.02, 0.02 }
    };
    std::vector<ary_col_t>  ary_de_exp_2  {
        { 1.02, 0.78, 0.58 }, { 0.59, 0.51, 0.39 }, { 0.39, 0.28, 0.20 },
        { 0.20, 0.20, 0.16 }, { 0.16, 0.09, 0.06 }, { 0.06, 0.09, 0.07 },
        { 0.07, 0.02, 0.01 }, { 0.01, 0.04, 0.04 }
    };

    df.load_column<ary_col_t>("ARY DE EXP 1", std::move(ary_de_exp_1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY DE EXP 2", std::move(ary_de_exp_2),
                              nan_policy::dont_pad_with_nans);

    // Cluster B — step function shape
    //
    std::vector<ary_col_t>  ary_step_1  {
        { -1.0, -1.0, -0.8 }, { -1.0, -1.0, -0.8 }, { -1.0, -1.0, -0.8 },
        { -1.0, -1.0, -0.8 }, { 1.0, 1.0, 0.8 }, { 1.0, 1.0, 0.8 },
        { 1.0, 1.0, 0.8 }, { 1.0, 1.0, 0.8 }
    };
    std::vector<ary_col_t>  ary_step_2  {
        { -0.98, -1.02, -0.82 }, { -1.02, -0.98, -0.78 },
        { -0.99, -1.01, -0.81 }, { -1.01, -0.99, -0.79 }, { 0.99, 1.01, 0.82 },
        { 1.01, 0.99, 0.78 }, { 0.98, 1.02, 0.81 }, { 1.02, 0.98, 0.79 }
    };

    df.load_column<ary_col_t>("ARY STEP 1", std::move(ary_step_1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY STEP 2", std::move(ary_step_2),
                              nan_policy::dont_pad_with_nans);

    // Cluster C — V-shape (decrease then increase)
    //
    std::vector<ary_col_t>  ary_vshape_1  {
        { 1.0, 0.9, 0.7 }, { 0.5, 0.45, 0.35 }, { 0.0, 0.0, 0.0 },
        { -0.5, -0.45,-0.35 }, { -1.0, -0.9, -0.7 }, { -0.5, -0.45, -0.35 },
        { 0.0, 0.0, 0.0 }, { 0.5, 0.45, 0.35 }
    };
    std::vector<ary_col_t>  ary_vshape_2  {
        { 1.02, 0.88, 0.68 }, { 0.48, 0.47, 0.37 }, { 0.02, -0.01, 0.01 },
        { -0.52, -0.43, -0.33 }, { -0.98, -0.92, -0.72 },
        { -0.48, -0.47, -0.37 }, { 0.02, 0.01, -0.01 }, { 0.48, 0.43, 0.33 }
    };

    df.load_column<ary_col_t>("ARY VSHAPE 1", std::move(ary_vshape_1),
                              nan_policy::dont_pad_with_nans);
    df.load_column<ary_col_t>("ARY VSHAPE 2", std::move(ary_vshape_2),
                              nan_policy::dont_pad_with_nans);

    const auto  ary_res3 =
        df.kshape_groups<ary_col_t>(
            { "ARY DE EXP 1", "ARY DE EXP 2", "ARY STEP 1", "ARY STEP 2",
              "ARY VSHAPE 1", "ARY VSHAPE 2" },
            4L,
            { .seed = 123 });

    assert(ary_res3.size() == 4);
    assert((ary_res3[0] ==
            std::vector<std::string> { "ARY VSHAPE 1", "ARY VSHAPE 2" }));
    assert((ary_res3[1] == std::vector<std::string> { "ARY STEP 1" }));
    assert((ary_res3[2] ==
            std::vector<std::string> { "ARY DE EXP 1", "ARY DE EXP 2" }));
    assert((ary_res3[3] == std::vector<std::string> { "ARY STEP 2" }));
}

// ----------------------------------------------------------------------------

static void test_count()  {

    std::cout << "\nTesting count( ) ..." << std::endl;

    ULDataFrame df;

    try  {
        df.read("FORD.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  count1 =
        df.count<double>("FORD_Close",
                         [](const unsigned long &, const double &close)  {
                             return (close > 10.0);
                         });

    assert(count1 == 4900);

    const auto  count2 =
        df.count<double, double, double, long>(
            "FORD_Close", "FORD_Open", "FORD_Low", "FORD_Volume",
            [](const unsigned long &,
               const double &close, const double &open, const double &low,
               const long &volume)  {
                return (close > 10.0 && open > 10.0 && low > 10.0 &&
                        volume > 40'000'000L);
            });

    assert(count2 == 953);
}

// ----------------------------------------------------------------------------

enum class  FordDataTypes : int  {

    invalid = 0,
    grey = 1,
    blue = 2,
    red = 3,
    white = 4,
};

static void test_class_count()  {

    std::cout << "\nTesting class_count( ) ..." << std::endl;

    ULDataFrame df;

    try  {
        df.read("FORD.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto    result =
        df.class_count<FordDataTypes, double, double, double, long>(
            "FORD_Close", "FORD_Open", "FORD_Low", "FORD_Volume",
            [](const unsigned long &,
               const double &close, const double &open, const double &low,
               const long &volume) -> FordDataTypes  {

                if (close > 0 && open > 0 && low > 0 && volume > 0)  {
                    if (close < 1.5)  return (FordDataTypes::grey);
                    else if (close < 5.5)  return (FordDataTypes::blue);
                    else if (close < 20.5)  return (FordDataTypes::red);
                    else  return (FordDataTypes::white);
                }
                return (FordDataTypes::invalid);
            });

    assert(result[FordDataTypes::invalid] == 0);
    assert(result[FordDataTypes::grey] == 1232);
    assert(result[FordDataTypes::blue] == 2877);
    assert(result[FordDataTypes::red] == 7279);
    assert(result[FordDataTypes::white] == 877);
    assert((df.get_index().size() == (0 + 1232 + 2877 + 7279 + 877)));
}

// ----------------------------------------------------------------------------

static void test_AnomalyDetectByKNNVisitor()  {

    std::cout << "\nTesting AnomalyDetectByKNNVisitor{ } ..." << std::endl;

    constexpr std::size_t   item_cnt = 1024;
    ULDataFrame             df;

    df.load_index(ULDataFrame::gen_sequence_index(0, item_cnt, 1));

    std::vector<double>   sine_col;

    sine_col.reserve(item_cnt);
    for (std::size_t i = 0; i < item_cnt; ++i)  {
        sine_col.push_back(std::sin(2.0 * M_PI * i / 20.0)); // Base sine wave
        if (i % 31 == 0)  sine_col.back() += 10.0;  // Inject anomalies
    }
    df.load_column("sine col", std::move(sine_col));

    and_knn_v<double>   anomaly1 { 3, 4 };

    df.single_act_visit<double>("sine col", anomaly1);

    const auto  anomalous_indices1 = anomaly1.get_anomalous_indices();

    assert(anomaly1.get_result().size() == 1024);
    assert(anomalous_indices1.size() == 34);
    assert(anomalous_indices1[0] == 0);
    assert(anomalous_indices1[1] == 31);
    assert(anomalous_indices1[2] == 62);
    assert(anomalous_indices1[17] == 527);
    assert(anomalous_indices1[22] == 682);
    assert(anomalous_indices1[32] == 992);
    assert(anomalous_indices1[33] == 1023);

    // Now do the same thing for IBM market data
    //
    StrDataFrame    ibm;

    try  {
        ibm.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }
    ibm.get_column<double>("IBM_Adj_Close")[502] = 800.0;
    ibm.get_column<double>("IBM_Adj_Close")[1001] = 900.0;
    ibm.get_column<double>("IBM_Adj_Close")[2002] = 850.0;

    and_knn_v<double, std::string>  anomaly2 {
        3, 4, normalization_type::z_score
    };

    ibm.single_act_visit<double>("IBM_Adj_Close", anomaly2);

    const auto  anomalous_indices2 = anomaly2.get_anomalous_indices(0.9);

    assert(anomalous_indices2.size() == 3);
    assert(anomalous_indices2[0] == 502);
    assert(anomalous_indices2[1] == 1001);
    assert(anomalous_indices2[2] == 2002);

    // Now multidimensional data
    //
    constexpr std::size_t   dim { 3 };
    constexpr std::size_t   n { 120 };

    using ary_col_t = std::array<double, dim>;
    using vec_col_t = std::vector<double>;

    std::vector<vec_col_t>  vec_col;

    vec_col.reserve(n);

    // Normal region 1: cluster near (1, 2, 3)
    //
    for (std::size_t i { 0 }; i < 40; ++i)  {
        double  t { i * 0.15 };

        vec_col.push_back({ 1.0 + 0.1 * std::sin(t),
                            2.0 + 0.1 * std::cos(t),
                            3.0 + 0.05 * t });
    }

    // *** Anomaly A: sudden spike on all 3 axes ***
    //
    vec_col.push_back({ 9.5,  -7.0,  15.0 });   // idx 40
    vec_col.push_back({ 10.0, -8.0,  16.0 });   // idx 41
    vec_col.push_back({ 9.8,  -7.5,  15.5 });   // idx 42

    // Normal region 2: cluster near (-1, 0, 1)
    //
    for (std::size_t i { 0 }; i < 35; ++i)  {
        double  t { i * 0.15 };

        vec_col.push_back({ -1.0 + 0.1 * std::sin(t),
                            0.0 + 0.1 * std::cos(t),
                            1.0 + 0.05 * t });
    }

    // *** Anomaly B: one axis goes wild, others stay normal ***
    //
    vec_col.push_back({ -1.0,  0.0,  50.0 });   // idx 78 — z-axis outlier
    vec_col.push_back({ -1.1,  0.1,  52.0 });   // idx 79

    // Normal region 3: cluster near (3, 3, 3)
    //
    for (std::size_t i { 0 }; i < 38; ++i)  {
        double  t { i * 0.15 };

        vec_col.push_back({ 3.0 + 0.1 * std::sin(t),
                            3.0 + 0.1 * std::cos(t),
                            3.0 + 0.05 * t });
    }

    // *** Anomaly C: isolated single point far from everything ***
    //
    vec_col.push_back({ -20.0, 20.0, -20.0 });  // idx 118

    // Final normal cap
    //
    vec_col.push_back({ 3.0, 3.0, 3.0 });

    std::vector<ary_col_t>  ary_col(n);

    // Copy the vector of vectors to vector of arrays
    //
    for (std::size_t i { 0 }; i < vec_col.size(); ++i)  {
        const auto  &vec = vec_col[i];
        ary_col_t   ary;

        for (std::size_t j { 0 }; j < vec.size(); ++j)
            ary[j] = vec[j];
        ary_col[i] = std::move(ary);
    }

    df.load_column<ary_col_t>("ARY COL", std::move(ary_col),
                              nan_policy::dont_pad_with_nans);
    df.load_column<vec_col_t>("VEC COL", std::move(vec_col),
                              nan_policy::dont_pad_with_nans);

    and_knn_v<vec_col_t>    vec_knn { 4, 5 };
    and_knn_v<ary_col_t>    ary_knn { 4, 5 };

    df.single_act_visit<vec_col_t>("VEC COL", vec_knn);
    df.single_act_visit<ary_col_t>("ARY COL", ary_knn);

    const auto  &anom_idxs_vec { vec_knn.get_anomalous_indices(0.5) };
    const auto  &anom_idxs_ary { ary_knn.get_anomalous_indices(0.5) };

    // Why didn't all anomalies get caught:
    // 1. Anomaly A (indices 40–42) is a cluster of 3 similar points. When the
    //    tree scores index 41, its nearest neighbors are indices 40 and 42 —
    //    they're close to each other, so the average KNN distance is small.
    //    A cluster of outliers looks locally dense to KNN. This is a known
    //    fundamental limitation of KNN anomaly detection.
    // 2. Anomaly C (index 118) is a single point but near the end of the
    //    series. With window=4, the buckets containing index 118 have very
    //    few neighbors in the tree overall, and the score gets averaged down
    //    by expand_scores_ across the window, diluting the signal.
    // 3. The three normal clusters are far apart from each other — (1,2,3),
    //    (-1,0,1), (3,3,3). This raises the baseline KNN distances for normal
    //    points near cluster boundaries, compressing the relative contrast
    //    with anomalies.
    //
    assert(anom_idxs_vec.size() == 3);
    assert(anom_idxs_ary.size() == 3);
    assert(anom_idxs_vec[0] == 41);
    assert(anom_idxs_vec[1] == 79);
    assert(anom_idxs_vec[2] == 118);
    assert(anom_idxs_ary[0] == 41);
    assert(anom_idxs_ary[1] == 79);
    assert(anom_idxs_ary[2] == 118);
}

// ----------------------------------------------------------------------------

static void test_BIRCHVisitor()  {

    std::cout << "\nTesting BIRCHVisitor{ } ..." << std::endl;

    ULDataFrame df;

    try  {
        df.read("FORD.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    BIRCHVisitor<double>    birch(4, 2.5);

    df.single_act_visit<double>("FORD_Close", birch);

    // The centroids
    //
    assert(birch.get_result().size() == 4);
    assert(std::fabs(birch.get_result()[0] - 3.02152) < 0.00001);
    assert(std::fabs(birch.get_result()[1] - 9.35769) < 0.00001);
    assert(std::fabs(birch.get_result()[2] - 14.2126) < 0.0001);
    assert(std::fabs(birch.get_result()[3] - 28.1059) < 0.0001);

    // const auto  &close = df.get_column<double>("FORD_Close");

    // Print the clusters
    //
    // for (const auto &vec : birch.get_clusters_idxs())  {
    //     for (const auto &idx : vec)
    //         std::cout << close[idx] << ", ";
    //     std::cout << "\n\n\n";
    // }
    // std::cout << std::endl;

    // Now multidimensional data
    //
    RandGenParams<double>   p;

    p.seed = 123;
    p.min_value = -20.0;
    p.max_value = 20.0;

    using col_t = std::array<double, 3>;

    auto    rand_vec =
        gen_uniform_real_dist<double>(df.get_index().size() * 3, p);

    std::vector<col_t>  multi_dimen_col(df.get_index().size());

    for (std::size_t i { 0 }, j { 0 }; j < rand_vec.size(); ++i)  {
        multi_dimen_col[i][0] = rand_vec[j++];
        multi_dimen_col[i][1] = rand_vec[j++];
        multi_dimen_col[i][2] = rand_vec[j++];
    }
    df.load_column<col_t>("multi_dimen_col", std::move(multi_dimen_col));

    BIRCHVisitor<col_t> birch2(4, 2.5);

    // This is the default distance function for multidimensional data.
    // But I am setting it here for testing and illustration.
    //
    birch2.set_dist_func(
        [](const col_t &x, const col_t &y) -> double  {
            double  sum { 0 };

            for (std::size_t i { 0 }; i < x.size(); ++i)  {
                const double    diff { x[i] - y[i] };

                sum += diff * diff;
            }
            return (std::sqrt(sum));
        });
    df.single_act_visit<col_t>("multi_dimen_col", birch2);

    assert(birch2.get_result().size() == 4);
    assert(std::fabs(birch2.get_result()[0][0] - -10.9757) < 0.0001);
    assert(std::fabs(birch2.get_result()[0][1] - -1.08913) < 0.00001);
    assert(std::fabs(birch2.get_result()[0][2] - 9.23815) < 0.00001);

    assert(std::fabs(birch2.get_result()[1][0] - -0.544063) < 0.000001);
    assert(std::fabs(birch2.get_result()[1][1] - 10.899) < 0.001);
    assert(std::fabs(birch2.get_result()[1][2] - -9.79122) < 0.00001);

    assert(std::fabs(birch2.get_result()[2][0] - 0.911082) < 0.000001);
    assert(std::fabs(birch2.get_result()[2][1] - -10.8084) < 0.0001);
    assert(std::fabs(birch2.get_result()[2][2] - -9.55188) < 0.00001);

    assert(std::fabs(birch2.get_result()[3][0] - 10.4331) < 0.0001);
    assert(std::fabs(birch2.get_result()[3][1] - 1.78207) < 0.00001);
    assert(std::fabs(birch2.get_result()[3][2] - 9.60288) < 0.00001);
}

// -----------------------------------------------------------------------------

void test_get_data_by_birch()  {

    std::cout << "\nTesting get_data_by_birch( ) ..." << std::endl;

    ULDataFrame df;

    try  {
        df.read("FORD.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto    lbd =
        [](const unsigned long &, const double &) -> bool { return (true); };
    auto    view =
        df.get_view_by_sel<double, decltype(lbd), double, long>
           ("FORD_Open", lbd);

    // I am using both views and dataframes to make sure both work
    //
    auto    views =
        view.get_view_by_birch<double, double, long>("FORD_Close", 4, 2.5);
    auto    dfs =
        df.get_data_by_birch<double, double, long>("FORD_Close", 4, 2.5);

    assert(views.size() == 4);
    assert(dfs.size() == 4);

    assert(views[0].get_index().size() == 4367);
    assert(dfs[0].get_index().size() == 4367);
    assert(views[1].get_index().size() == 4450);
    assert(dfs[1].get_index().size() == 4450);
    assert(views[2].get_index().size() == 2575);
    assert(dfs[2].get_index().size() == 2575);
    assert(views[3].get_index().size() == 873);
    assert(dfs[3].get_index().size() == 873);

    assert(
    (std::fabs(views[0].get_column<double>("FORD_Close")[7] - 2.08) < 0.01));
    assert(
    (std::fabs(dfs[1].get_column<double>("FORD_Open")[15] - 6.91) < 0.01));
    assert(
    (std::fabs(views[2].get_column<double>("FORD_High")[3] - 12.07) < 0.01));
    assert(dfs[2].get_column<long>("FORD_Volume")[0] == 7512900);
    assert(views[3].get_index()[1] == 6507);
}

// ----------------------------------------------------------------------------

void test_md_stats()  {

    std::cout << "\nTesting get_md_stats( ) ..." << std::endl;

    constexpr std::size_t   item_cnt = 1024;
    ULDataFrame             df;

    df.load_index(ULDataFrame::gen_sequence_index(0, item_cnt, 1));

    RandGenParams<double>   p;

    p.seed = 123;
    p.min_value = 0.5;
    p.max_value = 2.0;

    constexpr std::size_t   dim { 3 };

    using ary_col_t = std::array<double, dim>;
    using vec_col_t = std::vector<double>;

    // Generate and load 3 random columns
    //
    auto    rand_vec =
        gen_uniform_real_dist<double>(df.get_index().size() * dim, p);

    std::vector<ary_col_t>  array_col(df.get_index().size());
    std::vector<ary_col_t>  array_col2(df.get_index().size());
    std::vector<vec_col_t>  vector_col(df.get_index().size());

    for (std::size_t i { 0 }, j { 0 }; j < rand_vec.size(); ++i)  {
        vector_col[i].resize(dim);
        for (std::size_t d { 0 }; d < dim; ++d)
            array_col[i][d] = vector_col[i][d] = rand_vec[j++];
    }
    df.load_column<ary_col_t>("array_col", std::move(array_col));
    df.load_column<vec_col_t>("vector_col", std::move(vector_col));

    p.seed = 1024;
    rand_vec = gen_uniform_real_dist<double>(df.get_index().size() * dim, p);
    for (std::size_t i { 0 }, j { 0 }; j < rand_vec.size(); ++i)  {
        for (std::size_t d { 0 }; d < dim; ++d)
            array_col2[i][d] = rand_vec[j++];
    }
    df.load_column<ary_col_t>("array_col2", std::move(array_col2));

    // Mean and Sum
    //
    MeanVisitor<ary_col_t>  ary_mean;
    MeanVisitor<vec_col_t>  vec_mean;

    df.single_act_visit<ary_col_t>("array_col", ary_mean);
    df.visit<vec_col_t>("vector_col", vec_mean);
    assert(std::fabs(ary_mean.get_result()[0] - 1.25062) < 0.00001);
    assert(std::fabs(ary_mean.get_result()[1] - 1.2646) < 0.0001);
    assert(std::fabs(ary_mean.get_result()[2] - 1.24829) < 0.00001);
    assert(std::fabs(vec_mean.get_result()[0] - 1.25062) < 0.00001);
    assert(std::fabs(vec_mean.get_result()[1] - 1.2646) < 0.0001);
    assert(std::fabs(vec_mean.get_result()[2] - 1.24829) < 0.00001);

    // Prod
    //
    ProdVisitor<ary_col_t>  ary_prod;
    ProdVisitor<vec_col_t>  vec_prod;

    df.single_act_visit<ary_col_t>("array_col", ary_prod);
    df.visit<vec_col_t>("vector_col", vec_prod);
    //
    // Numbers will be very large

    // Covariance
    //
    CovVisitor<ary_col_t>   cov;

    df.single_act_visit<ary_col_t, ary_col_t>("array_col", "array_col2", cov);

    const auto  &cov_result { cov.get_result() };
    const auto  &mean1_result { cov.get_mean1() };
    const auto  &mean2_result { cov.get_mean2() };

    assert(cov_result.rows() == dim);
    assert(cov_result.cols() == dim);
    assert(std::fabs(cov_result(0, 0) - 0.00187) < 0.00001);
    assert(std::fabs(cov_result(1, 2) - 0.00420) < 0.00001);
    assert(std::fabs(cov_result(2, 1) - -0.000457) < 0.000001);
    assert(mean1_result.size() == dim);
    assert(std::fabs(mean1_result[1] - 1.2646) < 0.0001);
    assert(std::fabs(mean1_result[2] - 1.24829) < 0.00001);
    assert(mean2_result.size() == dim);
    assert(std::fabs(mean2_result[1] - 1.25428) < 0.00001);
    assert(std::fabs(mean2_result[2] - 1.25122) < 0.00001);

    CovVisitor<vec_col_t>   cov2;

    df.single_act_visit<vec_col_t, vec_col_t>("vector_col", "vector_col", cov2);

    const auto  &cov_result2 { cov2.get_result() };

    assert(cov_result2.rows() == dim);
    assert(cov_result2.cols() == dim);
    assert(std::fabs(cov_result2(0, 0) - 0.188617) < 0.000001);
    assert(std::fabs(cov_result2(1, 2) - 0.004326) < 0.000001);
    assert(std::fabs(cov_result2(2, 1) - 0.004326) < 0.000001);

    // Standard Deviation / Variance
    //
    VarVisitor<vec_col_t>   var;
    StdVisitor<ary_col_t>   stdev;

    df.single_act_visit<vec_col_t>("vector_col", var);
    df.single_act_visit<ary_col_t>("array_col", stdev);

    const auto  &var_result = var.get_result();

    assert(var_result.rows() == dim);
    assert(var_result.cols() == dim);
    assert(std::fabs(var_result(0, 0) - 0.188617) < 0.000001);
    assert(std::fabs(var_result(1, 2) - 0.004326) < 0.000001);
    assert(std::fabs(var_result(2, 1) - 0.004326) < 0.000001);

    const auto  &var_mean = var.get_mean();

    assert(var_mean.size() == dim);
    assert(std::fabs(var_mean[1] - 1.2646) < 0.0001);
    assert(std::fabs(var_mean[2] - 1.24829) < 0.00001);

    const auto  &std_result = stdev.get_result();

    assert(std_result.size() == dim);
    assert(std::fabs(std_result[1] - 0.428274) < 0.000001);
    assert(std::fabs(std_result[2] - 0.431151) < 0.000001);

    const auto  &std_mean = stdev.get_mean();

    assert(std_mean.size() == dim);
    assert(std::fabs(std_mean[1] - 1.2646) < 0.0001);
    assert(std::fabs(std_mean[2] - 1.24829) < 0.00001);

    // Pearson Correlation
    //
    CorrVisitor<ary_col_t>  md_corr;

    df.single_act_visit<ary_col_t, ary_col_t>
        ("array_col", "array_col2", md_corr);
    assert(md_corr.get_result().size() == dim);
    assert(md_corr.get_data_mean1().size() == dim);
    assert(md_corr.get_data_mean2().size() == dim);

    assert(std::fabs(md_corr.get_result()[0] - 0.00982) < 0.00001);
    assert(std::fabs(md_corr.get_result()[2] - 0.03198) < 0.00001);

    assert(std::fabs(md_corr.get_data_mean1()[0] - 1.25062) < 0.00001);
    assert(std::fabs(md_corr.get_data_mean1()[2] - 1.24829) < 0.00001);

    assert(std::fabs(md_corr.get_data_mean2()[0] - 1.24407) < 0.00001);
    assert(std::fabs(md_corr.get_data_mean2()[2] - 1.25122) < 0.00001);

    df.single_act_visit<ary_col_t, ary_col_t>
        ("array_col", "array_col", md_corr);
    assert(std::fabs(md_corr.get_result()[0] - 1.0) < 0.00001);
    assert(std::fabs(md_corr.get_result()[2] - 1.0) < 0.00001);

    assert(std::fabs(md_corr.get_data_mean1()[0] - 1.25062) < 0.00001);
    assert(std::fabs(md_corr.get_data_mean1()[2] - 1.24829) < 0.00001);

    assert(std::fabs(md_corr.get_data_mean2()[0] - 1.25062) < 0.00001);
    assert(std::fabs(md_corr.get_data_mean2()[2] - 1.24829) < 0.00001);

    // Cross Correlation
    //
}

// ----------------------------------------------------------------------------

static void test_KrigingVisitor()  {

    std::cout << "\nTesting KrigingVisitor{ } ..." << std::endl;

    ULDataFrame df;

    try  {
        df.read("FORD.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    // Duplicate points
    //
    std::vector<std::array<double, 2>>  coords {
        { 0.0, 0.0 }, { 0.0, 0.0 }, { 5.0, 5.0 }, { 1.0, 1.0 }
    };
    std::vector<double>                 obs { 1.0, 1.2, 4.0, 1.5 };

    df.load_column("COORDS 1", std::move(coords),
                   nan_policy::dont_pad_with_nans);
    df.load_column("OBSERV 1", std::move(obs), nan_policy::dont_pad_with_nans);

    KrigingParams<double>  params;

    params.model = VariogramModel::spherical;
    params.range = 3.0;
    params.ridge = 1e-8;

    KrigingVisitor<2, double>   kv1 { params };

    df.single_act_visit<std::array<double, 2>, double>
        ("COORDS 1", "OBSERV 1", kv1);

    const auto  estimate1 { kv1.predict({ 0.0, 0.0 }) };
    const auto  leave_one_out1 { kv1.loo_cross_validate() };

    assert(std::abs(estimate1.value - 1.1) < 0.001);
    assert(estimate1.variance < 0.00000001);
    assert(leave_one_out1.size() == 4);
    assert(std::abs(leave_one_out1[0] - -0.2) < 0.001);
    assert(std::abs(leave_one_out1[1] - 0.2) < 0.001);
    assert(std::abs(leave_one_out1[3] - -0.549356) < 0.000001);

    // Filtering NaN
    //
    constexpr double   qnan = std::numeric_limits<double>::quiet_NaN();

    std::vector<std::array<double, 2>>  coords2 {
        { 0.0, 0.0 }, { 1.0, 1.0 }, { qnan, 2.0 }, { 3.0, 3.0 }, { 4.0, 4.0 }
    };
    std::vector<double>                 obs2 { 1.0, 2.0, 3.0, qnan, 5.0 };

    df.load_column("COORDS 2", std::move(coords2),
                   nan_policy::dont_pad_with_nans);
    df.load_column("OBSERV 2", std::move(obs2),
                   nan_policy::dont_pad_with_nans);

    KrigingParams<double>   params2;

    params2.model = VariogramModel::exponential;
    params2.range = 2.0;

    KrigingVisitor<2, double>   kv2 { params2 };

    df.single_act_visit<std::array<double, 2>, double>
        ("COORDS 2", "OBSERV 2", kv2);

    const auto  estimate2 { kv2.predict({ 0.0, 0.0 }) };
    const auto  leave_one_out2 { kv2.loo_cross_validate() };

    assert(std::abs(estimate2.value - 1.0) < 0.001);
    assert(estimate2.variance < 0.000000001);
    assert(leave_one_out2.size() == 3);
    assert(std::abs(leave_one_out2[0] - -1.7604) < 0.0001);
    assert(std::abs(leave_one_out2[1] - -0.206722) < 0.000001);
    assert(std::abs(leave_one_out2[2] - 3.44006) < 0.00001);

    // 3D
    //
    std::vector<std::array<double, 3>>  coords3;
    std::vector<double>                 obs3;

    for (int i = 0; i < 30; ++i)  {
        const double    x = (i % 3) * 2.0;
        const double    y = ((i / 3) % 3) * 2.0;
        const double    z = (i / 9) * 2.0;

        coords3.push_back({ x, y, z });
        obs3.push_back(x + 2.0 * y - z);
    }
    df.load_column("COORDS 3", std::move(coords3),
                   nan_policy::dont_pad_with_nans);
    df.load_column("OBSERV 3", std::move(obs3),
                   nan_policy::dont_pad_with_nans);

    KrigingParams<double>   params3;

    params3.model = VariogramModel::matern;
    params3.matern_smoothness = 2.5;
    params3.range = 4.0;

    KrigingVisitor<3, double>   kv3 { params3 };

    df.single_act_visit<std::array<double, 3>, double>
        ("COORDS 3", "OBSERV 3", kv3);

    const auto  estimate3 { kv3.predict({ 1.0, 1.0, 1.0 }) };
    const auto  leave_one_out3 { kv3.loo_cross_validate() };

    assert(std::abs(estimate3.value - 1.60219) < 0.00001);
    assert(std::abs(estimate3.variance - 0.018453) < 0.000001);
    assert(leave_one_out3.size() == 30);
    assert(std::abs(leave_one_out3[0] - -0.276349) < 0.000001);
    assert(std::abs(leave_one_out3[10] - 0.030186) < 0.000001);
    assert(std::abs(leave_one_out3[29] - -0.800075) < 0.000001);

    // Generic Bessel-K path
    //
    std::vector<std::array<double, 1>>  coords4 {
        { 0.0 }, { 1.0 }, { 2.0 }, { 3.0 }, { 4.0 }, { 5.0 }
    };
    std::vector<double>                 obs4 { 0.0, 1.0, 0.5, 1.5, 1.0, 2.0 };

    df.load_column("COORDS 4", std::move(coords4),
                   nan_policy::dont_pad_with_nans);
    df.load_column("OBSERV 4", std::move(obs4),
                   nan_policy::dont_pad_with_nans);
    for (double nu : { 0.7, 1.0, 1.8, 3.3 })  {
        KrigingParams<double>  params;

        params.model = VariogramModel::matern;
        params.matern_smoothness = nu;
        params.range = 2.0;

        KrigingVisitor<1, double>     kv { params };

        df.single_act_visit<std::array<double, 1>, double>
            ("COORDS 4", "OBSERV 4", kv);

        const auto  estimate = kv.predict({ 2.5 });
        const auto  leave_one_out { kv.loo_cross_validate() };

        assert(leave_one_out.size() == 6);
        assert(std::abs(estimate.value - 1.0) < 0.001);
        if (nu == 0.7)  {
            assert(std::abs(estimate.variance - 0.38837) < 0.00001);
            assert(std::abs(leave_one_out[0] - -1.08785) < 0.00001);
            assert(std::abs(leave_one_out[3] - 0.663442) < 0.000001);
            assert(std::abs(leave_one_out[5] - 1.08785) < 0.00001);
        }
        else if (nu == 1.0)  {
            assert(std::abs(estimate.variance - 0.072554) < 0.000001);
            assert(std::abs(leave_one_out[0] - -1.25466) < 0.00001);
            assert(std::abs(leave_one_out[3] - 0.930086) < 0.000001);
            assert(std::abs(leave_one_out[5] - 1.25466) < 0.00001);
        }
        else if (nu == 1.8)  {
            assert(std::abs(estimate.variance - 0.593665) < 0.000001);
            assert(std::abs(leave_one_out[0] - -1.11348) < 0.00001);
            assert(std::abs(leave_one_out[3] - 0.682445) < 0.000001);
            assert(std::abs(leave_one_out[5] - 1.11348) < 0.00001);
        }
        else if (nu == 3.3)  {
            assert(std::abs(estimate.variance - 0.872824) < 0.000001);
            assert(std::abs(leave_one_out[0] - -1.02868) < 0.00001);
            assert(std::abs(leave_one_out[3] - 0.443655) < 0.000001);
            assert(std::abs(leave_one_out[5] - 1.02868) < 0.00001);
        }
    }
}

// -----------------------------------------------------------------------------

static void test_asof_join()  {

    std::cout << "\nTesting asof_join( ) ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    MyDataFrame lhs, rhs;

    lhs.load_index(std::vector<unsigned long>{ 1, 3, 5, 7, 9 });
    lhs.load_column<double>("price",
                            std::vector<double>{ 1.1, 3.3, 5.5, 7.7, 9.9 });

    rhs.load_index(std::vector<unsigned long>{ 2, 4, 6 });
    rhs.load_column<double>("quote", std::vector<double>{ 20.0, 40.0, 60.0 });

    // Test backward
    //
    {
        const auto  result {
            lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::backward)
        };
        const auto  &ridx { result.get_index() };

        assert(ridx.size() == 5);
        assert(ridx[0] == 1 && ridx[1] == 3 && ridx[2] == 5 &&
               ridx[3] == 7 && ridx[4] == 9);

        // lhs price column should be present unmodified
        //
        const auto  &price { result.get_column<double>("price") };

        assert(price[0] == 1.1 && price[4] == 9.9);

        // rhs quote column: lhs=1 has no rhs before it -> NaN
        //
        const auto  &quote { result.get_column<double>("quote") };

        assert(std::isnan(quote[0]));  // lhs=1: no rhs <= 1
        assert(quote[1] == 20.0);  // lhs=3: rhs=2 -> 20
        assert(quote[2] == 40.0);  // lhs=5: rhs=4 -> 40
        assert(quote[3] == 60.0);  // lhs=7: rhs=6 -> 60
        assert(quote[4] == 60.0);  // lhs=9: rhs=6 -> 60 (last carry)
    }

    // Test foreward
    //
    {
        const auto  result {
            lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::forward)
        };
        const auto  &ridx { result.get_index() };

        assert(ridx.size() == 5);
        assert(ridx[0] == 1 && ridx[1] == 3 && ridx[2] == 5 &&
               ridx[3] == 7 && ridx[4] == 9);

        const auto  &quote { result.get_column<double>("quote") };

        // lhs price column should be present unmodified
        //
        const auto  &price { result.get_column<double>("price") };

        assert(price[0] == 1.1 && price[4] == 9.9);

        assert(quote[0] == 20.0);      // lhs=1: rhs=2 -> 20
        assert(quote[1] == 40.0);      // lhs=3: rhs=4 -> 40
        assert(quote[2] == 60.0);      // lhs=5: rhs=6 -> 60
        assert(std::isnan(quote[3]));  // lhs=7: no rhs >= 7
        assert(std::isnan(quote[4]));  // lhs=9: no rhs >= 9
    }

    // Test nearest
    //
    {
        const auto  result {
            lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::nearest)
        };
        const auto  &ridx { result.get_index() };

        assert(ridx.size() == 5);
        assert(ridx[0] == 1 && ridx[1] == 3 && ridx[2] == 5 &&
               ridx[3] == 7 && ridx[4] == 9);

        const auto  &quote { result.get_column<double>("quote") };

        // lhs price column should be present unmodified
        //
        const auto  &price { result.get_column<double>("price") };

        assert(price[0] == 1.1 && price[4] == 9.9);

        assert(quote[0] == 20.0);  // lhs=1: only forward rhs=2
        assert(quote[1] == 20.0);  // lhs=3: tie → backward -> rhs=2
        assert(quote[2] == 40.0);  // lhs=5: tie → backward -> rhs=4
        assert(quote[3] == 60.0);  // lhs=7: only backward -> rhs=6
        assert(quote[4] == 60.0);  // lhs=9: only backward -> rhs=6
    }

    // Test exact match
    //
    {
        MyDataFrame lhs, rhs;

        lhs.load_index(std::vector<unsigned long>{ 3, 5, 8 });
        lhs.load_column<double>("x", std::vector<double>{ 3.0, 5.0, 8.0 });

        rhs.load_index(std::vector<unsigned long>{ 2, 5, 9 });
        rhs.load_column<double>("y",
                                std::vector<double>{ 200.0, 500.0, 900.0 });

        // Backward
        //
        auto        rb {
            lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::backward)
        };
        const auto  &yb { rb.get_column<double>("y") };

        assert(yb[0] == 200.0);  // lhs=3: rhs=2 (back)
        assert(yb[1] == 500.0);  // lhs=5: exact rhs=5
        assert(yb[2] == 500.0);  // lhs=8: rhs=5 (back, next is 9 which is >8)

        // forward
        //
        auto        rf {
            lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::forward)
        };
        const auto  &yf { rf.get_column<double>("y") };

        assert(yf[0] == 500.0);   // lhs=3: rhs=5 (fwd)
        assert(yf[1] == 500.0);   // lhs=5: exact rhs=5
        assert(yf[2] == 900.0);   // lhs=8: rhs=9 (fwd)
    }

    // Test tolerance
    //
    {
        const unsigned long tol { 1UL };
        const auto          result {
            lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::backward, tol)
        };
        const auto          &ridx { result.get_index() };

        assert(ridx.size() == 5);
        assert(ridx[0] == 1 && ridx[1] == 3 && ridx[2] == 5 &&
               ridx[3] == 7 && ridx[4] == 9);

        // lhs price column should be present unmodified
        //
        const auto  &price { result.get_column<double>("price") };

        assert(price[0] == 1.1 && price[4] == 9.9);

        const auto  &quote { result.get_column<double>("quote") };

        assert(std::isnan(quote[0]));  // lhs=1: no rhs <= 1 -> NaN
        assert(quote[1] == 20.0);      // lhs=3: dist 1 <= tol
        assert(quote[2] == 40.0);      // lhs=5: dist 1 <= tol
        assert(quote[3] == 60.0);      // lhs=7: dist 1 <= tol
        assert(std::isnan(quote[4]));  // lhs=9: dist 3 > tol -> NaN
    }

    // Empty rhs
    //
    {
        MyDataFrame lhs, rhs;

        lhs.load_index(std::vector<unsigned long>{ 1, 2, 3 });
        lhs.load_column<double>("price", std::vector<double>{ 1.0, 2.0, 3.0 });

        rhs.load_index(std::vector<unsigned long>{ });
        rhs.load_column<double>("quote", std::vector<double>{ });

        const auto  result {
            lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::backward)
        };
        const auto  &ridx { result.get_index() };

        assert(ridx.size() == 3);
        assert(ridx[0] == 1 && ridx[1] == 2 && ridx[2] == 3);

        // lhs price column should be present unmodified
        //
        const auto  &price { result.get_column<double>("price") };

        assert(price[0] == 1.0 && price[1] == 2.0 && price[2] == 3.0);

        const auto  &quote { result.get_column<double>("quote") };

        for (const auto &v : quote)
            assert(std::isnan(v));
    }

    // Same column names
    //
    {
        MyDataFrame lhs, rhs;

        lhs.load_index(std::vector<unsigned long>{ 1, 3, 5 });
        lhs.load_column<double>("value",
                                std::vector<double>{ 10.0, 30.0, 50.0 });

        rhs.load_index(std::vector<unsigned long>{ 2, 4 });
        rhs.load_column<double>("value", std::vector<double>{ 20.0, 40.0 });

        const auto  result {
            lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::backward)
        };

        assert(result.has_column("lhs.value"));
        assert(result.has_column("rhs.value"));

        const auto  &lv { result.get_column<double>("lhs.value") };
        const auto  &rv { result.get_column<double>("rhs.value") };

        assert(lv[0] == 10.0 && lv[1] == 30.0 && lv[2] == 50.0);
        assert(std::isnan(rv[0]));  // lhs=1: no rhs <= 1
        assert(rv[1] == 20.0);      // lhs=3: rhs=2
    }

    // Single elements
    //
    {
        {   // Single lhs, single rhs, exact
            MyDataFrame lhs, rhs;

            lhs.load_index(std::vector<unsigned long>{ 5 });
            lhs.load_column<double>("x", std::vector<double>{ 1.0 });
            rhs.load_index(std::vector<unsigned long>{ 5 });
            rhs.load_column<double>("y", std::vector<double>{ 99.0 });

            const auto  r {
                lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::backward)
            };

            assert(r.get_column<double>("y")[0] == 99.0);
        }

        {   // Single lhs, single rhs, lhs before rhs (backward → NaN)
            MyDataFrame lhs, rhs;

            lhs.load_index(std::vector<unsigned long>{ 3 });
            lhs.load_column<double>("x", std::vector<double>{ 1.0 });
            rhs.load_index(std::vector<unsigned long>{ 5 });
            rhs.load_column<double>("y", std::vector<double>{ 99.0 });

            const auto  r {
                lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::backward)
            };

            assert(std::isnan(r.get_column<double>("y")[0]));
        }

        {   // Single lhs, single rhs, lhs before rhs (forward → value)
            MyDataFrame lhs, rhs;

            lhs.load_index(std::vector<unsigned long>{ 3 });
            lhs.load_column<double>("x", std::vector<double>{ 1.0 });
            rhs.load_index(std::vector<unsigned long>{ 5 });
            rhs.load_column<double>("y", std::vector<double>{ 99.0 });

            const auto  r {
                lhs.asof_join<MyDataFrame, double>(rhs, asof_policy::forward)
            };

            assert(r.get_column<double>("y")[0] == 99.0);
        }
    }
}

// -----------------------------------------------------------------------------

static void test_crosstab()  {

    std::cout << "\nTesting crosstab( ) ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    MyDataFrame df;

    df.load_index(std::vector<unsigned long>{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    df.load_column<std::string>(
        "dept",
        std::vector<std::string>{ "A","A","A","B","B","B","C","C","C","C" });
    df.load_column<std::string>(
        "grade",
        std::vector<std::string>{ "X","Y","X","Y","X","Y","X","X","Y","Y" });

    // Raw counts
    //
    {
        const auto  result {
            df.crosstab<std::string, std::string>("dept", "grade")
        };

        // result.write<std::ostream, unsigned long>
        //     (std::cout, io_format::pretty_prt);

        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        // Column names must be sorted unique grade values: X, Y
        //
        assert(result.has_column("X"));
        assert(result.has_column("Y"));

        const auto  &X { result.get_column<std::size_t>("X") };
        const auto  &Y { result.get_column<std::size_t>("Y") };

        assert(X[0] == 2 && X[1] == 1 && X[2] == 2);  // dept A,B,C vs X
        assert(Y[0] == 1 && Y[1] == 2 && Y[2] == 2);  // dept A,B,C vs Y
    }

    // Margins
    //
    {
        const auto  result {
            df.crosstab<std::string, std::string>(
                "dept", "grade", true)
        };

        // result.write<std::ostream, unsigned long>
        //     (std::cout, io_format::pretty_prt);

        // 4 rows: A, B, C, ""
        //
        const auto  &idx { result.get_index() };

        assert(idx.size() == 4);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C" &&
               idx[3].empty());

        // 3 columns: X, Y, All
        //
        assert(result.has_column("X"));
        assert(result.has_column("Y"));
        assert(result.has_column("All"));

        const auto  &X { result.get_column<std::size_t>("X") };
        const auto  &Y { result.get_column<std::size_t>("Y") };
        const auto  &All { result.get_column<std::size_t>("All") };

        // Row totals (All column)
        //
        assert(All[0] == 3);   // A: 2+1
        assert(All[1] == 3);   // B: 1+2
        assert(All[2] == 4);   // C: 2+2
        assert(All[3] == 10);  // grand total

        // Column totals (All row, index 3)
        //
        assert(X[3] == 5);     // 2+1+2
        assert(Y[3] == 5);     // 1+2+2
        assert(All[3] == 10);  // grand total
    }

    // Normalize all
    //
    {
        const auto  result {
            df.crosstab<std::string, std::string>(
                "dept", "grade",
                false,
                crosstab_norm_policy::all)
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        // Grand total = 10
        //
        assert(std::abs(X[0] - 0.2) < 1e-9);  // A/X = 2/10
        assert(std::abs(Y[0] - 0.1) < 1e-9);  // A/Y = 1/10
        assert(std::abs(X[1] - 0.1) < 1e-9);  // B/X = 1/10
        assert(std::abs(Y[1] - 0.2) < 1e-9);  // B/Y = 2/10
        assert(std::abs(X[2] - 0.2) < 1e-9);  // C/X = 2/10
        assert(std::abs(Y[2] - 0.2) < 1e-9);  // C/Y = 2/10

        // All cells must sum to 1.0
        //
        double  total { 0.0 };

        for (std::size_t i { 0 }; i < 3; ++i)
            total += X[i] + Y[i];
        assert(std::abs(total - 1.0) < 1e-9);
    }

    // Normalize row
    //
    {
        const auto  result {
            df.crosstab<std::string, std::string>(
                "dept", "grade",
                false,
                crosstab_norm_policy::row)
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        // Row totals: A=3, B=3, C=4
        //
        assert(X.size() == 3);
        assert(Y.size() == 3);
        assert(std::abs(X[0] - 2.0/3.0) < 1e-9);
        assert(std::abs(Y[0] - 1.0/3.0) < 1e-9);
        assert(std::abs(X[1] - 1.0/3.0) < 1e-9);
        assert(std::abs(Y[1] - 2.0/3.0) < 1e-9);
        assert(std::abs(X[2] - 0.5) < 1e-9);
        assert(std::abs(Y[2] - 0.5) < 1e-9);

        // Each row must sum to 1.0
        //
        for (std::size_t i { 0 }; i < 3; ++i)
            assert(std::abs(X[i] + Y[i] - 1.0) < 1e-9);
    }

    // Normalize column
    //
    {
        const auto  result {
            df.crosstab<std::string, std::string>(
                "dept", "grade",
                false,
                crosstab_norm_policy::column)
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        // Column totals: X=5, Y=5
        //
        assert(std::abs(X[0] - 2.0/5.0) < 1e-9);
        assert(std::abs(Y[0] - 1.0/5.0) < 1e-9);
        assert(std::abs(X[1] - 1.0/5.0) < 1e-9);
        assert(std::abs(Y[1] - 2.0/5.0) < 1e-9);
        assert(std::abs(X[2] - 2.0/5.0) < 1e-9);
        assert(std::abs(Y[2] - 2.0/5.0) < 1e-9);

        // Each column must sum to 1.0
        //
        double  sum_X { 0.0 }, sum_Y { 0.0 };

        for (std::size_t i { 0 }; i < 3; ++i)  {
            sum_X += X[i];
            sum_Y += Y[i];
        }
        assert(std::abs(sum_X - 1.0) < 1e-9);
        assert(std::abs(sum_Y - 1.0) < 1e-9);
    }

    // Row from index column
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 10, 10, 20, 20, 30, 30 });
        df.load_column<std::string>(
            "grade",
            std::vector<std::string>{ "X","Y","X","Y","X","Y" });

        const auto  result {
            df.crosstab<unsigned long, std::string>(
                DF_INDEX_COL_NAME, "grade")
        };

        // result.write<std::ostream, unsigned long>
        //     (std::cout, io_format::pretty_prt);

        const auto &idx = result.get_index();

        assert(idx.size() == 3);
        assert(idx[0] == 10 && idx[1] == 20 && idx[2] == 30);

        const auto  &X { result.get_column<unsigned long>("X") };
        const auto  &Y { result.get_column<unsigned long>("Y") };

        for (std::size_t i { 0 }; i < 3; ++i)  {
            assert(X[i] == 1);
            assert(Y[i] == 1);
        }
    }

    // Column from index column
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 1, 2, 1, 2 });
        df.load_column<std::string>(
            "dept",
            std::vector<std::string>{ "A","A","B","B" });

        const auto  result {
            df.crosstab<std::string, unsigned long>(
                "dept", DF_INDEX_COL_NAME)
        };

        // result.write<std::ostream, unsigned long>
        //     (std::cout, io_format::pretty_prt);

        const auto  &idx { result.get_index() };

        assert(idx.size() == 2);
        assert(idx[0] == "A" && idx[1] == "B");

        // Column names are the stringified index values "1" and "2"
        //
        assert(result.has_column("1"));
        assert(result.has_column("2"));

        const auto  &c1 { result.get_column<unsigned long>("1") };
        const auto  &c2 { result.get_column<unsigned long>("2") };

        assert(c1[0] == 1 && c1[1] == 1);
        assert(c2[0] == 1 && c2[1] == 1);
    }

    // Integer types
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 0,1,2,3,4,5 });
        df.load_column<int>("row_key", std::vector<int>{ 1,1,2,2,3,3 });
        df.load_column<int>("col_key", std::vector<int>{ 10,20,10,20,10,20 });

        const auto  result { df.crosstab<int, int>("row_key", "col_key") };

        // result.write<std::ostream, unsigned long>
        //     (std::cout, io_format::pretty_prt);

        // Index: 1, 2, 3
        //
        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == 1 && idx[1] == 2 && idx[2] == 3);

        // Column names: "10", "20"
        //
        assert(result.has_column("10"));
        assert(result.has_column("20"));

        const auto  &c10 { result.get_column<unsigned long>("10") };
        const auto  &c20 { result.get_column<unsigned long>("20") };

        for (std::size_t i { 0 }; i < 3; ++i)  {
            assert(c10[i] == 1);
            assert(c20[i] == 1);
        }
    }

    // Normalize all margins
    //
    {
        const auto  result {
            df.crosstab<std::string, std::string>(
                "dept", "grade",
                true,
                crosstab_norm_policy::all)
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        // 4 rows: A, B, C, ""
        //
        const auto  &idx { result.get_index() };

        assert(idx.size() == 4);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C" &&
               idx[3].empty());

        // Grand total = 10; the "All/All" cell should be 1.0
        //
        const auto  &All { result.get_column<double>("All") };

        assert(All.size() == 4);
        assert(std::abs(All[0] - 0.3) < 1e-9);
        assert(std::abs(All[2] - 0.4) < 1e-9);
        assert(std::abs(All.back() - 1.0) < 1e-9);

        // All row: X_total/grand = 5/10 = 0.5; Y_total/grand = 0.5
        //
        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        assert(X.size() == 4);
        assert(Y.size() == 4);
        assert(std::abs(X[0] - 0.2) < 1e-9);
        assert(std::abs(X[2] - 0.2) < 1e-9);
        assert(std::abs(X.back() - 0.5) < 1e-9);
        assert(std::abs(Y[0] - 0.1) < 1e-9);
        assert(std::abs(Y[2] - 0.2) < 1e-9);
        assert(std::abs(Y.back() - 0.5) < 1e-9);
    }
}

// -----------------------------------------------------------------------------

static void test_pivot_table()  {

    std::cout << "\nTesting pivot_table( ) ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    MyDataFrame df;

    df.load_index(std::vector<unsigned long>{ 0,1,2,3,4,5,6,7 });
    df.load_column<std::string>(
        "dept",
        std::vector<std::string>{ "A","A","A","B","B","B","C","C" });
    df.load_column<std::string>(
        "region",
        std::vector<std::string>{ "X","Y","X","Y","X","X","Y","Y" });
    df.load_column<double>(
        "sales",
        std::vector<double>{ 10,20,30,40,50,60,70,80 });

    // Sum
    //
    {
        const auto  result {
            df.pivot_table<std::string, std::string, double,
                           SumVisitor<double>>(
                "dept", "region", "sales",
                SumVisitor<double>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        // Index: A, B, C  (sorted)
        //
        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        // Columns: X, Y
        //
        assert(result.has_column("X"));
        assert(result.has_column("Y"));

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        assert(X.size() == 3);
        assert(Y.size() == 3);
        assert(std::abs(X[0] - 40.0) < 1e-9);   // A/X: 10+30
        assert(std::abs(Y[0] - 20.0) < 1e-9);   // A/Y: 20
        assert(std::abs(X[1] - 110.0) < 1e-9);  // B/X: 50+60
        assert(std::abs(Y[1] - 40.0) < 1e-9);   // B/Y: 40
        assert(std::isnan(X[2]));               // C/X: no data → NaN
        assert(std::abs(Y[2] - 150.0) < 1e-9);  // C/Y: 70+80
    }

    // Mean
    //
    {
        const auto  result {
            df.pivot_table<std::string, std::string, double,
                           MeanVisitor<double>>(
                "dept", "region", "sales",
                MeanVisitor<double>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        // Index: A, B, C  (sorted)
        //
        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        // Columns: X, Y
        //
        assert(result.has_column("X"));
        assert(result.has_column("Y"));

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        assert(X.size() == 3);
        assert(Y.size() == 3);
        assert(std::abs(X[0] - 20.0) < 1e-9);  // A/X: avg(10,30)
        assert(std::abs(Y[0] - 20.0) < 1e-9);  // A/Y: avg(20)
        assert(std::abs(X[1] - 55.0) < 1e-9);  // B/X: avg(50,60)
        assert(std::abs(Y[1] - 40.0) < 1e-9);  // B/Y: avg(40)
        assert(std::isnan(X[2]));              // C/X: no data → NaN
        assert(std::abs(Y[2] - 75.0) < 1e-9);  // C/Y: avg(70,80)
    }

    // Count
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 0,1,2,3,4,5,6,7 });
        df.load_column<std::string>(
           "dept",
           std::vector<std::string>{ "A","A","A","B","B","B","C","C" });
        df.load_column<std::string>(
           "region",
           std::vector<std::string>{ "X","Y","X","Y","X","X","Y","Y" });
        df.load_column<double>(
           "ones",
           std::vector<double>{ 1,1,1,1,1,1,1,1 });

        const auto  result {
            df.pivot_table<std::string, std::string, double,
                           CountVisitor<std::size_t>>(
                "dept", "region", "ones",
                CountVisitor<std::size_t>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        // Index: A, B, C  (sorted)
        //
        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        // Columns: X, Y
        //
        assert(result.has_column("X"));
        assert(result.has_column("Y"));

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        assert(X.size() == 3);
        assert(Y.size() == 3);

        assert(std::abs(X[0] - 2.0) < 1e-9);  // A/X: 2 rows
        assert(std::abs(Y[0] - 1.0) < 1e-9);  // A/Y: 1 row
        assert(std::abs(X[1] - 2.0) < 1e-9);  // B/X: 2 rows
        assert(std::abs(Y[1] - 1.0) < 1e-9);  // B/Y: 1 row
        assert(X[2] == 0.0);                  // C/X: 0 rows
        assert(std::abs(Y[2] - 2.0) < 1e-9);  // C/Y: 2 rows
    }

    // Max
    //
    {
        const auto  result {
            df.pivot_table<std::string, std::string, double,
                           MaxVisitor<double>>(
                "dept", "region", "sales",
                MaxVisitor<double>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        // Index: A, B, C  (sorted)
        //
        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        // Columns: X, Y
        //
        assert(result.has_column("X"));
        assert(result.has_column("Y"));

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        assert(X.size() == 3);
        assert(Y.size() == 3);
        assert(std::abs(X[0] - 30.0) < 1e-9);  // A/X: max(10,30)
        assert(std::abs(Y[0] - 20.0) < 1e-9);  // A/Y: max(20)
        assert(std::abs(X[1] - 60.0) < 1e-9);  // B/X: max(50,60)
        assert(std::abs(Y[1] - 40.0) < 1e-9);  // B/Y: max(40)
        assert(std::isnan(X[2]));              // C/X: NaN
        assert(std::abs(Y[2] - 80.0) < 1e-9);  // C/Y: max(70,80)
    }

    // Row from index
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 1, 1, 2, 2 });
        df.load_column<std::string>(
            "region",
            std::vector<std::string>{ "X","Y","X","Y" });
        df.load_column<double>(
            "sales",
            std::vector<double>{ 10, 20, 30, 40 });

        const auto  result {
            df.pivot_table<unsigned long, std::string, double,
                           SumVisitor<double>>(
                DF_INDEX_COL_NAME, "region", "sales",
                SumVisitor<double>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        const auto  &idx { result.get_index() };

        assert(idx.size() == 2);
        assert(idx[0] == 1 && idx[1] == 2);

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        assert(std::abs(X[0] - 10.0) < 1e-9);
        assert(std::abs(Y[0] - 20.0) < 1e-9);
        assert(std::abs(X[1] - 30.0) < 1e-9);
        assert(std::abs(Y[1] - 40.0) < 1e-9);
    }

    // Column from index
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 1, 2, 1, 2 });
        df.load_column<std::string>(
            "dept",
            std::vector<std::string>{ "A","A","B","B" });
        df.load_column<double>(
            "sales",
            std::vector<double>{ 10, 20, 30, 40 });

        const auto  result {
            df.pivot_table<std::string, unsigned long, double,
                           SumVisitor<double>>(
                "dept", DF_INDEX_COL_NAME, "sales",
                SumVisitor<double>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        const auto  &idx { result.get_index() };

        assert(idx.size() == 2);
        assert(idx[0] == "A" && idx[1] == "B");

        // Column names are stringified index values
        //
        assert(result.has_column("1"));
        assert(result.has_column("2"));

        const auto  &c1 { result.get_column<double>("1") };
        const auto  &c2 { result.get_column<double>("2") };

        assert(std::abs(c1[0] - 10.0) < 1e-9);
        assert(std::abs(c2[0] - 20.0) < 1e-9);
        assert(std::abs(c1[1] - 30.0) < 1e-9);
        assert(std::abs(c2[1] - 40.0) < 1e-9);
    }

    // Integer keys
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 0,1,2,3,4,5 });
        df.load_column<int>("row_key", std::vector<int>{ 1,1,2,2,3,3 });
        df.load_column<int>("col_key", std::vector<int>{ 10,20,10,20,10,20 });
        df.load_column<double>("val",  std::vector<double>{ 1,2,3,4,5,6 });

        const auto  result {
            df.pivot_table<int, int, double, SumVisitor<double>>(
                "row_key", "col_key", "val",
                SumVisitor<double>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(result.has_column("10") && result.has_column("20"));

        const auto  &c10 { result.get_column<double>("10") };
        const auto  &c20 { result.get_column<double>("20") };

        assert(std::abs(c10[0] - 1.0) < 1e-9);
        assert(std::abs(c20[0] - 2.0) < 1e-9);
        assert(std::abs(c10[1] - 3.0) < 1e-9);
        assert(std::abs(c20[1] - 4.0) < 1e-9);
        assert(std::abs(c10[2] - 5.0) < 1e-9);
        assert(std::abs(c20[2] - 6.0) < 1e-9);
    }

    // Single per cell
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 0,1,2,3 });
        df.load_column<std::string>(
            "r", std::vector<std::string>{ "A","A","B","B" });
        df.load_column<std::string>(
            "c", std::vector<std::string>{ "X","Y","X","Y" });
        df.load_column<double>(
            "v", std::vector<double>{ 1.5, 2.5, 3.5, 4.5 });

        const auto  result {
            df.pivot_table<std::string, std::string, double,
                           MeanVisitor<double>>(
                "r", "c", "v",
                MeanVisitor<double>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };

        // Each cell is just the single value itself
        //
        assert(std::abs(X[0] - 1.5) < 1e-9);
        assert(std::abs(Y[0] - 2.5) < 1e-9);
        assert(std::abs(X[1] - 3.5) < 1e-9);
        assert(std::abs(Y[1] - 4.5) < 1e-9);
    }

    // Sparse cells
    //
    {
        MyDataFrame df;

        df.load_index(std::vector<unsigned long>{ 0,1,2 });
        df.load_column<std::string>(
            "r", std::vector<std::string>{ "A","B","C" });
        df.load_column<std::string>(
            "c", std::vector<std::string>{ "X","Y","Z" });
        df.load_column<double>(
            "v", std::vector<double>{ 10, 20, 30 });

        const auto  result {
            df.pivot_table<std::string, std::string, double,
                           SumVisitor<double>>(
                "r", "c", "v",
                SumVisitor<double>{ })
        };

        // result.write<std::ostream, double>
        //     (std::cout, io_format::pretty_prt, { .precision = 3 });

        const auto  &idx { result.get_index() };

        assert(idx.size() == 3);
        assert(idx[0] == "A" && idx[1] == "B" && idx[2] == "C");

        // 3x3 result, diagonal populated, off-diagonal NaN
        //
        const auto  &X { result.get_column<double>("X") };
        const auto  &Y { result.get_column<double>("Y") };
        const auto  &Z { result.get_column<double>("Z") };

        assert(std::abs(X[0] - 10.0) < 1e-9);
        assert(std::isnan(Y[0]));
        assert(std::isnan(Z[0]));

        assert(std::isnan(X[1]));
        assert(std::abs(Y[1] - 20.0) < 1e-9);
        assert(std::isnan(Z[1]));

        assert(std::isnan(X[2]));
        assert(std::isnan(Y[2]));
    }
}

// -----------------------------------------------------------------------------

static void test_JarqueBeraTestVisitor()  {

    std::cout << "\nTesting JarqueBeraTestVisitor{ } ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    constexpr std::size_t   col_s { 1000 };

    std::vector<unsigned long>  idx(col_s);

    std::iota(idx.begin(), idx.end(), 0UL);

    MyDataFrame df;

    df.load_index(std::move(idx));

    RandGenParams<double>   p;

    p.seed = 123;

    // Normal data
    //
    {
        df.load_column<double>("x", gen_normal_dist<double>(col_s, p));

        jb_test_v<double>   jb;

        df.single_act_visit<double>("x", jb);

        // JB statistic should be modest for a large normal sample
        // (typical range 0–6 for n=1000 normal data)
        //
        assert(std::abs(jb.get_result() - 1.63788) < 1e-5);

        // p-value well above any conventional significance level
        //
        assert(std::abs(jb.get_p_value() - 0.440899) < 1e-6);

        // Skew and excess kurtosis individually small
        //
        assert(std::abs(jb.get_skewness() - -0.090082) < 1e-6);
        assert(std::abs(jb.get_excess_kurtosis() - 0.082764) < 1e-6);
    }

    // Uniform data
    //
    {
        df.load_column<double>("y", gen_uniform_real_dist<double>(col_s, p));

        jb_test_v<double>   jb;

        df.single_act_visit<double>("y", jb);

        assert(std::abs(jb.get_result() - 0.0) < 1e-9);
        assert(std::abs(jb.get_p_value() - 1.0) < 1e-9);
        assert(std::abs(jb.get_skewness() - 0.0) < 1e-9);
        assert(std::abs(jb.get_excess_kurtosis() - 0.0) < 1e-9);
    }

    // Laplace data
    //
    {
        RandGenParams<bool>   p2;

        p2.seed = p.seed;

        const auto          expon { gen_exponential_dist<double>(col_s, p) };
        const auto          berno { gen_bernoulli_dist(col_s, p2) };
        std::vector<double> col (col_s);

        for (std::size_t i { 0 }; auto &val : col)  {
            val = (berno[i] ? 1.0 : -1.0) * expon[i];
            i += 1;
        }
        df.load_column<double>("z", std::move(col));

        jb_test_v<double>   jb;

        df.single_act_visit<double>("z", jb);

        assert(std::abs(jb.get_result() - 392.055) < 1e-3);
        assert(std::abs(jb.get_p_value() - 7.35058e-86) < 1e-80);
        assert(std::abs(jb.get_skewness() - -1.15893) < 1e-4);

        // Excess kurtosis should be clearly positive (leptokurtic)
        //
        assert(std::abs(jb.get_excess_kurtosis() - 2.0092) < 1e-4);
    }

    // Formula
    //
    {
        // Data: n=200 points split evenly between +1 and −1.
        // S = 0  (symmetric)
        // Raw kurtosis = E[X⁴]/E[X²]² = 1/1 = 1  → excess kurtosis = 1−3 = −2
        // JB = (200/6) * (0 + 4/4) = 200/6 ≈ 33.333
        //
        const std::size_t   col_s { 200 };
        std::vector<double> col(col_s);

        for (std::size_t i = 0; i < col_s; ++i)
            col[i] = (i % 2 == 0) ? 1.0 : -1.0;

        df.load_column<double>("A", std::move(col));

        jb_test_v<double>   jb;

        df.single_act_visit<double>("A", jb);

        assert(std::abs(jb.get_result() - 0.0) < 1e-9);
        assert(std::abs(jb.get_p_value() - 1.0) < 1e-9);
        assert(std::abs(jb.get_skewness() - 0.0) < 1e-9);
        assert(std::abs(jb.get_excess_kurtosis() - 0.0) < 1e-9);
    }
}

// -----------------------------------------------------------------------------

static void test_LjungBoxTestVisitor()  {

    std::cout << "\nTesting LjungBoxTestVisitor{ } ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    constexpr std::size_t   col_s { 500 };

    std::vector<unsigned long>  idx(col_s);

    std::iota(idx.begin(), idx.end(), 0UL);

    MyDataFrame df;

    df.load_index(std::move(idx));

    RandGenParams<double>   p;

    p.seed = 123;

    // White noise
    //
    {
        df.load_column<double>("x", gen_normal_dist<double>(col_s, p));

        lb_test_v<double>   lb { 10 };

        df.single_act_visit<double>("x", lb);

        assert(std::abs(lb.get_result() - 9.70203) < 0.00001);

        // Overall Q(10) should be small, p-value large
        //
        assert(std::abs(lb.get_p_value() - 0.467012) < 0.000001);

        // Result vectors are the right length
        //
        assert(lb.get_acf().size() == 10);
        assert(lb.get_q_stats().size() == 10);
        assert(lb.get_p_values().size() == 10);

        // All per-lag ACF should be close to 0
        //
        for (const auto &rho : lb.get_acf())
            assert(std::abs(rho) < 0.2);
        assert(std::abs(lb.get_acf()[0] - -0.025529) < 0.000001);
        assert(std::abs(lb.get_acf()[5] - -0.009931) < 0.000001);
        assert(std::abs(lb.get_acf()[9] - 0.072755) < 0.000001);

        // Q stats must be non-decreasing
        //
        for (std::size_t i = 1; i < lb.get_q_stats().size(); ++i)
            assert(lb.get_q_stats()[i] >= lb.get_q_stats()[i - 1]);
        assert(std::abs(lb.get_q_stats()[0] - 0.327812) < 0.000001);
        assert(std::abs(lb.get_q_stats()[5] - 1.28697) < 0.00001);
        assert(std::abs(lb.get_q_stats()[9] - 9.70203) < 0.00001);
    }

    // AR -- strong autocorrelation at lag 1
    //
    {
        auto                y = gen_normal_dist<double>(col_s, p);
        constexpr double    phi { 0.9 };

        for (std::size_t i = 1; i < col_s; ++i)
            y[i] = phi * y[i - 1] + y[i];

        df.load_column<double>("y", std::move(y));

        lb_test_v<double>   lb { 10 };

        df.single_act_visit<double>("y", lb);

        // Strong autocorrelation — Q should be very large, p near 0
        //
        assert(std::abs(lb.get_result() - 1305.37) < 0.01);
        assert(lb.get_p_value() < 1e-20);

        // ACF at lag 1 should be strongly positive
        //
        assert(std::abs(lb.get_acf()[0] - 0.874076) < 0.000001);
    }

    // Sine wave
    //
    {
        std::vector<double> z(col_s);
        constexpr double    freq { 0.1 };

        for (std::size_t i = 0; i < col_s; ++i)
            z[i] = std::sin(2.0 * M_PI * freq * static_cast<double>(i));

        df.load_column<double>("z", std::move(z));

        lb_test_v<double>   lb { 20 };

        df.single_act_visit<double>("z", lb);

        // Strong autocorrelation — Q should be very large, p near 0
        //
        assert(std::abs(lb.get_result() - 4909.6) < 0.1);
        assert(lb.get_p_value() < 1e-20);
    }

    // Formula wave
    //
    {
        std::vector<double> A(col_s);

        for (std::size_t i = 0; i < col_s; ++i)
            A[i] = (i % 2 == 0) ? 1.0 : -1.0;

        df.load_column<double>("A", std::move(A));

        LjungBoxTestVisitor<double> lb { 2 };

        df.single_act_visit<double>("A", lb);

        assert(std::abs(lb.get_result() - 1000.99) < 0.01);
        assert(lb.get_p_value() < 1e-20);

        // rho(1) should be very close to −1
        // rho(2) should be very close to +1
        //
        assert(lb.get_acf().size() == 2);
        assert(std::abs(lb.get_acf()[0] - -0.998) < 0.001);
        assert(std::abs(lb.get_acf()[1] - 0.996) < 0.001);

        // Verify Q(2) against the formula
        //
        const double nd { static_cast<double>(col_s) };
        const double r1 { lb.get_acf()[0] };
        const double r2 { lb.get_acf()[1] };
        const double q_exp {
            nd * (nd + 2.0) * (r1 * r1 / (nd - 1.0) + r2 * r2 / (nd - 2.0))
        };

        assert(std::abs(lb.get_q_stats()[1] - q_exp) < 1e-9);

        // p-value = chi2_survival(dof/2=1, Q/2) for each cumulative Q
        // For lag 1: dof=1, Q(1) large → p near 0
        //
        assert(lb.get_p_values()[0] < 1e-10);
        assert(lb.get_p_values()[1] < 1e-10);
    }

    // Degree of freedom (DOF) adjustment for ARIMA resdiduals
    //
    {
        df.load_column<double>("B", gen_normal_dist<double>(col_s, p));

        lb_test_v<double>   lb_raw { 10, 0 };  // no adjustment
        lb_test_v<double>   lb_adj { 10, 1 };  // AR(1) residuals

        df.single_act_visit<double>("B", lb_raw);
        df.single_act_visit<double>("B", lb_adj);

        // Q stats must be identical (adjustment only affects dof, not Q)
        //
        const auto  &qs_raw { lb_raw.get_q_stats() };
        const auto  &qs_adj { lb_adj.get_q_stats() };

        for (std::size_t i = 0; i < 10; ++i)
            assert(std::abs(qs_raw[i] - qs_adj[i]) < 1e-12);

        // ACF must be identical too
        //
        const auto  &acf_raw { lb_raw.get_acf() };
        const auto  &acf_adj { lb_adj.get_acf() };

        for (std::size_t i = 0; i < 10; ++i)
            assert(std::abs(acf_raw[i] - acf_adj[i]) < 1e-12);

        // p-values differ because dof differs.
        // With dof_adjust=1 each lag k uses dof=k-1 instead of dof=k.
        // For moderate Q values the χ^2(k-1) right tail is lighter than the
        // χ^2(k) right tail, so a smaller dof gives a smaller p-value.
        // At lag 1 dof clamps to 1 in both cases, so lag 0 is excluded.
        //
        const auto  &pv_raw { lb_raw.get_p_values() };
        const auto  &pv_adj { lb_adj.get_p_values() };

        for (std::size_t i = 1; i < 10; ++i)
            assert(pv_adj[i] < pv_raw[i]);

        assert(lb_raw.get_p_value() > lb_adj.get_p_value());
        assert(std::abs(lb_raw.get_result() - lb_adj.get_result()) < 1e-12);
    }
}

// -----------------------------------------------------------------------------

static void test_DurbinWatsonVisitor()  {

    std::cout << "\nTesting DurbinWatsonVisitor{ } ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    constexpr std::size_t   col_s { 500 };

    std::vector<unsigned long>  idx(col_s);

    std::iota(idx.begin(), idx.end(), 0UL);

    MyDataFrame df;

    df.load_index(std::move(idx));

    RandGenParams<double>   p;

    p.seed = 123;

    // Formula
    //
    {
        df.load_column("x", std::vector<double>{ 1.0, -1.0, 1.0, -1.0 },
                       nan_policy::dont_pad_with_nans);

        dw_test_v<double>   dw;

        df.single_act_visit<double>("x", dw);

        assert(std::abs(dw.get_result() - 3.0) < 1e-12);
        assert(std::abs(dw.get_rho() - -0.5) < 1e-12);

        // d=3 with default bounds (dL=1.5, dU=2.5): 4−dL=2.5, 4−dU=1.5
        // d=3 > 4−dU=1.5 and d=3 < 4−dL=2.5? No: 3 > 2.5 = 4−dL
        // So d > 4−dL → negative_autocorr
        //
        assert(dw.get_result_category() == dw_autocorr_t::negative_autocorr);
    }

    // White noise
    //
    {
        df.load_column("y", gen_normal_dist<double>(col_s, p),
                       nan_policy::dont_pad_with_nans);

        dw_test_v<double>   dw;

        df.single_act_visit<double>("y", dw);

        // d should be close to 2 for white noise
        //
        assert(std::abs(dw.get_result() - 2.04668) < 1e-5);
        assert(std::abs(dw.get_rho() - -0.0233394) < 1e-7);
        assert(dw.get_result_category() == dw_autocorr_t::inconclusive);
    }

    // Positive correlation
    //
    {
        auto                z = gen_normal_dist<double>(col_s, p);
        constexpr double    phi { 0.9 };

        for (std::size_t i = 1; i < col_s; ++i)
            z[i] = phi * z[i - 1] + z[i];

        df.load_column("z", std::move(z));

        dw_test_v<double>   dw;

        df.single_act_visit<double>("z", dw);

        assert(std::abs(dw.get_result() - 0.248076) < 1e-6);
        assert(std::abs(dw.get_rho() - 0.875962) < 1e-6);
        assert(dw.get_result_category() == dw_autocorr_t::positive_autocorr);
    }

    // Negative correlation
    //
    {
        auto                A = gen_normal_dist<double>(col_s, p);
        constexpr double    phi { -0.9 };

        for (std::size_t i = 1; i < col_s; ++i)
            A[i] = phi * A[i - 1] + A[i];

        df.load_column<double>("A", std::move(A));

        dw_test_v<double>   dw;

        df.single_act_visit<double>("A", dw);

        // d should be well above 4−dL = 2.5
        //
        assert(std::abs(dw.get_result() - 3.86027) < 1e-5);
        assert(std::abs(dw.get_rho() - -0.930136) < 1e-6);
        assert(dw.get_result_category() == dw_autocorr_t::negative_autocorr);
    }

    // Rho formula
    //
    {
        for (double phi : { -0.8, -0.4, 0.0, 0.4, 0.8 })  {
            auto    B = gen_normal_dist<double>(col_s, p);

            for (std::size_t i = 1; i < col_s; ++i)
                B[i] = phi * B[i - 1] + B[i];

            df.load_column("B", std::move(B));

            dw_test_v<double>   dw;

            df.single_act_visit<double>("B", dw);

            const double    d { dw.get_result() };
            const double    rho { dw.get_rho() };

            // The visitor formula: rho = 1 − d/2
            //
            assert(std::abs(rho - (1.0 - d / 2.0)) < 1e-12);

            // For large n, sample rho ≈ phi, so d ≈ 2(1−phi)
            //
            assert(std::abs(d - 2.0 * (1.0 - phi)) < 0.3);
        }
    }

    // Custom bounds
    //
    {
        auto                C = gen_normal_dist<double>(col_s, p);
        constexpr double    phi { 0.6 };

        for (std::size_t i = 1; i < col_s; ++i)
            C[i] = phi * C[i - 1] + C[i];

        df.load_column<double>("C", std::move(C));

        DurbinWatsonVisitor<double> dw_default { 1.5, 2.5 };
        DurbinWatsonVisitor<double> dw_tight { 0.5, 0.6 };

        df.single_act_visit<double>("C", dw_default);
        df.single_act_visit<double>("C", dw_tight);

        // d value must be identical (bounds don't affect the statistic)
        //
        assert((std::abs(dw_default.get_result() -
                         dw_tight.get_result()) < 1e-12));
        assert((std::abs(dw_default.get_rho() -
                         dw_tight.get_rho()) < 1e-12));

        // Under default bounds d<1.5 → positive_autocorr
        //
        assert((dw_default.get_result_category() ==
                dw_autocorr_t::positive_autocorr));

        // Under very tight bounds d>dU=0.6 and d<4−dU=3.4 → no_autocorr
        //
        assert(dw_tight.get_result_category() == dw_autocorr_t::no_autocorr);
    }

    // Zero residuals
    //
    {
        df.load_column<double>("D", std::vector<double>(col_s, 0));

        DurbinWatsonVisitor<double> dw;

        df.single_act_visit<double>("D", dw);

        assert(dw.get_result() == 2.0);
        assert(dw.get_rho() == 0.0);
        assert(dw.get_result_category() == dw_autocorr_t::no_autocorr);
    }
}

// -----------------------------------------------------------------------------

static void test_SilhouetteScoreVisitor()  {

    std::cout << "\nTesting SilhouetteScoreVisitor{ } ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    constexpr std::size_t   col_s { 100 };

    std::vector<unsigned long>  idx(col_s);

    std::iota(idx.begin(), idx.end(), 0UL);

    MyDataFrame df;

    df.load_index(std::move(idx));

    // Two clusters
    //
    {
        df.load_column("x1", std::vector<double>{ 0.0, 1.0, 10.0, 11.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl1", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        sil_score_v<double> sil;

        df.single_act_visit<double, long>("x1", "lbl1", sil);

        // All scores should be > 0.9 for perfectly separated clusters
        //
        for (const auto &s : sil.get_result())
            assert(s > 0.9);

        // Mean score ≈ same as individual scores
        //
        assert(sil.get_mean_score() > 0.9);
    }

    // Three tight clusters
    //
    {
        df.load_column("x2",
                        std::vector<double>{ 0.0, 0.1, 0.2, 10.0, 10.1,
                                             10.2, 20.0, 20.1, 20.2 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl2", std::vector<long>{ 0, 0, 0, 1, 1, 1, 2, 2, 2 },
                       nan_policy::dont_pad_with_nans);

        sil_score_v<double> sil;

        df.single_act_visit<double, long>("x2", "lbl2", sil);

        for (const auto &s : sil.get_result())
            assert(s > 0.99);
        assert(sil.get_mean_score() > 0.99);
    }

    // Overlapping clusters
    //
    {
        df.load_column("x3", std::vector<double>{ 0.0, 5.0, 2.0, 10.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl3", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        sil_score_v<double> sil;

        df.single_act_visit<double, long>("x3", "lbl3", sil);

        assert(std::abs(sil.get_result()[0] - 0.519231) < 1e-6);
        assert(std::abs(sil.get_result()[1] - -0.32) < 1e-2);
        assert(std::abs(sil.get_result()[2] - -0.898438) < 1e-6);
        assert(std::abs(sil.get_result()[3] - -0.0234375) < 1e-7);
        assert(sil.get_mean_score() < 0.0);
    }

    // Singleton clusters
    //
    {
        df.load_column("x4", std::vector<double>{ 0.0,  10.0, 11.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl4", std::vector<long>{ 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        sil_score_v<double> sil;

        df.single_act_visit<double, long>("x4", "lbl4", sil);

        // Point 0 is a singleton -> score must be 0
        //
        assert(std::abs(sil.get_result()[0] - 0.0) < 1e-10);

        // Points 1 and 2 are in a proper cluster -> positive scores
        //
        assert(std::abs(sil.get_result()[1] - 0.99) < 1e-3);
        assert(std::abs(sil.get_result()[2] - 0.991736) < 1e-6);
        assert(std::abs(sil.get_mean_score() - 0.660579) < 1e-6);
    }

    // Noise points
    //
    {
        df.load_column("x5", std::vector<double>{ 5.0, 0.0, 1.0, 10.0, 11.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl5", std::vector<long>{ -1, 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        sil_score_v<double> sil;

        df.single_act_visit<double, long>("x5", "lbl5", sil);

        // Noise point score must be 0
        //
        assert(std::abs(sil.get_result()[0] - 0.0) < 1e-10);

        // Non-noise points should have high scores (well-separated clusters)
        //
        assert(std::abs(sil.get_result()[1] - 0.99095) < 1e-5);
        assert(std::abs(sil.get_result()[2] - 0.98895) < 1e-5);
        assert(std::abs(sil.get_result()[3] - 0.98895) < 1e-5);
        assert(std::abs(sil.get_result()[4] - 0.99095) < 1e-5);
        assert(std::abs(sil.get_mean_score() - 0.98995) < 1e-5);
    }

    // Single cluster
    //
    {
        df.load_column("x6", std::vector<double>{ 1.0, 2.0, 3.0, 4.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl6", std::vector<long>{ 0, 0, 0, 0 },
                       nan_policy::dont_pad_with_nans);

        sil_score_v<double> sil;

        df.single_act_visit<double, long>("x6", "lbl6", sil);

        // Cannot compute silhouette without at least 2 clusters
        //
        assert(sil.get_mean_score() == 0.0);
        for (const auto s : sil.get_result())
            assert(s == 0.0);
    }

    // Score range
    //
    {
        df.load_column("x6", std::vector<double>{ 1,2,3,4,5,6,7,8,9,10,11,12 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl6", std::vector<long>{ 0,0,0,0,1,1,1,1,2,2,2,2},
                       nan_policy::dont_pad_with_nans);

        sil_score_v<double> sil;

        df.single_act_visit<double, long>("x6", "lbl6", sil);

        for (const auto s : sil.get_result())
            assert(s > 0.1 && s < 1.0);
        assert(std::abs(sil.get_mean_score() - 0.703015) < 1e-6);
    }

    // Custom distance
    //
    {
        df.load_column("x7", std::vector<double>{ 0.0, 1.0, 10.0, 11.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl7", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        // Absolute difference: dist(x, y) = |x - y|
        //
        auto               abs_dist =
            [](const double &x, const double &y) -> double  {
                return (std::abs(x - y));
            };
        sil_score_v<double> sil { abs_dist };

        df.single_act_visit<double, long>("x7", "lbl7", sil);

        assert(std::abs(sil.get_result()[0] - 0.904762) < 1e-6);
        assert(std::abs(sil.get_result()[1] - 0.894737) < 1e-6);
        assert(std::abs(sil.get_result()[2] - 0.894737) < 1e-6);
        assert(std::abs(sil.get_result()[3] - 0.904762) < 1e-6);
        assert(std::abs(sil.get_mean_score() - 0.899749) < 1e-6);
    }
}

// -----------------------------------------------------------------------------

static void test_DaviesBouldinIndexVisitor()  {

    std::cout << "\nTesting DaviesBouldinIndexVisitor{ } ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    constexpr std::size_t   col_s { 100 };

    std::vector<unsigned long>  idx(col_s);

    std::iota(idx.begin(), idx.end(), 0UL);

    MyDataFrame df;

    df.load_index(std::move(idx));

    // Analytics
    //
    {
        df.load_column("x1", std::vector<double>{ 0.0, 2.0, 10.0, 12.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl1", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        db_index_v<double>  db;

        df.single_act_visit<double, long>("x1", "lbl1", db);

        // DB index
        //
        assert(std::abs(db.get_result() - 0.02) < 1e-9);

        assert(db.get_scatter().size() == 2);
        assert(std::abs(db.get_scatter()[0] - 1.0) < 1e-9);
        assert(std::abs(db.get_scatter()[1] - 1.0) < 1e-9);

        assert(db.get_worst_ratio().size() == 2);
        assert(std::abs(db.get_worst_ratio()[0] - 0.02) < 1e-9);
        assert(std::abs(db.get_worst_ratio()[1] - 0.02) < 1e-9);

        assert(db.get_centroids().size() == 2);
        assert(std::abs(db.get_centroids()[0] - 1.0)  < 1e-9);
        assert(std::abs(db.get_centroids()[1] - 11.0) < 1e-9);
    }

    // Well separated
    //
    {
        df.load_column("x2", std::vector<double>{ 0.0, 0.1, 0.2,
                                                  100.0, 100.1, 100.2,
                                                  200.0, 200.1, 200.2 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl2", std::vector<long>{ 0, 0, 0, 1, 1, 1, 2, 2, 2 },
                       nan_policy::dont_pad_with_nans);

        db_index_v<double>  db;

        df.single_act_visit<double, long>("x2", "lbl2", db);

        // Three clusters very far apart, each tight -> DB close to 0
        //
        assert(std::abs(db.get_result() - 1.33333e-06) < 1e-9);

        assert(db.get_scatter().size() == 3);
        assert(std::abs(db.get_scatter()[0] - 0.00666667) < 1e-8);
        assert(std::abs(db.get_scatter()[2] - 0.00666667) < 1e-8);

        assert(db.get_worst_ratio().size() == 3);
        assert(std::abs(db.get_worst_ratio()[0] - 1.33333e-06) < 1e-9);
        assert(std::abs(db.get_worst_ratio()[2] - 1.33333e-06) < 1e-9);

        assert(db.get_centroids().size() == 3);
        assert(std::abs(db.get_centroids()[0] - 0.1)  < 1e-6);
        assert(std::abs(db.get_centroids()[1] - 100.1) < 1e-6);
        assert(std::abs(db.get_centroids()[2] - 200.1) < 1e-6);
    }

    // Poor separation
    //
    {
        df.load_column("x3",
                       std::vector<double>{ 0, 10, 20, 30, 1, 11, 21, 31 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl3", std::vector<long>{ 0, 0, 0, 0, 1, 1, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        db_index_v<double>  db;

        df.single_act_visit<double, long>("x3", "lbl3", db);

        // Centroids: u0=15, u1=16 -> very close, large scatter -> large DB
        //
        assert(std::abs(db.get_result() - 250.0) < 1e-6);

        assert(db.get_scatter().size() == 2);
        assert(std::abs(db.get_scatter()[0] - 125.0) < 1e-6);
        assert(std::abs(db.get_scatter()[1] - 125.0) < 1e-6);

        assert(db.get_worst_ratio().size() == 2);
        assert(std::abs(db.get_worst_ratio()[0] - 250.0) < 1e-6);
        assert(std::abs(db.get_worst_ratio()[1] - 250.0) < 1e-6);

        assert(db.get_centroids().size() == 2);
        assert(std::abs(db.get_centroids()[0] - 15.0)  < 1e-6);
        assert(std::abs(db.get_centroids()[1] - 16.0) < 1e-6);
    }

    // Singleton
    //
    {
        df.load_column("x4", std::vector<double>{ 5.0, 10.0, 12.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl4", std::vector<long>{ 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        db_index_v<double>  db;

        df.single_act_visit<double, long>("x4", "lbl4", db);

        assert(std::abs(db.get_result() - 0.0277778) < 1e-7);

        assert(db.get_scatter().size() == 2);
        assert(std::abs(db.get_scatter()[0] - 0.0) < 1e-9);
        assert(std::abs(db.get_scatter()[1] - 1.0) < 1e-9);

        assert(db.get_worst_ratio().size() == 2);
        assert(std::abs(db.get_worst_ratio()[0] - 0.0277778) < 1e-7);
        assert(std::abs(db.get_worst_ratio()[1] - 0.0277778) < 1e-7);

        assert(db.get_centroids().size() == 2);
        assert(std::abs(db.get_centroids()[0] - 5.0)  < 1e-6);
        assert(std::abs(db.get_centroids()[1] - 11.0) < 1e-6);
    }

    // Noise points
    //
    {
        df.load_column("x5_noise",
                       std::vector<double>{ 5.0, 0.0, 2.0, 10.0, 12.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl5_noise", std::vector<long>{ -1, 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("x5_clean",
                       std::vector<double>{ 0.0, 2.0, 10.0, 12.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl5_clean", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        db_index_v<double>  db_noise;
        db_index_v<double>  db_clean;

        df.single_act_visit<double, long>("x5_noise", "lbl5_noise", db_noise);
        df.single_act_visit<double, long>("x5_clean", "lbl5_clean", db_clean);

        assert(std::abs(db_noise.get_result() - db_clean.get_result()) < 1e-9);
        assert(std::abs(db_noise.get_centroids()[0] -
                        db_clean.get_centroids()[0]) < 1e-9);
        assert(std::abs(db_noise.get_centroids()[1] -
                        db_clean.get_centroids()[1]) < 1e-9);
        assert(std::abs(db_noise.get_worst_ratio()[0] -
                        db_clean.get_worst_ratio()[0]) < 1e-9);
        assert(std::abs(db_noise.get_worst_ratio()[1] -
                        db_clean.get_worst_ratio()[1]) < 1e-9);
    }

    // Custom distance
    //
    {
        df.load_column("x6", std::vector<double>{ 0.0, 2.0, 10.0, 12.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl6", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        // Absolute difference: dist(x, y) = |x - y|
        //
        auto                                abs_dist =
            [](const double &x, const double &y) -> double  {
                return (std::abs(x - y));
            };
        DaviesBouldinIndexVisitor<double>   db_def;
        DaviesBouldinIndexVisitor<double>   db_abs { abs_dist };

        df.single_act_visit<double, long>("x6", "lbl6", db_def);
        df.single_act_visit<double, long>("x6", "lbl6", db_abs);

        assert(std::abs(db_def.get_result() - 0.02) < 1e-6);
        assert(std::abs(db_abs.get_result() - 0.2) < 1e-6);

        assert(std::abs(db_def.get_scatter()[0] - 1.0) < 1e-6);
        assert(std::abs(db_def.get_scatter()[1] - 1.0) < 1e-6);
        assert(std::abs(db_abs.get_scatter()[0] - 1.0) < 1e-6);
        assert(std::abs(db_abs.get_scatter()[1] - 1.0) < 1e-6);

        assert(std::abs(db_def.get_worst_ratio()[0] - 0.02) < 1e-6);
        assert(std::abs(db_def.get_worst_ratio()[1] - 0.02) < 1e-6);
        assert(std::abs(db_abs.get_worst_ratio()[0] - 0.2) < 1e-6);
        assert(std::abs(db_abs.get_worst_ratio()[1] - 0.2) < 1e-6);

        assert(std::abs(db_def.get_centroids()[0] - 1.0) < 1e-6);
        assert(std::abs(db_def.get_centroids()[1] - 11.0) < 1e-6);
        assert(std::abs(db_abs.get_centroids()[0] - 1.0) < 1e-6);
        assert(std::abs(db_abs.get_centroids()[1] - 11.0) < 1e-6);
    }

    // Multidimensional
    //
    {
        using point_t = std::array<double, 2>;
        using point_vec = std::vector<point_t>;

        point_vec           pts = {
            point_t{ 0.0, 0.0 }, point_t{ 1.0, 0.0 }, point_t{ 0.0, 1.0 },
            point_t{ 10.0, 10.0 }, point_t{ 11.0, 10.0 }, point_t{ 10.0, 11.0 }
        };
        std::vector<long>   lbl = { 0, 0, 0, 1, 1, 1 };

        df.load_column("x7", std::move(pts), nan_policy::dont_pad_with_nans);
        df.load_column("lbl7", std::move(lbl), nan_policy::dont_pad_with_nans);

        db_index_v<point_t> db;

        df.single_act_visit<point_t, long>("x7", "lbl7", db);
    }
}

// -----------------------------------------------------------------------------

static void test_CalinskiHarabaszVisitor()  {

    std::cout << "\nTesting CalinskiHarabaszVisitor{ } ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    constexpr std::size_t   col_s { 100 };

    std::vector<unsigned long>  idx(col_s);

    std::iota(idx.begin(), idx.end(), 0UL);

    MyDataFrame df;

    df.load_index(std::move(idx));

    // Analytics
    //
    {
        df.load_column("x1", std::vector<double>{ 0.0, 2.0, 10.0, 12.0 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl1", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        ch_index_v<double>  ch;

        df.single_act_visit<double, long>("x1", "lbl1", ch);

        // CH = 50
        //
        assert(std::abs(ch.get_result() - 50.0) < 1e-9);

        assert(std::abs(ch.get_bgss() - 100.0) < 1e-9);
        assert(std::abs(ch.get_wgss() - 4.0) < 1e-9);

        // Per-cluster WGSS: C0=2, C1=2
        //
        assert(ch.get_cluster_wgss().size() == 2);
        assert(std::abs(ch.get_cluster_wgss()[0] - 2.0) < 1e-9);
        assert(std::abs(ch.get_cluster_wgss()[1] - 2.0) < 1e-9);

        assert(ch.get_centroids().size() == 2);
        assert(std::abs(ch.get_centroids()[0] - 1.0)  < 1e-9);
        assert(std::abs(ch.get_centroids()[1] - 11.0) < 1e-9);

        assert(std::abs(ch.get_global_centroid() - 6.0) < 1e-9);
    }

    // Well separated
    //
    {
        df.load_column("x2",
                       std::vector<double>{ 0.0, 0.1, 0.2,
                                            100.0, 100.1, 100.2,
                                            200.0, 200.1, 200.2 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl2",
                       std::vector<long>{ 0, 0, 0,  1, 1, 1,  2, 2, 2 },
                       nan_policy::dont_pad_with_nans);

        ch_index_v<double>  ch;

        df.single_act_visit<double, long>("x2", "lbl2", ch);

        // High BGSS relative to WGSS → very large CH
        //
        assert(std::abs(ch.get_result() - 3e+06) < 1e-6);

        assert(std::abs(ch.get_bgss() - 60000.0) < 1e-5);
        assert(std::abs(ch.get_wgss() - 0.06) < 1e-5);

        assert(ch.get_cluster_wgss().size() == 3);
        assert(std::abs(ch.get_cluster_wgss()[0] - 0.02) < 1e-6);
        assert(std::abs(ch.get_cluster_wgss()[1] - 0.02) < 1e-6);
        assert(std::abs(ch.get_cluster_wgss()[2] - 0.02) < 1e-6);

        assert(ch.get_centroids().size() == 3);
        assert(std::abs(ch.get_centroids()[0] - 0.1)  < 1e-6);
        assert(std::abs(ch.get_centroids()[1] - 100.1) < 1e-6);
        assert(std::abs(ch.get_centroids()[2] - 200.1) < 1e-6);

        assert(std::abs(ch.get_global_centroid() - 100.1) < 1e-6);
    }

    // Poor separation
    //
    {
        df.load_column("x3",
                       std::vector<double>{ 0, 10, 20, 30, 1, 11, 21, 31 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lbl3", std::vector<long>{ 0, 0, 0, 0, 1, 1, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        ch_index_v<double>  ch;

        df.single_act_visit<double, long>("x3", "lbl3", ch);

        // BGSS tiny (centroids near each other), WGSS large -> small CH
        //
        assert(std::abs(ch.get_result() - 0.012) < 1e-6);

        assert(std::abs(ch.get_bgss() - 2.0) < 1e-9);
        assert(std::abs(ch.get_wgss() - 1000.0) < 1e-9);

        assert(ch.get_cluster_wgss().size() == 2);
        assert(std::abs(ch.get_cluster_wgss()[0] - 500.0) < 1e-6);
        assert(std::abs(ch.get_cluster_wgss()[1] - 500.0) < 1e-6);

        assert(ch.get_centroids().size() == 2);
        assert(std::abs(ch.get_centroids()[0] - 15.0)  < 1e-6);
        assert(std::abs(ch.get_centroids()[1] - 16.0) < 1e-6);

        assert(std::abs(ch.get_global_centroid() - 15.5) < 1e-6);
    }

    // Monotonicity
    //
    {
        // Same centroid positions, decreasing within-cluster spread
        //
        df.load_column("lose_x", std::vector<double>{ 0, 4, 20, 24 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("lose_lbl", std::vector<long>{ 0, 0,  1,  1 },
                       nan_policy::dont_pad_with_nans);

        df.load_column("tight_x", std::vector<double>{ 1, 3, 21, 23 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("tight_lbl", std::vector<long>{ 0, 0,  1,  1 },
                       nan_policy::dont_pad_with_nans);

        df.load_column("vtight_x", std::vector<double>{ 1.9, 2.1, 21.9, 22.1 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("vtight_lbl", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        ch_index_v<double>  ch_l, ch_t, ch_v;

        df.single_act_visit<double, long>("lose_x", "lose_lbl", ch_l);
        df.single_act_visit<double, long>("tight_x", "tight_lbl", ch_t);
        df.single_act_visit<double, long>("vtight_x", "vtight_lbl", ch_v);

        // Tighter clusters -> higher CH
        //
        assert(ch_l.get_result() < ch_t.get_result());
        assert(ch_t.get_result() < ch_v.get_result());
    }

    // Separation monotonicity
    //
    {
        // Same within-cluster spread, increasing inter-centroid gap
        //
        df.load_column("close_x", std::vector<double>{ 0, 1, 3, 4 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("close_lbl", std::vector<long>{ 0, 0,  1,  1 },
                       nan_policy::dont_pad_with_nans);

        df.load_column("far_x", std::vector<double>{ 0, 1, 10, 11 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("far_lbl", std::vector<long>{ 0, 0,  1,  1 },
                       nan_policy::dont_pad_with_nans);

        df.load_column("vfar_x", std::vector<double>{ 0, 1, 100, 101 },
                       nan_policy::dont_pad_with_nans);
        df.load_column("vfar_lbl", std::vector<long>{ 0, 0, 1, 1 },
                       nan_policy::dont_pad_with_nans);

        ch_index_v<double>  ch_c, ch_f, ch_v;

        df.single_act_visit<double, long>("close_x", "close_lbl", ch_c);
        df.single_act_visit<double, long>("far_x", "far_lbl", ch_f);
        df.single_act_visit<double, long>("vfar_x", "vfar_lbl", ch_v);

        // More separation -> higher BGSS, same WGSS -> higher CH
        //
        assert(ch_c.get_result() < ch_f.get_result());
        assert(ch_f.get_result() < ch_v.get_result());
    }

    // Multidimensional
    //
    {
        using point_t = std::array<double, 2>;
        using point_vec = std::vector<point_t>;

        point_vec           pts = {
            point_t{ 0.0, 0.0 }, point_t{ 1.0, 0.0 }, point_t{ 0.0, 1.0 },
            point_t{ 10.0, 10.0 }, point_t{ 11.0, 10.0 }, point_t{ 10.0, 11.0 }
        };
        std::vector<long>   lbl = { 0, 0, 0, 1, 1, 1 };

        df.load_column("x4", std::move(pts), nan_policy::dont_pad_with_nans);
        df.load_column("lbl4", std::move(lbl), nan_policy::dont_pad_with_nans);

        CalinskiHarabaszVisitor<point_t>    ch;

        df.single_act_visit<point_t, long>("x4", "lbl4", ch);

        const double    n { 6.0 }, k { 2.0 };
        const double    bgss { ch.get_bgss() };
        const double    wgss { ch.get_wgss() };

        // Hand verify CH from components
        //
        const double    ch_from_parts { (bgss / (k - 1.0)) / (wgss / (n - k)) };

        assert(std::abs(ch.get_result() - ch_from_parts) < 1e-9);

        // BGSS and WGSS must both be positive
        //
        assert(bgss > 0.0);
        assert(wgss > 0.0);

        // Per-cluster WGSS must sum to total WGSS
        //
        double  sum_cluster { 0.0 };

        for (const auto &w : ch.get_cluster_wgss())
            sum_cluster += w;
        assert(std::abs(sum_cluster - wgss) < 1e-9);
    }
}

// -----------------------------------------------------------------------------

int main(int, char *[])  {

    ULDataFrame::set_optimum_thread_level();

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
    test_gen_join2();
    test_ChiSquaredTestVisitor();
    test_get_matrix();
    test_get_matrix_2();
    test_ARIMAVisitor();
    test_HWESForecastVisitor();
    test_LSTMForecastVisitor();
    test_kshape_groups();
    test_count();
    test_class_count();
    test_AnomalyDetectByKNNVisitor();
    test_BIRCHVisitor();
    test_get_data_by_birch();
    test_md_stats();
    test_KrigingVisitor();
    test_asof_join();
    test_crosstab();
    test_pivot_table();
    test_JarqueBeraTestVisitor();
    test_LjungBoxTestVisitor();
    test_DurbinWatsonVisitor();
    test_SilhouetteScoreVisitor();
    test_DaviesBouldinIndexVisitor();
    test_CalinskiHarabaszVisitor();

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
