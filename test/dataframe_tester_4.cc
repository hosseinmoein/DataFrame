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
using MyDataFrame = StdDataFrame256<unsigned long>;
using MyStdDataFrame = StdDataFrame<unsigned long>;
using StrDataFrame = StdDataFrame<std::string>;
using DTDataFrame = StdDataFrame256<DateTime>;

template<typename T>
using StlVecType = typename MyDataFrame::template StlVecType<T>;

// ----------------------------------------------------------------------------

static void test_starts_with()  {

    std::cout << "\nTesting starts_with( ) ..." << std::endl;

    std::vector<std::string>                idx =
        { "XXDnh\1", "XXD974h", "fbbgd", std::string("XXDoiendg\0\0", 11),
          "yehtfg", "mnbvcd", "dfgsret", "jhnbdfg", "XXDomagdfert",
          "XXmj;'[-09", "XDimnaxcdf3", "207652234", "XXD", "XX" };
    std::vector<std::string>                s1 =
        { "XXDnh\1", "XXD974h", "fbbgd", std::string("XXDoiendg\0\0", 11),
          "yehtfg", "mnbvcd", "dfgsret", "jhnbdfg", "XXDomagdfert",
          "XXmj;'[-09", "XDimnaxcdf3", "207652234", "XXD", "XX" };
    std::vector<std::vector<unsigned char>> v1 =
        { { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 8, 9, 10 },
          { 100 },
          { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 23, 30, 31, 32, 100 } };
    std::vector<double>                     d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 10 };
    std::vector<int>                        i1 = { 22, 23, 24, 25, 99 };
    StrDataFrame                            df;

    df.load_data(std::move(idx),
                 std::make_pair("str_col", s1),
                 std::make_pair("col2", v1),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    const auto  res1 = df.starts_with<std::string>("str_col", "XXD");

    {
        std::vector<char>  out_res =
            { 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0 };

        assert(res1 == out_res);
    }

    const auto  res2 =
        df.starts_with<std::vector<unsigned char>>("col2", { 8, 9, 10, 11 });

    {
         std::vector<char>  out_res =
            { 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0 };

        assert(res2 == out_res);
    }

    const auto  res3 = df.starts_with<std::string>(DF_INDEX_COL_NAME, "XXD");

    {
        std::vector<char>  out_res =
            { 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0 };

        assert(res3 == out_res);
    }
}

// ----------------------------------------------------------------------------

static void test_ends_with()  {

    std::cout << "\nTesting ends_with( ) ..." << std::endl;

    std::vector<std::string>                idx =
        { "nh\1XXD", "974hXXD", "fbbgd", std::string("oiendg\0\0XXD", 11),
          "yehtfg", "mnbvcd", "dfgsret", "jhnbdfg", "omagdfertXXD",
          "XXmj;'[-09", "XDimnaxcdf3", "207652234", "XXD", "XX" };
    std::vector<std::string>                s1 =
        { "nh\1XXD", "974hXXD", "fbbgd", std::string("oiendg\0\0XXD", 11),
          "yehtfg", "mnbvcd", "dfgsret", "jhnbdfg", "omagdfertXXD",
          "XXmj;'[-09", "XDimnaxcdf3", "207652234", "XXD", "XX" };
    std::vector<std::vector<unsigned char>> v1 =
        { { 12, 13, 14, 20, 22, 23, 30, 31, 32, 100, 8, 9, 10, 11 },
          { 12, 13, 14, 20, 22, 23, 30, 31, 32, 100, 8, 9, 10, 11 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 12, 13, 14, 20, 22, 23, 30, 31, 32, 100, 8, 9, 10, 11 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 8, 9, 10 },
          { 100 },
          { 12, 13, 14, 20, 22, 23, 30, 31, 32, 100, 8, 9, 10, 11 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 23, 30, 31, 32, 100 } };
    std::vector<double>                     d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 10 };
    std::vector<int>                        i1 = { 22, 23, 24, 25, 99 };
    StrDataFrame                            df;

    df.load_data(std::move(idx),
                 std::make_pair("str_col", s1),
                 std::make_pair("col2", v1),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    const auto  res1 = df.ends_with<std::string>("str_col", "XXD");

    {
        std::vector<char>  out_res =
            { 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0 };

        assert(res1 == out_res);
    }

    const auto  res2 =
        df.ends_with<std::vector<unsigned char>>("col2", { 8, 9, 10, 11 });

    {
         std::vector<char>  out_res =
            { 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0 };

        assert(res2 == out_res);
    }

    const auto  res3 = df.ends_with<std::string>(DF_INDEX_COL_NAME, "XXD");

    {
        std::vector<char>  out_res =
            { 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0 };

        assert(res3 == out_res);
    }
}

// ----------------------------------------------------------------------------

static void test_in_between()  {

    std::cout << "\nTesting in_between( ) ..." << std::endl;

    StlVecType<unsigned long>               idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466 };
    StlVecType<std::string>                 s1 =
        { "XXDnh\1", "XXD974h", "fbbgd", std::string("XXDoiendg\0\0", 11),
          "yehtfg", "mnbvcd", "dfgsret", "jhnbdfg", "XXDomagdfert",
          "XXmj;'[-09", "XDimnaxcdf3", "207652234", "XXD", "XX" };
    StlVecType<StlVecType<unsigned char>>   v1 =
        { { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 8, 9, 10 },
          { 100 },
          { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 100 },
          { 23, 30, 31, 32, 100 } };
    StlVecType<double>                      d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 10 };
    StlVecType<int>                         i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                             df;

    df.load_data(std::move(idx),
                 std::make_pair("str_col", s1),
                 std::make_pair("col2", v1),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    const auto  res1 = df.in_between<double>("col_3", 15.0, 19.0);

    {
        StlVecType<char>    out_res =
            { 1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0 };

        assert(res1 == out_res);
    }

    const auto  res2 =
        df.in_between<unsigned long>(DF_INDEX_COL_NAME, 123453, 123460);

    {
        StlVecType<char>    out_res =
            { 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0 };

        assert(res2 == out_res);
    }
}

// ----------------------------------------------------------------------------

static void test_peaks()  {

    std::cout << "\nTesting peaks( ) ..." << std::endl;

    MyDataFrame                df;
    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0,
          3.0, 9.0, 10.0};
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 103.9, 106.55, 106.34, 1.8,
          111.0, 112.0, 111.5, 114.0, 115.0, 116.0};
    StlVecType<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));

    const auto  res1 = df.peaks<double>("dbl_col_2");

    {
        StlVecType<char>    out_res =
            { 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0 };

        assert(res1 == out_res);
    }

    const auto  res2 = df.peaks<unsigned long>(DF_INDEX_COL_NAME);

    {
        StlVecType<char>    out_res =
            { 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0 };

        assert(res2 == out_res);
    }
}

// ----------------------------------------------------------------------------

static void test_valleys()  {

    std::cout << "\nTesting valleys( ) ..." << std::endl;

    MyDataFrame                df;
    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0,
          3.0, 9.0, 10.0};
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 103.9, 106.55, 106.34, 1.8,
          111.0, 112.0, 111.5, 114.0, 115.0, 116.0};
    StlVecType<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("dbl_col_2_mask_1", df.valleys<double>("dbl_col_2"));
    df.load_column("dbl_col_2_mask_2", df.valleys<double>("dbl_col_2", 2));
    df.load_column("dbl_col_2_mask_3", df.valleys<double>("dbl_col_2", 3));

    {
        StlVecType<char>    out_res1 =
            { 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0 };
        StlVecType<char>    out_res2 =
            { 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0 };
        StlVecType<char>    out_res3 =
            { 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0 };

        assert((df.get_column<char>("dbl_col_2_mask_1") == out_res1));
        assert((df.get_column<char>("dbl_col_2_mask_2") == out_res2));
        assert((df.get_column<char>("dbl_col_2_mask_3") == out_res3));
    }

    df.load_column("index_mask", df.valleys<double>(DF_INDEX_COL_NAME));
    {
        StlVecType<char>    out_res =
            { 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0 };

        assert((df.get_column<char>("index_mask") == out_res));
    }
}

// ----------------------------------------------------------------------------

static void test_apply()  {

    std::cout << "\nTesting apply( ) ..." << std::endl;

    MyDataFrame                 df;
    StlVecType<unsigned long>   idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>          dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, -12.0, 11.0, 8.0, 7.0, 0.0, 5.0, 4.0,
          3.0, 9.0, -10.0 };
    StlVecType<double>          dblvec2 =
        { 1.0, 0.05, 0.28, 0.31, 0.01, 0.68, 0.12, 1, 0.98,
          0.9, 0.81, 0.82, 0.777, 0.34, 0.25 };
    StlVecType<std::string>     strvec =
        { "zz", "bb", "zz", "ww", "ee", "ff", "gg", "hh", "zz", "jj", "kk",
          "ll", "mm", "nn", "zz" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));

    auto    softp = [](const unsigned long &, double &val) -> bool  {
        val = std::log(1.0 + std::exp(val * 0.5)) / 0.5;
        return (true);
    };

    df.apply<double>("dbl_col", softp);

    const auto  &dbl_col = df.get_column<double>("dbl_col");

    {
        StlVecType<double>  out_res =
            { 1.38629, 15.0011, 0.00182293, 2.62652, 1.94815, 0.00495137,
              11.0082, 8.0363, 7.0595, 1.38629, 5.15778, 4.25386, 3.40283,
              9.0221, 0.0134307 };

        for (const auto &[ans, act]
                 : std::ranges::views::zip(out_res, dbl_col))
            assert(std::fabs(ans - act) < 0.0001);
    }

    auto    haphaz =
        [](const unsigned long &,
           double &val1,
           double &val2,
           std::string &val3) -> bool  {
            if (val3 == "zz")
                val1 += val2;
            return (true);
        };

    df.apply<double, double, std::string>
        ("dbl_col", "dbl_col_2", "str_col", haphaz);

    {
        StlVecType<double>  out_res =
            { 2.38629, 15.0011, 0.281823, 2.62652, 1.94815, 0.00495137,
              11.0082, 8.0363, 8.0395, 1.38629, 5.15778, 4.25386, 3.40283,
              9.0221, 0.263431 };

        for (const auto &[ans, act]
                 : std::ranges::views::zip(out_res, dbl_col))
            assert(std::fabs(ans - act) < 0.0001);
    }

}

// ----------------------------------------------------------------------------

static void test_truncate()  {

    std::cout << "\nTesting truncate( ) ..." << std::endl;

    MyDataFrame                df;
    StlVecType<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0,
          3.0, 9.0, 10.0};
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 103.9, 106.55, 106.34, 1.8,
          111.0, 112.0, 111.5, 114.0, 115.0, 116.0};
    StlVecType<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };
    StlVecType<int>            intvec = { 1, 2, 3, 4, 5 };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col",
                   std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    {
        auto    df2 = df;

        StlVecType<unsigned long>  index = { 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        StlVecType<double>         dbl_col = { 2, 1, 12, 11, 8, 7, 6, 5, 4 };
        StlVecType<double>         dbl_col_2 =
            { 103, 104, 103.9, 106.55, 106.34, 1.8, 111, 112, 111.5 };
        StlVecType<std::string>    str_col =
            { "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll" };
        StlVecType<int>            int_col = { 4, 5 };

        df2.truncate<double, std::string, int>(4, 12);
        assert(df2.get_index() == index);
        assert(df2.get_column<double>("dbl_col") == dbl_col);
        assert(df2.get_column<double>("dbl_col_2") == dbl_col_2);
        assert(df2.get_column<std::string>("str_col") == str_col);
        assert(df2.get_column<int>("int_col") == int_col);
    }
    {
        auto    df2 = df;

        StlVecType<unsigned long>  index =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        StlVecType<double>         dbl_col =
            { 0, 15, -14, 2, 1, 12, 11, 8, 7, 6, 5, 4 };
        StlVecType<double>         dbl_col_2 =
            { 100, 101, 102, 103, 104, 103.9, 106.55, 106.34, 1.8, 111, 112,
              111.5 };
        StlVecType<std::string>    str_col =
            { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj",
              "kk", "ll" };
        StlVecType<int>            int_col = { 1, 2, 3, 4, 5 };

        df2.truncate<double, std::string, int>(1, 12);
        assert(df2.get_index() == index);
        assert(df2.get_column<double>("dbl_col") == dbl_col);
        assert(df2.get_column<double>("dbl_col_2") == dbl_col_2);
        assert(df2.get_column<std::string>("str_col") == str_col);
        assert(df2.get_column<int>("int_col") == int_col);
    }
    {
        auto    df2 = df;

        StlVecType<unsigned long>  index =
            { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        StlVecType<double>         dbl_col =
            { 2, 1, 12, 11, 8, 7, 6, 5, 4, 3, 9, 10 };
        StlVecType<double>         dbl_col_2 =
            { 103, 104, 103.9, 106.55, 106.34, 1.8, 111, 112, 111.5, 114,
              115, 116 };
        StlVecType<std::string>    str_col =
            { "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm",
              "nn", "oo" };
        StlVecType<int>            int_col = { 4, 5 };

        df2.truncate<double, std::string, int>(4, 15);
        assert(df2.get_index() == index);
        assert(df2.get_column<double>("dbl_col") == dbl_col);
        assert(df2.get_column<double>("dbl_col_2") == dbl_col_2);
        assert(df2.get_column<std::string>("str_col") == str_col);
        assert(df2.get_column<int>("int_col") == int_col);
    }
    {
        auto    df2 = df;

        StlVecType<unsigned long>  index =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        StlVecType<double>         dbl_col =
            { 0, 15, -14, 2, 1, 12, 11, 8, 7, 6, 5, 4, 3, 9, 10 };
        StlVecType<double>         dbl_col_2 =
            { 100, 101, 102, 103, 104, 103.9, 106.55, 106.34, 1.8, 111, 112,
              111.5, 114, 115, 116 };
        StlVecType<std::string>    str_col =
            { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj",
              "kk", "ll", "mm", "nn", "oo" };
        StlVecType<int>            int_col = { 1, 2, 3, 4, 5 };

        df2.truncate<double, std::string, int>(0, 16);
    }
    {
        auto    df2 = df;

        StlVecType<unsigned long>  index = { 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        StlVecType<double>         dbl_col = { 11, 8, 7, 6, 5, 4, 3, 9, 10 };
        StlVecType<double>         dbl_col_2 =
            { 106.55, 106.34, 1.8, 111, 112, 111.5, 114, 115, 116 };
        StlVecType<std::string>    str_col =
            { "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn", "oo" };
        StlVecType<int>            int_col = {  };

        df2.truncate<double, std::string, int>(7, 15);
        assert(df2.get_index() == index);
        assert(df2.get_column<double>("dbl_col") == dbl_col);
        assert(df2.get_column<double>("dbl_col_2") == dbl_col_2);
        assert(df2.get_column<std::string>("str_col") == str_col);
        assert(df2.get_column<int>("int_col") == int_col);
    }
}

// ----------------------------------------------------------------------------

static void test_load_column()  {

    std::cout << "\nTesting load_column( ) ..." << std::endl;

    MyDataFrame                 df;
    StlVecType<unsigned long>   idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>          dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, -12.0, 11.0, 8.0, 7.0, 0.0, 5.0, 4.0,
          3.0, 9.0, -10.0 };
    StlVecType<double>          dblvec2 =
        { 1.0, 0.05, 0.28, 0.31, 0.01, 0.68, 0.12, 1, 0.98,
          0.9, 0.81, 0.82, 0.777, 0.34, 0.25 };
    StlVecType<std::string>     strvec =
        { "zz", "bb", "zz", "ww", "ee", "ff", "gg", "hh", "zz", "jj", "kk",
          "ll", "mm", "nn", "zz" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));

    auto    lbd = [](const unsigned long &, const double &val) -> double  {
        return (val * 2.0);
    };

    df.load_column<double, double>("new_dbl_col", "dbl_col", std::move(lbd));
    {
        StlVecType<double>  new_dbl_col =
            { 0, 30, -28, 4, 2, -24, 22, 16, 14, 0, 10, 8, 6, 18, -20 };

        assert((df.get_column<double>("new_dbl_col") == new_dbl_col));
    }
}

// ----------------------------------------------------------------------------

static void test_explode()  {

    std::cout << "\nTesting load_explode( ) ..." << std::endl;

    DTDataFrame    df;

    try  {
        df.read("AAPL_10dBucketWithMaps_small.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    df.write<std::ostream,
             double,
             long,
             std::map<std::string, double>,
             std::unordered_map<std::string, double>,
             std::vector<std::string>,
             std::set<double>,
             std::set<std::string>>
        (std::cout, io_format::csv2);

    auto exploded1 = df.explode<std::unordered_map<std::string, double>,
                                double,
                                long,
                                std::map<std::string, double>,
                                std::vector<std::string>,
                                std::set<double>,
                                std::set<std::string>>("Unordered Map");

    std::cout << "\n\n";
    exploded1.write<std::ostream,
                    std::pair<std::string, double>,
                    double,
                    long,
                    std::map<std::string, double>,
                    std::vector<std::string>,
                    std::set<double>,
                    std::set<std::string>>
        (std::cout, io_format::csv2);

    auto exploded2 = df.explode<std::map<std::string, double>,
                                double,
                                long,
                                std::vector<std::string>,
                                std::unordered_map<std::string, double>,
                                std::set<double>,
                                std::set<std::string>>("Map 1");

    std::cout << "\n\n";
    exploded2.write<std::ostream,
                    std::pair<std::string, double>,
                    double,
                    long,
                    std::unordered_map<std::string, double>,
                    std::vector<std::string>,
                    std::set<double>,
                    std::set<std::string>>
        (std::cout, io_format::csv2);

    auto exploded3 = df.explode<std::set<double>,
                                double,
                                long,
                                std::vector<std::string>,
                                std::map<std::string, double>,
                                std::unordered_map<std::string, double>,
                                std::set<std::string>>("Double Set");

    std::cout << "\n\n";
    exploded3.write<std::ostream,
                    double,
                    long,
                    std::map<std::string, double>,
                    std::unordered_map<std::string, double>,
                    std::vector<std::string>,
                    std::set<std::string>>
        (std::cout, io_format::csv2);

    auto exploded4 = df.explode<std::vector<std::string>,
                                double,
                                long,
                                std::map<std::string, double>,
                                std::set<double>,
                                std::unordered_map<std::string, double>,
                                std::set<std::string>>("Str Vec");

    std::cout << "\n\n";
    exploded4.write<std::ostream,
                    std::string,
                    double,
                    long,
                    std::map<std::string, double>,
                    std::unordered_map<std::string, double>,
                    std::set<double>,
                    std::set<std::string>>
        (std::cout, io_format::csv2);
}

// ----------------------------------------------------------------------------

static void test_read_write_pairs()  {

    std::cout << "\nTesting read_write_pairs ..." << std::endl;

    MyDataFrame df;

    try  {
        df.read("sample_data_2.csv", io_format::csv);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    df.write<std::ostream,
             unsigned long,
             int,
             std::string,
             double,
             bool,
             std::pair<std::string, std::string>,
             std::pair<double, double>,
             std::pair<std::string, double>>
        (std::cout, io_format::csv);

    df.write<unsigned long,
             int,
             std::string,
             double,
             bool,
             std::pair<std::string, std::string>,
             std::pair<double, double>,
             std::pair<std::string, double>>
        ("./tmp_sample_data_2.csv", io_format::csv2);
    df.write<unsigned long,
             int,
             std::string,
             double,
             bool,
             std::pair<std::string, std::string>,
             std::pair<double, double>,
             std::pair<std::string, double>>
        ("./tmp_sample_data_2.dat", io_format::binary);

    MyDataFrame df2;

    df2.read("./tmp_sample_data_2.csv", io_format::csv2);
    assert(df.get_index() == df2.get_index());
    assert((df.get_column<int>("xint_col_2") ==
                df2.get_column<int>("xint_col_2")));
    assert((df.get_column<std::string>("str_col_2") ==
                df2.get_column<std::string>("str_col_2")));
    assert((df.get_column<unsigned long>("ul_col_2") ==
                df2.get_column<unsigned long>("ul_col_2")));
    assert((
 df.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[0] ==
 df2.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[0]));
    assert((
 df.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[1] ==
 df2.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[1]));
    assert((
 df.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[2] ==
 df2.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[2]));
    assert((
 df.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[3] ==
 df2.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[3]));
    assert((
 df.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[4] ==
 df2.get_column<std::pair<std::string, std::string>>("str_str_pair_col")[4]));
    assert((
 df.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[0] ==
 df2.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[0]));
    assert((std::isnan(df.get_column<std::pair<double, double>>
                              ("dbl_dbl_pair_col")[1].first)));
    assert((std::isnan(df.get_column<std::pair<double, double>>
                              ("dbl_dbl_pair_col")[1].second)));
    assert((std::isnan(df2.get_column<std::pair<double, double>>
                               ("dbl_dbl_pair_col")[1].first)));
    assert((std::isnan(df2.get_column<std::pair<double, double>>
                               ("dbl_dbl_pair_col")[1].second)));
    assert((std::isnan(df.get_column<std::pair<double, double>>
                              ("dbl_dbl_pair_col")[2].first)));
    assert((std::isnan(df2.get_column<std::pair<double, double>>
                               ("dbl_dbl_pair_col")[2].first)));
    assert((
 df.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[2].second ==
 df2.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[2].second));
    assert((std::isnan(df.get_column<std::pair<double, double>>
                              ("dbl_dbl_pair_col")[3].second)));
    assert((std::isnan(df2.get_column<std::pair<double, double>>
                               ("dbl_dbl_pair_col")[3].second)));
    assert((
 df.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[3].first ==
 df2.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[3].first));
    assert((
 df.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[4] ==
 df2.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[4]));

    assert((
 df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[0] ==
 df2.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[0]));
    assert((
 df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[1].first ==
 df2.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[1].first));
    assert((std::isnan(df.get_column<std::pair<std::string, double>>
                              ("str_dbl_pair_col")[1].second)));
    assert((std::isnan(df2.get_column<std::pair<std::string, double>>
                              ("str_dbl_pair_col")[1].second)));
    assert((
 df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[2].first ==
 df2.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[2].first));
    assert((
 df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[2].second ==
 df2.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[2].second));
    assert((std::isnan(df.get_column<std::pair<std::string, double>>
                              ("str_dbl_pair_col")[3].second)));
    assert((std::isnan(df2.get_column<std::pair<std::string, double>>
                              ("str_dbl_pair_col")[3].second)));
    assert((
 df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[3].first ==
 df2.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[3].first));
    assert((
 df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[4] ==
 df2.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[4]));

    MyDataFrame df3;

    df3.read("./tmp_sample_data_2.dat", io_format::binary);
    assert(df.get_index() == df3.get_index());
    assert((df.get_column<int>("xint_col_2") ==
                df3.get_column<int>("xint_col_2")));
    assert((df.get_column<std::string>("str_col_2") ==
                df3.get_column<std::string>("str_col_2")));
    assert((df.get_column<unsigned long>("ul_col_2") ==
                df3.get_column<unsigned long>("ul_col_2")));
    assert((
  df.get_column<std::pair<std::string, std::string>>("str_str_pair_col") ==
  df3.get_column<std::pair<std::string, std::string>>("str_str_pair_col")));
    assert((
  df.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[0] ==
  df3.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[0]));
    assert((std::isnan(df.get_column<std::pair<double, double>>
                              ("dbl_dbl_pair_col")[1].first)));
    assert((std::isnan(df.get_column<std::pair<double, double>>
                              ("dbl_dbl_pair_col")[1].second)));
    assert((std::isnan(df3.get_column<std::pair<double, double>>
                               ("dbl_dbl_pair_col")[1].first)));
    assert((std::isnan(df3.get_column<std::pair<double, double>>
                               ("dbl_dbl_pair_col")[1].second)));
    assert((std::isnan(df.get_column<std::pair<double, double>>
                              ("dbl_dbl_pair_col")[2].first)));
    assert((std::isnan(df3.get_column<std::pair<double, double>>
                               ("dbl_dbl_pair_col")[2].first)));
    assert((
  df.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[2].second ==
  df3.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[2].second));
    assert((std::isnan(df.get_column<std::pair<double, double>>
                              ("dbl_dbl_pair_col")[3].second)));
    assert((std::isnan(df3.get_column<std::pair<double, double>>
                               ("dbl_dbl_pair_col")[3].second)));
    assert((
  df.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[3].first ==
  df3.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[3].first));
    assert((
  df.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[4] ==
  df3.get_column<std::pair<double, double>>("dbl_dbl_pair_col")[4]));

    assert((
  df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[0] ==
  df3.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[0]));
    assert((
  df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[1].first ==
  df3.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[1].first));
    assert((std::isnan(df.get_column<std::pair<std::string, double>>
                              ("str_dbl_pair_col")[1].second)));
    assert((std::isnan(df3.get_column<std::pair<std::string, double>>
                              ("str_dbl_pair_col")[1].second)));
    assert((
  df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[2].first ==
  df3.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[2].first));
    assert((
 df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[2].second ==
 df3.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[2].second));
    assert((std::isnan(df.get_column<std::pair<std::string, double>>
                              ("str_dbl_pair_col")[3].second)));
    assert((std::isnan(df3.get_column<std::pair<std::string, double>>
                              ("str_dbl_pair_col")[3].second)));
    assert((
 df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[3].first ==
 df3.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[3].first));
    assert((
  df.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[4] ==
  df3.get_column<std::pair<std::string, double>>("str_dbl_pair_col")[4]));

    std::remove("./tmp_sample_data_2.csv");
    std::remove("./tmp_sample_data_2.dat");
}

// ----------------------------------------------------------------------------

static void test_difference()  {

    std::cout << "\nTesting difference( ) ..." << std::endl;

    MyDataFrame                df;
    StlVecType<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    StlVecType<double>         dblvec =
        { 0, 15, -14, 2, 1, 12, 11, 8, 7, 6, 5, 4, 3, 9, 10};
    StlVecType<double>         dblvec2 =
        { 100, 101, 102, 103, 104, 103.9, 106.55, 106.34, 1.8, 111, 112, 111.5,
          114, 115, 116};
    StlVecType<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));

    MyDataFrame df2 = df;

    df2.get_column<double>("dbl_col")[3] = 1000.0;
    df2.get_column<double>("dbl_col")[6] = 1001.0;
    df2.get_column<std::string>("str_col")[1] = "Changed";
    df2.get_column<std::string>("str_col")[5] = "Changed 2";

    MyDataFrame diff_df = df.difference<double, std::string>(df2);

    assert((diff_df.get_index() ==
                StlVecType<unsigned long>{ 1, 2, 3, 4, 5, 6, 7 }));
    assert(diff_df.get_column<double>("self_dbl_col").size() == 7);
    assert(diff_df.get_column<double>("other_dbl_col").size() == 7);
    assert(std::isnan(diff_df.get_column<double>("self_dbl_col")[0]));
    assert(std::isnan(diff_df.get_column<double>("other_dbl_col")[0]));
    assert(std::isnan(diff_df.get_column<double>("self_dbl_col")[1]));
    assert(std::isnan(diff_df.get_column<double>("other_dbl_col")[1]));
    assert(diff_df.get_column<double>("self_dbl_col")[3] == 2.0);
    assert(diff_df.get_column<double>("other_dbl_col")[3] == 1000.0);
    assert(std::isnan(diff_df.get_column<double>("self_dbl_col")[4]));
    assert(std::isnan(diff_df.get_column<double>("other_dbl_col")[4]));
    assert(diff_df.get_column<double>("self_dbl_col")[6] == 11.0);
    assert(diff_df.get_column<double>("other_dbl_col")[6] == 1001.0);
    assert(diff_df.get_column<std::string>("self_str_col").size() == 6);
    assert(diff_df.get_column<std::string>("other_str_col").size() == 6);
    assert(diff_df.get_column<std::string>("self_str_col")[0] == "");
    assert(diff_df.get_column<std::string>("other_str_col")[0] == "");
    assert(diff_df.get_column<std::string>("self_str_col")[1] == "bb");
    assert(diff_df.get_column<std::string>("other_str_col")[1] == "Changed");
    assert(diff_df.get_column<std::string>("self_str_col")[2] == "");
    assert(diff_df.get_column<std::string>("other_str_col")[2] == "");
    assert(diff_df.get_column<std::string>("self_str_col")[5] == "ff");
    assert(diff_df.get_column<std::string>("other_str_col")[5] == "Changed 2");
    assert(! diff_df.has_column("self_dbl_col_2"));
    assert(! diff_df.has_column("other_dbl_col_2"));
    assert(! diff_df.has_column("dbl_col_2"));
}

// ----------------------------------------------------------------------------

static void test_get_data_at_times()  {

    std::cout << "\nTesting get_data_at_times( ) ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("DT_Intraday.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result = df.get_view_at_times<double, long>(16, 30);

    assert(result.get_index().size() == 7);
    assert(result.get_index()[0].date() == 19861115);
    assert(result.get_index()[1].date() == 19861214);
    assert(result.get_index()[6].date() == 19870613);
    assert(result.get_index()[5].date() == 19870508);
    assert(result.get_column<double>("dbl value").size() == 7);
    assert(result.get_column<double>("dbl value")[0] == 4.0);
    assert(result.get_column<double>("dbl value")[1] == 655.5);
    assert(result.get_column<double>("dbl value")[6] == 4767.0);
    assert(result.get_column<double>("dbl value")[5] == 3935.5);
    assert(result.get_column<long>("lng value").size() == 7);
    assert(result.get_column<long>("lng value")[0] == 80);
    assert(result.get_column<long>("lng value")[1] == 13110);
    assert(result.get_column<long>("lng value")[6] == 95340);
    assert(result.get_column<long>("lng value")[5] == 78710);
}

// ----------------------------------------------------------------------------

static void test_get_data_before_times()  {

    std::cout << "\nTesting get_data_before_times( ) ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("DT_Intraday.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result = df.get_view_before_times<double, long>(1, 16);

    assert(result.get_index().size() == 523);
    assert(result.get_index()[0].date() == 19861116);
    assert(result.get_index()[10].date() == 19861120);
    assert(result.get_index()[500].date() == 19870615);
    assert(result.get_index()[522].date() == 19870624);
    assert(result.get_column<double>("dbl value").size() == 523);
    assert(result.get_column<double>("dbl value")[0] == 11.0);
    assert(result.get_column<double>("dbl value")[10] == 101.0);
    assert(result.get_column<double>("dbl value")[500] == 4796.0);
    assert(result.get_column<double>("dbl value")[522] == 4996.0);
    assert(result.get_column<long>("lng value").size() == 523);
    assert(result.get_column<long>("lng value")[0] == 220);
    assert(result.get_column<long>("lng value")[10] == 2020);
    assert(result.get_column<long>("lng value")[500] == 95920);
    assert(result.get_column<long>("lng value")[522] == 99920);
}

// ----------------------------------------------------------------------------

static void test_get_data_after_times()  {

    std::cout << "\nTesting get_data_after_times( ) ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("DT_Intraday.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result = df.get_view_after_times<double, long>(23, 40);

    assert(result.get_index().size() == 137);
    assert(result.get_index()[0].date() == 19861118);
    assert(result.get_index()[10].date() == 19861203);
    assert(result.get_index()[100].date() == 19870423);
    assert(result.get_index()[136].date() == 19870623);
    assert(result.get_column<double>("dbl value").size() == 137);
    assert(result.get_column<double>("dbl value")[0] == 78.0);
    assert(result.get_column<double>("dbl value")[10] == 415.0);
    assert(result.get_column<double>("dbl value")[100] == 3601.5);
    assert(result.get_column<double>("dbl value")[136] == 4995.0);
    assert(result.get_column<long>("lng value").size() == 137);
    assert(result.get_column<long>("lng value")[0] == 1560);
    assert(result.get_column<long>("lng value")[10] == 8300);
    assert(result.get_column<long>("lng value")[100] == 72030);
    assert(result.get_column<long>("lng value")[136] == 99900);
}

// ----------------------------------------------------------------------------

static void test_get_data_on_days()  {

    std::cout << "\nTesting get_data_on_days( ) ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("DT_Intraday.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result =
        df.get_view_on_days<double, long>(
            { DT_WEEKDAY::MON, DT_WEEKDAY::FRI });

    assert(result.get_index().size() == 2861);
    assert(result.get_index()[0].date() == 19861117);
    assert(result.get_index()[100].date() == 19861124);
    assert(result.get_index()[1000].date() == 19870202);
    assert(result.get_index()[2860].date() == 19870622);
    assert(result.get_column<double>("dbl value").size() == 2861);
    assert(result.get_column<double>("dbl value")[0] == 32.5);
    assert(result.get_column<double>("dbl value")[100] == 195.5);
    assert(result.get_column<double>("dbl value")[1000] == 1771.5);
    assert(result.get_column<double>("dbl value")[2860] == 4973.5);
    assert(result.get_column<long>("lng value").size() == 2861);
    assert(result.get_column<long>("lng value")[0] == 650);
    assert(result.get_column<long>("lng value")[100] == 3910);
    assert(result.get_column<long>("lng value")[1000] == 35430);
    assert(result.get_column<long>("lng value")[2860] == 99470);
}

// ----------------------------------------------------------------------------

static void test_get_data_in_months()  {

    std::cout << "\nTesting get_data_in_months( ) ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("DT_Intraday.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result =
        df.get_view_in_months<double, long>({ DT_MONTH::JAN, DT_MONTH::NOV });

    assert(result.get_index().size() == 2101);
    assert(result.get_index()[0].date() == 19861115);
    assert(result.get_index()[100].date() == 19861117);
    assert(result.get_index()[1000].date() == 19870107);
    assert(result.get_index()[2100].date() == 19870131);
    assert(result.get_column<double>("dbl value").size() == 2101);
    assert(result.get_column<double>("dbl value")[0] == 0.5);
    assert(result.get_column<double>("dbl value")[100] == 50.5);
    assert(result.get_column<double>("dbl value")[1000] == 1198.0);
    assert(result.get_column<double>("dbl value")[2100] == 1748.0);
    assert(result.get_column<long>("lng value").size() == 2101);
    assert(result.get_column<long>("lng value")[0] == 10);
    assert(result.get_column<long>("lng value")[100] == 1010);
    assert(result.get_column<long>("lng value")[1000] == 23960);
    assert(result.get_column<long>("lng value")[2100] == 34960);
}

// ----------------------------------------------------------------------------

static void test_get_data_on_days_in_month()  {

    std::cout << "\nTesting get_data_on_days_in_month( ) ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("DT_Intraday.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result = df.get_view_on_days_in_month<double, long>({ 31, 5 });

    assert(result.get_index().size() == 505);
    assert(result.get_index()[0].date() == 19861205);
    assert(result.get_index()[100].date() == 19870105);
    assert(result.get_index()[300].date() == 19870331);
    assert(result.get_index()[504].date() == 19870605);
    assert(result.get_column<double>("dbl value").size() == 505);
    assert(result.get_column<double>("dbl value")[0] == 439.5);
    assert(result.get_column<double>("dbl value")[100] == 1145.5);
    assert(result.get_column<double>("dbl value")[300] == 3071.0);
    assert(result.get_column<double>("dbl value")[504] == 4594.0);
    assert(result.get_column<long>("lng value").size() == 505);
    assert(result.get_column<long>("lng value")[0] == 8790);
    assert(result.get_column<long>("lng value")[100] == 22910);
    assert(result.get_column<long>("lng value")[300] == 61420);
    assert(result.get_column<long>("lng value")[504] == 91880);
}

// ----------------------------------------------------------------------------

static void test_get_data_between_times()  {

    std::cout << "\nTesting get_data_between_times( ) ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("DT_Intraday.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    // Between 11am and 11:30am
    //
    const auto  result =
        df.get_view_between_times<double, long>(11, 11, 0, 30);

    assert(result.get_index().size() == 207);
    assert(result.get_index()[0].date() == 19861116);
    assert(result.get_index()[10].date() == 19861125);
    assert(result.get_index()[100].date() == 19870303);
    assert(result.get_index()[206].date() == 19870623);
    assert(result.get_column<double>("dbl value").size() == 207);
    assert(result.get_column<double>("dbl value")[0] == 21.0);
    assert(result.get_column<double>("dbl value")[10] == 225.5);
    assert(result.get_column<double>("dbl value")[100] == 2438.0);
    assert(result.get_column<double>("dbl value")[206] == 4984.5);
    assert(result.get_column<long>("lng value").size() == 207);
    assert(result.get_column<long>("lng value")[0] == 420);
    assert(result.get_column<long>("lng value")[10] == 4510);
    assert(result.get_column<long>("lng value")[100] == 48760);
    assert(result.get_column<long>("lng value")[206] == 99690);
}

// ----------------------------------------------------------------------------

static void test_remove_top_n_data()  {

    std::cout << "\nTesting remove_top_n_data( ) ..." << std::endl;

    StlVecType<unsigned long>   idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123463 };
    StlVecType<double>          d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    StlVecType<double>          d2 =
        { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 1.89 };
    StlVecType<double>          d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 10 };
    StlVecType<int>             i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("col_1", d1),
                 std::make_pair("col_2", d2),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    MyDataFrame df2 = df;

    auto    lbd =
        [](const unsigned long &, const double &val) -> bool {
            return (val < 100.0);
        };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, int>
            ("col_1", lbd);

    df.remove_top_n_data<double, int, double>("col_3", 4);
    view.remove_top_n_data<double, int, double>("col_3", 4);

    assert(df.get_index().size() == 10);
    assert(view.get_index().size() == 10);
    assert(df.get_column<double>("col_2").size() == 10);
    assert(view.get_column<double>("col_2").size() == 10);
    assert(df.get_column<int>("col_4").size() == 10);
    assert(view.get_column<int>("col_4").size() == 10);
    assert(df.get_index()[4] == 123457);
    assert(view.get_index()[4] == 123457);
    assert(df.get_column<double>("col_1")[6] == 10);
    assert(view.get_column<double>("col_1")[6] == 10);
    assert(df.get_column<int>("col_4")[2] == 24);
    assert(view.get_column<int>("col_4")[2] == 24);
}

// ----------------------------------------------------------------------------

static void test_remove_bottom_n_data()  {

    std::cout << "\nTesting remove_bottom_n_data( ) ..." << std::endl;

    StlVecType<unsigned long>   idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123463 };
    StlVecType<double>          d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    StlVecType<double>          d2 =
        { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 1.89 };
    StlVecType<double>          d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 10 };
    StlVecType<int>             i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("col_1", d1),
                 std::make_pair("col_2", d2),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    MyDataFrame df2 = df;

    auto    lbd =
        [](const unsigned long &, const double &val) -> bool {
            return (val < 100.0);
        };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, int>
            ("col_1", lbd);

    df.remove_bottom_n_data<double, int, double>("col_3", 4);
    view.remove_bottom_n_data<double, int, double>("col_3", 4);

    assert(df.get_index().size() == 10);
    assert(view.get_index().size() == 10);
    assert(df.get_column<double>("col_2").size() == 10);
    assert(view.get_column<double>("col_2").size() == 10);
    assert(df.get_column<int>("col_4").size() == 10);
    assert(view.get_column<int>("col_4").size() == 10);
    assert(df.get_index()[4] == 123454);
    assert(view.get_index()[4] == 123454);
    assert(df.get_column<double>("col_1")[6] == 7);
    assert(view.get_column<double>("col_1")[6] == 7);
    assert(df.get_column<int>("col_4")[2] == 24);
    assert(view.get_column<int>("col_4")[2] == 24);
}

// ----------------------------------------------------------------------------

static void test_remove_above_quantile_data()  {

    std::cout << "\nTesting remove_above_quantile_data( ) ..." << std::endl;

    StlVecType<unsigned long>   idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123463 };
    StlVecType<double>          d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    StlVecType<double>          d2 =
        { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 1.89 };
    StlVecType<double>          d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 10 };
    StlVecType<int>             i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("col_1", d1),
                 std::make_pair("col_2", d2),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    MyDataFrame df2 = df;

    auto    lbd =
        [](const unsigned long &, const double &val) -> bool {
            return (val < 100.0);
        };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, int>("col_1", lbd);

    df.remove_above_quantile_data<double, int, double>("col_3", 0.45);
    view.remove_above_quantile_data<double, int, double>("col_3", 0.45);

    StlVecType<unsigned long>   index =
        { 123457, 123458, 123459, 123460, 123461, 123463 };
    StlVecType<double>          col_1 = { 8, 9, 10, 11, 12, 14 };
    StlVecType<double>          col_2 = { 20, 22, 23, 30, 31, 1.89 };
    StlVecType<double>          col_3 = { 0.34, 1.56, 0.34, 2.3, 0.34, 10 };
    StlVecType<int>             col_4 = { 0, 0, 0, 0, 0, 0 };

    assert(df.get_index() == index);
    assert(df.get_column<double>("col_1") == col_1);
    assert(df.get_column<double>("col_2") == col_2);
    assert(df.get_column<double>("col_3") == col_3);
    assert(df.get_column<int>("col_4") == col_4);
}

// ----------------------------------------------------------------------------

static void test_remove_below_quantile_data()  {

    std::cout << "\nTesting remove_below_quantile_data( ) ..." << std::endl;

    StlVecType<unsigned long>   idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123463 };
    StlVecType<double>          d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    StlVecType<double>          d2 =
        { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23, 30, 31, 32, 1.89 };
    StlVecType<double>          d3 =
        { 15, 16, 15, 18, 19, 16, 21, 0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 10 };
    StlVecType<int>             i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("col_1", d1),
                 std::make_pair("col_2", d2),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1));

    MyDataFrame df2 = df;

    auto    lbd =
        [](const unsigned long &, const double &val) -> bool {
            return (val < 100.0);
        };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, int>("col_1", lbd);

    df.remove_below_quantile_data<double, int, double>("col_3", 0.45);
    view.remove_below_quantile_data<double, int, double>("col_3", 0.45);

    StlVecType<unsigned long>   index =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456, 123462 };
    StlVecType<double>          col_1 = { 1, 2, 3, 4, 5, 6, 7, 13 };
    StlVecType<double>          col_2 = { 8, 9, 10, 11, 12, 13, 14, 32 };
    StlVecType<double>          col_3 = { 15, 16, 15, 18, 19, 16, 21, 19 };
    StlVecType<int>             col_4 = { 22, 23, 24, 25, 99, 0, 0, 0 };

    assert(df.get_index() == index);
    assert(df.get_column<double>("col_1") == col_1);
    assert(df.get_column<double>("col_2") == col_2);
    assert(df.get_column<double>("col_3") == col_3);
    assert(df.get_column<int>("col_4") == col_4);
}

// ----------------------------------------------------------------------------

static void test_remove_data_by_stdev()  {

    std::cout << "\nTesting remove_data_by_stdev( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    StrDataFrame    df2 = df;

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    df.remove_data_by_stdev <double, double, long>("IBM_Close", 0.25, -0.5);
    view.remove_data_by_stdev<double, double, long>("IBM_Close", 0.25, -0.5);

    assert(df.get_index().size() == 570);
    assert(view.get_index().size() == 570);
    assert(view.get_column<double>("IBM_Open").size() == 570);
    assert(view.get_column<long>("IBM_Volume").size() == 570);
    assert(df.get_column<double>("IBM_Low").size() == 570);
    assert(df.get_column<long>("IBM_Volume").size() == 570);
    assert(df.get_index()[500] == "2019-04-08");
    assert(view.get_index()[101] == "2016-04-26");
    assert(view.get_column<double>("IBM_Open")[45] == 142.600006);
    assert(df.get_column<long>("IBM_Volume")[300] == 4300500);
}

// ----------------------------------------------------------------------------

static void test_get_data_by_stdev()  {

    std::cout << "\nTesting get_data_by_stdev( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.dat", io_format::binary);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    StrDataFrame    df2 = df;

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    auto    result_df =
        df.get_data_by_stdev <double, double, long>("IBM_Close", 0.1, -0.4);
    auto    result_view =
        view.get_view_by_stdev<double, double, long>("IBM_Close", 0.1, -0.4);

    assert(result_df.get_index().size() == 379);
    assert(result_view.get_index().size() == 379);
    assert(result_view.get_column<double>("IBM_Open").size() == 379);
    assert(result_view.get_column<long>("IBM_Volume").size() == 379);
    assert(result_df.get_column<double>("IBM_Low").size() == 379);
    assert(result_df.get_column<long>("IBM_Volume").size() == 379);
    assert(result_df.get_index()[300] == "2018-08-23");
    assert(result_view.get_index()[300] == "2018-08-23");
    assert(result_df.get_index()[101] == "2016-06-13");
    assert(result_view.get_index()[101] == "2016-06-13");
    assert(result_view.get_column<double>("IBM_Open")[45] == 141.740005);
    assert(result_df.get_column<double>("IBM_Open")[45] == 141.740005);
    assert(result_df.get_column<long>("IBM_Volume")[230] == 4413200);
    assert(result_view.get_column<long>("IBM_Volume")[230] == 4413200);
}

// ----------------------------------------------------------------------------

static void test_get_data_by_kmeans()  {

    std::cout << "\nTesting get_data_by_kmeans( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.dat", io_format::binary);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    StrDataFrame    df2 = df;

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    auto    result_df =
        df.get_data_by_kmeans <4, double, double, long>
            ("IBM_Close",
             [](const double &x, const double &y) -> double  {
                 return (std::fabs(x - y));
             },
             1000,  // Number of iterations
             1234); // Random number seed
    auto    result_view =
        view.get_view_by_kmeans<4, double, double, long>
            ("IBM_Close",
             [](const double &x, const double &y) -> double  {
                 return (std::fabs(x - y));
             },
             1000,  //  Number of iterations
             1234); // Random number seed

    assert(result_df.size() == 4);
    assert(result_df.size() == result_view.size());

    assert(result_df[0].get_index().size() == 272);
    assert(result_df[0].get_column<double>("IBM_Open").size() == 272);
    assert(result_df[0].get_index()[0] == "2014-01-02");
    assert(result_df[0].get_index()[271] == "2017-04-07");
    assert(result_df[0].get_column<double>("IBM_High")[200] == 182.839996);
    assert(result_df[0].get_column<long>("IBM_Volume")[100] == 3721600);
    assert(result_view[0].get_index().size() == 272);
    assert(result_view[0].get_column<double>("IBM_Open").size() == 272);
    assert(result_view[0].get_index()[0] == "2014-01-02");
    assert(result_view[0].get_index()[271] == "2017-04-07");
    assert(result_view[0].get_column<double>("IBM_High")[200] == 182.839996);
    assert(result_view[0].get_column<long>("IBM_Volume")[100] == 3721600);

    assert(result_df[1].get_index().size() == 606);
    assert(result_df[1].get_column<double>("IBM_Open").size() == 606);
    assert(result_df[1].get_index()[0] == "2015-08-21");
    assert(result_df[1].get_index()[605] == "2020-10-08");
    assert(result_df[1].get_column<double>("IBM_High")[200] == 145.880005);
    assert(result_df[1].get_column<long>("IBM_Volume")[100] == 4386200);
    assert(result_view[1].get_index().size() == 606);
    assert(result_view[1].get_column<double>("IBM_Open").size() == 606);
    assert(result_view[1].get_index()[0] == "2015-08-21");
    assert(result_view[1].get_index()[605] == "2020-10-08");
    assert(result_view[1].get_column<double>("IBM_High")[200] == 145.880005);
    assert(result_view[1].get_column<long>("IBM_Volume")[100] == 4386200);

    assert(result_df[2].get_index().size() == 585);
    assert(result_df[2].get_column<double>("IBM_Open").size() == 585);
    assert(result_df[2].get_index()[0] == "2014-10-20");
    assert(result_df[2].get_index()[584] == "2020-02-21");
    assert(result_df[2].get_column<double>("IBM_High")[200] == 153.100006);
    assert(result_df[2].get_column<long>("IBM_Volume")[100] == 3749600);
    assert(result_view[2].get_index().size() == 585);
    assert(result_view[2].get_column<double>("IBM_Open").size() == 585);
    assert(result_view[2].get_index()[0] == "2014-10-20");
    assert(result_view[2].get_index()[584] == "2020-02-21");
    assert(result_view[2].get_column<double>("IBM_High")[200] == 153.100006);
    assert(result_view[2].get_column<long>("IBM_Volume")[100] == 3749600);

    assert(result_df[3].get_index().size() == 258);
    assert(result_df[3].get_column<double>("IBM_Open").size() == 258);
    assert(result_df[3].get_index()[0] == "2016-01-15");
    assert(result_df[3].get_index()[257] == "2020-10-30");
    assert(result_df[3].get_column<double>("IBM_High")[200] == 127.239998);
    assert(result_df[3].get_column<long>("IBM_Volume")[100] == 12502100);
    assert(result_view[3].get_index().size() == 258);
    assert(result_view[3].get_column<double>("IBM_Open").size() == 258);
    assert(result_view[3].get_index()[0] == "2016-01-15");
    assert(result_view[3].get_index()[257] == "2020-10-30");
    assert(result_view[3].get_column<double>("IBM_High")[200] == 127.239998);
    assert(result_view[3].get_column<long>("IBM_Volume")[100] == 12502100);
}

// ----------------------------------------------------------------------------

void test_get_data_by_affin()  {

    std::cout << "\nTesting get_data_by_affin( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.dat", io_format::binary);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    StrDataFrame    df2 = df;

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    auto    views =
        view.get_view_by_affin<double, double, long>
            ("IBM_Close",
             [](const double &x, const double &y) -> double  {
                 return (std::fabs(x - y));
             },
             25);  //  Number of iterations

    assert(views.size() == 4);

    assert(views[0].get_index().size() == 157);
    assert(views[0].get_column<double>("IBM_Open").size() == 157);
    assert(views[0].get_index()[0] == "2014-10-21");
    assert(views[0].get_index()[156] == "2018-02-01");
    assert(views[0].get_column<double>("IBM_High")[140] == 162.899994);
    assert(views[0].get_column<long>("IBM_Volume")[100] == 2543100);

    assert(views[1].get_index().size() == 309);
    assert(views[1].get_column<double>("IBM_Open").size() == 309);
    assert(views[1].get_index()[0] == "2014-01-02");
    assert(views[1].get_index()[308] == "2018-01-18");
    assert(views[1].get_column<double>("IBM_High")[200] == 182.839996);
    assert(views[1].get_column<long>("IBM_Volume")[100] == 3721600);

    assert(views[2].get_index().size() == 256);
    assert(views[2].get_column<double>("IBM_Open").size() == 256);
    assert(views[2].get_index()[0] == "2014-11-20");
    assert(views[2].get_index()[255] == "2020-02-13");
    assert(views[2].get_column<double>("IBM_High")[200] == 156.800003);
    assert(views[2].get_column<long>("IBM_Volume")[100] == 2838100);

    assert(views[3].get_index().size() == 999);
    assert(views[3].get_column<double>("IBM_Open").size() == 999);
    assert(views[3].get_index()[0] == "2014-12-15");
    assert(views[3].get_index()[998] == "2020-10-30");
    assert(views[3].get_column<double>("IBM_High")[200] == 152.929993);
    assert(views[3].get_column<long>("IBM_Volume")[100] == 3924800);
}

// ----------------------------------------------------------------------------

static void test_remove_data_by_hampel()  {

    std::cout << "\nTesting remove_data_by_hampel( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.csv", io_format::csv2);
        assert(df.get_index().size() == 1721);
        assert(df.get_column<double>("IBM_Open").size() == 1721);
        assert(df.get_column<double>("IBM_Close").size() == 1721);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    StrDataFrame    df2 = df;

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    df.remove_data_by_hampel <double, double, long>("IBM_Close", 10);
    view.remove_data_by_hampel<double, double, long>("IBM_Close", 10);

    assert(df.get_index().size() == 1644);
    assert(view.get_index().size() == 1644);
    assert(view.get_column<double>("IBM_Open").size() == 1644);
    assert(view.get_column<long>("IBM_Volume").size() == 1644);
    assert(df.get_column<double>("IBM_Low").size() == 1644);
    assert(df.get_column<long>("IBM_Volume").size() == 1644);
    assert(df.get_index()[500] == "2016-01-25");
    assert(view.get_index()[101] == "2014-05-30");
    assert(view.get_column<double>("IBM_Open")[45] == 187.550003);
    assert(df.get_column<long>("IBM_Volume")[300] == 4255400);
}

// ----------------------------------------------------------------------------

static void test_DBSCANVisitor()  {

    std::cout << "\nTesting DBSCANVisitor{ } ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    DBSCANVisitor<double, std::string, 64>  dbscan(
        10,
        4,
        [](const double &x, const double &y)  { return (std::fabs(x - y)); });

    view.single_act_visit<double>("IBM_Close", dbscan);

    assert(dbscan.get_noisey_idxs().size() == 2);
    assert(dbscan.get_noisey_idxs()[0] == 1564);
    assert(dbscan.get_noisey_idxs()[1] == 1565);

    assert(dbscan.get_result().size() == 19);
    assert(dbscan.get_result()[0].size() == 11);
    assert(dbscan.get_result()[4].size() == 31);
    assert(dbscan.get_result()[10].size() == 294);
    assert(dbscan.get_result()[14].size() == 82);
    assert(dbscan.get_result()[18].size() == 10);
    assert(dbscan.get_result()[0][6] == 185.679993);
    assert(dbscan.get_result()[4][18] == 167.330002);
    assert(dbscan.get_result()[10][135] == 145.160004);
    assert(dbscan.get_result()[18][3] == 103.550003);
}

// ----------------------------------------------------------------------------

static void test_MeanShiftVisitor()  {

    std::cout << "\nTesting MeanShiftVisitor{ } ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    MeanShiftVisitor<double, std::string, 64>   mshift(
        1.0,
        4,
        mean_shift_kernel::gaussian,
        // mean_shift_kernel::triweight,
        [](const double &x, const double &y)  { return (std::fabs(x - y)); });

    df.single_act_visit<double>("IBM_Close", mshift);

    assert(mshift.get_result().size() == 19);
    assert(mshift.get_result()[0].size() == 106);
    assert(mshift.get_result()[4].size() == 19);
    assert(mshift.get_result()[6].size() == 274);
    assert(mshift.get_result()[10].size() == 180);
    assert(mshift.get_result()[14].size() == 29);
    assert(mshift.get_result()[18].size() == 2);
    assert(std::fabs(mshift.get_result()[0][6] - 184.16) < 0.001);
    assert(std::fabs(mshift.get_result()[4][18] - 194.0) < 0.001);
    assert(std::fabs(mshift.get_result()[6][273] - 154.31) < 0.001);
    assert(std::fabs(mshift.get_result()[10][135] - 137.61) < 0.001);
    assert(std::fabs(mshift.get_result()[18][1] - 94.77) < 0.001);
}

// ----------------------------------------------------------------------------

void test_get_data_by_dbscan()  {

    std::cout << "\nTesting get_data_by_dbscan( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.dat", io_format::binary);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    StrDataFrame    df2 = df;

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    // I am using both views and dataframes to make sure both work
    //
    auto    views =
        view.get_view_by_dbscan<double, double, long>
            ("IBM_Close", 10, 4,
             [](const double &x, const double &y) -> double  {
                 return (std::fabs(x - y));
             });
    auto    dfs =
        df.get_data_by_dbscan<double, double, long>
            ("IBM_Close", 10, 4,
             [](const double &x, const double &y) -> double  {
                 return (std::fabs(x - y));
             });

    assert(views.size() == 20);

    assert(views[0].get_index().size() == 11);
    assert(views[0].get_column<double>("IBM_Close")[7] == 184.779999);

    assert(dfs[5].get_index().size() == 127);
    assert(dfs[5].get_column<double>("IBM_Open")[15] == 162.0);

    assert(views[16].get_index().size() == 29);
    assert(views[16].get_column<double>("IBM_High")[3] == 117.75);

    // This is the last DataFrame which contains the data corresponding to
    // noisy close prices
    //
    assert(views[19].get_index().size() == 2);
    assert(views[19].get_column<long>("IBM_Volume")[0] == 10546500);
    assert(views[19].get_index()[1] == "2020-03-23");
}

// ----------------------------------------------------------------------------

void test_get_data_by_mshift()  {

    std::cout << "\nTesting get_data_by_mshift( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.dat", io_format::binary);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    StrDataFrame    df2 = df;

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    // I am using both views and dataframes to make sure both work
    //
    auto    views =
        view.get_view_by_mshift<double, double, long>
            ("IBM_Close", 1, 4, mean_shift_kernel::gaussian,
             [](const double &x, const double &y) -> double  {
                 return (std::fabs(x - y));
             });
    auto    dfs =
        df.get_data_by_mshift<double, double, long>
            ("IBM_Close", 1, 4, mean_shift_kernel::gaussian,
             [](const double &x, const double &y) -> double  {
                 return (std::fabs(x - y));
             });

    assert(views.size() == 19);
    assert(dfs.size() == 19);
    assert(views[0].get_index().size() == 106);
    assert(dfs[0].get_index().size() == 106);
    assert(views[4].get_index().size() == 19);
    assert(views[6].get_index().size() == 274);
    assert(views[10].get_index().size() == 180);
    assert(views[14].get_index().size() == 29);
    assert(views[18].get_index().size() == 2);
    assert(dfs[18].get_index().size() == 2);

    assert(
    (std::fabs(views[0].get_column<double>("IBM_Close")[7] - 185.92) < 0.001));
    assert(
    (std::fabs(dfs[5].get_column<double>("IBM_Open")[15] - 163.7) < 0.001));
    assert(
    (std::fabs(views[16].get_column<double>("IBM_High")[3] - 106.04) < 0.001));
    assert(dfs[18].get_column<long>("IBM_Volume")[0] == 10546500);
    assert(views[18].get_index()[1] == "2020-03-23");
}

// ----------------------------------------------------------------------------

static void test_view_assign()  {

    std::cout << "\nTesting view_assign( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    ptr_view =
        df.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);
    auto    view = df.get_view_by_loc<double, long>(Index2D<long>{ 100, 500 });

    StrDataFrame    df2;
    StrDataFrame    df3;

    df2.assign<decltype(ptr_view), double, long>(ptr_view);
    df3.assign<decltype(view), double, long>(view);

    assert(df2.get_index().size() == 1721);
    assert(df3.get_index().size() == 400);
    assert(std::fabs(df2.get_column<double>("IBM_Open")[100] - 184.48) < 0.001);
    assert(df3.get_column<long>("IBM_Volume")[100] == 4350200);

    StrDataFrame::View      dfv;
    StrDataFrame::PtrView   dfpv;

    dfv.assign<StrDataFrame, double, long>(df2);
    dfpv.assign<StrDataFrame, double, long>(df3);
    assert(dfv.get_index().size() == 1721);
    assert(dfpv.get_index().size() == 400);
    assert(std::fabs(dfv.get_column<double>("IBM_Open")[100] - 184.48) < 0.001);
    assert(dfpv.get_column<long>("IBM_Volume")[100] == 4350200);

    StrDataFrame::View      dfv2;
    StrDataFrame::PtrView   dfpv2;

    dfv2.assign<decltype(dfpv), double, long>(dfpv);
    dfpv2.assign<decltype(dfv), double, long>(dfv);
    assert(dfv2.get_index().size() == 400);
    assert(dfpv2.get_index().size() == 1721);
    assert((
        std::fabs(dfv2.get_column<double>("IBM_Open")[100] - 181.24) < 0.001));
    assert(dfpv2.get_column<long>("IBM_Volume")[100] == 3721600);
}

// ----------------------------------------------------------------------------

static void test_CrossCorrVisitor()  {

    std::cout << "\nTesting CrossCorrVisitor{ } ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    CrossCorrVisitor<double, std::string>   cc(-16, 16);

    df.single_act_visit<double, double>("IBM_Close", "IBM_Open", cc);

    assert(cc.get_result().size() == 32);
    assert(std::fabs(cc.get_result()[0] - 0.906) < 0.0001);
    assert(std::fabs(cc.get_result()[1] - 0.9117) < 0.0001);
    assert(std::fabs(cc.get_result()[15] - 0.9919) < 0.0001);
    assert(std::fabs(cc.get_result()[16] - 0.9971) < 0.0001);
    assert(std::fabs(cc.get_result()[30] - 0.9239) < 0.0001);
    assert(std::fabs(cc.get_result()[31] - 0.9179) < 0.0001);
}

// ----------------------------------------------------------------------------

static void test_PartialAutoCorrVisitor()  {

    std::cout << "\nTesting PartialAutoCorrVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    PartialAutoCorrVisitor<double, std::string> pacf { 50 };

    df.single_act_visit<double> ("IBM_Close", pacf);

    assert(pacf.get_result().size() == 50);
    assert(std::fabs(pacf.get_result()[0] - 1.0) < 0.000001);
    assert(std::fabs(pacf.get_result()[1] - 0.999915) < 0.000001);
    assert(std::fabs(pacf.get_result()[10] - 0.982959) < 0.000001);
    assert(std::fabs(pacf.get_result()[30] - 0.983226) < 0.000001);
    assert(std::fabs(pacf.get_result()[48] - 0.98751) < 0.000001);
    assert(std::fabs(pacf.get_result()[49] - 0.987886) < 0.000001);
}

// ----------------------------------------------------------------------------

static void test_make_stationary()  {

    std::cout << "\nTesting make_stationary( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto    df2 = df;
    auto    df3 = df;
    auto    df4 = df;
    auto    df5 = df;
    auto    df6 = df;

    df.make_stationary<double>("IBM_Close", stationary_method::differencing);

    const auto  &close = df.get_column<double>("IBM_Close");

    assert(close.size() == 5031);
    assert(std::fabs(close[0] - 3.375) < 0.001);
    assert(std::fabs(close[1] - 3.375) < 0.001);
    assert(std::fabs(close[702] - 0.120003) < 0.0001);
    assert(std::fabs(close[1695] - -1.34) < 0.001);
    assert(std::fabs(close[5029] - 2.26) < 0.001);
    assert(std::fabs(close[5030] - 2.75) < 0.001);

    df2.make_stationary<double>("IBM_Close", stationary_method::log_trans);

    const auto  &close2 = df2.get_column<double>("IBM_Close");

    assert(close2.size() == 5031);
    assert(std::fabs(close2[0] - 4.59069) < 0.0001);
    assert(std::fabs(close2[1] - 4.62436) < 0.0001);
    assert(std::fabs(close2[702] - 4.41848) < 0.0001);
    assert(std::fabs(close2[1695] - 4.71752) < 0.0001);
    assert(std::fabs(close2[5029] - 4.69052) < 0.0001);
    assert(std::fabs(close2[5030] - 4.71546) < 0.0001);

    df3.make_stationary<double>("IBM_Close", stationary_method::sqrt_trans);

    const auto  &close3 = df3.get_column<double>("IBM_Close");

    assert(close3.size() == 5031);
    assert(std::fabs(close3[0] - 9.92786) < 0.0001);
    assert(std::fabs(close3[1] - 10.0964) < 0.0001);
    assert(std::fabs(close3[702] - 9.10879) < 0.0001);
    assert(std::fabs(close3[1695] - 10.5778) < 0.0001);
    assert(std::fabs(close3[5029] - 10.436) < 0.001);
    assert(std::fabs(close3[5030] - 10.5669) < 0.0001);

    df4.make_stationary<double>("IBM_Close",
                                stationary_method::boxcox_trans,
                                { .bc_type = box_cox_type::original,
                                  .lambda = 1.5,
                                  .is_all_positive = true });

    const auto  &close4 = df4.get_column<double>("IBM_Close");

    assert(close4.size() == 5031);
    assert(std::fabs(close4[0] - 651.677) < 0.001);
    assert(std::fabs(close4[1] - 685.469) < 0.001);
    assert(std::fabs(close4[702] - 503.171) < 0.001);
    assert(std::fabs(close4[1695] - 788.367) < 0.001);
    assert(std::fabs(close4[5029] - 757.056) < 0.001);
    assert(std::fabs(close4[5030] - 785.936) < 0.001);

    df5.make_stationary<double>(
        "IBM_Close",
        stationary_method::decomposition,
        { .season_period = 132,
          .dcom_fraction = 0.6667,
          .dcom_delta = 0,
          .dcom_type = decompose_type::multiplicative });

    const auto  &close5 = df5.get_column<double>("IBM_Close");

    assert(close5.size() == 5031);
    assert(std::fabs(close5[0] - 1.20614) < 0.0001);
    assert(std::fabs(close5[1] - 1.2488) < 0.0001);
    assert(std::fabs(close5[702] - 0.926802) < 0.0001);
    assert(std::fabs(close5[1695] - 1.04238) < 0.0001);
    assert(std::fabs(close5[5029] - 0.817188) < 0.001);
    assert(std::fabs(close5[5030] - 0.836598) < 0.0001);

    df6.make_stationary<double>("IBM_Close",
                                stationary_method::smoothing,
                                { .decay_spec = exponential_decay_spec::span,
                                  .decay_alpha = 1.5,
                                  .finite_adjust = true });

    const auto  &close6 = df6.get_column<double>("IBM_Close");

    assert(close6.size() == 5031);
    assert(std::fabs(close6[0] - 98.5625) < 0.0001);
    assert(std::fabs(close6[1] - 101.375) < 0.0001);
    assert(std::fabs(close6[702] - 82.9529) < 0.0001);
    assert(std::fabs(close6[1695] - 112.106) < 0.001);
    assert(std::fabs(close6[5029] - 108.634) < 0.001);
    assert(std::fabs(close6[5030] - 111.055) < 0.001);
}

// ----------------------------------------------------------------------------

static void test_StationaryCheckVisitor()  {

    std::cout << "\nTesting StationaryCheckVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    RandGenParams<double>   p;

    p.mean = 0;
    p.std = 1;
    p.seed = 123;

    df.load_column("normal_col",
                   gen_normal_dist<double>(df.get_index().size(), p));

    p.max_value = 1000;
    p.min_value = -1000;
    df.load_column("uniform col",
                   gen_uniform_real_dist<double>(df.get_index().size(), p));

    std::vector<double> log_close;

    log_close.reserve(df.get_index().size());
    for (const auto val : df.get_column<double>("IBM_Close"))
        log_close.push_back(std::log(val));
    df.load_column("log close", std::move(log_close));

    DecomposeVisitor<double, std::string>   d_v (280, 0.6, 0.01);

    df.single_act_visit<double>("IBM_Close", d_v);
    df.load_column("residual close", std::move(d_v.get_residual()));

    // KPSS tests
    //
    StationaryCheckVisitor<double, std::string> sc { stationary_test::kpss };

    df.single_act_visit<double>("IBM_Close", sc);
    assert(std::fabs(sc.get_kpss_value() - 63.5831) < 0.0001);
    assert(sc.get_kpss_statistic() == 0);

    df.single_act_visit<double>("normal_col", sc);
    assert(sc.get_kpss_value() < 0.078);
    assert(sc.get_kpss_statistic() == 0.1);

    df.single_act_visit<double>("uniform col", sc);
    assert(sc.get_kpss_value() < 0.08);
    assert(sc.get_kpss_statistic() == 0.1);

    df.single_act_visit<double>("log close", sc);
    assert(sc.get_kpss_value() < 62.7013);
    assert(sc.get_kpss_statistic() == 0);

    df.single_act_visit<double>("residual close", sc);
    assert(sc.get_kpss_value() < 46.41);
    assert(sc.get_kpss_statistic() == 0);

    // ADF tests
    //
    StationaryCheckVisitor<double, std::string> sc2 {
        stationary_test::adf, { .adf_lag = 10, .adf_with_trend = false } };

    df.single_act_visit<double>("IBM_Close", sc2);
    assert(std::fabs(sc2.get_adf_statistic() - 0.989687) < 0.00001);

    StationaryCheckVisitor<double, std::string> sc3 {
        stationary_test::adf, { .adf_lag = 25, .adf_with_trend = false } };

    df.single_act_visit<double>("IBM_Close", sc3);
    assert(std::fabs(sc3.get_adf_statistic() - 0.974531) < 0.0000001);

    StationaryCheckVisitor<double, std::string> sc4 {
        stationary_test::adf, { .adf_lag = 10, .adf_with_trend = false } };

    df.single_act_visit<double>("normal_col", sc4);
    assert(std::fabs(sc4.get_adf_statistic() - 0.0289613) < 0.0000001);

    StationaryCheckVisitor<double, std::string> sc5 {
        stationary_test::adf, { .adf_lag = 25, .adf_with_trend = false } };

    df.single_act_visit<double>("normal_col", sc5);
    assert(std::fabs(sc5.get_adf_statistic() - 0.0208191) < 0.0000001);

    // ADF tests with trend
    //
    StationaryCheckVisitor<double, std::string> sc6 {
        stationary_test::adf, { .adf_lag = 10, .adf_with_trend = true } };

    df.single_act_visit<double>("IBM_Close", sc6);
    assert(std::fabs(sc6.get_adf_statistic() - 0.977705) < 0.000001);

    StationaryCheckVisitor<double, std::string> sc7 {
        stationary_test::adf, { .adf_lag = 25, .adf_with_trend = true } };

    df.single_act_visit<double>("IBM_Close", sc7);
    assert(std::fabs(sc7.get_adf_statistic() - 0.946614) < 0.000001);

    StationaryCheckVisitor<double, std::string> sc8 {
        stationary_test::adf, { .adf_lag = 10, .adf_with_trend = true } };

    df.single_act_visit<double>("normal_col", sc8);
    assert(std::fabs(sc8.get_adf_statistic() - 0.0289582) < 0.0000001);

    StationaryCheckVisitor<double, std::string> sc9 {
        stationary_test::adf, { .adf_lag = 25, .adf_with_trend = true } };

    df.single_act_visit<double>("normal_col", sc9);
    assert(std::fabs(sc9.get_adf_statistic() - 0.020812) < 0.0000001);

    StationaryCheckVisitor<double, std::string> sc10 {
        stationary_test::adf, { .adf_lag = 10, .adf_with_trend = true } };

    df.single_act_visit<double>("log close", sc10);
    assert(std::fabs(sc10.get_adf_statistic() - 0.972062) < 0.000001);

    StationaryCheckVisitor<double, std::string> sc11 {
        stationary_test::adf, { .adf_lag = 10, .adf_with_trend = true } };

    df.single_act_visit<double>("residual close", sc11);
    assert(std::fabs(sc11.get_adf_statistic() - 0.679027) < 0.000001);
}

// ----------------------------------------------------------------------------

static void test_covariance_matrix()  {

    std::cout << "\nTesting covariance_matrix( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  cov_mat =
        df.covariance_matrix<double>({ "IBM_Close", "IBM_Open",
                                       "IBM_High", "IBM_Low" });

    assert(cov_mat.rows() == 4);
    assert(cov_mat.cols() == 4);
    assert(std::fabs(cov_mat(0, 0) - 1467.58) < 0.01);
    assert(std::fabs(cov_mat(0, 2) - 1469.69) < 0.01);
    assert(std::fabs(cov_mat(2, 1) - 1469.48) < 0.01);
    assert(std::fabs(cov_mat(2, 2) - 1472.86) < 0.01);
    assert(std::fabs(cov_mat(3, 2) - 1466.15) < 0.01);
    assert(std::fabs(cov_mat(3, 3) - 1461.0) < 0.01);

    const auto  cov_mat2 =
        df.covariance_matrix<double>({ "IBM_Close", "IBM_Open",
                                       "IBM_High", "IBM_Low" },
                                     normalization_type::z_score);

    assert(cov_mat2.rows() == 4);
    assert(cov_mat2.cols() == 4);
    assert(std::fabs(cov_mat2(0, 0) - 1.0) < 0.01);
    assert(std::fabs(cov_mat2(0, 2) - 0.99964) < 0.00001);
    assert(std::fabs(cov_mat2(2, 1) - 0.99963) < 0.00001);
    assert(std::fabs(cov_mat2(2, 2) - 1.0) < 0.01);
    assert(std::fabs(cov_mat2(3, 2) - 0.99948) < 0.00001);
    assert(std::fabs(cov_mat2(3, 3) - 1.0) < 0.01);
}

// ----------------------------------------------------------------------------

static void test_pca_by_eigen()  {

    std::cout << "\nTesting pca_by_eigen( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  pca_mat = df.pca_by_eigen<double>(
        { "IBM_Close", "IBM_Open", "IBM_High", "IBM_Low" });

    // Dimensions were reduced to 1 containing at least 90% of the information.
    // This makes sense, since these 4 columns are highly correlated.
    //
    assert(pca_mat.cols() == 1);
    assert(pca_mat.rows() == 5031);
    assert(std::fabs(pca_mat(0, 0) - 197.063) < 0.001);
    assert(std::fabs(pca_mat(1, 0) - 200.875) < 0.001);
    assert(std::fabs(pca_mat(491, 0) - 149.02) < 0.01);
    assert(std::fabs(pca_mat(1348, 0) - 166.44) < 0.01);
    assert(std::fabs(pca_mat(2677, 0) - 333.405) < 0.001);
    assert(std::fabs(pca_mat(5029, 0) - 216.175) < 0.001);
    assert(std::fabs(pca_mat(5030, 0) - 219.555) < 0.001);

    const auto  pca_mat2 = df.pca_by_eigen<double>(
        { "IBM_Close", "IBM_Open", "IBM_High", "IBM_Low" },
        { .num_comp_to_keep = 3 });

    // 3 most significant dimensions are kept.
    // As you can see the first column is unchanged and clearly contains
    // almost all of the information.
    //
    assert(pca_mat2.cols() == 3);
    assert(pca_mat2.rows() == 5031);

    assert(std::fabs(pca_mat2(0, 0) - 197.063) < 0.001);
    assert(std::fabs(pca_mat2(0, 1) - -0.0951913) < 0.001);
    assert(std::fabs(pca_mat2(0, 2) - 1.85473) < 0.001);

    assert(std::fabs(pca_mat2(1, 0) - 200.875) < 0.001);
    assert(std::fabs(pca_mat2(1, 1) - -2.08604) < 0.001);
    assert(std::fabs(pca_mat2(1, 2) - 2.68895) < 0.001);

    assert(std::fabs(pca_mat2(491, 0) - 149.02) < 0.01);
    assert(std::fabs(pca_mat2(491, 1) - -1.34957) < 0.01);
    assert(std::fabs(pca_mat2(491, 2) - 2.09026) < 0.01);

    assert(std::fabs(pca_mat2(1348, 0) - 166.44) < 0.01);
    assert(std::fabs(pca_mat2(1348, 1) - 0.0354559) < 0.01);
    assert(std::fabs(pca_mat2(1348, 2) - 0.41972) < 0.01);

    assert(std::fabs(pca_mat2(2677, 0) - 333.405) < 0.001);
    assert(std::fabs(pca_mat2(2677, 1) - -1.33686) < 0.001);
    assert(std::fabs(pca_mat2(2677, 2) - 2.13684) < 0.001);

    assert(std::fabs(pca_mat2(5029, 0) - 216.175) < 0.001);
    assert(std::fabs(pca_mat2(5029, 1) - -1.18141) < 0.001);
    assert(std::fabs(pca_mat2(5029, 2) - 2.18029) < 0.001);

    assert(std::fabs(pca_mat2(5030, 0) - 219.555) < 0.001);
    assert(std::fabs(pca_mat2(5030, 1) - -2.66858) < 0.001);
    assert(std::fabs(pca_mat2(5030, 2) - 2.85412) < 0.001);
}

// ----------------------------------------------------------------------------

static void test_compact_svd()  {

    std::cout << "\nTesting compact_svd( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  [U, S, V] =
        df.compact_svd<double>(
            { "IBM_Close", "IBM_Open", "IBM_High", "IBM_Low" });

    assert(U.rows() == 5031);
    // Compact version has the same column # as the original matrix
    //
    assert(U.cols() == 4);
    assert(std::fabs(U(0, 0) - -0.0115747) < 0.000001);
    assert(std::fabs(U(2, 3) - -0.0110622) < 0.000001);
    assert(std::fabs(U(4040, 2) - -0.0147074) < 0.000001);
    assert(std::fabs(U(4994, 1) - 0.0194639) < 0.000001);
    assert(std::fabs(U(5030, 3) - -0.000878688) < 0.000001);

    // In compact version zero rows at the end are omitted
    //
    assert(S.rows() == 4);
    assert(S.cols() == 4);
    assert(std::fabs(S(0, 0) - 141.821) < 0.001);
    assert(std::fabs(S(1, 1) - 1.91734) < 0.00001);
    assert(std::fabs(S(2, 2) - 1.62214) < 0.00001);
    assert(std::fabs(S(3, 3) - 0.73194) < 0.00001);
    assert(S(0, 2) == 0.0);
    assert(S(1, 2) == 0.0);
    assert(S(3, 0) == 0.0);

    assert(V.rows() == 4);
    assert(V.cols() == 4);
    assert(std::fabs(V(0, 0) - 0.499988) < 0.000001);
    assert(std::fabs(V(0, 2) - 0.003710) < 0.000001);
    assert(std::fabs(V(2, 2) - 0.700869) < 0.000001);
    assert(std::fabs(V(3, 1) - -0.00079) < 0.000001);
    assert(std::fabs(V(3, 3) - 0.491216) < 0.000001);
}

// ----------------------------------------------------------------------------

static void test_SpectralClusteringVisitor()  {

    std::cout << "\nTesting SpectralClusteringVisitor{ } ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.csv", io_format::csv2,
                 { .starting_row = 0, .num_rows = 1000 });
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    spect_v<4, double, std::string, 64> spc(1000, 7, 123);

    df.single_act_visit<double>("IBM_Close", spc);

    const auto  &clusters = spc.get_result();

    assert(clusters.size() == 4);

    assert(clusters[0].size() == 89);
    assert(clusters[1].size() == 688);
    assert(clusters[2].size() == 208);
    assert(clusters[3].size() == 15);

    assert(std::fabs(clusters[0][0] - 177.9) < 0.001);
    assert(std::fabs(clusters[0][23] - 171.12) < 0.001);
    assert(std::fabs(clusters[0][61] - 177.18) < 0.001);
    assert(std::fabs(clusters[0][88] - 170.05) < 0.001);

    assert(std::fabs(clusters[1][0] - 169.1) < 0.001);
    assert(std::fabs(clusters[1][300] - 140.19) < 0.001);
    assert(std::fabs(clusters[1][542] - 152.51) < 0.001);
    assert(std::fabs(clusters[1][687] - 153.23) < 0.001);

    assert(std::fabs(clusters[2][0] - 185.53) < 0.001);
    assert(std::fabs(clusters[2][100] - 181.22) < 0.001);
    assert(std::fabs(clusters[2][200] - 179.4) < 0.001);
    assert(std::fabs(clusters[2][207] - 179.45) < 0.001);

    assert(std::fabs(clusters[3][0] - 121.86) < 0.001);
    assert(std::fabs(clusters[3][8] - 124.83) < 0.001);
    assert(std::fabs(clusters[3][12] - 120.19) < 0.001);
    assert(std::fabs(clusters[3][14] - 122.74) < 0.001);
}

// ----------------------------------------------------------------------------

static void test_get_data_by_spectral()  {

    std::cout << "\nTesting get_data_by_spectral( ) ..." << std::endl;

    typedef StdDataFrame64<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("SHORT_IBM.csv", io_format::csv2,
                 { .starting_row = 1000, .num_rows = 500 });
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    StrDataFrame    df2 = df;

    auto    lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double, long>
            ("IBM_Open", lbd);

    auto    result_df =
        df.get_data_by_spectral <3, double, double, long>("IBM_Close", 8, 89);
    auto    result_view =
        view.get_view_by_spectral<3, double, double, long>("IBM_Close", 8, 89);

    assert(result_df.size() == 3);
    assert(result_df.size() == result_view.size());

    assert(result_df[0].get_index().size() == 47);
    assert(result_df[0].get_column<double>("IBM_Open").size() == 47);
    assert(result_df[0].get_index()[0] == "2018-10-29");
    assert(result_df[0].get_index()[46] == "2019-01-22");
    assert(result_df[0].get_column<double>("IBM_High")[20] == 121.68);
    assert(result_df[0].get_column<long>("IBM_Volume")[35] == 4346700);
    assert(result_view[0].get_index().size() == 47);
    assert(result_view[0].get_column<double>("IBM_Open").size() == 47);
    assert(result_view[0].get_index()[0] == "2018-10-29");
    assert(result_view[0].get_index()[46] == "2019-01-22");
    assert(result_view[0].get_column<double>("IBM_High")[20] == 121.68);
    assert(result_view[0].get_column<long>("IBM_Volume")[35] == 4346700);

    assert(result_df[1].get_index().size() == 1);
    assert(result_df[1].get_column<double>("IBM_Open").size() == 1);
    assert(result_df[1].get_index()[0] == "2018-12-24");
    assert(result_df[1].get_column<double>("IBM_High")[0] == 111.0);
    assert(result_df[1].get_column<long>("IBM_Volume")[0] == 3821400);
    assert(result_view[1].get_column<double>("IBM_High")[0] == 111.0);
    assert(result_view[1].get_column<long>("IBM_Volume")[0] == 3821400);

    assert(result_df[2].get_index().size() == 452);
    assert(result_df[2].get_column<double>("IBM_Open").size() == 452);
    assert(result_df[2].get_index()[0] == "2017-12-20");
    assert(result_df[2].get_index()[451] == "2019-12-16");
    assert(result_df[2].get_column<double>("IBM_High")[200] == 149.070007);
    assert(result_df[2].get_column<long>("IBM_Volume")[300] == 4958000);
    assert(result_view[2].get_index().size() == 452);
    assert(result_view[2].get_column<double>("IBM_Open").size() == 452);
    assert(result_view[2].get_index()[0] == "2017-12-20");
    assert(result_view[2].get_index()[451] == "2019-12-16");
    assert(result_view[2].get_column<double>("IBM_High")[200] == 149.070007);
    assert(result_view[2].get_column<long>("IBM_Volume")[300] == 4958000);
}

// ----------------------------------------------------------------------------

static void test_canon_corr()  {

    std::cout << "\nTesting canon_corr( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result = df.canon_corr<double>({ "IBM_Close", "IBM_Open" },
                                               { "IBM_High", "IBM_Low" });

    assert(result.coeffs.size() == 2);
    assert(std::fabs(result.coeffs[0] - 0.999944) < 0.000001);
    assert(std::fabs(result.coeffs[1] - 0.262927) < 0.000001);
    assert(std::fabs(result.x_red_idx - 0.534073) < 0.000001);
    assert(std::fabs(result.y_red_idx - 0.535897) < 0.000001);
}

// ----------------------------------------------------------------------------

static void test_MC_station_dist()  {

    std::cout << "\nTesting MC_station_dist( ) ..." << std::endl;

    const std::size_t   item_cnt = 20;
    MyDataFrame         df;

    df.load_index(MyDataFrame::gen_sequence_index(0, item_cnt, 1));

    RandGenParams<double>       p;
    std::vector<const char *>   col_names (item_cnt, nullptr);

    p.seed = 16;
    df.load_column("0_col_name", gen_normal_dist<double, 256>(item_cnt, p));
    for (std::size_t i = 1; i < item_cnt; ++i)  {
        p.seed = i;
        df.load_column((std::to_string(i) + "_col_name").c_str(),
                       gen_normal_dist<double, 256>(item_cnt, p));
    }
    for (std::size_t i = 0; i < item_cnt; ++i)
        col_names[i] = df.col_idx_to_name(i);

    const auto  result =
        df.MC_station_dist<double>(
            std::forward<std::vector<const char *>>(col_names));

    assert(result.size() == 20);
    assert(std::fabs(result[0] - -0.45676) < 0.000001);
    assert(std::fabs(result[5] - -0.0338666) < 0.000001);
    assert(std::fabs(result[15] - -0.10117) < 0.000001);
    assert(std::fabs(result[19] - 0.232359) < 0.000001);
}

// ----------------------------------------------------------------------------

static void test_SeasonalPeriodVisitor()  {

    std::cout << "\nTesting SeasonalPeriodVisitor{ } ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("IcecreamProduction.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    ssp_v<double, DateTime> ssp({ .de_serial_corr = true});

    df.single_act_visit<double>("IceCreamProduction", ssp);

    assert(std::fabs(ssp.get_max_magnitude() - 4073.55) < 0.01);
    assert(ssp.get_dominant_index() == 53);
    assert(std::fabs(ssp.get_dominant_frequency() - 0.08346) < 0.00001);
    assert(std::fabs(ssp.get_period() - 11.9811) < 0.0001);
    assert(ssp.get_period() == ssp.get_result());
}

// ----------------------------------------------------------------------------

static void test_read_data_file_with_schema()  {

    std::cout << "\nTesting test_read_data_file_with_schema{ } ..."
              << std::endl;

    MyDataFrame             df1;
    MyDataFrame             df2;
    ReadParams::SchemaVec   schema {  // First is the index column
        { .col_type = file_dtypes::ULONG, .num_rows = 12, .col_idx = 0 },
        { "Open", file_dtypes::DOUBLE, 12, 1 },
        { "High", file_dtypes::DOUBLE, 12, 2 },
        { "Low", file_dtypes::DOUBLE, 12, 3 },
        { "Close", file_dtypes::DOUBLE, 12, 4 },
        { "Adj_Close", file_dtypes::DOUBLE, 12, 5 },
        { "Volume", file_dtypes::LONG, 12, 6 },
    };

    try  {
        df1.read("SchemaWithHeader.csv", io_format::csv2, { .schema = schema });
        df2.read("SchemaWithoutHeader.csv", io_format::csv2,
                 { .skip_first_line = false, .schema = schema });
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    assert(df1.get_index().size() == 12);
    assert(df1.get_index()[0] == 1);
    assert(df1.get_index()[6] == 7);
    assert(df1.get_index()[11] == 12);
    assert(df1.get_column<double>("Close").size() == 12);
    assert((std::fabs(df1.get_column<double>("Close")[0] - 185.53) < 0.01));
    assert((std::fabs(df1.get_column<double>("Close")[6] - 187.26) < 0.01));
    assert((std::fabs(df1.get_column<double>("Close")[11] - 190.09) < 0.01));
    assert(df1.get_column<long>("Volume").size() == 12);
    assert(df1.get_column<long>("Volume")[0] == 4546500);
    assert(df1.get_column<long>("Volume")[6] == 4022400);
    assert(df1.get_column<long>("Volume")[11] == 7644600);

    assert(df2.get_index().size() == 12);
    assert(df2.get_index()[0] == 1);
    assert(df2.get_index()[6] == 7);
    assert(df2.get_index()[11] == 12);
    assert(df2.get_column<double>("Close").size() == 12);
    assert((std::fabs(df2.get_column<double>("Close")[0] - 185.53) < 0.01));
    assert((std::fabs(df2.get_column<double>("Close")[6] - 187.26) < 0.01));
    assert((std::fabs(df2.get_column<double>("Close")[11] - 190.09) < 0.01));
    assert(df2.get_column<long>("Volume").size() == 12);
    assert(df2.get_column<long>("Volume")[0] == 4546500);
    assert(df2.get_column<long>("Volume")[6] == 4022400);
    assert(df2.get_column<long>("Volume")[11] == 7644600);
}

// ----------------------------------------------------------------------------

static void test_knn()  {

    std::cout << "\nTesting knn( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result =
        df.knn<double>({ "IBM_Open", "IBM_High", "IBM_Low", "IBM_Close" },
                       { 78.95, 80.48, 78.35, 80.48 }, 4);

    assert(result.size() == 4);

    assert(result[0].second == 500);  // Index into the IBM data columns
    assert(result[0].first.size() == 4);
    assert((std::fabs(result[0].first[0] - 78.9) < 0.01));
    assert((std::fabs(result[0].first[2] - 78.32) < 0.01));
    assert((std::fabs(result[0].first[3] - 80.4) < 0.01));

    assert(result[1].second == 541);  // Index into the IBM data columns
    assert(result[1].first.size() == 4);
    assert((std::fabs(result[1].first[0] - 78.8) < 0.01));
    assert((std::fabs(result[1].first[2] - 78.19) < 0.01));
    assert((std::fabs(result[1].first[3] - 80.57) < 0.01));

    assert(result[2].second == 558);  // Index into the IBM data columns
    assert(result[2].first.size() == 4);
    assert((std::fabs(result[2].first[0] - 78.5) < 0.01));
    assert((std::fabs(result[2].first[2] - 78.36) < 0.01));
    assert((std::fabs(result[2].first[3] - 80.11) < 0.01));

    assert(result[3].second == 1232);  // Index into the IBM data columns
    assert(result[3].first.size() == 4);
    assert((std::fabs(result[3].first[0] - 79.25) < 0.01));
    assert((std::fabs(result[3].first[2] - 78.87) < 0.01));
    assert((std::fabs(result[3].first[3] - 80.36) < 0.01));
}

// ----------------------------------------------------------------------------

static void test_DynamicTimeWarpVisitor()  {

    std::cout << "\nTesting DynamicTimeWarpVisitor{ } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    dtw_v<double, std::string>  dtw;

    df.single_act_visit<double, double>("IBM_Open", "IBM_Close", dtw);
    assert((std::fabs(dtw.get_result() - 2682.91) < 0.01));

    df.single_act_visit<double, double>("IBM_High", "IBM_Low", dtw);
    assert((std::fabs(dtw.get_result() - 6255.33) < 0.01));

    df.single_act_visit<double, double>("IBM_Open", "IBM_Low", dtw);
    assert((std::fabs(dtw.get_result() - 3649.94) < 0.01));

    df.single_act_visit<double, double>("IBM_Close", "IBM_Low", dtw);
    assert((std::fabs(dtw.get_result() - 3851.43) < 0.01));

    df.single_act_visit<double, double>("IBM_Open", "IBM_High", dtw);
    assert((std::fabs(dtw.get_result() - 3833.26) < 0.01));

    df.single_act_visit<double, double>("IBM_Close", "IBM_High", dtw);
    assert((std::fabs(dtw.get_result() - 3737.44) < 0.01));

    DynamicTimeWarpVisitor<double, std::string> dtw2 (
        normalization_type::z_score);

    df.single_act_visit<double, double>("IBM_Open", "IBM_Close", dtw2);
    assert((std::fabs(dtw2.get_result() - 70.4392) < 0.0001));

    df.single_act_visit<double, double>("IBM_High", "IBM_Low", dtw2);
    assert((std::fabs(dtw2.get_result() - 90.0254) < 0.0001));

    df.single_act_visit<double, double>("IBM_Open", "IBM_Low", dtw2);
    assert((std::fabs(dtw2.get_result() - 77.116) < 0.0001));

    df.single_act_visit<double, double>("IBM_Close", "IBM_Low", dtw2);
    assert((std::fabs(dtw2.get_result() - 76.7581) < 0.0001));

    df.single_act_visit<double, double>("IBM_Open", "IBM_High", dtw2);
    assert((std::fabs(dtw2.get_result() - 77.2208) < 0.0001));

    df.single_act_visit<double, double>("IBM_Close", "IBM_High", dtw2);
    assert((std::fabs(dtw2.get_result() - 77.2344) < 0.0001));
}

// ----------------------------------------------------------------------------

static void test_AnomalyDetectByFFTVisitor()  {

    std::cout << "\nTesting AnomalyDetectByFFTVisitor{ } ..." << std::endl;

    constexpr std::size_t   item_cnt = 1024;
    MyStdDataFrame          df;

    df.load_index(MyStdDataFrame::gen_sequence_index(0, item_cnt, 1));

    std::vector<double>   sine_col;

    sine_col.reserve(item_cnt);
    for (std::size_t i = 0; i < item_cnt; ++i)  {
        sine_col.push_back(std::sin(2.0 * M_PI * i / 20.0)); // Base sine wave
        if (i % 30 == 0)  sine_col.back() += 2.0;  // Inject anomalies
    }
    df.load_column("sine col", std::move(sine_col));

    // Keep at least 10% of the frequencies as dominant frequencies.
    //
    and_fft_v<double>               anomaly1(100, 1.0);
    const std::vector<std::size_t>  result1 =
        { 0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360, 390, 420,
          450, 480, 510, 540, 570, 600, 630, 660, 690, 720, 750, 780, 810, 840,
          870, 900, 930, 960, 990, 1020 };

    df.single_act_visit<double>("sine col", anomaly1);
    assert((anomaly1.get_result() == result1));

    and_fft_v<double>   anomaly2(10, 1.5);

    df.single_act_visit<double>("sine col", anomaly2);
    assert((anomaly2.get_result() == result1));

    and_fft_v<double>   anomaly3(100, 1.0, normalization_type::z_score);

    df.single_act_visit<double>("sine col", anomaly3);
    assert((anomaly3.get_result() == result1));

    and_fft_v<double>   anomaly4(10, 1.5, normalization_type::z_score);

    df.single_act_visit<double>("sine col", anomaly4);
    assert((anomaly4.get_result() == result1));

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
    ibm.get_column<double>("IBM_Close")[502] = 800.0;
    ibm.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm.get_column<double>("IBM_Close")[2002] = 850.0;

    // Keep at least 10% of the frequencies as dominant frequencies.
    // In case of IBM market data, I had to keep more
    //
    and_fft_v<double, std::string>  anomaly5(1000, 80.0);
    const std::vector<std::size_t>  result2 =
        { 500, 501, 502, 503, 504, 998, 999, 1000, 1001, 1002, 1003, 2000,
          2001, 2002, 2003, 2004 };

    ibm.single_act_visit<double>("IBM_Close", anomaly5);
    assert((anomaly5.get_result() == result2));

    and_fft_v<double, std::string>  anomaly6(1000, 250.0,
                                             normalization_type::z_score);
    const std::vector<std::size_t>  result3 = { 502, 1001, 2002 };

    ibm.single_act_visit<double>("IBM_Close", anomaly6);
    assert((anomaly6.get_result() == result3));
}

// ----------------------------------------------------------------------------

static void test_remove_data_by_fft()  {

    std::cout << "\nTesting remove_data_by_fft( ) ..." << std::endl;

    constexpr std::size_t   item_cnt = 1024;
    MyStdDataFrame          df;

    df.load_index(MyStdDataFrame::gen_sequence_index(0, item_cnt, 1));

    std::vector<double>   sine_col;

    sine_col.reserve(item_cnt);
    for (std::size_t i = 0; i < item_cnt; ++i)  {
        sine_col.push_back(std::sin(2.0 * M_PI * i / 20.0)); // Base sine wave
        if (i % 30 == 0)  sine_col.back() += 2.0;  // Inject anomalies
    }
    df.load_column("sine col", std::move(sine_col));

    MyStdDataFrame  df2 = df;

    auto    lbd =
        [](const unsigned long &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double>("sine col", lbd);

    assert((df.get_column<double>("sine col").size() == 1024));
    assert((view.get_column<double>("sine col").size() == 1024));

    // Keep at least 10% of the frequencies as dominant frequencies.
    //
    df.remove_data_by_fft<double, double>("sine col", 100, 1.0);
    assert((df.get_column<double>("sine col").size() == (1024 - 35)));

    view.remove_data_by_fft<double, double>("sine col", 100, 1.0);
    assert((view.get_column<double>("sine col").size() == (1024 - 35)));

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
    ibm.get_column<double>("IBM_Close")[502] = 800.0;
    ibm.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm.get_column<double>("IBM_Close")[2002] = 850.0;

    StrDataFrame    ibm2 = ibm;
    auto            ibm_lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto            ibm_view =
        ibm2.get_view_by_sel<double, decltype(ibm_lbd), double, long>
            ("IBM_Open", ibm_lbd);

    ibm_view.get_column<double>("IBM_Close")[502] = 800.0;
    ibm_view.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm_view.get_column<double>("IBM_Close")[2002] = 850.0;

    assert((ibm.get_column<double>("IBM_Open").size() == 5031));
    assert((ibm_view.get_column<double>("IBM_Open").size() == 5031));

    ibm.remove_data_by_fft<double, double, long>("IBM_Close", 1000, 250,
                                                 normalization_type::z_score);
    assert((ibm.get_column<double>("IBM_Open").size() == (5031 - 3)));

    ibm_view.remove_data_by_fft<double, double, long>
        ("IBM_Close", 1000, 250, normalization_type::z_score);
    assert((ibm_view.get_column<double>("IBM_Open").size() == (5031 - 3)));
}

// ----------------------------------------------------------------------------

static void test_AnomalyDetectByIQRVisitor()  {

    std::cout << "\nTesting AnomalyDetectByIQRVisitor{ } ..." << std::endl;

    constexpr std::size_t   item_cnt = 1024;
    MyStdDataFrame          df;

    df.load_index(MyStdDataFrame::gen_sequence_index(0, item_cnt, 1));

    std::vector<double>   sine_col;

    sine_col.reserve(item_cnt);
    for (std::size_t i = 0; i < item_cnt; ++i)  {
        sine_col.push_back(std::sin(2.0 * M_PI * i / 20.0)); // Base sine wave
        if (i % 30 == 0)  sine_col.back() += 2.0;  // Inject anomalies
    }
    df.load_column("sine col", std::move(sine_col));

    and_iqr_v<double>               anomaly1(0.5, 0.5);
    const std::vector<std::size_t>  result1 =
        { 0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360, 390, 420,
          450, 480, 510, 540, 570, 600, 630, 660, 690, 720, 750, 780, 810, 840,
          870, 900, 930, 960, 990, 1020 };

    df.single_act_visit<double>("sine col", anomaly1);
    assert((anomaly1.get_result() == result1));

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
    ibm.get_column<double>("IBM_Close")[502] = 800.0;
    ibm.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm.get_column<double>("IBM_Close")[2002] = 850.0;

    and_iqr_v<double>               anomaly2;
    const std::vector<std::size_t>  result2 = { 502, 1001, 2002 };

    ibm.single_act_visit<double>("IBM_Close", anomaly2);
    assert((anomaly2.get_result() == result2));
}

// ----------------------------------------------------------------------------

static void test_AnomalyDetectByZScoreVisitor()  {

    std::cout << "\nTesting AnomalyDetectByZScoreVisitor{ } ..." << std::endl;

    constexpr std::size_t   item_cnt = 1024;
    MyStdDataFrame          df;

    df.load_index(MyStdDataFrame::gen_sequence_index(0, item_cnt, 1));

    std::vector<double>   sine_col;

    sine_col.reserve(item_cnt);
    for (std::size_t i = 0; i < item_cnt; ++i)  {
        sine_col.push_back(std::sin(2.0 * M_PI * i / 20.0)); // Base sine wave
        if (i % 30 == 0)  sine_col.back() += 2.0;  // Inject anomalies
    }
    df.load_column("sine col", std::move(sine_col));

    and_zscr_v<double>              anomaly1 { 2.0 };
    const std::vector<std::size_t>  result1 =
        { 0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360, 390, 420,
          450, 480, 510, 540, 570, 600, 630, 660, 690, 720, 750, 780, 810, 840,
          870, 900, 930, 960, 990, 1020 };

    df.single_act_visit<double>("sine col", anomaly1);
    assert((anomaly1.get_result() == result1));

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
    ibm.get_column<double>("IBM_Close")[502] = 800.0;
    ibm.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm.get_column<double>("IBM_Close")[2002] = 850.0;

    and_zscr_v<double>              anomaly2 { 15.0 };
    const std::vector<std::size_t>  result2 = { 502, 1001, 2002 };

    ibm.single_act_visit<double>("IBM_Close", anomaly2);
    assert((anomaly2.get_result() == result2));
}

// ----------------------------------------------------------------------------

static void test_remove_data_by_iqr()  {

    std::cout << "\nTesting remove_data_by_iqr( ) ..." << std::endl;

    constexpr std::size_t   item_cnt = 1024;
    MyStdDataFrame          df;

    df.load_index(MyStdDataFrame::gen_sequence_index(0, item_cnt, 1));

    std::vector<double>   sine_col;

    sine_col.reserve(item_cnt);
    for (std::size_t i = 0; i < item_cnt; ++i)  {
        sine_col.push_back(std::sin(2.0 * M_PI * i / 20.0)); // Base sine wave
        if (i % 30 == 0)  sine_col.back() += 2.0;  // Inject anomalies
    }
    df.load_column("sine col", std::move(sine_col));

    MyStdDataFrame  df2 = df;

    auto    lbd =
        [](const unsigned long &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double>("sine col", lbd);

    assert((df.get_column<double>("sine col").size() == 1024));
    assert((view.get_column<double>("sine col").size() == 1024));

    df.remove_data_by_iqr<double, double>("sine col", 0.5, 0.5);
    assert((df.get_column<double>("sine col").size() == (1024 - 35)));

    view.remove_data_by_iqr<double, double>("sine col", 0.5, 0.5);
    assert((view.get_column<double>("sine col").size() == (1024 - 35)));

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
    ibm.get_column<double>("IBM_Close")[502] = 800.0;
    ibm.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm.get_column<double>("IBM_Close")[2002] = 850.0;

    StrDataFrame    ibm2 = ibm;
    auto            ibm_lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto            ibm_view =
        ibm2.get_view_by_sel<double, decltype(ibm_lbd), double, long>
            ("IBM_Open", ibm_lbd);

    ibm_view.get_column<double>("IBM_Close")[502] = 800.0;
    ibm_view.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm_view.get_column<double>("IBM_Close")[2002] = 850.0;

    assert((ibm.get_column<double>("IBM_Open").size() == 5031));
    assert((ibm_view.get_column<double>("IBM_Open").size() == 5031));

    ibm.remove_data_by_iqr<double, double, long>("IBM_Close");
    assert((ibm.get_column<double>("IBM_Open").size() == (5031 - 3)));

    ibm_view.remove_data_by_iqr<double, double, long>("IBM_Close");
    assert((ibm_view.get_column<double>("IBM_Open").size() == (5031 - 3)));
}

// ----------------------------------------------------------------------------

static void test_remove_data_by_zscore()  {

    std::cout << "\nTesting remove_data_by_zscore( ) ..." << std::endl;

    constexpr std::size_t   item_cnt = 1024;
    MyStdDataFrame          df;

    df.load_index(MyStdDataFrame::gen_sequence_index(0, item_cnt, 1));

    std::vector<double>   sine_col;

    sine_col.reserve(item_cnt);
    for (std::size_t i = 0; i < item_cnt; ++i)  {
        sine_col.push_back(std::sin(2.0 * M_PI * i / 20.0)); // Base sine wave
        if (i % 30 == 0)  sine_col.back() += 2.0;  // Inject anomalies
    }
    df.load_column("sine col", std::move(sine_col));

    MyStdDataFrame  df2 = df;

    auto    lbd =
        [](const unsigned long &, const double &) -> bool { return (true); };
    auto    view =
        df2.get_view_by_sel<double, decltype(lbd), double>("sine col", lbd);

    assert((df.get_column<double>("sine col").size() == 1024));
    assert((view.get_column<double>("sine col").size() == 1024));

    df.remove_data_by_zscore<double, double>("sine col", 2.0);
    assert((df.get_column<double>("sine col").size() == (1024 - 35)));

    view.remove_data_by_zscore<double, double>("sine col", 2.0);
    assert((view.get_column<double>("sine col").size() == (1024 - 35)));

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
    ibm.get_column<double>("IBM_Close")[502] = 800.0;
    ibm.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm.get_column<double>("IBM_Close")[2002] = 850.0;

    StrDataFrame    ibm2 = ibm;
    auto            ibm_lbd =
        [](const std::string &, const double &) -> bool { return (true); };
    auto            ibm_view =
        ibm2.get_view_by_sel<double, decltype(ibm_lbd), double, long>
            ("IBM_Open", ibm_lbd);

    ibm_view.get_column<double>("IBM_Close")[502] = 800.0;
    ibm_view.get_column<double>("IBM_Close")[1001] = 900.0;
    ibm_view.get_column<double>("IBM_Close")[2002] = 850.0;

    assert((ibm.get_column<double>("IBM_Open").size() == 5031));
    assert((ibm_view.get_column<double>("IBM_Open").size() == 5031));

    ibm.remove_data_by_zscore<double, double, long>("IBM_Close", 15.0);
    assert((ibm.get_column<double>("IBM_Open").size() == (5031 - 3)));

    ibm_view.remove_data_by_zscore<double, double, long>("IBM_Close", 15.0);
    assert((ibm_view.get_column<double>("IBM_Open").size() == (5031 - 3)));
}

// ----------------------------------------------------------------------------

static void test_AnomalyDetectByLOFVisitor()  {

    std::cout << "\nTesting AnomalyDetectByLOFVisitor{ } ..." << std::endl;

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
    ibm.get_column<double>("IBM_Adj_Close")[2002] = 450.0;
    ibm.get_column<double>("IBM_Adj_Close")[5000] = 1.5;
    ibm.get_column<double>("IBM_Adj_Close")[5030] = 20.0;

    and_lof_v<double, std::string>  anomaly { 10, 2.0 };

    ibm.single_act_visit<double>("IBM_Adj_Close", anomaly);

    const std::vector<std::size_t>  result = { 502, 1001, 2002, 5000 };

    assert((anomaly.get_result() == result));

    constexpr std::size_t   item_cnt = 1024;
    MyStdDataFrame          df2;

    df2.load_index(MyStdDataFrame::gen_sequence_index(0, item_cnt, 1));

    std::vector<double>   sine_col;

    sine_col.reserve(item_cnt);
    for (std::size_t i = 0; i < item_cnt; ++i)  {
        sine_col.push_back(std::sin(2.0 * M_PI * i / 20.0)); // Base sine wave
        if (i % 30 == 0)  sine_col.back() += 10.0;  // Inject anomalies
    }
    df2.load_column("sine col", std::move(sine_col));

    and_lof_v<double>   anomaly2 { 10, 1.5 };

    // It doesn't work for sine wave data. All the scores are 1.
    //
    df2.single_act_visit<double>("sine col", anomaly2);
    assert(anomaly2.get_result().empty());
}

// ----------------------------------------------------------------------------

static void test_detect_and_change()  {

    std::cout << "\nTesting detect_and_change( ) ..." << std::endl;

    StrDataFrame    ibm;

    try  {
        ibm.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto    &close_col = ibm.get_column<double>("IBM_Close");
    auto    &open_col = ibm.get_column<double>("IBM_Open");

    {
        close_col[502] = 800.0;
        close_col[1001] = 900.0;
        close_col[2002] = 850.0;
        open_col[2] = 1.0;
        open_col[3000] = 2.5;
        open_col[5029] = 850.0;

        ibm.detect_and_change<double>({ "IBM_Close", "IBM_Open" },
                                      detect_method::zscore,
                                      fill_policy::fill_forward,
                                      { .threshold = 3.0 });

        assert((std::fabs(close_col[502] - 82.5) < 0.01));
        assert((std::fabs(close_col[1001] - 89.5) < 0.01));
        assert((std::fabs(close_col[2002] - 92.51) < 0.01));
        assert((std::fabs(open_col[2] - 99.0) < 0.01));
        assert((std::fabs(open_col[3000] - 210.28) < 0.01));
        assert((std::fabs(open_col[5029] - 108.66) < 0.01));
    }

    {
        close_col[502] = 800.0;
        close_col[1001] = 900.0;
        close_col[2002] = 850.0;
        open_col[2] = 1.0;
        open_col[3000] = 2.5;
        open_col[5029] = 850.0;

        ibm.detect_and_change<double>({ "IBM_Close", "IBM_Open" },
                                      detect_method::hampel,
                                      fill_policy::fill_backward,
                                      { .window_size = 10,
                                        .htype = hampel_type::median,
                                        .num_stdev = 2.0 });

        assert((std::fabs(close_col[502] - 81.54) < 0.01));
        assert((std::fabs(close_col[1001] - 90.11) < 0.01));
        assert((std::fabs(close_col[2002] - 83.6) < 0.01));
        assert((std::fabs(open_col[2] - 1.0) < 0.01)); // It didn't catch it
        assert((std::fabs(open_col[3000] - 210.02) < 0.01));
        assert((std::fabs(open_col[5029] - 107.9) < 0.01));
    }

    {
        close_col[502] = 800.0;
        close_col[1001] = 900.0;
        close_col[2002] = 850.0;
        open_col[2] = 1.0;
        open_col[3000] = 2.5;
        open_col[5029] = 850.0;

        ibm.detect_and_change<double>(
            { "IBM_Close", "IBM_Open" },
            detect_method::fft,
            fill_policy::mid_point,
            { .threshold = 250.0,
              .norm_type = normalization_type::z_score,
              .freq_num = 1000 });

        assert((std::fabs(close_col[502] - 82.02) < 0.01));
        assert((std::fabs(close_col[1001] - 89.805) < 0.01));
        assert((std::fabs(close_col[2002] - 88.055) < 0.01));
        assert((std::fabs(open_col[2] - 1.0) < 0.01));    // It didn't catch it
        assert((std::fabs(open_col[3000] - 2.5) < 0.01)); // It didn't catch it
        assert((std::fabs(open_col[5029] - 108.28) < 0.01));
    }

    // Now we need a DataFrame with a numeric index to be able to use
    // interpolation
    //
    MyDataFrame ford;

    try  {
        ford.read("FORD.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    auto    &fclose_col = ford.get_column<double>("FORD_Close");
    auto    &fopen_col = ford.get_column<double>("FORD_Open");

    {
        fclose_col[502] = 200.0;
        fclose_col[1001] = 300.0;
        fclose_col[2002] = 250.0;
        fopen_col[2] = 0.01;
        fopen_col[3000] = 0.05;
        fopen_col[5029] = 850.0;

        ford.detect_and_change<double>({ "FORD_Close", "FORD_Open" },
                                      detect_method::iqr,
                                      fill_policy::linear_interpolate,
                                      { .high_fence = 0.5, .low_fence = 0.5 });

        assert((std::fabs(fclose_col[502] - 1.6889) < 0.0001));
        assert((std::fabs(fclose_col[1001] - 1.8146) < 0.0001));
        assert((std::fabs(fclose_col[2002] - 0.9022) < 0.0001));
        assert((std::fabs(fopen_col[2] - 0.01) < 0.01));    // No catch
        assert((std::fabs(fopen_col[3000] - 0.05) < 0.01)); // No catch
        assert((std::fabs(fopen_col[5029] - 7.9947) < 0.0001));
    }
}

// ----------------------------------------------------------------------------

static void test_KolmoSmirnovTestVisitor()  {

    std::cout << "\nTesting KolmoSmirnovTestVisitor{ } ..." << std::endl;

    StrDataFrame    ibm;

    try  {
        ibm.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    MinVisitor<double, std::string> min_val;
    MaxVisitor<double, std::string> max_val;

    ibm.single_act_visit<double>("IBM_Close", min_val);
    ibm.single_act_visit<double>("IBM_Close", max_val);

    const auto              col_s = ibm.get_index().size();
    RandGenParams<double>   p1 { .min_value = min_val.get_result(),
                                 .max_value = max_val.get_result() };
    RandGenParams<double>   p2 { .min_value = min_val.get_result(),
                                 .max_value = max_val.get_result() };

    p1.mean = 4;
    p1.std = 2.0;
    p1.seed = 1056;
    ibm.load_column("normal 1", gen_normal_dist<double>(col_s, p1));

    p1.mean = 0;
    p1.std = 1.0;
    p1.seed = 123;
    ibm.load_column("normal 2", gen_normal_dist<double>(col_s, p1));

    p2.seed = 123;
    ibm.load_column("uniform", gen_uniform_real_dist<double>(col_s, p2));
    ibm.load_column("exponential", gen_exponential_dist<double>(col_s, p2));
    ibm.load_column("lognormal", gen_lognormal_dist<double>(col_s, p2));

    ks_test_v<double, std::string>  ks_test;

    ibm.single_act_visit<double, double>("IBM_Close", "IBM_Open", ks_test);
    assert((std::fabs(ks_test.get_result() - 0.0034) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 1.0) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Low", "IBM_High", ks_test);
    assert((std::fabs(ks_test.get_result() - 0.0296) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 0.0242) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Close", "uniform", ks_test);
    assert((std::fabs(ks_test.get_result() - 0.1224) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Close", "normal 1", ks_test);
    assert((std::fabs(ks_test.get_result() - 1.0) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Close", "normal 2", ks_test);
    assert((std::fabs(ks_test.get_result() - 1.0) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Close", "lognormal", ks_test);
    assert((std::fabs(ks_test.get_result() - 0.9998) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("normal 2", "normal 1", ks_test);
    assert((std::fabs(ks_test.get_result() - 0.8326) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("normal 2", "lognormal", ks_test);
    assert((std::fabs(ks_test.get_result() - 0.5353) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Close", "IBM_Close", ks_test);
    assert((std::fabs(ks_test.get_result() - 0.0) < 0.0001));
    assert((std::fabs(ks_test.get_p_value() - 1.0) < 0.0001));
}

// ----------------------------------------------------------------------------

static void test_MannWhitneyUTestVisitor()  {

    std::cout << "\nTesting MannWhitneyUTestVisitor{ } ..." << std::endl;

    StrDataFrame    ibm;

    try  {
        ibm.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    MinVisitor<double, std::string> min_val;
    MaxVisitor<double, std::string> max_val;

    ibm.single_act_visit<double>("IBM_Close", min_val);
    ibm.single_act_visit<double>("IBM_Close", max_val);

    const auto              col_s = ibm.get_index().size();
    RandGenParams<double>   p { .min_value = min_val.get_result(),
                                .max_value = max_val.get_result(),
                                .seed = 123 };

    ibm.load_column("uniform", gen_uniform_real_dist<double>(col_s, p));
    ibm.load_column("exponential", gen_exponential_dist<double>(col_s, p));
    ibm.load_column("lognormal", gen_lognormal_dist<double>(col_s, p));

    mwu_test_v<double, std::string> mwu_test;

    ibm.single_act_visit<double, double>("IBM_Close", "IBM_Open", mwu_test);
    assert((std::fabs(mwu_test.get_result() - 12643394.5) < 0.0001));
    assert((std::fabs(mwu_test.get_u1() - 12667566.5) < 0.0001));
    assert((std::fabs(mwu_test.get_u2() - 12643394.5) < 0.0001));
    assert((std::fabs(mwu_test.get_zscore() - -0.083) < 0.001));
    assert((std::fabs(mwu_test.get_pvalue() - 0.9339) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Low", "IBM_High", mwu_test);
    assert((std::fabs(mwu_test.get_result() - 12213043.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u1() - 12213043.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u2() - 13097918.0) < 0.0001));
    assert((std::fabs(mwu_test.get_zscore() - -3.0369) < 0.001));
    assert((std::fabs(mwu_test.get_pvalue() - 0.0024) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Close", "uniform", mwu_test);
    assert((std::fabs(mwu_test.get_result() - 11666858.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u1() - 11666858.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u2() - 13644103.0) < 0.0001));
    assert((std::fabs(mwu_test.get_zscore() - -6.7858) < 0.001));
    assert((std::fabs(mwu_test.get_pvalue() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Close", "lognormal", mwu_test);
    assert((std::fabs(mwu_test.get_result() - 30.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u1() - 25310931.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u2() - 30.0) < 0.0001));
    assert((std::fabs(mwu_test.get_zscore() - -86.8661) < 0.001));
    assert((std::fabs(mwu_test.get_pvalue() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("uniform", "exponential", mwu_test);
    assert((std::fabs(mwu_test.get_result() - 0.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u1() - 25310961.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u2() - 0.0) < 0.0001));
    assert((std::fabs(mwu_test.get_zscore() - -86.8663) < 0.001));
    assert((std::fabs(mwu_test.get_pvalue() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("exponential", "lognormal", mwu_test);
    assert((std::fabs(mwu_test.get_result() - 9727387.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u1() - 9727387.0) < 0.0001));
    assert((std::fabs(mwu_test.get_u2() - 15583574.0) < 0.0001));
    assert((std::fabs(mwu_test.get_zscore() - -20.0982) < 0.001));
    assert((std::fabs(mwu_test.get_pvalue() - 0.0) < 0.0001));

    ibm.single_act_visit<double, double>("IBM_Close", "IBM_Close", mwu_test);
    assert((std::fabs(mwu_test.get_result() - 12655480.5) < 0.0001));
    assert((std::fabs(mwu_test.get_u1() - 12655480.5) < 0.0001));
    assert((std::fabs(mwu_test.get_u2() - 12655480.5) < 0.0001));
    assert((std::fabs(mwu_test.get_zscore() - 0.0) < 0.001));
    assert((std::fabs(mwu_test.get_pvalue() - 1.0) < 0.0001));
}

// ----------------------------------------------------------------------------

static void test_mask()  {

    std::cout << "\nTesting mask( ) ..." << std::endl;

    StrDataFrame    ibm;

    try  {
        ibm.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    ibm.load_column("close_mask",
                    ibm.mask<double, char>("IBM_Close",
                                           [](const double &close) -> char {
                                               return (close > 180.0);
                                           })
                    );
    ibm.load_column("close_diff",
                    ibm.mask<double, double>("IBM_Close",
                                            [](const double &close) -> double {
                                                return (180.0 - close);
                                            })
                    );

    const auto  &close_mask = ibm.get_column<char>("close_mask");
    const auto  &close_diff = ibm.get_column<double>("close_diff");

    assert(close_mask[0] == 0);
    assert(close_mask[10] == 0);
    assert(close_mask[247] == 0);
    assert(close_mask[1180] == 0);
    assert(close_mask[2761] == 1);
    assert(close_mask[2806] == 1);
    assert(close_mask[3372] == 1);
    assert(close_mask[4106] == 1);
    assert(close_mask[4966] == 0);
    assert(close_mask[5030] == 0);

    assert((std::fabs(close_diff[0] - 81.4375) < 0.0001));
    assert((std::fabs(close_diff[10] - 80.625) < 0.0001));
    assert((std::fabs(close_diff[247] - 71.93) < 0.0001));
    assert((std::fabs(close_diff[1180] - 98.19) < 0.0001));
    assert((std::fabs(close_diff[2761] - -0.360001) < 0.000001));
    assert((std::fabs(close_diff[2806] - -6.17999) < 0.0001));
    assert((std::fabs(close_diff[3372] - -13.55) < 0.0001));
    assert((std::fabs(close_diff[4106] - -0.529999) < 0.000001));
    assert((std::fabs(close_diff[4966] - 57.06) < 0.0001));
    assert((std::fabs(close_diff[5030] - 68.34) < 0.0001));
}

// ----------------------------------------------------------------------------

static void test_fast_ica()  {

    std::cout << "\nTesting fast_ica( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  result = df.fast_ica<double>(
        { "IBM_Close", "IBM_Open", "IBM_High", "IBM_Low" },
        2,
        { .seed = 21 });

    assert(result.rows() == 5031);
    assert(result.cols() == 2);
    assert((std::fabs(result(0, 0) - 0.01778) < 0.00001));
    assert((std::fabs(result(0, 1) - -0.0123) < 0.00001));
    assert((std::fabs(result(678, 0) - 0.0484) < 0.00001));
    assert((std::fabs(result(678, 1) - -0.01284) < 0.00001));
    assert((std::fabs(result(1852, 0) - 0.02188) < 0.00001));
    assert((std::fabs(result(1852, 1) - 0.01310) < 0.00001));
    assert((std::fabs(result(4008, 0) - -0.03373) < 0.00001));
    assert((std::fabs(result(4008, 1) - 0.02964) < 0.00001));
    assert((std::fabs(result(5030, 0) - -0.0237) < 0.00001));
    assert((std::fabs(result(5030, 1) - -0.06062) < 0.00001));
}

// ----------------------------------------------------------------------------

static void test_DateTime_write()  {

    std::cout << "\nTesting test_DateTime_write( ) ..." << std::endl;

    DTDataFrame df;

    try  {
        df.read("DT_sample.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    df.write<std::ostream, double, long, DateTime>
        (std::cout, io_format::csv2, { .max_recs = 5 });
    std::cout << "\n\n\n";

    df.write<std::ostream, double, long, DateTime>
        (std::cout, io_format::csv2,
         { .max_recs = 5, .dt_format = DT_FORMAT::ISO_DT_TM });
    std::cout << "\n\n\n";

    df.write<std::ostream, double, long, DateTime>
        (std::cout, io_format::csv2,
         { .max_recs = 5, .dt_format = DT_FORMAT::AMR_DT_TM });
    std::cout << "\n\n\n";

    df.write<std::ostream, double, long, DateTime>
        (std::cout, io_format::csv2,
         { .max_recs = 5, .dt_format = DT_FORMAT::EUR_DT_TM });
    std::cout << "\n\n\n";

    df.write<std::ostream, double, long, DateTime>
        (std::cout, io_format::csv2,
         { .max_recs = 5, .dt_format = DT_FORMAT::ISO_DT });
    std::cout << "\n\n\n";

    df.write<std::ostream, double, long, DateTime>
        (std::cout, io_format::csv2,
         { .max_recs = 5, .dt_format = DT_FORMAT::AMR_DT });
    std::cout << "\n\n\n";

    df.write<std::ostream, double, long, DateTime>
        (std::cout, io_format::csv2,
         { .max_recs = 5, .dt_format = DT_FORMAT::EUR_DT });
    std::cout << "\n\n\n";
}

// ----------------------------------------------------------------------------

static void test_read_selected_cols_from_file()  {

    std::cout << "\nTesting test_read_selected_cols_from_file{ } ..."
              << std::endl;

    MyDataFrame             df1;
    MyDataFrame             df2;
    ReadParams::SchemaVec   schema1 {  // First is always the index column
        { .col_type = file_dtypes::ULONG, .num_rows = 12, .col_idx = 0 },
        { "Close", file_dtypes::DOUBLE, 12, 4 },
        { "Volume", file_dtypes::LONG, 12, 6 },
    };
    ReadParams::SchemaVec   schema2 {  // First is always the index column
        { .col_type = file_dtypes::ULONG, .num_rows = 12, .col_idx = 0 },
        { "Open", file_dtypes::DOUBLE, 12, 1 },
        { "Low", file_dtypes::DOUBLE, 12, 3 },
        { "Close", file_dtypes::DOUBLE, 12, 4 },
    };

    try  {
        df1.read("SchemaWithHeader.csv", io_format::csv2,
                 { .schema = schema1 });
        df2.read("SchemaWithoutHeader.csv", io_format::csv2,
                 { .skip_first_line = false, .schema = schema2 });
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto  df_shape1 = df1.shape();
    const auto  df_shape2 = df2.shape();

    assert(df_shape1.first == 12);  // Rows
    assert(df_shape1.second == 2);  // Columns
    assert(df_shape2.first == 12);  // Rows
    assert(df_shape2.second == 3);  // Columns

    assert(df1.get_index()[0] == 1);
    assert(df1.get_index()[6] == 7);
    assert(df1.get_index()[11] == 12);
    assert(df1.get_column<double>("Close").size() == 12);
    assert((std::fabs(df1.get_column<double>("Close")[0] - 185.53) < 0.01));
    assert((std::fabs(df1.get_column<double>("Close")[6] - 187.26) < 0.01));
    assert((std::fabs(df1.get_column<double>("Close")[11] - 190.09) < 0.01));
    assert(df1.get_column<long>("Volume").size() == 12);
    assert(df1.get_column<long>("Volume")[0] == 4546500);
    assert(df1.get_column<long>("Volume")[6] == 4022400);
    assert(df1.get_column<long>("Volume")[11] == 7644600);

    assert(df2.get_index()[0] == 1);
    assert(df2.get_index()[6] == 7);
    assert(df2.get_index()[11] == 12);
    assert(df2.get_column<double>("Close").size() == 12);
    assert((std::fabs(df2.get_column<double>("Close")[0] - 185.53) < 0.01));
    assert((std::fabs(df2.get_column<double>("Close")[6] - 187.26) < 0.01));
    assert((std::fabs(df2.get_column<double>("Close")[11] - 190.09) < 0.01));
    assert(df2.get_column<double>("Open").size() == 12);
    assert((std::fabs(df2.get_column<double>("Open")[0] - 187.21) < 0.01));
    assert((std::fabs(df2.get_column<double>("Open")[6] - 188.31) < 0.01));
    assert((std::fabs(df2.get_column<double>("Open")[11] - 188.04) < 0.01));
    assert(df2.get_column<double>("Low").size() == 12);
    assert((std::fabs(df2.get_column<double>("Low")[0] - 185.2) < 0.1));
    assert((std::fabs(df2.get_column<double>("Low")[6] - 186.28) < 0.01));
    assert((std::fabs(df2.get_column<double>("Low")[11] - 187.86) < 0.01));
}

// ----------------------------------------------------------------------------

static void test_MutualInfoVisitor()  {

    std::cout << "\nTesting MutualInfoVisitor{ } ..." << std::endl;

    using MyDataFrame = StdDataFrame<unsigned long>;

    MyDataFrame                 df;
    std::vector<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 15, 16, 17, 18,
          19, 20, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };
    RandGenParams<int>          p;

    p.seed = 123;
    p.max_value = 4;
    p.min_value = -4;
    df.load_data(std::move(idxvec),
                 std::make_pair("int_col_1",
                                gen_uniform_int_dist<int>(idxvec.size(), p)),
                 std::make_pair("str_col", strvec));
    p.seed = 675;
    df.load_column("int_col_2", gen_uniform_int_dist<int>(idxvec.size(), p));

    std::vector<int>    intcol3(df.get_index().size());
    const auto          int_col_1 = df.get_column<int>("int_col_1");

    for (std::size_t i { 0 }; i < intcol3.size(); ++i)  {
        if (int_col_1[i] >= 0)  intcol3[i] = 1;
        else  intcol3[i] = -1;
    }
    df.load_column("int_col_3", intcol3);

    MutualInfoVisitor<int>  minfo;

    df.single_act_visit<int, int>("int_col_1", "int_col_1", minfo);
    assert((std::fabs(minfo.get_result() - 12.4866) < 0.0001));

    df.single_act_visit<int, int>("int_col_1", "int_col_2", minfo);
    assert((std::fabs(minfo.get_result() - 1.81499) < 0.00001));

    df.single_act_visit<int, int>("int_col_1", "int_col_3", minfo);
    assert((std::fabs(minfo.get_result() - 4.24521) < 0.00001));
}

// ----------------------------------------------------------------------------

static void test_io_format_csv2_with_bars()  {

    std::cout << "\nTesting io_format_csv2_with_bars( ) ..." << std::endl;

    MyDataFrame df_read;

    try  {
        df_read.read("csv2_format_data_with_bars.csv",
                     io_format::csv2,
                     { .delim = '|' });
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }
    df_read.write<std::ostream,
                  int,
                  unsigned long,
                  unsigned char,
                  char,
                  double,
                  bool,
                  std::string>(std::cout, io_format::csv2);
    std::cout << "\n\n";
    df_read.write<std::ostream,
                  int,
                  unsigned long,
                  unsigned char,
                  char,
                  double,
                  bool,
                  std::string>(std::cout, io_format::csv2, { .delim = '|' });
}

// -----------------------------------------------------------------------------

static void test_AndersonDarlingTestVisitor()  {

    std::cout << "\nTesting AndersonDarlingTestVisitor{ } ..." << std::endl;

    StrDataFrame    ibm;

    try  {
        ibm.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto              col_s = ibm.get_index().size();
    RandGenParams<double>   p1 { .min_value = 99,
                                 .max_value = 200,
                                 .seed = 123 };

    ibm.load_column("uniform", gen_uniform_real_dist<double>(col_s, p1));
    ibm.load_column("exponential", gen_exponential_dist<double>(col_s, p1));
    ibm.load_column("lognormal", gen_lognormal_dist<double>(col_s, p1));
    ibm.load_column("normal", gen_normal_dist<double>(col_s, p1));

    RandGenParams<double>   p2 { .seed = 123, .mean = 0, .std = 1.0 };

    ibm.load_column("std_normal", gen_normal_dist<double>(col_s, p2));

    AndersonDarlingTestVisitor<double, std::string> adt;

    ibm.single_act_visit<double>("IBM_Close", adt);
    assert((std::fabs(adt.get_result() - 63.9697) < 0.0001));
    assert((std::fabs(adt.get_p_value() - 1.02759e-125) < 0.0000000000001));

    ibm.single_act_visit<double>("uniform", adt);
    assert((std::fabs(adt.get_result() - 56.7522) < 0.0001));
    assert((std::fabs(adt.get_p_value() - 7.38848e-115) < 0.0000000000001));

    ibm.single_act_visit<double>("exponential", adt);
    assert((std::fabs(adt.get_result() - 229.787) < 0.001));
    assert((std::fabs(adt.get_p_value() - 2.29024e-143) < 0.0000000000001));

    ibm.single_act_visit<double>("lognormal", adt);
    assert((! std::isfinite(adt.get_result())));
    assert(std::isnan(adt.get_p_value()));

    ibm.single_act_visit<double>("normal", adt);
    assert((std::fabs(adt.get_result() - 0.26783) < 0.00001));
    assert((std::fabs(adt.get_p_value() - 0.68509) < 0.00001));

    ibm.single_act_visit<double>("std_normal", adt);
    assert((std::fabs(adt.get_result() - 0.26783) < 0.00001));
    assert((std::fabs(adt.get_p_value() - 0.68509) < 0.00001));
}

// -----------------------------------------------------------------------------

static void test_ShapiroWilkTestVisitor()  {

    std::cout << "\nTesting ShapiroWilkTestVisitor{ } ..." << std::endl;

    StrDataFrame    ibm;

    try  {
        ibm.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto              col_s = ibm.get_index().size();
    RandGenParams<double>   p1 { .min_value = 99,
                                 .max_value = 200,
                                 .seed = 123 };

    ibm.load_column("uniform", gen_uniform_real_dist<double>(col_s, p1));
    ibm.load_column("exponential", gen_exponential_dist<double>(col_s, p1));
    ibm.load_column("lognormal", gen_lognormal_dist<double>(col_s, p1));
    ibm.load_column("normal", gen_normal_dist<double>(col_s, p1));

    RandGenParams<double>   p2 { .seed = 123, .mean = 0, .std = 1.0 };

    ibm.load_column("std_normal", gen_normal_dist<double>(col_s, p2));

    ShapiroWilkTestVisitor<double, std::string> swt;

    ibm.single_act_visit<double>("IBM_Close", swt);
    assert((std::fabs(swt.get_result() - 0.953874) < 0.000001));
    assert((std::fabs(swt.get_p_value() - 3.56659e-37) < 0.0000000000001));

    ibm.single_act_visit<double>("uniform", swt);
    assert((std::fabs(swt.get_result() - 0.954293) < 0.000001));
    assert((std::fabs(swt.get_p_value() - 4.8411e-37) < 0.0000000000001));

    ibm.single_act_visit<double>("exponential", swt);
    assert((std::fabs(swt.get_result() - 0.819813) < 0.000001));
    assert((std::fabs(swt.get_p_value() - 9.28713e-60) < 0.0000000000001));

    ibm.single_act_visit<double>("lognormal", swt);
    assert((std::fabs(swt.get_result() - 0.524102) < 0.000001));
    assert(swt.get_p_value() == 0.0);

    ibm.single_act_visit<double>("normal", swt);
    assert((std::fabs(swt.get_result() - 0.99974) < 0.00001));
    assert((std::fabs(swt.get_p_value() - 0.816855) < 0.000001));

    ibm.single_act_visit<double>("std_normal", swt);
    assert((std::fabs(swt.get_result() - 0.99974) < 0.00001));
    assert((std::fabs(swt.get_p_value() - 0.816855) < 0.000001));
}

// ----------------------------------------------------------------------------

static void test_CramerVonMisesTestVisitor()  {

    std::cout << "\nTesting CramerVonMisesTestVisitor{ } ..." << std::endl;

    StrDataFrame    ibm;

    try  {
        ibm.read("IBM.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
        ::exit(-1);
    }

    const auto              col_s = ibm.get_index().size();
    RandGenParams<double>   p1 { .min_value = 99,
                                 .max_value = 200,
                                 .seed = 123 };

    ibm.load_column("uniform", gen_uniform_real_dist<double>(col_s, p1));
    ibm.load_column("exponential", gen_exponential_dist<double>(col_s, p1));
    ibm.load_column("lognormal", gen_lognormal_dist<double>(col_s, p1));
    ibm.load_column("normal", gen_normal_dist<double>(col_s, p1));

    RandGenParams<double>   p2 { .seed = 123, .mean = 0, .std = 1.0 };

    ibm.load_column("std_normal", gen_normal_dist<double>(col_s, p2));

    CramerVonMisesTestVisitor<double, std::string>  cvmt;

    ibm.single_act_visit<double>("IBM_Close", cvmt);
    assert((std::fabs(cvmt.get_result() - 8.56304) < 0.00001));
    assert((std::fabs(cvmt.get_p_value() - 2.22045e-16) < 0.0000000000001));

    ibm.single_act_visit<double>("uniform", cvmt);
    assert((std::fabs(cvmt.get_result() - 7.78703) < 0.00001));
    assert((std::fabs(cvmt.get_p_value() - 2.22045e-16) < 0.0000000000001));

    ibm.single_act_visit<double>("exponential", cvmt);
    assert((std::fabs(cvmt.get_result() - 39.0971) < 0.0001));
    assert((std::fabs(cvmt.get_p_value() - 2.22045e-16) < 0.0000000000001));

    ibm.single_act_visit<double>("lognormal", cvmt);
    assert((std::fabs(cvmt.get_result() - 96.4868) < 0.0001));
    assert((std::fabs(cvmt.get_p_value() - 2.22045e-16) < 0.0000000000001));

    ibm.single_act_visit<double>("normal", cvmt);
    assert((std::fabs(cvmt.get_result() - 0.040035) < 0.000001));
    assert((std::fabs(cvmt.get_p_value() - 0.99) < 0.01));

    ibm.single_act_visit<double>("std_normal", cvmt);
    assert((std::fabs(cvmt.get_result() - 0.040035) < 0.000001));
    assert((std::fabs(cvmt.get_p_value() - 0.99) < 0.01));
}

// ----------------------------------------------------------------------------

static void test_unpivot()  {

    std::cout << "\nTesting unpivot( ) ..." << std::endl;

    ULDataFrame df;

    df.load_data(
        std::vector<unsigned long>{ 1, 2, 3, 4, 5, 6, 7 },
        std::make_pair("day",
                       std::vector<std::string>{ "Monday", "Tuesday",
                                                 "Wednesday", "Thursday",
                                                 "Friday", "Saturday", "Sunday"
                                }),
        std::make_pair("Chicago",
                       std::vector<double>{ 32, 30, 28, 22, 30, 20, 25 }),
        std::make_pair("Tehran",
                       std::vector<double>{ 75, 77, 75, 82, 83, 81, 77 }),
        std::make_pair("Berlin",
                       std::vector<double>{ 41, 43, 45, 38, 30, 45, 47 }),
        std::make_pair("Str Column",
                       std::vector<std::string>{ "AA", "BB", "CC", "DD", "EE",
                                                 "FF", "GG" }));

    df.write<std::ostream, std::string, double>(std::cout, io_format::csv2);
    std::cout << "\n\n";

    const auto  mdf1 =
        df.unpivot<std::string, double>
            ("day", { "Chicago", "Tehran", "Berlin" });

    mdf1.write<std::ostream, std::string, double>(std::cout, io_format::csv2);
    std::cout << "\n\n";

    const auto  mdf2 = df.unpivot<std::string, double>("day");

    mdf2.write<std::ostream, std::string, double>(std::cout, io_format::csv2);

    assert((mdf1.is_equal<double, std::string>(mdf2)));
}

// ----------------------------------------------------------------------------

int main(int, char *[]) {

    MyDataFrame::set_optimum_thread_level();

    test_starts_with();
    test_ends_with();
    test_in_between();
    test_peaks();
    test_valleys();
    test_apply();
    test_truncate();
    test_load_column();
    test_explode();
    test_read_write_pairs();
    test_difference();
    test_get_data_at_times();
    test_get_data_before_times();
    test_get_data_after_times();
    test_get_data_on_days();
    test_get_data_in_months();
    test_get_data_on_days_in_month();
    test_get_data_between_times();
    test_remove_top_n_data();
    test_remove_bottom_n_data();
    test_remove_above_quantile_data();
    test_remove_below_quantile_data();
    test_remove_data_by_stdev();
    test_get_data_by_stdev();
    test_get_data_by_kmeans();
    // test_get_data_by_affin();
    test_remove_data_by_hampel();
    test_DBSCANVisitor();
    test_MeanShiftVisitor();
    test_get_data_by_dbscan();
    test_get_data_by_mshift();
    test_view_assign();
    test_CrossCorrVisitor();
    test_PartialAutoCorrVisitor();
    test_make_stationary();
    test_StationaryCheckVisitor();
    test_covariance_matrix();
    test_pca_by_eigen();
    test_compact_svd();
    test_SpectralClusteringVisitor();
    test_get_data_by_spectral();
    test_canon_corr();
    test_MC_station_dist();
    test_SeasonalPeriodVisitor();
    test_read_data_file_with_schema();
    test_knn();
    test_DynamicTimeWarpVisitor();
    test_AnomalyDetectByFFTVisitor();
    test_remove_data_by_fft();
    test_AnomalyDetectByIQRVisitor();
    test_AnomalyDetectByZScoreVisitor();
    test_remove_data_by_iqr();
    test_remove_data_by_zscore();
    test_AnomalyDetectByLOFVisitor();
    test_detect_and_change();
    test_KolmoSmirnovTestVisitor();
    test_MannWhitneyUTestVisitor();
    test_mask();
    test_fast_ica();
    test_DateTime_write();
    test_read_selected_cols_from_file();
    test_MutualInfoVisitor();
    test_io_format_csv2_with_bars();
    test_AndersonDarlingTestVisitor();
    test_ShapiroWilkTestVisitor();
    test_CramerVonMisesTestVisitor();
    test_unpivot();

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
