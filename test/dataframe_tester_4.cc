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
using MyDataFrame = StdDataFrame256<unsigned long>;
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

int main(int, char *[]) {

    MyDataFrame::set_optimum_thread_level();

    test_starts_with();
    test_ends_with();
    test_in_between();
    test_peaks();
    test_valleys();
    test_apply();
    test_truncate();

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
