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
#include <iostream>
#include <string>

using namespace hmdf;

// A DataFrame with ulong index type
//
using MyDataFrame = StdDataFrame256<unsigned long>;
using StrDataFrame = StdDataFrame256<std::string>;

template<typename T>
using StlVecType = typename MyDataFrame::template StlVecType<T>;

// -----------------------------------------------------------------------------

static void test_groupby_edge()  {

    std::cout << "\nTesting groupby( ) ..." << std::endl;

    MyDataFrame                df;
    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0,
          3.0, 9.0, 10.0};
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8,
          111.0, 112.0, 113.0, 114.0, 115.0, 116.0};
    StlVecType<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    StlVecType<std::string>    strvec =
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

    FactorizeVisitor<double, unsigned long, 256>
        fact([] (const double &f) -> bool {
            return (f > 11106.0 && f < 30000.0);
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

    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, 14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0,
          9.0, 10.0 };
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8, 111.0,
          112.0, 113.0, 114.0, 115.0, 116.0 };
    StlVecType<int>            intvec =
        { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9, 10, 15, 12, 13 };
    StlVecType<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df1.load_data(std::move(idxvec),
                  std::make_pair("dbl_col", dblvec),
                  std::make_pair("int_col", intvec),
                  std::make_pair("str_col", strvec));

    MyDataFrame         df2 = df1;
    const MyDataFrame   &const_df1 = df1;
    const MyDataFrame   &const_df2 = df2;

    df2.load_column("dbl_col_2", std::move(dblvec2));

    auto    result1 =
        df1.concat_view<decltype(df2), double, int, std::string>(df2);
    auto    const_result1 =
        const_df1.concat_view<decltype(df2), double, int, std::string>(df2);

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

    assert(const_result1.get_index().size() == 30);
    assert(const_result1.get_column<double>("dbl_col").size() == 30);
    assert(const_result1.get_column<std::string>("str_col").size() == 30);
    assert(const_result1.get_column<int>("int_col").size() == 30);
    assert(const_result1.get_index()[0] == 1);
    assert(const_result1.get_index()[14] == 14);
    assert(const_result1.get_index()[15] == 1);
    assert(const_result1.get_index()[29] == 14);
    assert(const_result1.get_column<std::string>("str_col")[0] == "zz");
    assert(const_result1.get_column<std::string>("str_col")[14] == "oo");
    assert(const_result1.get_column<std::string>("str_col")[15] == "zz");
    assert(const_result1.get_column<int>("int_col")[0] == 1);
    assert(const_result1.get_column<int>("int_col")[14] == 13);
    assert(const_result1.get_column<int>("int_col")[15] == 1);

    auto    result2 =
        df2.concat_view<decltype(df2), double, int, std::string>
            (df1, concat_policy::lhs_and_common_columns);
    auto    const_result2 =
        const_df2.concat_view<decltype(df2), double, int, std::string>
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

    assert(const_result2.get_index().size() == 30);
    assert(const_result2.get_column<double>("dbl_col").size() == 30);
    assert(const_result2.get_column<std::string>("str_col").size() == 30);
    assert(const_result2.get_column<std::string>("str_col")[0] == "zz");
    assert(const_result2.get_column<std::string>("str_col")[14] == "oo");
    assert(const_result2.get_column<std::string>("str_col")[15] == "zz");
    assert(const_result2.get_column<double>("dbl_col_2").size() == 15);
    assert(const_result2.get_column<double>("dbl_col_2")[0] == 100.0);
    assert(const_result2.get_column<double>("dbl_col_2")[5] == 105.0);
    assert(const_result2.get_column<double>("dbl_col_2")[10] == 112.0);

    auto    result3 =
        df1.concat_view<decltype(df1), double, int, std::string>
            (df2, concat_policy::all_columns);
    auto    const_result3 =
        const_df1.concat_view<decltype(df1), double, int, std::string>
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

    assert(const_result3.get_index().size() == 30);
    assert(const_result3.get_column<double>("dbl_col").size() == 30);
    assert(const_result3.get_column<std::string>("str_col").size() == 30);
    assert(const_result3.get_column<double>("dbl_col_2").size() == 15);
    assert(const_result3.get_column<std::string>("str_col")[0] == "zz");
    assert(const_result3.get_column<std::string>("str_col")[14] == "oo");
    assert(const_result3.get_column<std::string>("str_col")[15] == "zz");
    assert(const_result3.get_column<double>("dbl_col_2").size() == 15);
    assert(const_result3.get_column<double>("dbl_col_2")[0] == 100.0);
    assert(const_result3.get_column<double>("dbl_col_2")[5] == 105.0);
    assert(const_result3.get_column<double>("dbl_col_2")[10] == 112.0);
}

// -----------------------------------------------------------------------------

/*
static void test_multithreading(int j)  {

    std::cout << "\nTesting test ..." << std::endl;

    MyDataFrame                df;
    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0,
          3.0, 9.0, 10.0};
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8,
          111.0, 112.0, 113.0, 114.0, 115.0, 116.0};
    StlVecType<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    StlVecType<std::string>    strvec =
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
    FactorizeVisitor<double, unsigned long, 256>
        fact([] (const double &f) -> bool {
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

static void test_to_from_string()  {

    std::cout << "\nTesting to_from_string() ..." << std::endl;

    StlVecType<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466 };
    StlVecType<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    StlVecType<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                               30, 31, 32, 1.89 };
    StlVecType<double> d3 = { 15, 16, 17, 18, 19, 20, 21,
                               0.34, 1.56, 0.34, 2.3, 0.1, 0.89, 0.45 };
    StlVecType<int>    i1 = { 22, 23, 24, 25, 99, 100, 101, 3, 2 };
    StlVecType<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn" };
    MyDataFrame         df;

    df.load_data(std::move(idx),
                 std::make_pair("col_1", d1),
                 std::make_pair("col_2", d2),
                 std::make_pair("col_3", d3),
                 std::make_pair("col_4", i1),
                 std::make_pair("str_col", strvec));

    std::future<std::string>    f =
        df.to_string_async<double, int, std::string>();
    const std::string           str_dump = f.get();

    // std::cout << str_dump << std::endl;

    MyDataFrame df2;

    df2.from_string(str_dump.c_str());
    // std::cout << '\n' << std::endl;
    // df2.write<std::ostream, double, int, std::string>(std::cout);
    assert((df.is_equal<double, int, std::string>(df2)));
}

// -----------------------------------------------------------------------------

static void test_CoppockCurveVisitor()  {

    std::cout << "\nTesting CoppockCurveVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        coppc_v<double, std::string, 256>  copp;

        df.single_act_visit<double>("IBM_Close", copp);

        assert(copp.get_result().size() == 1721);
        assert(std::isnan(copp.get_result()[0]));
        assert(std::abs(copp.get_result()[14] - -0.051884971603) < 0.0000001);
        assert(std::abs(copp.get_result()[18] - -0.100660882748) < 0.0000001);
        assert(std::abs(copp.get_result()[25] - -0.124090378548) < 0.0000001);
        assert(std::abs(copp.get_result()[1720] - -0.219247796696) < 0.0000001);
        assert(std::abs(copp.get_result()[1712] - 0.0630742594051) < 0.0000001);
        assert(std::abs(copp.get_result()[1707] - 0.0766481878384) < 0.0000001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_BiasVisitor()  {

    std::cout << "\nTesting BiasVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);
        df.remove_data_by_loc<double, long>({ 0, 1500 });

        using avg1 = MeanVisitor<double, std::string>;
        avg1                                avg1_v;
        bias_v<avg1, double, std::string, 256>   bias1 (avg1_v);

        df.single_act_visit<double>("IBM_Close", bias1);

        assert(bias1.get_result().size() == 221);
        assert(std::isnan(bias1.get_result()[0]));
        assert(std::isnan(bias1.get_result()[24]));
        assert(std::abs(bias1.get_result()[25] - 0.0309) < 0.0001);
        assert(std::abs(bias1.get_result()[30] - 0.0477) < 0.0001);
        assert(std::abs(bias1.get_result()[35] - 0.0907) < 0.0001);
        assert(std::abs(bias1.get_result()[220] - -0.0698) < 0.0001);
        assert(std::abs(bias1.get_result()[215] - -0.049) < 0.0001);
        assert(std::abs(bias1.get_result()[210] - 0.0242) < 0.0001);

        using s_avg1 = StableMeanVisitor<double, std::string>;
        s_avg1                              s_avg1_v;
        bias_v<s_avg1, double, std::string, 256> s_bias1 (s_avg1_v);

        df.single_act_visit<double>("IBM_Close", s_bias1);

        assert(s_bias1.get_result().size() == 221);
        assert(std::isnan(s_bias1.get_result()[0]));
        assert(std::isnan(s_bias1.get_result()[24]));
        assert(std::abs(s_bias1.get_result()[25] - 0.0309) < 0.0001);
        assert(std::abs(s_bias1.get_result()[30] - 0.0477) < 0.0001);
        assert(std::abs(s_bias1.get_result()[35] - 0.0907) < 0.0001);
        assert(std::abs(s_bias1.get_result()[220] - -0.0698) < 0.0001);
        assert(std::abs(s_bias1.get_result()[215] - -0.049) < 0.0001);
        assert(std::abs(s_bias1.get_result()[210] - 0.0242) < 0.0001);

        using avg2 = WeightedMeanVisitor<double, std::string>;
        avg2                                avg2_v;
        bias_v<avg2, double, std::string, 256>   bias2 (avg2_v);

        df.single_act_visit<double>("IBM_Close", bias2);

        assert(bias2.get_result().size() == 221);
        assert(std::isnan(bias2.get_result()[0]));
        assert(std::isnan(bias2.get_result()[24]));
        assert(std::abs(bias2.get_result()[25] - 0.0224) < 0.0001);
        assert(std::abs(bias2.get_result()[30] - 0.0381) < 0.0001);
        assert(std::abs(bias2.get_result()[35] - 0.068) < 0.0001);
        assert(std::abs(bias2.get_result()[220] - -0.0532) < 0.0001);
        assert(std::abs(bias2.get_result()[215] - -0.0496) < 0.0001);
        assert(std::abs(bias2.get_result()[210] - 0.0168) < 0.0001);

        using avg3 = GeometricMeanVisitor<double, std::string>;
        avg3                                avg3_v;
        bias_v<avg3, double, std::string, 256>   bias3 (avg3_v);

        df.single_act_visit<double>("IBM_Close", bias3);

        assert(bias3.get_result().size() == 221);
        assert(std::isnan(bias3.get_result()[0]));
        assert(std::isnan(bias3.get_result()[24]));
        assert(std::abs(bias3.get_result()[25] - 0.0311) < 0.0001);
        assert(std::abs(bias3.get_result()[30] - 0.0479) < 0.0001);
        assert(std::abs(bias3.get_result()[35] - 0.0919) < 0.0001);
        assert(std::abs(bias3.get_result()[220] - -0.0685) < 0.0001);
        assert(std::abs(bias3.get_result()[215] - -0.0485) < 0.0001);
        assert(std::abs(bias3.get_result()[210] - 0.0245) < 0.0001);

        using avg4 = HarmonicMeanVisitor<double, std::string>;
        avg4                                avg4_v;
        bias_v<avg4, double, std::string, 256>   bias4 (avg4_v);

        df.single_act_visit<double>("IBM_Close", bias4);

        assert(bias4.get_result().size() == 221);
        assert(std::isnan(bias4.get_result()[0]));
        assert(std::isnan(bias4.get_result()[24]));
        assert(std::abs(bias4.get_result()[25] - 0.0313) < 0.0001);
        assert(std::abs(bias4.get_result()[30] - 0.0481) < 0.0001);
        assert(std::abs(bias4.get_result()[35] - 0.093) < 0.0001);
        assert(std::abs(bias4.get_result()[220] - -0.0672) < 0.0001);
        assert(std::abs(bias4.get_result()[215] - -0.048) < 0.0001);
        assert(std::abs(bias4.get_result()[210] - 0.0248) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_BalanceOfPowerVisitor()  {

    std::cout << "\nTesting BalanceOfPowerVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        bop_v<double, std::string, 256>  bop;

        df.single_act_visit<double, double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", bop);

        assert(bop.get_result().size() == 1721);
        assert(std::abs(bop.get_result()[0] - -0.7636) < 0.0001);
        assert(std::abs(bop.get_result()[14] - 0.4319) < 0.0001);
        assert(std::abs(bop.get_result()[18] - 0.1591) < 0.0001);
        assert(std::abs(bop.get_result()[25] - 0.6466) < 0.0001);
        assert(std::abs(bop.get_result()[1720] - 0.9284) < 0.0001);
        assert(std::abs(bop.get_result()[1712] - -0.7341) < 0.0001);
        assert(std::abs(bop.get_result()[1707] - -0.5465) < 0.0001);

        bop_v<double, std::string>  bop2 (true);

        df.single_act_visit<double, double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", bop2);

        assert(bop2.get_result().size() == 1721);
        assert(std::isnan(bop2.get_result()[0]));
        assert(std::isnan(bop2.get_result()[12]));
        assert(std::abs(bop2.get_result()[13] - -0.0328) < 0.0001);
        assert(std::abs(bop2.get_result()[18] - -0.0743) < 0.0001);
        assert(std::abs(bop2.get_result()[25] - -0.0421) < 0.0001);
        assert(std::abs(bop2.get_result()[1720] - -0.1183) < 0.0001);
        assert(std::abs(bop2.get_result()[1712] - -0.0729) < 0.0001);
        assert(std::abs(bop2.get_result()[1707] - -0.0102) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_ChandeKrollStopVisitor()  {

    std::cout << "\nTesting ChandeKrollStopVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        cksp_v<double, std::string, 256>  cksp;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", cksp);

        assert(cksp.get_result().size() == 1721);
        assert(std::isnan(cksp.get_long_stop()[0]));
        assert(std::isnan(cksp.get_long_stop()[18]));
        assert(std::abs(cksp.get_long_stop()[20] - 182.5875) < 0.0001);
        assert(std::abs(cksp.get_long_stop()[25] - 182.5875) < 0.0001);
        assert(std::abs(cksp.get_long_stop()[35] - 182.5875) < 0.0001);
        assert(std::abs(cksp.get_long_stop()[1720] - 127.2591) < 0.0001);
        assert(std::abs(cksp.get_long_stop()[1712] - 127.2591) < 0.0001);
        assert(std::abs(cksp.get_long_stop()[1707] - 124.5657) < 0.0001);

        assert(cksp.get_short_stop().size() == 1721);
        assert(std::isnan(cksp.get_short_stop()[0]));
        assert(std::isnan(cksp.get_short_stop()[18]));
        assert(std::abs(cksp.get_short_stop()[20] - 192.6438) < 0.0001);
        assert(std::abs(cksp.get_short_stop()[25] - 192.6438) < 0.0001);
        assert(std::abs(cksp.get_short_stop()[35] - 187.8592) < 0.0001);
        assert(std::abs(cksp.get_short_stop()[1720] - 131.7065) < 0.0001);
        assert(std::abs(cksp.get_short_stop()[1712] - 131.7065) < 0.0001);
        assert(std::abs(cksp.get_short_stop()[1707] - 131.7065) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_VortexVisitor()  {

    std::cout << "\nTesting VortexVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        vtx_v<double, std::string, 256>  vtx;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", vtx);

        assert(vtx.get_result().size() == 1721);
        assert(std::isnan(vtx.get_plus_indicator()[0]));
        assert(std::isnan(vtx.get_plus_indicator()[12]));
        assert(std::abs(vtx.get_plus_indicator()[20] - 0.7804) < 0.0001);
        assert(std::abs(vtx.get_plus_indicator()[25] - 0.7063) < 0.0001);
        assert(std::abs(vtx.get_plus_indicator()[35] - 1.2725) < 0.0001);
        assert(std::abs(vtx.get_plus_indicator()[1720] - 0.6619) < 0.0001);
        assert(std::abs(vtx.get_plus_indicator()[1712] - 0.8658) < 0.0001);
        assert(std::abs(vtx.get_plus_indicator()[1707] - 0.9571) < 0.0001);

        assert(vtx.get_minus_indicator().size() == 1721);
        assert(std::isnan(vtx.get_minus_indicator()[0]));
        assert(std::isnan(vtx.get_minus_indicator()[12]));
        assert(std::abs(vtx.get_minus_indicator()[20] - 1.1256) < 0.0001);
        assert(std::abs(vtx.get_minus_indicator()[25] - 1.1854) < 0.0001);
        assert(std::abs(vtx.get_minus_indicator()[35] - 0.7427) < 0.0001);
        assert(std::abs(vtx.get_minus_indicator()[1720] - 1.187) < 0.0001);
        assert(std::abs(vtx.get_minus_indicator()[1712] - 1.016) < 0.0001);
        assert(std::abs(vtx.get_minus_indicator()[1707] - 0.9819) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_KeltnerChannelsVisitor()  {

    std::cout << "\nTesting KeltnerChannelsVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        kch_v<double, std::string, 256>  kch;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", kch);

        assert(kch.get_result().size() == 1721);
        assert(std::abs(kch.get_upper_band()[0] - 189.93) < 0.0001);
        assert(std::abs(kch.get_upper_band()[12] - 193.3376) < 0.0001);
        assert(std::abs(kch.get_upper_band()[20] - 187.5627) < 0.0001);
        assert(std::abs(kch.get_upper_band()[25] - 184.0657) < 0.0001);
        assert(std::abs(kch.get_upper_band()[35] - 186.5203) < 0.0001);
        assert(std::abs(kch.get_upper_band()[1720] - 123.722) < 0.0001);
        assert(std::abs(kch.get_upper_band()[1712] - 130.6271) < 0.0001);
        assert(std::abs(kch.get_upper_band()[1707] - 130.4991) < 0.0001);

        assert(kch.get_lower_band().size() == 1721);
        assert(std::abs(kch.get_lower_band()[0] - 181.13) < 0.0001);
        assert(std::abs(kch.get_lower_band()[12] - 181.8944) < 0.0001);
        assert(std::abs(kch.get_lower_band()[20] - 175.9381) < 0.0001);
        assert(std::abs(kch.get_lower_band()[25] - 173.3489) < 0.0001);
        assert(std::abs(kch.get_lower_band()[35] - 175.6794) < 0.0001);
        assert(std::abs(kch.get_lower_band()[1720] - 110.3163) < 0.0001);
        assert(std::abs(kch.get_lower_band()[1712] - 116.7584) < 0.0001);
        assert(std::abs(kch.get_lower_band()[1707] - 117.0264) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_TrixVisitor()  {

    std::cout << "\nTesting TrixVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        trix_v<double, std::string, 256> trix;

        df.single_act_visit<double>("IBM_Close", trix);

        assert(trix.get_result().size() == 1721);
        assert(std::isnan(trix.get_result()[0]));
        assert(std::abs(trix.get_result()[4] - 0.0009) < 0.0001);
        assert(std::abs(trix.get_result()[14] - 0.0001) < 0.0001);
        assert(std::abs(trix.get_result()[18] - -0.001) < 0.0001);
        assert(std::abs(trix.get_result()[25] - -0.0024) < 0.0001);
        assert(std::abs(trix.get_result()[1720] - -0.0027) < 0.0001);
        assert(std::abs(trix.get_result()[1712] - 0.0008) < 0.0001);
        assert(std::abs(trix.get_result()[1707] - 0.0003) < 0.0001);

        trix_v<double, std::string, 256> trix2 (14, true);

        df.single_act_visit<double>("IBM_Close", trix2);

        assert(trix2.get_result().size() == 1721);
        assert(std::isnan(trix2.get_result()[0]));
        assert(std::isnan(trix2.get_result()[5]));
        assert(std::abs(trix2.get_result()[6] - 0.0008) < 0.0001);
        assert(std::abs(trix2.get_result()[14] - 0.0003) < 0.0001);
        assert(std::abs(trix2.get_result()[18] - -0.0002) < 0.0001);
        assert(std::abs(trix2.get_result()[25] - -0.0019) < 0.0001);
        assert(std::abs(trix2.get_result()[1720] - -0.0011) < 0.0001);
        assert(std::abs(trix2.get_result()[1712] - 0.0006) < 0.0001);
        assert(std::abs(trix2.get_result()[1707] - -0.0005) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_PrettyGoodOsciVisitor()  {

    std::cout << "\nTesting PrettyGoodOsciVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        pgo_v<double, std::string, 256>  pgo;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", pgo);

        assert(pgo.get_result().size() == 1721);
        assert(std::isnan(pgo.get_result()[0]));
        assert(std::isnan(pgo.get_result()[12]));
        assert(std::abs(pgo.get_result()[14] - -1.3523) < 0.0001);
        assert(std::abs(pgo.get_result()[20] - -1.8941) < 0.0001);
        assert(std::abs(pgo.get_result()[25] - -0.2143) < 0.0001);
        assert(std::abs(pgo.get_result()[35] - 1.3048) < 0.0001);
        assert(std::abs(pgo.get_result()[1720] - -1.7059) < 0.0001);
        assert(std::abs(pgo.get_result()[1712] - -2.2014) < 0.0001);
        assert(std::abs(pgo.get_result()[1707] - 0.652) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_col_name_to_idx()  {

    std::cout << "\nTesting col_name_to_idx( )/col_idx_to_name( ) ..." <<
              std::endl;

    MyDataFrame df;

    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, 14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0,
          9.0, 10.0 };
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8, 111.0,
          112.0, 113.0, 114.0, 115.0, 116.0 };
    StlVecType<int>            intvec =
        { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9, 10, 15, 12, 13 };
    StlVecType<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("int_col", intvec),
                 std::make_pair("str_col", strvec));

    assert ((df.col_name_to_idx("int_col") == 1));
    assert ((df.col_name_to_idx("str_col") == 2));
    try  {
        std::cout << df.col_name_to_idx("xxxxx") << std::endl;
        assert(false);
    }
    catch (const ColNotFound &)  {
        assert(true);
    }

    df.load_column("dbl_col_2", std::move(dblvec2));

    assert ((df.col_name_to_idx("int_col") == 1));
    assert ((df.col_name_to_idx("str_col") == 2));
    assert ((df.col_name_to_idx("dbl_col_2") == 3));
    assert ((! strcmp(df.col_idx_to_name(0), "dbl_col")));
    assert ((! strcmp(df.col_idx_to_name(3), "dbl_col_2")));
    try  {
        std::cout << df.col_idx_to_name(5) << std::endl;
        assert(false);
    }
    catch (const ColNotFound &)  {
        assert(true);
    }
}

// -----------------------------------------------------------------------------

static void test_ZeroLagMovingMeanVisitor()  {

    std::cout << "\nTesting ZeroLagMovingMeanVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        zlmm_v<double, std::string, 256>  zlmm(10);

        df.single_act_visit<double>("IBM_Close", zlmm);

        assert(zlmm.get_result().size() == 1721);
        assert(std::isnan(zlmm.get_result()[0]));
        assert(std::isnan(zlmm.get_result()[3]));
        assert(std::abs(zlmm.get_result()[14] - 184.6943) < 0.0001);
        assert(std::abs(zlmm.get_result()[20] - 175.7459) < 0.0001);
        assert(std::abs(zlmm.get_result()[25] - 174.5764) < 0.0001);
        assert(std::abs(zlmm.get_result()[35] - 183.6864) < 0.0001);
        assert(std::abs(zlmm.get_result()[1720] - 108.6729) < 0.0001);
        assert(std::abs(zlmm.get_result()[1712] - 122.576) < 0.0001);
        assert(std::abs(zlmm.get_result()[1707] - 127.9991) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}


// -----------------------------------------------------------------------------

static void test_describe()  {

    std::cout << "\nTesting describe( ) ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        auto    desc = df.describe<double, long>();

        desc.write<std::ostream, double>(std::cout, io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_T3MovingMeanVisitor()  {

    std::cout << "\nTesting T3MovingMeanVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        t3_v<double, std::string, 256>   t3;

        df.single_act_visit<double> ("IBM_Close", t3);

        assert(t3.get_result().size() == 5031);
        assert(std::abs(t3.get_result()[0] - 98.5625) < 0.0001);
        assert(std::abs(t3.get_result()[12] - 99.065) < 0.001);
        assert(std::abs(t3.get_result()[14] - 99.2797) < 0.0001);
        assert(std::abs(t3.get_result()[20] - 99.3028) < 0.0001);
        assert(std::abs(t3.get_result()[5030] - 116.5671) < 0.0001);
        assert(std::abs(t3.get_result()[5026] - 122.7203) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_append_row()  {

    std::cout << "\nTesting append_row( ) ..." << std::endl;

    MyDataFrame                 df;
    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, -14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0,
          3.0, 9.0, 10.0};
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8,
          111.0, 112.0, 113.0, 114.0, 115.0, 116.0};
    StlVecType<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    StlVecType<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };
    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col", std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    df.write<std::ostream, std::string, double, int>
        (std::cout, io_format::csv2);

    unsigned long   index_val = 15;

    df.append_row(&index_val,
                  std::make_pair("dbl_col", 100.0),
                  std::make_pair("dbl_col_2", 300.0),
                  std::make_pair("str_col", std::string("APPEND")));
    df.write<std::ostream, std::string, double, int>
        (std::cout, io_format::csv2);

    df.append_row(&index_val,
                  std::make_pair("dbl_col", 200.0),
                  std::make_pair("dbl_col_2", 600.0),
                  std::make_pair("int_col", 10000),
                  std::make_pair("str_col", std::string("HYPE")));
    df.write<std::ostream, std::string, double, int>
        (std::cout, io_format::csv2);
}

// -----------------------------------------------------------------------------

static void test_load_result_as_column()  {

    std::cout << "\nTesting load_result_as_column( ) ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        // CoppockCurveVisitor
        //
        coppc_v<double, std::string, 256>  copp;

        df.single_act_visit<double>("IBM_Close", copp);
        df.load_result_as_column(copp, "IBM_close_curve");

        const auto curve_col = df.get_column<double>("IBM_close_curve");

        assert(curve_col.size() == 1721);
        assert(std::isnan(curve_col[0]));
        assert(std::abs(curve_col[14] - -0.051884971603) < 0.0000001);
        assert(std::abs(curve_col[18] - -0.100660882748) < 0.0000001);
        assert(std::abs(curve_col[25] - -0.124090378548) < 0.0000001);
        assert(std::abs(curve_col[1720] - -0.219247796696) < 0.0000001);
        assert(std::abs(curve_col[1712] - 0.0630742594051) < 0.0000001);
        assert(std::abs(curve_col[1707] - 0.0766481878384) < 0.0000001);
        assert(copp.get_result().size() == 0); // Data was moved
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_load_indicators()  {

    std::cout << "\nTesting load_indicators( ) ..." << std::endl;

    MyDataFrame                 df;
    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, 14.0, 0.0, 1.0, 14.0, 11.5, 11.5, 7.25, 7.25, 7.25, 14.0,
          7.25, 15.0, 0.0};
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8,
          111.0, 112.0, 113.0, 114.0, 115.0, 116.0};
    StlVecType<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    StlVecType<std::string>    strvec =
        { "blue", "blue", "red", "green", "black", "green", "white", "black",
          "black", "white", "red", "yellow", "green", "green", "green" };
    df.load_data(std::move(idxvec),
                 std::make_pair("levels", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("colors", strvec));
    df.load_column("int_col", std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    df.write<std::ostream, std::string, double, int>
        (std::cout, io_format::csv2);

    auto  count = df.load_indicators<std::string>("colors");

    df.write<std::ostream, std::string, double, int>
        (std::cout, io_format::csv2);
    std::cout << "Load count: " << count << std::endl;

    count = df.load_indicators<std::string, bool>("colors", "bool_");
    df.write<std::ostream, std::string, double, int, bool>
        (std::cout, io_format::csv2);
    std::cout << "Load count: " << count << std::endl;

    count = df.load_indicators<double, double>("levels", "level_");
    df.write<std::ostream, std::string, double, int, bool>
        (std::cout, io_format::csv2);
    std::cout << "Load count: " << count << std::endl;
}

// -----------------------------------------------------------------------------

static void test_from_indicators()  {

    std::cout << "\nTesting from_indicators( ) ..." << std::endl;

    MyDataFrame                 df;
    StlVecType<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    StlVecType<double>         dblvec =
        { 0.0, 15.0, 14.0, 0.0, 1.0, 14.0, 11.5, 11.5, 7.25, 7.25, 7.25, 14.0,
          7.25, 15.0, 0.0};
    StlVecType<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8,
          111.0, 112.0, 113.0, 114.0, 115.0, 116.0};
    StlVecType<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    StlVecType<std::string>    strvec =
        { "blue", "blue", "red", "green", "black", "green", "white", "black",
          "black", "white", "red", "yellow", "green", "green", "green" };
    df.load_data(std::move(idxvec),
                 std::make_pair("levels", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("colors", strvec));
    df.load_column("int_col", std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    df.load_indicators<std::string>("colors");
    df.load_indicators<std::string, bool>("colors", "bool_");
    df.load_indicators<double, double>("levels", "level_");

    auto  count =
        df.from_indicators(
            { "blue", "green", "white", "black", "red", "yellow" },
            "colors_copy");

    assert(count == 15);
    assert((df.get_column<std::string>("colors") ==
            df.get_column<std::string>("colors_copy")));

    count =
        df.from_indicators<double, double>(
            { "level_0", "level_15", "level_14", "level_1",
              "level_11.5", "level_7.25" },
            "levels_copy",
            "level_");
    assert(count == 15);
    assert((df.get_column<std::string>("levels") ==
            df.get_column<std::string>("levels_copy")));
}

// -----------------------------------------------------------------------------

static void test_TreynorRatioVisitor()  {

    std::cout << "\nTesting TreynorRatioVisitor{  } ..." << std::endl;

    StlVecType<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    StlVecType<double>         d1 =
        { 2.5, 2.45, -0.65, -0.1, -1.1, 1.87, 0.98, 0.34, 1.56, -0.34, 2.3,
          -0.34, -1.9, 0.387, 0.123, 1.06, -0.65, 2.03, 0.4, -1.0, 0.59 };
    StlVecType<double>         d2 =
        { 0.2, 0.58, -0.60, -0.08, 0.05, 0.87, 0.2, 0.4, 0.5, 0.06, 0.3, -0.34,
          -0.9, 0.8, -0.4, 0.86, 0.01, 1.02, -0.02, -1.5, 0.2 };
    StlVecType<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("asset", d1),
                 std::make_pair("benchmark", d2),
                 std::make_pair("col_3", i1));

    treynorr_v<double>  ty_ratio;
    const auto          result =
        df.single_act_visit<double, double>("asset", "benchmark",
                                            ty_ratio, true).get_result();

    assert(fabs(result - 0.26761) < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_ImpurityVisitor()  {

    std::cout << "\nTesting ImpurityVisitor{  } ..." << std::endl;

    StlVecType<unsigned long>   idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    StlVecType<std::string>     metal = { "Gold", "Gold", "Gold", "Gold" };
    StlVecType<std::string>     metal2 = { "Gold", "Silver", "Silver", "Gold" };
    StlVecType<double>          numbers =
        { 2.5, 2.5, 2.5, -0.1, -1.1, -0.1, -1.1, -1.1, -0.1, 34.5, -1.1,
          34.5, 34.5, 34.5, 0.123, 0.123, 0.123, 0.5, 0.4, 2.5, 0.5 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("Numbers", numbers));
    df.load_column("Metals", std::move(metal),
                   nan_policy::dont_pad_with_nans);
    df.load_column("Metals2", std::move(metal2),
                   nan_policy::dont_pad_with_nans);

    impu_v<std::string> impu (4, impurity_type::gini_index);
    const auto          result =
        df.single_act_visit<std::string>("Metals", impu).get_result();

    assert(result.size() == 1);
    assert(result[0] == 0);

    impu_v<std::string> impu2 (4, impurity_type::info_entropy);
    const auto          result2 =
        df.single_act_visit<std::string>("Metals", impu2).get_result();

    assert(result2.size() == 1);
    assert(result2[0] == 0);

    impu_v<std::string> impu3 (4, impurity_type::gini_index);
    const auto          result3 =
        df.single_act_visit<std::string>("Metals2", impu3).get_result();

    assert(result3.size() == 1);
    assert(result3[0] == 0.5);

    impu_v<std::string> impu4 (4, impurity_type::info_entropy);
    const auto          result4 =
        df.single_act_visit<std::string>("Metals2", impu4).get_result();

    assert(result4.size() == 1);
    assert(result4[0] == 1.0);

    impu_v<double>  impu5 (3, impurity_type::gini_index);
    const auto      result5 =
        df.single_act_visit<double>("Numbers", impu5).get_result();

    assert(result5.size() == 19);
    assert(result5[0] == 0);
    assert(fabs(result5[1] - 0.4444) < 0.0001);
    assert(fabs(result5[2] - 0.6667) < 0.0001);
    assert(fabs(result5[3] - 0.4444) < 0.0001);
    assert(fabs(result5[4] - 0.4444) < 0.0001);
    assert(fabs(result5[18] - 0.6667) < 0.0001);
    assert(fabs(result5[17] - 0.6667) < 0.0001);
    assert(fabs(result5[16] - 0.6667) < 0.0001);
    assert(fabs(result5[15] - 0.4444) < 0.0001);
    assert(result5[14] == 0);
    assert(fabs(result5[13] - 0.4444) < 0.0001);

    impu_v<double>  impu6 (3, impurity_type::info_entropy);
    const auto      &result6 =
        df.single_act_visit<double>("Numbers", impu6).get_result();

    assert(result6.size() == 19);
    assert(result6[0] == 0);
    assert(fabs(result6[1] - 0.9183) < 0.0001);
    assert(fabs(result6[2] - 1.585) < 0.0001);
    assert(fabs(result6[3] - 0.9183) < 0.0001);
    assert(fabs(result6[4] - 0.9183) < 0.0001);
    assert(fabs(result6[18] - 1.585) < 0.0001);
    assert(fabs(result6[17] - 1.585) < 0.0001);
    assert(fabs(result6[16] - 1.585) < 0.0001);
    assert(fabs(result6[15] - 0.9183) < 0.0001);
    assert(result6[14] == 0);
    assert(fabs(result6[13] - 0.9183) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_ExponentiallyWeightedVarVisitor()  {

    std::cout << "\nTesting ExponentiallyWeightedVarVisitor{  } ..."
              << std::endl;

    StlVecType<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468 };
    StlVecType<double>         d1 =
        { 1.0, 1.5, 1.0, 1.2, 1.7, 1.5, 1.2, 1.7, 1.7, 1.3, 1.4, 1.5, 1.2,
          1.1, 1.15, 1.0 };
    StlVecType<double>         d2 =
        { 0.2, 0.58, -0.60, -0.08, 0.05, 0.87, 0.2, 0.4, 0.5, 0.06, 0.3, -0.34,
          -0.9, 0.8, -0.4, 0.86 };
    StlVecType<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                df;

    df.load_data(std::move(idx),
                 std::make_pair("values", d1),
                 std::make_pair("benchmark", d2),
                 std::make_pair("col_3", i1));

    ewm_var_v<double>   ewmvar(exponential_decay_spec::span, 3);
    const auto          &result =
        df.single_act_visit<double>("values", ewmvar).get_result();

    assert(result.size() == 16);
    assert(std::isnan(result[0]));
    assert(fabs(result[1] - 0.125) < 0.001);
    assert(fabs(result[2] - 0.0893) < 0.0001);
    assert(fabs(result[3] - 0.0396) < 0.0001);
    assert(fabs(result[4] - 0.1258) < 0.0001);
    assert(fabs(result[15] - 0.0228) < 0.0001);
    assert(fabs(result[14] - 0.0205) < 0.0001);
    assert(fabs(result[13] - 0.0377) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_ExponentiallyWeightedCovVisitor()  {

    std::cout << "\nTesting ExponentiallyWeightedCovVisitor{  } ..."
              << std::endl;

    StlVecType<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468 };
    StlVecType<double>         d1 =
        { 1.0, 1.5, 1.0, 1.2, 1.7, 1.5, 1.2, 1.7, 1.7, 1.3, 1.4, 1.5, 1.2,
          1.1, 1.15, 1.0 };
    StlVecType<double>         d2 =
        { 1.5, 1.0, 1.1, 1.3, 1.35, 1.2, 1.2, 1.6, 1.6, 1.8, 1.4, 1.5, 1.25,
          1.3, 1.25, 1.0 };
    StlVecType<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                df;

    df.load_data(std::move(idx),
                 std::make_pair("X Column", d1),
                 std::make_pair("Y Column", d2),
                 std::make_pair("col_3", i1));

    ewm_cov_v<double>   ewmcov(exponential_decay_spec::span, 3);
    const auto          &result =
        df.single_act_visit<double, double>
            ("X Column", "Y Column", ewmcov).get_result();

    assert(result.size() == 16);
    assert(std::isnan(result[0]));
    assert(fabs(result[1] - -0.125) < 0.001);
    assert(fabs(result[2] - -0.0321) < 0.0001);
    assert(fabs(result[3] - -0.0099) < 0.0001);
    assert(fabs(result[4] - 0.0219) < 0.0001);
    assert(fabs(result[15] - 0.0263) < 0.0001);
    assert(fabs(result[14] - 0.0121) < 0.0001);
    assert(fabs(result[13] - 0.0197) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_ExponentiallyWeightedCorrVisitor()  {

    std::cout << "\nTesting ExponentiallyWeightedCorrVisitor{  } ..."
              << std::endl;

    StlVecType<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468 };
    StlVecType<double>         d1 =
        { 1.0, 1.5, 1.0, 1.2, 1.7, 1.5, 1.2, 1.7, 1.7, 1.3, 1.4, 1.5, 1.2,
          1.1, 1.15, 1.0 };
    StlVecType<double>         d2 =
        { 1.5, 1.0, 1.1, 1.3, 1.35, 1.2, 1.2, 1.6, 1.6, 1.8, 1.4, 1.5, 1.25,
          1.3, 1.25, 1.0 };
    StlVecType<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                df;

    df.load_data(std::move(idx),
                 std::make_pair("X Column", d1),
                 std::make_pair("Y Column", d2),
                 std::make_pair("col_3", i1));

    ewm_corr_v<double>  ewmcorr(exponential_decay_spec::span, 3);
    const auto          &result =
        df.single_act_visit<double, double>
            ("X Column", "Y Column", ewmcorr).get_result();

    assert(result.size() == 16);
    assert(std::isnan(result[0]));
    assert(fabs(result[1] - -1.0) < 0.001);
    assert(fabs(result[2] - -0.5153) < 0.0001);
    assert(fabs(result[3] - -0.2841) < 0.0001);
    assert(fabs(result[4] - 0.4287) < 0.0001);
    assert(fabs(result[15] - 0.8746) < 0.0001);
    assert(fabs(result[14] - 0.714) < 0.0001);
    assert(fabs(result[13] - 0.6865) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_reading_in_chunks()  {

    std::cout << "\nTesting reading_in_chunks(  ) ..." << std::endl;

    try  {
        StrDataFrame    df1;

        df1.read("data/SHORT_IBM.csv", io_format::csv2, false, 0, 10);
        assert(df1.get_index().size() == 10);
        assert(df1.get_column<double>("IBM_Close").size() == 10);
        assert(df1.get_index()[0] == "2014-01-02");
        assert(df1.get_index()[9] == "2014-01-15");
        assert(fabs(df1.get_column<double>("IBM_Close")[0] - 185.53) < 0.0001);
        assert(fabs(df1.get_column<double>("IBM_Close")[9] - 187.74) < 0.0001);

        StrDataFrame    df2;

        df2.read("data/SHORT_IBM.csv", io_format::csv2, false, 800, 10);
        assert(df2.get_index().size() == 10);
        assert(df2.get_column<double>("IBM_Close").size() == 10);
        assert(df2.get_index()[0] == "2017-03-08");
        assert(df2.get_index()[9] == "2017-03-21");
        assert(fabs(df2.get_column<double>("IBM_Close")[0] - 179.45) < 0.0001);
        assert(fabs(df2.get_column<double>("IBM_Close")[9] - 173.88) < 0.0001);

        StrDataFrame    df3;

        df3.read("data/SHORT_IBM.csv", io_format::csv2, false, 1716, 10);
        assert(df3.get_index().size() == 5);
        assert(df3.get_column<double>("IBM_Close").size() == 5);
        assert(df3.get_index()[0] == "2020-10-26");
        assert(df3.get_index()[4] == "2020-10-30");
        assert(fabs(df3.get_column<double>("IBM_Close")[0] - 112.22) < 0.0001);
        assert(fabs(df3.get_column<double>("IBM_Close")[4] - 111.66) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_FixedAutoCorrVisitor()  {

    std::cout << "\nTesting FixedAutoCorrVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        FixedAutoCorrVisitor<double,
                             std::string>   fac { 31, roll_policy::blocks };

        df.single_act_visit<double> ("IBM_Close", fac);

        assert(fac.get_result().size() == 162);
        assert(std::abs(fac.get_result()[0] - -0.5436) < 0.0001);
        assert(std::abs(fac.get_result()[12] - 0.1328) < 0.001);
        assert(std::abs(fac.get_result()[14] - -0.594) < 0.0001);
        assert(std::abs(fac.get_result()[161] - -0.1109) < 0.0001);
        assert(std::abs(fac.get_result()[160] - -0.231) < 0.0001);
        assert(std::abs(fac.get_result()[159] - 0.075) < 0.0001);

        FixedAutoCorrVisitor<double,
                             std::string>   fac2 { 31, roll_policy::continuous };

        df.single_act_visit<double> ("IBM_Close", fac2);

        assert(fac2.get_result().size() == 5000);
        assert(std::abs(fac2.get_result()[0] - -0.5436) < 0.0001);
        assert(std::abs(fac2.get_result()[12] - -0.7213) < 0.001);
        assert(std::abs(fac2.get_result()[14] - -0.6657) < 0.0001);
        assert(std::abs(fac2.get_result()[4999] - 0.1446) < 0.0001);
        assert(std::abs(fac2.get_result()[4998] - 0.1809) < 0.0001);
        assert(std::abs(fac2.get_result()[4997] - 0.1732) < 0.0001);

    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_RVIVisitor()  {

    std::cout << "\nTesting RVIVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        RVIVisitor<double, std::string> rvi;

        // Here we use exponentially weighted mean and also adjust for
        // finite series
        //
        df.single_act_visit<double, double, double>
            ("IBM_Close", "IBM_High", "IBM_Low", rvi);

        assert(rvi.get_result().size() == 1721);
        assert(std::isnan(rvi.get_result()[0]));
        assert(std::isnan(rvi.get_result()[12]));
        assert(rvi.get_result()[13] == 0);
        assert(std::abs(rvi.get_result()[14] - 100.0) < 0.0001);
        assert(std::abs(rvi.get_result()[15] - 41.8105) < 0.0001);
        assert(std::abs(rvi.get_result()[18] - 20.4976) < 0.0001);
        assert(std::abs(rvi.get_result()[25] - 42.6008) < 0.0001);
        assert(std::abs(rvi.get_result()[1720] - 43.5703) < 0.0001);
        assert(std::abs(rvi.get_result()[1712] - 38.7802) < 0.0001);
        assert(std::abs(rvi.get_result()[1707] - 32.3759) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_LinregMovingMeanVisitor()  {

    std::cout << "\nTesting LinregMovingMeanVisitor{  } ..." << std::endl;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        linregmm_v<double, std::string> linreg;

        df.single_act_visit<double>("IBM_Close", linreg);

        assert(linreg.get_result().size() == 1721);
        assert(std::isnan(linreg.get_result()[0]));
        assert(std::isnan(linreg.get_result()[13]));
        assert(std::abs(linreg.get_result()[14] - 186.9714) < 0.0001);
        assert(std::abs(linreg.get_result()[15] - 185.8377) < 0.0001);
        assert(std::abs(linreg.get_result()[16] - 184.1874) < 0.0001);
        assert(std::abs(linreg.get_result()[18] - 180.6528) < 0.0001);
        assert(std::abs(linreg.get_result()[25] - 172.4149) < 0.0001);
        assert(std::abs(linreg.get_result()[1720] - 109.3095) < 0.0001);
        assert(std::abs(linreg.get_result()[1712] - 126.9701) < 0.0001);
        assert(std::abs(linreg.get_result()[1707] - 126.8379) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

int main(int, char *[]) {

    test_groupby_edge();
    test_concat_view();
    test_to_from_string();
    test_CoppockCurveVisitor();
    test_BiasVisitor();
    test_BalanceOfPowerVisitor();
    test_ChandeKrollStopVisitor();
    test_VortexVisitor();
    test_KeltnerChannelsVisitor();
    test_TrixVisitor();
    test_PrettyGoodOsciVisitor();
    test_col_name_to_idx();
    test_ZeroLagMovingMeanVisitor();
    test_describe();
    test_T3MovingMeanVisitor();
    test_append_row();
    test_load_result_as_column();
    test_load_indicators();
    test_from_indicators();
    test_TreynorRatioVisitor();
    test_ImpurityVisitor();
    test_ExponentiallyWeightedVarVisitor();
    test_ExponentiallyWeightedCovVisitor();
    test_ExponentiallyWeightedCorrVisitor();
    test_reading_in_chunks();
    test_FixedAutoCorrVisitor();
    test_RVIVisitor();
    test_LinregMovingMeanVisitor();

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
