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

static void test_get_reindexed()  {

    std::cout << "\nTesting get_reindexed( ) ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, 14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0,
          9.0, 10.0 };
    std::vector<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8, 111.0,
          112.0, 113.0, 114.0, 115.0, 116.0 };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
       { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
         "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col",
                   std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    auto    result1 =
        df.get_reindexed<double, int, double, std::string>
            ("dbl_col", "OLD_IDX");

    assert(result1.get_index().size() == 15);
    assert(result1.get_column<double>("dbl_col_2").size() == 15);
    assert(result1.get_column<unsigned long>("OLD_IDX").size() == 15);
    assert(result1.get_column<std::string>("str_col").size() == 15);
    assert(result1.get_column<int>("int_col").size() == 11);
    assert(result1.get_index()[0] == 0);
    assert(result1.get_index()[14] == 10.0);
    assert(result1.get_column<int>("int_col")[3] == 4);
    assert(result1.get_column<int>("int_col")[9] == 14);
    assert(result1.get_column<std::string>("str_col")[5] == "ff");
    assert(result1.get_column<double>("dbl_col_2")[10] == 112.0);

    auto    result2 =
        df.get_reindexed<int, int, double, std::string>("int_col", "OLD_IDX");

    assert(result2.get_index().size() == 11);
    assert(result2.get_column<double>("dbl_col_2").size() == 11);
    assert(result2.get_column<double>("dbl_col").size() == 11);
    assert(result2.get_column<unsigned long>("OLD_IDX").size() == 11);
    assert(result2.get_column<std::string>("str_col").size() == 11);
    assert(result2.get_column<double>("dbl_col_2")[10] == 112.0);
    assert(result2.get_column<double>("dbl_col")[3] == 2.0);
    assert(result2.get_column<std::string>("str_col")[5] == "ff");
    assert(result2.get_index()[0] == 1);
    assert(result2.get_index()[10] == 9);
}

// -----------------------------------------------------------------------------

static void test_get_reindexed_view()  {

    std::cout << "\nTesting get_reindexed_view( ) ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, 14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0,
          9.0, 10.0 };
    std::vector<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8, 111.0,
          112.0, 113.0, 114.0, 115.0, 116.0 };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col",
                   std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    auto    result1 =
        df.get_reindexed_view<double, int, double, std::string>
            ("dbl_col", "OLD_IDX");

    assert(result1.get_index().size() == 15);
    assert(result1.get_column<double>("dbl_col_2").size() == 15);
    assert(result1.get_column<unsigned long>("OLD_IDX").size() == 15);
    assert(result1.get_column<std::string>("str_col").size() == 15);
    assert(result1.get_column<int>("int_col").size() == 11);
    assert(result1.get_index()[0] == 0);
    assert(result1.get_index()[14] == 10.0);
    assert(result1.get_column<int>("int_col")[3] == 4);
    assert(result1.get_column<int>("int_col")[9] == 14);
    assert(result1.get_column<std::string>("str_col")[5] == "ff");
    assert(result1.get_column<double>("dbl_col_2")[10] == 112.0);

    auto    result2 =
        df.get_reindexed_view<int, int, double, std::string>
            ("int_col", "OLD_IDX");

    assert(result2.get_index().size() == 11);
    assert(result2.get_column<double>("dbl_col_2").size() == 11);
    assert(result2.get_column<double>("dbl_col").size() == 11);
    assert(result2.get_column<unsigned long>("OLD_IDX").size() == 11);
    assert(result2.get_column<std::string>("str_col").size() == 11);
    assert(result2.get_column<double>("dbl_col_2")[10] == 112.0);
    assert(result2.get_column<double>("dbl_col")[3] == 2.0);
    assert(result2.get_column<std::string>("str_col")[5] == "ff");
    assert(result2.get_index()[0] == 1);
    assert(result2.get_index()[10] == 9);

    result2.get_column<double>("dbl_col")[3] = 1002.45;
    assert(result2.get_column<double>("dbl_col")[3] == 1002.45);
    assert(df.get_column<double>("dbl_col")[3] ==
           result2.get_column<double>("dbl_col")[3]);
}

// -----------------------------------------------------------------------------

static void test_retype_column()  {

    std::cout << "\nTesting retype_column( ) ..." << std::endl;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL,
          10UL, 13UL, 10UL, 15UL, 14UL };
    std::vector<int>            intvec =
        { -1, 2, 3, 4, 5, 8, -6, 7, 11, 14, -9, 12, 13, 14, 15 };
    std::vector<std::string>    strvec =
        { "11", "22", "33", "44", "55", "66", "-77", "88", "99", "100",
          "101", "102", "103", "104", "-105" };

    MyDataFrame df;

    df.load_data(std::move(idxvec),
                 std::make_pair("str_col", strvec),
                 std::make_pair("int_col", intvec));

    df.retype_column<int, unsigned int>("int_col");
    assert(df.get_index().size() == 15);
    assert(df.get_column<unsigned int>("int_col").size() == 15);
    assert(df.get_column<unsigned int>("int_col")[0] == 4294967295);
    assert(df.get_column<unsigned int>("int_col")[1] == 2);
    assert(df.get_column<unsigned int>("int_col")[6] == 4294967290);
    assert(df.get_column<unsigned int>("int_col")[8] == 11);
    assert(df.get_column<std::string>("str_col")[0] == "11");
    assert(df.get_column<std::string>("str_col")[6] == "-77");

    df.retype_column<std::string, int>("str_col",
                                       [](const std::string &val) -> int {
                                           return (std::stoi(val));
                                       });
    assert(df.get_index().size() == 15);
    assert(df.get_column<unsigned int>("int_col").size() == 15);
    assert(df.get_column<int>("str_col").size() == 15);
    assert(df.get_column<unsigned int>("int_col")[6] == 4294967290);
    assert(df.get_column<unsigned int>("int_col")[8] == 11);
    assert(df.get_column<int>("str_col")[0] == 11);
    assert(df.get_column<int>("str_col")[6] == -77);
}

// -----------------------------------------------------------------------------

static void test_load_align_column()  {

    std::cout << "\nTesting load_align_column( ) ..." << std::endl;

    std::vector<unsigned long>  idxvec =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
          16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28 };
    std::vector<int>            intvec =
        { -1, 2, 3, 4, 5, 8, -6, 7, 11, 14, -9, 12, 13, 14, 15 };
    std::vector<double>         summary_vec = { 100, 200, 300, 400, 500 };

    MyDataFrame df;

    df.load_data(std::move(idxvec), std::make_pair("int_col", intvec));
    df.load_align_column("summary_col",
                         std::move(summary_vec),
                         5,
                         true,
                         std::sqrt(-1));

    std::vector<double> summary_vec_2 = { 102, 202, 302, 402, 502 };

    df.load_align_column("summary_col_2",
                         std::move(summary_vec_2),
                         5,
                         false,
                         std::sqrt(-1));

    assert(df.get_column<double>("summary_col").size() == 28);
    assert(df.get_column<double>("summary_col_2").size() == 28);
    assert(df.get_column<double>("summary_col")[0] == 100);
    assert(std::isnan(df.get_column<double>("summary_col_2")[0]));
    assert(df.get_column<double>("summary_col")[5] == 200);
    assert(std::isnan(df.get_column<double>("summary_col")[6]));
    assert(df.get_column<double>("summary_col_2")[5] == 102);
    assert(df.get_column<double>("summary_col")[20] == 500);
    assert(df.get_column<double>("summary_col_2")[25] == 502);
    assert(std::isnan(df.get_column<double>("summary_col")[27]));
    assert(std::isnan(df.get_column<double>("summary_col")[26]));
    assert(std::isnan(df.get_column<double>("summary_col_2")[27]));
    assert(std::isnan(df.get_column<double>("summary_col_2")[26]));
}

// -----------------------------------------------------------------------------

static void test_get_columns_info()  {

    std::cout << "\nTesting get_columns_info( ) ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, 14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0,
          9.0, 10.0 };
    std::vector<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8, 111.0,
          112.0, 113.0, 114.0, 115.0, 116.0 };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col",
                   std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    auto    result = df.get_columns_info<int, double, std::string>();
    bool    dbl_col_found = false;
    bool    dbl_col_2_found = false;
    bool    str_col_found = false;
    bool    int_col_found = false;

    for (auto citer: result)  {
        if (std::get<0>(citer) == "dbl_col")  {
            dbl_col_found = true;
            assert(std::get<1>(citer) == 15);
            assert(std::get<2>(citer) == std::type_index(typeid(double)));
        }
        if (std::get<0>(citer) == "dbl_col_2")  {
            dbl_col_2_found = true;
            assert(std::get<1>(citer) == 15);
            assert(std::get<2>(citer) == std::type_index(typeid(double)));
        }
        if (std::get<0>(citer) == "str_col")  {
            str_col_found = true;
            assert(std::get<1>(citer) == 15);
            assert(std::get<2>(citer) == std::type_index(typeid(std::string)));
        }
        if (std::get<0>(citer) == "int_col")  {
            int_col_found = true;
            assert(std::get<1>(citer) == 11);
            assert(std::get<2>(citer) == std::type_index(typeid(int)));
        }
    }
    assert(dbl_col_found);
    assert(dbl_col_2_found);
    assert(str_col_found);
    assert(int_col_found);
}

// -----------------------------------------------------------------------------

static void test_CategoryVisitor()  {

    std::cout << "\nTesting CategoryVisitor{ } ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, 14.0, 15.0, 1.0, 12.0, 11.0, 8.0, 15.0, 6.0, sqrt(-1),
          4.0, 14.0, 14.0, 20.0 };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "zz", "ww", "ee", "ff", "gg", "zz", "ii", "jj", "kk",
          "ll", "mm", "ee", "" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col",
                   std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    CategoryVisitor<double> cat;
    auto                    result =
        df.single_act_visit<double>("dbl_col", cat).get_result();

    assert(result.size() == 15);
    assert(result[0] == 0);
    assert(result[1] == 1);
    assert(result[2] == 2);
    assert(result[3] == 1);
    assert(result[4] == 3);
    assert(result[8] == 1);
    assert(result[13] == 2);
    assert(result[12] == 2);
    assert(result[11] == 8);
    assert(result[10] == static_cast<unsigned long>(-1));

    CategoryVisitor<std::string>    cat2;
    auto                            result2 =
        df.single_act_visit<std::string>("str_col", cat2).get_result();

    assert(result2.size() == 15);
    assert(result2[0] == 0);
    assert(result2[1] == 1);
    assert(result2[2] == 0);
    assert(result2[13] == 3);
}

// -----------------------------------------------------------------------------

static void test_FactorizeVisitor()  {

    std::cout << "\nTesting FactorizeVisitor{ } ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, 14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0,
          9.0, 10.0 };
    std::vector<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8, 111.0,
          112.0, 113.0, 114.0, 115.0, 116.0 };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj", "kk",
          "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col",
                   std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    FactorizeVisitor<double>    fact([] (const double &f) -> bool {
                                         return (f > 106.0 && f < 114.0);
                                     });
    df.load_column("bool_col",
                   df.single_act_visit<double>("dbl_col_2", fact).get_result());
    assert(df.get_column<bool>("bool_col").size() == 15);
    assert(df.get_column<bool>("bool_col")[0] == false);
    assert(df.get_column<bool>("bool_col")[4] == false);
    assert(df.get_column<bool>("bool_col")[6] == true);
    assert(df.get_column<bool>("bool_col")[7] == true);
    assert(df.get_column<bool>("bool_col")[8] == false);
    assert(df.get_column<bool>("bool_col")[9] == true);
    assert(df.get_column<bool>("bool_col")[11] == true);
    assert(df.get_column<bool>("bool_col")[13] == false);
}

// -----------------------------------------------------------------------------

static void test_pattern_match()  {

    std::cout << "\nTesting pattern_match( ) ..." << std::endl;

    const size_t            item_cnt = 8192;
    MyDataFrame             df;
    RandGenParams<double>   p;

    p.mean = 5.6;
    p.std = 0.5;
    p.seed = 123;
    p.min_value = 0;
    p.max_value = 30;

    df.load_data(MyDataFrame::gen_sequence_index(0, item_cnt, 1),
                 std::make_pair("lognormal",
                                gen_lognormal_dist<double>(item_cnt, p)),
                 std::make_pair("normal",
                                gen_normal_dist<double>(item_cnt, p)),
                 std::make_pair("uniform_real",
                                gen_uniform_real_dist<double>(item_cnt, p)));
    p.mean = 0;
    p.std = 1.0;
    p.min_value = -30;
    p.max_value = 30;
    df.load_column("std_normal", gen_normal_dist<double>(item_cnt, p));
    df.load_column<unsigned long>(
        "increasing",
        MyDataFrame::gen_sequence_index(0, item_cnt, 1));

    bool    result =
        df.pattern_match<double>("lognormal",
                                 pattern_spec::normally_distributed,
                                 0.01);
    assert(result == false);

    result = df.pattern_match<double>("normal",
                                     pattern_spec::normally_distributed,
                                     0.01);
    assert(result == true);

    result = df.pattern_match<double>(
                 "std_normal",
                 pattern_spec::standard_normally_distributed,
                 0.05);
    assert(result == true);

    result = df.pattern_match<double>("lognormal",
                                      pattern_spec::lognormally_distributed,
                                      0.01);
    assert(result == true);

    result = df.pattern_match<double>("normal",
                                      pattern_spec::lognormally_distributed,
                                      0.01);
    assert(result == false);

    result = df.pattern_match<double>("uniform_real",
                                      pattern_spec::lognormally_distributed,
                                      1.0);
    assert(result == false);

    result = df.pattern_match<double>("uniform_real",
                                      pattern_spec::normally_distributed,
                                      0.05);
    assert(result == false);

    result = df.pattern_match<unsigned long>(
                 "increasing",
                 pattern_spec::monotonic_increasing);
    assert(result == true);
    result = df.pattern_match<unsigned long>(
                 "increasing",
                 pattern_spec::strictly_monotonic_increasing);
    assert(result == true);

    df.get_column<unsigned long>("increasing")[10] = 9;

    result = df.pattern_match<unsigned long>(
                 "increasing",
                 pattern_spec::monotonic_increasing);
    assert(result == true);
    result = df.pattern_match<unsigned long>(
                 "increasing",
                 pattern_spec::strictly_monotonic_increasing);
    assert(result == false);

    df.get_column<unsigned long>("increasing")[1000] = 988;

    result = df.pattern_match<unsigned long>(
                 "increasing",
                 pattern_spec::monotonic_increasing);
    assert(result == false);
    result = df.pattern_match<unsigned long>(
                 "increasing",
                 pattern_spec::strictly_monotonic_increasing);
    assert(result == false);
}

// -----------------------------------------------------------------------------

static void test_ClipVisitor()  {

    std::cout << "\nTesting ClipVisitor{ } ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, 14.0, 15.0, 1.0, 12.0, 11.0, 8.0, 15.0, 6.0, sqrt(-1),
          4.0, 14.0, 14.0, 20.0 };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "zz", "ww", "ee", "ff", "gg", "zz", "ii", "jj", "kk",
          "ll", "mm", "ee", "" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col",
                   std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    ClipVisitor<double> clip (14, 5);
    auto                result = df.visit<double>("dbl_col", clip).get_result();

    assert(result == 7);
    assert(df.get_column<double>("dbl_col")[0] == 5.0);
    assert(df.get_column<double>("dbl_col")[1] == 14.0);
    assert(df.get_column<double>("dbl_col")[2] == 14.0);
    assert(df.get_column<double>("dbl_col")[4] == 5.0);
    assert(df.get_column<double>("dbl_col")[5] == 12.0);
}

// -----------------------------------------------------------------------------

static void test_SharpeRatioVisitor()  {

    std::cout << "\nTesting SharpeRatioVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    std::vector<double>         d1 =
        { 2.5, 2.45, -0.65, -0.1, -1.1, 1.87, 0.98, 0.34, 1.56, -0.34, 2.3,
          -0.34, -1.9, 0.387, 0.123, 1.06, -0.65, 2.03, 0.4, -1.0, 0.59 };
    std::vector<double>         d2 =
        { 0.2, 0.58, -0.60, -0.08, 0.05, 0.87, 0.2, 0.4, 0.5, 0.06, 0.3, -0.34,
          -0.9, 0.8, -0.4, 0.86, 0.01, 1.02, -0.02, -1.5, 0.2 };
    std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("asset", d1),
                 std::make_pair("benchmark", d2),
                 std::make_pair("col_3", i1));

    SharpeRatioVisitor<double>  sh_ratio;
    const auto                  result =
        df.single_act_visit<double, double>("asset", "benchmark",
                                            sh_ratio, true).get_result();

    assert(fabs(result - 0.425631) < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_RankVisitor()  {

    std::cout << "\nTesting RankVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    std::vector<double>         d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
          20, 21 };
    std::vector<double>         d2 =
        { 10, 2, 3, 4, 5, 13, 7, 8, 9, 10, 1, 12, 13, 10, 15, 16, 17, 18, 19,
          20, 13 };
    std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("d1_col", d1),
                 std::make_pair("d2_col", d2),
                 std::make_pair("col_3", i1));

    RankVisitor<double> avg_rank_v(rank_policy::average);
    RankVisitor<double> first_rank_v(rank_policy::first);
    RankVisitor<double> last_rank_v(rank_policy::last);
    RankVisitor<double> actual_rank_v(rank_policy::actual);
    const auto          actual_result =
        df.single_act_visit<double>("d1_col", actual_rank_v).get_result();
    const auto          avg_result =
        df.single_act_visit<double>("d1_col", avg_rank_v).get_result();
    const auto          first_result =
        df.single_act_visit<double>("d1_col", first_rank_v).get_result();
    const auto          last_result =
        df.single_act_visit<double>("d1_col", last_rank_v).get_result();

    for (size_t i = 0; i < actual_result.size(); ++i)
        assert(actual_result[i] == double(i));
    assert(actual_result == avg_result);
    assert(actual_result == last_result);
    assert(actual_result == first_result);

    const auto  actual_result2 =
        df.single_act_visit<double>("d2_col", actual_rank_v).get_result();
    const auto  avg_result2 =
        df.single_act_visit<double>("d2_col", avg_rank_v).get_result();
    const auto  first_result2 =
        df.single_act_visit<double>("d2_col", first_rank_v).get_result();
    const auto  last_result2 =
        df.single_act_visit<double>("d2_col", last_rank_v).get_result();

    std::vector<double> ar_equal { 8, 1, 2, 3, 4, 12, 5, 6, 7, 9, 0, 11, 13,
                                   10, 15, 16, 17, 18, 19, 20, 14 };

    assert(actual_result2 == ar_equal);
    ar_equal = std::vector<double> { 9, 1, 2, 3, 4, 13, 5, 6, 7, 9, 0, 11, 13,
                                     9, 15, 16, 17, 18, 19, 20, 13 };
    assert(avg_result2 == ar_equal);
    ar_equal = std::vector<double> { 8, 1, 2, 3, 4, 12, 5, 6, 7, 8, 0, 11, 12,
                                     8, 15, 16, 17, 18, 19, 20, 12 };
    assert(first_result2 == ar_equal);
    ar_equal = std::vector<double> { 10, 1, 2, 3, 4, 14, 5, 6, 7, 10, 0, 11, 14,
                                     10, 15, 16, 17, 18, 19, 20, 14 };
    assert(last_result2 == ar_equal);
}

// -----------------------------------------------------------------------------

static void test_SigmoidVisitor()  {

    std::cout << "\nTesting SigmoidVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    std::vector<double>         d1 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
          20, 21 };
    std::vector<double>         d2 =
        { 0.23, 0.25, 0.256, 0.26, 0.268, 0.271, 0.279, 0.285, 0.29, 0.3, 0.5,
          -0.2, 1, 0, 2, 0, -0.1, 0.55, 0.58, 0.6, 0.7 };
    std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("d1_col", d1),
                 std::make_pair("d2_col", d2),
                 std::make_pair("col_3", i1));

    SigmoidVisitor<double>  sig_log(sigmoid_type::logistic);
    SigmoidVisitor<double>  sig_alg(sigmoid_type::algebraic);
    SigmoidVisitor<double>  sig_tan(sigmoid_type::hyperbolic_tan);
    SigmoidVisitor<double>  sig_atan(sigmoid_type::arc_tan);
    SigmoidVisitor<double>  sig_err(sigmoid_type::error_function);
    SigmoidVisitor<double>  sig_gud(sigmoid_type::gudermannian);
    SigmoidVisitor<double>  sig_smo(sigmoid_type::smoothstep);
    const auto              log_result =
        df.single_act_visit<double>("d1_col", sig_log).get_result();
    const auto              alg_result =
        df.single_act_visit<double>("d1_col", sig_alg).get_result();
    const auto              tan_result =
        df.single_act_visit<double>("d1_col", sig_tan).get_result();
    const auto              atan_result =
        df.single_act_visit<double>("d1_col", sig_atan).get_result();
    const auto              err_result =
        df.single_act_visit<double>("d1_col", sig_err).get_result();
    const auto              gud_result =
        df.single_act_visit<double>("d1_col", sig_gud).get_result();
    const auto              smo_result =
        df.single_act_visit<double>("d2_col", sig_smo).get_result();

    std::vector<double> result {
        0.731059, 0.880797, 0.952574, 0.982014, 0.993307, 0.997527, 0.999089,
        0.999665, 0.999877, 0.999955, 0.999983, 0.999994, 0.999998, 0.999999,
        1, 1, 1, 1, 1, 1, 1 };

    for (size_t idx = 0; idx < result.size(); ++idx)
        assert(fabs(result[idx] - log_result[idx]) < 0.00001);

    result = std::vector<double> {
        0.707107, 0.447214, 0.316228, 0.242536, 0.196116, 0.164399, 0.141421,
        0.124035, 0.110432, 0.0995037, 0.0905357, 0.0830455, 0.0766965,
        0.071247, 0.066519, 0.0623783, 0.058722, 0.05547, 0.0525588,
        0.0499376, 0.0475651 };
    for (size_t idx = 0; idx < result.size(); ++idx)
        assert(fabs(result[idx] - alg_result[idx]) < 0.00001);

    result = std::vector<double> {
        0.761594, 0.964028, 0.995055, 0.999329, 0.999909, 0.999988, 0.999998,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    for (size_t idx = 0; idx < result.size(); ++idx)
        assert(fabs(result[idx] - tan_result[idx]) < 0.00001);

    result = std::vector<double> {
        0.785398, 1.10715, 1.24905, 1.32582, 1.3734, 1.40565, 1.4289, 1.44644,
        1.46014, 1.47113, 1.48014, 1.48766, 1.49402, 1.49949, 1.50423, 1.50838,
        1.51204, 1.5153, 1.51821, 1.52084, 1.52321 };
    for (size_t idx = 0; idx < result.size(); ++idx)
        assert(fabs(result[idx] - atan_result[idx]) < 0.00001);

    result = std::vector<double> {
        0.842701, 0.995322, 0.999978, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1 };
    for (size_t idx = 0; idx < result.size(); ++idx)
       assert(fabs(result[idx] - err_result[idx]) < 0.00001);

    result = std::vector<double> {
        0.865769, 1.30176, 1.4713, 1.53417, 1.55732, 1.56584, 1.56897, 1.57013,
        1.57055, 1.57071, 1.57076, 1.57078, 1.57079, 1.57079, 1.5708, 1.5708,
        1.5708, 1.5708, 1.5708, 1.5708, 1.5708 };
    for (size_t idx = 0; idx < result.size(); ++idx)
       assert(fabs(result[idx] - gud_result[idx]) < 0.00001);

    result = std::vector<double> {
        0.134366, 0.15625, 0.163054, 0.167648, 0.176974, 0.180518, 0.190088,
        0.197377, 0.203522, 0.216, 0.5, 0, 1, 0, 1, 0, 0, 0.57475, 0.618976,
        0.648, 0.784 };
    for (size_t idx = 0; idx < result.size(); ++idx)
       assert(fabs(result[idx] - smo_result[idx]) < 0.00001);
}

// -----------------------------------------------------------------------------

static double my_max(const double &d1, const double &d2, const double &d3)  {

    return (std::max<double>({ d1, d2, d3 }));
}

static void test_combine()  {

    std::cout << "\nTesting combine( ) ..." << std::endl;

    std::vector<unsigned long>  idx1 =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    std::vector<unsigned long>  idx2 =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    std::vector<unsigned long>  idx3 =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    std::vector<unsigned long>  idx4 =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473 };
    std::vector<double>         d1 =
        { 1, 2, 100, 4, 5, 6, 7, 8, 9, 10, 11, 300, 13, 14, 15, 16, 17, 18, 19,
          20, 200 };
    std::vector<double>         d2 =
        { 1, 2, 1000, 4, 5, 6, 7, 8, 9, 10, 11, 3000, 13, 14, 15, 16, 17, 18,
          19, 20, 2000 };
    std::vector<double>         d3 =
        { 1, 2, 5000, 4, 5, 6, 7, 8, 9, 10, 11, 7000, 13, 14, 15, 16, 17, 18,
          19, 20, 8000 };
    std::vector<double>         d4 =
        { 1, 2, 10000, 4, 5, 6, 7, 8, 9, 10, 11, 20000, 13, 14, 15, 16, 17,
          18, 19, 20, 30000 };
    MyDataFrame                 df1;
    MyDataFrame                 df2;
    MyDataFrame                 df3;
    MyDataFrame                 df4;

    df1.load_data(std::move(idx1), std::make_pair("d1_col", d1));
    df2.load_data(std::move(idx2), std::make_pair("d1_col", d2));
    df3.load_data(std::move(idx3), std::make_pair("d1_col", d3));
    df4.load_data(std::move(idx4), std::make_pair("d1_col", d4));

    df1.load_column("d2_col", df1.combine<double>("d1_col", df2, df3, my_max));

    std::vector<double> result {
        1, 2, 5000, 4, 5, 6, 7, 8, 9, 10, 11, 7000, 13, 14, 15, 16, 17, 18,
        19, 20, 8000 };

    assert(df1.get_column<double>("d2_col") == result);
}

// -----------------------------------------------------------------------------

static void test_RSIVisitor()  {

    std::cout << "\nTesting RSIVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        RSIVisitor<double, std::string> rsi(return_policy::percentage);

        df.single_act_visit<double>("IBM_Close", rsi);

        assert(rsi.get_result().size() == 1708);
        assert(std::abs(rsi.get_result()[1] - 41.0826) < 0.0001);
        assert(std::abs(rsi.get_result()[9] - 35.7869) < 0.0001);
        assert(std::abs(rsi.get_result()[11] - 37.3135) < 0.0001);
        assert(std::abs(rsi.get_result()[15] - 46.4432) < 0.0001);
        assert(std::abs(rsi.get_result()[26] - 53.6717) < 0.0001);
        assert(std::abs(rsi.get_result()[1707] - 43.3186) < 0.0001);
        assert(std::abs(rsi.get_result()[1699] - 47.0717) < 0.0001);
        assert(std::abs(rsi.get_result()[1694] - 54.2938) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_remove_duplicates()  {

    std::cout << "\nTesting remove_duplicates( ) ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL,
          10UL, 13UL, 10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, 15.0, 14.0, 2.0, 15.0, 12.0, 11.0, 8.0, 7.0, 6.0,
          5.0, 4.0, 3.0, 9.0, 10.0 };
    std::vector<double>         dblvec2 =
        { 100.0, 101.0, 102.0, 103.0, 101.0, 105.0, 106.55, 107.34, 1.8, 111.0,
          112.0, 113.0, 114.0, 115.0, 116.0 };
    std::vector<int>            intvec = { 1, 2, 3, 4, 2, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "cc", "ww", "bb", "ff", "gg", "hh", "ii", "jj",
          "kk", "ll", "mm", "nn", "oo" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col",
                   std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    auto    result1 =
        df.remove_duplicates<double, int, double, std::string, int>
        ("dbl_col", "int_col", false, remove_dup_spec::keep_first);
    auto    result2 =
        df.remove_duplicates<double, double, int, std::string,
                             double, std::string, int>
        ("dbl_col", "dbl_col_2", "int_col", "str_col",
         false, remove_dup_spec::keep_first);

    std::vector<double>         actual_d {
        100, 101, 102, 103, 105, 106.55, 107.34, 1.8, 111, 112, 113,
        114, 115, 116 };
    std::vector<std::string>    actual_s {
        "zz", "bb", "cc", "ww", "ff", "gg", "hh",  "ii", "jj", "kk",
        "ll", "mm", "nn", "oo" };

    assert(result2.get_index().size() == 14);
    assert(result2.get_column<double>("dbl_col_2") == actual_d);
    assert(result2.get_column<std::string>("str_col") == actual_s);

    auto    result3 =
        df.remove_duplicates<double, double, int, std::string,
                             double, std::string, int>
        ("dbl_col", "dbl_col_2", "int_col", "str_col",
         false, remove_dup_spec::keep_last);

    actual_d = std::vector<double> {
        100, 102, 103, 101, 105, 106.55, 107.34, 1.8, 111, 112, 113,
        114, 115, 116 };
    actual_s = std::vector<std::string> {
        "zz", "cc", "ww", "bb", "ff", "gg", "hh",  "ii", "jj", "kk",
        "ll", "mm", "nn", "oo" };
    assert(result3.get_index().size() == 14);
    assert(result3.get_column<double>("dbl_col_2") == actual_d);
    assert(result3.get_column<std::string>("str_col") == actual_s);

    auto    result4 =
        df.remove_duplicates<double, double, int, std::string,
                             double, std::string, int>
        ("dbl_col", "dbl_col_2", "int_col", "str_col",
         false, remove_dup_spec::keep_none);

    actual_d = std::vector<double> {
        100, 102, 103, 105, 106.55, 107.34, 1.8, 111, 112, 113,
        114, 115, 116 };
    actual_s = std::vector<std::string> {
        "zz", "cc", "ww", "ff", "gg", "hh",  "ii", "jj", "kk",
        "ll", "mm", "nn", "oo" };
    assert(result4.get_index().size() == 13);
    assert(result4.get_column<double>("dbl_col_2") == actual_d);
    assert(result4.get_column<std::string>("str_col") == actual_s);

    auto    result5 =
        df.remove_duplicates<double, double, int, std::string,
                             double, std::string, int>
        ("dbl_col", "dbl_col_2", "int_col", "str_col",
         true, remove_dup_spec::keep_none);

    actual_d = std::vector<double> {
        100, 101, 102, 103, 101, 105, 106.55, 107.34, 1.8, 111, 112, 113,
        114, 115, 116 };
    actual_s = std::vector<std::string> {
        "zz", "bb", "cc", "ww", "bb", "ff", "gg", "hh",  "ii", "jj", "kk",
        "ll", "mm", "nn", "oo" };
    assert(result5.get_index().size() == 15);
    assert(result5.get_column<double>("dbl_col_2") == actual_d);
    assert(result5.get_column<std::string>("str_col") == actual_s);

    auto    result6 =
        df.remove_duplicates<double, double, std::string, int>
        ("dbl_col", false, remove_dup_spec::keep_first);

    actual_d = std::vector<double>
        { 100, 101, 102, 103, 105, 106.55, 107.34, 1.8, 111, 112, 113, 114,
          115, 116 };
    actual_s = std::vector<std::string>
        { "zz", "bb", "cc", "ww", "ff", "gg", "hh", "ii", "jj", "kk", "ll",
          "mm", "nn", "oo"
        };
    assert(result6.get_index().size() == 14);
    assert(result6.get_column<double>("dbl_col_2") == actual_d);
    assert(result6.get_column<std::string>("str_col") == actual_s);
}

// -----------------------------------------------------------------------------

static void test_bucketize()  {

    std::cout << "\nTesting bucketize( ) ..." << std::endl;

    MyDataFrame df;

    try  {
        df.read("data/FORD.csv", io_format::csv2);

        auto        fut =
            df.bucketize_async(
                bucket_type::by_distance,
                100,
                LastVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
                std::make_tuple("Date", "Date", LastVisitor<std::string>()),
                std::make_tuple("FORD_Close", "High", MaxVisitor<double>()),
                std::make_tuple("FORD_Close", "Low", MinVisitor<double>()),
                std::make_tuple("FORD_Close", "Open", FirstVisitor<double>()),
                std::make_tuple("FORD_Close", "Close", LastVisitor<double>()),
                std::make_tuple("FORD_Close", "Mean", MeanVisitor<double>()),
                std::make_tuple("FORD_Close", "Std", StdVisitor<double>()),
                std::make_tuple("FORD_Volume", "Volume", SumVisitor<long>()));
        MyDataFrame result = fut.get();

        result.write<std::ostream, std::string, double, long>
            (std::cout, io_format::csv2);

        // FORD index is just an increasing number starting from 0.
        // So, by_count should give the same result as by_distance
        //
        auto        fut2 =
            df.bucketize_async(
                bucket_type::by_count,
                100,
                LastVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
                std::make_tuple("Date", "Date", LastVisitor<std::string>()),
                std::make_tuple("FORD_Close", "High", MaxVisitor<double>()),
                std::make_tuple("FORD_Close", "Low", MinVisitor<double>()),
                std::make_tuple("FORD_Close", "Open", FirstVisitor<double>()),
                std::make_tuple("FORD_Close", "Close", LastVisitor<double>()),
                std::make_tuple("FORD_Close", "Mean", MeanVisitor<double>()),
                std::make_tuple("FORD_Close", "Std", StdVisitor<double>()),
                std::make_tuple("FORD_Volume", "Volume", SumVisitor<long>()));
        MyDataFrame result2 = fut2.get();

        assert((result.is_equal<double, std::string, long>(result2)));
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_groupby()  {

    std::cout << "\nTesting groupby( ) ..." << std::endl;

    std::vector<unsigned long>  ulgvec2 =
        { 123450, 123451, 123452, 123450, 123455, 123450, 123449,
          123450, 123451, 123450, 123452, 123450, 123455, 123450,
          123454, 123450, 123450, 123457, 123458, 123459, 123450,
          123441, 123442, 123432, 123450, 123450, 123435, 123450 };
    std::vector<unsigned long>  xulgvec2 = ulgvec2;
    std::vector<int>            intvec2 =
        { 1, 2, 3, 4, 5, 3, 7, 3, 9, 10, 3, 2, 3, 14,
          2, 2, 2, 3, 2, 3, 3, 3, 3, 3, 36, 2, 45, 2 };
    std::vector<double>         xdblvec2 =
        { 10, 20, 11, 11, 30, 40, 50, 40, 60, 70, 80, 90, 50, 100, 11, 25, 20,
          30, 1, 3, 4, 12, 6, 2, 3, 10, 4, 5 };
    std::vector<double>         dblvec22 =
        { 0.998, 1.545, 0.056, 0.15678, 1.545, 0.923, 0.06743,
          0.1, -1.545, 0.07865, -0.9999, 1.545, 0.1002, -0.8888,
          0.14, 0.0456, -1.545, -0.8999, 0.01119, 0.8002, -1.545,
          0.2, 0.1056, 0.87865, -0.6999, 1.545, 0.1902, -1.545 };
    std::vector<std::string>    strvec2 =
        { "A", "B", "C", "D", "X", "Y", "W", "P", "Z", "S", "M", "B",
          "A", "H", "X", "Q", "V", "P", "W", "K", "I", "L", "J", "N",
          "Y", "G", "T", "U" };

    MyDataFrame df;

    df.load_data(std::move(ulgvec2),
                 std::make_pair("xint_col", intvec2),
                 std::make_pair("dbl_col", xdblvec2),
                 std::make_pair("dbl_col_2", dblvec22),
                 std::make_pair("str_col", strvec2),
                 std::make_pair("ul_col", xulgvec2));

    auto    fut1 =
        df.groupby1_async<unsigned long>
            (DF_INDEX_COL_NAME,
             LastVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));
    auto    result1 = fut1.get();

    result1.write<std::ostream, std::string, double, int>
        (std::cout, io_format::csv2);

    auto    fut2 =
        df.groupby1_async<unsigned long>
            ("ul_col",
             LastVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));
    auto    result2 = fut2.get();

    result2.write<std::ostream, std::string, double, int, unsigned long>
        (std::cout, io_format::csv2);

    auto    fut3 =
        df.groupby1_async<double>
            ("dbl_col_2",
             MaxVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));
    auto    result3 = fut3.get();

    result3.write<std::ostream, std::string, double, int>
        (std::cout, io_format::csv2);
}

// -----------------------------------------------------------------------------

static void test_groupby_2()  {

// Due to a bug in MS VC++ compiler being used by appveyor, this cannot be
// compiled by them
//
#ifndef _MSC_VER

    std::cout << "\nTesting groupby_2( ) ..." << std::endl;

    std::vector<unsigned long>  ulgvec2 =
        { 123450, 123451, 123452, 123450, 123455, 123450, 123449,
          123450, 123451, 123450, 123452, 123450, 123455, 123450,
          123454, 123450, 123450, 123457, 123458, 123459, 123450,
          123441, 123442, 123432, 123450, 123450, 123435, 123450 };
    std::vector<unsigned long>  xulgvec2 = ulgvec2;
    std::vector<int>            intvec2 =
        { 1, 2, 3, 4, 5, 3, 7, 3, 9, 10, 3, 2, 3, 14,
          2, 2, 2, 3, 2, 3, 3, 3, 3, 3, 36, 2, 45, 2 };
    std::vector<double>         xdblvec2 =
        { 10, 20, 11, 11, 30, 40, 50, 40, 60, 70, 80, 90, 50, 100, 11, 25, 20,
          30, 1, 3, 4, 12, 6, 2, 3, 10, 4, 5 };
    std::vector<double>         dblvec22 =
        { 0.998, 1.545, 0.056, 0.15678, 1.545, 0.923, 0.06743,
          0.1, -1.545, 0.07865, -0.9999, 1.545, 0.1002, -0.8888,
          0.14, 0.0456, -1.545, -0.8999, 0.01119, 0.8002, -1.545,
          0.2, 0.1056, 0.87865, -0.6999, 1.545, 0.1902, -1.545 };
    std::vector<std::string>    strvec2 =
        { "A", "B", "C", "D", "X", "Y", "W", "P", "Z", "S", "M", "B",
          "A", "H", "X", "Q", "V", "P", "W", "K", "I", "L", "J", "N",
          "Y", "G", "T", "U" };

    MyDataFrame df;

    df.load_data(std::move(ulgvec2),
                 std::make_pair("xint_col", intvec2),
                 std::make_pair("dbl_col", xdblvec2),
                 std::make_pair("dbl_col_2", dblvec22),
                 std::make_pair("str_col", strvec2),
                 std::make_pair("ul_col", xulgvec2));

    auto    result1 =
        df.groupby2<unsigned long, double>
            (DF_INDEX_COL_NAME,
             "dbl_col_2",
             LastVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col_2", "cnt_dbl", CountVisitor<double>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));

    result1.write<std::ostream, std::string, double, std::size_t, int>
        (std::cout, io_format::csv2);

    auto    result2 =
        df.groupby2<double, unsigned long>
            ("dbl_col_2",
             DF_INDEX_COL_NAME,
             MinVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col_2", "cnt_dbl", CountVisitor<double>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));

    result2.write<std::ostream, std::string, double, std::size_t, int>
        (std::cout, io_format::csv2);

    auto    result3 =
        df.groupby2<double, int>
            ("dbl_col_2",
             "xint_col",
             MaxVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col_2", "cnt_dbl", CountVisitor<double>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));

    result3.write<std::ostream, std::string, double, std::size_t, int>
        (std::cout, io_format::csv2);

    auto    result4 =
        df.groupby2<int, double>
            ("xint_col",
             "dbl_col_2",
             FirstVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col_2", "cnt_dbl", CountVisitor<double>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));

    result4.write<std::ostream, std::string, double, std::size_t, int>
        (std::cout, io_format::csv2);

    auto    result5 =
        df.groupby2<std::string, unsigned long>
            ("str_col",
             DF_INDEX_COL_NAME,
             FirstVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col_2", "cnt_dbl", CountVisitor<double>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));

    result5.write<std::ostream, std::string, double, std::size_t, int>
        (std::cout, io_format::csv2);

#endif // !_MSC_VER
}

// -----------------------------------------------------------------------------

static void test_groupby_3()  {

// Due to a bug in MS VC++ compiler being used by appveyor, this cannot be
// compiled by them
//
#ifndef _MSC_VER

    std::cout << "\nTesting groupby_3( ) ..." << std::endl;

    std::vector<unsigned long>  ulgvec2 =
        { 1, 2, 2, 2, 3, 4, 5, 5, 6, 6, 6, 7, 8, 9, 10, 10, 10, 11, 11, 11, 12,
          13, 13, 14, 15, 16, 17, 17 };
    std::vector<unsigned long>  xulgvec2 = ulgvec2;
    std::vector<int>            intvec2 =
        { 1, 2, 3, 4, 5, 3, 7, 3, 9, 10, 3, 2, 3, 14,
          2, 2, 2, 3, 2, 3, 3, 3, 3, 3, 36, 2, 45, 2 };
    std::vector<double>         xdblvec2 =
        { 10, 20, 20, 11, 30, 40, 50, 50, 50, 50, 80, 90, 50, 11, 11, 25, 20,
          30, 1, 2, 2, 2, 6, 2, 3, 10, 4, 5 };
    std::vector<double>         dblvec22 =
        { 0.998, 1.545, 0.056, 0.15678, 1.545, 0.923, 0.06743,
          0.1, -1.545, 0.07865, -0.9999, 1.545, 0.1002, -0.8888,
          0.14, 0.0456, -1.545, -0.8999, 0.01119, 0.8002, -1.545,
          0.2, 0.1056, 0.87865, -0.6999, 1.545, 0.1902, -1.545 };
    std::vector<std::string>    strvec2 =
        { "A", "A", "A", "B", "C", "C", "C", "C", "Z", "S", "M", "B",
          "A", "H", "X", "B", "Y", "Y", "W", "K", "K", "K", "J", "N",
          "Y", "G", "K", "B" };

    MyDataFrame df;

    df.load_data(std::move(ulgvec2),
                 std::make_pair("xint_col", intvec2),
                 std::make_pair("dbl_col", xdblvec2),
                 std::make_pair("dbl_col_2", dblvec22),
                 std::make_pair("str_col", strvec2),
                 std::make_pair("ul_col", xulgvec2));

    auto    result1 =
        df.groupby3<double, unsigned long, std::string>
            ("dbl_col",
             DF_INDEX_COL_NAME,
             "str_col",
             LastVisitor<MyDataFrame::IndexType, MyDataFrame::IndexType>(),
             std::make_tuple("str_col", "sum_str", SumVisitor<std::string>()),
             std::make_tuple("xint_col", "max_int", MaxVisitor<int>()),
             std::make_tuple("xint_col", "min_int", MinVisitor<int>()),
             std::make_tuple("dbl_col_2", "cnt_dbl", CountVisitor<double>()),
             std::make_tuple("dbl_col", "sum_dbl", SumVisitor<double>()));

    result1.write<std::ostream, std::string, double, std::size_t, int>
        (std::cout, io_format::csv2);

#endif // !_MSC_VER
}

// -----------------------------------------------------------------------------

static void test_io_format_csv2()  {

    std::cout << "\nTesting io_format_csv2( ) ..." << std::endl;

    std::vector<unsigned long>  ulgvec2 =
        { 123450, 123451, 123452, 123450, 123455, 123450, 123449,
          123450, 123451, 123450, 123452, 123450, 123455, 123450,
          123454, 123450, 123450, 123457, 123458, 123459, 123450,
          123441, 123442, 123432, 123450, 123450, 123435, 123450 };
    std::vector<unsigned long>  xulgvec2 = ulgvec2;
    std::vector<int>            intvec2 =
        { 1, 2, 3, 4, 5, 3, 7, 3, 9, 10, 3, 2, 3, 14,
          2, 2, 2, 3, 2, 3, 3, 3, 3, 3, 36, 2, 45, 2 };
    std::vector<double>         xdblvec2 =
        { 1.2345, 2.2345, 3.2345, 4.2345, 5.2345, 3.0, 0.9999,
          10.0, 4.25, 0.009, 8.0, 2.2222, 3.3333,
          11.0, 5.25, 1.009, 2.111, 9.0, 3.2222, 4.3333,
          12.0, 6.25, 2.009, 3.111, 10.0, 4.2222, 5.3333 };
    std::vector<double>         dblvec22 =
        { 0.998, 0.3456, 0.056, 0.15678, 0.00345, 0.923, 0.06743,
          0.1, 0.0056, 0.07865, 0.0111, 0.1002, -0.8888,
          0.14, 0.0456, 0.078654, -0.8999, 0.8002, -0.9888,
          0.2, 0.1056, 0.87865, -0.6999, 0.4111, 0.1902, -0.4888 };
    std::vector<std::string>    strvec2 =
        { "4% of something", "Description 4/5", "This is bad",
          "3.4% of GDP", "Market drops", "Market pulls back",
          "$15 increase", "Running fast", "C++14 development",
          "Some explanation", "More strings", "Bonds vs. Equities",
          "Almost done", "XXXX04",
          "XXXX2", "XXXX3", "XXXX4", "XXXX4", "XXXX5", "XXXX6",
          "XXXX7", "XXXX10", "XXXX11", "XXXX02", "XXXX03" };
    std::vector<bool>           boolvec =
        { true, true, true, false, false, true };

    MyDataFrame df;

    df.load_data(std::move(ulgvec2), std::make_pair("ul_col", xulgvec2));
    df.load_column("xint_col",
                   std::move(intvec2),
                   nan_policy::dont_pad_with_nans);
    df.load_column("str_col",
                   std::move(strvec2),
                   nan_policy::dont_pad_with_nans);
    df.load_column("dbl_col",
                   std::move(xdblvec2),
                   nan_policy::dont_pad_with_nans);
    df.load_column("dbl_col_2",
                   std::move(dblvec22),
                   nan_policy::dont_pad_with_nans);
    df.load_column("bool_col",
                   std::move(boolvec),
                   nan_policy::dont_pad_with_nans);

    df.write<std::ostream,
             int,
             unsigned long,
             double,
             bool,
             std::string>(std::cout, io_format::csv2);

    MyDataFrame df_read;

    try  {
        df_read.read("data/csv2_format_data.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
    df_read.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  bool,
                  std::string>(std::cout, io_format::csv2);
}

// -----------------------------------------------------------------------------

static void test_BoxCoxVisitor()  {

    std::cout << "\nTesting BoxCoxVisitor{ } ..." << std::endl;

    const size_t            item_cnt = 16;
    MyDataFrame             df;
    RandGenParams<double>   p;

    p.mean = 5.6;
    p.std = 0.5;
    p.seed = 123;
    p.min_value = -15;
    p.max_value = 30;

    df.load_data(MyDataFrame::gen_sequence_index(0, item_cnt, 1),
                 std::make_pair("lognormal",
                                gen_lognormal_dist<double>(item_cnt, p)),
                 std::make_pair("normal",
                                gen_normal_dist<double>(item_cnt, p)),
                 std::make_pair("uniform_real",
                                gen_uniform_real_dist<double>(item_cnt, p)));

    BoxCoxVisitor<double>   bc_v1(box_cox_type::original, 1.5, true);
    const auto              &result1 =
        df.single_act_visit<double>("lognormal", bc_v1).get_result();
    BoxCoxVisitor<double>   bc_v2(box_cox_type::original, 1.5, false);
    const auto              &result2 =
        df.single_act_visit<double>("uniform_real", bc_v2).get_result();
    BoxCoxVisitor<double>   bc_v3(box_cox_type::modulus, -0.5, false);
    const auto              &result3 =
        df.single_act_visit<double>("uniform_real", bc_v3).get_result();
    BoxCoxVisitor<double>   bc_v4(box_cox_type::exponential, -0.5, false);
    const auto              &result4 =
        df.single_act_visit<double>("uniform_real", bc_v4).get_result();

    for(auto citer : result1)
        std::cout << citer << ", ";
    std::cout << std::endl;
    for(auto citer : result2)
        std::cout << citer << ", ";
    std::cout << std::endl;
    for(auto citer : result3)
        std::cout << citer << ", ";
    std::cout << std::endl;
    for(auto citer : result4)
        std::cout << citer << ", ";
    std::cout << std::endl;
}

// -----------------------------------------------------------------------------

static void test_NormalizeVisitor()  {

    std::cout << "\nTesting NormalizeVisitor{ } ..." << std::endl;

    std::vector<unsigned long>  ulgvec2 =
        { 123450, 123451, 123452, 123450, 123455, 123450, 123449,
          123450, 123451, 123450, 123452, 123450, 123455, 123450,
          123454, 123450, 123450, 123457, 123458, 123459, 123450,
          123441, 123442, 123432, 123450, 123450, 123435, 123450 };
    std::vector<double>         dblvec =
        { 1.2345, 2.2345, 3.2345, 4.2345, 5.2345, 3.0, 0.9999,
          10.0, 4.25, 0.009, 8.0, 2.2222, 3.3333, 15.6,
          11.0, 5.25, 1.009, 2.111, 9.0, 3.2222, 4.3333,
          12.0, 6.25, 2.009, 3.111, 10.0, 4.2222, 5.3333 };

    MyDataFrame df;

    df.load_data(std::move(ulgvec2), std::make_pair("dbl_col", dblvec));

    NormalizeVisitor<double>    norm_v;
    StandardizeVisitor<double>  stand_v;
    auto                        result =
        df.single_act_visit<double>("dbl_col", norm_v).get_result();
    std::vector<double>         norm_result = {
        0.078603, 0.142743, 0.206882, 0.271022, 0.335161, 0.191841, 0.0635559,
        0.640818, 0.272016, 0, 0.512539, 0.141954, 0.213219, 1, 0.704958,
        0.336155, 0.0641396, 0.134821, 0.576679, 0.206093, 0.277359, 0.769098,
        0.400295, 0.128279, 0.198961, 0.640818, 0.270233, 0.341498,
    };
    std::vector<double>         stand_result = {
        -1.00542, -0.744444, -0.48347, -0.222497, 0.0384758, -0.544669,
        -1.06664, 1.28214, -0.218452, -1.32524, 0.760197, -0.747654, -0.457686,
        2.74359, 1.54312, 0.0425209, -1.06427, -0.776674, 1.02117, -0.48668,
        -0.196713, 1.80409, 0.303494, -0.803293, -0.515701, 1.28214, -0.225707,
        0.06426
    };

    for (size_t idx = 0; idx < result.size(); ++idx)
       assert(fabs(result[idx] - norm_result[idx]) < 0.00001);
    result = df.single_act_visit<double>("dbl_col", stand_v).get_result();
    for (size_t idx = 0; idx < result.size(); ++idx)
       assert(fabs(result[idx] - stand_result[idx]) < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_HampelFilterVisitor()  {

    std::cout << "\nTesting HampelFilterVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    std::vector<double>         d1 =
        { 2.5, 2.45, -1.65, -0.1, -1.1, 1.87, 0.98,
          0.34, 1.56, -12.34, 2.3, -0.34, -1.9, 0.387,
          0.123, 1.06, -0.65, 2.03, 0.4, -1.0, 0.59,
          0.125, 1.9, -0.68, 2.0045, 50.8, -1.0, 0.78,
          0.48, 1.99, -0.97, 1.03, 8.678, -1.4, 1.59,
        };
    MyDataFrame                 df;

    df.load_data(std::move(idx), std::make_pair("dbl_col", d1));

    HampelFilterVisitor<double> hf_v(7, hampel_type::mean, 2);
    auto                        result =
        df.single_act_visit<double>("dbl_col", hf_v).get_result();
    std::vector<double>         hampel_result = {
        2.5, 2.45, -1.65, -0.1, -1.1, 1.87, 0.98, 0.34, 1.56,
        std::numeric_limits<double>::quiet_NaN(), 2.3, -0.34, -1.9, 0.387,
        0.123, 1.06, -0.65, 2.03, 0.4, -1, 0.59, 0.125, 1.9, -0.68, 2.0045,
        std::numeric_limits<double>::quiet_NaN(), -1, 0.78, 0.48, 1.99,
        -0.97, 1.03, 8.678, -1.4, 1.59
    };
    const auto                  &column = df.get_column<double>("dbl_col");

    assert(result == 2);
    for (size_t idx = 0; idx < hampel_result.size(); ++idx)  {
        const auto  v = column[idx];

        if (std::isnan(v))
            assert(std::isnan(hampel_result[idx]));
        else
            assert(hampel_result[idx] == v);
    }
}

// -----------------------------------------------------------------------------

static void test_PolyFitVisitor()  {

    std::cout << "\nTesting PolyFitVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    MyDataFrame                 df;

    df.load_index(std::move(idx));
    df.load_column<double>("X1",
                           { 1, 2, 3, 4, 5 },
                           nan_policy::dont_pad_with_nans);
    df.load_column<double>("Y1",
                           { 6, 7, 8, 9, 3 },
                           nan_policy::dont_pad_with_nans);
    df.load_column<double>("X2",
                           { 0.0, 1.0, 2.0, 3.0,  4.0,  5.0 },
                           nan_policy::dont_pad_with_nans);
    df.load_column<double>("Y2",
                           { 0.0, 0.8, 0.9, 0.1, -0.8, -1.0 },
                           nan_policy::dont_pad_with_nans);

    PolyFitVisitor<double>  poly_v1 (2);
    PolyFitVisitor<double>  poly_v12 (
        2,
        [](const unsigned int &, std::size_t idx) -> double {
            const std::array<double, 5> weights = { 0.1, 0.8, 0.3, 0.5, 0.2 };

            return (weights[idx]);
        });
    auto                    result1 =
        df.single_act_visit<double, double>("X1", "Y1", poly_v1).get_result();
    auto                    result12 =
        df.single_act_visit<double, double>("X1", "Y1", poly_v12).get_result();
    auto                    actual1 = std::vector<double> { 0.8, 5.6, -1 };
    auto                    actual12 =
        std::vector<double> { -1.97994, 6.99713, -1.14327 };

    assert(std::fabs(poly_v1.get_residual() - 5.6) < 0.00001);
    for (size_t idx = 0; idx < result1.size(); ++idx)
       assert(fabs(result1[idx] - actual1[idx]) < 0.00001);

    assert(std::fabs(poly_v12.get_residual() - 0.70981) < 0.00001);
    for (size_t idx = 0; idx < result12.size(); ++idx)
       assert(fabs(result12[idx] - actual12[idx]) < 0.00001);

    PolyFitVisitor<double>  poly_v2 (3);
    auto                    result2 =
        df.single_act_visit<double, double>("X2", "Y2", poly_v2).get_result();
    auto                    actual2 =
        std::vector<double> { -0.0396825, 1.69312, -0.813492, 0.087037 };

    for (size_t idx = 0; idx < result2.size(); ++idx)
       assert(fabs(result2[idx] - actual2[idx]) < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_HurstExponentVisitor()  {

    std::cout << "\nTesting HurstExponentVisitor{ } ..." << std::endl;

    RandGenParams<double>   p;

    p.seed = 123;
    p.min_value = 0;
    p.max_value = 30;

    std::vector<double> d1 = gen_uniform_real_dist<double>(1024, p);
    std::vector<double> d2 =
        { 0.04, 0.02, 0.05, 0.08, 0.02, -0.17, 0.05, 0.0 };
    std::vector<double> d3 =
        { 0.04, 0.05, 0.055, 0.06, 0.061, 0.072, 0.073, 0.8 };

    MyDataFrame df;

    df.load_index(MyDataFrame::gen_sequence_index(0, 1024, 1));
    df.load_column("d1_col", std::move(d1), nan_policy::dont_pad_with_nans);
    df.load_column("d2_col", std::move(d2), nan_policy::dont_pad_with_nans);
    df.load_column("d3_col", std::move(d3), nan_policy::dont_pad_with_nans);

    HurstExponentVisitor<double>    he_v1 ({ 1, 2, 4 });
    auto                            result1 =
        df.single_act_visit<double>("d2_col", he_v1).get_result();

    assert(result1 - 0.865926 < 0.00001);

    HurstExponentVisitor<double>    he_v2 ({ 1, 2, 4, 5, 6, 7 });
    auto                            result2 =
        df.single_act_visit<double>("d1_col", he_v2).get_result();

    assert(result2 - 0.487977 < 0.00001);

    HurstExponentVisitor<double>    he_v3 ({ 1, 2, 4 });
    auto                            result3 =
        df.single_act_visit<double>("d3_col", he_v3, true).get_result();

    assert(result3 - 0.903057 < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_LogFitVisitor()  {

    std::cout << "\nTesting LogFitVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    MyDataFrame                 df;

    df.load_index(std::move(idx));
    df.load_column<double>("X1",
                           { 1, 2, 3, 4, 5 },
                           nan_policy::dont_pad_with_nans);
    df.load_column<double>("Y1",
                           { 6, 7, 8, 9, 3 },
                           nan_policy::dont_pad_with_nans);
    df.load_column<double>("X2",
                           { 1, 2, 4, 6, 8 },
                           nan_policy::dont_pad_with_nans);
    df.load_column<double>("Y2",
                           { 1, 3, 4, 5, 6 },
                           nan_policy::dont_pad_with_nans);

    LogFitVisitor<double>   log_v1;
    auto                    result1 =
        df.single_act_visit<double, double>("X1", "Y1", log_v1).get_result();
    auto                    actual1 =
        std::vector<double> { 6.98618, -0.403317 };

    assert(std::fabs(log_v1.get_residual() - 20.9372) < 0.0001);
    for (size_t idx = 0; idx < result1.size(); ++idx)
       assert(fabs(result1[idx] - actual1[idx]) < 0.00001);

    LogFitVisitor<double>   log_v2;
    auto                    result2 =
        df.single_act_visit<double, double>("X2", "Y2", log_v2).get_result();
    auto                    actual2 = std::vector<double> { 1.11199, 2.25859 };

    assert(std::fabs(log_v2.get_residual() - 0.237476) < 0.00001);
    for (size_t idx = 0; idx < result2.size(); ++idx)
       assert(fabs(result2[idx] - actual2[idx]) < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_ExpoSmootherVisitor()  {

    std::cout << "\nTesting ExpoSmootherVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    std::vector<double>         d1 =
        { 2.5, 2.45, -1.65, -0.1, -1.1, 1.87, 0.98,
          0.34, 1.56, -12.34, 2.3, -0.34, -1.9, 0.387,
          0.123, 1.06, -0.65, 2.03, 0.4, -1.0, 0.59,
          0.125, 1.9, -0.68, 2.0045, 50.8, -1.0, 0.78,
          0.48, 1.99, -0.97, 1.03, 8.678, -1.4, 1.59,
        };
    std::vector<double>         d1_copy = d1;
    MyDataFrame                 df;

    df.load_data(std::move(idx), std::make_pair("dbl_col", d1));

    MyDataFrame df2 = df;

    ExpoSmootherVisitor<double> es_v1(1);

    df.single_act_visit<double>("dbl_col", es_v1);

    const auto  &col1 = df.get_column<double>("dbl_col");

    for (size_t idx = 0; idx < col1.size(); ++idx)
       assert(fabs(col1[idx] - d1_copy[idx]) < 0.00001);

    ExpoSmootherVisitor<double> es_v2(0.3);

    df.single_act_visit<double>("dbl_col", es_v2);

    auto    actual2 = std::vector<double> {
        2.5, 2.485, 1.22, -1.185, -0.4, -0.209, 1.603,
        0.788, 0.706, -2.61, -7.948, 1.508, -0.808, -1.2139,
        0.3078, 0.4041, 0.547, 0.154, 1.541, -0.02, -0.523,
        0.4505, 0.6575, 1.126, 0.12535, 16.6431, 35.26, -0.466,
        0.69, 0.933, 1.102, -0.37, 3.3244, 5.6546, -0.503
    };

    for (size_t idx = 0; idx < col1.size(); ++idx)
       assert(fabs(col1[idx] - actual2[idx]) < 0.0001);

    df.get_column<double>("dbl_col") = d1_copy;

    ExpoSmootherVisitor<double> es_v3(0.8);

    df.single_act_visit<double>("dbl_col", es_v3);

    auto    actual3 = std::vector<double> {
        2.5, 2.46, -0.83, -0.41, -0.9, 1.276, 1.158,
        0.468, 1.316, -9.56, -0.628, 0.188, -1.588, -0.0704,
        0.1758, 0.8726, -0.308, 1.494, 0.726, -0.72, 0.272,
        0.218, 1.545, -0.164, 1.4676, 41.0409, 9.36, 0.424,
        0.54, 1.688, -0.378, 0.63, 7.1484, 0.6156, 0.992
    };

    for (size_t idx = 0; idx < col1.size(); ++idx)
       assert(fabs(col1[idx] - actual3[idx]) < 0.0001);

    ExpoSmootherVisitor<double> es_v3_4 (0.8, 4);
    const auto                  &col21 = df2.get_column<double>("dbl_col");

    df2.single_act_visit<double>("dbl_col", es_v3_4);

    auto    actual4 = std::vector<double> {
        2.5, 2.47952, 0.77968, -0.27248, -0.67824, 0.261712, 0.9932, 0.799584,
        0.97488, -4.33518, -3.8625, -1.05213, -0.877632, -0.632813, -0.087968,
        0.494816, 0.193696, 0.731832, 0.922821, 0.051104, -0.055568, 0.152752,
        0.895104, 0.532416, 0.838499, 21.5731, 20.6916, 7.763, 1.66618,
        1.1872, 0.509888, 0.343776, 3.87912, 3.11763, 1.43558
    };

    for (size_t idx = 0; idx < col21.size(); ++idx)
       assert(fabs(col21[idx] - actual4[idx]) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_HWExpoSmootherVisitor()  {

    std::cout << "\nTesting HWExpoSmootherVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    std::vector<double>         d1 =
        { 2.5, 2.45, -1.65, -0.1, -1.1, 1.87, 0.98,
          0.34, 1.56, -12.34, 2.3, -0.34, -1.9, 0.387,
          0.123, 1.06, -0.65, 2.03, 0.4, -1.0, 0.59,
          0.125, 1.9, -0.68, 2.0045, 50.8, -1.0, 0.78,
          0.48, 1.99, -0.97, 1.03, 8.678, -1.4, 1.59,
        };
    std::vector<double>         d1_copy = d1;
    MyDataFrame                 df;

    df.load_data(std::move(idx), std::make_pair("dbl_col", d1));

    HWExpoSmootherVisitor<double>   es_v1(1, 1);

    df.single_act_visit<double>("dbl_col", es_v1);

    const auto  &col1 = df.get_column<double>("dbl_col");

    for (size_t idx = 0; idx < col1.size(); ++idx)
       assert(fabs(col1[idx] - d1_copy[idx]) < 0.00001);

    HWExpoSmootherVisitor<double>   es_v2(0.3, 0.4);

    df.single_act_visit<double>("dbl_col", es_v2);

    auto    actual2 = std::vector<double> {
        2.5, 2.45, 1.185, -2.354, -0.6674, -0.64944, 2.17034,
        0.879202, 0.581521, -2.34309, -11.6799, 3.36809, -0.431147, -1.42459,
        0.821747, 0.638548, 0.950029, -0.0829826, 2.14921, -0.111474, -0.969884,
        0.627569, 0.633542, 1.60863, -0.307475, 17.1351, 49.2179, -6.59525,
        -2.48915, -1.05849, 0.329906, -1.66206, 3.10917, 7.6669, -2.11746,
    };

    for (size_t idx = 0; idx < col1.size(); ++idx)
       assert(fabs(col1[idx] - actual2[idx]) < 0.0001);

    df.get_column<double>("dbl_col") = d1_copy;

    HWExpoSmootherVisitor<double>   es_v3(0.8, 0.8);

    df.single_act_visit<double>("dbl_col", es_v3);

    auto    actual3 = std::vector<double> {
        2.5, 2.45, -0.84, -1.068, -0.7836, 1.13928, 1.60586,
        0.415171, 1.20303, -9.38739, -2.81748, 2.0925, -1.6295, -0.3283,
        0.49014, 0.893228, -0.153954, 1.25121, 1.10624, -0.904752, 0.0110497,
        0.42021, 1.51104, 0.113208, 1.11024, 41.3989, 17.2389, -6.28822,
        -0.517644, 1.42847, -0.188306, 0.194339, 7.38127, 1.88585, -0.366429
    };

    for (size_t idx = 0; idx < col1.size(); ++idx)
       assert(fabs(col1[idx] - actual3[idx]) < 0.0001);
}

// -----------------------------------------------------------------------------

static std::vector<std::string>
add_columns(MyDataFrame::IndexVecType::const_iterator idx_begin,
            MyDataFrame::IndexVecType::const_iterator idx_end,
            std::vector<double>::const_iterator b_citer1,
            std::vector<double>::const_iterator e_citer1,
            std::vector<double>::const_iterator b_citer2,
            std::vector<double>::const_iterator e_citer2,
            std::vector<std::string>::const_iterator b_citer3,
            std::vector<std::string>::const_iterator e_citer3)  {

    const std::size_t           col_s =
        std::min ({ std::distance(b_citer1, e_citer1),
                    std::distance(b_citer2, e_citer2),
                    std::distance(b_citer3, e_citer3) });
    std::vector<std::string>    result (col_s);

    for (std::size_t i = 0; i < col_s; ++i)
        result[i] =
            *(b_citer3 + i) + std::to_string(*(b_citer1 + i) + *(b_citer2 + i));
    return (result);
}

// --------------------------------------

static void test_consolidate()  {

    std::cout << "\nTesting consolidate( ) ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec = {
        1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL,
        10UL, 13UL, 10UL, 15UL, 14UL
    };
    std::vector<double>         dblvec = {
        0.0, 15.0, 14.0, 2.0, 1.0, 12.0, 11.0, 8.0, 7.0, 6.0,
        5.0, 4.0, 3.0, 9.0, 10.0
    };
    std::vector<double>         dblvec2 = {
        100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.55, 107.34, 1.8, 111.0,
        112.0, 113.0, 114.0, 115.0, 116.0
    };
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec = {
        "zz", "bb", "cc", "ww", "ee", "ff", "gg", "hh", "ii", "jj",
        "kk", "ll", "mm", "nn", "oo"
    };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("dbl_col_2", dblvec2),
                 std::make_pair("str_col", strvec));
    df.load_column("int_col", std::move(intvec),
                   nan_policy::dont_pad_with_nans);

    df.consolidate<double, double, std::string, std::string>
        ("dbl_col", "dbl_col_2", "str_col", "new_str_col", add_columns, true);
    assert(! df.has_column("dbl_col"));
    assert(! df.has_column("dbl_col_2"));
    assert(! df.has_column("str_col"));
    assert(df.has_column("new_str_col"));

    const auto                     &new_str_col =
        df.get_column<std::string>("new_str_col");
    const std::vector<const char *> actual = {
        "zz100.000000", "bb116.000000", "cc116.000000", "ww105.000000",
        "ee105.000000", "ff117.000000", "gg117.550000", "hh115.340000",
        "ii8.800000", "jj117.000000", "kk117.000000", "ll117.000000",
        "mm117.000000", "nn124.000000", "oo126.000000"
    };

    for (size_t idx = 0; idx < actual.size(); ++idx)
       assert(new_str_col[idx] == actual[idx]);
}

// -----------------------------------------------------------------------------

static void test_ExtremumSubArrayVisitor()  {

    std::cout << "\nTesting ExtremumSubArrayVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    MyDataFrame                 df;

    df.load_index(std::move(idx));
    df.load_column<double>("dbl_col1",
                           { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
                           nan_policy::dont_pad_with_nans);
    df.load_column<double>("dbl_col2",
                           { -3, 1, -8, 4, -1, 2, 1, -5, 5 },
                           nan_policy::dont_pad_with_nans);

    ExtremumSubArrayVisitor<double> msa_v;

    df.visit<double>("dbl_col1", msa_v);
    assert(msa_v.get_result() == 55);
    assert(msa_v.get_begin_idx() == 0);
    assert(msa_v.get_end_idx() == 10);

    df.visit<double>("dbl_col2", msa_v);
    assert(msa_v.get_result() == 6);
    assert(msa_v.get_begin_idx() == 3);
    assert(msa_v.get_end_idx() == 7);
}

// -----------------------------------------------------------------------------

static void test_NExtremumSubArrayVisitor()  {

    std::cout << "\nTesting NExtremumSubArrayVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    MyDataFrame                 df;

    df.load_index(std::move(idx));
    df.load_column<double>("dbl_col1",
                           { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
                           nan_policy::dont_pad_with_nans);
    df.load_column<double>("dbl_col2",
                           { -3, 1, -8, 4, -1, 2, 1, -5, 5 },
                           nan_policy::dont_pad_with_nans);

    NExtremumSubArrayVisitor<5, double> msa_v;

    df.visit<double>("dbl_col1", msa_v);
    assert((msa_v.get_result()[0].sum == 21));
    assert((msa_v.get_result()[0].begin_index == 0));
    assert((msa_v.get_result()[0].end_index == 6));
    assert((msa_v.get_result()[1].sum == 28));
    assert((msa_v.get_result()[1].begin_index == 0));
    assert((msa_v.get_result()[1].end_index == 7));
    assert((msa_v.get_result()[2].sum == 36));
    assert((msa_v.get_result()[2].begin_index == 0));
    assert((msa_v.get_result()[2].end_index == 8));
    assert((msa_v.get_result()[3].sum == 45));
    assert((msa_v.get_result()[3].begin_index == 0));
    assert((msa_v.get_result()[3].end_index == 9));
    assert((msa_v.get_result()[4].sum == 55));
    assert((msa_v.get_result()[4].begin_index == 0));
    assert((msa_v.get_result()[4].end_index == 10));

    NExtremumSubArrayVisitor<4, double> msa_v2;

    df.visit<double>("dbl_col2", msa_v2);
    assert((msa_v2.get_result()[0].sum == 1));
    assert((msa_v2.get_result()[0].begin_index == 1));
    assert((msa_v2.get_result()[0].end_index == 2));
    assert((msa_v2.get_result()[1].sum == 4));
    assert((msa_v2.get_result()[1].begin_index == 3));
    assert((msa_v2.get_result()[1].end_index == 4));
    assert((msa_v2.get_result()[2].sum == 5));
    assert((msa_v2.get_result()[2].begin_index == 3));
    assert((msa_v2.get_result()[2].end_index == 6));
    assert((msa_v2.get_result()[3].sum == 6));
    assert((msa_v2.get_result()[3].begin_index == 3));
    assert((msa_v2.get_result()[3].end_index == 7));

    NMinSubArrayVisitor<5, double, unsigned long>   msa_v3;

    df.visit<double>("dbl_col1", msa_v3);
    assert((msa_v3.get_result()[0].sum == 1));
    assert((msa_v3.get_result()[0].begin_index == 0));
    assert((msa_v3.get_result()[0].end_index == 1));

    df.visit<double>("dbl_col2", msa_v3);
    assert((msa_v3.get_result()[0].sum == -3));
    assert((msa_v3.get_result()[0].begin_index == 0));
    assert((msa_v3.get_result()[0].end_index == 1));
    assert((msa_v3.get_result()[1].sum == -10));
    assert((msa_v3.get_result()[1].begin_index == 0));
    assert((msa_v3.get_result()[1].end_index == 3));
}

// -----------------------------------------------------------------------------

static void test_LowessVisitor()  {

    std::cout << "\nTesting LowessVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    std::vector<double>         x_vec = {
        0.5578196, 2.0217271, 2.5773252, 3.4140288, 4.3014084, 4.7448394,
        5.1073781, 6.5411662, 6.7216176, 7.2600583, 8.1335874, 9.1224379,
        1.9296663, 2.3797674, 3.2728619, 4.2767453, 5.3731026, 5.6476637,
        8.5605355, 8.5866354, 8.7572812,
    };
    std::vector<double>         y_vec = {
        18.63654, 103.49646, 150.35391, 190.51031, 208.70115, 213.71135,
        228.49353, 233.55387, 234.55054, 223.89225, 227.68339, 223.91982,
        168.01999, 164.95750, 152.61107, 160.78742, 168.55567, 152.42658,
        221.70702, 222.69040, 243.18828,
    };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("indep_var", x_vec),
                 std::make_pair("dep_var", y_vec));

    LowessVisitor<double>   l_v;

    df.single_act_visit<double, double>("dep_var", "indep_var", l_v);

    auto    actual_yfit = std::vector<double> {
        68.1432, 119.432, 122.75, 135.633, 142.724, 165.905, 169.447, 185.617,
        186.017, 191.865, 198.03, 202.234, 206.178, 215.053, 216.586, 220.408,
        226.671, 229.052, 229.185, 230.023, 231.657,
    };

    for (size_t idx = 0; idx < actual_yfit.size(); ++idx)
        assert(fabs(l_v.get_result()[idx] - actual_yfit[idx]) < 0.001);

    auto    actual_weights = std::vector<double> {
        0.641773, 0.653544, 0.940738, 0.865302, 0.990575, 0.971522, 0.92929,
        0.902444, 0.918228, 0.924041, 0.855054, 0.824388, 0.586045, 0.945216,
        0.94831, 0.998031, 0.999834, 0.991263, 0.993165, 0.972067, 0.990308,
    };

    for (size_t idx = 0; idx < actual_weights.size(); ++idx)
        assert(fabs(l_v.get_residual_weights()[idx] -
                    actual_weights[idx]) < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_StepRollAdopter()  {

    std::cout << "\nTesting StepRollAdopter{ } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123463,
          123464, 123458, 123459, 123460, 123461, 123462
        };
    std::vector<double> d1 =
        { 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5 };
    MyDataFrame         df;

    df.load_data(std::move(idx), std::make_pair("col_1", d1));

    StepRollAdopter<MeanVisitor<double>, double>  mean_roller(
        MeanVisitor<double>(), 5);
    auto                                          result =
        df.single_act_visit<double>("col_1", mean_roller).get_result();

    assert(result == 1.0);

    StepRollAdopter<MeanVisitor<double>, double>  mean_roller2(
        MeanVisitor<double>(), 3);

    result = df.single_act_visit<double>("col_1", mean_roller2).get_result();
    assert(result == 2.857142857142857);
}

// -----------------------------------------------------------------------------

static void test_DecomposeVisitor()  {

    std::cout << "\nTesting DecomposeVisitor{  } ..." << std::endl;

    std::vector<double>         y_vec =
        { 131.157, 131.367, 132.215, 132.725, 132.648, 130.585, 130.701,
          129.631, 129.168, 129.554, 129.467, 129.670, 128.397, 129.014,
          129.496, 131.067, 130.219, 128.947, 129.602, 128.118, 127.356,
          127.231, 127.154, 128.417, 129.091, 129.082, 128.937, 130.441,
          129.371, 129.294, 129.381, 129.564, 129.708, 130.701, 130.663,
          130.113, 130.046, 130.393, 128.026, 129.204, 130.530, 129.499,
          129.266, 129.357, 130.431, 131.810, 131.761, 131.675, 130.923,
          131.694, 133.005, 133.323, 134.152, 138.702, 137.719, 135.492,
          133.622, 134.518, 132.725, 131.839, 138.548, 140.996, 143.734,
          150.693, 151.108, 149.423, 150.416, 149.491, 151.273, 150.299,
          146.783, 147.173, 146.939, 147.290, 145.946, 142.624, 138.027,
          136.118, 129.650, 126.767, 130.809, 125.550, 130.732, 126.183,
          124.410, 114.748, 121.527, 114.904, 100.138, 105.144,
        };
    MyDataFrame                 df;

    df.load_data(MyDataFrame::gen_sequence_index(0, y_vec.size(), 1),
                 std::make_pair("IBM_closes", y_vec));

    DecomposeVisitor<double>    d_v (7, 0.6, 0.01);

    df.single_act_visit<double>("IBM_closes", d_v);

    auto    actual_trends = std::vector<double>
        { 130.613, 130.55, 130.489, 130.43, 130.372, 130.317, 130.263, 130.211,
          130.161, 130.111, 130.064, 130.017, 129.972, 129.928, 129.885,
          129.842, 129.801, 129.76, 129.72, 129.681, 129.642, 129.603, 129.564,
          129.526, 129.49, 129.458, 129.435, 129.459, 129.496, 129.546, 129.61,
          129.688, 129.78, 129.885, 130.002, 130.129, 130.267, 130.414,
          130.568, 130.73, 130.898, 131.071, 131.248, 131.429, 131.613,
          131.801, 131.994, 132.193, 132.4, 132.618, 132.847, 133.089, 133.343,
          133.594, 133.814, 133.958, 133.982, 133.867, 133.633, 133.329,
          133.013, 132.715, 132.449, 132.214, 131.88, 131.594, 131.336,
          131.094, 130.862, 130.636, 130.412, 130.191, 129.97, 129.749,
          129.527, 129.305, 129.082, 128.858, 128.633, 128.406, 128.178,
          127.949, 127.717, 127.484, 127.249, 127.012, 126.773, 126.531,
          126.288, 126.043
        };

    for (size_t idx = 0; idx < actual_trends.size(); ++idx)
        assert(fabs(d_v.get_trend()[idx] - actual_trends[idx]) < 0.001);

    auto    actual_seasonals = std::vector<double>
        { 0.499135, -0.362488, -0.0226401, -0.138991, -0.774313, -0.152695,
          0.951993, 0.499135, -0.362488, -0.0226401, -0.138991, -0.774313,
          -0.152695, 0.951993, 0.499135, -0.362488, -0.0226401, -0.138991,
          -0.774313, -0.152695, 0.951993, 0.499135, -0.362488, -0.0226401,
          -0.138991, -0.774313, -0.152695, 0.951993, 0.499135, -0.362488,
          -0.0226401, -0.138991, -0.774313, -0.152695, 0.951993, 0.499135,
          -0.362488, -0.0226401, -0.138991, -0.774313, -0.152695, 0.951993,
          0.499135, -0.362488, -0.0226401, -0.138991, -0.774313, -0.152695,
          0.951993, 0.499135, -0.362488, -0.0226401, -0.138991, -0.774313,
          -0.152695, 0.951993, 0.499135, -0.362488, -0.0226401, -0.138991,
          -0.774313, -0.152695, 0.951993, 0.499135, -0.362488, -0.0226401,
          -0.138991, -0.774313, -0.152695, 0.951993, 0.499135, -0.362488,
          -0.0226401, -0.138991, -0.774313, -0.152695, 0.951993, 0.499135,
          -0.362488, -0.0226401, -0.138991, -0.774313, -0.152695, 0.951993,
          0.499135, -0.362488, -0.0226401, -0.138991, -0.774313, -0.152695
        };

    for (size_t idx = 0; idx < actual_seasonals.size(); ++idx)
        assert(fabs(d_v.get_seasonal()[idx] - actual_seasonals[idx]) < 0.00001);

    auto    actual_residuals = std::vector<double>
        { 0.0450645, 1.17948, 1.74866, 2.43421, 3.04989, 0.420796, -0.514129,
          -1.07918, -0.630027, -0.534809, -0.457752, 0.427013, -1.42233,
          -1.86582, -0.887751, 1.58716, 0.440736, -0.674248, 0.656095,
          -1.41002, -3.2376, -2.87093, -2.04782, -1.08684, -0.260342, 0.397977,
          -0.345524, 0.0298508, -0.623855, 0.110697, -0.206247, 0.014974,
          0.702412, 0.968872, -0.290624, -0.515527, 0.141332, 0.0017837,
          -2.40348, -0.751754, -0.215182, -2.52399, -2.48155, -1.7098,
          -1.15976, 0.147774, 0.541487, -0.36517, -2.4292, -1.42281, 0.520609,
          0.256637, 0.94848, 5.88205, 4.05781, 0.58219, -0.858726, 1.01365,
          -0.88524, -1.35148, 6.30971, 8.4335, 10.3326, 17.98, 19.5905,
          17.8516, 19.2189, 19.171, 20.5634, 18.7112, 15.8714, 17.3448,
          16.992, 17.6804, 17.1932, 13.4718, 7.99313, 6.76106, 1.3799,
          -1.61643, 2.7699, -1.62422, 3.16745, -2.25312, -3.33817, -11.9014,
          -5.22295, -11.4883, -25.3757, -20.7463
        };

    for (size_t idx = 0; idx < actual_residuals.size(); ++idx)
        assert(fabs(d_v.get_residual()[idx] - actual_residuals[idx]) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_DT_IBM_data()  {

    std::cout << "\nTesting DT_IBM_data(  ) ..." << std::endl;

    typedef StdDataFrame<DateTime>  DT_DataFrame;

    DT_DataFrame    df;

    try  {
        df.read("data/DT_IBM.csv", io_format::csv2);

        assert(df.get_column<double>("IBM_Open")[0] == 98.4375);
        assert(df.get_column<double>("IBM_Close")[18] == 97.875);
        assert(df.get_index()[18] == DateTime(20001128));
        assert(fabs(df.get_column<double>("IBM_High")[5030] - 111.8) < 0.001);
        assert(df.get_column<long>("IBM_Volume")[5022] == 21501100L);
        assert(df.get_index()[5020] == DateTime(20201016));
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// --------------------------------------------------------------------

static void test_TTestVisitor()  {

    std::cout << "\nTesting TTestVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466,
          123467, 123468, 123469, 123470, 123471, 123472, 123473,
        };
    std::vector<double>         x_vec = {
        0.5578196, 2.0217271, 2.5773252, 3.4140288, 4.3014084, 4.7448394,
        5.1073781, 6.5411662, 6.7216176, 7.2600583, 8.1335874, 9.1224379,
        1.9296663, 2.3797674, 3.2728619, 4.2767453, 5.3731026, 5.6476637,
        8.5605355, 8.5866354, 8.7572812,
    };
    std::vector<double>         y_vec = {
        18.63654, 103.49646, 150.35391, 190.51031, 208.70115, 213.71135,
        228.49353, 233.55387, 234.55054, 223.89225, 227.68339, 223.91982,
        168.01999, 164.95750, 152.61107, 160.78742, 168.55567, 152.42658,
        221.70702, 222.69040, 243.18828,
    };
    std::vector<double>         z_vec = {
        0.5578296, 2.0217275, 2.5773252, 3.4140288, 4.3084084, 4.7448394,
        5.1079781, 6.5411662, 6.1216176, 7.1600583, 8.1335174, 9.1223379,
        1.9296663, 2.3727674, 3.2728619, 4.2767953, 5.3731056, 5.6426637,
        8.5602355, 8.5866354, 8.7572819,
    };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("x_data", x_vec),
                 std::make_pair("y_data", y_vec),
                 std::make_pair("z_data", z_vec));

    TTestVisitor<double>    tt_v (false);

    df.visit<double, double>("x_data", "y_data", tt_v);
    assert(fabs(tt_v.get_result() - -15.3585) < 0.0001);
    assert(tt_v.get_deg_freedom() == 40);

    df.visit<double, double>("x_data", "z_data", tt_v);
    assert(fabs(tt_v.get_result() - 0.0421697) < 0.000001);
    assert(tt_v.get_deg_freedom() == 40);

    TTestVisitor<double>    tt_v2 (true);

    df.visit<double, double>("x_data", "y_data", tt_v2);
    assert(fabs(tt_v2.get_result() - -15.9748) < 0.0001);
    assert(tt_v2.get_deg_freedom() == 20);

    df.visit<double, double>("x_data", "z_data", tt_v2);
    assert(fabs(tt_v2.get_result() - 1.16854) < 0.00001);
    assert(tt_v2.get_deg_freedom() == 20);
}

// -----------------------------------------------------------------------------

static void test_MassIndexVisitor()  {

    std::cout << "\nTesting MassIndexVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx = {
        123450, 123451, 123452, 123453, 123454, 123455, 123456, 123457,
        123458, 123459, 123460, 123461, 123462, 123466, 123467, 123468,
        123469, 123470, 123471, 123472, 123473,
    };
    std::vector<double>         high = {
        121.75, 122.75, 124.83, 124.39, 135.5, 132, 128.25, 127.15, 126.94,
        125.22, 126.43, 127.35, 120.15, 117.69, 116.06, 116.62, 114.9, 112.22,
        109.73, 109.64, 111.8,
    };
    std::vector<double>         low = {
        118.82, 121.05, 121.59, 122.32, 129.77, 127.6, 126.44, 124.46, 125.13,
        123.85, 124.66, 125.08, 116.84, 117.69, 112.98, 115.53, 111.84, 110.03,
        105.92, 106.55, 107.75,
    };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("high", high),
                 std::make_pair("low", low));

    MassIndexVisitor<double>    mi_v (3, 5);

    // The values here are nonsensical, because the time-series and periods
    // are too short
    //
    df.single_act_visit<double, double>("high", "low", mi_v);

    assert(mi_v.get_result().size() == 21);
    assert(std::isnan(mi_v.get_result()[0]));
    assert(std::isnan(mi_v.get_result()[3]));
    assert(fabs(mi_v.get_result()[6] - 5.21367) < 0.00001);
    assert(fabs(mi_v.get_result()[10] - 4.3473) < 0.0001);
    assert(fabs(mi_v.get_result()[20] - 5.51902) < 0.00001);
    assert(fabs(mi_v.get_result()[17] - 4.87194) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_HullRollingMeanVisitor()  {

    std::cout << "\nTesting HullRollingMeanVisitor{ } ..." << std::endl;

    std::vector<unsigned long>  ulgvec2 =
        { 123450, 123451, 123452, 123450, 123455, 123450, 123449,
          123450, 123451, 123450, 123452, 123450, 123455, 123450,
          123454, 123450, 123450, 123457, 123458, 123459, 123450,
          123441, 123442, 123432, 123450, 123450, 123435, 123450,
          123441, 123442, 123432, 123450, 123450, 123435, 123450,
          123441, 123442, 123432, 123450, 123450, 123435, 123450 };
    std::vector<double>         dbl_vec =
        { 1, 2, 3, 4, 5, 3, 7, 3, 9, 10, 3, 2, 3, 14,
          2, 2, 2, 3, 2, 3, 3, 3, 3, 3, 36, 2, 45, 2,
          2, 3, 5, 6, 7, 7, 8, 1, 10, 11, 9, 8, 7, 6 };
    MyDataFrame                 df;

    df.load_data(std::move(ulgvec2), std::make_pair("col_1", dbl_vec));

    HullRollingMeanVisitor<double>  hull_roller(6);
    const auto                      &result =
        df.single_act_visit<double>("col_1", hull_roller).get_result();

    assert(result.size() == 42);
    assert(std::isnan(result[0]));
    assert(std::isnan(result[1]));
    assert(std::isnan(result[4]));
    assert(abs(result[5] - 4.19048) < 0.00001);
    assert(abs(result[6] - 5.42857) < 0.00001);
    assert(abs(result[10] - 7.61905) < 0.00001);
    assert(abs(result[20] - 2.95238) < 0.00001);
    assert(abs(result[41] - 6.8254) < 0.0001);
    assert(abs(result[40] - 7.84127) < 0.00001);
    assert(abs(result[39] - 9.93651) < 0.00001);
    assert(abs(result[38] - 10.9365) < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_RollingMidValueVisitor()  {

    std::cout << "\nTesting RollingMidValueVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456, 123457,
          123458, 123459, 123460, 123461, 123462, 123466, 123467, 123468,
          123469, 123470, 123471, 123472, 123473,
        };
    std::vector<double>         high = {
        121, 122, 124, 124.5, 135.5, 132, 128, 127, 126,
        125, 126.5, 127, 120, 117, 116, 116.5, 114, 112,
        109, 109.5, 111,
    };
    std::vector<double>         low = {
        118, 121, 121.5, 122, 129, 127, 126, 124, 125,
        123, 124, 125, 116, 114, 112, 115, 111, 110,
        105, 106, 107,
    };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("high", high),
                 std::make_pair("low", low));

    RollingMidValueVisitor<double>  rmv_v (5);

    df.single_act_visit<double, double>("low", "high", rmv_v);
    assert(rmv_v.get_result().size() == 21);
    assert(std::isnan(rmv_v.get_result()[0]));
    assert(std::isnan(rmv_v.get_result()[1]));
    assert(std::isnan(rmv_v.get_result()[3]));
    assert(rmv_v.get_result()[4] == 126.75);
    assert(rmv_v.get_result()[5] == 128.25);
    assert(rmv_v.get_result()[10] == 125.5);
    assert(rmv_v.get_result()[11] == 125);
    assert(rmv_v.get_result()[20] == 109.5);
    assert(rmv_v.get_result()[19] == 110.75);
}

// -----------------------------------------------------------------------------

static void test_DrawdownVisitor()  {

    std::cout << "\nTesting DrawdownVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456, 123457,
          123458, 123459, 123460, 123461, 123462, 123466, 123467, 123468,
          123469, 123470, 123471, 123472, 123473,
        };
    std::vector<double>         close = {
        121, 122, 124, 124.5, 135.5, 132, 128, 127, 126,
        125, 126.5, 127, 120, 135.6, 116, 116.5, 114, 112,
        109, 136, 111,
    };
    MyDataFrame                 df;

    df.load_data(std::move(idx), std::make_pair("close", close));

    DrawdownVisitor<double> dd_v;

    df.single_act_visit<double>("close", dd_v);
    assert(dd_v.get_result().size() == 21);
    assert(dd_v.get_log_drawdown().size() == 21);
    assert(dd_v.get_pct_drawdown().size() == 21);
    assert(dd_v.get_result()[0] == 0);
    assert(dd_v.get_log_drawdown()[0] == 0);
    assert(dd_v.get_pct_drawdown()[0] == 0);
    assert(dd_v.get_result()[3] == 0);
    assert(dd_v.get_log_drawdown()[3] == 0);
    assert(dd_v.get_pct_drawdown()[3] == 0);
    assert(dd_v.get_result()[13] == 0);
    assert(dd_v.get_log_drawdown()[13] == 0);
    assert(dd_v.get_pct_drawdown()[13] == 0);
    assert(dd_v.get_result()[19] == 0);
    assert(dd_v.get_log_drawdown()[19] == 0);
    assert(dd_v.get_pct_drawdown()[19] == 0);
    assert(dd_v.get_result()[8] == 9.5);
    assert(std::abs(dd_v.get_log_drawdown()[8] - 0.0726897) < 0.000001);
    assert(std::abs(dd_v.get_pct_drawdown()[8] - 0.0701107) < 0.000001);
    assert(std::abs(dd_v.get_result()[15] - 19.1) < 0.00001);
    assert(std::abs(dd_v.get_log_drawdown()[15] - 0.151818) < 0.00001);
    assert(std::abs(dd_v.get_pct_drawdown()[15] - 0.140855) < 0.00001);
    assert(std::abs(dd_v.get_result()[20] - 25) < 0.00001);
    assert(std::abs(dd_v.get_log_drawdown()[20] - 0.203125) < 0.00001);
    assert(std::abs(dd_v.get_pct_drawdown()[20] - 0.183824) < 0.00001);
}

// -----------------------------------------------------------------------------

static void test_WilliamPrcRVisitor()  {

    std::cout << "\nTesting WilliamPrcRVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        WilliamPrcRVisitor<double, std::string> wpr_v;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", wpr_v);

        assert(wpr_v.get_result().size() == 5031);
        assert(std::isnan(wpr_v.get_result()[0]));
        assert(std::isnan(wpr_v.get_result()[12]));
        assert(std::abs(wpr_v.get_result()[14] - -46.0784) < 0.0001);
        assert(std::abs(wpr_v.get_result()[20] - -85.2941) < 0.0001);
        assert(std::abs(wpr_v.get_result()[5030] - -73.2151) < 0.0001);
        assert(std::abs(wpr_v.get_result()[5026] - -98.3939) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_PSLVisitor()  {

    std::cout << "\nTesting PSLVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        PSLVisitor<double, std::string> psl_v;

        df.single_act_visit<double, double>("IBM_Close", "IBM_Open", psl_v);
        assert(psl_v.get_result().size() == 5031);
        assert(std::isnan(psl_v.get_result()[0]));
        assert(std::isnan(psl_v.get_result()[12]));
        assert(std::abs(psl_v.get_result()[14] - 57.1429) < 0.0001);
        assert(std::abs(psl_v.get_result()[20] - 42.8571) < 0.0001);
        assert(std::abs(psl_v.get_result()[5030] - 42.8571) < 0.0001);
        assert(std::abs(psl_v.get_result()[5026] - 42.8571) < 0.0001);
        assert(std::abs(psl_v.get_result()[5021] - 57.1429) < 0.0001);

        df.single_act_visit<double>("IBM_Close", psl_v);
        assert(psl_v.get_result().size() == 5031);
        assert(std::isnan(psl_v.get_result()[0]));
        assert(std::isnan(psl_v.get_result()[12]));
        assert(std::abs(psl_v.get_result()[14] - 50) < 0.0001);
        assert(std::abs(psl_v.get_result()[20] - 42.8571) < 0.0001);
        assert(std::abs(psl_v.get_result()[5030] - 42.8571) < 0.0001);
        assert(std::abs(psl_v.get_result()[5026] - 42.8571) < 0.0001);
        assert(std::abs(psl_v.get_result()[5021] - 42.8571) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_CCIVisitor()  {

    std::cout << "\nTesting CCIVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        CCIVisitor<double, std::string> cci_v;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", cci_v);

        assert(cci_v.get_result().size() == 5031);
        assert(std::isnan(cci_v.get_result()[0]));
        assert(std::isnan(cci_v.get_result()[12]));
        assert(std::abs(cci_v.get_result()[14] - 30.3681) < 0.0001);
        assert(std::abs(cci_v.get_result()[20] - -178.37) < 0.001);
        assert(std::abs(cci_v.get_result()[5030] - -77.4585) < 0.0001);
        assert(std::abs(cci_v.get_result()[5026] - -127.358) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_EntropyVisitor()  {

    std::cout << "\nTesting EntropyVisitor{  } ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456, 123457,
          123458, 123459, 123460, 123461, 123462, 123466, 123467, 123468,
          123469, 123470, 123471, 123472, 123473, 22, 23, 24, 25, 26, 27, 28
        };
    std::vector<double>         close =
        { 1.80, 2.80, 1.90, 14.00, 1.10, 6.00, 13.00, 8.00, 9.00, 2.80, 1.90,
          4.30, 20.00, 1.85, 3.00, 34.00, 67.00, 23.00, 87.00, 9.00, 45.00,
          1.00, 11.00, 456.00, 34.00, 7.00, 7778.00, 5.00
        };
    MyDataFrame                 df;

    df.load_data(std::move(idx), std::make_pair("close", close));

    EntropyVisitor<double>  e_v (5);

    df.single_act_visit<double>("close", e_v);

    assert(e_v.get_result().size() == 28);
    assert(std::isnan(e_v.get_result()[0]));
    assert(std::isnan(e_v.get_result()[7]));
    assert(std::abs(e_v.get_result()[8] - 2.18974) < 0.00001);
    assert(std::abs(e_v.get_result()[10] - 1.98477) < 0.00001);
    assert(std::abs(e_v.get_result()[14] - 1.7154) < 0.0001);
    assert(std::abs(e_v.get_result()[27] - 0.596666) < 0.00001);
    assert(std::abs(e_v.get_result()[25] - 0.822228) < 0.00001);
    assert(std::abs(e_v.get_result()[22] - 1.49397) < 0.0001);
}

// -----------------------------------------------------------------------------

static void test_GarmanKlassVolVisitor()  {

    std::cout << "\nTesting GarmanKlassVolVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        GarmanKlassVolVisitor<double, std::string>  gkv_v;

        df.single_act_visit<double, double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", gkv_v);

        assert(gkv_v.get_result().size() == 5031);
        assert(std::isnan(gkv_v.get_result()[0]));
        assert(std::isnan(gkv_v.get_result()[28]));
        assert(std::abs(gkv_v.get_result()[29] - 0.392054) < 0.00001);
        assert(std::abs(gkv_v.get_result()[34] - 0.401494) < 0.00001);
        assert(std::abs(gkv_v.get_result()[5030] - 0.230028) < 0.00001);
        assert(std::abs(gkv_v.get_result()[5026] - 0.221514) < 0.00001);
        assert(std::abs(gkv_v.get_result()[5021] - 0.216817) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_YangZhangVolVisitor()  {

    std::cout << "\nTesting YangZhangVolVisitor{  } ..." << std::endl;

    MyDataFrame df;

    try  {
        df.read("data/FORD.csv", io_format::csv2);

        YangZhangVolVisitor<double> yz_v;

        df.single_act_visit<double, double, double, double>
            ("FORD_Low", "FORD_High", "FORD_Open", "FORD_Close", yz_v);

        assert(yz_v.get_result().size() == 12265);
        assert(std::isnan(yz_v.get_result()[0]));
        assert(std::isnan(yz_v.get_result()[29]));
        assert(std::abs(yz_v.get_result()[30] - 0.169461) < 0.00001);
        assert(std::abs(yz_v.get_result()[35] - 0.181149) < 0.00001);
        assert(std::abs(yz_v.get_result()[12264] - 0.292034) < 0.00001);
        assert(std::abs(yz_v.get_result()[12260] - 0.279347) < 0.00001);
        assert(std::abs(yz_v.get_result()[12255] - 0.293528) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_no_index_writes()  {

    std::cout << "\nTesting no_index_writes ..." << std::endl;

    std::vector<unsigned long>  ulgvec2 =
        { 123450, 123451, 123452, 123450, 123455, 123450, 123449,
          123450, 123451, 123450, 123452, 123450, 123455, 123450,
          123454, 123450, 123450, 123457, 123458, 123459, 123450,
          123441, 123442, 123432, 123450, 123450, 123435, 123450 };
    std::vector<unsigned long>  xulgvec2 = ulgvec2;
    std::vector<int>            intvec2 =
        { 1, 2, 3, 4, 5, 3, 7, 3, 9, 10, 3, 2, 3, 14,
          2, 2, 2, 3, 2, 3, 3, 3, 3, 3, 36, 2, 45, 2 };
    std::vector<double>         xdblvec2 =
        { 1.2345, 2.2345, 3.2345, 4.2345, 5.2345, 3.0, 0.9999,
          10.0, 4.25, 0.009, 8.0, 2.2222, 3.3333,
          11.0, 5.25, 1.009, 2.111, 9.0, 3.2222, 4.3333,
          12.0, 6.25, 2.009, 3.111, 10.0, 4.2222, 5.3333 };
    std::vector<double>         dblvec22 =
        { 0.998, 0.3456, 0.056, 0.15678, 0.00345, 0.923, 0.06743,
          0.1, 0.0056, 0.07865, 0.0111, 0.1002, -0.8888,
          0.14, 0.0456, 0.078654, -0.8999, 0.8002, -0.9888,
          0.2, 0.1056, 0.87865, -0.6999, 0.4111, 0.1902, -0.4888 };
    std::vector<std::string>    strvec2 =
        { "4% of something", "Description 4/5", "This is bad",
          "3.4% of GDP", "Market drops", "Market pulls back",
          "$15 increase", "Running fast", "C++14 development",
          "Some explanation", "More strings", "Bonds vs. Equities",
          "Almost done", "XXXX04",
          "XXXX2", "XXXX3", "XXXX4", "XXXX4", "XXXX5", "XXXX6",
          "XXXX7", "XXXX10", "XXXX11", "XXXX02", "XXXX03" };
    std::vector<bool>           boolvec =
        { true, true, true, false, false, true };

    MyDataFrame df;

    df.load_data(std::move(ulgvec2), std::make_pair("ul_col", xulgvec2));
    df.load_column("xint_col",
                   std::move(intvec2),
                   nan_policy::dont_pad_with_nans);
    df.load_column("str_col",
                   std::move(strvec2),
                   nan_policy::dont_pad_with_nans);
    df.load_column("dbl_col",
                   std::move(xdblvec2),
                   nan_policy::dont_pad_with_nans);
    df.load_column("dbl_col_2",
                   std::move(dblvec22),
                   nan_policy::dont_pad_with_nans);
    df.load_column("bool_col",
                   std::move(boolvec),
                   nan_policy::dont_pad_with_nans);

    df.write<std::ostream,
             int,
             unsigned long,
             double,
             bool,
             std::string>(std::cout, io_format::csv, false);
    std::cout << std::endl;
    df.write<std::ostream,
             int,
             unsigned long,
             double,
             bool,
             std::string>(std::cout, io_format::csv, true);
    std::cout << '\n' << std::endl;

    df.write<std::ostream,
             int,
             unsigned long,
             double,
             bool,
             std::string>(std::cout, io_format::csv2, false);
    std::cout << std::endl;
    df.write<std::ostream,
             int,
             unsigned long,
             double,
             bool,
             std::string>(std::cout, io_format::csv2, true);
    std::cout << '\n' << std::endl;

    df.write<std::ostream,
             int,
             unsigned long,
             double,
             bool,
             std::string>(std::cout, io_format::json, false);
    std::cout << std::endl;
    df.write<std::ostream,
             int,
             unsigned long,
             double,
             bool,
             std::string>(std::cout, io_format::json, true);
}

// -----------------------------------------------------------------------------

static void test_no_index_reads()  {

    std::cout << "\nTesting no_index_reads ..." << std::endl;

    MyDataFrame df;
    MyDataFrame df2;
    MyDataFrame df3;

    try  {
        df.read("data/csv2_format_data.csv", io_format::csv2, false);
        df.read("data/csv2_format_data_2.csv", io_format::csv2, true);
        df.read("data/csv2_format_data_no_index.csv", io_format::csv2, true);
        df.write<std::ostream,
                 int,
                 unsigned long,
                 double,
                 bool,
                 std::string>(std::cout, io_format::csv2);

        std::cout << '\n' << std::endl;
        df2.read("data/sample_data.csv", io_format::csv, false);
        df2.read("data/sample_data_2.csv", io_format::csv, true);
        df2.read("data/sample_data_no_index.csv", io_format::csv, true);
        df2.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  bool,
                  std::string>(std::cout, io_format::csv2);

        std::cout << '\n' << std::endl;
        df3.read("data/sample_data.json", io_format::json, false);
        df3.read("data/sample_data_2.json", io_format::json, true);
        df3.read("data/sample_data_no_index.json", io_format::json, true);
        df3.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  bool,
                  std::string>(std::cout, io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_KamaVisitor()  {

    std::cout << "\nTesting KamaVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        KamaVisitor<double, std::string>    k_v;

        df.single_act_visit<double>("IBM_Close", k_v);

        assert(k_v.get_result().size() == 5031);
        assert(std::isnan(k_v.get_result()[0]));
        assert(std::isnan(k_v.get_result()[8]));
        assert(k_v.get_result()[9] == 0);
        assert(std::abs(k_v.get_result()[29] - 31.6281) < 0.0001);
        assert(std::abs(k_v.get_result()[34] - 47.2049) < 0.0001);
        assert(std::abs(k_v.get_result()[5030] - 112.438) < 0.001);
        assert(std::abs(k_v.get_result()[5026] - 118.829) < 0.001);
        assert(std::abs(k_v.get_result()[5021] - 125.937) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_FisherTransVisitor()  {

    std::cout << "\nTesting FisherTransVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        FisherTransVisitor<double, std::string> ft_v;

        df.single_act_visit<double, double>("IBM_Low", "IBM_High", ft_v);

        assert(ft_v.get_result().size() == 5031);
        assert(std::isnan(ft_v.get_result()[0]));
        assert(std::isnan(ft_v.get_result()[7]));
        assert(ft_v.get_result()[8] == 0);
        assert(std::abs(ft_v.get_result()[29] - -1.47814) < 0.00001);
        assert(std::abs(ft_v.get_result()[34] - -2.12198) < 0.00001);
        assert(std::abs(ft_v.get_result()[5030] - -2.82683) < 0.00001);
        assert(std::abs(ft_v.get_result()[5026] - -2.12427) < 0.00001);
        assert(std::abs(ft_v.get_result()[5021] - -0.266774) < 0.000001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_PercentPriceOSCIVisitor()  {

    std::cout << "\nTesting PercentPriceOSCIVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        PercentPriceOSCIVisitor<double, std::string>    ppo_v;

        df.single_act_visit<double>("IBM_Close", ppo_v);

        assert(ppo_v.get_result().size() == 5031);
        assert(std::isnan(ppo_v.get_result()[0]));
        assert(std::isnan(ppo_v.get_result()[24]));
        assert(std::abs(ppo_v.get_result()[25] - -1.01156) < 0.00001);
        assert(std::abs(ppo_v.get_result()[29] - -1.63896) < 0.00001);
        assert(std::abs(ppo_v.get_result()[34] - -3.17651) < 0.00001);
        assert(std::abs(ppo_v.get_result()[5030] - -3.46821) < 0.00001);
        assert(std::abs(ppo_v.get_result()[5026] - -0.00785639) < 0.00001);
        assert(std::abs(ppo_v.get_result()[5021] - 1.69995) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_SlopeVisitor()  {

    std::cout << "\nTesting SlopeVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        SlopeVisitor<double, std::string>   s_v (10, true, true);

        df.single_act_visit<double>("IBM_Close", s_v);

        assert(s_v.get_result().size() == 5031);
        assert(std::isnan(s_v.get_result()[0]));
        assert(std::isnan(s_v.get_result()[9]));
        assert(std::abs(s_v.get_result()[10] - 4.64508) < 0.00001);
        assert(std::abs(s_v.get_result()[29] - -40.5718) < 0.0001);
        assert(std::abs(s_v.get_result()[34] - -47.07) < 0.001);
        assert(std::abs(s_v.get_result()[5030] - -54.9783) < 0.0001);
        assert(std::abs(s_v.get_result()[5026] - -56.2923) < 0.0001);
        assert(std::abs(s_v.get_result()[5021] - 19.341) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_UltimateOSCIVisitor()  {

    std::cout << "\nTesting UltimateOSCIVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        UltimateOSCIVisitor<double, std::string> uo_v;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", uo_v);

        assert(uo_v.get_result().size() == 5031);
        assert(std::isnan(uo_v.get_result()[0]));
        assert(std::isnan(uo_v.get_result()[26]));
        assert(std::abs(uo_v.get_result()[27] - 41.3509) < 0.0001);
        assert(std::abs(uo_v.get_result()[31] - 32.1768) < 0.0001);
        assert(std::abs(uo_v.get_result()[5030] - 45.076) < 0.001);
        assert(std::abs(uo_v.get_result()[5026] - 31.3935) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_shifting_column()  {

    std::cout << "\nTesting shifting columns ..." << std::endl;

    std::vector<unsigned long>  idx =
        { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
          123457, 123458, 123459, 123460, 123461, 123462, 123466 };
    std::vector<double>         d1 =
        { 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28 };
    MyDataFrame                 df;

    df.load_data(std::move(idx), std::make_pair("dbl_col", d1));

    df.load_column("dbl_col t-1",
                   df.shift<double>("dbl_col", 1, shift_policy::down));
    df.load_column("dbl_col t-2",
                   df.shift<double>("dbl_col", 2, shift_policy::down));
    df.load_column("dbl_col t-3",
                   df.shift<double>("dbl_col", 3, shift_policy::down));
    df.load_column("dbl_col t-4",
                   df.shift<double>("dbl_col", 4, shift_policy::down));
    df.load_column("dbl_col t-5",
                   df.shift<double>("dbl_col", 5, shift_policy::down));
    df.load_column("dbl_col t-6",
                   df.shift<double>("dbl_col", 6, shift_policy::down));
    df.load_column("dbl_col t-7",
                   df.shift<double>("dbl_col", 7, shift_policy::down));

    df.load_column("dbl_col t+1",
                   df.shift<double>("dbl_col", 1, shift_policy::up));
    df.load_column("dbl_col t+2",
                   df.shift<double>("dbl_col", 2, shift_policy::up));
    df.load_column("dbl_col t+3",
                   df.shift<double>("dbl_col", 3, shift_policy::up));
    df.load_column("dbl_col t+4",
                   df.shift<double>("dbl_col", 4, shift_policy::up));
    df.load_column("dbl_col t+5",
                   df.shift<double>("dbl_col", 5, shift_policy::up));
    df.load_column("dbl_col t+6",
                   df.shift<double>("dbl_col", 6, shift_policy::up));
    df.load_column("dbl_col t+7",
                   df.shift<double>("dbl_col", 7, shift_policy::up));

    df.write<std::ostream, double>(std::cout, io_format::csv2);
}

// -----------------------------------------------------------------------------

static void test_UlcerIndexVisitor()  {

    std::cout << "\nTesting UlcerIndexVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        UlcerIndexVisitor<double, std::string>  ui_v;

        df.single_act_visit<double>("IBM_Close", ui_v);

        assert(ui_v.get_result().size() == 5031);
        assert(std::isnan(ui_v.get_result()[0]));
        assert(std::isnan(ui_v.get_result()[12]));
        assert(ui_v.get_result()[13] == 0);
        assert(std::abs(ui_v.get_result()[27] - 6.10378) < 0.00001);
        assert(std::abs(ui_v.get_result()[31] - 8.48463) < 0.00001);
        assert(std::abs(ui_v.get_result()[5030] - 11.1348) < 0.0001);
        assert(std::abs(ui_v.get_result()[5026] - 7.98096) < 0.00001);

        UlcerIndexVisitor<double, std::string>  ui_v2 (14, false);

        df.single_act_visit<double>("IBM_Close", ui_v2);

        assert(ui_v2.get_result().size() == 5031);
        assert(std::isnan(ui_v2.get_result()[0]));
        assert(std::isnan(ui_v2.get_result()[12]));
        assert(ui_v2.get_result()[13] == 0);
        assert(std::abs(ui_v2.get_result()[27] - 1.6313) < 0.0001);
        assert(std::abs(ui_v2.get_result()[31] - 2.26761) < 0.00001);
        assert(std::abs(ui_v2.get_result()[5030] - 2.9759) < 0.0001);
        assert(std::abs(ui_v2.get_result()[5026] - 2.133) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_RSXVisitor()  {

    std::cout << "\nTesting RSXVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        RSXVisitor<double, std::string> rsx_v;

        df.single_act_visit<double>("IBM_Close", rsx_v);

        assert(rsx_v.get_result().size() == 5031);
        assert(std::isnan(rsx_v.get_result()[0]));
        assert(std::isnan(rsx_v.get_result()[12]));
        assert(rsx_v.get_result()[13] == 0);
        assert(rsx_v.get_result()[14] == 50.0);
        assert(rsx_v.get_result()[26] == 50.0);
        assert(std::abs(rsx_v.get_result()[29] - 44.2103) < 0.0001);
        assert(std::abs(rsx_v.get_result()[34] - 37.4583) < 0.0001);
        assert(std::abs(rsx_v.get_result()[5030] - 21.0277) < 0.0001);
        assert(std::abs(rsx_v.get_result()[5026] - 31.1071) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_TTMTrendVisitor()  {

    std::cout << "\nTesting TTMTrendVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/IBM.csv", io_format::csv2);

        TTMTrendVisitor<double, std::string>    ttmt_v;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", ttmt_v);

        assert(ttmt_v.get_result().size() == 5031);
        assert((! ttmt_v.get_result()[0]));
        assert((! ttmt_v.get_result()[8]));
        assert(ttmt_v.get_result()[9]);
        assert(ttmt_v.get_result()[13]);
        assert((! ttmt_v.get_result()[14]));
        assert(ttmt_v.get_result()[5030]);
        assert((! ttmt_v.get_result()[5027]));
        assert(ttmt_v.get_result()[5016]);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_ParabolicSARVisitor()  {

    std::cout << "\nTesting ParabolicSARVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        ParabolicSARVisitor<double, std::string>    psar_v;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", psar_v);

        assert(psar_v.get_result().size() == 1721);
        assert((! psar_v.get_result()[0]));
        assert((psar_v.get_result()[2]));
        assert((psar_v.get_result()[3]));
        assert((psar_v.get_result()[26]));
        assert((! psar_v.get_result()[1720]));
        assert((! psar_v.get_result()[1718]));
        assert((psar_v.get_result()[1709]));

        assert(psar_v.get_longs().size() == 1721);
        assert(std::isnan(psar_v.get_longs()[0]));
        assert(std::isnan(psar_v.get_longs()[2]));
        assert(std::abs(psar_v.get_longs()[5] - 185.401) < 0.001);
        assert(std::abs(psar_v.get_longs()[11] - 183.86) < 0.01);
        assert(std::isnan(psar_v.get_longs()[18]));
        assert(std::isnan(psar_v.get_longs()[1720]));
        assert(std::abs(psar_v.get_longs()[1708] - 124.46) < 0.01);

        assert(psar_v.get_shorts().size() == 1721);
        assert(std::isnan(psar_v.get_shorts()[0]));
        assert(std::abs(psar_v.get_shorts()[2] - 187.4) < 0.1);
        assert(std::isnan(psar_v.get_shorts()[6]));
        assert(std::abs(psar_v.get_shorts()[7] - 190.35) < 0.01);
        assert(std::abs(psar_v.get_shorts()[1720] - 120.7) < 0.1);
        assert(std::isnan(psar_v.get_shorts()[1708]));

        assert(psar_v.get_acceleration_factors().size() == 1721);
        assert(psar_v.get_acceleration_factors()[0] == 0.02);
        assert(psar_v.get_acceleration_factors()[2] == 0.02);
        assert(psar_v.get_acceleration_factors()[15] == 0.02);
        assert(psar_v.get_acceleration_factors()[16] == 0.04);
        assert(psar_v.get_acceleration_factors()[1720] == 0.14);
        assert(std::abs(psar_v.get_acceleration_factors()[1718] - 0.12) < 0.01);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_EBSineWaveVisitor()  {

    std::cout << "\nTesting EBSineWaveVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        EBSineWaveVisitor<double, std::string>  ebsw_v;

        df.single_act_visit<double>("IBM_Close", ebsw_v);

        assert(ebsw_v.get_result().size() == 1721);
        assert(std::isnan(ebsw_v.get_result()[0]));
        assert(std::abs(ebsw_v.get_result()[5] - 0.927837) < 0.00001);
        assert(std::abs(ebsw_v.get_result()[14] - -0.560866) < 0.00001);
        assert(std::abs(ebsw_v.get_result()[25] - -0.36883) < 0.00001);
        assert(std::abs(ebsw_v.get_result()[1720] - -0.901317) < 0.00001);
        assert(std::abs(ebsw_v.get_result()[1712] - -0.730321) < 0.00001);
        assert(std::abs(ebsw_v.get_result()[1707] - 0.841759) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_EhlerSuperSmootherVisitor()  {

    std::cout << "\nTesting EhlerSuperSmootherVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        EhlerSuperSmootherVisitor<double, std::string>  ssf_v2; // poles = 2
        EhlerSuperSmootherVisitor<double, std::string>  ssf_v3(3); // poles = 3

        df.single_act_visit<double>("IBM_Close", ssf_v2);
        df.single_act_visit<double>("IBM_Close", ssf_v3);

        assert(ssf_v2.get_result().size() == 1721);
        assert(ssf_v3.get_result().size() == 1721);

        assert(std::abs(ssf_v2.get_result()[0] - 185.53) < 0.01);
        assert(std::abs(ssf_v2.get_result()[5] - 188.117) < 0.001);
        assert(std::abs(ssf_v2.get_result()[14] - 185.497) < 0.001);
        assert(std::abs(ssf_v2.get_result()[25] - 174.736) < 0.001);
        assert(std::abs(ssf_v2.get_result()[1720] - 109.294) < 0.001);
        assert(std::abs(ssf_v2.get_result()[1712] - 123.43) < 0.01);
        assert(std::abs(ssf_v2.get_result()[1707] - 127.323) < 0.001);

        assert(std::abs(ssf_v3.get_result()[0] - 185.53) < 0.01);
        assert(std::abs(ssf_v3.get_result()[5] - 186.159) < 0.001);
        assert(std::abs(ssf_v3.get_result()[14] - 186.681) < 0.001);
        assert(std::abs(ssf_v3.get_result()[25] - 174.294) < 0.001);
        assert(std::abs(ssf_v3.get_result()[1720] - 109.187) < 0.001);
        assert(std::abs(ssf_v3.get_result()[1712] - 124.31) < 0.01);
        assert(std::abs(ssf_v3.get_result()[1707] - 127.603) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_VarIdxDynAvgVisitor()  {

    std::cout << "\nTesting VarIdxDynAvgVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        VarIdxDynAvgVisitor<double, std::string>    vidya_v;

        df.single_act_visit<double>("IBM_Close", vidya_v);

        assert(vidya_v.get_result().size() == 1721);

        assert(std::isnan(vidya_v.get_result()[0]));
        assert(std::isnan(vidya_v.get_result()[12]));
        assert(vidya_v.get_result()[13] == 0);
        assert(std::abs(vidya_v.get_result()[14] - 2.70068) < 0.00001);
        assert(std::abs(vidya_v.get_result()[21] - 57.6682) < 0.0001);
        assert(std::abs(vidya_v.get_result()[31] - 106.451) < 0.001);
        assert(std::abs(vidya_v.get_result()[1720] - 118.962) < 0.001);
        assert(std::abs(vidya_v.get_result()[1712] - 123.811) < 0.001);
        assert(std::abs(vidya_v.get_result()[1707] - 123.712) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_AbsVisitor()  {

    std::cout << "\nTesting AbsVisitor{ } ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL, 9UL, 12UL, 10UL, 13UL,
          10UL, 15UL, 14UL };
    std::vector<double>         dblvec =
        { 0.0, -15.0, 14.0, 15.0, -1.0, 12.0, 11.0, -8.0, 15.0, 6.0, -1,
          4.0, 14.0, 14.0, -20.0 };
    std::vector<int>            intvec =
        { -1, 2, 3, 4, 5, 8, -6, 7, 11, -14, 9, -3, -5, -4, 9 };
    std::vector<std::string>    strvec =
        { "zz", "bb", "zz", "ww", "ee", "ff", "gg", "zz", "ii", "jj", "kk",
          "ll", "mm", "ee", "" };

    df.load_data(std::move(idxvec),
                 std::make_pair("dbl_col", dblvec),
                 std::make_pair("int_col", intvec),
                 std::make_pair("str_col", strvec));

    AbsVisitor<double>  abs_v;
    AbsVisitor<int>     abs_v_int;
    auto                result =
        df.visit<double>("dbl_col", abs_v).get_result();
    auto                result_int =
        df.single_act_visit<int>("int_col", abs_v_int).get_result();

    assert(result == 5);
    assert(result_int == 6);

    std::vector<double> abs_dblvec =
        { 0.0, 15.0, 14.0, 15.0, 1.0, 12.0, 11.0, 8.0, 15.0, 6.0, 1,
          4.0, 14.0, 14.0, 20.0 };
    std::vector<int>    abs_intvec =
        { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9, 3, 5, 4, 9 };

    assert((df.get_column<double>("dbl_col") == abs_dblvec));
    assert((df.get_column<int>("int_col") == abs_intvec));
}

// -----------------------------------------------------------------------------

static void test_PivotPointSRVisitor()  {

    std::cout << "\nTesting PivotPointSRVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        PivotPointSRVisitor<double, std::string>    ppsr_v;

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", ppsr_v);

        assert(ppsr_v.get_result().size() == 1721);
        assert(std::abs(ppsr_v.get_result()[0] - 186.043) < 0.001);
        assert(std::abs(ppsr_v.get_result()[5] - 187.81) < 0.01);
        assert(std::abs(ppsr_v.get_result()[14] - 182.387) < 0.001);
        assert(std::abs(ppsr_v.get_result()[25] - 176.627) < 0.001);
        assert(std::abs(ppsr_v.get_result()[1720] - 110.403) < 0.001);
        assert(std::abs(ppsr_v.get_result()[1712] - 118.12) < 0.01);
        assert(std::abs(ppsr_v.get_result()[1707] - 125.57) < 0.01);

        assert(ppsr_v.get_resist_1().size() == 1721);
        assert(std::abs(ppsr_v.get_resist_1()[0] - 186.887) < 0.001);
        assert(std::abs(ppsr_v.get_resist_1()[5] - 189.07) < 0.01);
        assert(std::abs(ppsr_v.get_resist_1()[14] - 184.063) < 0.001);
        assert(std::abs(ppsr_v.get_resist_1()[25] - 178.183) < 0.001);
        assert(std::abs(ppsr_v.get_resist_1()[1720] - 113.057) < 0.001);
        assert(std::abs(ppsr_v.get_resist_1()[1712] - 119.4) < 0.01);
        assert(std::abs(ppsr_v.get_resist_1()[1707] - 126.68) < 0.01);

        assert(ppsr_v.get_support_1().size() == 1721);
        assert(std::abs(ppsr_v.get_support_1()[0] - 184.687) < 0.001);
        assert(std::abs(ppsr_v.get_support_1()[5] - 186.12) < 0.01);
        assert(std::abs(ppsr_v.get_support_1()[14] - 181.053) < 0.001);
        assert(std::abs(ppsr_v.get_support_1()[25] - 175.693) < 0.001);
        assert(std::abs(ppsr_v.get_support_1()[1720] - 109.007) < 0.001);
        assert(std::abs(ppsr_v.get_support_1()[1712] - 116.09) < 0.01);
        assert(std::abs(ppsr_v.get_support_1()[1707] - 123.99) < 0.01);

        assert(ppsr_v.get_resist_2().size() == 1721);
        assert(std::abs(ppsr_v.get_resist_2()[0] - 188.243) < 0.001);
        assert(std::abs(ppsr_v.get_resist_2()[5] - 190.76) < 0.01);
        assert(std::abs(ppsr_v.get_resist_2()[14] - 185.397) < 0.001);
        assert(std::abs(ppsr_v.get_resist_2()[25] - 179.117) < 0.001);
        assert(std::abs(ppsr_v.get_resist_2()[1720] - 114.453) < 0.001);
        assert(std::abs(ppsr_v.get_resist_2()[1712] - 121.43) < 0.01);
        assert(std::abs(ppsr_v.get_resist_2()[1707] - 128.26) < 0.01);

        assert(ppsr_v.get_support_2().size() == 1721);
        assert(std::abs(ppsr_v.get_support_2()[0] - 183.843) < 0.001);
        assert(std::abs(ppsr_v.get_support_2()[5] - 184.86) < 0.01);
        assert(std::abs(ppsr_v.get_support_2()[14] - 179.377) < 0.001);
        assert(std::abs(ppsr_v.get_support_2()[25] - 174.137) < 0.001);
        assert(std::abs(ppsr_v.get_support_2()[1720] - 106.353) < 0.001);
        assert(std::abs(ppsr_v.get_support_2()[1712] - 114.81) < 0.01);
        assert(std::abs(ppsr_v.get_support_2()[1707] - 122.88) < 0.01);

        assert(ppsr_v.get_resist_3().size() == 1721);
        assert(std::abs(ppsr_v.get_resist_3()[0] - 189.087) < 0.001);
        assert(std::abs(ppsr_v.get_resist_3()[5] - 192.02) < 0.01);
        assert(std::abs(ppsr_v.get_resist_3()[14] - 187.073) < 0.001);
        assert(std::abs(ppsr_v.get_resist_3()[25] - 180.673) < 0.001);
        assert(std::abs(ppsr_v.get_resist_3()[1720] - 117.107) < 0.001);
        assert(std::abs(ppsr_v.get_resist_3()[1712] - 122.71) < 0.01);
        assert(std::abs(ppsr_v.get_resist_3()[1707] - 129.37) < 0.01);

        assert(ppsr_v.get_support_3().size() == 1721);
        assert(std::abs(ppsr_v.get_support_3()[0] - 182.487) < 0.001);
        assert(std::abs(ppsr_v.get_support_3()[5] - 183.17) < 0.01);
        assert(std::abs(ppsr_v.get_support_3()[14] - 178.043) < 0.001);
        assert(std::abs(ppsr_v.get_support_3()[25] - 173.203) < 0.001);
        assert(std::abs(ppsr_v.get_support_3()[1720] - 104.957) < 0.001);
        assert(std::abs(ppsr_v.get_support_3()[1712] - 112.78) < 0.01);
        assert(std::abs(ppsr_v.get_support_3()[1707] - 121.3) < 0.01);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_AvgDirMovIdxVisitor()  {

    std::cout << "\nTesting AvgDirMovIdxVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        adx_v<double, std::string>  adx_v (3, 4);

        df.single_act_visit<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", adx_v);

        assert(adx_v.get_result().size() == 1721);
        assert(adx_v.get_result()[0] == 0);
        assert(std::abs(adx_v.get_result()[10] - 0.73029) < 0.00001);
        assert(std::abs(adx_v.get_result()[14] - 0.735792) < 0.000001);
        assert(std::abs(adx_v.get_result()[25] - 0.691082) < 0.000001);
        assert(std::abs(adx_v.get_result()[1720] - 0.372184) < 0.000001);
        assert(std::abs(adx_v.get_result()[1712] - 0.703394) < 0.000001);
        assert(std::abs(adx_v.get_result()[1707] - 0.383002) < 0.000001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_HoltWinterChannelVisitor()  {

    std::cout << "\nTesting HoltWinterChannelVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        hwc_v<double, std::string>  hwc;

        df.single_act_visit<double>("IBM_Close", hwc);

        assert(hwc.get_result().size() == 1721);
        assert(std::abs(hwc.get_result()[0] - 185.53) < 0.001);
        assert(std::abs(hwc.get_result()[5] - 187.349) < 0.001);
        assert(std::abs(hwc.get_result()[14] - 186.534) < 0.001);
        assert(std::abs(hwc.get_result()[25] - 171.785) < 0.001);
        assert(std::abs(hwc.get_result()[1720] - 111.435) < 0.001);
        assert(std::abs(hwc.get_result()[1712] - 126.602) < 0.001);
        assert(std::abs(hwc.get_result()[1707] - 126.443) < 0.001);

        assert(hwc.get_upper_band().size() == 1721);
        assert(std::abs(hwc.get_upper_band()[0] - 185.53) < 0.001);
        assert(std::abs(hwc.get_upper_band()[5] - 188.322) < 0.001);
        assert(std::abs(hwc.get_upper_band()[14] - 187.683) < 0.001);
        assert(std::abs(hwc.get_upper_band()[25] - 174.518) < 0.001);
        assert(std::abs(hwc.get_upper_band()[1720] - 117.324) < 0.001);
        assert(std::abs(hwc.get_upper_band()[1712] - 129.354) < 0.001);
        assert(std::abs(hwc.get_upper_band()[1707] - 129.713) < 0.001);

        assert(hwc.get_lower_band().size() == 1721);
        assert(std::abs(hwc.get_lower_band()[0] - 185.53) < 0.001);
        assert(std::abs(hwc.get_lower_band()[5] - 186.375) < 0.001);
        assert(std::abs(hwc.get_lower_band()[14] - 185.386) < 0.001);
        assert(std::abs(hwc.get_lower_band()[25] - 169.053) < 0.001);
        assert(std::abs(hwc.get_lower_band()[1720] - 105.545) < 0.001);
        assert(std::abs(hwc.get_lower_band()[1712] - 123.851) < 0.001);
        assert(std::abs(hwc.get_lower_band()[1707] - 123.173) < 0.001);

        assert(hwc.get_pct_diff().size() == 1721);
        assert(std::isnan(hwc.get_pct_diff()[0]));
        assert(std::isnan(hwc.get_pct_diff()[2]));
        assert(std::abs(hwc.get_pct_diff()[5] - 0.516164) < 0.00001);
        assert(std::abs(hwc.get_pct_diff()[14] - -1.15602) < 0.00001);
        assert(std::abs(hwc.get_pct_diff()[25] - 1.50004) < 0.00001);
        assert(std::abs(hwc.get_pct_diff()[1720] - 0.51913) < 0.00001);
        assert(std::abs(hwc.get_pct_diff()[1712] - -1.17764) < 0.00001);
        assert(std::abs(hwc.get_pct_diff()[1707] - 0.294638) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_HeikinAshiCndlVisitor()  {

    std::cout << "\nTesting HeikinAshiCndlVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        ha_cdl_v<double, std::string>  ha;

        df.single_act_visit<double, double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", ha);

        assert(ha.get_result().size() == 1721);
        assert(std::abs(ha.get_result()[0] - 186.335) < 0.001);
        assert(std::abs(ha.get_result()[10] - 188.02) < 0.001);
        assert(std::abs(ha.get_result()[14] - 182.147) < 0.001);
        assert(std::abs(ha.get_result()[25] - 176.38) < 0.001);
        assert(std::abs(ha.get_result()[1720] - 109.778) < 0.001);
        assert(std::abs(ha.get_result()[1712] - 118.54) < 0.001);
        assert(std::abs(ha.get_result()[1707] - 125.82) < 0.001);

        assert(ha.get_open().size() == 1721);
        assert(std::abs(ha.get_open()[0] - 186.37) < 0.001);
        assert(std::abs(ha.get_open()[10] - 186.432) < 0.001);
        assert(std::abs(ha.get_open()[14] - 185.131) < 0.001);
        assert(std::abs(ha.get_open()[25] - 174.113) < 0.001);
        assert(std::abs(ha.get_open()[1720] - 109.256) < 0.001);
        assert(std::abs(ha.get_open()[1712] - 125.839) < 0.001);
        assert(std::abs(ha.get_open()[1707] - 128.034) < 0.001);

        assert(ha.get_high().size() == 1721);
        assert(std::abs(ha.get_high()[0] - 187.4) < 0.001);
        assert(std::abs(ha.get_high()[10] - 188.99) < 0.01);
        assert(std::abs(ha.get_high()[14] - 185.131) < 0.001);
        assert(std::abs(ha.get_high()[25] - 177.56) < 0.001);
        assert(std::abs(ha.get_high()[1720] - 111.8) < 0.001);
        assert(std::abs(ha.get_high()[1712] - 125.839) < 0.001);
        assert(std::abs(ha.get_high()[1707] - 128.034) < 0.001);

        assert(ha.get_low().size() == 1721);
        assert(std::abs(ha.get_low()[0] - 185.2) < 0.001);
        assert(std::abs(ha.get_low()[10] - 186.432) < 0.001);
        assert(std::abs(ha.get_low()[14] - 180.71) < 0.001);
        assert(std::abs(ha.get_low()[25] - 174.113) < 0.001);
        assert(std::abs(ha.get_low()[1720] - 107.75) < 0.001);
        assert(std::abs(ha.get_low()[1712] - 116.84) < 0.001);
        assert(std::abs(ha.get_low()[1707] - 124.46) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_FastFourierTransVisitor()  {

    std::cout << "\nTesting FastFourierTransVisitor{  } ..." << std::endl;

    using cx = std::complex<double>;

    std::vector<unsigned long>  idxvec =
        { 1UL, 2UL, 3UL, 10UL, 5UL, 7UL, 8UL, 12UL };
    std::vector<double>         dblvec = { 1, 1, 1, 1, 0, 0, 0, 0 };
    std::vector<std::string>    strvec =
        { "11", "22", "33", "44", "55", "66", "-77", "88" };
    std::vector<cx>             cplxvec =
        { cx(0, 0), cx(1, 1), cx(3, 3), cx(4, 4),
          cx(4, 4), cx(3, 3), cx(1, 1) };

    MyDataFrame df;

    df.load_data(std::move(idxvec),
                 std::make_pair("str_col", strvec),
                 std::make_pair("dbl_col", dblvec));
    df.load_column("cplx_col", cplxvec, nan_policy::dont_pad_with_nans);

    fft_v<double>   fft;

    df.single_act_visit<double>("dbl_col", fft);

    for (auto citer : fft.get_result())
        std::cout << citer << " | ";
    std::cout << std::endl;
    df.load_column("FFT int col", fft.get_result(),
                   nan_policy::dont_pad_with_nans);

    fft_v<std::complex<double>> i_fft (true);

    df.single_act_visit<std::complex<double>>("FFT int col", i_fft);

    for (auto citer : i_fft.get_result())
        std::cout << citer << " | ";
    std::cout << std::endl;

    // The case of size is not a power of 2
    //
    fft_v<cx>   fft_cx;

    df.single_act_visit<cx>("cplx_col", fft_cx);

    for (auto citer : fft_cx.get_result())
        std::cout << citer << " | ";
    std::cout << std::endl;
    df.load_column("FFT int col 2", fft_cx.get_result(),
                   nan_policy::dont_pad_with_nans);

    fft_v<cx>   i_fft2 (true);

    df.single_act_visit<std::complex<double>>("FFT int col 2", i_fft2);

    for (auto citer : i_fft2.get_result())
        std::cout << citer << " | ";
    std::cout << std::endl;

    try  {
        typedef StdDataFrame<std::string> StrDataFrame;

        StrDataFrame    df2;

        df2.read("data/SHORT_IBM.csv", io_format::csv2);

        fft_v<double, std::string>  fft2;

        df2.single_act_visit<double>("IBM_Close", fft2);
        df2.load_column("FFT Close", fft2.get_result());

        fft_v<cx>   i_fft2 (true);

        df2.single_act_visit<cx>("FFT Close", i_fft2);

        /*
        for (auto citer : i_fft2.get_result())
            std::cout << citer << ", ";
        std::cout << std::endl;
        */
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_CenterOfGravityVisitor()  {

    std::cout << "\nTesting CenterOfGravityVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        cog_v<double, std::string>  cog;

        df.single_act_visit<double>("IBM_Close", cog);

        assert(cog.get_result().size() == 1721);
        assert(std::isnan(cog.get_result()[0]));
        assert(std::isnan(cog.get_result()[8]));
        assert(std::abs(cog.get_result()[10] - -5.4998) < 0.0001);
        assert(std::abs(cog.get_result()[14] - -5.51127) < 0.0001);
        assert(std::abs(cog.get_result()[25] - -5.51401) < 0.0001);
        assert(std::abs(cog.get_result()[1720] - -5.60765) < 0.0001);
        assert(std::abs(cog.get_result()[1712] - -5.54681) < 0.0001);
        assert(std::abs(cog.get_result()[1707] - -5.44354) < 0.0001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_ArnaudLegouxMAVisitor()  {

    std::cout << "\nTesting ArnaudLegouxMAVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        alma_v<double, std::string> alma;

        df.single_act_visit<double>("IBM_Close", alma);

        assert(alma.get_result().size() == 1721);
        assert(std::isnan(alma.get_result()[0]));
        assert(std::isnan(alma.get_result()[9]));
        assert(std::abs(alma.get_result()[10] - 187.533) < 0.001);
        assert(std::abs(alma.get_result()[14] - 186.359) < 0.001);
        assert(std::abs(alma.get_result()[25] - 176.892) < 0.001);
        assert(std::abs(alma.get_result()[1720] - 117.841) < 0.001);
        assert(std::abs(alma.get_result()[1712] - 127.677) < 0.001);
        assert(std::abs(alma.get_result()[1707] - 121.435) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_RateOfChangeVisitor()  {

    std::cout << "\nTesting RateOfChangeVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        roc_v<double, std::string>  roc (10); // 10 period rate of change

        df.single_act_visit<double>("IBM_Close", roc);

        assert(roc.get_result().size() == 1721);
        assert(std::isnan(roc.get_result()[0]));
        assert(std::isnan(roc.get_result()[9]));
        assert(std::abs(roc.get_result()[10] - 0.0174096) < 0.000001);
        assert(std::abs(roc.get_result()[14] - -0.0278768) < 0.000001);
        assert(std::abs(roc.get_result()[25] - -0.0133044) < 0.000001);
        assert(std::abs(roc.get_result()[1720] - -0.113317) < 0.00001);
        assert(std::abs(roc.get_result()[1712] - -0.0377142) < 0.000001);
        assert(std::abs(roc.get_result()[1707] - 0.0343972) < 0.000001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_AccumDistVisitor()  {

    std::cout << "\nTesting AccumDistVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        ad_v<double, std::string>   ad;

        std::future<ad_v<double, std::string> &>   fut =
            df.single_act_visit_async<double, double, double, double, long>
            ("IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", "IBM_Volume", ad);

        fut.get();
        assert(ad.get_result().size() == 1721);
        assert(std::abs(ad.get_result()[0] - -3471893.994401) < 0.00001);
        assert(std::abs(ad.get_result()[10] - -3089366.572853) < 0.00001);
        assert(std::abs(ad.get_result()[14] - 3190895.313251) < 0.00001);
        assert(std::abs(ad.get_result()[25] - -4599921.087384) < 0.00001);
        assert(std::abs(ad.get_result()[1720] - -70588883.462685) < 0.00001);
        assert(std::abs(ad.get_result()[1712] - -61812361.976164) < 0.00001);
        assert(std::abs(ad.get_result()[1707] - -47503851.035966) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_ChaikinMoneyFlowVisitor()  {

    std::cout << "\nTesting ChaikinMoneyFlowVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        cmf_v<double, std::string>  cmf;

        std::future<cmf_v<double, std::string> &>   fut =
            df.single_act_visit_async<double, double, double, double, long>
            ("IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", "IBM_Volume", cmf);

        fut.get();
        assert(cmf.get_result().size() == 1721);
        assert(std::isnan(cmf.get_result()[0]));
        assert(std::isnan(cmf.get_result()[19]));
        assert(std::abs(cmf.get_result()[20] - -0.0404048) < 0.00001);
        assert(std::abs(cmf.get_result()[24] - -0.0674374) < 0.00001);
        assert(std::abs(cmf.get_result()[25] - -0.0201182) < 0.00001);
        assert(std::abs(cmf.get_result()[1720] - -0.195288) < 0.00001);
        assert(std::abs(cmf.get_result()[1712] - -0.175841) < 0.00001);
        assert(std::abs(cmf.get_result()[1707] - -0.0827408) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_VertHorizFilterVisitor()  {

    std::cout << "\nTesting VertHorizFilterVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        vhf_v<double, std::string>  vhf;

        df.single_act_visit<double>("IBM_Close", vhf);

        assert(vhf.get_result().size() == 1721);
        assert(std::isnan(vhf.get_result()[0]));
        assert(std::isnan(vhf.get_result()[27]));
        assert(std::abs(vhf.get_result()[28] - 0.385992) < 0.000001);
        assert(std::abs(vhf.get_result()[30] - 0.371847) < 0.000001);
        assert(std::abs(vhf.get_result()[35] - 0.417574) < 0.000001);
        assert(std::abs(vhf.get_result()[1720] - 0.450244) < 0.00001);
        assert(std::abs(vhf.get_result()[1712] - 0.301387) < 0.000001);
        assert(std::abs(vhf.get_result()[1707] - 0.297249) < 0.000001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_OnBalanceVolumeVisitor()  {

    std::cout << "\nTesting OnBalanceVolumeVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        obv_v<double, std::string>  obv;

        std::future<obv_v<double, std::string> &>   fut =
            df.single_act_visit_async<double, long>
            ("IBM_Close", "IBM_Volume", obv);

        fut.get();
        assert(obv.get_result().size() == 1721);
        assert(std::abs(obv.get_result()[0] - 4546500) < 0.001);
        assert(std::abs(obv.get_result()[19] - -21855500) < 0.001);
        assert(std::abs(obv.get_result()[20] - -27048900) < 0.001);
        assert(std::abs(obv.get_result()[24] - -29581000) < 0.001);
        assert(std::abs(obv.get_result()[25] - -24888100) < 0.001);
        assert(std::abs(obv.get_result()[1720] - -18817000) < 0.001);
        assert(std::abs(obv.get_result()[1712] - -12925800) < 0.001);
        assert(std::abs(obv.get_result()[1707] - 10998800) < 0.001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}


// -----------------------------------------------------------------------------

static void test_TrueRangeVisitor()  {

    std::cout << "\nTesting TrueRangeVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        TrueRangeVisitor<double, std::string>   tr;

        std::future<TrueRangeVisitor<double, std::string> &>    fut =
            df.single_act_visit_async<double, double, double>
            ("IBM_Low", "IBM_High", "IBM_Close", tr);

        fut.get();
        assert(tr.get_result().size() == 1721);
        assert(std::isnan(tr.get_result()[0]));
        assert(std::abs(tr.get_result()[19] - 1.5) < 0.1);
        assert(std::abs(tr.get_result()[20] - 2.5) < 0.1);
        assert(std::abs(tr.get_result()[24] - 1.06001) < 0.00001);
        assert(std::abs(tr.get_result()[25] - 2.89) < 0.01);
        assert(std::abs(tr.get_result()[1720] - 4.05) < 0.01);
        assert(std::abs(tr.get_result()[1712] - 8.68) < 0.01);
        assert(std::abs(tr.get_result()[1707] - 2.75) < 0.01);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_DecayVisitor()  {

    std::cout << "\nTesting DecayVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        DecayVisitor<double, std::string>  decay(5, true);

        df.single_act_visit<double>("IBM_Close", decay);

        assert(decay.get_result().size() == 1721);
        assert(std::abs(decay.get_result()[0] - 185.53) < 0.01);
        assert(std::abs(decay.get_result()[27] - 179.7) < 0.01);
        assert(std::abs(decay.get_result()[28] - 180.24) < 0.01);
        assert(std::abs(decay.get_result()[30] - 183.69) < 0.01);
        assert(std::abs(decay.get_result()[35] - 183.45) < 0.01);
        assert(std::abs(decay.get_result()[1720] - 111.66) < 0.01);
        assert(std::abs(decay.get_result()[1712] - 125.513) < 0.01);
        assert(std::abs(decay.get_result()[1707] - 127.203) < 0.01);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_HodgesTompkinsVolVisitor()  {

    std::cout << "\nTesting HodgesTompkinsVolVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        ht_vol_v<double, std::string>   ht;

        df.single_act_visit<double>("IBM_Close", ht);

        assert(ht.get_result().size() == 1721);
        assert(std::isnan(ht.get_result()[0]));
        assert(std::isnan(ht.get_result()[28]));
        assert(std::abs(ht.get_result()[29] - 0.187655) < 0.00001);
        assert(std::abs(ht.get_result()[30] - 0.187132) < 0.00001);
        assert(std::abs(ht.get_result()[31] - 0.186253) < 0.00001);
        assert(std::abs(ht.get_result()[35] - 0.177077) < 0.00001);
        assert(std::abs(ht.get_result()[1720] - 0.365188) < 0.00001);
        assert(std::abs(ht.get_result()[1712] - 0.326883) < 0.00001);
        assert(std::abs(ht.get_result()[1707] - 0.298478) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

static void test_ParkinsonVolVisitor()  {

    std::cout << "\nTesting ParkinsonVolVisitor{  } ..." << std::endl;

    typedef StdDataFrame<std::string> StrDataFrame;

    StrDataFrame    df;

    try  {
        df.read("data/SHORT_IBM.csv", io_format::csv2);

        p_vol_v<double, std::string>    pv;

        df.single_act_visit<double, double>("IBM_Low", "IBM_High", pv);

        assert(pv.get_result().size() == 1721);
        assert(std::isnan(pv.get_result()[0]));
        assert(std::isnan(pv.get_result()[28]));
        assert(std::abs(pv.get_result()[29] - 0.143397) < 0.00001);
        assert(std::abs(pv.get_result()[30] - 0.145651) < 0.00001);
        assert(std::abs(pv.get_result()[31] - 0.145266) < 0.00001);
        assert(std::abs(pv.get_result()[35] - 0.144596) < 0.00001);
        assert(std::abs(pv.get_result()[1720] - 0.225651) < 0.00001);
        assert(std::abs(pv.get_result()[1712] - 0.208081) < 0.00001);
        assert(std::abs(pv.get_result()[1707] - 0.236226) < 0.00001);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }
}

// -----------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    test_get_reindexed();
    test_get_reindexed_view();
    test_retype_column();
    test_load_align_column();
    test_get_columns_info();
    test_CategoryVisitor();
    test_FactorizeVisitor();
    test_pattern_match();
    test_ClipVisitor();
    test_SharpeRatioVisitor();
    test_RankVisitor();
    test_SigmoidVisitor();
    test_combine();
    test_RSIVisitor();
    test_remove_duplicates();
    test_groupby();
    test_groupby_2();
    test_groupby_3();
    test_io_format_csv2();
    test_BoxCoxVisitor();
    test_NormalizeVisitor();
    test_HampelFilterVisitor();
    test_PolyFitVisitor();
    test_HurstExponentVisitor();
    test_LogFitVisitor();
    test_ExpoSmootherVisitor();
    test_HWExpoSmootherVisitor();
    test_consolidate();
    test_ExtremumSubArrayVisitor();
    test_NExtremumSubArrayVisitor();
    test_LowessVisitor();
    test_StepRollAdopter();
    test_DecomposeVisitor();
    test_DT_IBM_data();
    test_TTestVisitor();
    test_MassIndexVisitor();
    test_HullRollingMeanVisitor();
    test_RollingMidValueVisitor();
    test_DrawdownVisitor();
    test_WilliamPrcRVisitor();
    test_PSLVisitor();
    test_CCIVisitor();
    test_EntropyVisitor();
    test_GarmanKlassVolVisitor();
    test_YangZhangVolVisitor();
    test_no_index_writes();
    test_no_index_reads();
    test_KamaVisitor();
    test_FisherTransVisitor();
    test_PercentPriceOSCIVisitor();
    test_SlopeVisitor();
    test_UltimateOSCIVisitor();
    test_shifting_column();
    test_UlcerIndexVisitor();
    test_bucketize();
    test_RSXVisitor();
    test_TTMTrendVisitor();
    test_ParabolicSARVisitor();
    test_EBSineWaveVisitor();
    test_EhlerSuperSmootherVisitor();
    test_VarIdxDynAvgVisitor();
    test_AbsVisitor();
    test_PivotPointSRVisitor();
    test_AvgDirMovIdxVisitor();
    test_HoltWinterChannelVisitor();
    test_HeikinAshiCndlVisitor();
    test_FastFourierTransVisitor();
    test_CenterOfGravityVisitor();
    test_ArnaudLegouxMAVisitor();
    test_RateOfChangeVisitor();
    test_AccumDistVisitor();
    test_ChaikinMoneyFlowVisitor();
    test_VertHorizFilterVisitor();
    test_OnBalanceVolumeVisitor();
    test_TrueRangeVisitor();
    test_DecayVisitor();
    test_HodgesTompkinsVolVisitor();
    test_ParkinsonVolVisitor();

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
