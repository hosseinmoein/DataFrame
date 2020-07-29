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
#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/DataFrameTransformVisitors.h>
#include <DataFrame/RandGen.h>

#include <cassert>
#include <iostream>
#include <string>

using namespace hmdf;

typedef StdDataFrame<unsigned long> MyDataFrame;

// -----------------------------------------------------------------------------

static void test_get_reindexed()  {

    std::cout << "\nTesting get_reindexed( ) ..." << std::endl;

    MyDataFrame df;

    std::vector<unsigned long>  idxvec = { 1UL, 2UL, 3UL, 10UL, 5UL,
                                           7UL, 8UL, 12UL, 9UL, 12UL,
                                           10UL, 13UL, 10UL, 15UL, 14UL };
    std::vector<double>         dblvec = { 0.0, 15.0, 14.0, 2.0, 1.0,
                                           12.0, 11.0, 8.0, 7.0, 6.0,
                                           5.0, 4.0, 3.0, 9.0, 10.0};
    std::vector<double>         dblvec2 = { 100.0, 101.0, 102.0, 103.0, 104.0,
                                            105.0, 106.55, 107.34, 1.8, 111.0,
                                            112.0, 113.0, 114.0, 115.0, 116.0};
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec = { "zz", "bb", "cc", "ww", "ee",
                                           "ff", "gg", "hh", "ii", "jj",
                                           "kk", "ll", "mm", "nn", "oo" };

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

    std::vector<unsigned long>  idxvec = { 1UL, 2UL, 3UL, 10UL, 5UL,
                                           7UL, 8UL, 12UL, 9UL, 12UL,
                                           10UL, 13UL, 10UL, 15UL, 14UL };
    std::vector<double>         dblvec = { 0.0, 15.0, 14.0, 2.0, 1.0,
                                           12.0, 11.0, 8.0, 7.0, 6.0,
                                           5.0, 4.0, 3.0, 9.0, 10.0};
    std::vector<double>         dblvec2 = { 100.0, 101.0, 102.0, 103.0, 104.0,
                                            105.0, 106.55, 107.34, 1.8, 111.0,
                                            112.0, 113.0, 114.0, 115.0, 116.0};
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec = { "zz", "bb", "cc", "ww", "ee",
                                           "ff", "gg", "hh", "ii", "jj",
                                           "kk", "ll", "mm", "nn", "oo" };

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

    df.load_data(std::move(idxvec),
                 std::make_pair("int_col", intvec));
    df.load_align_column("summary_col", std::move(summary_vec), 5, true);

    std::vector<double> summary_vec_2 = { 102, 202, 302, 402, 502 };

    df.load_align_column("summary_col_2", std::move(summary_vec_2), 5, false);

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

    std::vector<unsigned long>  idxvec = { 1UL, 2UL, 3UL, 10UL, 5UL,
                                           7UL, 8UL, 12UL, 9UL, 12UL,
                                           10UL, 13UL, 10UL, 15UL, 14UL };
    std::vector<double>         dblvec = { 0.0, 15.0, 14.0, 2.0, 1.0,
                                           12.0, 11.0, 8.0, 7.0, 6.0,
                                           5.0, 4.0, 3.0, 9.0, 10.0};
    std::vector<double>         dblvec2 = { 100.0, 101.0, 102.0, 103.0, 104.0,
                                            105.0, 106.55, 107.34, 1.8, 111.0,
                                            112.0, 113.0, 114.0, 115.0, 116.0};
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec = { "zz", "bb", "cc", "ww", "ee",
                                           "ff", "gg", "hh", "ii", "jj",
                                           "kk", "ll", "mm", "nn", "oo" };

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

    std::vector<unsigned long>  idxvec = { 1UL, 2UL, 3UL, 10UL, 5UL,
                                           7UL, 8UL, 12UL, 9UL, 12UL,
                                           10UL, 13UL, 10UL, 15UL, 14UL };
    std::vector<double>         dblvec = { 0.0, 15.0, 14.0, 15.0, 1.0,
                                           12.0, 11.0, 8.0, 15.0, 6.0,
                                           sqrt(-1), 4.0, 14.0, 14.0, 20.0};
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec = { "zz", "bb", "zz", "ww", "ee",
                                           "ff", "gg", "zz", "ii", "jj",
                                           "kk", "ll", "mm", "ee", "" };

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

    std::vector<unsigned long>  idxvec = { 1UL, 2UL, 3UL, 10UL, 5UL,
                                           7UL, 8UL, 12UL, 9UL, 12UL,
                                           10UL, 13UL, 10UL, 15UL, 14UL };
    std::vector<double>         dblvec = { 0.0, 15.0, 14.0, 2.0, 1.0,
                                           12.0, 11.0, 8.0, 7.0, 6.0,
                                           5.0, 4.0, 3.0, 9.0, 10.0};
    std::vector<double>         dblvec2 = { 100.0, 101.0, 102.0, 103.0, 104.0,
                                            105.0, 106.55, 107.34, 1.8, 111.0,
                                            112.0, 113.0, 114.0, 115.0, 116.0};
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec = { "zz", "bb", "cc", "ww", "ee",
                                           "ff", "gg", "hh", "ii", "jj",
                                           "kk", "ll", "mm", "nn", "oo" };

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

    std::vector<unsigned long>  idxvec = { 1UL, 2UL, 3UL, 10UL, 5UL,
                                           7UL, 8UL, 12UL, 9UL, 12UL,
                                           10UL, 13UL, 10UL, 15UL, 14UL };
    std::vector<double>         dblvec = { 0.0, 15.0, 14.0, 15.0, 1.0,
                                           12.0, 11.0, 8.0, 15.0, 6.0,
                                           sqrt(-1), 4.0, 14.0, 14.0, 20.0};
    std::vector<int>            intvec = { 1, 2, 3, 4, 5, 8, 6, 7, 11, 14, 9 };
    std::vector<std::string>    strvec = { "zz", "bb", "zz", "ww", "ee",
                                           "ff", "gg", "zz", "ii", "jj",
                                           "kk", "ll", "mm", "ee", "" };

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
        { 2.5, 2.45, -0.65, -0.1, -1.1, 1.87, 0.98,
          0.34, 1.56, -0.34, 2.3, -0.34, -1.9, 0.387,
          0.123, 1.06, -0.65, 2.03, 0.4, -1.0, 0.59 };
    std::vector<double>         d2 =
        { 0.2, 0.58, -0.60, -0.08, 0.05, 0.87, 0.2,
          0.4, 0.5, 0.06, 0.3, -0.34, -0.9, 0.8,
          -0.4, 0.86, 0.01, 1.02, -0.02, -1.5, 0.2 };
    std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
    MyDataFrame                 df;

    df.load_data(std::move(idx),
                 std::make_pair("asset", d1),
                 std::make_pair("benchmark", d2),
                 std::make_pair("col_3", i1));

    SharpeRatioVisitor<double>  sh_ratio;
    const auto                  result =
        df.single_act_visit<double, double>("asset", "benchmark",
                                            sh_ratio).get_result();

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

    std::vector<double> ar_equal {8, 1, 2, 3, 4, 12, 5, 6, 7, 9, 0, 11, 13,
                                  10, 15, 16, 17, 18, 19, 20, 14 };

    assert(actual_result2 == ar_equal);
    ar_equal = std::vector<double> {9, 1, 2, 3, 4, 13, 5, 6, 7, 9, 0, 11, 13,
                                    9, 15, 16, 17, 18, 19, 20, 13};
    assert(avg_result2 == ar_equal);
    ar_equal = std::vector<double> {8, 1, 2, 3, 4, 12, 5, 6, 7, 8, 0, 11, 12,
                                    8, 15, 16, 17, 18, 19, 20, 12};
    assert(first_result2 == ar_equal);
    ar_equal = std::vector<double> {10, 1, 2, 3, 4, 14, 5, 6, 7, 10, 0, 11, 14,
                                    10, 15, 16, 17, 18, 19, 20, 14};
    assert(last_result2 == ar_equal);
}

// -----------------------------------------------------------------------------

static void test_SigmiodVisitor()  {

    std::cout << "\nTesting SigmiodVisitor{  } ..." << std::endl;

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

    SigmiodVisitor<double>  sig_log(sigmiod_type::logistic);
    SigmiodVisitor<double>  sig_alg(sigmiod_type::algebraic);
    SigmiodVisitor<double>  sig_tan(sigmiod_type::hyperbolic_tan);
    SigmiodVisitor<double>  sig_atan(sigmiod_type::arc_tan);
    SigmiodVisitor<double>  sig_err(sigmiod_type::error_function);
    SigmiodVisitor<double>  sig_gud(sigmiod_type::gudermannian);
    SigmiodVisitor<double>  sig_smo(sigmiod_type::smoothstep);
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

    df1.load_column("d2_col",
                    std::move(df1.combine<double>("d1_col", df2, df3, my_max)));

    std::vector<double> result {
        1, 2, 5000, 4, 5, 6, 7, 8, 9, 10, 11, 7000, 13, 14, 15, 16, 17, 18,
        19, 20, 8000 };

    assert(df1.get_column<double>("d2_col") == result);
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
    test_SigmiodVisitor();
    test_combine();

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
