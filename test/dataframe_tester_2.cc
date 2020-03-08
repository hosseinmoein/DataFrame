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

#include <cassert>
#include <iostream>

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

int main(int argc, char *argv[]) {

    test_get_reindexed();
    test_get_reindexed_view();

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
