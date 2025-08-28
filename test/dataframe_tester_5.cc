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

int main(int, char *[]) {

    MyDataFrame::set_optimum_thread_level();

    test_permutation_vec();

    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
