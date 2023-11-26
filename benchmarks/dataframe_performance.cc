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
#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/RandGen.h>

#include <chrono>
#include <iostream>

using namespace hmdf;
using namespace std::chrono;

constexpr std::size_t   ALIGNMENT = 64;
// constexpr std::size_t   SIZE = 300000000;
constexpr std::size_t   SIZE = 10000000;

typedef StdDataFrame64<time_t> MyDataFrame;

// -----------------------------------------------------------------------------

int main(int, char *[]) {

    const auto  first = high_resolution_clock::now();
    MyDataFrame df;

    df.load_data(
        MyDataFrame::gen_sequence_index(0, SIZE, 1),
        std::make_pair("normal", gen_normal_dist<double, ALIGNMENT>(SIZE)),
        std::make_pair("log_normal", gen_lognormal_dist<double, ALIGNMENT>(SIZE)),
        std::make_pair("exponential", gen_exponential_dist<double, ALIGNMENT>(SIZE)));

    const auto  second = high_resolution_clock::now();

    std::cout << "Data generation/load time: "
              << double(duration_cast<microseconds>(second - first).count()) / 1000000.0
              << " secs" << std::endl;

    MeanVisitor<double, time_t> n_mv;
    VarVisitor<double, time_t>  ln_vv;
    CorrVisitor<double, time_t> e_ln_cv;

    auto    mean = df.visit_async<double>("normal", n_mv);
    auto    var = df.visit_async<double>("log_normal", ln_vv);
    auto    corr = df.visit_async<double, double>("exponential", "log_normal", e_ln_cv);

    std::cout << mean.get().get_result() << ", "
              << var.get().get_result() << ", "
              << corr.get().get_result() << std::endl;

    const auto  third = high_resolution_clock::now();
    auto        functor = [](const auto &, const double &val)-> bool { return (val > 8); };
    const auto  df2 =
        df.get_view_by_sel<double, decltype(functor), double>("log_normal", functor);

    std::cout << "Number of rows after select: "
              << df2.get_column<double>("normal").size() << std::endl;

    const auto  fourth = high_resolution_clock::now();

    // df.sort<double, double, double>("log_normal", sort_spec::ascen,
    //                                 "exponential", sort_spec::ascen);
    // std::cout << "1001th value in normal column: "
    //           << df.get_column<double>("normal")[1001] << std::endl;

    const auto  fifth = high_resolution_clock::now();

    std::cout << "Calculation time: "
              << double(duration_cast<microseconds>(third - second).count()) / 1000000.0
              << " secs\n"
              << "Selection time: "
              << double(duration_cast<microseconds>(fourth - third).count()) / 1000000.0
              << " secs\n"
              // << "Sorting time: "
              // << double(duration_cast<microseconds>(fifth - fourth).count()) / 1000000.0
              // << " secs\n"
              << "Overall time: "
              << double(duration_cast<microseconds>(fifth - first).count()) / 1000000.0
              << " secs"
              << std::endl;
    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
