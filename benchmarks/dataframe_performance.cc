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

constexpr std::size_t   ALIGNMENT = 64;
constexpr std::size_t   SIZE = 100000000;

typedef StdDataFrame64<time_t> MyDataFrame;

// -----------------------------------------------------------------------------

using namespace std::chrono;

int main(int, char *[]) {

    const auto  first = high_resolution_clock::now();
    MyDataFrame df;

    df.load_data(
        MyDataFrame::gen_sequence_index(0, SIZE, 1),
        std::make_pair("normal", gen_normal_dist<double, ALIGNMENT>(SIZE)),
        std::make_pair("log_normal", gen_lognormal_dist<double, ALIGNMENT>(SIZE)),
        std::make_pair("exponential", gen_exponential_dist<double, ALIGNMENT>(SIZE)));

    const auto  second = high_resolution_clock::now();

    std::cout << "All data loadings are done. Calculating means ... "
              << double(duration_cast<microseconds>(second - first).count()) / 1000000.0
              << std::endl;

    MeanVisitor<double, time_t> n_mv;
    VarVisitor<double, time_t>  ln_vv;
    CorrVisitor<double, time_t> e_ln_cv;

    auto    fut1 = df.visit_async<double>("normal", n_mv);
    auto    fut2 = df.visit_async<double>("log_normal", ln_vv);
    auto    fut3 = df.visit_async<double, double>("exponential", "log_normal", e_ln_cv);

    std::cout << fut1.get().get_result() << ", "
              << fut2.get().get_result() << ", "
              << fut3.get().get_result() << std::endl;

    const auto  third = high_resolution_clock::now();

    std::cout << double(duration_cast<microseconds>(third - second).count()) / 1000000.0
              << ", "
              << double(duration_cast<microseconds>(third - first).count()) / 1000000.0
              << " All done" << std::endl;
    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
