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
#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/RandGen.h>

#include <iostream>

using namespace hmdf;

typedef StdDataFrame<time_t> MyDataFrame;

// -----------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    std::cout << "Starting ... " << time(nullptr) << std::endl;

    MyDataFrame df;
    auto        index_vec =
        MyDataFrame::gen_datetime_index("01/01/1970", "08/15/2019", time_frequency::secondly, 1);
    const auto  index_sz = index_vec.size();

    df.load_data(std::move(index_vec),
                 std::make_pair("normal", gen_normal_dist<double>(index_sz)),
                 std::make_pair("log_normal", gen_lognormal_dist<double>(index_sz)),
                 std::make_pair("exponential", gen_exponential_dist<double>(index_sz)));

    std::cout << "All memory allocations are done. Calculating means ... " << time(nullptr)<< std::endl;

    MeanVisitor<double, time_t> n_mv;
    MeanVisitor<double, time_t> ln_mv;
    MeanVisitor<double, time_t> e_mv;

    auto    fut1 = df.visit_async<double>("normal", n_mv);
    auto    fut2 = df.visit_async<double>("log_normal", ln_mv);
    auto    fut3 = df.visit_async<double>("exponential", e_mv);

    std::cout << fut1.get().get_result() << ", "
              << fut2.get().get_result() << ", "
              << fut3.get().get_result() << std::endl;
    std::cout << time(nullptr) << " ... Done" << std::endl;
    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
