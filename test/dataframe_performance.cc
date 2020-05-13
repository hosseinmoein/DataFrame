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

    MyDataFrame             df;
    const size_t            index_sz =
        df.load_index(
            MyDataFrame::gen_datetime_index("01/01/1970",
                                            "08/15/2019",
                                            time_frequency::secondly,
                                            1));
    RandGenParams<double>   p;

    p.mean = 1.0;  // Default
    p.std = 0.005;

    df.load_column("normal", gen_normal_dist<double>(index_sz, p));
    df.load_column("log_normal", gen_lognormal_dist<double>(index_sz));
    p.lambda = 1.5;
    df.load_column("exponential", gen_exponential_dist<double>(index_sz, p));

    std::cout << "All memory allocations are done. Calculating means ..."
              << std::endl;

    MeanVisitor<double, time_t> n_mv;
    MeanVisitor<double, time_t> ln_mv;
    MeanVisitor<double, time_t> e_mv;

    auto    fut1 = df.visit_async<double>("normal", n_mv);
    auto    fut2 = df.visit_async<double>("log_normal", ln_mv);
    auto    fut3 = df.visit_async<double>("exponential", e_mv);

    fut1.get();
    fut2.get();
    fut3.get();
    // std::cout << "Normal mean " << n_mv.get_result() << std::endl;
    // std::cout << "Log Normal mean " << ln_mv.get_result() << std::endl;
    // std::cout << "Exponential mean " << e_mv.get_result() << std::endl;
    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
