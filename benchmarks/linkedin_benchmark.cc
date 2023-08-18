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
#include <cstdint>
#include <iostream>

using namespace hmdf;

// Number of data points
//
constexpr std::size_t   DATA_SIZE = 100000000;

// A DataFrame with time_t type index
//
using MyDataFrame = StdDataFrame<time_t>;

// -----------------------------------------------------------------------------

// The visitor implementing the benchmark logic
//
template<arithmetic T, typename I>
struct  MyVisitor  {

    DEFINE_VISIT_BASIC_TYPES_2

    inline void operator() (const I &, const T &val)  {

        sum_ += std::labs(val - 2);
    }
    PASS_DATA_ONE_BY_ONE


    inline void pre ()  { sum_ = 0; }
    inline result_type get_result () const  { return (sum_); }
    inline void post ()  {  }

    MyVisitor() = default;

private:

    result_type sum_ { 0 };
};

// -----------------------------------------------------------------------------

int main(int, char *[]) {

    const auto              start = std::chrono::high_resolution_clock::now();
    auto                    index_vec = MyDataFrame::gen_sequence_index(0, DATA_SIZE, 1);
    MyDataFrame             df;
    RandGenParams<int64_t>  param;

    param.min_value = -10000000;
    param.max_value = 10000000;
    df.load_data(std::move(index_vec),
                 std::make_pair("uniform", gen_uniform_int_dist<int64_t>(DATA_SIZE, param)));

    const auto  middle = std::chrono::high_resolution_clock::now();

    MyVisitor<int64_t, time_t>  my_visit;

    df.visit<int64_t>("uniform", my_visit);

    const auto  last = std::chrono::high_resolution_clock::now();

    std::cout << "Result: " << my_visit.get_result() << '\n'
              << "Calculation Time: "
              << std::chrono::duration_cast<std::chrono::duration<double>>(last - middle).count() << '\n'
              << "Overall Time: "
              << std::chrono::duration_cast<std::chrono::duration<double>>(last - start).count()
              << std::endl;
    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
