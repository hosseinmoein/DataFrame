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

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <random>

using namespace hmdf;
using namespace std::chrono;

// A DataFrame with long type index
//
using MyDataFrame = StdDataFrame<long>;

// -----------------------------------------------------------------------------

int main(int, char *[]) {

/*
    const auto      start1 = std::chrono::high_resolution_clock::now();
    std::ofstream   stream;

    stream.open("Large_File.csv");


    std::random_device                      rd;
    std::mt19937_64                         gen(rd());
    std::uniform_real_distribution<double>  ddist;
    std::uniform_real_distribution<float>   fdist;

    stream << "INDEX:300000000:<long>,COL1:300000000:<uint>,COL2:300000000:<int>,COL3:300000000:<int>,COL4:300000000:<int>,COL5:300000000:<ulong>,COL6:300000000:<double>,COL7:300000000:<float>\n";

    std::srand(std::time(nullptr));
    for (std::size_t i = 0; i < 300'000'000; ++i)  {
        stream << i << ','
               << std::rand() << ','
               << std::rand() << ','
               << std::rand() << ','
               << std::rand() << ','
               << std::rand() << ','
               << ddist(gen) << ','
               << fdist(gen) << '\n';
    }
    stream.close();

    const auto  end1 = std::chrono::high_resolution_clock::now();

    std::cout
    << "Writing Took: "
    << double(duration_cast<microseconds>(end1 - start1).count()) / 1000000.0
    << " seconds\n";
*/

    // Now the reading
    //
    MyDataFrame df;

    const auto  start2 = std::chrono::high_resolution_clock::now();

    try  {
        df.read("Large_File.csv", io_format::csv2);
    }
    catch (const DataFrameError &ex)  {
        std::cout << ex.what() << std::endl;
    }

    const auto  end2 = std::chrono::high_resolution_clock::now();

    std::cout << "Column Length: "
              << df.get_column<double>("COL6").size() << '\n';
    std::cout
    << "Reading Took: "
    << double(duration_cast<microseconds>(end2 - start2).count()) / 1000000.0
    << " seconds\n";

/*
    df.write<long, unsigned int, int, unsigned long, double, float>
        ("Large_File.dat", io_format::binary);
*/

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
