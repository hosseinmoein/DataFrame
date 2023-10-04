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

#include <DataFrame/Utils/AlignedAllocator.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <vector>

using namespace hmdf;

struct MyData  {

    MyData()  { std::cout << "Printing from constructor" << std::endl;  }
    ~MyData()  { std::cout << "Printing from destructor" << std::endl; }

    int i { 101 };
};

// -----------------------------------------------------------------------------

static void test_aligned_allocator()  {

    std::cout << "\nTesting AlignedAllocator ..." << std::endl;

    const std::size_t   NUM = 5;

    {
        AlignedAllocator<double>        a1;
        AlignedAllocator<int>           a2;
        AlignedAllocator<double, 512>   a3;
        AlignedAllocator<int, 512>      a4;

        std::cout << "ai1 value: " << std::size_t(a1.align_value) << std::endl;
        std::cout << "ai2 value: " << std::size_t(a2.align_value) << std::endl;
        std::cout << "ai3 value: " << std::size_t(a3.align_value) << std::endl;
        std::cout << "ai4 value: " << std::size_t(a4.align_value) << std::endl;
    }
    {
        std::vector<double, AlignedAllocator<double, 512>>  vec;

        vec.reserve(NUM - 1);
        for (std::size_t i = 0; i < NUM; ++i)
            vec.push_back(double(i));
        for (auto citer : vec)
            std::cout << citer << ", ";
        std::cout << std::endl;
    }
    {
        std::vector<int, AlignedAllocator<int, 64>> vec;

        vec.reserve(NUM - 1);
        for (std::size_t i = 0; i < NUM; ++i)
            vec.push_back(int(i));
        for (auto citer : vec)
            std::cout << citer << ", ";
        std::cout << std::endl;
    }
    {
        std::vector<int, AlignedAllocator<int>> vec;

        vec.reserve(NUM - 1);
        for (std::size_t i = 0; i < NUM; ++i)
            vec.push_back(int(i));
        for (auto citer : vec)
            std::cout << citer << ", ";
        std::cout << std::endl;
    }
    {
        std::vector<int, AlignedAllocator<int, 1024>>   vec;

        vec.reserve(NUM - 1);
        for (std::size_t i = 0; i < NUM; ++i)
            vec.push_back(int(i + 1024));
        for (auto citer : vec)
            std::cout << citer << ", ";
        std::cout << std::endl;
    }
    {
        std::vector<char, AlignedAllocator<char, 512>>  vec;

        vec.reserve(NUM - 1);
        for (std::size_t i = 101; i < 101 + NUM; ++i)
            vec.push_back(char(i));
        for (auto citer : vec)
            std::cout << citer << ", ";
        std::cout << std::endl;
    }
    {
        std::vector<long double, AlignedAllocator<long double, 512>>    vec;

        vec.reserve(NUM);
        for (std::size_t i = 0; i < NUM; ++i)
            vec.push_back(static_cast<long double>(i));
        for (auto citer : vec)
            std::cout << citer << ", ";
        std::cout << std::endl;
    }
    {
        std::vector<MyData, AlignedAllocator<MyData, 512>>  vec(NUM);

        vec[1].i = 1001;
        vec[3].i = 3001;
        for (auto &citer : vec)
            std::cout << citer.i << ", ";
        std::cout << std::endl;
    }
    {
        std::vector<int, AlignedAllocator<int, 1024>>   vec1;
        std::vector<int, AlignedAllocator<int>>         vec2;

        vec1.reserve(NUM);
        for (std::size_t i = 0; i < NUM; ++i)  {
            vec1.push_back(int(i));
            vec2.push_back(int(i));
        }
        for (std::size_t i = 0; i < NUM; ++i)
            assert(vec1[i] == vec2[i]);
    }

    // This must fail to compile
    //
    /*
    {
        std::vector<long double, AlignedAllocator<long double, 8>>  vec;

        vec.reserve(NUM);
        for (std::size_t i = 0; i < NUM; ++i)
            vec.push_back(static_cast<long double>(i));
        for (auto citer : vec)
            std::cout << citer << ", ";
        std::cout << std::endl;
    }
    */
}

// -----------------------------------------------------------------------------

int main(int, char *[]) {

    test_aligned_allocator();

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
