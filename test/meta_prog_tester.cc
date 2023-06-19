// Hossein Moein
// June 07 2023
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

#include <DataFrame/Utils/MetaProg.h>

#include <cassert>
#include <iostream>
#include <list>
#include <set>
#include <vector>
#include <string>

using namespace hmdf;

// ----------------------------------------------------------------------------

static void test_for_each_chain ()  {

    std::cout << "Testing for_each_chain ...\n" << std::endl;

    std::vector<int>        iv1 { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::list<int>          il1 { 10, 20, 30, 40, 50, 60, 70 };
    std::vector<double>     dv1
        { 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9, 10.1, 100.5 };
    std::set<std::string>   ss1 { "Q", "D", "J", "B", "T", "A", "Z", "S", "V" };

    for_each_chain([](const auto &val) -> void  { std::cout << val << ", "; },
                   iv1.begin(), iv1.end(),
                   il1.begin(), il1.end(),
                   dv1.begin(), dv1.end(),
                   ss1.begin(), ss1.end());
    std::cout << std::endl;
}

// ----------------------------------------------------------------------------

static void test_for_each_list ()  {

    std::cout << "Testing for_each_list ...\n" << std::endl;

    std::vector<int>        iv1 { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::list<int>          il1 { 10, 20, 30, 40, 50, 60, 70 };
    std::vector<double>     dv1
        { 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9, 10.1, 100.5 };
    std::set<std::string>   ss1 { "Q", "D", "J", "B", "T", "A", "Z", "S", "V" };

    for_each_list2(iv1.begin(), iv1.end(),
                   il1.begin(), il1.end(),
                   [](const auto &val1, auto &val2) -> void  {
                       val2 += 1000;
                       std::cout << val1 << ", " << val2
                                 << std::endl;
                   });
    std::cout << std::endl;

    for_each_list3(iv1.begin(), iv1.end(),
                   il1.begin(), il1.end(),
                   dv1.begin(), dv1.end(),
                   [](const auto &val1, const auto &val2,
                      const auto &val3) -> void  {
                       std::cout << val1 << ", " << val2
                                 << ", " << val3
                                 << std::endl;
                   });
    std::cout << std::endl;

    for_each_list4(iv1.begin(), iv1.end(),
                   il1.begin(), il1.end(),
                   dv1.begin(), dv1.end(),
                   ss1.begin(), ss1.end(),
                   [](const auto &val1, const auto &val2,
                      const auto &val3, const auto &val4) -> void  {
                       std::cout << val1 << ", " << val2
                                 << ", " << val3 << ", " << val4
                                 << std::endl;
                   });
    std::cout << std::endl;

    for_each_list5(iv1.begin(), iv1.end(),
                   il1.begin(), il1.end(),
                   dv1.begin(), dv1.end(),
                   ss1.begin(), ss1.end(),
                   ss1.begin(), ss1.end(),
                   [](const auto &val1, const auto &val2,
                      const auto &val3, const auto &val4,
                      const auto &val5) -> void  {
                       std::cout << val1 << ", " << val2
                                 << ", " << val3 << ", " << val4
                                 << ", " << val5
                                 << std::endl;
                   });
    std::cout << std::endl;
}

// ----------------------------------------------------------------------------

static void test_list_size ()  {

    std::cout << "Testing list_size ...\n" << std::endl;

    struct  MyData  {
        int         i { 10 };
        double      d { 5.5 };
        std::string s { "Some Arbitrary String" };

        MyData() = default;
    };

    assert(
        (list_size_v<std::tuple<const int, double, std::string, MyData>> == 4));
}

// ----------------------------------------------------------------------------

static void test_tuple_all_of ()  {

    std::cout << "Testing tuple_all_of ...\n" << std::endl;

    const auto  tup = std::make_tuple(23, 5.6, 4.3f, short(56));
    const bool  smaller =
        tuple_all_of(tup, [](const auto &val) -> bool { return(val < 85.5); });
    const bool  bigger =
        tuple_all_of(tup, [](const auto &val) -> bool { return(val < 24); });

    assert(smaller == true);
    assert(bigger == false);
}

// ----------------------------------------------------------------------------

int main (int, char *[])  {

    test_for_each_chain();
    test_for_each_list ();
    test_list_size();
    test_tuple_all_of();

    return (EXIT_SUCCESS);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
