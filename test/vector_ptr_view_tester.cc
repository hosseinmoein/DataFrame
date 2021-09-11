// Hossein Moein
// April 29, 2019
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

#include <DataFrame/Vectors/VectorPtrView.h>

#include <cassert>
#include <iostream>

using namespace hmdf;

// ----------------------------------------------------------------------------

int main (int argCnt, char *argVctr [])  {

    std::vector<int>            int_vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    std::vector<int>            int_vec2 = { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
    VectorPtrView<int>          vec_view(int_vec);
    const VectorPtrView<int>    c_vec_view = int_vec;

    assert(vec_view.size() == int_vec.size());

    VectorPtrView<int>  vec_view2(int_vec.begin(), int_vec.end());

    assert(vec_view2.size() == vec_view.size());
    assert(vec_view2[2] == vec_view[2]);
    assert(vec_view2[9] == vec_view[9]);
    assert(vec_view2[2] == 3);
    assert(vec_view2[9] == 10);
    assert(vec_view2.front() == 1);
    assert(vec_view.back() == 10);

    int counter = 0;

#ifndef _MSC_VER
    for (VectorPtrView<int>::const_iterator citer = vec_view.begin();
         citer != vec_view.end(); ++citer)
        assert(*citer == ++counter);
    assert(std::size_t(counter) == vec_view.size());
#else
    for (std::size_t i = 0; i < vec_view.size(); ++i)
        assert(vec_view[i] == ++counter);
    assert(std::size_t(counter) == vec_view.size());
#endif // !_MSC_VER

    counter = 0;
#ifndef _MSC_VER
    for (VectorPtrView<int>::iterator iter = vec_view.begin();
         iter != vec_view.end(); ++iter)
        assert(*iter == ++counter);
    assert(std::size_t(counter) == vec_view.size());
#endif // !_MSC_VER

    counter = 0;
#ifndef _MSC_VER
    for (VectorPtrView<int>::const_iterator citer = c_vec_view.begin();
         citer != c_vec_view.end(); ++citer)
        assert(*citer == ++counter);
    assert(std::size_t(counter) == c_vec_view.size());
#else
    for (std::size_t i = 0; i < c_vec_view.size(); ++i)
        assert(c_vec_view[i] == ++counter);
    assert(std::size_t(counter) == c_vec_view.size());
#endif // !_MSC_VER

    VectorPtrView<int>  vec_view3(int_vec2.begin(), int_vec2.end());

    vec_view3.sort();
    counter = 0;
#ifndef _MSC_VER
    for (VectorPtrView<int>::const_iterator citer = vec_view3.begin();
         citer != vec_view3.end(); ++citer)
        assert(*citer == ++counter);
    assert(std::size_t(counter) == vec_view3.size());
#else
    for (std::size_t i = 0; i < vec_view3.size(); ++i)
        assert(vec_view3[i] == ++counter);
    assert(std::size_t(counter) == vec_view3.size());
#endif // !_MSC_VER

    counter = int_vec2.size();
    for (std::vector<int>::const_iterator citer = int_vec2.begin();
         citer != int_vec2.end(); ++citer)  {
        assert(*citer == counter);
        counter -= 1;
    }
    assert(counter == 0);

    vec_view2.push_back(&(int_vec[3]));
    assert(vec_view2.back() == 4);

    vec_view2.erase(vec_view2.size() - 1);
    assert(vec_view2.back() == 10);

    vec_view2.insert(3, &(int_vec2[0]));
    assert(vec_view2[3] == 10);
    assert(vec_view2[2] == 3);
    assert(vec_view2[4] == 4);

    vec_view2.insert(vec_view2.size(), int_vec2.begin(), int_vec2.end());
    assert(vec_view2.size() == int_vec2.size() * 2 + 1);
    assert(vec_view2[3] == 10);
    assert(vec_view2[2] == 3);
    assert(vec_view2[4] == 4);
    assert(vec_view2[20] == 1);
    assert(vec_view2[19] == 2);

    VectorPtrView<int>::const_iterator item = vec_view.begin();

    assert(*item == 1);
    assert(*(item++) == 1);
    assert(*item == 2);

    std::vector<int>    ivec { 0, 1, 2, 3 };
    VectorPtrView<int>  vpw = ivec;

    assert(vpw.size() == 4);

    const std::vector<int>  civec { 0, 1, 2, 3 };
    VectorConstPtrView<int> cvpw = civec;

    assert(cvpw.size() == 4);

    return (EXIT_SUCCESS);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
