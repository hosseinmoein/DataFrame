// Hossein Moein
// April 29, 2019
// Copyright (C) 2019-2021 Hossein Moein
// Distributed under the BSD Software License (see file License)

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

	std::size_t counter = 0;

#ifndef _WIN32
    for (VectorPtrView<int>::const_iterator citer = vec_view.begin();
         citer != vec_view.end(); ++citer)
        assert(*citer == ++counter);
    assert(counter == vec_view.size());
#else
    for (std::size_t i = 0; i < vec_view.size(); ++i)
        assert(vec_view[i] == ++counter);
    assert(counter == vec_view.size());
#endif // _WIN32

    counter = 0;
#ifndef _WIN32
    for (VectorPtrView<int>::iterator iter = vec_view.begin();
         iter != vec_view.end(); ++iter)
        assert(*iter == ++counter);
    assert(counter == vec_view.size());
#endif // _WIN32

    counter = 0;
#ifndef _WIN32
    for (VectorPtrView<int>::const_iterator citer = c_vec_view.begin();
         citer != c_vec_view.end(); ++citer)
        assert(*citer == ++counter);
    assert(counter == c_vec_view.size());
#else
    for (std::size_t i = 0; i < c_vec_view.size(); ++i)
        assert(c_vec_view[i] == ++counter);
    assert(counter == c_vec_view.size());
#endif // _WIN32

    VectorPtrView<int>  vec_view3(int_vec2.begin(), int_vec2.end());

    vec_view3.sort();
    counter = 0;
#ifndef _WIN32
    for (VectorPtrView<int>::const_iterator citer = vec_view3.begin();
         citer != vec_view3.end(); ++citer)
        assert(*citer == ++counter);
    assert(counter == vec_view3.size());
#else
    for (std::size_t i = 0; i < vec_view3.size(); ++i)
        assert(vec_view3[i] == ++counter);
    assert(counter == vec_view3.size());
#endif // _WIN32

    counter = int_vec2.size();
    for (std::vector<int>::const_iterator citer = int_vec2.begin();
         citer != int_vec2.end(); ++citer)
        assert(*citer == counter--);
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

    return (EXIT_SUCCESS);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
