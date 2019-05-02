// Hossein Moein
// April 29, 2019
// Copyright (C) 2019-2021 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <iostream>
#include <cassert>

#include <VectorPtrView.h>

using namespace hmdf;

//-----------------------------------------------------------------------------

int main (int argCnt, char *argVctr [])  {

    std::vector<int>    int_vec = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    VectorPtrView<int>  vec_view(int_vec);

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

    for (VectorPtrView<int>::const_iterator citer = vec_view.begin();
         citer != vec_view.end(); ++citer)
        assert(*citer == ++counter);
    assert(counter == vec_view.size());

    return (EXIT_SUCCESS);
}

//-----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
