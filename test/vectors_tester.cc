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

#include <DataFrame/Vectors/HeteroVector.h>

#include <cassert>
#include <iostream>
#include <string>
#include <typeinfo>

using namespace hmdf;

// -----------------------------------------------------------------------------

struct my_visitor : HeteroVector::visitor_base<int, double, std::string>  {

    template<class T>
    void operator() (T &i)  { i += i; std::cout << "-- " << i << std::endl;}
};

// -----------------------------------------------------------------------------

struct sort_functor : HeteroVector::visitor_base<int, double, std::string>  {

    template<class T>
    bool operator() (const T &lhs, const T &rhs)  {

        return (lhs < rhs);
    }
};

// -----------------------------------------------------------------------------

struct change_functor : HeteroVector::visitor_base<int, double, std::string>  {

    template<typename T>
    void operator() (T &val)  {
        for (int i = 0; i < 10; ++i)
            val.push_back(
        typename std::remove_reference<decltype(val)>::type::value_type());
    }
};

// -----------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    HeteroVector    hv;
    HeteroVector    hv2;
    HeteroVector    hv3;

    const std::vector<int>  &int_vec = hv.get_vector<int>();

    hv.push_back (3);
    hv.emplace_back<int> (4);
    hv.push_back (5);
    hv.emplace<int> (int_vec.begin (), 10);
    hv.push_back (1);
    hv.push_back (0);

    hv.push_back (4.25);
    hv.push_back (5.6845);
    hv.push_back (6.1112);
    hv.push_back (1.05);
    hv.push_back (0.456783);
    hv.push_back (0.123);

    hv.push_back (std::string("str_1"));
    hv.push_back (std::string("str_2"));
    hv.push_back (std::string("str_3"));
    hv.push_back (std::string("abc"));
    hv.push_back (std::string("fas"));

    {
        // Tesing HeteroVector View

        HeteroView  v = hv.get_view<double>();

        assert(v.at<double>(3) == 1.05);

        HeteroView  s = hv.get_view<std::string>(1, 3);

        assert(s.at<std::string>(0) == "str_2");
        assert(s.size<std::string>() == 2);
        assert(s.back<std::string>() == "str_3");
    }

    {
        // Tesing HeteroVector PtrView

        HeteroPtrView   v = hv.get_ptr_view<double>();

        assert(v.at<double>(3) == 1.05);

        HeteroPtrView   s = hv.get_ptr_view<std::string>(1, 3);

        assert(s.at<std::string>(0) == "str_2");
        assert(s.size<std::string>() == 2);
        assert(s.back<std::string>() == "str_3");
    }

    assert(hv.size<std::string>() == 5);
    hv.erase<std::string>(2);
    assert(hv.size<std::string>() == 4);

    hv2 = hv;
    hv3 = std::move(hv2);

    const std::vector<double>  &dbl_vec = hv3.get_vector<double>();

    for (const auto &iter : int_vec)
        std::cout << iter << std::endl;

    std::cout << std::endl;
    for (const auto &iter : dbl_vec)
        std::cout << iter << std::endl;

    my_visitor  mv;

    std::cout << "Visiting ..." << std::endl;
    hv.visit(std::move(mv));

    sort_functor    sort_func;

    std::cout << "Sorting ..." << std::endl;
    hv.sort(std::move(sort_func));
    std::cout << "Visiting ..." << std::endl;
    hv.visit(std::move(mv));

    change_functor    change_func;

    std::cout << "Changing ..." << std::endl;
    hv.change(std::move(change_func));
    std::cout << "Visiting ..." << std::endl;
    hv.visit(std::move(mv));

    hv.resize(100, 5);
    hv.pop_back<int>();
    hv.empty<int>();
    hv.at<int>(5);
    hv.back<int>();
    hv.front<int>();

    {
        // Tesing VectorView

        std::vector<int> vec { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        VectorView<int>  vec_view;

        vec_view = vec;
        assert(vec_view.size() == 10);
        assert(vec_view[0] == 1);
        assert(vec_view[5] == 6);
        assert(vec_view[9] == 10);

        vec_view[5] = 100;
        assert(vec_view[5] == 100);

        VectorView<int>::const_iterator item = vec_view.begin();

        assert(*item == 1);
        assert(*(item++) == 1);
        assert(*item == 2);
    }

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
