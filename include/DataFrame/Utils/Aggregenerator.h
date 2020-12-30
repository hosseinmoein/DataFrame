// Hossein Moein
// December 29 2020
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

#pragma once

#include <cassert>
#include <iterator>
#include <tuple>

#if defined(_WIN32) && defined(HMDF_SHARED)
#  ifdef LIBRARY_EXPORTS
#    define LIBRARY_API __declspec(dllexport)
#  else
#    define LIBRARY_API __declspec(dllimport)
#  endif // LIBRARY_EXPORTS
#else
#  define LIBRARY_API
#endif // _WIN32

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename OP, typename  ... Ts>
class Aggregenerator  {

public:

    Aggregenerator() = delete;
    Aggregenerator(const Aggregenerator &that) = delete;
    Aggregenerator operator = (const Aggregenerator &rhs) = delete;

    Aggregenerator(Aggregenerator &&that) = default;
    Aggregenerator operator = (Aggregenerator &&rhs) = default;
    ~Aggregenerator() = default;

    Aggregenerator (OP &&opt, Ts& ... args) : opt_(std::move(opt))  {

        args_ = std::forward_as_tuple(args ...);

        const std::size_t   vec_s =
        	std::distance(std::get<0>(args_), std::get<1>(args_));
    }

private:

    OP                     opt_;
    std::tuple<Ts& ...>    args_;
};

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
