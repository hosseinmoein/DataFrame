// Hossein Moein
// September 13, 2017
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

// ----------------------------------------------------------------------------

namespace hmdf
{

HeteroVector::HeteroVector ()  {

    clear_functions_.reserve(2);
    copy_functions_.reserve(2);
    move_functions_.reserve(2);
}

// ----------------------------------------------------------------------------

HeteroVector::HeteroVector (const HeteroVector &that)  { *this = that; }
HeteroVector::HeteroVector (HeteroVector &&that)  { *this = that; }

// ----------------------------------------------------------------------------

HeteroVector &HeteroVector::operator= (const HeteroVector &rhs)  {

    if (&rhs != this)  {
        clear();
        clear_functions_ = rhs.clear_functions_;
        copy_functions_ = rhs.copy_functions_;
        move_functions_ = rhs.move_functions_;

        for (auto &&copy_function : copy_functions_)
            copy_function(rhs, *this);
    }

    return (*this);
}

// ----------------------------------------------------------------------------

HeteroVector &HeteroVector::operator= (HeteroVector &&rhs)  {

    if (&rhs != this)  {
        clear();
        clear_functions_ = std::move(rhs.clear_functions_);
        copy_functions_ = std::move(rhs.copy_functions_);
        move_functions_ = std::move(rhs.move_functions_);

        for (auto &&move_function : move_functions_)
            move_function(rhs, *this);
    }

    return (*this);
}

// ----------------------------------------------------------------------------

void HeteroVector::clear()  {

    for (auto &&clear_func : clear_functions_)
        clear_func (*this);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End
