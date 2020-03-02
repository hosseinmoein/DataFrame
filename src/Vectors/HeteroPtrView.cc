// Hossein Moein
// June 24, 2019
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

#include <DataFrame/Vectors/HeteroPtrView.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

HeteroPtrView::HeteroPtrView (const HeteroPtrView &that)  { *this = that; }
HeteroPtrView::HeteroPtrView (HeteroPtrView &&that)  { *this = that; }

// ----------------------------------------------------------------------------

HeteroPtrView &HeteroPtrView::operator= (const HeteroPtrView &rhs)  {

    if (&rhs != this)  {
        clear();
        clear_function_ = rhs.clear_function_;
        copy_function_ = rhs.copy_function_;
        move_function_ = rhs.move_function_;

        copy_function_(rhs, *this);
    }

    return (*this);
}

// ----------------------------------------------------------------------------

HeteroPtrView &HeteroPtrView::operator= (HeteroPtrView &&rhs)  {

    if (&rhs != this)  {
        clear();
        clear_function_ = std::move(rhs.clear_function_);
        copy_function_ = std::move(rhs.copy_function_);
        move_function_ = std::move(rhs.move_function_);

        move_function_(rhs, *this);
    }

    return (*this);
}

// ----------------------------------------------------------------------------

void HeteroPtrView::clear()  {

    clear_function_(*this);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End
