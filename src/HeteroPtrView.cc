// Hossein Moein
// June 24, 2019
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

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
