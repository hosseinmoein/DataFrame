// Hossein Moein
// October 25, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/Vectors/HeteroView.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

HeteroView::HeteroView (const HeteroView &that)  { *this = that; }
HeteroView::HeteroView (HeteroView &&that)  { *this = that; }

// ----------------------------------------------------------------------------

HeteroView &HeteroView::operator= (const HeteroView &rhs)  {

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

HeteroView &HeteroView::operator= (HeteroView &&rhs)  {

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

void HeteroView::clear()  {

    clear_function_(*this);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End
