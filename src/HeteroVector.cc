// Hossein Moein
// September 13, 2017
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

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
