// Hossein Moein
// September 13, 2017
// To the extent possible under law, the author(s) have dedicated all
// copyright and related and neighboring rights to this software to
// the public domain worldwide. This software is distributed without
// any warranty.

#include <BaseContainer.h>

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
