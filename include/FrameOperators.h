// Hossein Moein
// October 28, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#include <vector>
#include "VectorView.h"

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename T>
std::vector<T> &operator= (std::vector<T> &lhs, const VectorView<T> &rhs)  {

    return (lhs.swap(std::vector<T>(rhs.begin(), rhs.end())));
}

// ----------------------------------------------------------------------------

template<typename T>
VectorView<T> &operator= (VectorView<T> &lhs, const std::vector<T> &rhs)  {

    return (lhs.swap(VectorView<T>(&(rhs.begin()), &(rhs.end()))));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
