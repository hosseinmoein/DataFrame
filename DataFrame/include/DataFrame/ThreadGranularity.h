// Hossein Moein
// November 27, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#ifndef HMDF_THREADGRANULARITY_HPP
#define HMDF_THREADGRANULARITY_HPP

#include "DataFrame_lib_exports.h"
// ----------------------------------------------------------------------------

namespace hmdf
{

struct HMDF_DLL_API ThreadGranularity  {

    static inline void
    set_thread_level(unsigned int n)  { num_of_threads_ = n; }
    static inline unsigned int
    get_thread_level()  { return (num_of_threads_); }

protected:

    ThreadGranularity() = default;

private:

    static unsigned int num_of_threads_;
};

} // namespace hmdf

#endif //HMDF_THREADGRANULARITY_HPP

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
