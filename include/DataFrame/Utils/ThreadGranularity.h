// Hossein Moein
// November 27, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

// ----------------------------------------------------------------------------

namespace hmdf
{

struct ThreadGranularity  {

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

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
