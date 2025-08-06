// Hossein Moein
// August 1, 2025
/*
Copyright (c) 2019-2026, Hossein Moein
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

#include <DataFrame/Internals/DataFrame_standalone.tcc>
#include <DataFrame/Utils/DateTime.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <algorithm>
#include <ios>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

namespace hmdf
{

inline static const char *
_get_horz_rule_(std::size_t num)  {

    static constexpr char           horzrule[] =
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________"
        "___________________________________________________________________";
    static constexpr std::size_t    hr_size { sizeof(horzrule) - 1 };

    return (horzrule + (hr_size - std::min(num, hr_size)));
}

// ----------------------------------------------------------------------------

inline static const char *
_get_space_(std::size_t num)  {

    static constexpr char           space[] =
        "                                                                   "
        "                                                                   "
        "                                                                   "
        "                                                                   "
        "                                                                   "
        "                                                                   "
        "                                                                   "
        "                                                                   "
        "                                                                   "
        "                                                                   "
        "                                                                   ";
    static constexpr std::size_t    s_size { sizeof(space) - 1 };

    return (space + (s_size - std::min(num, s_size)));
}

// ----------------------------------------------------------------------------

inline static std::size_t
_get_max_string_len_(const std::vector<std::string> &strings)  {

    std::size_t result { 0 };

    for (const auto &str : strings)
        result = std::max(result, str.size());
    return (result);
}

// ----------------------------------------------------------------------------

inline static std::vector<std::size_t>
_get_max_string_len_(const std::vector<std::vector<std::string>> &vecs)  {

    std::vector<std::size_t>    result;

    result.reserve(vecs.size());
    for (const auto &vec : vecs)
        result.push_back(_get_max_string_len_(vec));
    return (result);
}

// ----------------------------------------------------------------------------

template<typename V>
inline static std::vector<std::string>
_stringfy_(const V &vec,
           DT_FORMAT dt_format,
           long start_row,
           long end_row,
           std::streamsize precision)  {

    using value_type = typename V::value_type;

    std::vector<std::string>    result;
    const auto                  cc_size =
        end_row > start_row ? end_row - start_row : 0;

    result.reserve(cc_size <= long(vec.size()) ? cc_size : long(vec.size()));
    if constexpr (std::is_same_v<value_type, DateTime>)  {
        String128   buffer;

        for (long i { start_row }; i < end_row; ++i)  {
            vec[i].date_to_str(dt_format, buffer);
            result.push_back(buffer.c_str());
        }
    }
    else  {
        std::stringstream   ss;

        ss << std::fixed << std::setprecision(precision);
        std::transform(std::begin(vec) + start_row, std::begin(vec) + end_row,
                       std::back_inserter(result),
                       [&ss](const auto &val) -> std::string  {
                           ss.clear();
                           ss.seekp(0);
                           ss << val;
                           return (ss.str());
                       });
    }
    return (result);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
