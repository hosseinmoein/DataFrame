// Hossein Moein
// May 14, 2024
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

#include <DataFrame/DataFrameTypes.h>

#include <bit>
#include <climits>
#include <cstdint>

// ----------------------------------------------------------------------------

namespace hmdf
{

enum class  endians : unsigned char  {

    little_endian = 1,
    big_endian = 2,
    mixed_endian = 3,
};

// ----------------------------------------------------------------------------

template<typename T, std::size_t S>
struct  SwapBytes  {

    inline T operator()(T) const {

        throw NotImplemented("SwapBytes: Type/Size");
    }
};

// Specialisations
//
template<typename T>
struct  SwapBytes<T, 1>  {

    inline T operator()(T value) const  { return (value); }
};

template<typename T>
struct  SwapBytes<T, 2>  {

    inline T operator()(T value) const  {

        return (((value >> 8) & 0xff) |
                ((value & 0xff) << 8));
    }
};

template<typename T>
struct  SwapBytes<T, 4>  {

    inline T operator()(T value) const  {

        return (((value & 0xff000000) >> 24) |
                ((value & 0x00ff0000) >> 8)  |
                ((value & 0x0000ff00) << 8)  |
                ((value & 0x000000ff) << 24));
    }
};

template<>
struct  SwapBytes<float, 4>  {

    inline float operator()(float value) const  {

        const uint32_t  layout =
            SwapBytes<uint32_t, sizeof(uint32_t)>{ }(*((uint32_t *) &value));

        return (*((float *) &layout));

    }
};

template<typename T>
struct  SwapBytes<T, 8>  {

    inline T operator()(T value) const  {

        return (((value & 0xff00000000000000ULL) >> 56) |
                ((value & 0x00ff000000000000ULL) >> 40) |
                ((value & 0x0000ff0000000000ULL) >> 24) |
                ((value & 0x000000ff00000000ULL) >> 8)  |
                ((value & 0x00000000ff000000ULL) << 8)  |
                ((value & 0x0000000000ff0000ULL) << 24) |
                ((value & 0x000000000000ff00ULL) << 40) |
                ((value & 0x00000000000000ffULL) << 56));
    }
};

template<>
struct  SwapBytes<double, 8>  {

    inline double operator()(double value) const  {

        const uint64_t  layout =
            SwapBytes<uint64_t, sizeof(uint64_t)>{ }(*((uint64_t *) &value));

        return (*((double *) &layout));

    }
};

/*
template<typename T>
struct  SwapBytes<T, 16>  {

    inline T operator()(T value) const  {

        return (((value & 0xff000000000000000000000000000000ULL) >> 120) |
                ((value & 0x00ff0000000000000000000000000000ULL) >> 104) |
                ((value & 0x0000ff00000000000000000000000000ULL) >> 88)  |
                ((value & 0x000000ff000000000000000000000000ULL) >> 72)  |
                ((value & 0x00000000ff0000000000000000000000ULL) >> 56)  |
                ((value & 0x0000000000ff00000000000000000000ULL) >> 40)  |
                ((value & 0x000000000000ff000000000000000000ULL) >> 24)  |
                ((value & 0x00000000000000ff0000000000000000ULL) >> 8)   |
                ((value & 0x0000000000000000ff00000000000000ULL) << 8)   |
                ((value & 0x000000000000000000ff000000000000ULL) << 24)  |
                ((value & 0x00000000000000000000ff0000000000ULL) << 40)  |
                ((value & 0x0000000000000000000000ff00000000ULL) << 56)  |
                ((value & 0x000000000000000000000000ff000000ULL) << 72)  |
                ((value & 0x00000000000000000000000000ff0000ULL) << 88)  |
                ((value & 0x0000000000000000000000000000ff00ULL) << 104) |
                ((value & 0x000000000000000000000000000000ffULL) << 120));
    }
};

template<>
struct  SwapBytes<long double, 16>  {

    inline long double operator()(long double value) const  {

        const uint128_t layout =
            SwapBytes<uint128_t, sizeof(uint128_t)>{ }(*((uint128_t *) &value));

        return (*((long double *) &layout));

    }
};
*/

// ----------------------------------------------------------------------------

static inline
constexpr endians get_system_endian()  {

    if constexpr (std::endian::native == std::endian::big)
        return (endians::big_endian);
    else if constexpr (std::endian::native == std::endian::little)
        return (endians::little_endian);
    else
        return (endians::mixed_endian);
}

// ----------------------------------------------------------------------------

template<typename V>
static inline
void flip_endianness(V &vec)  {

    using VecType = typename std::remove_reference<V>::type;
    using ValueType = typename VecType::value_type;

    if constexpr (sizeof(ValueType) < 2)  return;

    const SwapBytes<ValueType, sizeof(ValueType)>   swaper { };

    for (auto &val : vec)  val = swaper(val);
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
