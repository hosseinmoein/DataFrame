// Hossein Moein
// July 17 2009
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

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>

// ----------------------------------------------------------------------------

namespace hmdf
{

#define snprintf_nowarn(...) (::snprintf(__VA_ARGS__) < 0 ? abort() : (void)0)

// This abstract base class makes it possible to pass different template
// instances around as one type and to be able to assign them interchangeably.
// The only penalty paid for having this base class is to carry around two
// additional (pointer size) members. There shouldn't be any performance
// penalty, since everything is still stack based and there is no virtuality.
//
// NOTE: VirtualString MAKES NO BOUNDARY CHECKS. IT IS THE RESPONSIBILITY
//       OF THE PROGRAMMER TO TAKE CARE OF THAT.
//

class   VirtualString {

public:

    using size_type = std::size_t;
    using value_type = char;
    using pointer = value_type *;
    using const_pointer = const value_type *;
    using reference = value_type &;
    using const_reference = const value_type &;
    using iterator = pointer;
    using const_iterator = const_pointer;

    inline static const size_type  npos = static_cast<size_type>(-1);

    [[nodiscard]] inline iterator begin() noexcept  { return (string_); }
    [[nodiscard]] inline const_iterator
    begin() const noexcept  { return (string_); }
    [[nodiscard]] inline iterator end() noexcept { return (string_ + size()); }
    [[nodiscard]] inline const_iterator
    end() const noexcept  { return (string_ + size()); }

    VirtualString() = delete;
    VirtualString(const VirtualString &) = delete;
    VirtualString(VirtualString &&) = delete;
    VirtualString &operator =(VirtualString &&) = delete;

    // Assignment methods.
    //
    inline VirtualString &operator =(const_pointer rhs) noexcept  {

        ::strcpy(string_, rhs);
        _forced_size();
        return (*this);
    }
    inline VirtualString &operator =(const VirtualString &rhs) noexcept  {

        return (*this = rhs.c_str());
    }
    [[nodiscard]] inline VirtualString &
    ncopy(const_pointer rhs, size_type len) noexcept  {

        snprintf_nowarn(string_, len, "%s", rhs);
        _forced_size();
        return (*this);
    }

    //
    // Appending methods.
    //

    inline VirtualString &append(const_pointer rhs) noexcept  {

        const size_type rhs_len { ::strlen(rhs) };

        std::memcpy(string_ + size_, rhs, rhs_len + 1);  // +1 copies the NULL
        size_ += rhs_len;
        return (*this);
    }
    inline VirtualString &append(const VirtualString &rhs) noexcept  {

        return (append(rhs.c_str()));
    }
    inline VirtualString &operator +=(const_pointer rhs) noexcept  {

        return (append(rhs));
    }
    inline VirtualString &operator +=(const VirtualString &rhs) noexcept  {

        return (append(rhs.c_str()));
    }

    [[nodiscard]] inline size_type
    find(const_reference token, size_type pos = 0) const noexcept  {

        const auto  ret { ::strchr(string_ + pos, token) };

        return (ret != nullptr ? static_cast<size_type>(ret - string_) : npos);
    }
    [[nodiscard]] inline size_type
    find(const_pointer token, size_type pos = 0) const noexcept  {

        const auto  ret { ::strstr(string_ + pos, token) };

        return (ret != nullptr ? static_cast<size_type>(ret - string_) : npos);
    }
    [[nodiscard]] inline size_type
    find(const VirtualString &token, size_type pos = 0) const noexcept  {

        return (find(token.c_str(), pos));
    }

    inline int printf(const char *format_str, ...) noexcept  {

        va_list argument_ptr;

        va_start(argument_ptr, format_str);

        const int   ret {
            ::vsnprintf(string_, 1000000, format_str, argument_ptr)
        };

        va_end(argument_ptr);
        if (ret >= 0)  size_ = static_cast<size_type>(ret);
        else  _forced_size();
        return (ret);
    }

    inline int append_printf(const char *format_str, ...) noexcept  {

        va_list argument_ptr;

        va_start(argument_ptr, format_str);

        const int   ret {
            ::vsnprintf(string_ + size(), 1000000, format_str, argument_ptr)
        };

        va_end(argument_ptr);
        if (ret >= 0)  size_ = static_cast<size_type>(ret);
        else  _forced_size();
        return (ret);
    }

    // Comparison methods.
    //
    [[nodiscard]] inline int compare(const_pointer rhs) const noexcept  {

        return (::strcmp(string_, rhs));
    }
    [[nodiscard]] inline int
    compare(const VirtualString &rhs) const noexcept  {

        return (compare(rhs.c_str()));
    }

    [[nodiscard]] inline bool operator ==(const_pointer rhs) const noexcept  {

        return (compare(rhs) == 0);
    }
    [[nodiscard]] inline bool
    operator ==(const VirtualString &rhs) const noexcept  {

        return (*this == rhs.c_str());
    }
    [[nodiscard]] inline bool operator !=(const_pointer rhs) const noexcept  {

        return (compare(rhs) != 0);
    }
    [[nodiscard]] inline bool
    operator !=(const VirtualString &rhs) const noexcept  {

        return (*this != rhs.c_str());
    }
    [[nodiscard]] inline bool operator >(const_pointer rhs) const noexcept  {

        return (compare(rhs) > 0);
    }
    [[nodiscard]] inline bool operator >=(const_pointer rhs) const noexcept  {

        return (compare(rhs) >= 0);
    }
    [[nodiscard]] inline bool
    operator >(const VirtualString &rhs) const noexcept  {

        return (*this > rhs.c_str());
    }
    [[nodiscard]] inline bool
    operator >=(const VirtualString &rhs) const noexcept  {

        return (*this >= rhs.c_str());
    }
    [[nodiscard]] inline bool operator <(const_pointer rhs) const noexcept  {

        return (compare(rhs) < 0);
    }
    [[nodiscard]] inline bool operator <=(const_pointer rhs) const noexcept  {

        return (compare(rhs) <= 0);
    }
    [[nodiscard]] inline bool
    operator <(const VirtualString &rhs) const noexcept  {

        return (*this < rhs.c_str());
    }
    [[nodiscard]] inline bool
    operator <=(const VirtualString &rhs) const noexcept  {

        return (*this <= rhs.c_str());
    }

    // char based access methods.
    //
    [[nodiscard]] inline const_reference
    operator [](size_type index) const noexcept  {

        return (string_[index]);
    }
    [[nodiscard]] inline reference operator [](size_type index) noexcept  {

        return (string_[index]);
    }

    inline void clear() noexcept  { *string_ = 0; size_ = 0; }

    // These two make it compatible with std::string
    //
    inline void resize(size_type) noexcept  {  }
    inline void resize(size_type, value_type) noexcept  {  }

    // const utility methods.
    //
    [[nodiscard]] inline const_pointer
    c_str() const noexcept  { return (string_); }

    [[nodiscard]] inline const_pointer
    data() const noexcept  { return (string_); }
    [[nodiscard]] inline pointer data() noexcept  { return (string_); }

    [[nodiscard]] inline const_pointer
    sub_c_str(size_type offset) const noexcept  {

        return (offset != npos ? string_ + offset : nullptr);
    }
    [[nodiscard]] inline size_type size() const noexcept  { return (size_); }
    [[nodiscard]] inline bool empty() const noexcept  { return (size_ == 0); }

    // Fowler–Noll–Vo (FNV-1a) hash function
    // This is for 64-bit systems
    //
    [[nodiscard]] inline size_type hash() const noexcept {

        size_type   h { 14695981039346656037UL }; // offset basis

        for (const_pointer s { string_ }; *s; ++s)
            h = (h ^ static_cast<unsigned char>(*s)) * 1099511628211UL;
        return (h);
    }

protected:

    inline VirtualString(pointer str) noexcept : string_(str)  {  }

    inline void _forced_size()  { size_ = ::strlen(string_); }

private:

    pointer     string_;
    size_type   size_ { 0 };
};

// ----------------------------------------------------------------------------

// This is a fixed size NTBS. Its sole purpose is performance. Most often
// programmers use utility strings with known upper limit size.
// FixedSizeString makes no dynamic allocation/de-allocation and is
// strictly stack based. This will improve the performance of multithreading
// applications that use a lot of utility strings.
//
// NOTE: FixedSizeString makes no boundary checks. It is the
//       responsibility of the programmer to take care of that.
//
template <unsigned int S>
class   FixedSizeString : public VirtualString  {

public:

    inline FixedSizeString() noexcept
        : VirtualString(buffer_) { *buffer_ = 0; _forced_size(); }
    inline FixedSizeString(const_pointer str) noexcept
        : VirtualString(buffer_)  { *this = str; }
    inline FixedSizeString(const FixedSizeString &that) noexcept
        : VirtualString(buffer_)  { *this = that; }
    inline FixedSizeString(const VirtualString &that) noexcept
        : VirtualString(buffer_)  { *this = that; }
    inline FixedSizeString(const std::string &that) noexcept
        : VirtualString(buffer_)  { *this = that.c_str(); }

    // This is a constructor with the same signature as std::string
    // but here the size is ignored
    //
    inline FixedSizeString(size_type , value_type v) noexcept
        : VirtualString(buffer_)  {

        std::memset(buffer_, v, S);
        buffer_[S] = 0;
        _forced_size();
    }

    // Assignment methods which cannot be inherited or virtual.
    //
    inline FixedSizeString &operator =(const FixedSizeString &rhs) noexcept  {

        ::strcpy(buffer_, rhs.buffer_);
        _forced_size();
        return (*this);
    }
    inline FixedSizeString &operator =(const_pointer rhs) noexcept {

        snprintf_nowarn(buffer_, S, "%s", rhs);
        _forced_size();
        return (*this);
    }
    inline FixedSizeString &operator =(const VirtualString &rhs) noexcept  {

        *this = rhs.c_str();
        return (*this);
    }
    inline FixedSizeString &operator =(const std::string &rhs) noexcept  {

        snprintf_nowarn(buffer_, S, "%s", rhs.c_str());
        _forced_size();
        return (*this);
    }

    static inline size_type capacity() noexcept  { return (S); }

private:

    value_type  buffer_[S + 1];
};

// ----------------------------------------------------------------------------

template<typename CharT, typename Traits>
inline std::basic_ostream<CharT, Traits> &
operator <<(std::basic_ostream<CharT, Traits> &lhs, const VirtualString &rhs) {

    return (lhs << rhs.c_str());
}

// ----------------------------------------------------------------------------

// Convenient typedefs
//
using String8 = FixedSizeString<7>;
using String16 = FixedSizeString<15>;
using String32 = FixedSizeString<31>;
using String64 = FixedSizeString<63>;
using String128 = FixedSizeString<127>;
using String512 = FixedSizeString<511>;
using String1K = FixedSizeString<1023>;
using String2K = FixedSizeString<2047>;

} // namespace hmdf

// ----------------------------------------------------------------------------

namespace std  {
template<>
struct  hash<typename hmdf::VirtualString>  {

    inline size_t operator()(const hmdf::VirtualString &key) const noexcept {

        return (key.hash());
    }
};

} // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
