// Hossein Moein
// March 18, 2018
/*
Copyright (c) 2019-2022, Hossein Moein
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

#include <cstdio>
#include <ctime>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <sys/timeb.h>
#include <time.h>

#if defined(_WIN32) || defined(_WIN64)
#  include <windows.h>
#  ifdef _MSC_VER
#    ifdef LIBRARY_EXPORTS
#      define LIBRARY_API __declspec(dllexport)
#    else
#      define LIBRARY_API __declspec(dllimport)
#    endif // LIBRARY_EXPORTS
#  else
#    define LIBRARY_API
#  endif // _MSC_VER
#  ifdef min
#    undef min
#  endif // min
#  ifdef max
#    undef max
#  endif // max
#else
#  define LIBRARY_API
#endif // _WIN32 || _WIN64

// ----------------------------------------------------------------------------

namespace hmdf
{

// The following constants are for formatting values in
// DateTime::string_format().
//
enum class DT_FORMAT : unsigned short int  {
    AMR_DT = 1,        // e.g. 09/16/99
    AMR_DT_CTY = 2,    // e.g. 09/16/1999
    EUR_DT = 3,        // e.g. 16/09/99
    EUR_DT_CTY = 4,    // e.g. 16/09/1999
    DT_TM = 5,         // e.g. 09/16/1999 13:51:04
    SCT_DT = 6,        // e.g. Sep 16, 1999
    DT_MMDDYYYY = 7,   // e.g. 09161999
    DT_YYYYMMDD = 8,   // e.g. 19990916
    DT_TM2 = 9,        // e.g. 09/16/1999 13:51:04.256
    DT_DATETIME = 10,  // e.g. 20010103   09:31:15.124
    DT_PRECISE = 11,   // e.g. 1516179600.874123908 = Epoch.Nanoseconds
    ISO_DT_TM = 12,    // e.g. 2015-05-05 13:51:04.000234
    ISO_DT = 13,       // e.g. 2015-05-05
};

// DO NOT change the values of these enums. They are offsets to an
// static array.
//
enum class DT_TIME_ZONE : short int  {
    LOCAL = -2,
    GMT = 0,
    AM_BUENOS_AIRES = 1,
    AM_CHICAGO = 2,
    AM_LOS_ANGELES = 3,
    AM_MEXICO_CITY = 4,
    AM_NEW_YORK = 5,
    AS_DUBAI = 6,
    AS_HONG_KONG = 7,
    AS_SHANGHAI = 8,
    AS_SINGAPORE = 9,
    AS_TEHRAN = 10,
    AS_TEL_AVIV = 11,
    AS_TOKYO = 12,
    AU_MELBOURNE = 13,
    AU_SYDNEY = 14,
    BR_RIO_DE_JANEIRO = 15,
    EU_BERLIN = 16,
    EU_LONDON = 17,
    EU_MOSCOW = 18,
    EU_PARIS = 19,
    EU_ROME = 20,
    EU_VIENNA = 21,
    EU_ZURICH = 22,
    UTC = 23,
    AS_SEOUL = 24,
    AS_TAIPEI = 25,
    EU_STOCKHOLM = 26,
    NZ = 27,
    EU_OSLO = 28,
    EU_WARSAW = 29,
    EU_BUDAPEST = 30
};

// 1 - 7 (Sunday - Saturday)
// DO NOT change the numbers
//
enum class DT_WEEKDAY : unsigned char  {
    BAD_DAY = 0,
    SUN = 1,
    MON = 2,
    TUE = 3,
    WED = 4,
    THU = 5,
    FRI = 6,
    SAT = 7
};

// 1 - 12 (January - December)
// DO NOT change the numbers
//
enum class DT_MONTH : unsigned char  {
    BAD_MONTH = 0,
    JAN = 1,
    FEB = 2,
    MAR = 3,
    APR = 4,
    MAY = 5,
    JUN = 6,
    JUL = 7,
    AUG = 8,
    SEP = 9,
    OCT = 10,
    NOV = 11,
    DEC = 12
};

// AME_STYLE: MM/DD/YYYY
// EUR_STYLE: YYYY/MM/DD
// ISO_STYLE: YYYY-MM-DD
//
enum class DT_DATE_STYLE : unsigned char  {
    YYYYMMDD = 1,
    AME_STYLE = 2,
    EUR_STYLE = 3,
    ISO_STYLE = 4,
};

// ----------------------------------------------------------------------------

class LIBRARY_API DateTime {

public:

    using DateType = unsigned int;           // YYYYMMDD
    using DatePartType = unsigned short int; // e.g. year, month etc.
    using HourType = unsigned short int;     // 0 - 23
    using MinuteType = unsigned short int;   // 0 - 59
    using SecondType = unsigned short int;   // 0 - 59
    using MillisecondType = short int;       // 0 - 999
    using MicrosecondType = int;             // 0 - 999,999
    using NanosecondType = int;              // 0 - 999,999,999
    using EpochType = time_t;                // Signed epoch
    using LongTimeType = long long int;      // Nano seconds since epoch

    // Initialized to now
    //
    explicit DateTime (DT_TIME_ZONE tz = DT_TIME_ZONE::LOCAL);

    explicit DateTime (DateType d,
                       HourType hr = 0,
                       MinuteType mn = 0,
                       SecondType sc = 0,
                       NanosecondType ns = 0,
                       DT_TIME_ZONE tz = DT_TIME_ZONE::LOCAL);

    // Currently, the following formats are supported:
    //  (1)  YYYYMMDD
    // AME_STYLE:
    //  (2)  MM/DD/YYYY
    //  (3)  MM/DD/YYYY HH
    //  (4)  MM/DD/YYYY HH:MM
    //  (5)  MM/DD/YYYY HH:MM:SS
    //  (6)  MM/DD/YYYY HH:MM:SS.MMM
    //
    // EUR_STYLE:
    //  (7)  YYYY/MM/DD
    //  (8)  YYYY/MM/DD HH
    //  (9)  YYYY/MM/DD HH:MM
    //  (10) YYYY/MM/DD HH:MM:SS
    //  (11) YYYY/MM/DD HH:MM:SS.MMM
    //
    // ISO_STYLE:
    //  (12) YYYY-MM-DD
    //  (13) YYYY-MM-DD HH
    //  (14) YYYY-MM-DD HH:MM
    //  (15) YYYY-MM-DD HH:MM:SS
    //  (16) YYYY-MM-DD HH:MM:SS.MMM
    //
    explicit DateTime (const char *s,
                       DT_DATE_STYLE ds = DT_DATE_STYLE::YYYYMMDD,
                       DT_TIME_ZONE tz = DT_TIME_ZONE::LOCAL);

    DateTime (const DateTime &that) = default;
    DateTime (DateTime &&that) = default;
    ~DateTime () = default;

    DateTime &operator = (const DateTime &rhs) = default;
    DateTime &operator = (DateTime &&rhs) = default;

    // A convenient method, if you already have a DateTime instance
    // and want to change the date/time quickly
    //
    void set_time (EpochType the_time, NanosecondType nanosec = 0) noexcept;

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    void set_timezone (DT_TIME_ZONE tz);
    DT_TIME_ZONE get_timezone () const;

    DateTime &operator = (DateType rhs);  // dt = 20181223

    // Currently, the following formats are supported:
    //  1)  YYYYMMDD [LOCAL | GMT]
    //  2)  YYYYMMDD HH:MM:SS.MMM [LOCAL | GMT]
    //
    DateTime &operator = (const char *rhs);  // dt = "20181223"

    // this (lhs) compared with rhs
    //
    EpochType compare(const DateTime &rhs) const;

    DateType date () const noexcept;            // eg. 20020303
    DatePartType year () const noexcept;        // eg. 1990
    DT_MONTH month () const noexcept;           // JAN - DEC
    DatePartType dmonth () const noexcept;      // 1 - 31
    DatePartType dyear () const noexcept;       // 1 - 366
    DT_WEEKDAY dweek () const noexcept;         // SUN - SAT
    HourType hour () const noexcept;            // 0 - 23
    MinuteType minute () const noexcept;        // 0 - 59
    SecondType sec () const noexcept;           // 0 - 59
    MillisecondType msec () const noexcept;     // 0 - 999
    MicrosecondType microsec () const noexcept; // 0 - 999,999
    NanosecondType nanosec () const noexcept;   // 0 - 999,999,999
    EpochType time () const noexcept;           // Like ::time()
    LongTimeType long_time () const noexcept;   // Nano seconds since epoch

    DatePartType days_in_month () const noexcept;  // 28, 29, 30, 31

    // These return the diff including the fraction of the unit.
    // That is why they return a double.
    // The diff could be +/- based on "this - that"
    //
    double diff_seconds (const DateTime &that) const;
    double diff_minutes (const DateTime &that) const noexcept;
    double diff_hours (const DateTime &that) const noexcept;
    double diff_days (const DateTime &that) const noexcept;
    double diff_weekdays (const DateTime &that) const noexcept;
    double diff_weeks (const DateTime &that) const noexcept;

    // The parameter to these methods could be +/-.
    // It will advance/pull back the date/time accordingly.
    //
    void add_nanoseconds (long nanosecs) noexcept;
    void add_seconds (EpochType secs) noexcept;
    void add_days (long days) noexcept;
    void add_weekdays (long days) noexcept;
    void add_months (long months) noexcept;
    void add_years (long years) noexcept;

    bool is_weekend () const noexcept;
    bool is_newyear () const noexcept;
    bool is_xmas () const noexcept;
    bool is_us_business_day () const noexcept;
    bool is_us_bank_holiday () const noexcept;
    bool is_valid () const noexcept;

    // Formats date/time into a string based on format parameter
    //
    template<typename T>
    void date_to_str (DT_FORMAT format, T &result) const;
    std::string string_format (DT_FORMAT format) const;

private:

    template<typename T>
    using INVALID_VALUE_ = typename std::numeric_limits<T>;

    inline static const char    *TIMEZONES_[] {
#ifdef _MSC_VER
        "\"TZ=GMT\"",
        "\"TZ=Argentina Standard Time\"",        // "America/Buenos_Aires",
        "\"TZ=Central Standard Time\"",          // "America/Chicago",
        "\"TZ=Pacific Standard Time\"",          // "America/Los_Angeles",
        "\"TZ=Central Standard Time (Mexico)\"", // "America/Mexico_City",
        "\"TZ=Eastern Standard Time\"",          // "America/New_York",
        "\"TZ=Arabian Standard Time\"",          // "Asia/Dubai",
        "\"TZ=China Standard Time\"",            // "Asia/Hong_Kong",
        "\"TZ=China Standard Time\"",            // "Asia/Shanghai",
        "\"TZ=Singapore Standard Time\"",        // "Asia/Singapore",
        "\"TZ=Iran Standard Time\"",             // "Asia/Tehran",
        "\"TZ=Israel Standard Time\"",           // "Asia/Tel_Aviv",
        "\"TZ=Tokyo Standard Time\"",            // "Asia/Tokyo",
        "\"TZ=AUS Eastern Standard Time\"",      // "Australia/Melbourne",
        "\"TZ=GMT-10\"",                         // "Australia/NSW",
        "\"TZ=E. South America Standard Time\"", // "Brazil/East",
        "\"TZ=W. Europe Standard Time\"",        // "Europe/Berlin",
        "\"TZ=GMT Standard Time\"",              // "Europe/London",
        "\"TZ=Russian Standard Time\"",          // "Europe/Moscow",
        "\"TZ=Romance Standard Time\"",          // "Europe/Paris",
        "\"TZ=W. Europe Standard Time\"",        // "Europe/Rome",
        "\"TZ=W. Europe Standard Time\"",        // "Europe/Vienna",
        "\"TZ=W. Europe Standard Time\"",        // "Europe/Zurich",
        "\"TZ=GMT+00\"",                         // "UTC",
        "\"TZ=Korea Standard Time\"",            // "Asia/Seoul",
        "\"TZ=Taipei Standard Time\"",           // "Asia/Taipei",
        "\"TZ=W. Europe Standard Time\"",        // "Eurpoe/Sweden",
        "\"TZ=New Zealand Standard Time\"",      // "NZ",
        "\"TZ=Central Europe Standard Time\"",   // "Europe/Oslo",
        "\"TZ=Central European Standard Time\"", // "Europe/Warsaw",
        "\"TZ=Central Europe Standard Time\""    // "Europe/Budapest"
#else
        "GMT",
        "America/Buenos_Aires",
        "America/Chicago",
        "America/Los_Angeles",
        "America/Mexico_City",
        "America/New_York",
        "Asia/Dubai",
        "Asia/Hong_Kong",
        "Asia/Shanghai",
        "Asia/Singapore",
        "Asia/Tehran",
        "Asia/Tel_Aviv",
        "Asia/Tokyo",
        "Australia/Melbourne",
        "Australia/NSW",
        "Brazil/East",
        "Europe/Berlin",
        "Europe/London",
        "Europe/Moscow",
        "Europe/Paris",
        "Europe/Rome",
        "Europe/Vienna",
        "Europe/Zurich",
        "UTC",
        "Asia/Seoul",
        "Asia/Taipei",
        "Eurpoe/Stockholm",
        "NZ",
        "Europe/Oslo",
        "Europe/Warsaw",
        "Europe/Budapest"
#endif // _MSC_VER
    };
    inline static const EpochType   INVALID_TIME_T_ {
        INVALID_VALUE_<EpochType>::max()
    };
    inline static const DateType    INVALID_DATE_ {
        INVALID_VALUE_<DateType>::max()
    };
    inline static const HourType    INVALID_HOUR_ {
        INVALID_VALUE_<HourType>::max()
    };
    inline static const MinuteType  INVALID_MINUTE_ {
        INVALID_VALUE_<MinuteType>::max()
    };
    inline static const SecondType  INVALID_SECOND_ {
        INVALID_VALUE_<SecondType>::max()
    };

    // This guy initializes anything that needs to be initialized
    // statically.
    //
    struct  DT_initializer  {
        DT_initializer () noexcept  {
#ifdef _MSC_VER
            _tzset ();
#else
            ::tzset ();
#endif // _MSC_VER
        }
    };

    inline static const DT_initializer  dt_init_ {  };

    DateType        date_ { INVALID_DATE_ };  // Like 20190518
    HourType        hour_ { INVALID_HOUR_ };
    MinuteType      minute_ { INVALID_MINUTE_ };
    SecondType      second_ { INVALID_SECOND_ };
    NanosecondType  nanosecond_ { 0 };
    EpochType       time_ { INVALID_TIME_T_ }; // Sec since 01/01/1970 (Epoch)
    DT_WEEKDAY      week_day_ { DT_WEEKDAY::BAD_DAY };
    DT_TIME_ZONE    time_zone_ { DT_TIME_ZONE::LOCAL };

    inline static void change_env_timezone_(DT_TIME_ZONE time_zone);
    inline static void reset_env_timezone_(DT_TIME_ZONE time_zone);

    static DatePartType
    days_in_month_ (DT_MONTH month, DatePartType year) noexcept;

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    EpochType maketime_ (struct tm &ltime) const noexcept;

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    void breaktime_ (EpochType the_time, NanosecondType nanosec) noexcept;

    inline static const char *const MONTH_[] {
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December"
    };
    inline static const char *const MONTH_BR_[] {
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec"
    };
    inline static const char *const WDAY_[] {
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday"
    };
    inline static const char *const WDAY_BR_[] {
        "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
    };
};

// ----------------------------------------------------------------------------

inline bool operator == (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.compare (rhs) == 0);
}

// ----------------------------------------------------------------------------

inline bool operator != (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.compare (rhs) != 0);
}

// ----------------------------------------------------------------------------

inline bool operator < (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.compare (rhs) < 0);
}

// ----------------------------------------------------------------------------

inline bool operator <= (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.compare (rhs) <= 0);
}

// ----------------------------------------------------------------------------

inline bool operator > (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.compare (rhs) > 0);
}

// ----------------------------------------------------------------------------

inline bool operator >= (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.compare (rhs) >= 0);
}

// ----------------------------------------------------------------------------

template<typename S>
inline S &operator << (S &o, const DateTime &rhs)  {

    return (o << rhs.string_format (DT_FORMAT::DT_TM2));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

namespace std  {
template<>
struct  hash<typename hmdf::DateTime>  {

    inline size_t
    operator()(const typename hmdf::DateTime &key) const noexcept {

        return (hash<typename hmdf::DateTime::LongTimeType>()(key.long_time()));
    }
};

} // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:

