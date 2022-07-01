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

#include <DataFrame/DataFrameExports.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <ctime>
#include <limits>
#include <stdexcept>
#include <string>

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

class   DateTime {

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
    HMDF_API explicit DateTime (DT_TIME_ZONE tz = DT_TIME_ZONE::LOCAL);

    HMDF_API explicit DateTime (DateType d,
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
    HMDF_API explicit DateTime (const char *s,
                                DT_DATE_STYLE ds = DT_DATE_STYLE::YYYYMMDD,
                                DT_TIME_ZONE tz = DT_TIME_ZONE::LOCAL);

    HMDF_API DateTime (const DateTime &that);
    HMDF_API DateTime (DateTime &&that);
    HMDF_API ~DateTime ();

    HMDF_API DateTime &operator = (const DateTime &rhs);
    HMDF_API DateTime &operator = (DateTime &&rhs);

    // A convenient method, if you already have a DateTime instance
    // and want to change the date/time quickly
    //
    HMDF_API void
    set_time (EpochType the_time,
              NanosecondType nanosec = 0) noexcept;

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    HMDF_API void set_timezone (DT_TIME_ZONE tz);
    HMDF_API DT_TIME_ZONE get_timezone () const;

    HMDF_API DateTime &operator = (DateType rhs);  // dt = 20181223

    // Currently, the following formats are supported:
    //  1)  YYYYMMDD [LOCAL | GMT]
    //  2)  YYYYMMDD HH:MM:SS.MMM [LOCAL | GMT]
    //
    HMDF_API DateTime &operator = (const char *rhs);  // dt = "20181223"

    // This compares lhs (self) with rhs
    //
    HMDF_API EpochType compare(const DateTime &rhs) const;

    HMDF_API DateType date () const noexcept;            // eg. 20020303
    HMDF_API DatePartType year () const noexcept;        // eg. 1990
    HMDF_API DT_MONTH month () const noexcept;           // JAN - DEC
    HMDF_API DatePartType dmonth () const noexcept;      // 1 - 31
    HMDF_API DatePartType dyear () const noexcept;       // 1 - 366
    HMDF_API DT_WEEKDAY dweek () const noexcept;         // SUN - SAT
    HMDF_API HourType hour () const noexcept;            // 0 - 23
    HMDF_API MinuteType minute () const noexcept;        // 0 - 59
    HMDF_API SecondType sec () const noexcept;           // 0 - 59
    HMDF_API MillisecondType msec () const noexcept;     // 0 - 999
    HMDF_API MicrosecondType microsec () const noexcept; // 0 - 999,999
    HMDF_API NanosecondType nanosec () const noexcept;   // 0 - 999,999,999
    HMDF_API EpochType time () const noexcept;           // Like ::time()
    HMDF_API LongTimeType long_time () const noexcept;   // Nanosec since epoch

    HMDF_API operator double() const noexcept;

    HMDF_API DatePartType days_in_month () const noexcept;  // 28, 29, 30, 31

    // These return the diff including the fraction of the unit.
    // That is why they return a double.
    // The diff could be +/- based on "this - that"
    //
    HMDF_API double diff_seconds (const DateTime &that) const;
    HMDF_API double diff_minutes (const DateTime &that) const;
    HMDF_API double diff_hours (const DateTime &that) const;
    HMDF_API double diff_days (const DateTime &that) const;
    HMDF_API double diff_weekdays (const DateTime &that) const;
    HMDF_API double diff_weeks (const DateTime &that) const;

    // The parameter to these methods could be +/-.
    // It will advance/pull back the date/time accordingly.
    //
    HMDF_API void add_nanoseconds (long nanosecs) noexcept;
    HMDF_API void add_seconds (EpochType secs) noexcept;
    HMDF_API void add_days (long days) noexcept;
    HMDF_API void add_weekdays (long days) noexcept;
    HMDF_API void add_months (long months) noexcept;
    HMDF_API void add_years (long years) noexcept;

    HMDF_API bool is_weekend () const noexcept;
    HMDF_API bool is_newyear () const noexcept;
    HMDF_API bool is_xmas () const noexcept;
    HMDF_API bool is_us_business_day () const noexcept;
    HMDF_API bool is_us_bank_holiday () const noexcept;
    HMDF_API bool is_valid () const noexcept;

    // Formats date/time into a string based on format parameter
    //
    template<typename T>
    void date_to_str (DT_FORMAT format,
                      T &result) const;
    HMDF_API std::string string_format (DT_FORMAT format) const;

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
    days_in_month_ (DT_MONTH month,
                    DatePartType year) noexcept;

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    EpochType maketime_ (struct tm &ltime) const noexcept;

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    void breaktime_ (EpochType the_time,
                     NanosecondType nanosec) noexcept;

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

template<typename T>
void DateTime::date_to_str (DT_FORMAT format, T &result) const  {

    String128   buffer;

    switch (format)  {
        case DT_FORMAT::AMR_DT:
        {
            buffer.printf ("%002d/%002d/%002d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()) % 100);
        } break;

        case DT_FORMAT::AMR_DT_CTY:
        {
            buffer.printf ("%002d/%002d/%d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()));
        } break;

        case DT_FORMAT::EUR_DT:
        {
            buffer.printf ("%002d/%002d/%002d",
                           static_cast<int>(dmonth ()),
                           static_cast<int>(month ()),
                           static_cast<int>(year ()) % 100);
        } break;

        case DT_FORMAT::EUR_DT_CTY:
        {
            buffer.printf ("%002d/%002d/%d",
                           static_cast<int>(dmonth ()),
                           static_cast<int>(month ()),
                           static_cast<int>(year ()));
        } break;

        case DT_FORMAT::DT_TM:
        {
            buffer.printf ("%002d/%002d/%d %002d:%002d:%002d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()),
                           static_cast<int>(hour ()),
                           static_cast<int>(minute ()),
                           static_cast<int>(sec ()));
        } break;

        case DT_FORMAT::DT_TM2:
        {
            buffer.printf ("%002d/%002d/%d %002d:%002d:%002d.%0003d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()),
                           static_cast<int>(hour ()),
                           static_cast<int>(minute ()),
                           static_cast<int>(sec ()),
                           static_cast<int>(msec ()));
        } break;

        case DT_FORMAT::ISO_DT_TM:
        {
            buffer.printf ("%d-%002d-%002d %002d:%002d:%002d.%0000006d",
                           static_cast<int>(year ()),
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(hour ()),
                           static_cast<int>(minute ()),
                           static_cast<int>(sec ()),
                           static_cast<int>(microsec ()));
        } break;

        case DT_FORMAT::ISO_DT:
        {
            buffer.printf ("%d-%002d-%002d",
                           static_cast<int>(year ()),
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()));
        } break;

        case DT_FORMAT::DT_DATETIME:
        {
            buffer.printf ("%d%002d%002d  %002d:%002d:%002d.%0003d",
                           static_cast<int>(year ()),
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(hour ()),
                           static_cast<int>(minute ()),
                           static_cast<int>(sec ()),
                           static_cast<int>(msec ()));
        } break;

        case DT_FORMAT::SCT_DT:
        {
            buffer.printf ("%s %002d, %d",
                           MONTH_BR_ [static_cast<int>(month ()) - 1],
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()));
        } break;

        case DT_FORMAT::DT_MMDDYYYY:
        {
            buffer.printf ("%002d%002d%d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()));
        } break;

        case DT_FORMAT::DT_YYYYMMDD:
        {
            buffer.printf ("%002d%002d%002d",
                           static_cast<int>(year ()),
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()));
        } break;

        case DT_FORMAT::DT_PRECISE:  // e.g. Epoch.Nanoseconds
        {
            buffer.printf ("%ld.%d", this->time(), nanosec());
        } break;

        default:
        {
            buffer.printf ("ERROR: DateTime::date_to_str(): "
                           "Unknown format: '%u'",
                           format);
            throw std::runtime_error (buffer.c_str ());
        }
    }

    result = buffer.c_str ();
    return;
}

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

        return (hash<double>()(double(key)));
    }
};

} // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:

