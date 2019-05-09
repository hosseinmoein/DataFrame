// Hossein Moein
// March 18, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#ifndef HMDF_DATETIME_HPP
#define HMDF_DATETIME_HPP

#ifdef _WIN32
#include <windows.h>
#endif // _WIN32

#include <cstdio>
#include <ctime>
#include <stdexcept>

#include <sys/timeb.h>

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
    DT_PRECISE = 11    // e.g. 1516179600.874123908 = Epoch.Nanoseconds
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
//
enum class DT_DATE_STYLE : unsigned char  {
    YYYYMMDD = 1,
    AME_STYLE = 2,
    EUR_STYLE = 3
};

// ----------------------------------------------------------------------------

class   DateTime  {

public:

    using DateType = unsigned int;           // YYYYMMDD
    using DatePartType = unsigned short int; // e.g. year, month etc.
    using HourType = unsigned short int;     // 0 - 23
    using MinuteType = unsigned short int;   // 0 - 59
    using SecondType = unsigned short int;   // 0 - 59
    using MillisecondType = short int;       // 0 - 999
    using MicrosecondType = int;             // 0 - 999,999
    using NanosecondType =  int;             // 0 - 999,999,999
    using EpochType = time_t;                // Signed epoch

private:

    static const char       *TIMEZONES_[];
    static const EpochType  INVALID_TIME_T_ = -1;

    // This guy initializes anything that needs to be initialized
    // statically.
    //
    class   DT_initializer  { public: DT_initializer () noexcept; };

    static const DT_initializer dt_init_;

    friend class    DT_initializer;

    DateType        date_ { DateType(INVALID_TIME_T_) };   // e.g. 20001025
    HourType        hour_ { HourType(INVALID_TIME_T_) };
    MinuteType      minute_ { MinuteType(INVALID_TIME_T_) };
    SecondType      second_ { SecondType(INVALID_TIME_T_) };
    NanosecondType  nanosecond_ { 0 };
    EpochType       time_ { INVALID_TIME_T_ }; // Sec since 01/01/1970 (Epoch)
    DT_WEEKDAY      week_day_ { DT_WEEKDAY::BAD_DAY };
    DT_TIME_ZONE    time_zone_ { };

public:

    // Initialized to now
    explicit DateTime (DT_TIME_ZONE the_zone = DT_TIME_ZONE::LOCAL) noexcept;

    explicit DateTime (DateType d,
                       HourType hr = 0,
                       MinuteType mn = 0,
                       SecondType sc = 0,
                       NanosecondType ns = 0,
                       DT_TIME_ZONE tz = DT_TIME_ZONE::LOCAL) noexcept;

    // Currently, the following formats are supported:
    //  (1)  YYYYMMDD
    // AME_STYLE:
    //  (2)  DD/MM/YYYY
    //  (3)  DD/MM/YYYY HH
    //  (4)  DD/MM/YYYY HH:MM
    //  (5)  DD/MM/YYYY HH:MM:SS
    //  (6)  DD/MM/YYYY HH:MM:SS.MMM
    //
    // EUR_STYLE:
    //  (7)  YYYY/MM/DD
    //  (8)  YYYY/MM/DD HH
    //  (9)  YYYY/MM/DD HH:MM
    //  (10) YYYY/MM/DD HH:MM:SS
    //  (11) YYYY/MM/DD HH:MM:SS.MMM
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
    void set_time (EpochType the_time, NanosecondType nanosec = 0) noexcept;

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    void set_timezone (DT_TIME_ZONE tz);
    inline DT_TIME_ZONE get_timezone () const  { return (time_zone_); }

    DateTime &operator = (DateType rhs);  // dt = 20181223

    // Currently, the following formats are supported:
    //  1)  YYYYMMDD [LOCAL | GMT]
    //  2)  YYYYMMDD HH:MM:SS.MMM [LOCAL | GMT]
    //
    DateTime &operator = (const char *rhs);  // dt = "20181223"

    // this (lhs) compared with rhs
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
    EpochType time () const noexcept;           // Like time()

    DatePartType days_in_month () const noexcept;  // 28, 29, 30, 31

    // These return the diff including the fraction of the unit.
    // That is why they return a double.
    // The diff could be +/- based on "this - that"
    double diff_seconds (const DateTime &that) const;
    double diff_minutes (const DateTime &that) const noexcept;
    double diff_hours (const DateTime &that) const noexcept;
    double diff_days (const DateTime &that) const noexcept;
    double diff_weekdays (const DateTime &that) const noexcept;
    double diff_weeks (const DateTime &that) const noexcept;

    // The parameter to these methods could be +/-.
    // It will advance/pull back the date/time accordingly.
    void add_seconds (EpochType secs) noexcept;
    void add_days (long days) noexcept;
    void add_weekdays (long days) noexcept;

    bool is_weekend () const noexcept;
    bool is_newyear () const noexcept;
    bool is_xmas () const noexcept;
    bool is_us_business_day () const noexcept;
    bool is_us_bank_holiday () const noexcept;
    bool is_valid () const noexcept;

    // Formats date/time into a string based on format parameter
    template<typename T>
    void date_to_str (DT_FORMAT format, T &result) const;
    std::string string_format (DT_FORMAT format) const;

private:

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

    static const char *const    MONTH_[];
    static const char *const    MONTH_BR_[];
    static const char *const    WDAY_[];
    static const char *const    WDAY_BR_[];
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

#endif //HMDF_DATETIME_HPP

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:

